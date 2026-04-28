# restore-wwi.ps1 - One-time WideWorldImporters database restore.
# Run ONCE after up.ps1 has finished. Requires WideWorldImporters-Full.bak in the project root.
# Run from the project root: .\restore-wwi.ps1

$MSSQL_PASSWORD = "ActionDag!1"
$SCRIPT_DIR     = $PSScriptRoot
$TF_DIR         = "$SCRIPT_DIR\infra\terraform"
$DB_NAME        = "WideWorldImporters"
$TARGET_DB      = "TargetDB"
$PORT           = 1433
$LOGIN          = "sqladmin"
$MAX_ATTEMPTS   = 40

function Info($msg)  { Write-Host "[ok]  $msg" -ForegroundColor Green }
function Fail($msg)  { Write-Host "[error] $msg" -ForegroundColor Red; exit 1 }

$SOURCE_S3 = "s3://actiondag-ks/WideWorldImporters-Full.bak"

Set-Location $TF_DIR
$RDS       = terraform output -raw sqlserver_endpoint
$ARTIFACTS = terraform output -raw artifacts_bucket
$S3_ARN    = "arn:aws:s3:::$ARTIFACTS/WideWorldImporters-Full.bak"

# Detect public IP and open SQL Server port in the security group
$MY_IP = (Invoke-RestMethod -Uri "https://checkip.amazonaws.com").Trim()
Info "Your public IP: $MY_IP - opening port 1433 in RDS security group..."
$env:TF_VAR_developer_cidr_blocks = '["' + $MY_IP + '/32"]'
terraform apply -auto-approve `
    -var="db_password=$MSSQL_PASSWORD" `
    -var="mssql_password=$MSSQL_PASSWORD"
Info "Security group updated"

Info "Copying backup from $SOURCE_S3 to s3://$ARTIFACTS ..."
aws s3 cp $SOURCE_S3 "s3://$ARTIFACTS/WideWorldImporters-Full.bak"
Info "Copy complete"

# --- Initiate restore --------------------------------------------------------
Write-Host "Initiating restore of '$DB_NAME' from $S3_ARN ..." -ForegroundColor Yellow

sqlcmd -S "$RDS,$PORT" -U $LOGIN -P $MSSQL_PASSWORD -b -Q "
EXEC msdb.dbo.rds_restore_database
    @restore_db_name = '$DB_NAME',
    @s3_arn_to_restore_from = '$S3_ARN',
    @with_norecovery = 0,
    @type = 'FULL';"

Write-Host "Restore task submitted. Polling every 30s (typical time: 5-15 min) ..."

# --- Poll until done ---------------------------------------------------------
$STATUS = ""
for ($i = 1; $i -le $MAX_ATTEMPTS; $i++) {
    Start-Sleep -Seconds 30
    $STATUS = sqlcmd -S "$RDS,$PORT" -U $LOGIN -P $MSSQL_PASSWORD -h -1 -W -Q "
SET NOCOUNT ON;
SELECT TOP 1 lifecycle FROM msdb.dbo.rds_task_status
WHERE database_name = '$DB_NAME' ORDER BY created_at DESC;" 2>$null |
        Where-Object { $_ -match '\S' } | Select-Object -First 1
    $STATUS = if ($STATUS) { $STATUS.Trim() } else { "PENDING" }
    Write-Host "  [$i/$MAX_ATTEMPTS] status = $STATUS"

    if ($STATUS -eq "SUCCESS") { break }
    if ($STATUS -eq "ERROR") {
        Write-Host "Restore failed. Task details:" -ForegroundColor Red
        sqlcmd -S "$RDS,$PORT" -U $LOGIN -P $MSSQL_PASSWORD -Q "
SELECT TOP 1 task_info, error_message FROM msdb.dbo.rds_task_status
WHERE database_name = '$DB_NAME' ORDER BY created_at DESC;"
        exit 1
    }
}

if ($STATUS -ne "SUCCESS") { Fail "Timed out after $($MAX_ATTEMPTS * 30)s. Check rds_task_status manually." }
Info "Restore completed successfully."

# --- Create TargetDB and Reporting tables ------------------------------------
Write-Host "Creating TargetDB database and Reporting tables..."

# Create TargetDB if it doesn't exist (connect to master first)
sqlcmd -S "$RDS,$PORT" -U $LOGIN -P $MSSQL_PASSWORD -b -Q "
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '$TARGET_DB')
    CREATE DATABASE [$TARGET_DB];
PRINT 'TargetDB ready.';"

sqlcmd -S "$RDS,$PORT" -U $LOGIN -P $MSSQL_PASSWORD -d $TARGET_DB -b -Q "
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'OrderEnriched')
    CREATE TABLE dbo.OrderEnriched (
        OrderID INT, CustomerID INT, CustomerName NVARCHAR(100), IsOnCreditHold BIT,
        CreditLimit DECIMAL(18,2), StandardDiscountPercentage DECIMAL(18,3),
        SalespersonPersonID INT, SalespersonName NVARCHAR(50), OrderDate DATE,
        ExpectedDeliveryDate DATE, CustomerPurchaseOrderNumber NVARCHAR(20),
        IsUndersupplyBackordered BIT, Comments NVARCHAR(MAX), PromisedLeadDays INT,
        etl_loaded_at DATETIME2 DEFAULT SYSDATETIME());

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'OrderLineDetail')
    CREATE TABLE dbo.OrderLineDetail (
        OrderLineID INT, OrderID INT, StockItemID INT, StockItemDescription NVARCHAR(100),
        Quantity INT, UnitPrice DECIMAL(18,2), TaxRate DECIMAL(18,3), LineTotal DECIMAL(18,2),
        TaxAmount DECIMAL(18,2), LineTotalIncTax DECIMAL(18,2), PickedQuantity INT,
        OutstandingQty INT, PickingStatus NVARCHAR(20), PickingCompletedWhen DATETIME2,
        etl_loaded_at DATETIME2 DEFAULT SYSDATETIME());

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'CustomerMonthlySales')
    CREATE TABLE dbo.CustomerMonthlySales (
        CustomerID INT, CustomerName NVARCHAR(100), IsOnCreditHold BIT, CreditLimit DECIMAL(18,2),
        OrderMonth DATE, OrderCount INT, TotalOrderLines INT, TotalUnits INT,
        TotalRevenue DECIMAL(18,2), TotalTax DECIMAL(18,2), TotalRevIncTax DECIMAL(18,2),
        AvgLineValue DECIMAL(18,2), UniqueItemsBought INT,
        etl_loaded_at DATETIME2 DEFAULT SYSDATETIME());

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'InventoryValuation')
    CREATE TABLE dbo.InventoryValuation (
        StockItemID INT, StockItemName NVARCHAR(100), SupplierID INT, SupplierName NVARCHAR(100),
        CurrentSalePrice DECIMAL(18,2), RecommendedRetailPrice DECIMAL(18,2), TaxRate DECIMAL(18,3),
        LeadTimeDays INT, IsChillerStock BIT, Brand NVARCHAR(50), Size NVARCHAR(20),
        QuantityOnHand INT, BinLocation NVARCHAR(20), LastCostPrice DECIMAL(18,2),
        LastStocktakeQuantity INT, ReorderLevel INT, TargetStockLevel INT,
        StockCostValue DECIMAL(18,2), StockRetailValue DECIMAL(18,2), StockSaleValue DECIMAL(18,2),
        StockGap INT, StockStatus NVARCHAR(20), etl_loaded_at DATETIME2 DEFAULT SYSDATETIME());

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'SlowMovingStock')
    CREATE TABLE dbo.SlowMovingStock (
        StockItemID INT, StockItemName NVARCHAR(100), SupplierID INT, SupplierName NVARCHAR(100),
        IsChillerStock BIT, QuantityOnHand INT, ReorderLevel INT, LastCostPrice DECIMAL(18,2),
        StockValue DECIMAL(18,2), TxCount30Days INT, TotalQtyOut30Days INT,
        AvgDailyMovement DECIMAL(18,2), DaysOfStock INT, MovementCategory NVARCHAR(20),
        etl_loaded_at DATETIME2 DEFAULT SYSDATETIME());

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'SupplierPerformance')
    CREATE TABLE dbo.SupplierPerformance (
        SupplierID INT, SupplierName NVARCHAR(100), PhoneNumber NVARCHAR(20), PaymentDays INT,
        AvgLeadTimeDays DECIMAL(18,1), UniqueItemsStocked INT, TotalTransactions30Days INT,
        TotalUnitsReceived30Days INT, TotalUnitsReturned30Days INT, ReturnRatePct DECIMAL(18,2),
        etl_loaded_at DATETIME2 DEFAULT SYSDATETIME());

PRINT 'TargetDB tables ready.';"

# Remove developer IP from security group now that restore is done
Info "Closing port 1433 - removing developer IP from security group..."
Set-Location $TF_DIR
$env:TF_VAR_developer_cidr_blocks = "[]"
terraform apply -auto-approve `
    -var="db_password=$MSSQL_PASSWORD" `
    -var="mssql_password=$MSSQL_PASSWORD"
Remove-Item Env:TF_VAR_developer_cidr_blocks -ErrorAction SilentlyContinue
Info "Security group restored to VPC-only access"

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Green
Write-Host "  WideWorldImporters restore complete." -ForegroundColor Green
Write-Host "=====================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next: trigger wwi_sales_etl and wwi_inventory_etl DAGs from the MWAA console."
