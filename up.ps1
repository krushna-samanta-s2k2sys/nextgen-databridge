# up.ps1 - Full NextGenDatabridge provisioning and deployment.
# Run from the project root: .\up.ps1

$DB_PASSWORD    = "ActionDag!1"
$MSSQL_PASSWORD = "ActionDag!1"
$REGION         = if ($env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION } else { "us-east-1" }
$SCRIPT_DIR     = $PSScriptRoot

$env:TF_VAR_db_password    = $DB_PASSWORD
$env:TF_VAR_mssql_password = $MSSQL_PASSWORD

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Info($msg)  { Write-Host "[ok]  $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "[warn] $msg" -ForegroundColor Yellow }
function Fail($msg)  { Write-Host "[error] $msg" -ForegroundColor Red; exit 1 }
function Require($cmd) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Fail "'$cmd' not found in PATH. Install it and retry."
    }
}

Require aws
Require terraform
Require docker
Require kubectl

# --- Step 1: Terraform state bucket ------------------------------------------
Step "STEP 1/5 - Terraform state bucket"
$STATE_BUCKET = "nextgen-databridge-terraform-state"
$null = (aws s3api head-bucket --bucket $STATE_BUCKET 2>&1)
if ($LASTEXITCODE -ne 0) {
    Info "Creating state bucket s3://$STATE_BUCKET ..."
    aws s3api create-bucket --bucket $STATE_BUCKET --region $REGION
    aws s3api put-bucket-versioning --bucket $STATE_BUCKET --versioning-configuration Status=Enabled
} else {
    Info "State bucket already exists: s3://$STATE_BUCKET"
}

# --- Step 2: Terraform -------------------------------------------------------
Step "STEP 2/5 - Terraform: provision all infrastructure (~30 min)"
Set-Location "$SCRIPT_DIR\infra\terraform"

# Clear cached modules so init can re-download cleanly
$modulesCache = "$SCRIPT_DIR\infra\terraform\.terraform\modules"
if (Test-Path $modulesCache) {
    Remove-Item $modulesCache -Recurse -Force -ErrorAction SilentlyContinue
}

terraform init -upgrade -reconfigure
terraform apply -auto-approve -var="db_password=$DB_PASSWORD" -var="mssql_password=$MSSQL_PASSWORD"
if ($LASTEXITCODE -ne 0) { Fail "terraform apply failed." }

$CLUSTER_NAME = terraform output -raw cluster_name
$MWAA_BUCKET  = terraform output -raw mwaa_bucket
$DB_ENDPOINT  = terraform output -raw audit_db_endpoint
$MWAA_URL     = terraform output -raw mwaa_webserver_url
$ACCOUNT      = aws sts get-caller-identity --query Account --output text
$REGISTRY     = "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"

Info "Cluster : $CLUSTER_NAME"
Info "Registry: $REGISTRY"

# --- Step 3: Docker build & push ---------------------------------------------
Step "STEP 3/5 - Docker: build and push API, UI, Transform images"
Set-Location $SCRIPT_DIR

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REGISTRY
if ($LASTEXITCODE -ne 0) { Warn "docker login warning - buildx push may still work" }

docker buildx build --platform linux/amd64 --push -t "$REGISTRY/nextgen-databridge/api:latest" ./backend
if ($LASTEXITCODE -ne 0) { Fail "API image build failed." }
Info "API image pushed"

docker buildx build --platform linux/amd64 --push -t "$REGISTRY/nextgen-databridge/ui:latest" ./frontend
if ($LASTEXITCODE -ne 0) { Fail "UI image build failed." }
Info "UI image pushed"

docker buildx build --platform linux/amd64 --push -t "$REGISTRY/nextgen-databridge/transform:latest" ./eks/jobs
if ($LASTEXITCODE -ne 0) { Fail "Transform image build failed." }
Info "Transform image pushed"

# --- Step 4: Deploy to EKS + MWAA --------------------------------------------
Step "STEP 4/5 - Deploy to EKS and upload DAGs to MWAA"

aws eks update-kubeconfig --region $REGION --name $CLUSTER_NAME
Info "kubeconfig updated"

$DATABASE_URL      = "postgresql+asyncpg://airflow:${DB_PASSWORD}@${DB_ENDPOINT}:5432/airflow"
$MANIFEST_TEMPLATE = "$SCRIPT_DIR\eks\manifests\platform.yaml"
$MANIFEST_TMP      = "$env:TEMP\nextgen-databridge-platform.yaml"

(Get-Content $MANIFEST_TEMPLATE -Raw) `
    -replace "__DATABASE_URL__", $DATABASE_URL `
    -replace "__AIRFLOW_URL__",  "https://$MWAA_URL" |
    Set-Content $MANIFEST_TMP -Encoding utf8

kubectl apply -f $MANIFEST_TMP
kubectl rollout status deployment/nextgen-databridge-api -n nextgen-databridge --timeout=180s
kubectl rollout status deployment/nextgen-databridge-ui  -n nextgen-databridge --timeout=120s
Info "K8s rollout complete"

aws s3 sync "$SCRIPT_DIR\airflow\dags" "s3://$MWAA_BUCKET/dags/" --delete
Info "DAGs synced"

$PLUGINS_ZIP = "$env:TEMP\plugins.zip"
if (Test-Path $PLUGINS_ZIP) { Remove-Item $PLUGINS_ZIP -Force }
Compress-Archive -Path "$SCRIPT_DIR\airflow\plugins\*" -DestinationPath $PLUGINS_ZIP
aws s3 cp $PLUGINS_ZIP "s3://$MWAA_BUCKET/plugins.zip"
Info "Plugins uploaded"

aws s3 cp "$SCRIPT_DIR\airflow\requirements.txt" "s3://$MWAA_BUCKET/requirements.txt"
Info "requirements.txt uploaded"

# --- Step 5: Create TargetDB and output tables in SQL Server -----------------
Step "STEP 5/5 - Create TargetDB and output tables in SQL Server"

$SQL_ENDPOINT = terraform -chdir="$SCRIPT_DIR\infra\terraform" output -raw sqlserver_endpoint
$MY_IP = (Invoke-RestMethod -Uri "https://checkip.amazonaws.com").Trim()
Info "Opening port 1433 for IP $MY_IP ..."
$env:TF_VAR_developer_cidr_blocks = '["' + $MY_IP + '/32"]'
terraform -chdir="$SCRIPT_DIR\infra\terraform" apply -auto-approve `
    -var="db_password=$DB_PASSWORD" `
    -var="mssql_password=$MSSQL_PASSWORD"

sqlcmd -S "$SQL_ENDPOINT,1433" -U sqladmin -P $MSSQL_PASSWORD -b -Q "
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'TargetDB')
    CREATE DATABASE [TargetDB];
PRINT 'TargetDB ready.';"
if ($LASTEXITCODE -ne 0) { Warn "TargetDB creation failed - check SQL Server connectivity" }

sqlcmd -S "$SQL_ENDPOINT,1433" -U sqladmin -P $MSSQL_PASSWORD -d TargetDB -b -Q "
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
if ($LASTEXITCODE -ne 0) { Warn "TargetDB table creation failed - check SQL Server connectivity" }

Info "Closing port 1433..."
$env:TF_VAR_developer_cidr_blocks = "[]"
terraform -chdir="$SCRIPT_DIR\infra\terraform" apply -auto-approve `
    -var="db_password=$DB_PASSWORD" `
    -var="mssql_password=$MSSQL_PASSWORD"
Remove-Item Env:TF_VAR_developer_cidr_blocks -ErrorAction SilentlyContinue
Info "TargetDB and Reporting tables ready"

# --- Done --------------------------------------------------------------------
Write-Host ""
Write-Host "=====================================================" -ForegroundColor Green
Write-Host "  NextGenDatabridge is live." -ForegroundColor Green
Write-Host "=====================================================" -ForegroundColor Green
Write-Host ""
kubectl get svc -n nextgen-databridge `
    -o custom-columns='NAME:.metadata.name,EXTERNAL-IP:.status.loadBalancer.ingress[0].hostname,PORT:.spec.ports[0].port' `
    2>$null
Write-Host ""
