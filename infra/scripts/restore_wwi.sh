#!/usr/bin/env bash
# restore_wwi.sh — Restore WideWorldImporters from S3 into RDS SQL Server.
#
# Run this ONCE after `terraform apply` creates the RDS instance.
# Prerequisites: sqlcmd CLI installed locally, network access to the RDS endpoint
#               (either via VPN / bastion, or temporarily make the instance public).
#
# The WideWorldImporters-Full.bak file must be uploaded to the artifacts S3 bucket BEFORE running.
# Upload command:
#   aws s3 cp WideWorldImporters-Full.bak s3://<artifacts_bucket>/WideWorldImporters-Full.bak
#
# Usage:
#   ./restore_wwi.sh <rds_endpoint> <sqladmin_password> <artifacts_bucket>
#
# Example (reads bucket from Terraform output):
#   TF_DIR="$(dirname "$0")/../terraform"
#   RDS=$(terraform -chdir="$TF_DIR" output -raw sqlserver_endpoint)
#   BUCKET=$(terraform -chdir="$TF_DIR" output -raw artifacts_bucket)
#   ./restore_wwi.sh "$RDS" "YourStr0ng!Pass" "$BUCKET"

set -euo pipefail

ENDPOINT="${1:?Usage: $0 <rds_endpoint> <sqladmin_password> <artifacts_bucket>}"
PASSWORD="${2:?Usage: $0 <rds_endpoint> <sqladmin_password> <artifacts_bucket>}"
ARTIFACTS_BUCKET="${3:?Usage: $0 <rds_endpoint> <sqladmin_password> <artifacts_bucket>}"
PORT=1433
LOGIN="sqladmin"
DB_NAME="WideWorldImporters"
TARGET_DB="TargetDB"
S3_ARN="arn:aws:s3:::${ARTIFACTS_BUCKET}/WideWorldImporters-Full.bak"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

sqlcmd_exec() {
  # -h -1  = no header row   -W = trim trailing spaces   -b = exit on error
  sqlcmd -S "${ENDPOINT},${PORT}" -U "$LOGIN" -P "$PASSWORD" \
         -h -1 -W -b -Q "$1" 2>&1 | head -5
}

# ── Step 1: Initiate the native restore ──────────────────────────────────────
echo -e "${YELLOW}Initiating restore of '$DB_NAME' from $S3_ARN ...${NC}"

sqlcmd -S "${ENDPOINT},${PORT}" -U "$LOGIN" -P "$PASSWORD" -b -Q "
EXEC msdb.dbo.rds_restore_database
    @restore_db_name = '$DB_NAME',
    @s3_arn_to_restore_from = '$S3_ARN',
    @with_norecovery = 0,
    @type = 'FULL';
"

echo "Restore task submitted. Polling rds_task_status every 30 s ..."
echo "(Typical WideWorldImporters restore time: 5–15 minutes)"

# ── Step 2: Poll until done ───────────────────────────────────────────────────
MAX_ATTEMPTS=40  # 40 × 30 s = 20 min
for i in $(seq 1 $MAX_ATTEMPTS); do
  STATUS=$(sqlcmd -S "${ENDPOINT},${PORT}" -U "$LOGIN" -P "$PASSWORD" \
    -h -1 -W -Q "
SET NOCOUNT ON;
SELECT TOP 1 lifecycle
FROM msdb.dbo.rds_task_status
WHERE database_name = '$DB_NAME'
ORDER BY created_at DESC;" 2>/dev/null | head -1 | tr -d '[:space:]')

  echo "  [${i}/${MAX_ATTEMPTS}] status = ${STATUS:-PENDING}"

  if [[ "$STATUS" == "SUCCESS" ]]; then
    echo -e "${GREEN}✅  Restore completed successfully.${NC}"
    break
  elif [[ "$STATUS" == "ERROR" ]]; then
    echo -e "${RED}❌  Restore failed. Task details:${NC}"
    sqlcmd -S "${ENDPOINT},${PORT}" -U "$LOGIN" -P "$PASSWORD" -Q "
SELECT TOP 1 task_info, error_message
FROM msdb.dbo.rds_task_status
WHERE database_name = '$DB_NAME'
ORDER BY created_at DESC;"
    exit 1
  fi

  sleep 30
done

if [[ "$STATUS" != "SUCCESS" ]]; then
  echo -e "${RED}⚠️  Timed out after $((MAX_ATTEMPTS * 30)) seconds. Check rds_task_status manually.${NC}"
  exit 1
fi

# ── Step 3: Create TargetDB and Reporting tables ───────────────────────────────
echo ""
echo "Creating TargetDB database and Reporting tables..."

sqlcmd -S "${ENDPOINT},${PORT}" -U "$LOGIN" -P "$PASSWORD" -b -Q "
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '$TARGET_DB')
    CREATE DATABASE [$TARGET_DB];
PRINT 'TargetDB ready.';"

sqlcmd -S "${ENDPOINT},${PORT}" -U "$LOGIN" -P "$PASSWORD" -d "$TARGET_DB" -b -Q "
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'OrderEnriched')
    CREATE TABLE dbo.OrderEnriched (
        OrderID                     INT,
        CustomerID                  INT,
        CustomerName                NVARCHAR(100),
        IsOnCreditHold              BIT,
        CreditLimit                 DECIMAL(18,2),
        StandardDiscountPercentage  DECIMAL(18,3),
        SalespersonPersonID         INT,
        SalespersonName             NVARCHAR(50),
        OrderDate                   DATE,
        ExpectedDeliveryDate        DATE,
        CustomerPurchaseOrderNumber NVARCHAR(20),
        IsUndersupplyBackordered    BIT,
        Comments                    NVARCHAR(MAX),
        PromisedLeadDays            INT,
        etl_loaded_at               DATETIME2 DEFAULT SYSDATETIME()
    );

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'OrderLineDetail')
    CREATE TABLE dbo.OrderLineDetail (
        OrderLineID          INT,
        OrderID              INT,
        StockItemID          INT,
        StockItemDescription NVARCHAR(100),
        Quantity             INT,
        UnitPrice            DECIMAL(18,2),
        TaxRate              DECIMAL(18,3),
        LineTotal            DECIMAL(18,2),
        TaxAmount            DECIMAL(18,2),
        LineTotalIncTax      DECIMAL(18,2),
        PickedQuantity       INT,
        OutstandingQty       INT,
        PickingStatus        NVARCHAR(20),
        PickingCompletedWhen DATETIME2,
        etl_loaded_at        DATETIME2 DEFAULT SYSDATETIME()
    );

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'CustomerMonthlySales')
    CREATE TABLE dbo.CustomerMonthlySales (
        CustomerID        INT,
        CustomerName      NVARCHAR(100),
        IsOnCreditHold    BIT,
        CreditLimit       DECIMAL(18,2),
        OrderMonth        DATE,
        OrderCount        INT,
        TotalOrderLines   INT,
        TotalUnits        INT,
        TotalRevenue      DECIMAL(18,2),
        TotalTax          DECIMAL(18,2),
        TotalRevIncTax    DECIMAL(18,2),
        AvgLineValue      DECIMAL(18,2),
        UniqueItemsBought INT,
        etl_loaded_at     DATETIME2 DEFAULT SYSDATETIME()
    );

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'InventoryValuation')
    CREATE TABLE dbo.InventoryValuation (
        StockItemID            INT,
        StockItemName          NVARCHAR(100),
        SupplierID             INT,
        SupplierName           NVARCHAR(100),
        CurrentSalePrice       DECIMAL(18,2),
        RecommendedRetailPrice DECIMAL(18,2),
        TaxRate                DECIMAL(18,3),
        LeadTimeDays           INT,
        IsChillerStock         BIT,
        Brand                  NVARCHAR(50),
        Size                   NVARCHAR(20),
        QuantityOnHand         INT,
        BinLocation            NVARCHAR(20),
        LastCostPrice          DECIMAL(18,2),
        LastStocktakeQuantity  INT,
        ReorderLevel           INT,
        TargetStockLevel       INT,
        StockCostValue         DECIMAL(18,2),
        StockRetailValue       DECIMAL(18,2),
        StockSaleValue         DECIMAL(18,2),
        StockGap               INT,
        StockStatus            NVARCHAR(20),
        etl_loaded_at          DATETIME2 DEFAULT SYSDATETIME()
    );

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'SlowMovingStock')
    CREATE TABLE dbo.SlowMovingStock (
        StockItemID       INT,
        StockItemName     NVARCHAR(100),
        SupplierID        INT,
        SupplierName      NVARCHAR(100),
        IsChillerStock    BIT,
        QuantityOnHand    INT,
        ReorderLevel      INT,
        LastCostPrice     DECIMAL(18,2),
        StockValue        DECIMAL(18,2),
        TxCount30Days     INT,
        TotalQtyOut30Days INT,
        AvgDailyMovement  DECIMAL(18,2),
        DaysOfStock       INT,
        MovementCategory  NVARCHAR(20),
        etl_loaded_at     DATETIME2 DEFAULT SYSDATETIME()
    );

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND name = 'SupplierPerformance')
    CREATE TABLE dbo.SupplierPerformance (
        SupplierID               INT,
        SupplierName             NVARCHAR(100),
        PhoneNumber              NVARCHAR(20),
        PaymentDays              INT,
        AvgLeadTimeDays          DECIMAL(18,1),
        UniqueItemsStocked       INT,
        TotalTransactions30Days  INT,
        TotalUnitsReceived30Days INT,
        TotalUnitsReturned30Days INT,
        ReturnRatePct            DECIMAL(18,2),
        etl_loaded_at            DATETIME2 DEFAULT SYSDATETIME()
    );

PRINT 'TargetDB tables ready.';
"

echo -e "${GREEN}✅  WideWorldImporters restore and schema setup complete.${NC}"
echo ""
echo "Next steps:"
echo "  1. Run the MWAA deploy script:  MWAA_BUCKET=\$(cd ../../infra/terraform && terraform output -raw mwaa_bucket) ./airflow/deploy_to_mwaa.sh"
echo "  2. Trigger wwi_sales_etl and wwi_inventory_etl DAGs from the MWAA console."
