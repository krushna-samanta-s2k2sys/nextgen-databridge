-- =============================================================================
-- setup_targetdb.sql
-- Creates TargetDB and all ETL output tables for NextGenDatabridge pipelines.
-- Idempotent — safe to run multiple times.
-- =============================================================================

-- ── Create database ──────────────────────────────────────────────���───────────
IF DB_ID('TargetDB') IS NULL
BEGIN
    CREATE DATABASE TargetDB;
    PRINT 'Created database TargetDB';
END
ELSE
    PRINT 'TargetDB already exists — skipping creation';
GO

USE TargetDB;
GO

-- =============================================================================
-- wwi_inventory_etl targets
-- =============================================================================

-- ── InventoryValuation ───────────────────────────────────────────────────────
IF OBJECT_ID('dbo.InventoryValuation', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.InventoryValuation (
        StockItemID             INT             NOT NULL,
        StockItemName           NVARCHAR(100)   NOT NULL,
        SupplierID              INT             NOT NULL,
        SupplierName            NVARCHAR(100)   NULL,
        CurrentSalePrice        DECIMAL(18,2)   NOT NULL,
        RecommendedRetailPrice  DECIMAL(18,2)   NULL,
        TaxRate                 DECIMAL(18,3)   NOT NULL,
        LeadTimeDays            INT             NULL,
        IsChillerStock          BIT             NOT NULL,
        Brand                   NVARCHAR(100)   NULL,
        Size                    NVARCHAR(20)    NULL,
        QuantityOnHand          INT             NOT NULL,
        BinLocation             NVARCHAR(20)    NULL,
        LastCostPrice           DECIMAL(18,2)   NULL,
        LastStocktakeQuantity   INT             NULL,
        ReorderLevel            INT             NULL,
        TargetStockLevel        INT             NULL,
        StockCostValue          DECIMAL(18,2)   NULL,
        StockRetailValue        DECIMAL(18,2)   NULL,
        StockSaleValue          DECIMAL(18,2)   NULL,
        StockGap                INT             NULL,
        StockStatus             NVARCHAR(20)    NOT NULL,
        etl_loaded_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_InventoryValuation PRIMARY KEY (StockItemID)
    );
    CREATE INDEX IX_InventoryValuation_SupplierID ON dbo.InventoryValuation (SupplierID);
    CREATE INDEX IX_InventoryValuation_StockStatus ON dbo.InventoryValuation (StockStatus);
    PRINT 'Created table dbo.InventoryValuation';
END
ELSE
    PRINT 'dbo.InventoryValuation already exists — skipping';
GO

-- ── SlowMovingStock ──────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.SlowMovingStock', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SlowMovingStock (
        StockItemID         INT             NOT NULL,
        StockItemName       NVARCHAR(100)   NOT NULL,
        SupplierID          INT             NOT NULL,
        SupplierName        NVARCHAR(100)   NULL,
        IsChillerStock      BIT             NOT NULL,
        QuantityOnHand      INT             NOT NULL,
        ReorderLevel        INT             NULL,
        LastCostPrice       DECIMAL(18,2)   NULL,
        StockValue          DECIMAL(18,2)   NULL,
        TxCount30Days       INT             NOT NULL DEFAULT 0,
        TotalQtyOut30Days   INT             NOT NULL DEFAULT 0,
        AvgDailyMovement    DECIMAL(18,2)   NOT NULL DEFAULT 0,
        DaysOfStock         DECIMAL(10,0)   NULL,
        MovementCategory    NVARCHAR(10)    NOT NULL,
        etl_loaded_at       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_SlowMovingStock PRIMARY KEY (StockItemID)
    );
    CREATE INDEX IX_SlowMovingStock_MovementCategory ON dbo.SlowMovingStock (MovementCategory);
    CREATE INDEX IX_SlowMovingStock_SupplierID       ON dbo.SlowMovingStock (SupplierID);
    PRINT 'Created table dbo.SlowMovingStock';
END
ELSE
    PRINT 'dbo.SlowMovingStock already exists — skipping';
GO

-- ── SupplierPerformance ──────────────────────────────────────────────────────
IF OBJECT_ID('dbo.SupplierPerformance', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SupplierPerformance (
        SupplierID                  INT             NOT NULL,
        SupplierName                NVARCHAR(100)   NOT NULL,
        PhoneNumber                 NVARCHAR(20)    NULL,
        PaymentDays                 INT             NULL,
        AvgLeadTimeDays             DECIMAL(18,1)   NULL,
        UniqueItemsStocked          INT             NOT NULL DEFAULT 0,
        TotalTransactions30Days     INT             NOT NULL DEFAULT 0,
        TotalUnitsReceived30Days    INT             NOT NULL DEFAULT 0,
        TotalUnitsReturned30Days    INT             NOT NULL DEFAULT 0,
        ReturnRatePct               DECIMAL(18,2)   NOT NULL DEFAULT 0,
        etl_loaded_at               DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_SupplierPerformance PRIMARY KEY (SupplierID)
    );
    PRINT 'Created table dbo.SupplierPerformance';
END
ELSE
    PRINT 'dbo.SupplierPerformance already exists — skipping';
GO

-- =============================================================================
-- wwi_sales_etl targets
-- =============================================================================

-- ── OrderEnriched ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.OrderEnriched', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.OrderEnriched (
        OrderID                     INT             NOT NULL,
        CustomerID                  INT             NOT NULL,
        CustomerName                NVARCHAR(100)   NULL,
        IsOnCreditHold              BIT             NOT NULL DEFAULT 0,
        CreditLimit                 DECIMAL(18,2)   NULL,
        StandardDiscountPercentage  DECIMAL(18,3)   NOT NULL DEFAULT 0,
        SalespersonPersonID         INT             NULL,
        SalespersonName             NVARCHAR(50)    NULL,
        OrderDate                   DATE            NOT NULL,
        ExpectedDeliveryDate        DATE            NULL,
        CustomerPurchaseOrderNumber NVARCHAR(20)    NULL,
        IsUndersupplyBackordered    BIT             NOT NULL DEFAULT 0,
        Comments                    NVARCHAR(MAX)   NULL,
        PromisedLeadDays            INT             NULL,
        etl_loaded_at               DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_OrderEnriched PRIMARY KEY (OrderID)
    );
    CREATE INDEX IX_OrderEnriched_CustomerID        ON dbo.OrderEnriched (CustomerID);
    CREATE INDEX IX_OrderEnriched_SalespersonID     ON dbo.OrderEnriched (SalespersonPersonID);
    CREATE INDEX IX_OrderEnriched_OrderDate         ON dbo.OrderEnriched (OrderDate);
    PRINT 'Created table dbo.OrderEnriched';
END
ELSE
    PRINT 'dbo.OrderEnriched already exists — skipping';
GO

-- ── OrderLineDetail ──────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.OrderLineDetail', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.OrderLineDetail (
        OrderLineID             INT             NOT NULL,
        OrderID                 INT             NOT NULL,
        StockItemID             INT             NOT NULL,
        StockItemDescription    NVARCHAR(100)   NOT NULL,
        Quantity                INT             NOT NULL,
        UnitPrice               DECIMAL(18,2)   NOT NULL,
        TaxRate                 DECIMAL(18,3)   NOT NULL,
        LineTotal               DECIMAL(18,2)   NOT NULL,
        TaxAmount               DECIMAL(18,2)   NOT NULL,
        LineTotalIncTax         DECIMAL(18,2)   NOT NULL,
        PickedQuantity          INT             NOT NULL DEFAULT 0,
        OutstandingQty          INT             NOT NULL DEFAULT 0,
        PickingStatus           NVARCHAR(20)    NOT NULL,
        PickingCompletedWhen    DATETIME2       NULL,
        etl_loaded_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_OrderLineDetail PRIMARY KEY (OrderLineID)
    );
    CREATE INDEX IX_OrderLineDetail_OrderID     ON dbo.OrderLineDetail (OrderID);
    CREATE INDEX IX_OrderLineDetail_StockItemID ON dbo.OrderLineDetail (StockItemID);
    PRINT 'Created table dbo.OrderLineDetail';
END
ELSE
    PRINT 'dbo.OrderLineDetail already exists — skipping';
GO

-- ── CustomerMonthlySales ─────────────────────────────────────────────────────
IF OBJECT_ID('dbo.CustomerMonthlySales', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.CustomerMonthlySales (
        CustomerID          INT             NOT NULL,
        CustomerName        NVARCHAR(100)   NOT NULL,
        IsOnCreditHold      BIT             NOT NULL DEFAULT 0,
        CreditLimit         DECIMAL(18,2)   NULL,
        OrderMonth          DATE            NOT NULL,
        OrderCount          INT             NOT NULL DEFAULT 0,
        TotalOrderLines     INT             NOT NULL DEFAULT 0,
        TotalUnits          INT             NOT NULL DEFAULT 0,
        TotalRevenue        DECIMAL(18,2)   NOT NULL DEFAULT 0,
        TotalTax            DECIMAL(18,2)   NOT NULL DEFAULT 0,
        TotalRevIncTax      DECIMAL(18,2)   NOT NULL DEFAULT 0,
        AvgLineValue        DECIMAL(18,2)   NULL,
        UniqueItemsBought   INT             NOT NULL DEFAULT 0,
        etl_loaded_at       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_CustomerMonthlySales PRIMARY KEY (CustomerID, OrderMonth)
    );
    CREATE INDEX IX_CustomerMonthlySales_OrderMonth ON dbo.CustomerMonthlySales (OrderMonth);
    PRINT 'Created table dbo.CustomerMonthlySales';
END
ELSE
    PRINT 'dbo.CustomerMonthlySales already exists — skipping';
GO

PRINT 'TargetDB setup complete.';
