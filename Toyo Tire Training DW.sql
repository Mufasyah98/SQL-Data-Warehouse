--Choose DataBase
USE [Training Data];
GO

SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA ='dbo'
ORDER BY TABLE_NAME;


-- STAGING--
IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC ('CREATE SCHEMA stg')

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg_c')
    EXEC('CREATE SCHEMA stg_c');

GO

-----RAW Staging---

IF OBJECT_ID('stg_Load_Batch_Audit') IS NULL
BEGIN
    CREATE TABLE stg_Load_Batch_Audit
    (
        Load_Batch_ID   UNIQUEIDENTIFIER NOT NULL,
        Source_System   VARCHAR(200) NOT NULL,     
        Source_File     VARCHAR(400) NULL,         
        Started_at      DATETIME2(3)  NOT NULL,
        Status_         VARCHAR(50)    NOT NULL,
        Error_message   VARCHAR(MAX)   NULL
    );
END
GO

-- Sales Staging Table --
IF OBJECT_ID('stg.sales_raw') IS NULL
CREATE TABLE stg.sales_raw(
  order_id     VARCHAR(100) NULL,
  order_date   VARCHAR(100) NULL,
  product_id   VARCHAR(50)  NULL,
  customer_id  VARCHAR(50)  NULL,
  store_id     VARCHAR(50)  NULL,
  salesrep_id  VARCHAR(50)  NULL,
  quantity     VARCHAR(50)  NULL,
  unit_price   VARCHAR(100) NULL,
  discount     VARCHAR(50)  NULL,
  load_batch_id UNIQUEIDENTIFIER NOT NULL,
  source_path   VARCHAR(1000)   NULL,
  load_ts       DATETIME2(3)     NOT NULL
);
GO


-- Customers Staging Table --

IF OBJECT_ID('stg.customers_raw') IS NULL
BEGIN
    CREATE TABLE stg.customers_raw (
        Customer_id    VARCHAR(50)   NULL,
        Full_name      VARCHAR(200)  NULL,
        Gender         VARCHAR(50)   NULL,
        Birth_date     DATE          NULL, 
        Join_date     DATE          NULL,
        Email          VARCHAR(200)  NULL,
        Phone          VARCHAR(100)  NULL,
        Customer_segment VARCHAR(100)  NULL,
        Loyalty_points  INT           NULL,
        Preferred_payment  VARCHAR(100)  NULL,
        Country        VARCHAR(100)  NULL,
        Address_line1  VARCHAR(200)  NULL,
        Source_path    VARCHAR(260)  NULL,
        Load_batch_id  UNIQUEIDENTIFIER NOT NULL,
        Load_ts        DATETIME2(3) NOT NULL
    );
END
GO

-- Products Staging Table --
-- 2B. Product RAW
IF OBJECT_ID('stg.Product_Raw') IS NULL
BEGIN
    CREATE TABLE stg.Product_Raw
    (
        ProductID          VARCHAR(50)    NULL,
        ProductName        VARCHAR(200)   NULL,
        PricePerUnit       VARCHAR(100)   NULL,
        CostPerUnit        VARCHAR(100)   NULL,
        CategoryID         VARCHAR(50)    NULL,
        Brand              VARCHAR(100)   NULL,
        SupplierName       VARCHAR(200)   NULL,

        Load_Batch_ID      UNIQUEIDENTIFIER NOT NULL,
        Source_File        VARCHAR(400)    NULL,
        Load_Timestamp     DATETIME2(3) 
    );
END
GO

-- Staging New Product Update --
IF OBJECT_ID('stg.NewProduct') IS NULL
BEGIN
    CREATE TABLE stg.NewProduct
    (
        ProductID          VARCHAR(50)    NULL,
        ProductName        VARCHAR(200)   NULL,
        PricePerUnit       VARCHAR(100)   NULL,
        CostPerUnit        VARCHAR(100)   NULL,
        CategoryID         VARCHAR(50)    NULL,
        Brand              VARCHAR(100)   NULL,
        SupplierName       VARCHAR(200)   NULL,
        Load_Batch_ID      UNIQUEIDENTIFIER NOT NULL,
        Source_File        VARCHAR(400)    NULL,
        Load_Timestamp     DATETIME2(3) 
    );
END
GO

-- 2E. Store RAW
IF OBJECT_ID('stg.Store_Raw') IS NULL
BEGIN
    CREATE TABLE stg.Store_Raw
    (
        StoreID            VARCHAR(50)    NULL,
        StoreName          VARCHAR(200)   NULL,
        Address            VARCHAR(300)   NULL,
        City               VARCHAR(100)   NULL,
        State              VARCHAR(100)   NULL,
        Country            VARCHAR(100)   NULL,

        Load_Batch_ID      UNIQUEIDENTIFIER NOT NULL,
        Source_File        VARCHAR(400)    NULL,
        Load_Timestamp     DATETIME2(3)
    );
END
GO

-- 2F. Marketing Spend RAW
IF OBJECT_ID('stg.MarketingSpend_Raw') IS NULL
BEGIN
    CREATE TABLE stg.MarketingSpend_Raw
    (
        SpendDate          VARCHAR(100)   NULL,
        Channel            VARCHAR(100)   NULL,
        Campaign           VARCHAR(200)   NULL,
        StoreID           VARCHAR(50)    NULL,
        Amount             VARCHAR(100)   NULL,
        Target_Audience     VARCHAR(100)   NULL,
        Revenue_generated   DECIMAL(18,2)    NULL,
        Load_Batch_ID      UNIQUEIDENTIFIER NOT NULL,
        Source_File        VARCHAR(400)    NULL,
        Load_Timestamp     DATETIME2(3)
    );
END
GO

-- 2G. Public Holidays RAW
IF OBJECT_ID('stg.PublicHoliday_Raw') IS NULL
BEGIN
    CREATE TABLE stg.PublicHoliday_Raw
    (
        HolidayDate        VARCHAR(100)   NULL,
        HolidayName        VARCHAR(200)   NULL,
        State              VARCHAR(100)   NULL,
        IsNational         VARCHAR(50)    NULL,

        Load_Batch_ID      UNIQUEIDENTIFIER NOT NULL,
        Source_File        VARCHAR(400)    NULL,
        Load_Timestamp     DATETIME2(3)
    );
END
GO

-- 2H. Discontinued Product RAW
IF OBJECT_ID('stg.DiscontinuedProduct_Raw') IS NULL
BEGIN
    CREATE TABLE stg.DiscontinuedProduct_Raw
    (
        ProductID          VARCHAR(50)    NULL,
        DiscontinuedDate   VARCHAR(100)   NULL,
        Reason             VARCHAR(200)   NULL,

        Load_Batch_ID      UNIQUEIDENTIFIER NOT NULL,
        Source_File        VARCHAR(400)    NULL,
        Load_Timestamp     DATETIME2(3)
    );
END
GO

-- Sales reps RAW
IF OBJECT_ID('stg.SalesReps_Raw') IS NULL
BEGIN
    CREATE TABLE stg.SalesReps_Raw
    (
        SalesRepID        VARCHAR(50)    NULL,
        SalesRepName      VARCHAR(200)   NULL,
        Gender            VARCHAR(50)    NULL,
        DateBirth        DATE           NULL,
        HireDate         DATE           NULL,
        Position         VARCHAR(100)   NULL,
        Email             VARCHAR(200)   NULL,
        Phone             VARCHAR(100)   NULL,
        SalaryMonthly     DECIMAL(18,2)  NULL,
        Load_Batch_ID      UNIQUEIDENTIFIER NOT NULL,
        Source_File        VARCHAR(400)    NULL,
        Load_Timestamp     DATETIME2(3)
    );
END
-- 2I. New Product Update RAW
IF OBJECT_ID('stg.NewProductUpdate_Raw') IS NULL
BEGIN
    CREATE TABLE stg.NewProductUpdate_Raw
    (
        ProductID          VARCHAR(50)    NULL,
        ProductName        VARCHAR(200)   NULL,
        PricePerUnit       VARCHAR(100)   NULL,
        CostPerUnit        VARCHAR(100)   NULL,
        CategoryID         VARCHAR(50)    NULL,
        Brand              VARCHAR(100)   NULL,
        SupplierName       VARCHAR(200)   NULL,

        Load_Batch_ID      UNIQUEIDENTIFIER NOT NULL,
        Source_File        VARCHAR(400)    NULL,
        Load_Timestamp     DATETIME2(3)
    );
END
GO

 -- =============================================
   --3) STAGING (CLEANSED MINIMUM) tables
-- ============================================= */

-- 3A. Sales (cleansed)
IF OBJECT_ID('stg_c.Sales') IS NULL
BEGIN
    CREATE TABLE stg_c.Sales
    (
        OrderID        BIGINT          NOT NULL,
        OrderDate      DATE            NOT NULL,
        ProductID      VARCHAR(50)    NOT NULL,
        CustomerID     VARCHAR(50)    NOT NULL,
        StoreID        VARCHAR(50)    NULL,
        SalesRepID     VARCHAR(50)    NULL,
        Quantity       INT             NOT NULL,
        UnitPrice      DECIMAL(18,2)   NOT NULL,
        Discount       DECIMAL(9,4)    NULL,
        Load_Batch_ID  UNIQUEIDENTIFIER NOT NULL,
        Load_Timestamp DATETIME2(3)     NOT NULL
    );
END
GO

-- 3B. Product (cleansed)
IF OBJECT_ID('stg_c.Product') IS NULL
BEGIN
    CREATE TABLE stg_c.Product
    (
        ProductID      VARCHAR(50)   NOT NULL,
        ProductName    VARCHAR(200)  NOT NULL,
        PricePerUnit   DECIMAL(18,2)  NULL,
        CostPerUnit    DECIMAL(18,2)  NULL,
        CategoryID     VARCHAR(50)   NULL,
        Brand          VARCHAR(100)  NULL,
        SupplierName   VARCHAR(200)  NULL,
        Load_Batch_ID  UNIQUEIDENTIFIER NOT NULL,
        Load_Timestamp DATETIME2(3)     NOT NULL
    );
END
GO

-- 3C. Customer (cleansed)
IF OBJECT_ID('stg_c.Customer') IS NULL
BEGIN
    CREATE TABLE stg_c.Customer
    (
        CustomerID     VARCHAR(50)    NOT NULL,
        CustomerName   VARCHAR(200)   NOT NULL,
        Email          VARCHAR(200)   NULL,
        Phone          VARCHAR(100)   NULL,
        City           VARCHAR(100)   NULL,
        State          VARCHAR(100)   NULL,
        Country        VARCHAR(100)   NULL,
        Load_Batch_ID  UNIQUEIDENTIFIER NOT NULL,
        Load_Timestamp DATETIME2(3)     NOT NULL
    );
END
GO

-- PULL FROM DATA SOURCE to RAW STAGING ---

-- Sales Raw to Sales Stagging
INSERT INTO stg.sales_raw(
    order_id,
    order_date,
    product_id,
    customer_id,
    store_id,
    salesrep_id,
    quantity,
    load_batch_id,
    source_path,
    load_ts
)
SELECT
    CAST([OrderID]   AS VARCHAR(100))  AS order_id,
    CAST([Order Date]  AS VARCHAR(100))  AS order_date,
    CAST([Product ID]  AS VARCHAR(50))   AS product_id,
    CAST([Customer ID] AS VARCHAR(50))   AS customer_id,
    CAST([Store ID]    AS VARCHAR(50))   AS store_id,
    CAST([Sales Person ID] AS VARCHAR(50))   AS salesrep_id,
    CAST([Quantity Sold]   AS VARCHAR(50))   AS quantity,
    NEWID()    AS load_batch_id,        
    'Fabric:dbo.sales_data_2023'       AS source_path,
    SYSUTCDATETIME()                   AS load_ts             
FROM dbo.sales_data_2023;
SELECT * FROM dbo.Sales_Reps;
-- Sales Reps Raw to Sales Reps Stagging
INSERT INTO stg.SalesReps_Raw (
    SalesRepID,
    SalesRepName,
    Gender,
    DateBirth,
    HireDate,
    Position,
    Email,
    Phone,
    SalaryMonthly,
    Load_Batch_ID,
    Source_File,
    Load_Timestamp
)
SELECT
    CAST([Sales Person ID] AS VARCHAR(50)) AS SalesRepID,
    CAST([Full Name] AS VARCHAR(200)) AS SalesRepName,
    Gender,
    TRY_CAST([Date of Birth] AS DATE) AS DateBirth,
    TRY_CAST([Hire Date] AS DATE) AS HireDate, 
    Position,
    Email,
    [Phone Number] AS Phone,
    [Monthly Salary _USD_] AS SalaryMonthly,
    NEWID() AS Load_Batch_ID,
    'Fabric:dbo.Sales_Reps' AS Source_File,
    SYSUTCDATETIME() AS Load_Timestamp
FROM dbo.Sales_Reps;


-- Customers Raw to Customers Stagging
INSERT INTO stg.customers_raw (
    Customer_id,
    Full_name,
    Gender,
    Birth_date,
    Join_date,
    Email,
    Phone,
    Customer_segment,
    Loyalty_points,
    Preferred_payment,
    Address_line1,
    Source_path,
    Load_batch_id,
    Load_ts
)
SELECT
    CAST([Customer ID] AS VARCHAR(50)) AS Customer_id,
    CAST([Full Name] AS VARCHAR(200)) AS Full_name,
    Gender,
    TRY_CAST([Date of Birth] AS DATE) AS Birth_date,
    TRY_CAST([Join Date] AS DATE) AS Join_date,
    Email,
    [Phone Number] AS Phone,
    [Customer Segment] AS Customer_segment,
    [Loyalty Points] AS Loyalty_points, 
    [Preferred Payment Method] AS Preferred_payment,
    [Address] AS Address_line1,
    'Fabric:dbo.[Customer Data]' AS source_path,
    NEWID() AS load_batch_id,
    SYSUTCDATETIME() AS Load_ts
FROM dbo.[Customer Data];


-- Products Raw to Products Stagging

INSERT INTO stg.Product_Raw (
    ProductID,
    ProductName,
    PricePerUnit,
    CostPerUnit,
    CategoryID,
    Brand,
    SupplierName,
    Load_Batch_ID,
    Source_File,
    Load_Timestamp
)  
SELECT
    CAST([Product ID] AS VARCHAR(50)) AS ProductID,
    CAST([Product Name] AS VARCHAR(200)) AS ProductName,
    CAST([Price Per Unit] AS VARCHAR(100)) AS PricePerUnit,
    CAST([Cost Per Unit] AS VARCHAR(100)) AS CostPerUnit,
    CAST([Category ID] AS VARCHAR(50)) AS CategoryID,
    CAST([Brand] AS VARCHAR(100)) AS Brand,
    CAST([Supplier Name] AS VARCHAR(200)) AS SupplierName,
    NEWID() AS Load_Batch_ID,
    'Fabric:dbo.[Product]' AS Source_File,
    SYSUTCDATETIME() AS Load_Timestamp  
FROM dbo.[Product];


-- Marketing Spend Raw to Marketing Spend Stagging

INSERT INTO stg.MarketingSpend_Raw (
    SpendDate,
    Channel,
    Campaign,
    StoreID,
    Amount,
    Target_Audience,
    Revenue_generated,
    Load_Batch_ID,
    Source_File,
    Load_Timestamp 
)
SELECT
    CAST([Date] AS VARCHAR(100)) AS SpendDate,
    CAST([Channel] AS VARCHAR(100)) AS Channel,
    CAST([Campaign Name] AS VARCHAR(200)) AS Campaign,
    [Store ID] AS StoreID,
    CAST([Spend Amount _RM_] AS DECIMAL(18,2)) AS Amount,
    [Target Audience] AS Target_Audience,
    [Revenue Generated _RM_] AS Revenue_generated,
    NEWID() AS Load_Batch_ID,
    'Fabric:dbo.[marketing_spend_sample]' AS Source_File,
    SYSUTCDATETIME() AS Load_Timestamp
FROM dbo.[marketing_spend_sample];


-- New Products Raw to New Products Stagging

INSERT INTO stg.Product_Raw (
    ProductID,
    ProductName,
    PricePerUnit,
    CostPerUnit,
    CategoryID,
    Brand,
    SupplierName,
    Load_Batch_ID,
    Source_File,
    Load_Timestamp
)  
SELECT
    CAST([Product ID] AS VARCHAR(50)) AS ProductID,
    CAST([Product Name] AS VARCHAR(200)) AS ProductName,
    CAST([Price Per Unit] AS VARCHAR(100)) AS PricePerUnit,
    CAST([Cost Per Unit] AS VARCHAR(100)) AS CostPerUnit,
    CAST([Category ID] AS VARCHAR(50)) AS CategoryID,
    CAST([Brand] AS VARCHAR(100)) AS Brand,
    CAST([Supplier Name] AS VARCHAR(200)) AS SupplierName,
    NEWID() AS Load_Batch_ID,
    'Fabric:dbo.[New Product Update]' AS Source_File,
    SYSUTCDATETIME() AS Load_Timestamp  
FROM dbo.[New Product Update];


-- Remove Discontinued Products from stg.Product_Raw
DELETE P
FROM stg.Product_Raw AS P
JOIN dbo.Discontinued AS D
ON D.[Product ID] = P.ProductID;

-- Add Revenue Column to stg.sales_raw

ALTER TABLE stg.sales_raw
ADD Revenue DECIMAL(18,2);

UPDATE s
SET s.Revenue = 
CAST(s.quantity AS DECIMAL(18,2)) * 
CAST(p.PricePerUnit AS DECIMAL(18,2))
FROM stg.sales_raw AS s
INNER JOIN stg.Product_Raw AS p
    ON s.product_id = p.ProductID;


-- Add Cost Column in stg.sales_raw

ALTER TABLE stg.sales_raw
ADD Cost DECIMAL(18,2);

UPDATE s
SET s.Cost = 
CAST(s.quantity AS DECIMAL(18,2)) * 
CAST(p.CostPerUnit AS DECIMAL(18,2))
FROM stg.sales_raw AS s
INNER JOIN stg.Product_Raw AS p
    ON s.product_id = p.ProductID;

-- Add Profit Column in stg.sales_raw
ALTER TABLE stg.sales_raw
ADD Profit DECIMAL(18,2);

UPDATE s
SET s.Profit = s.Revenue - s.Cost  
FROM stg.sales_raw AS s;


-- Add Category name Column in product Staging
ALTER TABLE stg.Product_Raw
ADD CategoryName VARCHAR(100);

UPDATE p
SET p.CategoryName = c.[Category Name]
FROM stg.Product_Raw AS p
INNER JOIN dbo.Categories AS c
    ON p.CategoryID = c.[Category ID];

-- Add Column Age in Customer Staging --
ALTER TABLE stg.customers_raw
ADD Age INT;

UPDATE c
SET c.Age = DATEDIFF(YEAR, c.Birth_date, GETDATE()) - 
    CASE 
        WHEN MONTH(c.Birth_date) > MONTH(GETDATE()) 
             OR (MONTH(c.Birth_date) = MONTH(GETDATE()) AND DAY(c.Birth_date) > DAY(GETDATE())) 
        THEN 1 
        ELSE 0 
    END
FROM stg.customers_raw AS c;

-- Add Column Age Group in Customer Staging --
ALTER TABLE stg.customers_raw  
ADD AgeGroup VARCHAR(50);

UPDATE c
SET c.AgeGroup =
    CASE 
        WHEN c.Age < 18 THEN 'Under 18'
        WHEN c.Age BETWEEN 18 AND 24 THEN '18-24'
        WHEN c.Age BETWEEN 25 AND 34 THEN '25-34'
        WHEN c.Age BETWEEN 35 AND 44 THEN '35-44'
        WHEN c.Age BETWEEN 45 AND 54 THEN '45-54'
        WHEN c.Age BETWEEN 55 AND 64 THEN '55-64'
        WHEN c.Age >= 65 THEN '65 and over'
        ELSE 'Unknown'
    END
FROM stg.customers_raw AS c;

SELECT * FROM dbo.Sales_Reps;


-- Add Column Year of Service in Sales Reps Staging --
ALTER TABLE stg.SalesReps_Raw
ADD YearsOfService INT;

UPDATE s
SET s.YearsOfService = DATEDIFF(YEAR, s.HireDate, GETDATE()) - 
    CASE 
        WHEN MONTH(s.HireDate) > MONTH(GETDATE()) 
             OR (MONTH(s.HireDate) = MONTH(GETDATE()) AND DAY(s.HireDate) > DAY(GETDATE())) 
        THEN 1 
        ELSE 0 
    END
FROM stg.SalesReps_Raw AS s;

SELECT * FROM stg.Store_Raw;