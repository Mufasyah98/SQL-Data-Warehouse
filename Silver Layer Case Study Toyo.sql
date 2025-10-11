IF OBJECT_ID('stg.load_silver_layer', 'P') IS NOT NULL
    DROP PROCEDURE stg.load_silver_layer;
GO

CREATE PROCEDURE stg.load_silver_layer

AS 
BEGIN

PRINT 'SILVER LAYER'

PRINT '============================================='

PRINT '-----------------------------------------------'

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


PRINT 'Updated stg.Product_Raw'
PRINT '                                                                                     '
-- Remove Discontinued Products from stg.Product_Raw
DELETE P
FROM stg.Product_Raw AS P
JOIN dbo.Discontinued AS D
ON D.[Product ID] = P.ProductID;

-- Add Revenue Column to stg.sales_raw
IF NOT EXISTS (
    SELECT 1 FROM sys.columns 
    WHERE Name = N'Revenue' AND Object_ID = Object_ID(N'stg.sales_raw')
)
BEGIN
    ALTER TABLE stg.sales_raw ADD Revenue DECIMAL(18,2);

ALTER TABLE stg.sales_raw
ADD Revenue DECIMAL(18,2);

END

UPDATE s
SET s.Revenue = 
CAST(s.quantity AS DECIMAL(18,2)) * 
CAST(p.PricePerUnit AS DECIMAL(18,2))
FROM stg.sales_raw AS s
INNER JOIN stg.Product_Raw AS p
    ON s.product_id = p.ProductID;


-- Add Cost Column in stg.sales_raw

IF NOT EXISTS (
    SELECT 1 FROM sys.columns 
    WHERE Name = N'Revenue' AND Object_ID = Object_ID(N'stg.sales_raw')
)
BEGIN
ALTER TABLE stg.sales_raw
ADD Cost DECIMAL(18,2);
END

UPDATE s
SET s.Cost = 
CAST(s.quantity AS DECIMAL(18,2)) * 
CAST(p.CostPerUnit AS DECIMAL(18,2))
FROM stg.sales_raw AS s
INNER JOIN stg.Product_Raw AS p
    ON s.product_id = p.ProductID;

-- Add Profit Column in stg.sales_raw
IF NOT EXISTS (
    SELECT 1 FROM sys.columns 
    WHERE Name = N'Revenue' AND Object_ID = Object_ID(N'stg.sales_raw')
)
BEGIN

ALTER TABLE stg.sales_raw
ADD Profit DECIMAL(18,2);
END

UPDATE s
SET s.Profit = s.Revenue - s.Cost  
FROM stg.sales_raw AS s;

-- Add Category name Column in product Staging
IF NOT EXISTS (
    SELECT 1 FROM sys.columns 
    WHERE Name = N'Revenue' AND Object_ID = Object_ID(N'stg.sales_raw')
)
BEGIN

ALTER TABLE stg.Product_Raw
ADD CategoryName VARCHAR(100);
END

UPDATE p
SET p.CategoryName = c.[Category Name]
FROM stg.Product_Raw AS p
INNER JOIN dbo.Categories AS c
    ON p.CategoryID = c.[Category ID];

PRINT 'Updated stg.customers_raw'
PRINT '                                                                                     '
-- Add Column Age in Customer Staging --
IF NOT EXISTS (
    SELECT 1 FROM sys.columns 
    WHERE Name = N'Revenue' AND Object_ID = Object_ID(N'stg.sales_raw')
)
BEGIN
ALTER TABLE stg.customers_raw
ADD Age INT;
END

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
IF NOT EXISTS (
    SELECT 1 FROM sys.columns 
    WHERE Name = N'Revenue' AND Object_ID = Object_ID(N'stg.sales_raw')
)
BEGIN

ALTER TABLE stg.customers_raw  
ADD AgeGroup VARCHAR(50);
END

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

PRINT 'Updated stg.SalesReps_Raw'
PRINT '                                                                                     '
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

END