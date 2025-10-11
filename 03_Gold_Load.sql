-- File: 03_Gold_Load.sql
-- Purpose: Create stored procedure for Gold layer load into Fact_Sales

USE Training_Power_BI_2025;
GO

IF OBJECT_ID('dbo.Load_Sales_Gold', 'P') IS NOT NULL
    DROP PROCEDURE dbo.Load_Sales_Gold;
GO

CREATE PROCEDURE dbo.Load_Sales_Gold
AS
BEGIN
    SET NOCOUNT ON;

    PRINT 'Truncating dbo.Fact_Sales...';
    TRUNCATE TABLE dbo.Fact_Sales;

    PRINT 'Loading Fact_Sales from stg.sales_clean joined to dimensions...';

    INSERT INTO dbo.Fact_Sales
    (
        CustomerID,
        ProductID,
        StoreID,
        SalesRepID,
        OrderDate,
        Revenue,
        Quantity
    )
    SELECT
        c.CustomerID,
        p.ProductID,
        s.StoreID,
        sr.SalesRepID,
        sc.OrderDate,
        sc.Revenue,
        sc.Quantity
    FROM stg.sales_clean AS sc
    INNER JOIN dbo.Dim_Product  AS p  ON sc.ProductID  = p.ProductID
    INNER JOIN dbo.Dim_Customer AS c  ON sc.CustomerID = c.CustomerID
    INNER JOIN dbo.Dim_Store    AS s  ON sc.StoreID    = s.StoreID
    INNER JOIN dbo.Dim_SalesRep AS sr ON sc.SalesRepID = sr.SalesRepID;

    PRINT 'Gold load completed successfully!';
END;
GO
