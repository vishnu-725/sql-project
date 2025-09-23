USE DataWarehouse;
GO

-- ============================
-- Silver: crm_cust_info (customers)
-- ============================
IF OBJECT_ID('Silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE Silver.crm_cust_info;
GO

CREATE TABLE Silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(100),
    cst_lastname NVARCHAR(100),
     cst_marital_status NVARCHAR(50), -- normalized name (bronze had cst_martial_status)
    cst_gndr NVARCHAR(20),
    cst_create_date DATE ,
    dwh_createcst_marital_status_date    DATETIME2 DEFAULT GETDATE()
);
GO

;WITH CustClean AS (
    SELECT
        cst_id,
        cst_key,
        LTRIM(RTRIM(cst_firstname)) AS cst_firstname,
        LTRIM(RTRIM(cst_lastname))  AS cst_lastname,
        LTRIM(RTRIM(cst_martial_status)) AS cst_marital_status,
        CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,

        cst_create_date,
        ROW_NUMBER() OVER (
            PARTITION BY cst_key
            ORDER BY cst_create_date DESC, cst_id DESC
        ) AS rn
    FROM Bronze.crm_cust_info
    WHERE cst_key IS NOT NULL -- drop rows missing the natural key
)
INSERT INTO Silver.crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
)
SELECT
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
FROM CustClean
WHERE rn = 1; -- keep latest row per cst_key
GO

-- ============================
-- Silver: crm_prd_info (products)
-- ============================
IF OBJECT_ID('Silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE Silver.crm_prd_info;
GO

CREATE TABLE Silver.crm_prd_info (
    prd_id INT,
    cat_id nvarchar(50) ,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(200),
    prd_cost INT,
    prd_line NVARCHAR(100),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME ,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
;WITH ProdClean AS (
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, 
        LTRIM(RTRIM(prd_nm))  AS prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
        END AS prd_line,
        TRY_CONVERT(datetime, prd_start_dt) AS prd_start_dt,
        TRY_CONVERT(datetime, prd_end_dt)   AS prd_end_dt,
        ROW_NUMBER() OVER (
            PARTITION BY LTRIM(RTRIM(prd_key))
            ORDER BY TRY_CONVERT(datetime, prd_start_dt) DESC, prd_id DESC
        ) AS rn
    FROM Bronze.crm_prd_info
    WHERE prd_key IS NOT NULL
)
INSERT INTO Silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
)
SELECT prd_id,cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
FROM ProdClean
WHERE rn = 1;
GO


-- ============================
-- Silver: crm_sales_details (sales)
-- ============================
IF OBJECT_ID('Silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE Silver.crm_sales_details;
GO

CREATE TABLE Silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales DECIMAL(18,2),
    sls_quantity INT,
    sls_price DECIMAL(18,2) ,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- convert integer YYYYMMDD style dates to proper date (style 112)
INSERT INTO Silver.crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt,
    sls_sales, sls_quantity, sls_price
)
SELECT DISTINCT
    LTRIM(RTRIM(sls_ord_num)) AS sls_ord_num,
    LTRIM(RTRIM(sls_prd_key)) AS sls_prd_key,
    sls_cust_id,
    CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
    CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
    CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
    CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
            END AS sls_sales,
    sls_quantity,
   CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
FROM Bronze.crm_sales_details
WHERE sls_ord_num IS NOT NULL
  AND sls_prd_key IS NOT NULL
  AND sls_cust_id IS NOT NULL;
GO


-- ============================
-- Silver: erp_loc_a101  (location)  -- mapping Bronze.erp_loc101 -> Silver.erp_loc_a101
-- ============================
IF OBJECT_ID('Silver.erp_loc_a101','U') IS NOT NULL
    DROP TABLE Silver.erp_loc_a101;
GO

CREATE TABLE Silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(100) ,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

INSERT INTO Silver.erp_loc_a101 (cid, cntry)
SELECT DISTINCT
    REPLACE(cid, '-', '') AS cid,
    CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry
FROM Bronze.erp_loc101
WHERE cid IS NOT NULL;
GO

-- ============================
-- Silver: erp_cust_az12 (ERP customer supplemental data)
-- ============================
IF OBJECT_ID('Silver.erp_cust_az12','U') IS NOT NULL
    DROP TABLE Silver.erp_cust_az12;
GO

CREATE TABLE Silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(20) ,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

INSERT INTO Silver.erp_cust_az12 (cid, bdate, gen)
SELECT DISTINCT
    CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid,
    CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
    CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen
FROM Bronze.erp_cust_az12
WHERE cid IS NOT NULL;
GO


-- ============================
-- Silver: erp_px_cat_g1v2 (product categories)
-- ============================
IF OBJECT_ID('Silver.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE Silver.erp_px_cat_g1v2;
GO

CREATE TABLE Silver.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(200),
    subcat NVARCHAR(200),
    maintenance NVARCHAR(200) ,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

INSERT INTO Silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT DISTINCT
    LTRIM(RTRIM(id))         AS id,
    LTRIM(RTRIM(cat))        AS cat,
    LTRIM(RTRIM(subcat))     AS subcat,
    LTRIM(RTRIM(maintenance)) AS maintenance
FROM Bronze.erp_px_cat_g1v2
WHERE id IS NOT NULL;
GO

-- ============================
-- Quick sanity checks
-- ============================
PRINT 'Counts (Bronze -> Silver):';
SELECT 'bronze.crm_cust_info' AS source, COUNT(*) AS rows FROM Bronze.crm_cust_info;
SELECT 'silver.crm_cust_info' AS target, COUNT(*) AS rows FROM Silver.crm_cust_info;

SELECT 'bronze.crm_prd_info' AS source, COUNT(*) AS rows FROM Bronze.crm_prd_info;
SELECT 'silver.crm_prd_info' AS target, COUNT(*) AS rows FROM Silver.crm_prd_info;

SELECT 'bronze.crm_sales_details' AS source, COUNT(*) AS source_rows FROM Bronze.crm_sales_details;
SELECT 'silver.crm_sales_details' AS target, COUNT(*) AS target_rows FROM Silver.crm_sales_details;
GO

-- sample data checks
SELECT TOP 10 * FROM Silver.crm_cust_info;
SELECT TOP 10 * FROM Silver.crm_prd_info;
SELECT TOP 10 * FROM Silver.crm_sales_details;
GO
