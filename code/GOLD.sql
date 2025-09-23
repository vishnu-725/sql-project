
USE DataWarehouse;
GO

/* ==================================================
   GOLD LAYER: DIMENSIONS + FACT
   ================================================== */

/* =======================
   DIM CUSTOMER
   ======================= */
IF OBJECT_ID('Gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW Gold.dim_customer;
GO

CREATE VIEW Gold.dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- surrogate key
    ci.cst_id           AS source_customer_id,              -- natural key
    ci.cst_key          AS customer_number,
    ci.cst_firstname    AS first_name,
    ci.cst_lastname     AS last_name,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END                 AS gender,
    ca.bdate            AS birthdate,
    la.cntry            AS country,
    ci.cst_create_date  AS create_date
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_a101 la ON ci.cst_key = la.cid;
GO

/* =======================
   DIM PRODUCT
   ======================= */
IF OBJECT_ID('Gold.dim_product', 'V') IS NOT NULL
    DROP VIEW Gold.dim_product;
GO

CREATE VIEW Gold.dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.prd_id) AS product_key, -- surrogate key
    p.prd_id          AS source_product_id,              -- natural key
    p.prd_key,
    p.cat_id,
    pc.cat,
    pc.subcat,
    pc.maintenance,
    p.prd_nm          AS product_name,
    p.prd_cost,
    p.prd_line,
    p.prd_start_dt,
    p.prd_end_dt
FROM Silver.crm_prd_info p
LEFT JOIN Silver.erp_px_cat_g1v2 pc
    ON p.cat_id = pc.id;
GO

/* =======================
   DIM DATE
   ======================= */
IF OBJECT_ID('Gold.dim_date', 'V') IS NOT NULL
    DROP VIEW Gold.dim_date;
GO

-- Build a proper date dimension from sales data
CREATE VIEW Gold.dim_date AS
SELECT DISTINCT
    CAST(sd.sls_order_dt AS DATE) AS full_date,
    CONVERT(INT, FORMAT(sd.sls_order_dt, 'yyyyMMdd')) AS date_key, -- surrogate
    DATEPART(YEAR, sd.sls_order_dt)  AS year,
    DATEPART(QUARTER, sd.sls_order_dt) AS quarter,
    DATEPART(MONTH, sd.sls_order_dt) AS month,
    DATENAME(MONTH, sd.sls_order_dt) AS month_name,
    DATEPART(DAY, sd.sls_order_dt)   AS day,
    DATEPART(WEEKDAY, sd.sls_order_dt) AS weekday_number,
    DATENAME(WEEKDAY, sd.sls_order_dt) AS weekday_name
FROM Silver.crm_sales_details sd
WHERE sd.sls_order_dt IS NOT NULL;
GO

/* =======================
   FACT SALES
   ======================= */
IF OBJECT_ID('Gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW Gold.fact_sales;
GO

CREATE VIEW Gold.fact_sales AS
SELECT
    sd.sls_ord_num   AS order_number,
    dc.customer_key,
    dp.product_key,
    dd.date_key      AS order_date_key,
    sd.sls_ship_dt   AS ship_date,
    sd.sls_due_dt    AS due_date,
    sd.sls_quantity,
    sd.sls_price,
    sd.sls_sales
FROM Silver.crm_sales_details sd
JOIN Gold.dim_customer dc
    ON sd.sls_cust_id = dc.source_customer_id   -- join on natural key
JOIN Gold.dim_product dp
    ON sd.sls_prd_key = dp.prd_key              -- join on natural key
LEFT JOIN Gold.dim_date dd
    ON sd.sls_order_dt = dd.full_date;
GO

/* ==================================================
   QUICK CHECKS
   ================================================== */
PRINT 'Gold Layer Objects Created Successfully';

SELECT TOP 5 * FROM Gold.dim_customer;
SELECT TOP 5 * FROM Gold.dim_product;
SELECT TOP 5 * FROM Gold.dim_date;
SELECT TOP 5 * FROM Gold.fact_sales;
GO
