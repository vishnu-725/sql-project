
-- BRONZE LEVEL 
create database DataWarehouse
use DataWarehouse
create schema Bronze;
go
create schema Silver;
go
create schema Gold;
go


create table Bronze.crm_cust_info (
cst_id int ,
cst_key nvarchar(50) ,
cst_firstname nvarchar(50) ,
cst_lastname nvarchar(50) ,
cst_martial_status nvarchar(50) ,
cst_gndr nvarchar(50) ,
cst_create_date date
);


create table Bronze.crm_prd_info(
prd_id int ,
prd_key nvarchar(50) ,
prd_nm nvarchar(50) ,
prd_cost int ,
prd_line nvarchar(50) ,
prd_start_dt datetime ,
prd_end_dt datetime 
);

create table Bronze.crm_sales_details(
sls_ord_num nvarchar(50) ,
sls_prd_key nvarchar(50) ,
sls_cust_id int ,
sls_order_dt int ,
sls_ship_dt int ,
sls_due_dt int ,
sls_sales int ,
sls_quantity int ,
sls_price int
);


create table Bronze.erp_loc101(
cid nvarchar(50) ,
cntry nvarchar(50)
);

create table Bronze.erp_cust_az12(
cid nvarchar(50) ,
bdate date ,
gen nvarchar(50)
);

create table Bronze.erp_px_cat_g1v2(
id nvarchar(50) ,
cat nvarchar(50) ,
subcat nvarchar(50) ,
maintenance nvarchar(50) ,
);

bulk insert Bronze.crm_cust_info
from "C:\Users\vishn\OneDrive\Documents\customer.txt"
with (
firstrow = 2 ,
fieldterminator = ',' ,
tablock
);

select * from DataWarehouse.Bronze.crm_cust_info

bulk insert Bronze.crm_prd_info
from "C:\Users\vishn\OneDrive\Documents\product.txt"
with (
firstrow = 2 ,
fieldterminator = ',' ,
tablock
);

select * from DataWarehouse.Bronze.crm_prd_info

bulk insert Bronze.crm_sales_details
from "C:\Users\vishn\OneDrive\Documents\sales.txt"
with (
firstrow = 2 ,
fieldterminator = ',' ,
tablock
);

select * from DataWarehouse.Bronze.crm_sales_details

bulk insert Bronze.erp_loc101
from "C:\Users\vishn\OneDrive\Documents\loc.txt"
with (
firstrow = 2 ,
fieldterminator = ',' ,
tablock
);

select * from DataWarehouse.Bronze.erp_loc101

bulk insert Bronze.erp_cust_az12
from "C:\Users\vishn\OneDrive\Documents\custo.txt"
with (
firstrow = 2 ,
fieldterminator = ',' ,
tablock
);



select * from DataWarehouse.Bronze.erp_cust_az12

bulk insert Bronze.erp_px_cat_g1v2
from "C:\Users\vishn\OneDrive\Documents\cattt.txt"
with (
firstrow = 2 ,
fieldterminator = ',' ,
tablock
);



select top(10) *  from DataWarehouse.Bronze.crm_cust_info
select top(10) * from DataWarehouse.Bronze.crm_prd_info
select top(10) * from DataWarehouse.Bronze.crm_sales_details
select top(10) * from DataWarehouse.Bronze.erp_loc101
select top(10) * from DataWarehouse.Bronze.erp_cust_az12
select top(10) * from DataWarehouse.Bronze.erp_px_cat_g1v2 
select * from DataWarehouse.Bronze.crm_cust_info where cst_gndr is null
select * from DataWarehouse.Bronze.crm_cust_info where cst_id is null
--update DataWarehouse.Bronze.crm_cust_info set first_name = trim(first_name) where first_name != trim(first_name)
select * from DataWarehouse.Bronze.crm_cust_info where contains(cst_gndr , 'M')

select * from(select * , ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag from Bronze.crm_cust_info)t where flag > 1

select trim(cst_firstname) from Bronze.crm_cust_info where cst_firstname !=trim(cst_firstname)
SELECT cst_key from Bronze.crm_cust_info
