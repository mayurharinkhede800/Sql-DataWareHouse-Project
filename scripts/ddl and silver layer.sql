
-- ddl for Silver Lavel
-- First Table 
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
-- Second Table 
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
	cat_id			NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    date,
    prd_end_dt      date,
    dwh_create_date DATETIME2 DEFAULT SYSDATETIME()
);
-- third Table
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Creating Schemas for silver level for ERP 
-- first table
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid              NVARCHAR(50),
    bdate            DATE,
    gen              NVARCHAR(50),
    dwh_create_date  DATETIME2 DEFAULT GETDATE()
);

-- second table
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid              NVARCHAR(50),
    cntry            NVARCHAR(50),
    dwh_create_date  DATETIME2 DEFAULT getdate()
);
-- third table
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id               NVARCHAR(50),
    cat              NVARCHAR(50),
    subcat           NVARCHAR(50),
    maintenance      NVARCHAR(50),
    dwh_create_date  DATETIME2 DEFAULT SYSDATETIME()
);

--========================================================================================
-- QUALITY CHECK OF DATA AND CLEANING
--========================================================================================
--====================FIRST CRM CUST_INFO TABLE=============================
SELECT 
CST_ID,
COUNT(CST_ID)
FROM bronze.crm_cust_info
GROUP BY CST_ID
HAVING COUNT(*) > 1
/*
FIND DUPLICATE AND NULL VALUES
CST_ID	(No column name)
29449	2
29473	2
29433	2
NULL	0
29483	2
29466	3
*/

-- check null values and duplicate values
-- leading and trailing spaces in name and lastname
-- replacing sort name with full names i.e 
--			F=Female,M=Male,S=Single,M=Married
--===================================================================
exec silver.load_silver  -- execute the silver layer store procedure
create or alter procedure silver.load_silver as -- Create store procedure for inserting data into silver level 
begin
	print '>> truncating table : silver.crm_cust_info'
	truncate table silver.crm_cust_info
	print '>> Inserting data into : silver.crm_cust_info'
	insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when upper(trim(cst_marital_status)) ='S' then 'Single'
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
		else 'n/a'
	end cst_marital_status ,
	case when upper(trim(cst_gndr)) ='F' then 'Female'
		when upper(trim(cst_gndr)) = 'M' then 'Male'
		else 'n/a'
	end cst_gndr,
	cst_created_date
	from (
	SELECT 
		* ,
		ROW_NUMBER() OVER(PARTITION BY CST_ID ORDER BY CST_CREATED_DATE DESC ) AS FLAG_LAST	
		FROM bronze.crm_cust_info
		where cst_id is not null
	)T WHERE FLAG_LAST = 1

	--=====================CRM SECOND:PRODUCT INFO TABLE=============================
	-- order date must be less than the shipping date
	print '>> Truncating data : silver.crm_prd_info'
	truncate table silver.crm_prd_info
	print '>> Inserting data into : silver.crm_prd_info'
	insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		replace(SUBSTRING(prd_key,7,20),'-','_') as prd_key,
		prd_nm,
		coalesce(prd_cost,0) as prd_cost,
		case UPPER(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'n/a'
		end prd_line,
		cast(prd_start_dt as Date) as prd_start_dt ,
		cast(lead(prd_start_dt) over( partition by prd_key order by prd_start_dt )-1 as Date ) as prd_end_dt
	from bronze.crm_prd_info

	--=====================CRM SECOND:Sales INFO TABLE=============================
	-- order date must be lesser then shipping date and due date 
	-- business rules sales and sales values and sales price must not be null
	--==========================================================================================
	print '>> Truncating data : silver.crm_sales_details'
	truncate table silver.crm_sales_details
	print '>> Inserting data into : silver.crm_sales_details'
	insert into silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	select 
		sls_ord_num ,
		sls_prd_key,
		sls_cust_id,
			case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
				else CAST(cast(sls_order_dt as varchar)as date) 
			end as sls_order_dt,
			case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				else CAST(cast(sls_ship_dt as varchar)as date) 
			end as sls_ship_dt,
			case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
				else CAST(cast(sls_due_dt as varchar)as date) 
			end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity* abs(sls_price)
				then sls_quantity * abs(sls_price)
			else sls_sales
		end sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
			then sls_sales / nullif(sls_quantity,0)
			else sls_price
		end as sls_price
	from bronze.crm_sales_details 


	--=====================CRM FIRST: erp_cust_az12 TABLE=============================
	print '>> Truncating data : silver.erp_cust_az12'
	truncate table silver.erp_cust_az12
	print '>> Inserting data into : silver.erp_cust_az12'
	insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	select 
		case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
			else cid
		end cidnew,
		case when bdate > GETDATE() then null
			else bdate
		end bdate,
		case when upper(trim(gen)) in ('F', 'FEMALE')  then 'Female'
			when upper(trim(gen)) in ('M', 'MALE')  then 'Male'
			else 'n/a'
		end as gen
	from bronze.erp_cust_az12
	--=====================CRM second: erp_loc_a101 TABLE=============================
	print '>> Truncating data : silver.erp_loc_a101'
	truncate table silver.erp_loc_a101
	print '>> Inserting data into : silver.erp_loc_a101'
	insert into silver.erp_loc_a101
	(
	cid,
	cntry
	)
	select 
		REPLACE(cid,'-','') cidnew,
			case when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) in ('US','USA') then 'United States'
				when trim(cntry) is null or cntry ='' then 'n/a'
			else trim(cntry)
		end as cntry
	from bronze.erp_loc_a101

	--=====================CRM thirt: erp_px_cat_g1v2 TABLE=============================
	print '>> Truncating data : silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2
	print '>> Inserting data into : silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2
	(id,cat,subcat,maintenance)
	select 
	id ,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2
end
-- ============================= silver layer Done ============================

-- stuffs 
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


use DataWareHouse 

select * from silver.crm_cust_info

--cantinue

--====================FIRST CRM CUST_INFO TABLE=============================
-- stuffs onky checking not a replacing query 
select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

SELECT 
CST_ID,
COUNT(CST_ID)
FROM silver.crm_cust_info
GROUP BY CST_ID
HAVING COUNT(*) > 1

select 
cst_gndr ,
count(*)
from silver.crm_cust_info
group by cst_gndr

select 
cst_marital_status,
count(*)
from bronze.crm_cust_info
group by cst_marital_status

select top 200 * from silver.crm_cust_info
--=====================CRM SECOND:PRODUCT INFO TABLE=============================
select top 100 * from bronze.crm_prd_info

-- checking duplicate and null
SELECT 
prd_id,
COUNT(prd_id)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id is null
-- no duplicate primary key or null

-- cheking null values 
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null 

--

select distinct gen 
from bronze.erp_cust_az12

select distinct 
cntry 
c
from bronze.erp_loc_a101
group by cntry
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%