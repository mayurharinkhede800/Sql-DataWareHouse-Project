-- ==================================             ===============================================
--                                   CREATE TABLES
--===================================             ===============================================
create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_created_date date
);
-- Create product info table
CREATE TABLE bronze.crm_prd_info (
    prd_id        INT,
    prd_key       NVARCHAR(50),
    prd_nm        NVARCHAR(50),
    prd_cost      INT,
    prd_line      NVARCHAR(50),
    prd_start_dt  DATETIME,
    prd_end_dt    DATETIME
);
-- Create sales details table
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num    NVARCHAR(50),
    sls_prd_key    NVARCHAR(50),
    sls_cust_id    INT,
    sls_order_dt   INT,
    sls_ship_dt    INT,
    sls_due_dt     INT,
    sls_sales      INT,
    sls_quantity   INT,
    sls_price      INT
);

-- Create ERP location table
CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO


-- Create ERP customer table
CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

-- Create ERP product category table
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO
-- ======================================================            ===============================================
--                                                       BULK INSERT
-- ======================================================            ===============================================

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime ,@end_time datetime,@batch_start_time datetime ,@batch_end_time datetime;
	begin try
		-- loading bronze layer
		set @batch_start_time = GETDATE();
		print '==========================================================';
		print 'Loading Bronze layer';
		print '==========================================================';

		print '==========================================================';
		print 'Loading CRM Tabe';
		print '==========================================================';
		--crm first table
		print '>> Truncate table : bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print '>> Truncated table :bronze.crm_cust_info';
		set @start_time = GETDATE();
		bulk insert bronze.crm_cust_info
		from 'E:\SQL With Baraa\DataWareHouseProject\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow =2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print '>> load duration : '+ cast(datediff(second,@start_time , @end_time)as nvarchar) + 'Seconds';
		print '==========================================================';

		-- second
		print '>> Truncating table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		print '>> Truncated table : bronze.crm_prd_info';
		set @start_time = GETDATE();
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\SQL With Baraa\DataWareHouseProject\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> load duration : '+ cast(datediff(second,@start_time , @end_time)as nvarchar) + 'Seconds';
		print '==========================================================';

		-- third
		print '>> Truncating table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		print '>> Truncated table : bronze.crm_sales_details';
		set @start_time = GETDATE();
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\SQL With Baraa\DataWareHouseProject\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> load duration : '+ cast(datediff(second,@start_time , @end_time)as nvarchar) + 'Seconds';
		print '==========================================================';


		print '==========================================================';
		print 'Loading ERP Tabe';
		print '==========================================================';
		-- erp first table
		print '>> Truncating table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		print '>> Truncated table : bronze.erp_cust_az12';
		set @start_time  = GETDATE();
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\SQL With Baraa\DataWareHouseProject\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> load duration : '+ cast(datediff(second,@start_time , @end_time)as nvarchar) + 'Seconds';
		print '==========================================================';

		-- second
		print 'truncating tabel :bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		print '>> Truncated table :bronze.erp_loc_a101';
		set @start_time = GETDATE();
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\SQL With Baraa\DataWareHouseProject\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> load duration : '+ cast(datediff(second,@start_time , @end_time)as nvarchar) + 'Seconds';
		print '==========================================================';

		-- third
		print '>> truncating table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		print '>> Trunacted table : bronze.erp_px_cat_g1v2';
		set @start_time = GETDATE();
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\SQL With Baraa\DataWareHouseProject\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> load duration : '+ cast(datediff(second,@start_time , @end_time)as nvarchar) + 'Seconds';
		print '==========================================================';

		set @batch_end_time = GETDATE();
		print '===========================================================';
		print 'Bronze layer Completed';
		print '===========================================================';
		print 'Total Daration : '+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds';
		print '===========================================================';

	end try
	begin catch
		print ' Error Occured';
		print 'Error message:'+error_message();
	end catch
end;

exec bronze.load_bronze