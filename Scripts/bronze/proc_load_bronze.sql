/*
==================================================================================
Store Procedure: Load Bronze Layer (source --> Bronze)
==================================================================================
Script Purpuse:
    This stored procedure loads into the 'bronze' schema from ecternal CSV files.
    it performs the following actions:
    -Truncate the bronze tables before loading data.
    -Uses the 'Bulk Insert' command to load data from csv files to bronze tables.

Parameters:
   None:
   This stored procedure doesn not accept any parametaers or return any values.

Usage Example:
    EXEC bronze.load_bronze;
==================================================================================

*/

create or alter procedure bronze.load_bronze as
begin
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	begin try
	        set @batch_start_time =getdate();
			print'==================================';
			print'Loading Bronze Layer';
			print'==================================';

			print'----------------------------------'
			print'Loading CRM Tables';
			print'----------------------------------'

			set @start_time = GetDate();
			print'>>Truncating Table: Bronze.crm_cust_info'
			truncate table bronze.crm_cust_info;

			print'>>Inserting Data into: Bronze.crm_cust_info'
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\Manju M\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			set @end_time = GetDate();
			print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
			print'>>------------------------'

			set @start_time = GetDate();
			print'>>Truncating Table: Bronze.crm_prd_info'
			truncate table bronze.crm_prd_info;

			print'>>Inserting Data into: Bronze.crm_prd_info'
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\Manju M\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			set @end_time = GetDate();
			print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
			print'>>------------------------'

			set @start_time = GetDate();
			print'>>Truncating Table: Bronze.crm_sales_details'
			truncate table bronze.crm_sales_details;

			print'>>Inserting Data into: Bronze.crm_sales_details'
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\Manju M\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			set @end_time = GetDate();
			print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
			print'>>------------------------'


			print'----------------------------------'
			print'Loading ERP Tables';
			print'----------------------------------'

			set @start_time = GetDate();
			print'>>Truncating Table: Bronze.erp_loc_a101'
			truncate table bronze.erp_loc_a101;

			print'>>Inserting Data into: Bronze.erp_loc_a101'
			bulk insert bronze.erp_loc_a101
			from 'C:\Users\Manju M\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
			with(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			set @end_time = GetDate();
			print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
			print'>>------------------------'

			set @start_time = GetDate();
			print'>>Truncating Table: Bronze.erp_cust_az12'
			truncate table bronze.erp_cust_az12;

			print'>>Inserting Data into: Bronze.erp_cust_az12'
			bulk insert bronze.erp_cust_az12
			from 'C:\Users\Manju M\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
			with(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			set @end_time = GetDate();
			print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
			print'>>------------------------'

			set @start_time = GetDate();
			print'>>Truncating Table: Bronze.erp_px_cat_g1v2'
			truncate table bronze.erp_px_cat_g1v2;

			print'>>Inserting Data into: Bronze.erp_px_cat_g1v2'
			bulk insert bronze.erp_px_cat_g1v2
			from 'C:\Users\Manju M\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
			with(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			set @end_time = GetDate();
			print'>> Load Duration: ' +cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
			print'>>------------------------'

			set @batch_end_time = getdate();
			print'============================'
			print' Bronze laye is completed';
			print' -Total load Duration: '+ cast(datediff(second,@batch_start_time, @batch_end_time) as nvarchar) +'seconds';
			print'============================'
	end try
	begin catch
	      print'==========================================='
		  print'Error Occured During Loading bronze layer'
		  print'Error Message ' + Error_Message();
		  print'Error Message ' + cast (Error_number() as nvarchar);
		  print'Error Message ' + cast (Error_state() as nvarchar);
		  print'==========================================='
	end catch
end;
