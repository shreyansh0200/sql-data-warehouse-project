/*
store procidure: load bronze layer (source ->bronze).
this store procidure do not accept any parameter and return a value.
-- perform:- secondly--'bulk insert' 
            first-- truncate
*/
exec bronze_load_bronze

go

create or alter procedure bronze_load_bronze as
begin
 declare @start_time datetime,@end_time datetime;
     begin try
        print '========================================';
		print 'loading bronze layer';
        print '========================================';

		print'-----------------------------------------';
		print'loading CRM table';
		print'-----------------------------------------';

		print'truncating the data';
		truncate table bronze.crm_cust_info
		print'inserting the data';
		
		set @start_time=getdate();
		bulk insert bronze.crm_cust_info 
		from 'C:\Users\shrey\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with
		(	
			firstrow=2,
			fieldterminator=',',
			tablock
		)
		set @end_time=getdate();
		print 'load time :- ' +cast(dateDiff(second,@start_time,@end_time)as nvarchar(20)) + 'second';

		--select* from bronze.crm_cust_info

		--file2
		print'truncating the data';

		truncate table bronze.crm_prd_info
		print'inserting the data';
		set @start_time=getdate();

		bulk insert bronze.crm_prd_info
		from 'C:\Users\shrey\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with
		(
			firstrow=2,
			fieldTerminator=',',
			tablock
		)

		set @end_time=getdate();
		print 'load time :- ' +cast(dateDiff(second,@start_time,@end_time)as nvarchar(20)) + 'second';

		--file3
		print'truncating the data';

		truncate table bronze.crm_sales_details
		print'inserting the data';
		set @start_time=getdate();

		bulk insert bronze.crm_sales_details
		from 'C:\Users\shrey\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow=2,
			fieldTerminator=',',
			tablock
			)
		set @end_time=getdate();
		print 'load time :- ' +cast(dateDiff(second,@start_time,@end_time)as nvarchar(20)) + 'second';
        
		print'-----------------------------------------';
		print'loading ERP table';
		print'-----------------------------------------';
	--	select top 10* from bronze.crm_sales_details
		--file4
		print'truncating the data';

		truncate table bronze.erp_cust_az12
		print'inserting the data';
		set @start_time=getdate();

		bulk insert bronze.erp_cust_az12
		from 'C:\Users\shrey\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow=2,
			fieldTerminator=',',
			tablock
			)
		set @end_time=getdate();
		print 'load time :- ' +cast(dateDiff(second,@start_time,@end_time)as nvarchar(20)) + 'second';

--		select top 10 * from bronze.erp_cust_az12

		--file5
		print'truncating the data';
		truncate table bronze.erp_loc_a101
		print'inserting the data';
		set @start_time=getdate();

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\shrey\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
			firstrow=2,
			fieldTerminator=',',
			tablock
			)
		set @end_time=getdate();
		print 'load time :- ' +cast(dateDiff(second,@start_time,@end_time)as nvarchar(20)) + 'second';

--		select top 10 * from bronze.erp_LOC_A101
		--file6
		print'truncating the data';
		truncate table [bronze].[erp_PX_CAT_G1V2]
		print'inserting the data';
		set @start_time=getdate();

		bulk insert [bronze].[erp_PX_CAT_G1V2]
		from 'C:\Users\shrey\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
			firstrow=2,
			fieldTerminator=',',
			tablock
		)
		set @end_time=getdate();
		print 'load time :- ' +cast(dateDiff(second,@start_time,@end_time)as nvarchar(20)) + 'second';

--		select* from [bronze].[erp_PX_CAT_G1V2]
	end try

	begin catch
		print'=====================================';
		print'error occured during loading bronze layer';
		print'errorMessage'+ error_message();
		print'errorMessage'+ cast(error_number() as nvarchar);
		print'errorMessage'+ cast(error_state() as nvarchar);
		print'=====================================';

	end catch
end

