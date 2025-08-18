/*
--> stored procedure : load silver layer (bronz->silver)
-->preform :- 1: ELT
-->TRUNCATE and LOAD.

command use for loding execution :-   silver.load_silver;
*/


exec silver.load_silver
 go
create or alter procedure silver.load_silver as
begin
declare @startTime datetime,@endTime datetime;
    begin try
       --* cleaning data.
         -- selecting unique pk
         -- data consistency and standerdationn
         print'===========================';
         print'loading silver layer';
         print'===========================';

         print'-------------------------------';
         print'loading CRM';
         print'-------------------------------';
         

         print'first truncate then insert';
         truncate table  silver.crm_cust_info;
 
         set @startTime=getdate();
         insert into silver.crm_cust_info (
          cst_id,
          cst_key,
          cst_firstname,
          cst_lastname,
          cst_materia_status,
          cst_gndr,
          cst_create_date

         )
         --cleansing table1
         select
         cst_id,
         cst_key,
         trim(cst_firstname) as cst_firstname,    --2.removing unwanted space
         trim(cst_lastname) as cst_lastname,
         case 
	        when upper(trim(cst_materia_status))='S' then 'Single'  --3.data normalization and standization
	        when upper(trim(cst_materia_status))='M' then 'Married'
	        else 'n/a'                                              --4.handeling the missing value
         end cst_materia_status,
         case 
	        when upper(trim(cst_gndr))='M' then 'Male'
	        when upper(trim(cst_gndr))='F' then 'Female'
	        else 'n/a'
        end cst_gndr,
         cst_create_date
 
         from(
         select
         *,
         row_number() over(partition by cst_id  order by cst_create_date desc) as flag_last --1.handeling duplicate pks
          from [bronze].[crm_cust_info]
          where cst_id is not NULL

         ) t where flag_last=1;
         set @endTime=getdate();
         print'load time:- ' + cast(datediff(second,@startTime,@endTime) as nvarchar(20)) 

         --===========================================
         print'first truncate then insert';
         set @startTime=getdate();

         truncate table silver.crm_prd_info;
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

        --cleaning data of table 2
        select 
        prd_id,
        replace(substring(prd_key,1,5),'-' , '_') as cat_id,-- foriegn key 
        substring(prd_key, 7, len(prd_key)) as prd_key,     --foriegn key
        prd_nm,
        isnull(prd_cost,0) prd_cost,


        case upper(trim(prd_line)) 
	        when 'R' then 'Mountain'
	        when 'S' then 'Road'
	        when 'T' then 'Other Sales'
	        when 'M' then 'Touring'
            else 'n/a'
        end prd_line ,

        cast(prd_start_dt as date) as prd_start_dt, -- data transformation 

        cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt -- date fixing logic.

        from [bronze].[crm_prd_info];
        set @endTime=getdate();
         print'load time:- ' + cast(datediff(second,@startTime,@endTime) as nvarchar(20)) ;




        --=========================================
         print'first truncate then insert';
         set @startTime=getdate();

        truncate table silver.[crm_sales_details];

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
        -- cleaning table 3
        select   
            sls_ord_num,   -- done
            sls_prd_key,   -- done
            sls_cust_id,   -- done

            case
                when sls_order_dt=0 or len(cast(sls_order_dt as varchar(8))) != 8 then NULL
                else cast(cast(sls_order_dt as varchar(8)) as date)
            end as sls_order_dt,

            case
                when sls_ship_dt=0 or len(cast(sls_ship_dt as varchar(8))) != 8 then NULL
                else cast(cast(sls_ship_dt as varchar(8)) as date)
            end as sls_ship_dt,

            case
                when sls_due_dt=0 or len(cast(sls_due_dt as varchar(8))) != 8 then NULL
                else cast(cast(sls_due_dt as varchar(8)) as date)
            end as sls_due_dt,

            case 
                when sls_sales is NULL or sls_sales <= 0 
                     or sls_sales != sls_quantity * abs(sls_price)  
                then sls_quantity * abs(sls_price)
                else sls_sales
            end as sls_sales,

            sls_quantity,

            case    
                when sls_price is NULL or sls_price <= 0   
                then sls_sales / nullif(sls_quantity,0)
                else sls_price
            end as sls_price

        from [bronze].[crm_sales_details];
        set @endTime=getdate();
         print'load time:- ' + cast(datediff(second,@startTime,@endTime) as nvarchar(20)) ;




        ---===========================================
        print'-----------------------------';
        print'loading ERP';
        print'---------------------------';

        print'first truncate then insert';
        set @startTime=getdate();
        truncate table [silver].[erp_CUST_AZ12];

        insert into [silver].[erp_CUST_AZ12](cid,bdate,gen)

        --cleansing table 4.

        select 
        case 
            when cid like 'NAS%' then substring(cid,4,len(cid)) --removing 'NAS' prefix as it is forign key
            else cid
        end cid,

        case when bdate > getdate() then null                -- set future birtndate to null
             else bdate
        end bdate,

        case  	
	        when upper(trim(gen))in('M','Male') then 'Male'
	        when upper(trim(gen))in('F','Female') then 'Female'
	        else 'n/a'
        end gen                                                     --normalize gender value after checking standardization


 
        from [bronze].[erp_CUST_AZ12];
        set @endTime=getdate();
         print'load time:- ' + cast(datediff(second,@startTime,@endTime) as nvarchar(20)) ;


        --========================================
         print'first truncate then insert';
         set @startTime=getdate();

        truncate table [silver].[erp_LOC_A101];
        insert into [silver].[erp_LOC_A101](
            cid,
            cntry
        )
        --cleansing table 5
        select 
        replace(cid,'-','') cid,            --removing unmatch value from this forign key.
        case when trim(cntry)='DE' then'Germany'-- normalize the data.
             when trim(cntry) in ('US','USA') THEN 'United States'
             when len(cntry) is null or len(cntry)=' ' then 'n/a'
             else trim(cntry)
        end cntry
        from [bronze].[erp_LOC_A101];
        set @endTime=getdate();
         print'load time:- ' + cast(datediff(second,@startTime,@endTime) as nvarchar(20)) ;


        --=============================================
         print'first truncate then insert';
         set @startTime=getdate();

        truncate table  silver.[erp_PX_CAT_G1V2];

        insert into silver.[erp_PX_CAT_G1V2]
        (id,cat,subcat,maintenance)

        select
        id,
        cat,
        subcat,
        maintenance
        from bronze.[erp_PX_CAT_G1V2];
        set @endTime=getdate();
         print'load time:- ' + cast(datediff(second,@startTime,@endTime) as nvarchar(20)) ;

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


