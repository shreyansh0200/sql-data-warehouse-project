/* checking the quality of data(silver layer) in each table (total table =6)
  what i do?
--> tracking null and duplicate pk
-->unwanted spaces in string fields.
-->data standardization and consistency.
-->invalid data ranges.
-->data consistancy in related field
*/


--===============
--table 1/6
--===============
-->data quality

--check for null and duplicates in primary key
-- expectation : no result
select
cst_id,
count(*)
from silver.[crm_cust_info]


group by cst_id
having count(*)>1 or cst_id is null

go

-- check  for unwanted spaces
-- expectation : no results

use datawarehouse
select cst_firstname
from silver.crm_cust_info
where cst_firstname !=trim(cst_firstname)

go
-- data consistency and standerdationn

select distinct
cst_materia_status
from silver.[crm_cust_info]

go
-- data consistency and standerdationn

select distinct
cst_gndr
from silver.[crm_cust_info]

go


--============
--table 2/6
--===========
--data quality


--checking null
--expecting: no result
select
prd_nm
from [bronze].[crm_prd_info]
where prd_nm is NULL


--checking null
--expecting: no result
select
prd_key
from silver.[crm_prd_info]
where prd_key is NULL

--checkunwanted space
--expect: no result
select
prd_nm
from silver.[crm_prd_info]
where prd_nm!= trim(prd_nm)

--checking null
--expecting no result
select
prd_cost
from silver.[crm_prd_info]
where prd_cost is NULL

---data normalization and standardization
select distinct
prd_line
from silver.[crm_prd_info]

--checking null
select 
prd_start_dt
from
silver.[crm_prd_info]
where prd_start_dt is null


--=======
--table 3/6
--=========

--data quality



-- invalid date tracking
--expecting: no result
select top 10
*
from silver.[crm_sales_details]
where sls_order_dt>sls_ship_dt or sls_order_dt> sls_due_dt


-- checking data consistency between sales, quantity 
-->>sales= quantity*price
-->> value , must be not null ,zero , or negative
--expecting:no result

select 
sls_sales,
sls_quantity,
sls_price,
case 
     when sls_sales is NULL or sls_sales <=0 or sls_sales!=sls_quantity*sls_price  then sls_quantity*abs(sls_price)
     else sls_sales
end sls_sales,

sls_quantity,

case    
    when  sls_price is NULL or sls_price <=0   then sls_sales/nullif(sls_quantity,0)
    else sls_price
end sls_price



from silver.[crm_sales_details]
where 
sls_sales!=(sls_quantity*sls_price)
or sls_sales<=0  or sls_sales is null
or sls_quantity<=0 or sls_quantity is null
or sls_price<=0 or sls_price is null


--=========
--table 4/6
--==========
--> data quality


-- identifing the range of date
--expect : no result
select
count(bdate) over(),
bdate
from silver.[erp_CUST_AZ12]
where  bdate>getdate()


-- data standardization and consistancy
select distinct 
gen
,case  	
	when upper(trim(gen))in('M','Male') then 'Male'
	when upper(trim(gen))in('F','Female') then 'Female'
	else 'n/a'
end gen
from silver.[erp_CUST_AZ12]



--==========
--table 5/6
--==========
--data standardization and consistency.
select distinct
cntry

from silver.[erp_LOC_A101]


--==========
-- table 6/6
--==========

-- check unwanted space

select 
cat
from [bronze].[erp_PX_CAT_G1V2]
where trim(cat)!=cat or trim(SUBCAT)!=subcat or trim(MAINTENANCE)!=maintenance

-- data standardization and consistency
select distinct
MAINTENANCE
from [bronze].[erp_PX_CAT_G1V2]




