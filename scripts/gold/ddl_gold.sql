/*--==============
dl gold view
--==================
script creat gold layer of data warehouse
 using star schema
 these views can quirefied for analytic and reporting*/









--create views for dim_customers

if object_id('gold.dim_customers' ,'v') is not null
    drop view gold.dim_customers;
	go
create view gold.dim_customers as
select 
ROW_NUMBER() over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as frist_name,
ci.cst_lastname as last_name,
la.CNTRY as country,
ci.cst_materia_status as marital_status,
case 
	when ci.cst_gndr != 'n/a' then ci.cst_gndr --crm is master for gender info.
	else coalesce(ca.gen,'n/a' )
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from [silver].[crm_cust_info] ci
left join [silver].[erp_CUST_AZ12] ca
on ci.cst_key=ca.cid
left join [silver].[erp_LOC_A101] la
on ci.cst_key=la.cid

go


--- create view for dim_products


if object_id('gold.dim_products' ,'v') is not null
 drop view gold.dim_products;
 go
create view gold.dim_products as
select  
row_number() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as catagory_id,
pc.cat as catagory,
pc.SUBCAT as subcatagory,
pc.maintenance,

pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date

from silver.[crm_prd_info] pn
left join [silver].[erp_PX_CAT_G1V2] pc
on pc.id=pn.cat_id
where pn.prd_end_dt is null  -- filtering out all historical date

go
-- create for  fact_sales
if object_id('gold.fact_sales' ,'v') is not nULL
   drop view gold.fact_sales;
   go

create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
cu.customer_key,
pr.product_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shiping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as price

from [silver].[crm_sales_details] sd
left join [gold].[dim_products] pr
on sd.sls_prd_key=pr.product_number
left join [gold].[dim_customers] cu
on cu.customer_id=sd.sls_cust_id
 

