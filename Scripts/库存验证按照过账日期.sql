select
	*
from
	csx_b2b_accounting.accounting_stock_detail_view
where
	credential_no = 'PZ0000000000248118'
	and product_code = '945518';
-- 1192422641693442048
 select
	*
from
	csx_b2b_accounting.accounting_credential_item ;

select location_code ,product_code ,amt ,purchase_group_code
from (
select
	location_code,
	purchase_group_code,
   reservoir_area_code ,
	product_code ,
	sum(if(in_or_out = 1, -txn_amt, txn_amt)) as amt
from
	accounting_stock_detail_view
where
	posting_time < '2019-10-01 00:00:00'
	-- and company_code = '2304'
	group by location_code,
	product_code ,
	reservoir_area_code ,
	purchase_group_code 
) a 

	;
	
	
select * from csx_b2b_accounting.accounting_stock_detail_view where 
	posting_time < '2019-11-01 00:00:00'
	-- and company_code = '2211'
	and location_code ='W0A3'
	and product_code ='816'
-- and reservoir_area_code ='TS01';
	


select
	location_code,
	location_name ,
	purchase_group_code,
	product_code,
	reservoir_area_code,
	sum(if(in_or_out = 1, -txn_amt, txn_amt)) as amt
from
	accounting_stock_detail_view
where
	posting_time < '2019-11-01 00:00:00'
	and company_code = '2304'
	group by location_code,
	location_name ,
	purchase_group_code,product_code,reservoir_area_code ;
	


select location_code ,reservoir_area_code ,purchase_group_code ,sum(inv_qty)inv_qty,sum(period_inv_amt)period_inv_amt
from 
(
  SELECT 
    location_code,
    reservoir_area_code,
    purchase_group_code,
    SUM(IF(in_or_out = 1, - txn_amt, txn_amt))as period_inv_amt,
    sum(if(in_or_out =1,- txn_qty ,txn_qty ))as inv_qty
FROM
    accounting_stock_detail_view
WHERE
    posting_time < '2019-10-01 00:00:00'
GROUP BY location_code ,reservoir_area_code, purchase_group_code 
union all 
SELECT 
    location_code,
    reservoir_area_code,
    purchase_group_code,
    SUM(IF(in_or_out = 1, - txn_amt, txn_amt))as period_inv_amt,
    sum(if(in_or_out =1,- txn_qty ,txn_qty ))as inv_qty
FROM
    accounting_stock_detail_view
WHERE
 posting_time >= '2019-10-01 00:00:00'
  and  posting_time < '2019-11-01 00:00:00'
GROUP BY location_code ,reservoir_area_code, purchase_group_code  
)a group  by location_code ,reservoir_area_code ,purchase_group_code;

select * from csx_b2b_accounting.accounting_stock_log_item  where product_code='932022' and location_code ='99B2'
