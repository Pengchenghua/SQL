select
	shipper_code,
 	location_code,
 	company_code,
 	reservoir_area_code,
 	product_code,
	wms_batch_no,
	end_qty ,
 	end_amt_no_tax 
 from 
(select
	shipper_code,
 	location_code,
 	company_code,
 	reservoir_area_code,
 	product_code,
	wms_batch_no ,
	sum( IF ( in_or_out = 0, txn_qty , IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS end_qty ,
 	sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_no_tax 
from
	csx_dw.dwd_cas_r_d_accounting_stock_detail
where
	sdt <= '20210114'
	-- and category_large_code ='1104'
	and location_code ='W053'
	and product_code ='5780'
group by 
	shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code,
	wms_batch_no
) a where end_qty!=0 and wms_batch_no  like 'TK%';


select
	shipper_code,
 	location_code,
 	company_code,
 	reservoir_area_code,
 	product_code,
	wms_batch_no,
	end_qty ,
 	end_amt_no_tax 
 from 
(select
	shipper_code,
 	location_code,
 	company_code,
 	reservoir_area_code,
 	product_code,
	wms_batch_no ,
	sum( IF ( in_or_out = 0, txn_qty , IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS end_qty ,
 	sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_no_tax 
from
	csx_dw.dws_wms_r_d_batch_detail 
where
	sdt <= '20210114'
	-- and category_large_code ='1104'
	and location_code ='W053'
	and product_code ='5780'
group by 
	shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code,
	wms_batch_no
) a where end_qty!=0 and wms_batch_no  like 'TK%';



SELECT
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 batch_no ,
 sum( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS end_qty ,
 sum( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS end_amt_no_tax ,
 sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_tax 
FROM
	csx_dw.dwd_cas_r_d_accounting_stock_detail
where posting_time < '2021-01-15 00:00:00'
 and sdt<='20210115'
 and location_code ='W053'
	and product_code ='5780'
GROUP BY
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 batch_no ,
 product_code 
 ;
 
