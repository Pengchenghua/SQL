select * from csx_b2b_scm.scm_apply_order_header saoh ;
select * from csx_b2b_accounting.accounting_stock_detail asd where location_code ='W0X6' and product_code ='1294102';
select * from data_sync.data_ending_inventory dei  where location_code ='W0X6' and product_code ='1294102' and data_month>='202011'; 


SELECT
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 tax_rate,
 ( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS end_qty ,
 ( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS end_amt_no_tax ,
 ( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_tax 
FROM
	csx_b2b_accounting.accounting_stock_detail
where posting_time < '2021-02-01 00:00:00'
 and location_code='W0X6'
 and product_code='1294102';