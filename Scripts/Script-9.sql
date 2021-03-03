SELECT
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 sum( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS qty ,
 sum( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS amt_no_tax 
FROM
 csx_b2b_accounting.accounting_stock_detail  
 where posting_time < '2020-06-01 00:00:00'
 and location_code='99B1'
GROUP BY
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code 
 ;
 select * from csx_b2b_accounting.accounting_stock_log_item asd 
 where 
 location_code ='W053'
        and create_time <'2020-04-01 00:00:00'
         and create_time >='2020-03-01 00:00:00'
        and move_type  in ('115A','116A');
 
 SELECT * FROM data_sync.data_sync_sale_order_item dssoi  where product_code ='1250452';
  SELECT * FROM csx_basic_data.md_category_info mci   where product_code ='1250452';