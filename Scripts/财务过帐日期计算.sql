-- 使用impala 查询

show create table csx_ods.source_wms_r_m_data_ending_inventory ;
-- 库存按照过帐日期查询 商品组别 
refresh csx_dw.dws_basic_w_a_csx_product_info;
set mapred.reduce.tasks=1;
drop table if exists csx_tmp.temp_post_goods;
create temporary table csx_tmp.temp_post_goods as 
select shipper_code,location_code,shop_name,a.company_code,a.reservoir_area_code,c.reservoir_area_name,
a.product_code,product_name,purchase_group_code,purchase_group_name,end_qty,end_amt_no_tax,end_amt_tax
from 
(SELECT
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 sum( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS end_qty ,
 sum( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS end_amt_no_tax ,
 sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_tax 
FROM
	csx_dw.dwd_cas_r_d_accounting_stock_detail
where posting_time < '2021-03-01 00:00:00'
 and sdt<='20210308'
GROUP BY
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code 
 
 ) a 
 left join 
 (select * from csx_dw.dws_basic_w_a_csx_product_info cpi where sdt='current' )b 
     on a.product_code=b.product_code and a.location_code=shop_code
LEFT JOIN 

(select warehouse_code,reservoir_area_code,reservoir_area_name from csx_ods.source_wms_w_a_wms_reservoir_area wra)c 
    on a.reservoir_area_code=c.reservoir_area_code and c.warehouse_code=location_code 
;
   
insert overwrite directory '/tmp/pengchenghua/stock_202103' row format delimited fields terminated by ';'
select * from csx_tmp.temp_post_goods
;
   
-- 验证数据
   select sum(end_qty),sum(end_amt_no_tax) from (
select shipper_code,location_code,shop_name,a.company_code,a.reservoir_area_code,c.reservoir_area_name,
a.product_code,product_name,purchase_group_code,purchase_group_name,end_qty,end_amt_no_tax,end_amt_tax
from 
(SELECT
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 sum( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS end_qty ,
 sum( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS end_amt_no_tax ,
 sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_tax 
FROM
	csx_dw.dwd_cas_r_d_accounting_stock_detail
where posting_time < '2021-05-01 00:00:00'
 and sdt<='20210506'
GROUP BY
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code  
 ) a 
 left join 
 (select * from csx_dw.dws_basic_w_a_csx_product_info cpi where sdt='current' )b 
     on a.product_code=b.product_code and a.location_code=shop_code
LEFT JOIN 

(select warehouse_code,reservoir_area_code,reservoir_area_name from csx_ods.source_wms_w_a_wms_reservoir_area wra)c 
    on a.reservoir_area_code=c.reservoir_area_code and c.warehouse_code=location_code 
) a ;
