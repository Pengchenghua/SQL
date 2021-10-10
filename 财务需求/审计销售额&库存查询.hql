--财务库存成本&定价成本计算周转
--销售额

-- 公司代码	公司名称	利润中心	课组	课组名称	大类编码	大类名称	不含税收入	不含税成本

SELECT company_code,
       company_name,
       dc_code,
       a.dc_name,
       department_code,
       department_name,
       category_large_code,
       category_large_name,
       sum(excluding_tax_sales)as  no_tax_sale,
       sum(excluding_tax_cost) AS no_tax_cost
FROM csx_dw.dws_sale_r_d_detail a 
WHERE sdt>='20200101'
  AND sdt<='20201231'
group by 
        company_code,
       company_name,
       dc_code,
       a.dc_name,
       department_code,
       department_name,
       category_large_code,
       category_large_name
  ;


--期末库存额
  -- 公司代码	公司名称	利润中心	课组	课组名称	大类编码	大类名称	不含税收入	不含税成本

SELECT b.company_code,
       b.company_name,
       dc_code,
       a.dc_name,
       a.department_id,
       department_name,
       category_large_code,
       category_large_name,
       sum(a.amt_no_tax)as  no_tax_sale
FROM csx_dw.dws_wms_r_d_accounting_stock_m a 
join 
(select shop_id,company_code,company_name 
from csx_dw.dws_basic_w_a_csx_shop_m 
where sdt='current' and table_type='1' and purpose !='06') b on a.dc_code=b.shop_id
WHERE sdt='20201231'
and reservoir_area_code not in ('PD01','PD02','TS01')
group by 
        b.company_code,
       b.company_name,
       dc_code,
       a.dc_name,
       department_id,
       department_name,
       category_large_code,
       category_large_name
  ;





-- 财务过帐期末库存
set mapred.reduce.tasks=1;
set mapred.map.tasks=1;
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
where posting_time < '2021-01-01 00:00:00'
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
SELECT shipper_code,
       b.company_code,
       b.company_name ,
       location_code,
       shop_name,
       purchase_group_code,
       purchase_group_name,
       category_large_code,
       category_large_name,
       sum(end_qty)as end_qty,
       sum(end_amt_no_tax) as end_amt_no_tax,
       sum(end_amt_tax) as end_amt_tax
FROM csx_tmp.temp_post_goods a
join 
(select shop_id,company_code,company_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and table_type='1' and purpose !='06') b on a.location_code=b.shop_id
join 
(select goods_id,category_large_code,category_large_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' ) c on a.product_code=c.goods_id
group by shipper_code,
       b.company_code,
       b.company_name ,
       location_code,
       shop_name,
       purchase_group_code,
       purchase_group_name,
       category_large_code,
       category_large_name
;
   
   
