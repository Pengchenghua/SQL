-- 总值
set tez.client.asynchronous-stop=false;
drop table if exists csx_tmp.temp_sale;
-- select * from csx_tmp.temp_sale;
CREATE TEMPORARY TABLE csx_tmp.temp_sale AS
SELECT mon,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
       dc_code,
       dc_name,
       goods_code,
       all_sale/sales_mon AS avg_sale_mon,
       sales_qty/sales_mon AS avg_qty_mon,
       all_sale, --销售额
       sales_qty, --销售量
       sale_sdt, --销售天数
       sales_mon, --销售月份
       sale_cust --销售客户数
FROM
  (SELECT substr(sdt,1,6) mon,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
          dc_code,
          a.dc_name,
          goods_code,
          sum(sales_value) all_sale, --销售额
          sum(sales_qty)sales_qty, --销售量
          count(DISTINCT sdt) AS sale_sdt, --销售天数
          count(DISTINCT substr(sdt,1,6)) AS sales_mon, --销售月份
          count(DISTINCT a.customer_no) AS sale_cust --销售客户数
FROM csx_dw.dws_sale_r_d_detail a
    WHERE sdt >= '20201201'
     AND sdt < '20210301'
    -- AND dc_code in ('W0A5','W0R9','W0A2','W0K7','W0L4','W0N0','W0W7','W0N1', 'W0A7','W0Q2','W0A6')
    -- AND attribute_code='1'
     AND a.business_type_name like '日配%'
     and a.region_code='1'
     AND substr(category_small_code,1,2) IN ('12','13','11')
   GROUP BY dc_code,
            goods_code,
            substr(sdt,1,6),
            a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.dc_name) a ;

-- 按照月份取最大与最小值
drop table if exists csx_tmp.temp_sale_01;
CREATE TEMPORARY TABLE csx_tmp.temp_sale_01 AS
SELECT a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  dc_code,
       goods_code,
       max(mon_sale)max_sale,
       min(mon_sale)min_sale,
       max(mon_qty)max_mon_qty,
       min(mon_qty)min_mon_qty,
       max(sale_cust)max_sale_cust,
       min(sale_cust)min_sale_cust
FROM
  (SELECT substr(sdt,1,6) AS mon,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
          dc_code,
          goods_code,
          sum(sales_value) mon_sale, --销售额
          sum(sales_qty) mon_qty,
          count(DISTINCT a.customer_no) AS sale_cust
   FROM csx_dw.dws_sale_r_d_detail a
--   JOIN
--      (SELECT customer_no
--       FROM csx_dw.dws_crm_w_a_customer_20200924
--       WHERE sdt='current'
--         AND attribute_code='1') b ON a.customer_no=b.customer_no
   WHERE sdt >= '20201201'
     AND sdt < '20210301'
    -- AND dc_code in ('W0A5','W0R9','W0A2','W0K7','W0L4','W0N0','W0W7','W0N1', 'W0A7','W0Q2','W0A6')
    and a.region_code='1'
    -- AND attribute_code='1'
    AND a.business_type_name like'日配%'
     AND  substr(category_small_code,1,2) IN ('12','13','11')
   GROUP BY dc_code,
            goods_code,
            substr(sdt,1,6),
            a.region_code,
  a.region_name,
  a.province_code,
  a.province_name) a
GROUP BY dc_code,
         goods_code,
         a.region_code,
  a.region_name,
  a.province_code,
  a.province_name;

--订单采购频次
drop table csx_tmp.temp_sale_02;

CREATE TEMPORARY TABLE csx_tmp.temp_sale_02 AS

SELECT sales_region_code,sales_region_name,sales_province_code,sales_province_name ,
    receive_location_code as dc_code,
    goods_code,
    count(DISTINCT order_code ) as wms_order_num
FROM csx_dw.dws_wms_r_d_entry_detail a 
join 
(select shop_id,sales_region_code,sales_region_name,sales_province_code,sales_province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and sales_region_code='1') b on a.receive_location_code=b.shop_id
WHERE sdt >= '20201201'
     AND sdt < '20210301'
  AND receive_status=2
  and receive_qty !=0
  and a.order_type_code like 'P%'
  -- AND receive_location_code  in ('W0A5','W0R9','W0A2','W0K7','W0L4','W0N0','W0W7','W0N1', 'W0A7','W0Q2','W0A6')
  group by 
    sales_region_code,sales_region_name,sales_province_code,sales_province_name ,
    receive_location_code ,
    goods_code;

-- insert overwrite directory '/tmp/pengchenghua/sale02' row format delimited fields terminated by'\t'
-- select * from csx_tmp.temp_sale_03;
drop table csx_tmp.temp_sale_03;
CREATE TEMPORARY TABLE csx_tmp.temp_sale_03 AS
select a.mon,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.dc_code,
    dc_name,
        a.goods_code,
        goods_name,
        short_name,
        unit_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       department_id,
       department_name,
       qualitative_period,
        avg_sale_mon,
        avg_qty_mon,
       all_sale, --销售额
       sales_qty, --销售量
       sale_sdt, --销售天数
       sales_mon, --销售月份
       sale_cust, --销售客户
       max_sale,
       min_sale,
       max_mon_qty,
       min_mon_qty,
       max_sale_cust,
       min_sale_cust,
       wms_order_num,
    --   final_amt,
    --   final_qty,
    --   cost,
    --   inv_sales_days,
      dense_rank()over(partition by category_middle_code,a.dc_code order by all_sale desc ) as sale_rank,
      des_specific_product_status,
      product_status_name 
from
(select a.mon,
    a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
    a.dc_code,
    dc_name,
        a.goods_code,
        
        avg_sale_mon,
        avg_qty_mon,
       all_sale, --销售额
       sales_qty, --销售量
       sale_sdt, --销售天数
       sales_mon, --销售月份
       sale_cust, --销售客户
       max_sale,
       min_sale,
       max_mon_qty,
       min_mon_qty,
       max_sale_cust,
       min_sale_cust,
       wms_order_num
from csx_tmp.temp_sale a 
left join
csx_tmp.temp_sale_01 b  on a.dc_code=b.dc_code and a.goods_code=b.goods_code
left join 
csx_tmp.temp_sale_02 c 
on a.dc_code=c.dc_code and a.goods_code=c.goods_code
)  a 
 left join
 (SELECT goods_id,
       goods_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       department_id,
       department_name,
       qualitative_period,
       unit_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') c on a.goods_code=c.goods_id
left join
(SELECT product_code,
      shop_code,
      short_name,
      des_specific_product_status,
      product_status_name
FROM csx_dw.dws_basic_w_a_csx_product_info
WHERE sdt='current'  ) d on a.dc_code=d.shop_code and a.goods_code=d.product_code
;
-- INVALIDATE METADATA csx_tmp.temp_sale_04;
-- insert overwrite directory '/tmp/pengchenghua/sale02' row format delimited fields terminated by'\t'
drop table csx_tmp.temp_sale_04;
create table csx_tmp.temp_sale_04 as 
select a.mon,
        a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.dc_code,
    dc_name,
        a.goods_code,
        goods_name,
        short_name,
        unit_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       department_id,
       department_name,
       qualitative_period,
        avg_sale_mon,
        avg_qty_mon,
       all_sale, --销售额
       sales_qty, --销售量
       sale_sdt, --销售天数
       sales_mon, --销售月份
       sale_cust, --销售客户
       max_sale,
       min_sale,
       max_mon_qty,
       min_mon_qty,
       max_sale_cust,
       min_sale_cust,
       wms_order_num,
       final_amt,
       final_qty,
       cost,
       inv_sales_days,
      dense_rank()over(partition by category_middle_code,a.dc_code order by all_sale desc ) as sale_rank,
      des_specific_product_status,
      product_status_name 
      from csx_tmp.temp_sale_03 a
left join 
(SELECT substr(sdt,1,6) as mon,
       dc_code,
       shop_name,
       goods_id,
       final_amt,
       final_qty,
       entry_value/entry_qty AS cost,
       inv_sales_days
FROM csx_tmp.ads_wms_r_d_goods_turnover a 
join 
(select shop_id,shop_name,sales_region_code,sales_region_name,sales_province_code,sales_province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and sales_region_code='1') b on a.dc_code=b.shop_id
WHERE sdt='20210228'
 -- AND dc_code in ('W0A5','W0R9','W0A2','W0K7','W0L4','W0N0','W0W7','W0N1', 'W0A7','W0Q2','W0A6')
  ) b on a.dc_code=b.dc_code and a.goods_code=b.goods_id and a.mon=b.mon; 
  
  select a.* from  csx_tmp.temp_sale_04 a ;
 
  show create table csx_dw.dws_basic_w_a_csx_product_info ;