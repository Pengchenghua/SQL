
SET hive.execution.engine=tez;


SET tez.queue.name=caishixian;

--查询 0、2 状态商品明细，创建日期小于20200601
drop table if exists csx_tmp.temp_goods_sps_00;
create temporary table csx_tmp.temp_goods_sps_00 as  
SELECT shop_code,
       product_code,
       des_specific_product_status,
       product_status_name,
       regexp_replace(to_date(create_time),'-','') as create_date
FROM csx_dw.dws_basic_w_a_csx_product_info
WHERE shop_code IN ('W0A2','W0A3','W0A5','W0A6','W0A7','W0A8','W0F4','W0K1','W0R9','W0K6','W0L3','W0N0','W0N1','W0P8','W0Q2','W0Q9')
  AND des_specific_product_status IN ('0','2')
  and regexp_replace(to_date(create_time),'-','')<'20200601'
  and sdt='20200916'
  ;
  
-- 查询近三个月销售商品明细
drop table if exists csx_tmp.temp_goods_sps_01;
create temporary table csx_tmp.temp_goods_sps_01 as 
SELECT 
       goods_code
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE dc_code IN ('W0A2','W0A3','W0A5','W0A6','W0A7','W0A8','W0F4','W0K1','W0R9','W0K6','W0L3','W0N0','W0N1','W0P8','W0Q2','W0Q9')
  and sdt>='20200601'
  group by 
       goods_code
  ;

-- 查询近三个月未入库商品
drop table if exists csx_tmp.temp_goods_sps_02;
create temporary table csx_tmp.temp_goods_sps_02 as 


SELECT receive_location_code as  dc_code,
       goods_code
FROM csx_dw.dws_wms_r_d_entry_order_all_detail
WHERE receive_location_code IN ('W0A2','W0A3','W0A5','W0A6','W0A7','W0A8','W0F4','W0K1','W0R9','W0K6','W0L3','W0N0','W0N1','W0P8','W0Q2','W0Q9')
  and sdt>='20200601'
  group by receive_location_code,
       goods_code
  ;

-- 统计数据
drop table if exists csx_tmp.temp_goods_spc;
create table csx_tmp.temp_goods_spc
as
select
    province_code,
    province_name ,
    a.shop_code,
    shop_name,
    a.product_code,
    a.des_specific_product_status,
    a.product_status_name,
    a.create_date,
    goods_name,
    unit_name,
    department_id,
    department_name,
    category_small_code,
    category_small_name,
    classify_large_code,
    classify_large_name
from 
(select  a.shop_code,
    a.product_code,
    a.des_specific_product_status,
    a.product_status_name,
    a.create_date,
    b.goods_code as sale_goods,
    entry_goods 
from 
(select  a.shop_code,
    a.product_code,
    a.des_specific_product_status,
    a.product_status_name,
    a.create_date,
   -- b.goods_code as sale_goods,
    c.goods_code as entry_goods 
from csx_tmp.temp_goods_sps_00 a
left join 
csx_tmp.temp_goods_sps_02 c on a.shop_code=c.dc_code and a.product_code=c.goods_code
where c.goods_code is null 
) a 
left join 
csx_tmp.temp_goods_sps_01 b on  a.product_code =b.goods_code
where b.goods_code is null 
)a 
join
(select goods_id,
    goods_name,
    unit_name,
    department_id,
    department_name,
    category_small_code,
    category_small_name,
    classify_large_code,
    classify_large_name
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt='current' 
)b on a.product_code=b.goods_id 
join
(select shop_id,shop_name,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current')d on a.shop_code=d.shop_id

;
