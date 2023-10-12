--
with aa as (select
basic_performance_region_code,
  basic_performance_region_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  create_order_by,
  recep_order_by,
  delivery_type_code,
  order_type_code,
  order_code,
  goods_code,
  
  sdt
from
  csx_dwd.csx_dwd_oms_sale_order_detail_di a
    join (
    select
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      basic_performance_region_code,
      basic_performance_region_name,
      shop_code,
      shop_name
    from
        csx_dim.csx_dim_shop
    where
      sdt = 'current'
     -- and purpose in('01','03','02','07','08')
  ) b on a.inventory_dc_code=b.shop_code
where
  sdt >= '20230901'
  and sdt<='20231009'
  and order_channel_code=1          -- 来源渠道: 1-b端  2-m端  3-bbc
 --  and order_business_type_code=1
 -- and inventory_dc_code = 'W0A3'
group by
  basic_performance_region_code,
  basic_performance_region_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  create_order_by,
  recep_order_by,
  delivery_type_code,
  order_type_code,
  order_code,
  goods_code,
  sdt
  ) 
  select
  substr(sdt,1,6),
  basic_performance_region_code,
  basic_performance_region_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  create_order_by,
  create_by_name,
 -- recep_order_by,
  user_number,
  user_name,
  case when delivery_type_code= 3 then '自提' when delivery_type_code=2 then '直送' else '配送' end delivery_type,    -- 配送类型编码：1-配送 2-直送 3-自提
  case when order_type_code=1 then '1-正常销售单' when order_type_code=2 then '2-紧急补货单' when order_type_code=3 then '3-城镇服务商过机单' else '其他' end order_type_name,                                                       -- 订单类型：1-正常销售单  2-紧急补货单 3-城镇服务商过机单
  count(distinct order_code) order_cn,
  count(goods_code) sku 
from aa a
left join 
(select user_id,user_name,user_number from csx_dim.csx_dim_uc_user where sdt='current' )  b on a.recep_order_by=b.user_id
left join 
(select user_id,user_name as create_by_name from csx_dim.csx_dim_uc_user where sdt='current' )  c on a.create_order_by=c.user_id
group by
  basic_performance_region_code,
  basic_performance_region_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  customer_code,
  customer_name,
  user_number,
  user_name,
  sub_customer_code,
  sub_customer_name,
  create_order_by,
  create_by_name,
  recep_order_by,
  case when delivery_type_code= 3 then '自提' when delivery_type_code=2 then '直送' else '配送' end ,
  case when order_type_code=1 then '1-正常销售单' when order_type_code=2 then '2-紧急补货单' when order_type_code=3 then '3-城镇服务商过机单' else '其他' end,
  substr(sdt,1,6)