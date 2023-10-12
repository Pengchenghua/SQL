-- 物流接单明细
select
  basic_performance_region_code,
  basic_performance_region_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  inventory_dc_code,
  shop_name,
  order_code,
  receipt_proofs_flag,
  confirm_proofs_flag,
  tms_confirm_proofs_flag,
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
      and purpose in('01', '03', '02', '07', '08')
  ) b on a.inventory_dc_code = b.shop_code
where
  sdt >= '20231001'
  and sdt <= '20231007'
  and delivery_type_code = 1 -- 配送类型编码：1-配送 2-直送 3-自提
  and order_channel_code = 1 -- 来源渠道: 1-b端  2-m端  3-bbc
  and order_business_type_code = 1 -- 订单业务类型: 1-日配 2-福利 3-大宗贸易 4-内购
  and order_status_code in ('60', '70') -- 订单状态 60 已配送 70 已完成
  and order_channel_detail_code in ('11', '12') -- 订单细分 12 小程序  11中台
  -- and inventory_dc_code = 'W0A3'
group by
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name,
  inventory_dc_code,
  shop_name,
  order_code,
  receipt_proofs_flag,
  confirm_proofs_flag,
  tms_confirm_proofs_flag,
  sdt
  ;



  --
with aa as (select
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name,
  inventory_dc_code,
  shop_name,
  order_code,
  receipt_proofs_flag,
  confirm_proofs_flag,
  tms_confirm_proofs_flag,
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
      and purpose in('01','03','02','07','08')
  ) b on a.inventory_dc_code=b.shop_code
where
  sdt >= '20231001'
  and sdt<='20231007'
 -- and inventory_dc_code = 'W0A3'
group by
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name,
  inventory_dc_code,
  shop_name,
  order_code,
  receipt_proofs_flag,
  confirm_proofs_flag,
  tms_confirm_proofs_flag,
  sdt
  ) 
  select  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name,
  inventory_dc_code,
  shop_name,
  current_all_order_cn,
  current_tms_order_cn,
  current_tms_order_cn/current_all_order_cn current_tms_ratio,
  current_receipt_order_cn,
  current_receipt_order_cn/current_all_order_cn as current_peceipt_ratio,
  current_repeat_order_cn,
  current_repeat_order_cn/current_all_order_cn as current_repeat_ratio,
  all_order_cn,
  tms_order_cn,
  tms_order_cn/all_order_cn tms_ratio,
  receipt_order_cn,
  receipt_order_cn/all_order_cn as receipt_ratio,
  repeat_order_cn,
  repeat_order_cn/all_order_cn as repeat_ratio
  from (
  select  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name,
  inventory_dc_code,
  shop_name,
  count(case when (receipt_proofs_flag =1 or tms_confirm_proofs_flag=1) and sdt='20231007' then order_code end ) as current_all_order_cn,
  count(case when receipt_proofs_flag =1  and sdt='20231007'  then order_code end ) as current_receipt_order_cn,
  count(case when tms_confirm_proofs_flag=1  and sdt='20231007'  then order_code end ) as current_tms_order_cn,
  count(case when receipt_proofs_flag =1 and tms_confirm_proofs_flag=1  and sdt='20231007'  then order_code end ) as current_repeat_order_cn,
  count(case when ( receipt_proofs_flag =1 or tms_confirm_proofs_flag=1 )    then order_code end ) as all_order_cn,
  count(case when receipt_proofs_flag =1  then order_code end ) as receipt_order_cn,
  count(case when tms_confirm_proofs_flag=1 then order_code end ) as tms_order_cn,
  count(case when receipt_proofs_flag =1 and tms_confirm_proofs_flag=1 then order_code end ) as repeat_order_cn
  from aa 
  group by basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name,
  inventory_dc_code,
  shop_name
  ) a 