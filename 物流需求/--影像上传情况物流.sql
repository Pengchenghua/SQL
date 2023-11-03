  -- 汇总影像数据
    with aa as (select
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
  sdt >= '20230101'
--  and sdt <= '20231016'
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
  ),
 bb as 
 (select sdt, a.order_code  ,b.source_type b_source_type,if(c.source_type=0,1,0) c_source_type  -- WMS 标识
from   
(select sdt,order_code  
    from       csx_dwd.csx_dwd_oms_sale_order_receipt_proofs_info_di 
where sdt>='20231001'
    and status=1
group by sdt,order_code
) a 
 left join 
 (select order_code  ,source_type
from     csx_dwd.csx_dwd_oms_sale_order_receipt_proofs_info_di 
where sdt>='20231001' and source_type=1
group by order_code,source_type
) b on a.order_code=b.order_code 
left join 
(select order_code  ,source_type
from     csx_dwd.csx_dwd_oms_sale_order_receipt_proofs_info_di 
where sdt>='20231001' and source_type=0
group by order_code,source_type
)c on a.order_code=c.order_code 
)
  select  
  basic_performance_region_code,
  coalesce(basic_performance_region_name,'总计')basic_performance_region_name,
  basic_performance_province_code,
  coalesce(basic_performance_province_name,'大区合计')basic_performance_province_name,
  basic_performance_city_code,
  coalesce(basic_performance_city_name,'')basic_performance_city_name,
  (current_receipt_order_cn+current_tms_order_cn+ current_repeat_order_cn) current_all_order_cn,
  current_tms_order_cn,
  current_tms_order_cn/(current_receipt_order_cn+current_tms_order_cn+ current_repeat_order_cn) current_tms_ratio,
  current_receipt_order_cn,
  current_receipt_order_cn/(current_receipt_order_cn+current_tms_order_cn+ current_repeat_order_cn) as current_peceipt_ratio,
  current_repeat_order_cn,
  current_repeat_order_cn/(current_receipt_order_cn+current_tms_order_cn+ current_repeat_order_cn) as current_repeat_ratio,
  (tms_order_cn+receipt_order_cn+ repeat_order_cn)  all_order_cn,
  tms_order_cn,
  tms_order_cn/(tms_order_cn+receipt_order_cn+ repeat_order_cn)   tms_ratio,
  receipt_order_cn,
  receipt_order_cn/(tms_order_cn+receipt_order_cn+ repeat_order_cn)   as receipt_ratio,
  repeat_order_cn,
  repeat_order_cn/(tms_order_cn+receipt_order_cn+ repeat_order_cn)   as repeat_ratio,
  AA
  from (
  select  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name,
 -- count(case when (c_source_type =1 and b_source_type=1) and sdt='20231016' then order_code end )+  count(case when c_source_type =1  and sdt='20231016'  then order_code end ) +count(case when b_source_type=1  and sdt='20231016'  then order_code end )+count(case when c_source_type =1 and b_source_type=1  and sdt='20231016'  then order_code end ) as current_all_order_cn,
  count(case when c_source_type =1  and sdt='20231016'  then order_code end ) as current_receipt_order_cn,
  count(case when b_source_type=1  and sdt='20231016'  then order_code end ) as current_tms_order_cn,
  count(case when c_source_type =1 and b_source_type=1  and sdt='20231016'  then order_code end ) as current_repeat_order_cn,
 -- count(case when (c_source_type =1 and b_source_type=1) then order_code end )+  count(case when c_source_type =1  then order_code end ) +count(case when b_source_type=1 then order_code end )+count(case when c_source_type =1 and b_source_type=1  then order_code end ) as all_order_cn,
  count(case when c_source_type =1  then order_code end ) as receipt_order_cn,
  count(case when b_source_type=1 then order_code end ) as tms_order_cn,
  count(case when c_source_type =1 and b_source_type=1 then order_code end ) as repeat_order_cn,
  GROUPING__ID AA  
  from
  (
select
  basic_performance_region_code,
  basic_performance_region_name,
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  inventory_dc_code,
  shop_name,
  aa.order_code,
  receipt_proofs_flag,
  confirm_proofs_flag,
  tms_confirm_proofs_flag,
  b_source_type,
  c_source_type,
  bb.sdt
from bb 
left join  aa on aa.order_code=bb.order_code 
where bb.sdt='20231016'
    and basic_performance_region_code!=''
   -- and basic_performance_province_name='广东深圳'
 ) a 
 group by  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name
 grouping sets (
 ( basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  basic_performance_region_code,
  basic_performance_region_name),
  ( 
  basic_performance_region_code,
  basic_performance_region_name)
  ,
  ()
  )
  ) a
  order by  case when basic_performance_region_code ='2' then 1 when basic_performance_region_code='4' then 2  when basic_performance_region_code='3' then 3 when basic_performance_region_code='1' then 4 when basic_performance_region_code='8' then 5 else 6 end ,
   case when basic_performance_province_name in ('福建','北京','贵州','江苏南京','安徽')  then 1 
        when basic_performance_province_name in ('广东深圳','河北','四川','江苏苏州','河南') then 2
        when basic_performance_province_name in ('江西','陕西','重庆','上海松江','湖北')then 3 else 4 end ,
      case when basic_performance_city_name in('莆田','重庆区','宁波') then 1 when basic_performance_city_name in('南平','黔江区','台州') then 2 when basic_performance_city_name in('福州','杭州','万州区') then 3 when basic_performance_city_name in( '泉州','舟山') then 4 when basic_performance_city_name='三明' then 5 when basic_performance_city_name='龙岩' then 6 when basic_performance_city_name='厦门' then 7 when basic_performance_city_name='宁德' then 8 else 9 end 
     ;


-- 影像上传明细明细

  with aa as (select
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
  sdt,
  delivery_type_code,
  to_date(delivery_time)delivery_time,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  receipt_date
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
  sdt >= '20230101'
--  and sdt <= '20231012'
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
  sdt,
  delivery_type_code,
   to_date(delivery_time) ,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_name,
  receipt_date
  ),
 bb as
 (select a.order_code  ,b.source_type b_source_type ,sdt,create_date,
  if(c.source_type=0,1,0) c_source_type -- wms
from   
(select order_code  ,to_date(create_time) create_date,sdt
    from csx_dwd.csx_dwd_oms_sale_order_receipt_proofs_info_di 
where sdt>='20231001'
    and status=1
group by order_code,to_date(create_time),sdt
) a 
 left join 
 -- TMS回单标识
 (select order_code  ,source_type
from     csx_dwd.csx_dwd_oms_sale_order_receipt_proofs_info_di 
where sdt>='20231001' 
  and source_type=1
group by order_code,source_type
) b on a.order_code=b.order_code 
left join 
-- TMS回单标识
(select order_code  ,source_type
from     csx_dwd.csx_dwd_oms_sale_order_receipt_proofs_info_di 
where sdt>='20231001' 
  and source_type=0
group by order_code,source_type
)c on a.order_code=c.order_code 
),
cc as 
(
select
  shipped_order_code,
  supplier_code,
  supplier_name,
  driver_name,
  aa 
from 
(
select
  shipped_order_code,
  supplier_code,
  supplier_name,
  driver_name,
  row_number()over(partition by shipped_order_code order by  update_time desc ) aa 
from
  csx_dwd.csx_dwd_tms_sign_shipped_order_detail_di
where
  sdt >= '20230101'
)a 
where aa =1 )
select
 -- basic_performance_region_code,
  basic_performance_region_name,
--  basic_performance_province_code,
  basic_performance_province_name,
--  basic_performance_city_code,
  basic_performance_city_name,
  inventory_dc_code,
  shop_name,
  bb.order_code,
  coalesce(b_source_type,0)b_source_type,
  coalesce(c_source_type,0)c_source_type,
  supplier_code,
  supplier_name,
  driver_name,
  bb.create_date,
  delivery_time,
  customer_code,
  regexp_replace(customer_name,'\n|\s','')  customer_name,
  sub_customer_code,
  regexp_replace(sub_customer_name,'\n|\s','') sub_customer_name,
  receipt_date
from bb 
left join  aa on bb.order_code=aa.order_code 
left join  cc on bb.order_code=cc.shipped_order_code 
where delivery_type_code=1
    and bb.sdt<='20231016'
 ;




-- 司机回单统计




CREATE  TABLE `csx_analyse`.`csx_analyse_fr_wms_image_upload_statis_di`(
  `basic_performance_region_code` string comment '大区编码', 
  `basic_performance_region_name` string comment '大区名称', 
  `basic_performance_province_code` string comment '省区编码', 
  `basic_performance_province_name` string comment '省区名称', 
  `basic_performance_city_code` string comment '城市编码', 
  `basic_performance_city_name` string comment '城市名称', 
  `dc_code` string comment 'DC编码', 
  `dc_name` string comment 'DC名称', 
  `order_code` string comment '订单号', 
  `tms_source_type` int comment 'TMS司机回单标识 1是 0 否', 
  `wms_source_type` int comment 'WMS回单标识 1是 0 否', 
  `supplier_code` string comment '承运商编码', 
  `supplier_name` string comment '承运商名称', 
  `driver_name` string comment '司机名称', 
  `create_date` string comment '上传日期/创建日期' , 
  `delivery_time` string comment '送货日期', 
  `customer_code` string comment '主客户编码', 
  `customer_name` string comment '主客户名称', 
  `sub_customer_code` string comment '子客户编码', 
  `sub_customer_name` string comment '子客户名称', 
  `receipt_date` int comment '客户回单周期')
  comment 'WMS影像上传明细'
  partitioned by (sdt string comment '上传日期创建分区')
STORED AS parquet 

