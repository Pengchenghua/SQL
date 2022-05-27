

-- 任务名称
set mapred.job.name=dws_wms_r_d_entry_detail;

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =10000;
set hive.exec.max.dynamic.partitions.pernode =10000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.snappycodec;
set mapred.output.compression.type=block;
set parquet.compression=snappy;

-- 旧入库明细表
set source_wms_old_entry_order = b2b.ord_orderflow_t;

-- 门店信息表
set source_basic_shop = csx_dw.dws_basic_w_a_csx_shop_m;

-- 商品信息表
set source_basic_goods = csx_dw.dws_basic_w_a_csx_product_m;

-- 供应商表
set source_supplier_info = csx_dw.dws_basic_w_a_csx_supplier_m;

-- 物流业务类型表
set source_wms_business_type = csx_dw.dws_wms_w_a_business_type;

-- 入库明细结果表
set target_wms_entry_detail = csx_dw.dws_wms_r_d_entry_detail;


-- 彩食鲜门店数据
with csx_shop_info as 
(
  select 
    shop_id as shop_code,
    shop_name,
    province_code,
    province_name,
    city_code,
    city_name,
    purpose,
    purpose_name
  from ${hiveconf:source_basic_shop} 
  where sdt = 'current' and table_type = 1
),
-- 商品数据
goods_info as 
(
  select 
    goods_id as goods_code,
    goods_name,
    bar_code as goods_bar_code,
    standard as spec,
    unit_name as unit,
    division_code,
    division_name,
    category_large_code, 
    category_large_name, 
    category_middle_code,
    category_middle_name,
    category_small_code, 
    category_small_name, 
    department_id as department_code,
    department_name,
    brand as brand_code,
    brand_name
  from ${hiveconf:source_basic_goods} 
  where sdt = 'current'
),
-- 业务类型
business_type as 
(
  select distinct 
    business_type_code,
    business_type_name
  from ${hiveconf:source_wms_business_type} 
),
supplier_info as 
(
  select 
    vendor_id as supplier_code,
    vendor_name as supplier_name
  from ${hiveconf:source_supplier_info} 
  where sdt = 'current'
),
csx_old_entry as 
(
  select 
    pur_doc_id as order_code,
    goodsid as goods_code,
    regexp_replace(vendor_id, '^0*', '') as supplier_code,
    shop_id_out as send_location_code,
    from_unixtime(UNIX_TIMESTAMP(plan_delivery_date,'yyyyMMdd'),'yyyy-MM-dd') as plan_receive_date,
    order_type as order_type_code,
    ordertype as order_type_name,
    t1.shop_code as receive_location_code,
    pur_doc_id_app as link_operate_order_code,
    org_doc_id as origin_order_code,
    pur_doc_id as link_order_code,
    from_unixtime(UNIX_TIMESTAMP(min_pstng_date_in,'yyyyMMdd'),'yyyy-MM-dd') as receive_time,
    shop_id_in as settlement_dc,
    pur_doc_type as business_type_code,
    taxrate as tax_rate,
    t1.province_code,
    t1.province_name,
    t1.city_code,
    t1.city_name,
    t1.purpose,
    t1.purpose_name,
    t1.shop_name as receive_location_name,
    sdt,
    sum(pur_order_qty) as plan_qty,
    sum(pur_qty_in) as receive_qty,
    sum(pur_qty_out) as shipped_qty,
    cast(sum(cast(pur_doc_net_price*(1+coalesce(taxrate, 0)/100) as decimal(20, 6)) * pur_qty_in)/sum(pur_qty_in) as decimal(20, 6)) as price,
    sum(tax_pur_val_in) as amount
from csx_shop_info t1 join 
  (
    select 
      *
    from ${hiveconf:source_wms_old_entry_order} 
    where (sdt >= '20191001' and sdt<'20200101') and shop_id_in is not null and trim(shop_id_in) <> '' 
      and (order_type is null or order_type <> '退货') and shop_id_in <> 'W098'
      and pur_org like 'P6%'
  )t2 on t1.shop_code = case when shop_id_in like '99%' then shop_id_in 
      when shop_id_in like '9%' then concat('E', substr(shop_id_in, 2, 3)) 
      else shop_id_in end
  group by pur_doc_id, goodsid, regexp_replace(vendor_id, '^0*', ''),
    shop_id_out,
    plan_delivery_date,
    order_type,
    ordertype,
    t1.shop_code,
    pur_doc_id_app,
    org_doc_id,
    pur_doc_id,
    min_pstng_date_in,
    shop_id_in,
    pur_doc_type,
    taxrate,
    t1.province_code,
    t1.province_name,
    t1.city_code,
    t1.city_name,
    t1.purpose,
    t1.purpose_name,
    t1.shop_name,
    sdt
)
-- 保存就系统入库明细数据
insert overwrite table ${hiveconf:target_wms_entry_detail} partition (sdt, sys)
select 
  concat('O',t1.order_code, t1.sdt, t1.goods_code) as id,
  t1.order_code,
  t1.goods_code,
  t3.goods_bar_code,
  t3.goods_name,
  t3.spec,
  t3.unit,
  t3.division_code,
  t3.division_name,
  t3.category_large_code,
  t3.category_large_name,
  t3.category_middle_code,
  t3.category_middle_name,
  t3.category_small_code,
  t3.category_small_name,
  t3.department_code,
  t3.department_name,
  t3.brand_code,
  t3.brand_name,
  '' as produce_date,
  'YHCSX' as shipper_code,
  '永辉彩食鲜' as shipper_name,
  t1.supplier_code,
  coalesce(t5.supplier_name, '') as supplier_name,
  t1.send_location_code,
  '' as send_location_name,
  t1.province_code,
  t1.province_name,
  t1.city_code,
  t1.city_name,
  t1.purpose,
  t1.purpose_name,
  t1.receive_location_code,
  t1.receive_location_name,
  '' as receive_area_code,
  '' as receive_area_name,
  '' as receive_store_location_code,
  '' as receive_store_location_name,
  '' as shelf_store_location_type,
  '' as shelf_area_code,
  '' as shelf_area_name,
  '' as shelf_store_location_code,
  '' as shelf_store_location_name,
  '' as reservation_begin,
  '' as reservation_end,
  t1.plan_receive_date,
  t1.receive_time,
  '' as close_time,
  '' as post_time,
  '' as close_by,
  t1.plan_qty,
  t1.receive_qty,
  0 as gift_qty,
  t1.shipped_qty,
  0.0 as shelf_qty,
  0.0 as pass_qty,
  0.0 as reject_qty,
  coalesce(cast(case when receive_qty = 0 then price
    when price = 0 then amount / receive_qty 
    else price end as decimal(20,6)),0) as price,
  1 as tax_type,
  t1.tax_rate,
  '' as tax_code,
  99 as price_type,
  0.0 as add_price_percent,
  coalesce(cast(amount as decimal(20,6)),0) as amount,                              -- 取入库总金额，单价通过入库金额反算
  coalesce(cast(amount as decimal(20,6))/(1+t1.tax_rate/100),0) as amount_no_tax ,  -- 取入库总金额，单价通过入库金额反算
  '' as direct_flag,
  0.0 as direct_price,
  0.0 as direct_amount,
  t1.order_type_code,
  t1.business_type_code,
  t4.business_type_name,
  99 as sale_channel,
  99 as compensation_type,
  1 as super_class,
  '' as source_system,
  '' as return_flag,
  2 as receive_status,
  2 as shelf_status,
  t1.link_operate_order_code,
  t1.origin_order_code,
  t1.link_order_code,
  '' as outside_order_code,
  t1.settlement_dc,
  '' as settlement_dc_name,
  '' as cost_center_code,
  '' as cost_center_name,
  99 as run_type,
  '' as assess_type,
  '' as assess_type_name,
  0 as entity_flag,
  0 as auto_status,
  '' as create_time,
  '' as create_by,
  '' as update_time,
  '' as update_by,
  t1.order_type_name,
  null as package_number,
  '' as diff_reason,
  '' as return_reason,
  t1.sdt,
  'old' as sys
from csx_old_entry t1 
left outer join goods_info t3 on t1.goods_code = t3.goods_code
left outer join business_type t4 on t1.business_type_code = t4.business_type_code
left outer join supplier_info t5 on t1.supplier_code = t5.supplier_code
;