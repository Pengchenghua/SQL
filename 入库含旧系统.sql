set hive.map.aggr = true;
set hive.groupby.skewindata=false;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.job.queuename=caishixian;

-- 过滤彩食鲜物流入库数据
drop table b2b_tmp.tmp_entry_order_sap_v1;
create temporary table b2b_tmp.tmp_entry_order_sap_v1 
as 
select 
  t.*,
  t1.shop_name as shop_in_name
from 
(
  select 
    *
  from b2b.ord_orderflow_t 
  where sdt >= regexp_replace(date_sub(current_date, 4), '-', '') 
    and length(shop_id_in) = 4 
)t join 
(
  select 
     *
  from csx_dw.shop_m 
  where sdt = 'current' and sales_belong_flag in ('4_企业购', '5_彩食鲜')
)t1 on t.shop_id_in = t1.shop_id;

insert overwrite table csx_dw.dws_wms_r_d_entry_order_all_detail partition(sdt, sys) 
select 
  concat('N', id) as id,
  order_code,
  batch_code,
  goods_code,
  goods_bar_code,
  goods_name,
  unit,
  produce_date,
  plan_qty,
  receive_qty,
  shipped_qty,
  shelf_qty,
  pass_qty,
  reject_qty,
  price,
  add_price_percent,
  amount,
  direct_flag,
  direct_price,
  direct_amount,
  shipper_code,
  shipper_name,
  supplier_code,
  supplier_name,
  send_location_code,
  send_location_name,
  plan_receive_date,
  entry_type,
  return_flag,
  super_class,
  receive_location_code,
  receive_location_name,
  receive_area_code,
  receive_area_name,
  receive_store_location_code,
  receive_store_location_name,
  shelf_store_location_type,
  shelf_area_code,
  shelf_area_name,
  shelf_store_location_code,
  shelf_store_location_name,
  receive_status,
  shelf_status,
  all_receive_flag,
  all_shelf_flag,
  print_times,
  link_operate_order_code,
  origin_order_code,
  link_order_code,
  receive_time,
  close_time,
  close_by,
  auto_status,
  sale_channel,
  compensation_type,
  outside_order_code,
  settlement_dc,
  settlement_dc_name,
  run_type,
  business_type,
  assess_type,
  assess_type_name,
  tax_type,
  tax_rate,
  tax_code,
  price_type,
  source_system,
  create_time,
  create_by,
  update_time,
  update_by,
  sdt,
  'new' as sys
from csx_dw.dwd_wms_r_d_entry_order_detail 
where sdt = regexp_replace(date_sub(current_date, 1), '-', '') 
union all 
select 
  concat('O',pur_doc_id, t.sdt,goodsid) as id,
  pur_doc_id_app as order_code,
  '' as batch_code,
  goodsid as goods_code,
  t2.bar_code as goods_bar_code,
  t2.goods_name,
  t2.unit,
  '' as produce_date,
  pur_qty_in as plan_qty,
  recpt_qty as receive_qty,
  qty_shipped as shipped_qty,
  0.0 as shelf_qty,
  0.0 as pass_qty,
  0.0 as reject_qty,
  cast(pur_doc_net_price*(1+coalesce(taxrate, 0)/100) as decimal(10, 6)) as price,
  0.0 as add_price_percent,
  tax_pur_val_in as amount,
  '' as direct_flag,
  0.0 as direct_price,
  0.0 as direct_amount,
  'YHCSX' as shipper_code,
  '永辉彩食鲜' as shipper_name,
  t.vendor_id as supplier_code,
  t3.vendor_name as supplier_name,
  shop_id_out as send_location_code,
  t4.shop_name as send_location_name,
  plan_delivery_date as plan_receive_date,
  order_type as entry_type,
  '' as return_flag,
  1 as super_class,
  shop_id_in as receive_location_code,
  shop_in_name as receive_location_name,
  '' as receive_area_code,
  '' as receive_area_name,
  '' as receive_store_location_code,
  '' as receive_store_location_name,
  '' as shelf_store_location_type,
  '' as shelf_area_code,
  '' as shelf_area_name,
  '' as shelf_store_location_code,
  '' as shelf_store_location_name,
  2 as receive_status,
  2 as shelf_status,
  1 as all_receive_flag,
  1 as all_shelf_flag,
  0 as print_times,
  pur_doc_id_app as link_operate_order_code,
  org_doc_id as origin_order_code,
  pur_doc_id as link_order_code,
  min_pstng_date_in as receive_time,
  '' as close_time,
  '' as close_by,
  0 as auto_status,
  99 as sale_channel,
  99 as compensation_type,
  '' as outside_order_code,
  shop_id_in as settlement_dc,
  shop_in_name as settlement_dc_name,
  99 as run_type,
  pur_doc_type as business_type,
  '' as assess_type,
  '' as assess_type_name,
  1 as tax_type,
  taxrate as tax_rate,
  '' as tax_code,
  99 as price_type,
  '' as source_system,
  '' as create_time,
  '' as create_by,
  '' as update_time,
  '' as update_by,
  t.sdt as sdt,
  'old' as sys
from
(
  select * from b2b_tmp.tmp_entry_order_sap_v1  where shop_id_in<>'W098'
)t
left outer join 
(
  select 
    *
  from csx_dw.goods_m 
  where sdt = 'current'
)t2 on t.goodsid = t2.goods_id
left outer join 
(
  select 
    *
  from csx_dw.vendor_m 
  where sdt = 'current' 
)t3 on regexp_replace(t.vendor_id, '^0*', '') = t3.vendor_id
left outer join 
(
  select 
    *
  from csx_dw.shop_m 
  where sdt = 'current'
)t4 on t.shop_id_out = t4.shop_id;