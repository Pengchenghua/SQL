set hive.map.aggr = true;
set hive.groupby.skewindata=false;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.job.queuename=caishixian;

-- 过滤彩食鲜物流入库数据
drop table b2b_tmp.tmp_ship_order_sap_v1;
create temporary table b2b_tmp.tmp_ship_order_sap_v1 
as 
select 
  t.*,
  t1.shop_name as shop_out_name
from 
(
  select 
     *
  from csx_dw.shop_m 
  where sdt = 'current' and sales_belong_flag in ('4_企业购', '5_彩食鲜')
)t1 
 join 
(
  select 
    *
  from b2b.ord_orderflow_t 
  where sdt >= regexp_replace(date_sub(current_date, 4), '-', '')
    and length(shop_id_out) = 4 
)t on t.shop_id_out = t1.shop_id;

-- 保存出库订单数据
insert overwrite table csx_dw.dws_wms_r_d_shipped_order_all_detail partition(sdt, sys) 
select 
  concat('N', id) as id,
  batch_code,
  order_no,
  link_scm_order_no,
  goods_code,
  goods_bar_code,
  goods_name,
  unit,
  sale_unit,
  plan_qty,
  order_shipped_qty,
  shipped_qty,
  receive_qty,
  price,
  amount,
  shipped_location_code,
  shipped_location_name,
  run_type,
  tax_type,
  tax_rate,
  tax_code,
  price_type,
  source_system,
  shipped_type,
  business_type,
  return_flag,
  shipper_code,
  shipper_name,
  supplier_code,
  supplier_name,
  customer_code,
  customer_name,
  sub_customer_code,
  sub_customer_address,
  distribute_shortage_flag,
  all_received_flag,
  origin_order_no,
  external_order_no,
  send_time,
  sale_channel,
  compensation_type,
  plan_date,
  order_type,
  sdt,
  'new' as sys
from csx_dw.dwd_wms_r_d_shipped_order_detail
where sdt = regexp_replace(date_sub(current_date, 4), '-', '') 
union all 
select 
  concat('O',pur_doc_id, t.sdt,goodsid) as id,
  '' as batch_code,
  pur_doc_id as order_no,
  pur_app_id as link_scm_order_no,
  goodsid as goods_code,
  t2.bar_code as goods_bar_code,
  t2.goods_name,
  t2.unit,
  '' as sale_unit,
  t.purchase_qty as plan_qty,
  t.pur_qty_out as order_shipped_qty,
  t.pur_qty_out as shipped_qty,
  t.recpt_qty as receive_qty,
  cast(pur_doc_net_price*(1+coalesce(taxrate, 0)/100) as decimal(10, 6)) as price,
  t.tax_pur_val_out as amount,
  t.shop_id_out as shipped_location_code,
  t.shop_out_name as shipped_location_name,
  99 as run_type,
  2 as tax_type,
  taxrate as tax_rate,
  '' as  tax_code,
  99 as price_type,
  '' as source_system,
  '' as shipped_type,
  pur_doc_type as business_type,
  '' as return_flag,
  'YHCSX' as shipper_code,
  '永辉彩食鲜' as shipper_name,
  t.vendor_id as supplier_code,
  t3.vendor_name as supplier_name,
  t.acct_id as customer_code,
  '' as customer_name,
  '' as sub_customer_code,
  '' as sub_customer_address,
  '' as distribute_shortage_flag,
  1 as all_received_flag,
  org_doc_id as origin_order_no,
  '' as external_order_no,
  min_pstng_date_out as send_time,
  '' as sale_channel,
  '' as compensation_type,
  plan_delivery_date as plan_date,
  '' as order_type,
  t.sdt,
  'old' as sys
from
(
  select * from b2b_tmp.tmp_ship_order_sap_v1  where shop_id_out<>'W098'
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