set mapreduce.job.queuename=caishixian;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.support.quoted.identifiers=none;

-- 入库订单未关单入库单头信息
drop table b2b_tmp.tmp_entry_order_header;
create temporary table b2b_tmp.tmp_entry_order_header 
as 
select 
  a.*
from 
(
  select 
    `(sdt)?+.+`
  from csx_dw.wms_entry_order_header 
  where sdt = 'now'
)a left outer join 
(
  select 
    *
  from csx_ods.wms_entry_order_header_ods 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)b on a.id = b.id 
where b.id is null
union all
select 
  a.*
from 
(
  select 
    `(sdt)?+.+`
  from csx_ods.wms_entry_order_header_ods 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)a left outer join csx_dw.wms_entry_order_history b 
  on a.id = b.header_id
where b.header_id is null;

-- 未关单入库订单明细记录
drop table b2b_tmp.tmp_entry_order_item;
create temporary table b2b_tmp.tmp_entry_order_item 
as 
select 
  a.*
from 
(
  select 
    `(close_time|receive_status|sdt)?+.+`
  from csx_dw.wms_entry_order_item 
  where sdt = 'now'
)a left outer join 
(
  select 
    *
  from csx_ods.wms_entry_order_item_ods 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)b on a.id = b.id 
where b.id is null
union all
select 
  a.*
from 
(
  select 
    `(sdt)?+.+`
  from csx_ods.wms_entry_order_item_ods 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)a left outer join csx_dw.wms_entry_order_history b 
  on a.id = b.order_id
where b.order_id is null;


-- 入库订单未关单入库批次信息
drop table b2b_tmp.tmp_entry_batch_detail;
create temporary table b2b_tmp.tmp_entry_batch_detail 
as 
select 
  a.*
from 
(
  select 
    `(close_time|receive_status|sdt)?+.+`
  from csx_dw.wms_entry_batch_detail 
  where sdt = 'now'
)a left outer join 
(
  select 
    *
  from csx_ods.wms_entry_batch_detail_ods 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)b on a.id = b.id 
where b.id is null
union all
select 
  a.*
from 
(
  select 
    `(sdt)?+.+`
  from csx_ods.wms_entry_batch_detail_ods 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)a left outer join csx_dw.wms_entry_order_history b 
  on a.id = b.id
where b.id is null;

-- 入库单未关单订单详情信息
drop table b2b_tmp.tmp_entry_order_detail;
create temporary table b2b_tmp.tmp_entry_order_detail 
as 
select 
  c.id,
  a.id as header_id,
  b.id as order_id,
  a.order_code,
  c.batch_code,
  b.product_code as goods_code,
  b.product_bar_code as goods_bar_code,
  b.product_name as goods_name,
  b.unit,
  b.produce_date,
  b.plan_qty,
  coalesce(c.receive_qty, b.receive_qty) as receive_qty,
  b.shipped_qty,
  b.shelf_qty,
  b.pass_qty,
  b.reject_qty,
  coalesce(c.price, b.price) as price,
  b.add_price_percent,
  coalesce(c.amount, b.amount) as amount,
  a.direct_flag,
  b.direct_price,
  b.direct_amount,
  a.shipper_code,
  a.shipper_name,
  a.supplier_code,
  a.supplier_name,
  a.send_location_code,
  a.send_location_name,
  a.plan_receive_date,
  a.entry_type,
  a.return_flag,
  a.super_class,
  b.location_code as receive_location_code,
  b.location_name as receive_location_name,
  b.reservoir_area_code as receive_area_code,
  b.reservoir_area_name as receive_area_name,
  b.store_location_code as receive_store_location_code,
  b.store_location_name as receive_store_location_name,
  b.shelf_store_location_type,
  b.shelf_reservoir_area_code as shelf_area_code,
  b.shelf_reservoir_area_name as shelf_area_name,
  b.shelf_store_location_code,
  b.shelf_store_location_name,
  a.receive_status,
  a.shelf_status,
  a.all_receive_flag,
  a.all_shelf_flag,
  a.print_times,
  a.link_operate_order_code,
  a.origin_order_code,
  a.link_order_code,
  a.receive_time,
  a.close_time,
  a.close_by,
  a.auto_status,
  a.sale_channel,
  a.compensation_type,
  a.outside_order_code,
  a.settlement_dc,
  a.settlement_dc_name,
  b.business_type as run_type,
  a.business_type,
  b.assess_type,
  b.assess_type_name,
  b.tax_type,
  b.tax_rate,
  b.tax_code,
  b.price_type,
  a.source_system,
  a.create_time,
  a.create_by,
  a.update_time,
  a.update_by
from b2b_tmp.tmp_entry_order_item b
left outer join b2b_tmp.tmp_entry_order_header a 
  on a.order_code = b.order_code and a.receive_location_code = b.location_code
left outer join b2b_tmp.tmp_entry_batch_detail c 
  on b.order_code = c.order_code and b.location_code = c.location_code and b.product_code = c.product_code;

-- 保存入库单明细
insert overwrite table csx_dw.wms_entry_order_m partition(sdt)
select 
  a.*,
  case when a.receive_status = 2 and close_time >= date_sub(current_date, 1) 
      then regexp_replace(split(close_time, ' ')[0], '-', '') 
    when a.receive_status = 2 and close_time < date_sub(current_date, 1) 
      then 'repair' 
    when a.receive_status = 3 then regexp_replace(split(update_time, ' ')[0], '-', '')
    else 'now' end as sdt
from b2b_tmp.tmp_entry_order_detail a
union all
select 
  *
from csx_dw.wms_entry_order_m 
where sdt in (regexp_replace(date_sub(current_date, 1), '-', ''), 
    regexp_replace(to_date(current_date), '-', ''), 'repair');

-- 更新入库单历史表
insert overwrite table csx_dw.wms_entry_order_history 
select 
  *
from csx_dw.wms_entry_order_history
union all
select 
  a.id,
  a.header_id,
  a.order_id,
  case when a.receive_status = 2 and close_time >=  date_sub(current_date, 1) 
      then regexp_replace(split(close_time, ' ')[0], '-', '') 
    when a.receive_status = 2 and close_time < date_sub(current_date, 1) 
      then 'repair' 
    else regexp_replace(split(update_time, ' ')[0], '-', '') end as sdt
from b2b_tmp.tmp_entry_order_detail a
where a.receive_status in (2, 3);

-- 保存入库单头信息
insert overwrite table csx_dw.wms_entry_order_header partition(sdt) 
select 
  a.*,
  case when a.receive_status = 2 and close_time >= date_sub(current_date, 1) 
      then regexp_replace(split(close_time, ' ')[0], '-', '') 
    when a.receive_status = 2 and close_time < date_sub(current_date, 1) 
      then 'repair' 
    when a.receive_status = 3 then regexp_replace(split(update_time, ' ')[0], '-', '')
    else 'now' end as sdt
from b2b_tmp.tmp_entry_order_header a
union all
select 
  *
from csx_dw.wms_entry_order_header 
where sdt in (regexp_replace(date_sub(current_date, 1), '-', ''), 
    regexp_replace(to_date(current_date), '-', ''), 'repair');

-- 保存入库单明细信息
insert overwrite table csx_dw.wms_entry_order_item partition(sdt) 
select 
  a.*,
  b.close_time,
  b.receive_status,
  case when b.receive_status = 2 and b.close_time >= date_sub(current_date, 1) 
      then regexp_replace(split(b.close_time, ' ')[0], '-', '') 
    when b.receive_status = 2 and b.close_time < date_sub(current_date, 1) 
      then 'repair' 
    when b.receive_status = 3 then regexp_replace(split(b.update_time, ' ')[0], '-', '')
    else 'now' end as sdt
from b2b_tmp.tmp_entry_order_item a 
left outer join b2b_tmp.tmp_entry_order_header b 
on a.order_code = b.order_code and b.receive_location_code = a.location_code
union all 
select 
  *
from csx_dw.wms_entry_order_item 
where sdt in (regexp_replace(date_sub(current_date, 1), '-', ''), 
    regexp_replace(to_date(current_date), '-', ''), 'repair');


-- 保存入库单批次信息
insert overwrite table csx_dw.wms_entry_batch_detail partition(sdt) 
select 
  a.*,
  b.close_time,
  b.receive_status,
  case when b.receive_status = 2 and b.close_time >= date_sub(current_date, 1) 
      then regexp_replace(split(b.close_time, ' ')[0], '-', '') 
    when b.receive_status = 2 and b.close_time < date_sub(current_date, 1) 
      then 'repair' 
    when b.receive_status = 3 then regexp_replace(split(b.update_time, ' ')[0], '-', '')
    else 'now' end as sdt
from b2b_tmp.tmp_entry_batch_detail a 
left outer join b2b_tmp.tmp_entry_order_header b 
  on a.order_code = b.order_code and b.receive_location_code = a.location_code 
union all
select 
  *
from csx_dw.wms_entry_batch_detail 
where sdt in (regexp_replace(date_sub(current_date, 1), '-', ''), 
    regexp_replace(to_date(current_date), '-', ''), 'repair');