drop table csx_tmp.temp_stock;
create table csx_tmp.temp_stock as 
with current_stock as 
(
  select 
    t1.*
  from 
  (
    select 
    id,
    credential_item_id,
    product_code,
    location_code,
    posting_time,
    reservoir_area_code,
    after_qty,
    after_amt,
    after_price,
    batch_no,
    move_type,
    wms_order_type,
    max(id) over(partition by product_code, location_code,reservoir_area_code) as last_id
    from csx_dw.dwd_cas_r_d_accounting_stock_detail
    where 1=1
     -- move_type not in ('114A','','110A','115A','116A')
    and reservoir_area_code not in ('PD01','PD02','TS01')
     and sdt<='20210203'
    -- and product_code='1170062'
   -- and location_code='W0A7'
  )t1 where id = last_id
),
credential_item as 
(
  select 
    a.detail_id,
    a.reservoir_area_code,
    a.batch_no,
    a.product_code,
    qty,
    amt,
    price
  from csx_dw.dwd_cas_r_d_accounting_stock_log_item a 
  join 
  (select 
    max(a.id) max_id,
    a.reservoir_area_code,
    detail_id
  from csx_dw.dwd_cas_r_d_accounting_stock_log_item a
  where sdt<='20210203'
  group by detail_id ,reservoir_area_code) b on a.id=b.max_id
 
--   and batch_no='CB20200926016247'
--   and  product_code='1069272' 
--   and detail_id=1353154057740488704
)
,
entry_batch as 
(
  select distinct 
    product_code,
    batch_no,
    credential_no,
    to_date(input_time) as input_time,
    source_order_no,
    wms_order_type,
    wms_order_no,
    supplier_code,
    supplier_name,
    qty,
    price,
    amt
  from  csx_dw.dws_wms_r_d_entry_batch_detail
)
select 
  id,
  province_code,
  province_name,
   t1.location_code,
  shop_name,
  t1.reservoir_area_code ,
  name,
  t1.product_code,
  product_name,
  unit,
  brand_name,
  purchase_group_code ,
  purchase_group_name ,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  category_small_code,
  product_status_name,
  t1.posting_time,
  t1.after_qty,
  t1.after_amt,
  t1.after_price,
  t3.batch_no,
  t1.move_type,
  t1.wms_order_type,
  t3.qty as stock_qty,
  t3.amt  as stock_amt,
  t3.price  as stock_price,
  t2.credential_no as entry_credential,
  t2.input_time,
  t2.source_order_no as entry_source_order_no,
  t2.wms_order_type as entry_wms_order_type,
  t2.wms_order_no as entry_wms_order_no,
  t2.qty as entry_qty,
  t2.price as entry_price,
  t2.amt as entry_mount,
  supplier_code,
  supplier_name
from current_stock t1 
left outer join credential_item t3 on t1.id = t3.detail_id and t1.product_code=t3.product_code -- and t1.batch_no=t3.batch_no
left outer join entry_batch t2 on t3.batch_no = t2.batch_no and t3.product_code = t2.product_code
left outer join (select 
                a.shop_code,
                a.shop_name,
                product_code,
                product_name,
                unit,
                brand_name,
                purchase_group_code ,
                purchase_group_name ,
                classify_large_code,
                classify_large_name,
                classify_middle_code,
                classify_middle_name,
                classify_small_code,
                classify_small_name,
                category_small_code,
                a.product_status_name
                from csx_dw.dws_basic_w_a_csx_product_info a 
                 join 
                (SELECT 
                     classify_large_code,
                     
                     classify_large_name,
                     classify_middle_code,
                     classify_middle_name,
                     classify_small_code,
                     classify_small_name,
                     category_small_code
                FROM csx_dw.dws_basic_w_a_manage_classify_m
                    WHERE sdt='current'
                        and classify_middle_code='B0304' )b on a.small_category_code=b.category_small_code
                    where sdt='current') t4 on t1.product_code = t4.product_code and t1.location_code=t4.shop_code
left outer join
(select province_code,province_name,location_code from csx_dw.csx_shop where sdt='current') c on t1.location_code=c.location_code
left outer join
(select code,name,parent_code from csx_dw.dws_wms_w_a_basic_warehouse_reservoir where level='3')d on t1.location_code=d.parent_code and t1.reservoir_area_code=d.code
where classify_middle_code='B0304' 
and after_qty!=0;

insert overwrite directory '/tmp/pengchenghua/11' row format delimited fields terminated by '\t'
select * from csx_tmp.temp_stock;