-- 水产批次入库与销售
create  table csx_tmp.temp_batch_01 as 
select 
a.credential_no,
a.business_type_name,
a.region_name,
a.province_name,
a.customer_no,
a.customer_name,
c.supplier_code,
c.supplier_name, 
a.goods_code,
goods_name,
  --sum(a.sales_value) sales_value,   --含税销售额
  --sum(a.profit) profit,   --含税毛利
  --sum(a.front_profit) front_profit,
  coalesce(b.link_wms_entry_move_type, '') as link_wms_entry_move_type,
  coalesce(b.link_wms_entry_move_name, '') as link_wms_entry_move_name,
  round(max(a.sales_qty),2) as sales_qty,
  round(max(a.sales_cost),2) as sales_cost,
  max(sales_value)sales_value,
  max(profit) profit,
  round(sum(entry_amt),2) as entry_amt,
  round(sum(b.entry_qty),2) as entry_qty,
  round(sum(entry_amt)/sum(entry_qty),2) as avg_cost
from 
  (
    select 
      sdt,
	  split(id, '&')[0] as credential_no,
      region_code,
      region_name,
      province_code,
      province_name,
	  city_group_code,
	  city_group_name,
	  business_type_name,
	  dc_code, 
      customer_no,
      customer_name,
      goods_code,
      goods_name,
	  is_factory_goods_desc,
      sales_qty,
      sales_value,
      sales_cost,
      profit,
	  front_profit,
	  purchase_price_flag,
      cost_price,
      case when purchase_price_flag='1' then purchase_price end as purchase_price,
      middle_office_price,
      sales_price
    from csx_dw.dws_sale_r_d_detail 
    where sdt>='20220701' and sdt<'20220801'
	and channel_code in ('1', '7', '9')
	-- and business_type_code ='4'
	and dc_code='W0A3'
--	and goods_code='1465978'
	and classify_middle_code='B0303'
	and sales_type<>'fanli'
  )a 
--批次操作明细表
  left join 
  (
   select
     credential_no,
     wms_order_no,
     goods_code,
     sum(if(in_or_out = 0, -1 * qty, qty)) as entry_qty,
     sum(if(in_or_out = 0, -1 * amt, amt)) as entry_amt,
     link_wms_entry_move_type,
     link_wms_entry_move_name
   from csx_dw.dws_wms_r_d_batch_detail
   where sdt >= '20201001'
   group by credential_no, wms_order_no, goods_code, link_wms_entry_move_type,link_wms_entry_move_name
  )b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
  left join 
  (
   select distinct
     supplier_code,
     supplier_name,
     order_code,
     goods_code
   from csx_dw.dws_wms_r_d_entry_detail
   where sdt >= '20201001' or sdt = '19990101'
    )c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
group by a.business_type_name,a.region_name,a.province_name,a.customer_no,a.customer_name,c.supplier_code,c.supplier_name,
coalesce(b.link_wms_entry_move_type, ''),coalesce(b.link_wms_entry_move_name, ''),
     a.goods_code,goods_name,
     a.credential_no;


select 
a.business_type_name,
a.region_name,
a.province_name,
a.customer_no,
a.customer_name,
a.supplier_code,
a.supplier_name, 
a.goods_code,
a.goods_name,
 classify_middle_code ,
 classify_middle_name ,
 classify_small_code ,
 classify_small_name ,
  round(sum(a.sales_qty),2) as sales_qty,
  round(sum(a.sales_cost),2) as sales_cost,
  sum(sales_value)sales_value,
   sum(profit) profit,
  round(sum (entry_amt),2) as entry_amt,
  round(sum (entry_qty),2) as entry_qty,
  round(sum(entry_amt)/sum(entry_qty),2) as avg_cost
from 
 csx_tmp.temp_batch_01 a
 join 
 (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
-- where sales_qty-entry_qty !=0
group by a.business_type_name,
a.region_name,
a.province_name,
a.customer_no,
a.customer_name,
a.supplier_code,
a.supplier_name, 
a.goods_code,
a.goods_name,
 classify_middle_code ,
 classify_middle_name ,
 classify_small_code ,
 classify_small_name 