-- 联营仓入库统计
create temporary table csx_tmp.temp_ly_01 as 
select 
      substr(sdt,1,6) mon,
      a.business_type_name,
      a.region_name,
      a.province_name,
      -- a.customer_no,
      -- a.customer_name,
      c.supplier_code,
      c.supplier_name, 
      a.goods_code,
      goods_name,
      unit,
      brand_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
  --sum(a.sales_value) sales_value,   --含税销售额
  --sum(a.profit) profit,   --含税毛利
  --sum(a.front_profit) front_profit,
--   coalesce(b.link_wms_entry_move_type, '') as link_wms_entry_move_type,
--   coalesce(b.link_wms_entry_move_name, '') as link_wms_entry_move_name,
  round(sum(b.sales_qty),2) as sales_qty1,
  round(sum(b.sales_cost),2) as sales_cost1,
  round(sum(b.sales_qty * a.sales_price),2) as sales_value1,
  round(sum(b.sales_qty * a.sales_price),2) - round(sum(b.sales_cost),2) as profit1,
  round(sum(b.sales_qty * a.sales_price),2) - round(sum(b.sales_qty * a.middle_office_price),2) as front_profit1 
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
      unit,
      brand_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
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
    where sdt>='20220101' and sdt<'20220315'
	and dc_code  in ('W0K4','W0Z7','WB26')
	and sales_type<>'fanli'
  )a 
--批次操作明细表
  left join 
  (
   select
     credential_no,
     wms_order_no,
     goods_code,
     sum(if(in_or_out = 0, -1 * qty, qty)) as sales_qty,
     sum(if(in_or_out = 0, -1 * amt, amt)) as sales_cost,
     link_wms_entry_move_type,
     link_wms_entry_move_name
   from csx_dw.dws_wms_r_d_batch_detail
   where sdt >= '20210101'
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
   where sdt >= '20210101' or sdt = '19990101'
    )c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
group by a.business_type_name,
    a.region_name,
    a.province_name,
    a.customer_no,a.customer_name,c.supplier_code,c.supplier_name,
    a.goods_code,
    goods_name,
      unit,
      brand_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      substr(sdt,1,6);
      
select * from csx_tmp.temp_ly_01 