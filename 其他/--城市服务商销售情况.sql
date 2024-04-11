--城市服务商销售情况

-- ******************************************************************** 
-- @功能描述：
-- @创建者： 公会敏 
-- @创建者日期：2022-09-02 14:21:59 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
insert overwrite table csx_analyse.csx_analyse_fr_sale_city_service_supplier_di partition (sdt)
select
  a.performance_region_code,
  a.performance_region_name,
  a.performance_province_code,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.business_type_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.sign_company_code,
  a.sign_company_name,
  a.customer_code,
  a.customer_name,
  c.supplier_code,
  c.supplier_name,
  coalesce(b.link_wms_move_type_code, '') as link_wms_move_type_code,
  coalesce(b.link_wms_move_type_name, '') as link_wms_move_type_name,
  round(sum(b.sale_qty),2) as sale_qty,
  round(sum(b.sale_qty * a.sale_price),2) as sale_amt,
  round(sum(b.sale_cost),2) as sale_cost,
  round(sum(b.sale_qty * a.sale_price),2) - round(sum(b.sale_cost),2) as profit,
  a.sdt
from
(
  select
    sdt,
    split(id, '&')[0] as credential_no,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    business_type_name,
    inventory_dc_code,
    inventory_dc_name,
    sign_company_code,
    sign_company_name,
    customer_code,
    customer_name,
    goods_code,
    sale_qty,
    sale_cost,
    sale_price
  from csx_dws.csx_dws_sale_detail_di 
  where sdt >= regexp_replace(add_months(trunc(date_sub('${current_date}', 1), 'MM'), -1), '-', '') 
	and channel_code in ('1', '7', '9')
    and business_type_code ='4' and order_channel_detail_code<>'25'
)a
--批次操作明细表
left join
(
  select
    credential_no,
    wms_order_no, -- wms入库订单号
    goods_code,
    sum(if(in_or_out = 0, -1 * qty, qty)) as sale_qty,
    sum(if(in_or_out = 0, -1 * amt, amt)) as sale_cost,
    link_wms_move_type_code,
    link_wms_move_type_name
  from csx_dws.csx_dws_wms_batch_detail_di
  where sdt >= regexp_replace(add_months(trunc(date_sub('${current_date}', 1), 'MM'), -3), '-', '')
  group by credential_no, wms_order_no, goods_code, link_wms_move_type_code, link_wms_move_type_name
)b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
left join
(
  select distinct
    supplier_code,
    supplier_name,
    order_code,
    goods_code
  from csx_dws.csx_dws_wms_entry_detail_di
  where sdt >= regexp_replace(add_months('${current_date}', -9), '-', '')
)c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
group by a.performance_region_code, a.performance_region_name, a.performance_province_code, a.performance_province_name, a.performance_city_code, a.performance_city_name,
  a.business_type_name, a.inventory_dc_code, a.inventory_dc_name, a.sign_company_code, a.sign_company_name, a.customer_code, a.customer_name, c.supplier_code,
  c.supplier_name, coalesce(b.link_wms_move_type_code, ''), coalesce(b.link_wms_move_type_name, ''), a.sdt;


--城市服务商销售情况 按月取数
--城市服务商销售情况

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
    where sdt>='20210901' and sdt<'20211201'
	and channel_code in ('1', '7', '9')
	and business_type_code ='4'
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




select 
      a.credential_no,
      c.order_code,
      origin_order_code,
      a.business_type_name,
      a.region_name,
      a.province_name,
      a.customer_no,
      a.customer_name,
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
  round(b.sales_qty,2) as sales_qty1,
  round(b.sales_cost,2) as sales_cost1,
  round(b.no_tax_amt,2) as no_tax_amt,         --未税入库成本
  round(b.sales_qty * a.sales_price,2) as sales_value1,
  round(b.sales_qty * a.sales_price,2) - round(sum(b.sales_cost),2) as profit1,
  round(b.sales_qty * a.sales_price,2) - round(sum(b.sales_qty * a.middle_office_price),2) as front_profit1,
  round(b.sales_qty * a.no_tax_price,2) as no_tax_sales_value1,
  round(b.sales_qty * a.no_tax_price,2) - round(sum(b.no_tax_amt),2) as profit1,
  round(b.sales_qty * a.no_tax_price,2) - round(sum(b.sales_qty * a.middle_office_price),2) as front_profit1 ,
  (entry_amt)entry_amt,
  (c.no_tax_amt) as c_no_tax_amt
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
      sales_price,
      sales_price/(1+tax_rate/100) as no_tax_price,
      cost_price/(1+tax_rate/100) as no_tax_cost
    from csx_dw.dws_sale_r_d_detail 
    where sdt>='20211101' and sdt<'20211201'
	and channel_code in ('1', '7', '9')
	and business_type_code ='4'
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
     sum(if(in_or_out = 0, -1 * amt_no_tax,amt_no_tax )) as no_tax_amt,
     link_wms_entry_move_type,
     link_wms_entry_move_name
   from csx_dw.dws_wms_r_d_batch_detail
   where sdt >= '20190101'
   group by credential_no, wms_order_no, goods_code, link_wms_entry_move_type,link_wms_entry_move_name
  )b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
  left join 
  (
   select 
     supplier_code,
     supplier_name,
     order_code,
     origin_order_code,
     goods_code,
     (amount) entry_amt,
     (amount/(1+tax_rate/100)) as no_tax_amt
   from csx_dw.dws_wms_r_d_entry_detail
   where sdt >= '20190101' or sdt = '19990101'
  
    )c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
;



--W0K4_
create temporary table csx_tmp.temp_01 as 
select 
      a.credential_no,
      c.order_code,
      origin_order_code,
      a.business_type_name,
      a.region_name,
      a.province_name,
      a.customer_no,
      a.customer_name,
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
  round(b.sales_qty,2) as sales_qty1,
  round(b.sales_cost,2) as sales_cost1,
  round(b.no_tax_amt,2) as no_tax_amt,         --未税入库成本
  round(b.sales_qty * a.sales_price,2) as sales_value1,
  round(b.sales_qty * a.sales_price,2) - round(b.sales_cost,2) as profit1,
  round(b.sales_qty * a.sales_price,2) - round(b.sales_qty * a.middle_office_price,2) as front_profit1,
  round(b.sales_qty * a.no_tax_price,2) as no_tax_sales_value1,
  round(b.sales_qty * a.no_tax_price,2) - round(b.no_tax_amt,2) as no_tax_profit1,
  (entry_amt)entry_amt,
  (c.no_tax_amt) as c_no_tax_amt
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
      sales_price,
      sales_price/(1+tax_rate/100) as no_tax_price,
      cost_price/(1+tax_rate/100) as no_tax_cost
    from csx_dw.dws_sale_r_d_detail 
    where sdt>='20200101' and sdt<'20211201'
	and channel_code in ('1', '7', '9')
	--and business_type_code ='4'
	and sales_type<>'fanli'
	and dc_code='W0K4'
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
     sum(if(in_or_out = 0, -1 * amt_no_tax,amt_no_tax )) as no_tax_amt,
     link_wms_entry_move_type,
     link_wms_entry_move_name
   from csx_dw.dws_wms_r_d_batch_detail
   where sdt >= '20190101'
   group by credential_no, wms_order_no, goods_code, link_wms_entry_move_type,link_wms_entry_move_name
  )b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
  left join 
  (
   select 
     supplier_code,
     supplier_name,
     order_code,
     origin_order_code,
     goods_code,
     (amount) entry_amt,
     (amount/(1+tax_rate/100)) as no_tax_amt
   from csx_dw.dws_wms_r_d_entry_detail
   where sdt >= '20190101' or sdt = '19990101'
  
    )c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
;

select * from  csx_tmp.temp_01 ;