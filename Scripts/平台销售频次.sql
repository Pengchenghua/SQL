--锘�--省锟斤拷锟斤拷品锟斤拷锟斤拷频锟斤拷
select
  channel_name,
  province_code,
  province_name,
  dept_id,
  dept_name,
  firm_code,
  firm_name,
  category_code,
  category_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  goods_code,
  goods_name,
  bar_code,
  brand_name,
  standard,
  price_zone,
  price_zone_name,
  COUNT(DISTINCT customer_no) cust_cn,
  COUNT(DISTINCT sdt) sdt_cn,
  sum(sales_value) sale,
  sum(sales_qty) qty,
  sum(profit) profit
from csx_dw.sale_goods_m1
where
  sdt >= '20190101'
  and sdt < '20191217'
  and bd_id in('12', '13')
GROUP BY
  channel_name,
  province_code,
  province_name,
  dept_id,
  dept_name,
  firm_code,
  firm_name,
  category_code,
  category_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  goods_code,
  goods_name,
  bar_code,
  brand_name,
  standard,
  price_zone,
  price_zone_name;
  
 --平台频次
select
  channel_name,
  dept_id,
  dept_name,
  firm_code,
  firm_name,
  category_code,
  category_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  goods_code,
  goods_name,
  bar_code,
  brand_name,
  standard,
  price_zone,
  price_zone_name,
  COUNT (DISTINCT province_code )as prov_cn,
  COUNT(DISTINCT customer_no) cust_cn,
  COUNT(DISTINCT sdt) sdt_cn,
  sum(sales_value) sale,
  sum(sales_qty) qty,
  sum(profit) profit
from csx_dw.sale_goods_m1
where
  sdt >= '20190101'
  and sdt < '20191217'
  and bd_id in('12', '13')
GROUP BY
  channel_name,
   dept_id,
  dept_name,
  firm_code,
  firm_name,
  category_code,
  category_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  goods_code,
  goods_name,
  bar_code,
  brand_name,
  standard,
  price_zone,
  price_zone_name;