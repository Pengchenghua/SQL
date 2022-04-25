select
  a.sdt,
  a.dc_code,
  a.dc_name,
  a.customer_no,
  a.customer_name,
  a.goods_code,
  a.goods_name,
  a.division_name,
  a.division_code,
  a.department_code,
  a.department_name,
  a.category_large_name,
  a.category_large_code,
  a.category_middle_name,
  a.category_middle_code,
  a.category_small_name,
  a.category_small_code,
  sum(sales_qty) as sales_qty,
  sum(sales_value) as sales_value,
  sum(sales_cost) as sales_cost,
  sum(sales_qty * middle_office_price) as middle_cost_value,
  sum(profit) as profit,
  if(sum(sales_value) <> 0, sum(profit)/sum(sales_value), 0.0) as profit_rate,
  sum(front_profit) as front_profit,
  if(sum(sales_value) <> 0, sum(front_profit)/sum(sales_value), 0.0) as front_profit_rate,
  if(c.goods_code is null, '不是工厂商品', '工厂商品') as is_self_product
from
(
  select 
    sdt,
    dc_code,
    dc_name,
    customer_no,
    customer_name,
    goods_code,
    goods_name,
    division_name,
    division_code,
    department_code,
    department_name,
    category_large_name,
    category_large_code,
    category_middle_name,
    category_middle_code,
    category_small_name,
    category_small_code,
    sales_value,
    origin_order_no,
    sales_qty,
    middle_office_price,
    origin_cost_price,
    profit,
    sales_cost,
    front_profit
  from csx_dw.sale_item_m 
  where sdt >= '20190901' and sdt < '20190924' and sales_type = 'anhui' and sales_qty <> 0
)a
left outer join 
(
  select distinct goods_code from csx_dw.factory_bom 
  where sdt = 'current'
)c on a.goods_code = c.goods_code
group by 
    a.sdt,
    dc_code,
    dc_name,
    customer_no,
    customer_name,
    a.goods_code,
    goods_name,
    division_name,
    division_code,
    department_code,
    department_name,
    category_large_name,
    category_large_code,
    category_middle_name,
    category_middle_code,
    category_small_name,
    category_small_code,
    if(c.goods_code is null, '不是工厂商品', '工厂商品');