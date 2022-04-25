select
  a.sdt,
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
  sum(sales_qty) as sales_qty,
  sum(sales_value) as sales_value,
  sum(sales_qty * coalesce(b.price, a.origin_cost_price)) as cost_value,
  sum(sales_qty * middle_office_price) as middle_cost_value,
  sum(sales_value) - sum(sales_qty * coalesce(b.price, a.origin_cost_price)) as profit,
  sum(sales_value) - sum(sales_qty * middle_office_price) as front_profit,
  if(c.product_code is null, '不是工厂商品', '工厂商品') as is_self_product
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
    origin_cost_price
  from csx_dw.sale_item_m 
  where sdt >= '20190901' and sales_type = 'anhui'
)a left outer join 
(
  select
    source_order_no,
    product_code,
    max(price) as price
  from csx_dw.accounting_credential_item
  where move_type = '114A' and direction = '-'
  group by source_order_no, product_code
)b on a.goods_code = b.product_code and a.origin_order_no = b.source_order_no
left outer join 
(
  select * from csx_ods.factory_setting_bom_ods 
  where sdt = '20190915'
)c on a.goods_code = c.product_code
group by 
    a.sdt,
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
    if(c.product_code is null, '不是工厂商品', '工厂商品');