select
a.channel,
d.channel,
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
  sum(sales_qty * coalesce(b.price, a.origin_sales_cost_price)) as sales_cost_value,
  sum(sales_qty * middle_office_price) as middle_sales_cost_value,
  sum(sales_value) - sum(sales_qty * coalesce(b.price, a.origin_sales_cost_price)) as profit,
  sum(sales_value) - sum(sales_qty * middle_office_price) as front_profit,
  if(c.goods_code is null, '���ǹ�����Ʒ', '������Ʒ') as is_self_product
from
(
  select 
  channel,
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
    origin_sales_cost_price
  from csx_dw.sale_item_m 
  where sdt >= '20190901'and sdt <= '20190915' and sales_type = 'anhui'
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
  select goods_code from csx_dw.factory_bom 
  where sdt = '20190915'
)c on a.goods_code = c.goods_code
left outer join
(select b.customer_no,b.channel from csx_dw.customer_m b where sdt='20190916') d on a.customer_no=d.customer_no
group by 
d.channel,
a.channel,
    a.sdt,
    dc_code,
    dc_name,
    a.customer_no,
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
    if(c.goods_code is null, '���ǹ�����Ʒ', '������Ʒ');