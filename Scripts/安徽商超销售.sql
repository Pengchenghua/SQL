select 
  a.*,
  if(b.goods_code is not null, '������Ʒ', '���ǹ�����Ʒ') as  `�Ƿ񹤳���Ʒ`
from 
(
  select
    sdt, 
    dc_code,
    dc_name,
    shop_code,
    shop_name,
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
    sum(coalesce(sales_cost_price, 0)*sales_qty) as sales_cost_value,
    sum(sales_value) - sum(coalesce(sales_cost_price, 0)*sales_qty) as profit
  from csx_dw.shop_sale_item_m 
  where  sdt >= '20190901' and sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
  group by 
    dc_code,
    dc_name,
    shop_code,
    shop_name,
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
    sdt
)a left outer join 
(
  select goods_code from csx_dw.factory_bom 
  where sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
)b on a.goods_code = b.goods_code;