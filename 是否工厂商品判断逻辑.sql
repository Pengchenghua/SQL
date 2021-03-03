set mapreduce.job.queuename=caishixian;


drop table b2b_tmp.factory_sale_1;
create temporary table b2b_tmp.factory_sale_1
as
select
  a.province_code,
  a.province_name,
  b.workshop_code,
  b.workshop_name,
  a.goods_code,
  a.sales_value,
  a.profit,
  coalesce(a.order_qty,a.sales_qty) as order_qty,
  a.sales_qty,
  a.sdt
from
(
  select
    *
  from csx_dw.customer_sale_m
  where channel in ('1','2','3','7') and dc_province_code is not null and dc_province_code<>''
    and province_name is not null and province_name<>''
    and sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,6),'01')
    and sdt<=regexp_replace(date_sub(current_date,1),'-','')
    and sales_type in ('anhui','sc')
)a
join
(
  select
    *
  from csx_dw.factory_setting_craft_once_all
  where sdt='current' and new_or_old=1
)b
on a.dc_province_code=b.province_code and a.goods_code=b.goods_code

union all
select
  a.province_code,
  a.province_name,
  b.workshop_code,
  b.workshop_name,
  a.goods_code,
  a.sales_value,
  a.profit,
  coalesce(a.order_qty,a.sales_qty) as order_qty,
  a.sales_qty,
  a.sdt
from
(
  select
    *
  from csx_dw.customer_sale_m
  where channel in ('1','2','3','7') and dc_province_code is not null and dc_province_code<>''
    and province_name is not null and province_name<>''
    and sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,6),'01')
    and sdt<=regexp_replace(date_sub(current_date,1),'-','')
    and sales_type in ('qyg','gc','bbc')
)a
join
(
  select
    *
  from csx_dw.factory_setting_craft_once_all
  where sdt='current' and new_or_old=2
)b
on a.dc_province_code=b.province_code and a.goods_code=b.goods_code
;

