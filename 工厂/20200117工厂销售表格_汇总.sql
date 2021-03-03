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




--当天明细
drop table b2b_tmp.factory_sale_2;
create temporary table b2b_tmp.factory_sale_2
as
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(sales_value) as sales_value,
  0 as round_sales_value,
  sum(profit) as profit,
  sum(sales_qty) as sales_qty,
  count(distinct goods_code) as dong_sku
from b2b_tmp.factory_sale_1
where sdt=regexp_replace(date_sub(current_date,1),'-','')
group by  province_code,province_name,workshop_code,workshop_name
union all
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  0 as sales_value,
  sum(sales_value) as round_sales_value, 
  0 as profit,
  0 as sales_qty,
  0 as dong_sku
from b2b_tmp.factory_sale_1
where sdt=regexp_replace(date_sub(current_date,2),'-','')
group by  province_code,province_name,workshop_code,workshop_name
;



----当天车间
drop table b2b_tmp.factory_sale_3;
create temporary table b2b_tmp.factory_sale_3
as
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(sales_qty) as sales_qty,
  sum(sales_value) as sales_value,
  sum(round_sales_value) as round_sales_value,
  sum(profit) as profit,
  sum(profit)/sum(sales_value) as profit_rate,
  sum(dong_sku) as dong_sku
from b2b_tmp.factory_sale_2
group by  province_code,province_name,workshop_code,workshop_name
;
----当天工厂
drop table b2b_tmp.factory_sale_4;
create temporary table b2b_tmp.factory_sale_4
as
select
  province_code,
  province_name,
  sum(sales_value) as sales_value,
  sum(round_sales_value) as round_sales_value,
  sum(profit) as profit,
  sum(profit)/sum(sales_value) as profit_rate
from b2b_tmp.factory_sale_2
group by  province_code,province_name
;


--当月明细
drop table b2b_tmp.factory_sale_5;
create temporary table b2b_tmp.factory_sale_5
as
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(sales_value) as sales_value,
  0 as round_sales_value,
  sum(profit) as profit,
  sum(sales_qty) as sales_qty,
  sum(order_qty) as order_qty,
  count(distinct goods_code) as dong_sku
from b2b_tmp.factory_sale_1
where sdt <=regexp_replace(date_sub(current_date,1),'-','') 
  and sdt>=regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','')
group by  province_code,province_name,workshop_code,workshop_name
union all
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  0 as sales_value,
  sum(sales_value) as round_sales_value, 
  0 as profit,
  0 as sales_qty,
  0 as order_qty,
  0 as dong_sku
from b2b_tmp.factory_sale_1
where sdt <=regexp_replace(add_months(date_sub(current_date,1),-1),'-','') 
  and sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','')
group by  province_code,province_name,workshop_code,workshop_name
;

----当月车间
drop table b2b_tmp.factory_sale_6;
create temporary table b2b_tmp.factory_sale_6
as
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(sales_value) as sales_value,
  sum(sales_qty)/sum(order_qty) as result_rate,
  sum(round_sales_value) as round_sales_value,
  sum(sales_qty) as sales_qty,
  sum(profit) as profit,
  sum(profit)/sum(sales_value) as profit_rate,
  sum(dong_sku) dong_sku
from b2b_tmp.factory_sale_5
group by  province_code,province_name,workshop_code,workshop_name
;

----当月工厂
drop table b2b_tmp.factory_sale_7;
create temporary table b2b_tmp.factory_sale_7
as
select
  province_code,
  province_name,
  sum(sales_value) as sales_value,
  sum(sales_qty)/sum(order_qty) as result_rate,
  sum(round_sales_value) as round_sales_value,
  sum(profit) as profit,
  sum(profit)/sum(sales_value) as profit_rate
from b2b_tmp.factory_sale_5
group by  province_code,province_name
;




----工厂生产新系统_日
drop table b2b_tmp.factory_sale_11;
create temporary table b2b_tmp.factory_sale_11
as
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(transfer_quantity) as transfer_quantity,
  sum(p_transfer_values) as p_transfer_values,
  sum(user_qty) as user_qty,
  sum(user_values) as user_values,
  sum(return_qty) as return_qty,
  sum(return_values) as return_values,
  sum(fact_qty) as fact_qty,
  sum(fact_values) as fact_values,
  sum(goods_reality_receive_qty) as goods_reality_receive_qty,
  sum(p_total) as p_total,
  sum(reality_receive_qty) as reality_receive_qty,
  sum(reality_receive_qty)/sum(fact_qty) as out_rate_qty,
  sum(p_total)/sum(fact_values) out_rate_values
from csx_dw.factory_order
where sdt=regexp_replace(date_sub(current_date,1),'-','') 
group by  province_code,province_name,workshop_code,workshop_name
;

----工厂生产新系统_车间月
drop table b2b_tmp.factory_sale_12;
create temporary table b2b_tmp.factory_sale_12
as
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(transfer_quantity) as transfer_quantity,
  sum(p_transfer_values) as p_transfer_values,
  sum(user_qty) as user_qty,
  sum(user_values) as user_values,
  sum(return_qty) as return_qty,
  sum(return_values) as return_values,
  sum(fact_qty) as fact_qty,
  sum(fact_values) as fact_values,
  sum(goods_reality_receive_qty) as goods_reality_receive_qty,
  sum(p_total) as p_total,
  sum(reality_receive_qty) as reality_receive_qty,
  sum(reality_receive_qty)/sum(fact_qty) as out_rate_qty,
  sum(p_total)/sum(fact_values) out_rate_values
from csx_dw.factory_order
where sdt>=regexp_replace(trunc(date_sub(current_date,1),'MM'), '-','')
  and sdt<=regexp_replace(date_sub(current_date,1),'-','') 
group by  province_code,province_name,workshop_code,workshop_name
;

--------------------------------------------------------------
--旧系统生产车间由于业务不明暂无
--------------------------------------------------------------

--工厂生产新系统_省区月
drop table b2b_tmp.factory_sale_21;
create temporary table b2b_tmp.factory_sale_21
as
select
  province_code,
  province_name,
  sum(p_total) as p_total,
  sum(transfer_quantity) as  transfer_quantity,
  sum(p_transfer_values) as p_transfer_values
from b2b_tmp.factory_sale_12
group by  province_code,province_name
;


----工厂生产旧系统_省区月_实际生产
drop table b2b_tmp.factory_sale_22;
create temporary table b2b_tmp.factory_sale_22
as 
select
  b.province_code,
  b.province_name,
  sum(p_total) as p_total
from
(
select
  plant as shop_id,
  sum(case when movetype in ('101') then rthfees else -1*rthfees end) p_total,
  0 as p_transfer_values
from csx_ods.mseg_ecc_dtl_fct 
where sdt>=concat(substr(regexp_replace(date_sub(current_date,1),'-','') ,1,6),'01') 
  and sdt<=regexp_replace(date_sub(current_date,1),'-','') 
  and  coorder<>''
  and  movetype in ('101','102')
group by  plant
)a
join
(
  select distinct
    shop_id,
    province_code,
    province_name
  from  csx_dw.shop_m
  where sdt='current'
)b
on a.shop_id=b.shop_id
group by   b.province_name,b.province_code
;




--工厂生产旧系统_调拨明细
drop table b2b_tmp.factory_sale_23;
create temporary table b2b_tmp.factory_sale_23
as 
select
  sdt,
  produce_month,
  province_code,
  province_name,
  goods_code,
  sum(p_transfer_values) as p_transfer_values,
  sum(transfer_quantity) as transfer_quantity
from
(
  select
    sdt,
    substr(regexp_replace(date_sub(current_date,1),'-','') ,1,6) as produce_month,
    c.province_code,
    c.province_name,
    a.goods_code,
    sum(a.amount) as p_transfer_values,
    sum(shelf_qty) as transfer_quantity
  from 
  (
  select
    *
  from csx_dw.wms_entry_order_m  
  WHERE send_location_code in ('W039','W048','W053','W079','W080','W081','W082','W088','W098')
    and sdt>=concat(substr(regexp_replace(date_sub(current_date,1),'-','') ,1,6),'01') 
    and sdt<=regexp_replace(date_sub(current_date,1),'-','') 
  )a
  left outer join
  (
  select
    transfer_code,
    goods_code
  from
    (
    select 
      transfer_code,
      goods_code,
      row_number()over(partition by transfer_code,goods_code) ranks
    from csx_dw.factory_order
    )aa
  where aa.ranks=1
  )b
  on a.origin_order_code=b.transfer_code and a.goods_code=b.goods_code
  join
  (
    select distinct
      shop_id,
      province_code,
      province_name
    from  csx_dw.shop_m
    where sdt='current'
  )c
  on a.send_location_code=c.shop_id
  where b.transfer_code is  null
  group by c.province_code,c.province_name,a.goods_code,sdt,substr(regexp_replace(date_sub(current_date,1),'-','') ,1,6) 
  union all
  select
    sdt,
    substr(regexp_replace(date_sub(current_date,1),'-','') ,1,6) as produce_month,
    dc_province_code as province_code,
    dc_province_name as province_name,
    goods_code,
    sum(excluding_tax_sales) as p_transfer_values,
    sum(sales_qty) as transfer_quantity
  from csx_dw.customer_sale_m
  where  dc_code in ('W039','W048','W053','W079','W080','W081','W082','W088','W098')
    and sdt>=concat(substr(regexp_replace(date_sub(current_date,1),'-','') ,1,6),'01') 
    and sdt<=regexp_replace(date_sub(current_date,1),'-','')
  group by dc_province_code,dc_province_name,goods_code,sdt,substr(regexp_replace(date_sub(current_date,1),'-','') ,1,6)
)aa
group by province_code,province_name,goods_code,sdt,produce_month
;







--工厂生产旧系统_调拨明细
drop table b2b_tmp.factory_sale_24;
create temporary table b2b_tmp.factory_sale_24
as 
select
  a.sdt as produce_date,
  a.produce_month,
  a.province_code,
  a.province_name,
  b.workshop_code,
  b.workshop_name,
  a.goods_code,
  a.p_transfer_values,
  a.transfer_quantity
from
(
select
  sdt,
  produce_month,
  province_code,
  province_name,
  goods_code,
  p_transfer_values,
  transfer_quantity
from b2b_tmp.factory_sale_23
)a
join
(
  select
    *
  from csx_dw.factory_setting_craft_once_all
  where sdt='current' and new_or_old=2
)b
on a.province_code=b.province_code and a.goods_code=b.goods_code
;


----工厂生产调拨车间_日
drop table b2b_tmp.factory_sale_25;
create temporary table b2b_tmp.factory_sale_25
as 
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(transfer_quantity) as transfer_quantity,
  sum(p_transfer_values) as p_transfer_values
from
(
  --旧系统
  select
    province_code,
    province_name,
    workshop_code,
    workshop_name,
    transfer_quantity,
    p_transfer_values
  from b2b_tmp.factory_sale_24
  where produce_date=regexp_replace(date_sub(current_date,1),'-','')
  --新系统
  union all
  select
    province_code,
    province_name,
    workshop_code,
    workshop_name,
    transfer_quantity,
    p_transfer_values
  from b2b_tmp.factory_sale_11
)aa
group by   province_code,province_name,workshop_code,workshop_name
;
----工厂生产调拨车间_月
drop table b2b_tmp.factory_sale_26;
create temporary table b2b_tmp.factory_sale_26
as 
select
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(transfer_quantity) as transfer_quantity,
  sum(p_transfer_values) as p_transfer_values
from
(
  --旧系统
  select
    province_code,
    province_name,
    workshop_code,
    workshop_name,
    transfer_quantity,
    p_transfer_values
  from b2b_tmp.factory_sale_24
  --新系统
  union all
  select
    province_code,
    province_name,
    workshop_code,
    workshop_name,
    transfer_quantity,
    p_transfer_values
  from b2b_tmp.factory_sale_12
)aa
group by  province_code,province_name,workshop_code,workshop_name
;



set hive.map.aggr = true;
set hive.groupby.skewindata=false;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true; -- 开启动态分析
set hive.exec.dynamic.partition.mode=nonstrict; -- 动态分区模式
set hive.exec.max.dynamic.partitions.pernode=10000;
-----车间——日
insert overwrite table csx_dw.factory_workshop_day partition(sdt)
select
 *,
 regexp_replace(date_sub(current_date,1),'-','') as sdt
from
(
  select
    province_name,
    workshop_name,
    sum(sales_qty)/10000 as sales_qty,
    sum(sales_value)/10000 as sales_value,
    sum(round_sales_value)/10000  as round_sales_value,
    (sum(sales_value)/sum(round_sales_value)-1) as round_sales_rate,
    sum(profit)/10000  as profit,
    sum(profit_rate) as profit_rate,
    sum(transfer_quantity)/10000  as transfer_quantity,
    sum(p_transfer_values)/10000  as p_transfer_values,
    sum(dong_sku) as dong_sku,
    sum(user_qty)/10000  as user_qty,
    sum(user_values)/10000  as user_values,
    sum(return_qty)/10000  as return_qty,
    sum(return_values)/10000  as return_values,
    sum(fact_qty)/10000  as fact_qty,
    sum(fact_values)/10000  as fact_values,
    sum(goods_reality_receive_qty)/10000  as goods_reality_receive_qty,
    sum(p_total)/10000  as p_total,
    sum(reality_receive_qty)/10000 as reality_receive_qty,
    sum(out_rate_qty) as out_rate_qty,
    sum(out_rate_values) out_rate_values
  from 
  (
  select
    province_name,
    workshop_name,
    sales_qty,
    sales_value,
    round_sales_value,
    profit,
    0 as transfer_quantity,
    0 as p_transfer_values,
    profit_rate,
    dong_sku,
    0 as user_qty,
    0 as user_values,
    0 as return_qty,
    0 as return_values,
    0 as fact_qty,
    0 as fact_values,
    0 as goods_reality_receive_qty,
    0 as p_total,
    0 as reality_receive_qty,
    0 as out_rate_qty,
    0 as out_rate_values
  from b2b_tmp.factory_sale_3
  union all
  select
    province_name,
    workshop_name,
    0 as sales_qty,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    0 as transfer_quantity,
    0 as p_transfer_values,
    0 as profit_rate,
    0 as dong_sku,
    user_qty,
    user_values,
    return_qty,
    return_values,
    fact_qty,
    fact_values,
    goods_reality_receive_qty,
    p_total,
    reality_receive_qty,
    out_rate_qty,
    out_rate_values
  from b2b_tmp.factory_sale_11
  union all
  select
    province_name,
    workshop_name,
    0 as sales_qty,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    transfer_quantity,
    p_transfer_values,
    0 as profit_rate,
    0 as dong_sku,
    0 as user_qty,
    0 as user_values,
    0 as return_qty,
    0 as return_values,
    0 as fact_qty,
    0 as fact_values,
    0 as goods_reality_receive_qty,
    0 as p_total,
    0 as reality_receive_qty,
    0 as out_rate_qty,
    0 as out_rate_values  
  from b2b_tmp.factory_sale_25
  )aa
  group by   province_name,workshop_name
)bb
where sales_value<>0
;

-----车间——月
insert overwrite table csx_dw.factory_workshop_month partition(sdt)
select
 *,
 regexp_replace(date_sub(current_date,1),'-','') as sdt
from 
(
  select
    province_name,
    workshop_name,
    sum(sales_value)/10000  as sales_value,
    --sum(factory_result_rate) as factory_result_rate,
    sum(round_sales_value)/10000  as round_sales_value,
    (sum(sales_value)/sum(round_sales_value)-1) as round_sales_rate,
    sum(sales_qty)/10000 as sales_qty,
    sum(profit)/10000 as profit,
    sum(profit_rate) as profit_rate,
    sum(transfer_quantity)/10000 as transfer_quantity,
    sum(p_transfer_values)/10000 as p_transfer_values,
    sum(dong_sku) as dong_sku,
    sum(user_qty)/10000 as user_qty,
    sum(user_values)/10000 as user_values,
    sum(return_qty)/10000 as return_qty,
    sum(return_values)/10000 as return_values,
    sum(fact_qty)/10000 as fact_qty,
    sum(fact_values)/10000 as fact_values,
    sum(goods_reality_receive_qty)/10000 as goods_reality_receive_qty,
    sum(p_total)/10000 as p_total,
    sum(reality_receive_qty)/10000 as reality_receive_qty,
    sum(out_rate_qty) as out_rate_qty,
    sum(out_rate_values) out_rate_values
  from 
  (
  select
    province_name,
    workshop_name,
    sales_qty,
    sales_value,
    round_sales_value,
    profit,
    0 as transfer_quantity,
    0 as p_transfer_values,
    profit_rate,
    dong_sku,
    0 as user_qty,
    0 as user_values,
    0 as return_qty,
    0 as return_values,
    0 as fact_qty,
    0 as fact_values,
    0 as goods_reality_receive_qty,
    0 as p_total,
    0 as reality_receive_qty,
    0 as out_rate_qty,
    0 as out_rate_values,
    0 as factory_result_rate
  from b2b_tmp.factory_sale_6
  union all
  select
    province_name,
    workshop_name,
    0 as sales_qty,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    0 as transfer_quantity,
    0 as p_transfer_values,
    0 as profit_rate,
    0 as dong_sku,
    user_qty,
    user_values,
    return_qty,
    return_values,
    fact_qty,
    fact_values,
    goods_reality_receive_qty,
    p_total,
    reality_receive_qty,
    out_rate_qty,
    out_rate_values,
    0 as factory_result_rate
  from b2b_tmp.factory_sale_12
  union all
  select
    province_name,
    workshop_name,
    0 as sales_qty,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    transfer_quantity,
    p_transfer_values,
    0 as profit_rate,
    0 as dong_sku,
    0 as user_qty,
    0 as user_values,
    0 as return_qty,
    0 as return_values,
    0 as fact_qty,
    0 as fact_values,
    0 as goods_reality_receive_qty,
    0 as p_total,
    0 as reality_receive_qty,
    0 as out_rate_qty,
    0 as out_rate_values,
    0 as factory_result_rate 
   from b2b_tmp.factory_sale_26
   union all
   select
    province_name,
    workshop_name,
    0 as sales_qty,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    0 as transfer_quantity,
    0 as p_transfer_values,
    0 as profit_rate,
    0 as dong_sku,
    0 as user_qty,
    0 as user_values,
    0 as return_qty,
    0 as return_values,
    0 as fact_qty,
    0 as fact_values,
    0 as goods_reality_receive_qty,
    0 as p_total,
    0 as reality_receive_qty,
    0 as out_rate_qty,
    0 as out_rate_values,
    result_rate as factory_result_rate
  from  b2b_tmp.factory_sale_6
)aa
group by  province_name,workshop_name
) bb
where bb.sales_value<>0
;





----省区整体当天销售
drop table b2b_tmp.factory_sale_41;
create temporary table b2b_tmp.factory_sale_41
as 
select
  province_name,
  sum(sales_value) as sales_value,
  0 as round_sales_value,
  sum(profit) as profit
from csx_dw.sale_goods_m1
where sdt=regexp_replace(date_sub(current_date,1),'-','')
  and channel in ('1','2','3','7')
  and province_name is not  null and province_name<>''
group by   province_name
union all
select
  province_name,
  0 as sales_value,
  sum(sales_value) as round_sales_value,
  0 as profit
from csx_dw.sale_goods_m1
where sdt=regexp_replace(date_sub(current_date,2),'-','')
  and channel in ('1','2','3','7')
  and province_name is not  null and province_name<>''
group by   province_name
;

drop table b2b_tmp.factory_sale_412;
create temporary table b2b_tmp.factory_sale_412
as 
select
  province_name,
  sum(sales_value) as sales_value,
  sum(round_sales_value) as round_sales_value,
  sum(profit) as profit,
  sum(profit)/sum(sales_value) as profit_rate
from b2b_tmp.factory_sale_41
group by province_name
;


----省区整体当月销售
drop table b2b_tmp.factory_sale_42;
create temporary table b2b_tmp.factory_sale_42
as 
select
  province_name,
  sum(sales_value) as sales_value,
  0 as round_sales_value,
  sum(profit) as profit
from csx_dw.sale_goods_m1
where sdt <=regexp_replace(date_sub(current_date,1),'-','') 
  and sdt>=regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','')
  and channel in ('1','2','3','7')
  and province_name is not  null and province_name<>''
group by   province_name
union all
select
  province_name,
  0 as sales_value,
  sum(sales_value) as round_sales_value,
  0 as profit
from csx_dw.sale_goods_m1
where sdt <=regexp_replace(add_months(date_sub(current_date,1),-1),'-','') 
  and sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','')
  and channel in ('1','2','3','7')
  and province_name is not  null and province_name<>''
group by   province_name
;
drop table b2b_tmp.factory_sale_422;
create temporary table b2b_tmp.factory_sale_422
as 
select
  province_name,
  sum(sales_value) as sales_value,
  sum(round_sales_value) as round_sales_value,
  sum(profit) as profit,
  sum(profit)/sum(sales_value) as profit_rate
from b2b_tmp.factory_sale_42
group by province_name
;



--工厂_客户数
drop table b2b_tmp.factory_sale_43;
create temporary table b2b_tmp.factory_sale_43
as 
select
  province_name,
  sum(dake_number) as dake_number,
  sum(shangchao_num) as shangchao_num
from
(
select
  province_name,
  count(distinct customer_no) as dake_number,
  0 as shangchao_num
from csx_dw.sale_goods_m1
where sdt <=regexp_replace(date_sub(current_date,1),'-','') 
  and sdt>=regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','')
  and channel in ('1', '7')
  and province_name is not  null and province_name<>''
group by   province_name
union all
select
  province_name,
  0 as dake_number,
  count(distinct customer_no) as shangchao_num
from csx_dw.sale_goods_m1
where sdt <=regexp_replace(date_sub(current_date,1),'-','') 
  and sdt>=regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','')
  and channel in ('2', '3')
  and province_name is not  null and province_name<>''
group by   province_name
)aa
group by province_name
;


----工厂当月订单生产
drop table b2b_tmp.factory_sale_44;
create temporary table b2b_tmp.factory_sale_44
as 
select
  province_name,
  sum(p_total) as p_total,
  sum(transfer_quantity) as transfer_quantity,
  sum(p_transfer_values) as p_transfer_values
from 
(
  --新系统
  select
    province_name,
    p_total,
    transfer_quantity,
    p_transfer_values
  from b2b_tmp.factory_sale_21
  --旧系统
  union all
  select
    province_name,
    p_total,
    0 as transfer_quantity,
    0 as p_transfer_values
  from  b2b_tmp.factory_sale_22
  union all
  select
    province_name,
    0 as p_total,
    transfer_quantity,
    p_transfer_values
  from b2b_tmp.factory_sale_26
)aa
group by province_name
;




------工厂当天
insert overwrite table csx_dw.factory_day partition(sdt)
select
  bb.*
from
(
  select
    province_name,
    sum(sales_value)/10000 as sales_value,
    sum(round_sales_value)/10000 as round_sales_value,
    (sum(sales_value)/sum(round_sales_value)-1) as round_sales_rate,
    sum(profit)/10000 as profit,
    sum(profit_rate) as profit_rate,
    sum(factory_sales_value)/10000 as factory_sales_value,
    sum(factory_sales_value)/sum(sales_value) factory_percent,
    sum(factory_round_sales_value)/10000 as factory_round_sales_value,
    sum(factory_profit)/10000 as factory_profit,
    sum(factory_profit_rate) as factory_profit_rate,
    regexp_replace(date_sub(current_date,1),'-','') as sdt 
  from
  (
  select
    province_name,
    sales_value,
    round_sales_value,
    profit,
    profit_rate,
    0 as factory_sales_value,
    0 as factory_round_sales_value,
    0 as factory_profit,
    0 as factory_profit_rate
  from b2b_tmp.factory_sale_412
  union all
  select
    province_name,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    0 as profit_rate,
    sales_value as factory_sales_value,
    round_sales_value as factory_round_sales_value,
    profit as factory_profit,
    profit_rate  as  factory_profit_rate
  from  b2b_tmp.factory_sale_4
  ) aa
  group by province_name
)bb
where bb.sales_value<>0
;
------工厂当月
insert overwrite table csx_dw.factory_month partition(sdt)
select
  bb.*
from
(
  select
    province_name,
    sum(sales_value)/10000 as sales_value,
    sum(round_sales_value)/10000 as round_sales_value,
    (sum(sales_value)/sum(round_sales_value)-1) as round_sales_rate,
    sum(profit)/10000 as profit,
    sum(profit_rate) as profit_rate,
    sum(factory_sales_value)/10000 as factory_sales_value,
   -- sum(factory_result_rate) as factory_result_rate,
    sum(factory_sales_value)/sum(sales_value) factory_percent,
    sum(factory_round_sales_value)/10000 as factory_round_sales_value,
    sum(factory_profit)/10000 as factory_profit,
    sum(factory_profit_rate) as factory_profit_rate,
    sum(p_total)/10000 as p_total,
    --sum(transfer_quantity) as transfer_quantity,
    sum(p_transfer_values)/10000 as p_transfer_values,
    sum(dake_number) as dake_number,
    sum(shangchao_num)  as shangchao_num,
     regexp_replace(date_sub(current_date,1),'-','') as sdt  
  from
  (
  select
    province_name,
    sales_value,
    round_sales_value,
    profit,
    profit_rate,
    0 as factory_sales_value,
    0 as factory_result_rate,
    0 as factory_round_sales_value,
    0 as factory_profit,
    0 as factory_profit_rate,
    0 as p_total,
    0 as transfer_quantity,
    0 as p_transfer_values,
    0 as dake_number,
    0 as shangchao_num
  from b2b_tmp.factory_sale_422
  union all
  select
    province_name,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    0 as profit_rate,
    sales_value as factory_sales_value,
    result_rate as factory_result_rate,
    round_sales_value as factory_round_sales_value,
    profit as factory_profit,
    profit_rate  as  factory_profit_rate,
    0 as p_total,
    0 as transfer_quantity,
    0 as p_transfer_values,
    0 as dake_number,
    0 as shangchao_num
  from  b2b_tmp.factory_sale_7
  union all
  select
    province_name,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    0 as profit_rate,
    0 as factory_sales_value,
    0 as factory_result_rate,
    0 as factory_round_sales_value,
    0 as factory_profit,
    0 as factory_profit_rate,    
    p_total,
    transfer_quantity,
    p_transfer_values,
    0 as dake_number,
    0 as shangchao_num
  from b2b_tmp.factory_sale_44
  union all
  select
    province_name,
    0 as sales_value,
    0 as round_sales_value,
    0 as profit,
    0 as profit_rate,
    0 as factory_sales_value,
    0 as factory_result_rate,
    0 as factory_round_sales_value,
    0 as factory_profit,
    0 as factory_profit_rate,    
    0 as p_total,
    0 as transfer_quantity,
    0 as p_transfer_values,
    dake_number,
    shangchao_num
  from b2b_tmp.factory_sale_43
  ) aa
  group by province_name
)bb
where bb.sales_value<>0
;




