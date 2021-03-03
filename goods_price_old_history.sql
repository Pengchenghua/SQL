-- 报价系统商品报价

set mapreduce.job.queuename=caishixian;
-- -----------------计算老系统当天采购报价------------------------------------------
-- 最新的导入价格覆盖原来的价格
set hivevar:start_day='2019-12-12';
set hivevar:end_day='2019-12-12';
drop table b2b_tmp.tmp_daily_purchase_price_p1;
create temporary table b2b_tmp.tmp_daily_purchase_price_p1
as 
select 
  t1.*,
  t2.date_str
from 
(
  select 
    *
  from csx_ods.purchase_prices_ods 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '') and confirm = '1'
)t1 join 
(
  select  
    date_str
  from csx_dw.date_m 
  where date_str >= "${start_day}" and date_str <= "${end_day}"
)t2 on 1 = 1
where substr(t1.import_time, 1, 10) <= t2.date_str;

drop table b2b_tmp.tmp_daily_purchase_price;
create temporary table b2b_tmp.tmp_daily_purchase_price 
as 
select
  t1.*
from
(
  select 
    *,
    max(id) over(partition by date_str, warehouse_code, basis_type, product_code) as max_id
  from b2b_tmp.tmp_daily_purchase_price_p1
  where substr(price_begin_time, 1, 10) <= date_str and substr(price_end_time, 1, 10) >= date_str
)t1 left outer join 
(
  select distinct
     sdt,
     warehouse_code
  from csx_dw.goods_prices_m
  where sdt >= regexp_replace("${start_day}", '-', '') and sdt <= regexp_replace("${end_day}", '-', '')
)t2 on t1.warehouse_code = t2.warehouse_code and regexp_replace(t1.date_str, '-', '') = t2.sdt
where t2.warehouse_code is null and t1.id = t1.max_id;

---------------------- 计算老系统中台报价 ----------------------------------------------------------------
-- 商品报价
drop table b2b_tmp.tmp_daily_middle_price_p1;
create temporary table b2b_tmp.tmp_daily_middle_price_p1 
as 
select 
  t1.*,
  t2.date_str
from 
(
  select 
    *
  from csx_ods.middle_office_prices_ods 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '') and confirm = '1' 
)t1 join 
(
  select  
    date_str
  from csx_dw.date_m 
  where date_str >= "${start_day}" and date_str <= "${end_day}"
)t2 on 1 = 1
where substr(t1.operation_time, 1, 10) <= t2.date_str;


drop table b2b_tmp.tmp_daily_middle_price;
create temporary table b2b_tmp.tmp_daily_middle_price 
as 
select 
  warehouse_code,
  type,
  dimension_value_code as goods_code,
  warehouse_rate,
  delivery_rate,
  credit_rate,
  run_rate,
  date_str
from b2b_tmp.tmp_daily_middle_price_p1 
where dimension = '商品';

-- 小类报价处理
insert into b2b_tmp.tmp_daily_middle_price 
select 
  a.warehouse_code,
  a.type,
  b.goods_id as goods_code,
  a.warehouse_rate,
  a.delivery_rate,
  a.credit_rate,
  a.run_rate,
  a.date_str
from 
(
  select 
    * 
  from b2b_tmp.tmp_daily_middle_price_p1 
  where dimension = '小类'
)a left outer join 
(
  select distinct
    goods_id,
    category_small_code
  from csx_dw.goods_m 
  where sdt = 'current'
)b on a.dimension_value_code = b.category_small_code
left outer join b2b_tmp.tmp_daily_middle_price c 
on c.goods_code = b.goods_id and a.warehouse_code = c.warehouse_code and a.type = c.type 
  and a.date_str = c.date_str
where c.goods_code is null;

-- 中类报价处理
insert into b2b_tmp.tmp_daily_middle_price 
select 
  a.warehouse_code,
  a.type,
  b.goods_id as goods_code,
  a.warehouse_rate,
  a.delivery_rate,
  a.credit_rate,
  a.run_rate,
  a.date_str
from 
(
  select 
    * 
  from b2b_tmp.tmp_daily_middle_price_p1 
  where dimension = '中类'
)a left outer join 
(
  select distinct
    goods_id,
    category_middle_code
  from csx_dw.goods_m 
  where sdt = 'current'
)b on a.dimension_value_code = b.category_middle_code
left outer join b2b_tmp.tmp_daily_middle_price c 
on c.goods_code = b.goods_id and a.warehouse_code = c.warehouse_code and a.type = c.type 
  and a.date_str = c.date_str
where c.goods_code is null;

-- 大类报价处理
insert into b2b_tmp.tmp_daily_middle_price 
select 
  a.warehouse_code,
  a.type,
  b.goods_id as goods_code,
  a.warehouse_rate,
  a.delivery_rate,
  a.credit_rate,
  a.run_rate,
  a.date_str
from 
(
  select 
    * 
  from b2b_tmp.tmp_daily_middle_price_p1 
  where dimension = '大类'
)a left outer join 
(
  select distinct
    goods_id,
    category_large_code
  from csx_dw.goods_m 
  where sdt = 'current'
)b on a.dimension_value_code = b.category_large_code
left outer join b2b_tmp.tmp_daily_middle_price c 
on c.goods_code = b.goods_id and a.warehouse_code = c.warehouse_code and a.type = c.type 
  and a.date_str = c.date_str
where c.goods_code is null;

----------------------------- 整合报价数据 ---------------------------------
-- 插入老系统报价数据
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.support.quoted.identifiers=none;

insert overwrite table csx_dw.goods_prices_m partition(sdt, sys) 
select 
  a.id,
  a.warehouse_code,
  a.basis_type,
  b.type,
  a.product_code as goods_code,
  a.product_name as goods_name,
  a.unit,
  c.division_code,
  c.division_name,
  a.big_category_name as category_large_name,
  a.big_category_code,
  a.mid_category_name,
  a.mid_category_code,
  a.small_category_name,
  a.small_category_code,
  a.guide_price,
  a.purchase_price,
  a.price_begin_time,
  a.import_time,
  a.price_end_time,
  a.last_put_time,
  a.last_put_supplier,
  a.last_purchase_price,
  a.moving_average_price,
  b.warehouse_rate,
  b.delivery_rate,
  b.credit_rate,
  b.run_rate,
  0.0000 as joint_venture_rate,
  (1+b.credit_rate+b.run_rate) * a.purchase_price as cut_through_price,
  (1+b.credit_rate+b.run_rate) * a.purchase_price as through_price,
  (1+b.credit_rate+b.run_rate+b.warehouse_rate+b.delivery_rate) * a.purchase_price as distribute_price,
  (1+b.credit_rate+b.run_rate+b.warehouse_rate) * a.purchase_price as take_delivery_price,
  1 as channel,
  '' as business_type,
  price_end_time as import_end_time,
  regexp_replace(a.date_str, '-', '') as sdt,
  'old' as sys
from b2b_tmp.tmp_daily_purchase_price a 
left outer join b2b_tmp.tmp_daily_middle_price b on 
  a.warehouse_code = b.warehouse_code and a.product_code = b.goods_code and a.date_str = b.date_str
left outer join 
(
  select distinct
    goods_id,
    division_code,
    division_name
  from csx_dw.goods_m 
  where sdt = 'current'
)c on a.product_code = c.goods_id;