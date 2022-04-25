-- 新老系统库存数据整合处理
-- 老系统：sap库存,九月前库存数据完全存在sap中
-- 新系统：9月自研系统上线，库存数据逐步切出
set
  mapreduce.job.queuename = caishixian;
set
  hive.exec.max.dynamic.partitions.pernode = 1000;
set
  hive.exec.max.dynamic.partitions = 1000;
set
  hive.exec.dynamic.partition.mode = nonstrict;
set
  hive.exec.dynamic.partition = true;
set
  hive.support.quoted.identifiers = none;
-- 新系统库存数据
  drop table b2b_tmp.tmp_accounting_stock_new;
create temporary table b2b_tmp.tmp_accounting_stock_new as
select
  concat(
    t1.location_code,
    '-',
    t1.reservoir_area_code,
    '-',
    shipper_code,
    '-',
    t1.product_code
  ) as id,
  t1.product_code as goods_code,
  t1.product_name as goods_name,
  t1.unit,
  t1.location_code as dc_code,
  t1.location_name as dc_name,
  t1.company_code,
  t1.company_name,
  t1.qty,
  t1.price,
  t1.amt,
  t1.amt_no_tax,
  t1.tax_rate / 100 as tax_rate,
  t1.update_time,
  t1.update_by,
  t1.create_time,
  t1.create_by,
  t1.shipper_code,
  t1.shipper_name,
  t1.valuation_category_code,
  t1.reservoir_area_code,
  t1.reservoir_area_name,
  t1.valuation_category_name,
  t1.is_bz_reservoir,
  t1.purchase_group_name,
  t2.date_str
from (
    select
      *
    from csx_ods.wms_accounting_stock_ods
    where
      sdt = regexp_replace(date_sub(current_date, 1), '-', '')
  ) t1
left outer join (
    select
      date_str
    from csx_dw.date_m
    where
      date_str >= to_date(current_date)
      and date_str <= date_sub(current_date, -1)
  ) t2 on 1 = 1
where
  t1.create_time <= t2.date_str;
-- 订单变动明细
  -- 新系统库存变动明细
  drop table b2b_tmp.tmp_accounting_stock_detail_new;
create temporary table b2b_tmp.tmp_accounting_stock_detail_new as
select
  t1.*,
  t2.date_str,
  max(id) over(
    partition by location_code,
    product_code,
    reservoir_area_code,
    t2.date_str
  ) as max_id
from (
    select
      id,
      location_code,
      product_code,
      reservoir_area_code,
      after_price,
      after_qty,
      after_amt,
      tax_rate,
      create_time
    from csx_dw.wms_accounting_stock_detail
    where
      sdt >= regexp_replace(date_sub(current_date, 1), '-', '')
  ) t1
left outer join (
    select
      date_str
    from csx_dw.date_m
    where
      date_str >= to_date(current_date)
      and date_str <= date_sub(current_date, -1)
  ) t2 on 1 = 1
where
  t1.create_time <= t2.date_str;
-- 根据库存变动明细,更新历史库存
  drop table b2b_tmp.tmp_accounting_stock_new_p1;
create temporary table b2b_tmp.tmp_accounting_stock_new_p1 as
select
  t1.id,
  t1.goods_code,
  t1.goods_name,
  t1.unit,
  t3.bar_code,
  t3.brand,
  t3.brand_name,
  t3.standard,
  t3.division_code,
  t3.division_name,
  t3.category_large_code,
  t3.category_large_name,
  t3.category_middle_code,
  t3.category_middle_name,
  t3.category_small_code,
  t3.category_small_name,
  t3.department_id,
  t3.department_name,
  t1.dc_code,
  t1.dc_name,
  t1.company_code,
  t1.company_name,
  coalesce(t2.after_qty, t1.qty) as qty,
  coalesce(t2.after_price, t1.price) as price,
  coalesce(t2.after_amt, t1.amt) as amt,
  coalesce(
    cast(
      t2.after_amt /(1 + t2.tax_rate / 100) as decimal(20, 6)
    ),
    t1.amt_no_tax
  ) as amt_no_tax,
  coalesce(t2.tax_rate / 100, t1.tax_rate) as tax_rate,
  t1.update_time,
  t1.update_by,
  t1.create_time,
  t1.create_by,
  t1.shipper_code,
  t1.shipper_name,
  t1.valuation_category_code,
  t1.reservoir_area_code,
  t1.reservoir_area_name,
  t1.valuation_category_name,
  t1.is_bz_reservoir,
  t1.purchase_group_name,
  regexp_replace(date_sub(t1.date_str, 1), '-', '') as sdt,
  'new' as sys
from b2b_tmp.tmp_accounting_stock_new t1
left outer join (
    select
      *
    from b2b_tmp.tmp_accounting_stock_detail_new
    where
      id = max_id
  ) t2 on t1.date_str = t2.date_str
  and t1.dc_code = t2.location_code
  and t1.reservoir_area_code = t2.reservoir_area_code
  and t1.goods_code = t2.product_code
left outer join (
    select
      distinct goods_id,
      bar_code,
      brand,
      brand_name,
      standard,
      division_code,
      division_name,
      category_large_code,
      category_large_name,
      category_middle_code,
      category_middle_name,
      category_small_code,
      category_small_name,
      department_id,
      department_name
    from csx_dw.goods_m
    where
      sdt = 'current'
  ) t3 on t1.goods_code = t3.goods_id;
-- 老系统库存数据从sap获得
  drop table b2b_tmp.tmp_accounting_stock_sap;
create temporary table b2b_tmp.tmp_accounting_stock_sap as
select
  t1.*,
  t2.shop_name,
  t2.company_name
from (
    select
      *
    from dw.inv_sap_setl_dly_fct
    where
      sdt >= regexp_replace(date_sub(current_date, 1), '-', '')
  ) t1
join (
    select
      distinct shop_id,
      shop_name,
      company_name
    from csx_dw.shop_m
    where
      sdt = 'current'
      and sales_dist_name like '%彩食鲜%'
  ) t2 on t1.shop_id = t2.shop_id;
-- 从老系统数据中过滤掉新系统数据
  drop table b2b_tmp.tmp_accounting_stock_sap_p1;
create temporary table b2b_tmp.tmp_accounting_stock_sap_p1 as
select
  concat(
    t1.shop_id,
    '-',
    t1.inv_place,
    '-',
    'YHCSX_',
    t1.goodsid
  ) as id,
  t1.goodsid as goods_code,
  t2.goods_name,
  t1.unit,
  t2.bar_code,
  t2.brand,
  t2.brand_name,
  t2.standard,
  t2.division_code,
  t2.division_name,
  t2.category_large_code,
  t2.category_large_name,
  t2.category_middle_code,
  t2.category_middle_name,
  t2.category_small_code,
  t2.category_small_name,
  t2.department_id,
  t2.department_name,
  t1.shop_id as dc_code,
  t1.shop_name as dc_name,
  t1.comp_code as company_code,
  t1.company_name,
  t1.inv_qty as qty,
  t1.cycle_unit_price as price,
  t1.inv_amt as amt,
  t1.inv_val as amt_no_tax,
  t1.in_vat_rare as tax_rate,
  t1.insert_time as update_time,
  '' as update_by,
  t1.calday as create_time,
  '' as create_by,
  'YHCSX' as shipper_code,
  '永辉彩食鲜' as shipper_name,
  '' as valuation_category_code,
  inv_place as reservoir_area_code,
  '' as reservoir_area_name,
  '' as valuation_category_name,
  '' as is_bz_reservoir,
  '' as purchase_group_name,
  t1.sdt,
  'old' as sys
from b2b_tmp.tmp_accounting_stock_sap t1
left outer join (
    select
      distinct goods_id,
      unit_name,
      goods_name,
      bar_code,
      brand,
      brand_name,
      standard,
      division_code,
      division_name,
      category_large_code,
      category_large_name,
      category_middle_code,
      category_middle_name,
      category_small_code,
      category_small_name,
      department_id,
      department_name
    from csx_dw.goods_m
    where
      sdt = 'current'
  ) t2 on t1.goodsid = t2.goods_id
left outer join (
    select
      distinct dc_code
    from b2b_tmp.tmp_accounting_stock_new
  ) t3 on t1.shop_id = t3.dc_code
where
  t3.dc_code is null;
-- 库存数据入库
insert overwrite table csx_dw.wms_accounting_stock_m partition (sdt, sys)
select
  *
from b2b_tmp.tmp_accounting_stock_new_p1 t1
union all
select
  *
from b2b_tmp.tmp_accounting_stock_sap_p1;