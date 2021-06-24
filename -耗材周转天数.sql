-- 耗材周转天数 
--周转天数=期末库存量/30天领用量
set edt='${enddate}';
set e_date=regexp_replace(${hiveconf:edt},'-','');
set s_date=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
--set s_date=regexp_replace(add_months(${hiveconf:edt},-30),'-','');
--select ${hiveconf:s_date};
--期末库存
drop table if exists csx_tmp.temp_fact_receipt;
create temporary table csx_tmp.temp_fact_receipt as 
select 
    zone_id,
    zone_name,
    province_code,
    province_name,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    bar_code,
    brand_name,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    department_id,
    department_name,
    end_inventoty_qty,
    end_inventoty_amt,
    receipt_amt,
    receipt_qty,
    material_take_amt,
    material_take_qty,
    purpose
from 
(select 
    zone_id,
    zone_name,
    province_code,
    province_name,
    location_code,
    dc_code,
    shop_name,
    goods_code,
    purpose,
    sum(end_inventoty_qty) as end_inventoty_qty,
    sum(end_inventoty_amt) as end_inventoty_amt,
    sum(receipt_amt) as receipt_amt,
    sum(receipt_qty) as receipt_qty,
    sum(material_take_amt) as material_take_amt,
    sum(material_take_qty) as material_take_qty
from (
select dc_code,
    goods_code,
    qty as end_inventoty_qty,
    amt as end_inventoty_amt,
    0 as receipt_amt,
    0 as receipt_qty,
    0 as  material_take_amt,
    0 as material_take_qty
from csx_dw.dws_wms_r_d_accounting_stock_m 
where sdt=${hiveconf:e_date}
    and division_code='15'
    and reservoir_area_code not in ('PD01','PD02','TS01')
union all 
select location_code as dc_code,
    product_code as goods_code,
    0 as end_inventoty_qty,
    0 as end_inventoty_amt,
    sum(case when move_type = '118A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '118B' then amt_no_tax*(1+tax_rate/100 )*-1  end) receipt_amt,
    sum(case when move_type = '118A' then txn_qty  when  move_type = '118B' then txn_qty*-1 end) receipt_qty,
    sum(case when move_type = '119A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '119B' then amt_no_tax*(1+tax_rate/100 )*-1  end) material_take_amt,
    sum(case when move_type = '119A' then txn_qty  when  move_type = '119B' then txn_qty*-1 end) material_take_qty
from csx_dw.dwd_cas_r_d_accounting_stock_detail
where sdt>=${hiveconf:s_date}
    and sdt<=${hiveconf:e_date}
    group by location_code,
    product_code
) a 
join 
(select zone_id,
zone_name,
province_code,
province_name,
location_code,
shop_name ,
purpose
from csx_dw.csx_shop where sdt='current' 
 and location_type_code in ('2','1')
-- and dist_code='20'
) b on a.dc_code=b.location_code
group by zone_id,
    zone_name,
    province_code,
    province_name,
    location_code,
    dc_code,
    shop_name,
    goods_code,
    purpose
) a 
join 
(SELECT goods_id,
       goods_name,
       bar_code,
       brand_name,
       division_code,
       division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       department_id,
       department_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
   -- and division_code='15'
and category_middle_code in ('150602','150105','150102','150101','150601','150601','150118','150704','150131','150133','150702','150106','150108','150701','150122','150120','150107')) b on a.goods_code=b.goods_id;


insert overwrite directory '/tmp/pengchenghua/ii' row format delimited fields terminated by '\t'
select  zone_id,
    zone_name,
    province_code,
    province_name,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    bar_code,
    brand_name,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    department_id,
    department_name,
    end_inventoty_qty,
    end_inventoty_amt,
    receipt_amt,
    receipt_qty,
    material_take_amt,
    material_take_qty,
coalesce(receipt_qty+material_take_qty,0) as use_qty,
coalesce(receipt_amt+material_take_amt,0) as use_qty,
coalesce(case when coalesce(receipt_qty+material_take_qty,0)<=0 and end_inventoty_qty<>0 then 9999 
else (end_inventoty_qty/(receipt_qty+material_take_qty))*30 end,0) as turn_day 
from csx_tmp.temp_fact_receipt 
;



-- select * from csx_dw.dws_basic_w_a_csx_product_m where goods_id='925871' and sdt='current';



-- 耗材周转天数 
--周转天数=期末库存量/30天领用量
set edt='${enddate}';
set e_date=regexp_replace(${hiveconf:edt},'-','');
set s_date=regexp_replace(trunc(date_sub(${hiveconf:edt},31),'MM'),'-','');
-- set s_date=regexp_replace(add_months(${hiveconf:edt},-60),'-','');
--select ${hiveconf:s_date};
--期末库存
drop table if exists csx_tmp.temp_fact_receipt;
create temporary table csx_tmp.temp_fact_receipt as 
select 
    zone_id,
    zone_name,
    province_code,
    province_name,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    bar_code,
    brand_name,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    department_id,
    department_name,
    end_inventoty_qty,
    end_inventoty_amt,
    receipt_amt,
    receipt_qty,
    material_take_amt,
    material_take_qty,
    purpose
from 
(select 
    zone_id,
    zone_name,
    province_code,
    province_name,
    location_code,
    dc_code,
    shop_name,
    goods_code,
    purpose,
    sum(end_inventoty_qty) as end_inventoty_qty,
    sum(end_inventoty_amt) as end_inventoty_amt,
    sum(receipt_amt) as receipt_amt,
    sum(receipt_qty) as receipt_qty,
    sum(material_take_amt) as material_take_amt,
    sum(material_take_qty) as material_take_qty
from (
select dc_code,
    goods_code,
    qty as end_inventoty_qty,
    amt as end_inventoty_amt,
    0 as receipt_amt,
    0 as receipt_qty,
    0 as  material_take_amt,
    0 as material_take_qty
from csx_dw.dws_wms_r_d_accounting_stock_m 
where sdt=${hiveconf:e_date}
    and division_code='15'
    and reservoir_area_code not in ('PD01','PD02','TS01')
union all 
select location_code as dc_code,
    product_code as goods_code,
    0 as end_inventoty_qty,
    0 as end_inventoty_amt,
    sum(case when move_type = '118A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '118B' then amt_no_tax*(1+tax_rate/100 )*-1  end) receipt_amt,
    sum(case when move_type = '118A' then txn_qty  when  move_type = '118B' then txn_qty*-1 end) receipt_qty,
    sum(case when move_type = '119A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '119B' then amt_no_tax*(1+tax_rate/100 )*-1  end) material_take_amt,
    sum(case when move_type = '119A' then txn_qty  when  move_type = '119B' then txn_qty*-1 end) material_take_qty
from csx_dw.dwd_cas_r_d_accounting_stock_detail
where sdt>=${hiveconf:s_date}
    and sdt<=${hiveconf:e_date}
    group by location_code,
    product_code
) a 
join 
(select zone_id,
zone_name,
province_code,
province_name,
location_code,
shop_name ,
purpose
from csx_dw.csx_shop where sdt='current' 
 and location_type_code in ('2','1')
-- and dist_code='20'
) b on a.dc_code=b.location_code
group by zone_id,
    zone_name,
    province_code,
    province_name,
    location_code,
    dc_code,
    shop_name,
    goods_code,
    purpose
) a 
join 
(SELECT goods_id,
       goods_name,
       bar_code,
       brand_name,
       division_code,
       division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       department_id,
       department_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
   -- and division_code='15'
and category_middle_code in ('150602','150105','150102','150101','150601','150601','150118','150704','150131','150133','150702','150106','150108','150701','150122','150120','150107')) b on a.goods_code=b.goods_id;


insert overwrite directory '/tmp/pengchenghua/ii' row format delimited fields terminated by '\t'
select  zone_id,
    zone_name,
    province_code,
    province_name,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    bar_code,
    brand_name,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    department_id,
    department_name,
    end_inventoty_qty,
    end_inventoty_amt,
    receipt_amt,
    receipt_qty,
    material_take_amt,
    material_take_qty,
coalesce(receipt_qty+material_take_qty,0) as use_qty,
coalesce(receipt_amt+material_take_amt,0) as use_qty,
coalesce(case when coalesce(receipt_qty+material_take_qty,0)<=0 and end_inventoty_qty<>0 then 9999 
else (end_inventoty_qty/(receipt_qty+material_take_qty))*60 end,0) as turn_day 
from csx_tmp.temp_fact_receipt 
;



-- select * from csx_dw.dws_basic_w_a_csx_product_m where goods_id='925871' and sdt='current';