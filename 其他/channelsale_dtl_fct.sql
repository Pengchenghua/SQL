set mapreduce.job.reduces=80;
set hive.map.aggr = true;
set hive.groupby.skewindata=false;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.job.queuename=caishixian;

-- 181220 更新：刷新8月后历史数据
-- 181219 更新：增加未税销售与毛利
-- 180822 更新：存储每天渠道数据
drop table if exists b2b_tmp.tmp_csx_wl2shop_01;
drop table if exists b2b_tmp.tmp_csx_normalwl;
drop table if exists b2b_tmp.tmp_csx_normalwl_day;
drop table if exists b2b_tmp.tmp_csx_wl2shop_02;
drop table if exists b2b_tmp.tmp_csx_gc2wl;

drop table b2b_tmp.tmp_csx_shop_goods_fct;
create temporary table b2b_tmp.tmp_csx_shop_goods_fct
as
select 
  a.goodsid as mgoodsid, 
  b.*
from
(
  select 
    goodsid,
    shop_id 
  from csx_dw.csx_gc2md_ps
  where sdt>=regexp_replace(date_sub(current_date,30),'-','') 
  group by goodsid,shop_id
) a
join
(
  select 
    shop_id,
    goodsid,
    goodsname,
    vendor_id,
    vendor_name,
    brand,
    brand_name,
    catg_id,
    logistics_pattern,
    shop_goods_sts_id,
    efct_sign_id,
    prod_area,
    operation_type_id,
    project_block_id,
    sdt
  from dw.shop_goods_fct
  where sdt=regexp_replace(date_sub(current_date,1),'-','')
) b
on a.goodsid=b.goodsid and a.shop_id=b.shop_id;


-- 工厂-物流-门店配送 关联 门店销售
CREATE temporary table b2b_tmp.tmp_csx_wl2shop_01
as
select calday,goodsid,max(shopid_gc) shopid_gc,sum(qty_gc) qty_gc,sum(untax_salevalue_gc) untax_salevalue_gc,
max(shopid_wl) shopid_wl,sum(pur_qty_out) pur_qty_out,sum(pur_val_out) pur_val_out,
shop_id,sum(qty_md) qty_md,sum(salevalue_md) salevalue_md,sum(profit_md) profit_md,
sum(untax_salevalue_md) untax_salevalue_md,sum(untax_profit_md) untax_profit_md
from
(  
  select 
    sdt as calday,
    goodsid,
    shopid_gc,
    qty_gc,
    untax_salevalue_gc,
    shopid_wl,
    pur_qty_out,
    pur_val_out,
    shop_id,
    0 as qty_md,
    0 as salevalue_md,
    0 as profit_md,
    0 as untax_salevalue_md,
    0 as untax_profit_md
  from b2b.csx_gc2md_ps
  where sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','')
  union all
  select 
    a.sales_date as calday,
    coalesce(b.mgoodsid,a.goods_code) goodsid,
    null shopid_gc,
    0 as qty_gc,
    0 as untax_salevalue_gc,
    null shopid_wl,
    0 as pur_qty_out,
    0 as pur_val_out,
    a.shop_id,
    sum(sales_qty) as qty_md,
    sum(sales_value) as salevalue_md,
    sum(profit) as profit_md,
    sum(excluding_tax_sales) as untax_salevalue_md,
    sum(excluding_tax_profit) as untax_profit_md
  from
  ( 
    select * from csx_dw.sale_b2b_item
    where sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') 
      and sales_type in ('qyg','md', 'anhui','sc')
  ) a
  left join b2b_tmp.tmp_csx_shop_goods_fct b
  on a.shop_id=b.shop_id and a.goods_code=b.goodsid
  group by a.sales_date,coalesce(b.mgoodsid,a.goods_code),a.shop_id
) a
group by calday,goodsid,shop_id;


-- 正常对应的（物流非空）数据
CREATE temporary table b2b_tmp.tmp_csx_normalwl
as
select calday,goodsid,shop_id,shopid_wl,shopid_gc from b2b_tmp.tmp_csx_wl2shop_01
where shopid_wl is not NULL;


-- 缺失彩食鲜工厂以上次入库替代
CREATE temporary table b2b_tmp.tmp_csx_normalwl_day
as
select a.calday,a.goodsid,a.shop_id,max(b.calday) max_calday
from
(select calday,goodsid,shop_id from b2b_tmp.tmp_csx_wl2shop_01 where shopid_wl is NULL) a,
b2b_tmp.tmp_csx_normalwl b
where a.goodsid=b.goodsid and a.shop_id=b.shop_id and b.calday<a.calday
group by a.calday,a.goodsid,a.shop_id;

-- 关联工厂上次数据
CREATE temporary table b2b_tmp.tmp_csx_wl2shop_02
as
select 
  a.calday,
  a.goodsid,
  coalesce(a.shopid_gc,c.shopid_gc) shopid_gc,
  a.qty_gc,a.untax_salevalue_gc,
  coalesce(a.shopid_wl,c.shopid_wl) shopid_wl,
  a.pur_qty_out,
  a.pur_val_out,
  a.shop_id,
  a.qty_md,
  a.salevalue_md,
  a.profit_md,
  a.untax_salevalue_md,
  a.untax_profit_md
from b2b_tmp.tmp_csx_wl2shop_01 a
left join b2b_tmp.tmp_csx_normalwl_day b
  on a.calday=b.calday and a.goodsid=b.goodsid and a.shop_id=b.shop_id
left join b2b_tmp.tmp_csx_normalwl c
  on a.goodsid=c.goodsid and a.shop_id=c.shop_id and b.max_calday=c.calday;


CREATE temporary table b2b_tmp.tmp_csx_gc2wl
as
select 
  calday,
  goodsid,
  shopid_gc as shop_id,
  cust_id,
  max(distr_chan) as distr_chan,
  sum(qty) qty,
  sum(salevalue) salevalue,
  sum(profit) profit,
  sum(untax_salevalue) untax_salevalue,
  sum(untax_profit) untax_profit,
  sum(qty_md) qty_md,
  sum(salevalue_md) salevalue_md,
  sum(profit_md) profit_md,
  sum(untax_salevalue_md) untax_salevalue_md,
  sum(untax_profit_md) untax_profit_md
from
(
  select 
    sales_date as calday,
    goods_code as goodsid,
    shop_id as shopid_gc,
    customer_no as cust_id,
    sales_channel as distr_chan,
    sum(sales_qty) qty,
    sum(sales_value) salevalue,
    sum(profit) profit,
    sum(excluding_tax_sales) untax_salevalue,
    sum(excluding_tax_profit) untax_profit,
    0 as qty_md,
    0 as salevalue_md,
    0 as profit_md,
    0 as untax_salevalue_md,
    0 as untax_profit_md 
  from csx_dw.sale_b2b_item
  where sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and shop_id like 'W%' 
    and sales_type in ('qyg', 'gc', 'anhui','sc')
  group by sales_date,goods_code,shop_id,customer_no,sales_channel
  union all
  select calday,goodsid,shopid_gc,shopid_wl as cust_id, 0 as distr_chan,
    0 as qty,0 as salevalue,0 as profit,0 as untax_salevalue,0 as untax_profit,
    sum(qty_md) qty_md,sum(salevalue_md) salevalue_md,sum(profit_md) profit_md,
    sum(untax_salevalue_md) untax_salevalue_md,sum(untax_profit_md) untax_profit_md
  from b2b_tmp.tmp_csx_wl2shop_02
  where shopid_wl is not null
  group by calday,goodsid,shopid_gc,shopid_wl
) a
group by calday,goodsid,shopid_gc,cust_id;





-- 汇总 1128修改6_其他
drop table if exists b2b_tmp.tmp_csx_sale_03;
CREATE temporary table b2b_tmp.tmp_csx_sale_03
as
select 
  a.calday,
  a.shop_id,
  concat(a.shop_id,'-',substring(e.shop_name,1,2),'工厂') shop_name,
  e.sflag,
  a.distr_chan,
  a.cust_id,
  f.cust_name,
  coalesce(b.sflag,'9_其他') as channel_name,
  a.goodsid,
  g.goodsname,
  g.unit_name,
  coalesce(d.workshop_id,c.dept_id, g.dept_id, '') as workshop,
  coalesce(d.workshop_name,c.dept_name, g.dept_name, '') as tworkshop,
  coalesce(c.dept_id, g.dept_id, '') as sdept_id,
  coalesce(c.dept_name, g.dept_name, '') as sdept_name,
  coalesce(c.mat_type,'') mat_type,
  a.qty,
  a.salevalue,
  a.profit,
  a.untax_salevalue,
  a.untax_profit,
  a.qty_md,
  a.salevalue_md,
  a.profit_md,
  a.untax_salevalue_md,
  a.untax_profit_md
from b2b_tmp.tmp_csx_gc2wl a
join (select distinct shop_id,shop_name,sflag from b2b.dim_shops_current) e
  on a.shop_id=e.shop_id
left outer join
(
  select 
    goodsid, 
    dept_id, 
    dept_name, 
    regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') as goodsname,
    unit_name 
  from dim.dim_goods where edate='9999-12-31'
) g on a.goodsid=g.goodsid
left join b2b.dim_shops_current b on a.cust_id=concat('S', b.shop_id)
LEFT JOIN csx_ods.marc_ecc c on a.shop_id=c.shop_id and a.goodsid=c.goodsid
left join b2b.b2b_zcsx_dlcj_new d on coalesce(c.dept_id, g.dept_id) = d.dept_id
left join csx_ods.b2b_customer_new f
  on a.cust_id=f.cust_id;


drop table b2b_tmp.tmp_csx_sale_res;
CREATE temporary table b2b_tmp.tmp_csx_sale_res
as
select 
  calday,
  shop_id,
  shop_name,
  cust_id,
  cust_name,
  channel_name,
  goodsid,
  goodsname,
  unit_name,
  case when length(workshop)=1 then concat('0',workshop,'-',tworkshop)
    else concat(workshop,'-',tworkshop) end workshop_name,
  concat(sdept_id,'-',sdept_name) dept_name,
  mat_type,
  qty,
  salevalue,
  profit,
  untax_salevalue,
  untax_profit,
  qty_md,
  salevalue_md,
  profit_md,
  untax_salevalue_md,
  untax_profit_md,
  calday as sdt
from b2b_tmp.tmp_csx_sale_03;

set hive.exec.max.dynamic.partitions.pernode=10000;
-- 插入明细结果
insert overwrite table csx_dw.channelsale_dtl_fct
partition (sdt)
select * from b2b_tmp.tmp_csx_sale_res a;