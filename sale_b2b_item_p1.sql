set hive.map.aggr = true;
set hive.groupby.skewindata=false;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set mapreduce.job.queuename=caishixian;
set hive.support.quoted.identifiers=none;
-- 汇总sap销售表，用来补足发票表缺失字段
drop table b2b_tmp.tmp_sap_sale;
create temporary table b2b_tmp.tmp_sap_sale 
as  
select distinct
  a.sdt,
  a.shop_id,
  a.goods_code,
  a.return_flag,
  a.bill_type,
  a.customer_no,
  a.sales_channel,
  a.promotion_code,
  a.vendor_code,
  a.storage_location,
  a.customer_group
from 
(
  select 
    sdt, 
    shop_id, 
    regexp_replace(goodsid, '(^0*)', '') as goods_code, 
    is_return as return_flag, 
    bill_type, 
    regexp_replace(cust_id, '(^0*)', '') as customer_no,
    distr_channel as sales_channel,
    pro_id as promotion_code,
    vendor_id as vendor_code,
    inv_place as storage_location,
    cust_grp as customer_group
  from dw.sale_sap_dtl_fct a
  where sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') 
  group by sdt, shop_id, regexp_replace(goodsid, '(^0*)', ''), is_return, bill_type, 
    regexp_replace(cust_id, '(^0*)', ''), distr_channel, pro_id, bar_code, div_id, 
    vendor_id, inv_place, cust_grp
)a join 
(
  select distinct shop_id from csx_dw.shop_m
  where sales_belong_flag in ('4_企业购', '5_彩食鲜')
)c on a.shop_id = c.shop_id;

-- 查询近两个月的发票数据,并关联门店的财务主体和客户的财务主体
drop table b2b_tmp.tmp_sales_2m;
create temporary table b2b_tmp.tmp_sales_2m 
as 
select
  a.shop_id,
  if(a.customer_no = '910001', s.province_code, a.customer_no) as customer_no,
  a.sales_date,
  a.sales_channel,
  a.promotion_code,
  a.goods_code, 
  d.bar_code,
  d.unit_name as unit,
  d.division_code,
  d.category_large_code,
  d.category_middle_code,
  d.category_small_code,
  a.sales_qty,
  a.sales_value,  
  a.sales_cost,
  a.profit,
  0.0 as promotion_deduction,
  a.excluding_tax_sales,
  a.excluding_tax_cost,
  a.excluding_tax_profit,
  0.0 as excluding_tax_deduction,
  a.tax_value,
  0.0 as sales_tax_rate,
  0.0 as cost_tax_rate,
  c.vendor_code,
  '' as run_type,
  a.storage_location,
  a.bill_type,
  a.return_flag,
  a.customer_group,
  a.origin_shop_id,
  a.sales_date as sdt,
  b.sales_belong_flag,
  b.financial_body as shop_financial_body,
  e.financial_body as customer_financial_body
from
(
  select
    shop_id,
    origin_shop_id,
    regexp_replace(sold_to, '(^0*)', '') as customer_no,
    sdt as sales_date,
    distr_chan as sales_channel,
    rt_promo as promotion_code,
    regexp_replace(material, '(^0*)', '') as goods_code,
    stor_loc as storage_location,
    bill_type,
    is_return as return_flag,
    cust_group as customer_group,
    cast(sum(sale_qty) as decimal(26, 3)) as sales_qty,
    cast(sum(tax_sale_val) as decimal(26, 2)) as sales_value,
    cast(sum(tax_cost) as decimal(26, 2)) as sales_cost,
    cast(sum(tax_profit) as decimal(26, 2)) as profit,
    cast(sum(untax_sale_val) as decimal(26, 2)) as excluding_tax_sales,
    cast(sum(untax_cost) as decimal(26, 2)) as excluding_tax_cost,
    cast(sum(untax_profit) as decimal(26, 2)) as excluding_tax_profit,
    cast(sum(cast(tax_amount as double)) as decimal(26, 2)) as tax_value  
  from csx_dw.csx_order_item 
  where sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') 
    and shop_id <> 'W098' 
  group by shop_id, regexp_replace(sold_to, '(^0*)', ''), sdt, distr_chan, rt_promo, 
    regexp_replace(material, '(^0*)', ''), stor_loc, bill_type, is_return, 
    cust_group,origin_shop_id
)a join 
(
  select distinct shop_id, province_name, sales_belong_flag, financial_body from csx_dw.shop_m 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)b on a.shop_id = b.shop_id
left outer join 
(
  select distinct shop_id, financial_body from csx_dw.shop_m 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)e on regexp_replace(a.customer_no, 'S', '') = e.shop_id
left outer join 
(
  select distinct 
    goods_id,
    bar_code,
    unit_name,
    division_code,
    category_large_code,
    category_middle_code,
    category_small_code
  from csx_dw.goods_m
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '')
)d on a.goods_code = d.goods_id
left outer join b2b_tmp.tmp_sap_sale c
  on a.sales_date = c.sdt and a.origin_shop_id = c.shop_id and a.customer_no = c.customer_no and 
    a.sales_channel = c.sales_channel and a.promotion_code = c.promotion_code and a.goods_code = c.goods_code and 
    a.storage_location = c.storage_location and a.bill_type = c.bill_type and 
    a.return_flag = c.return_flag and a.customer_group = c.customer_group
left outer join csx_ods.sys_province_ods s on b.province_name = s.province;


-- W0H4: 通过WH04出去到彩食鲜门店的是属于调拨,不统计,到永辉门店的属于销售,算供应链的业绩，
--   目前统计再qyg里。 其它彩食门店只统计bill_type为销售类型的,
--   其它门店销售到永辉门店不计算业绩
drop table b2b_tmp.tmp_sales_2m_qyg;
create temporary table b2b_tmp.tmp_sales_2m_qyg
as 
select 
  *
from b2b_tmp.tmp_sales_2m 
where bill_type in ('','S1','S2','ZF1','ZF2','ZR1','ZR2','ZFP','ZFP1') and sales_belong_flag <> '5_彩食鲜'
union all
select 
  *
from b2b_tmp.tmp_sales_2m
where shop_id = 'W0H4' and (customer_financial_body is null or customer_financial_body = 2) and 
  bill_type not in ('','S1','S2','ZF1','ZF2','ZR1','ZR2','ZFP','ZFP1'); 

-- 插入企业购数据
insert overwrite table csx_dw.sale_b2b_item partition(sdt, sales_type)
select 
  `(sales_belong_flag|shop_financial_body|customer_financial_body)?+.+`,
  'qyg' as sales_type
from b2b_tmp.tmp_sales_2m_qyg;

-- 工厂或彩食鲜dc到属于彩食鲜财务主题的门店属于调拨, 工厂销售到非彩食鲜财务主体的门店属于销售
insert overwrite table csx_dw.sale_b2b_item partition(sdt, sales_type) 
select 
  `(sales_belong_flag|shop_financial_body|customer_financial_body)?+.+`,
  'gc' as sales_type
from b2b_tmp.tmp_sales_2m 
where sales_belong_flag = '5_彩食鲜' and (customer_financial_body is null or customer_financial_body <> shop_financial_body);

-- 将属于调拨的数据插入到物流分区
insert overwrite table csx_dw.sale_b2b_item partition(sdt, sales_type)
select 
  `(sales_belong_flag|shop_financial_body|customer_financial_body)?+.+`,
  'wl' as sales_type
from b2b_tmp.tmp_sales_2m 
where sales_belong_flag in ('5_彩食鲜', '4_企业购') and customer_financial_body = shop_financial_body;

-- 将企业购到门店的数据插入到企业购门店分区
insert overwrite table csx_dw.sale_b2b_item partition(sdt, sales_type)
select 
 `(sales_belong_flag|shop_financial_body|customer_financial_body)?+.+`,
  'qyg_md' as sales_type
from b2b_tmp.tmp_sales_2m 
where sales_belong_flag = '4_企业购' and bill_type not in ('','S1','S2','ZF1','ZF2','ZR1','ZR2','ZFP','ZFP1') 
  and shop_id <> 'W0H4' and (customer_financial_body is null or customer_financial_body <> shop_financial_body);

