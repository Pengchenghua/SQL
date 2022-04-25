set mapreduce.job.queuename=caishixian;

-- 从DC W0B6 出去得算BBC，从W0H4出去且客户为门店得算供应链（S端）
--根据战报表逻辑生成近两个月的销售表，添加渠道，省区字段
-- 大客户有包含"平台"
drop table b2b_tmp.sale_goods_m_1;
create temporary table b2b_tmp.sale_goods_m_1
as
select
  shop_id,
  shop_name,
  customer_no,
  customer_name,
  sales_date,
  sales_channel,
  sales_id,
  sales_name, -- 销售员
  work_no,
  sales_supervisor_id,  --销售主管
  sales_supervisor_name,
  sales_supervisor_work_no,
  sales_manager_id, -- 销售经理
  sales_manager_name,
  sales_manager_work_no,
  city_manager_id,
  city_manager_name,  -- 城市经理
  city_manager_work_no,
  province_manager_id,
  province_manager_name,  -- 省区经理
  province_manager_work_no,
  first_category,
  first_category_code,
  second_category,
  second_category_code,
  third_category,
  third_category_code,
  promotion_code,
  aa.goods_code,
  aa.bar_code,
  goods_name,
  brand,
  brand_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  price_zone,
  price_zone_name,
  firm_code,
  firm_name,
  category_code,
  category_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  standard,
  unit,
  sales_qty,
  sales_value,
  sales_cost,
  profit,
  promotion_deduction,
  excluding_tax_sales,
  excluding_tax_cost,
  excluding_tax_profit,
  excluding_tax_deduction,
  tax_value,
  sales_tax_rate,
  cost_tax_rate,
  regexp_replace(vendor_code,'(^0*)','') as vendor_code,
  run_type,
  storage_location,
  bill_type,
  return_flag,
  customer_group,
  origin_shop_id,
  sdt,
  sales_type,
  channel,
  case
    when channel = '1'  OR channel = '' then '大客户'
    when channel = '2' then '商超(对内)'
    when channel = '3' then '商超(对外)'
    when channel = '4' then '大宗'
    when channel = '5' then '供应链(食百)'
    when channel = '6' then '供应链(生鲜)'
    when channel = '7' then '企业购'
    when channel = '8' then '其它'
  end as channel_name,
  case
    when channel = '5' then '平台-食百采购'
    when channel = '6' then '平台-生鲜采购'
    when channel = '4' then '平台-大宗'
    else aa.province_name
  end as province_name,
  city_name,
  region_city
from
(
select
  a.shop_id,
  d.shop_name,
  a.customer_no,
  b.customer_name,
  a.sales_date,
  a.sales_channel,
  b.sales_id,
  sales_name, -- 销售员
  work_no,
  sales_supervisor_id,--销售主管
  sales_supervisor_name,
  sales_supervisor_work_no,
  sales_manager_id, -- 销售经理
  sales_manager_name,
  sales_manager_work_no,
  city_manager_id,
  city_manager_name,  -- 城市经理
  city_manager_work_no,
  province_manager_id,
  province_manager_name,  -- 省区经理
  province_manager_work_no,
  first_category,
  first_category_code,
  second_category,
  second_category_code,
  third_category,
  third_category_code,
  a.promotion_code,
  a.goods_code,
  a.bar_code,
  goodsname as goods_name,
  brand,
  brand_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  price_zone,
  price_zone_name,
  firm_code,
  firm_name,
  div_id as category_code,
  div_name as category_name,
  catg_l_id as category_large_code,
  catg_l_name as category_large_name,
  catg_m_id as category_middle_code,
  catg_m_name as category_middle_name,
  catg_s_id as category_small_code,
  catg_s_name as category_small_name,
  standard,
  unit,
  a.sales_qty,
  a.sales_value,
  a.sales_cost,
  a.profit,
  a.promotion_deduction,
  a.excluding_tax_sales,
  a.excluding_tax_cost,
  a.excluding_tax_profit,
  a.excluding_tax_deduction,
  a.tax_value,
  a.sales_tax_rate,
  a.cost_tax_rate,
  a.vendor_code,
  a.run_type,
  a.storage_location,
  a.bill_type,
  a.return_flag,
  a.customer_group,
  a.origin_shop_id,
  a.sdt,
  a.sales_type,
  c.city_name,
  b.sales_city region_city,
  case when a.origin_shop_id = 'W0B6' or b.channel like '%企业购%' then '7'
    when (a.shop_id = 'W0H4' and a.customer_no like 'S%' and category_code in ('12','13','14') ) 
      or (b.channel like '供应链%' and category_code in ('12','13','14'))then '5'
    when (a.shop_id = 'W0H4' and a.customer_no like 'S%' and category_code in ('10','11'))
      or (b.channel like '供应链%' and category_code in ('10', '11'))then '6'  
    when (b.channel = '大客户' or b.channel = 'B端') then '1'
    when (b.channel ='M端'  or b.channel like '%对内%') then '2'
    when b.channel like '%对外%' then '3'
    when b.channel = '大宗' then '4'  
    when (channel='其他'or  channel='其它') then '8'
    else ''
  end as channel,

   case
     when a.shop_id in ('W0M1','W0M4','W0J6','W0M6','W0K5') then '商超平台' 
     when b.customer_no is not null and b.sales_province='BBC' then '福建省'
     when b.sales_province is not null and b.channel <> 'M端' and b.channel not like '商超%'  then b.sales_province
     when a.customer_no like 'S%' and substr(c.province_name, 1, 2) 
       in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') 
       then c.province_name
     else d.province_name end as province_name
from
 (
   select
     shop_id,
     customer_no,
     sales_date,
     sales_channel,
     promotion_code,
     goods_code,
     bar_code, 
     category_code,
     category_large_code,
     category_middle_code,
     category_small_code,
     unit,
     sales_qty,
     sales_value,
     sales_cost,
     profit,
     promotion_deduction,
     excluding_tax_sales,
     excluding_tax_cost,
     excluding_tax_profit,
     excluding_tax_deduction,
     tax_value,
     sales_tax_rate,
     cost_tax_rate,
     vendor_code,
     run_type,
     storage_location,
     bill_type,
     return_flag,
     customer_group,
     origin_shop_id,
     sdt,
     sales_type
   from csx_dw.sale_b2b_item 
   where   sales_type in('qyg','gc','anhui','sc') 
    and sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,6),'01')
    and sdt<=regexp_replace(date_sub(current_date,1),'-','')
 )a 
 left outer join
(select 
  goodsid,
  goodsname,
  brand,
  brand_name,
  standard,
  div_id,
  div_name,
  catg_l_id,
  catg_l_name,
  catg_m_id,
  catg_m_name,
  catg_s_id,
  catg_s_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  price_zone,
  price_zone_name,
  firm_g1_id as firm_code,
  firm_g1_name firm_name
  from dim.dim_goods where edate='9999-12-31'
)f on  a.goods_code=f.goodsid
 left outer join 
 (
   select
  customer_no,
  customer_name,
  sales_id,
  sales_name, -- 销售员
  work_no,
  first_supervisor_code as sales_supervisor_id,--销售主管
  first_supervisor_name as sales_supervisor_name,
  first_supervisor_work_no as sales_supervisor_work_no,
  second_supervisor_code as sales_manager_id,-- 销售经理
  second_supervisor_name as sales_manager_name, 
  second_supervisor_work_no as  sales_manager_work_no,
  third_supervisor_code as city_manager_id,
  third_supervisor_name as city_manager_name, -- 城市经理
  third_supervisor_work_no as city_manager_work_no,
  fourth_supervisor_code as  province_manager_id,
  fourth_supervisor_name as province_manager_name,-- 省区经理
  fourth_supervisor_work_no as province_manager_work_no,
  sales_province_code,
  sales_province,
  sales_city,
  channel,
  first_category,
  first_category_code,
  second_category,
  second_category_code,
  third_category,
  third_category_code
from
  csx_dw.customer_m
   where sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')  
     and source <>'fix' and customer_no <> '' 
 )b on a.customer_no = b.customer_no 
 left outer join 
 (
   select
  shop_id,
  case
    when shop_id in ('W055','W056') then '上海市'  else province_name
  end province_name,
  case
    when province_name like '%市' then province_name
    else city_name
  end city_name     
   from csx_dw.shop_m 
   where sdt = 'current'
 )c on
a.customer_no = concat('S',c.shop_id)
left outer join (
select
  shop_id,
  shop_name,
  province_name
from
  csx_dw.shop_m
where
  sdt = 'current' )d on
regexp_replace(a.shop_id,'(^E)','') = d.shop_id )aa ;

-- select DISTINCT channel_name from b2b_tmp.sale_goods_m_1;

--福建省的要划分到市，其余省份无该需求
drop table b2b_tmp.sale_goods_m_2;
create temporary table b2b_tmp.sale_goods_m_2
as
select
  a.channel,
  a.channel_name,
  a.shop_id,
  a.shop_name,
  a.sales_date,
  a.sales_channel,
  g.province_code,
  case when a.province_name='平台-B' then '大客户平台' else a.province_name end province_name,
  '' as city_code,
  a.city_name,
  case
    when a.province_name = '福建省' then coalesce(b.city_real,'福州、宁德、三明')  else '-' end city_real,
  case
    when a.province_name = '福建省' then coalesce(b.cityjob,'沈锋')else '-' end cityjob,
  a.customer_no,
  a.customer_name,
  a.sales_id,
  sales_name,
  sales_work_no,
  sales_supervisor_id,--销售主管
    sales_supervisor_name,
  sales_supervisor_work_no,
  sales_manager_id, -- 销售经理
    sales_manager_name,
  sales_manager_work_no,
  city_manager_id,
  city_manager_name,  -- 城市经理
    city_manager_work_no,
  province_manager_id,
  province_manager_name,  -- 省区经理
    province_manager_work_no,
  first_category,
  first_category_code,
  second_category,
  second_category_code,
  third_category,
  third_category_code,
  a.promotion_code,
  a.goods_code,
  a.goods_name,
  a.bar_code,
  a.brand,
  a.brand_name,
  bd_id,
  bd_name,
  firm_code,
  firm_name,
  category_code,
  category_name,
  dept_id,
  dept_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  price_zone,
  price_zone_name,
  standard,
  unit,
  sales_qty,
  sales_value,
  sales_cost,
  profit,
  promotion_deduction,
  excluding_tax_sales,
  excluding_tax_cost,
  excluding_tax_profit,
  excluding_tax_deduction,
  tax_value,
  sales_tax_rate,
  cost_tax_rate,
  vendor_code,
  vendor_name,
  run_type,
  storage_location,
  bill_type,
  return_flag,
  customer_group,
  origin_shop_id,
  sdt,
  sales_type
from
(
 select
  shop_id,
  shop_name,
  customer_no,
  customer_name,
  sales_date,
  sales_channel,
  sales_id,
  sales_name,
  work_no as sales_work_no ,
  sales_supervisor_id,--销售主管
    sales_supervisor_name,
  sales_supervisor_work_no,
  sales_manager_id, -- 销售经理
    sales_manager_name,
  sales_manager_work_no,
  city_manager_id,
  city_manager_name,  -- 城市经理
    city_manager_work_no,
  -- province_manager_id,
  -- province_manager_name, -- 省区经理
    -- province_manager_work_no,
  first_category,
  first_category_code,
  second_category,
  second_category_code,
  third_category,
  third_category_code,
  promotion_code,
  goods_code,
  bar_code,
  goods_name,
  brand,
  brand_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  price_zone,
  price_zone_name,
  category_code,
  category_name,
  firm_code,
  firm_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  standard,
  unit,
  sales_qty,
  sales_value,
  sales_cost,
  profit,
  promotion_deduction,
  excluding_tax_sales,
  excluding_tax_cost,
  excluding_tax_profit,
  excluding_tax_deduction,
  tax_value,
  sales_tax_rate,
  cost_tax_rate,
  regexp_replace(vendor_code, '(^0*)','') as vendor_code,
  run_type,
  storage_location,
  bill_type,
  return_flag,
  customer_group,
  origin_shop_id,
  sdt,
  sales_type,
    case when channel is null or channel='' then '1' when province_name='平台-B' and channel='1' then '1' else channel end channel,
    case when channel is null or channel='' then '大客户' when province_name='平台-B' and channel='1' then '大客户' else channel_name end channel_name,
    case when province_name ='成都' then '四川省' when channel='7' then '福建省' else province_name end province_name,
    case when channel='2'  then city_name 
      when channel='7' then '福州' 
      when (channel<>'2')  then region_city else '-' 
      end city_name
  from b2b_tmp.sale_goods_m_1
)a
left outer join
(
  select '泉州'city,'泉州'city_real,'张铮'cityjob
  union all 
  select '莆田'city,'莆田'city_real,'倪薇红'cityjob
  union all 
  select '南平'city,'南平'city_real,'林挺'cityjob
  union all 
  select '厦门'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
  union all 
  select '漳州'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
  union all 
  select '龙岩'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
  union all 
  select '福州'city,'福州、宁德、三明'city_real,'沈锋'cityjob
  union all 
  select '宁德'city,'福州、宁德、三明'city_real,'沈锋'cityjob
  union all 
  select '三明'city,'福州、宁德、三明'city_real,'沈锋'cityjob
)b on substr(a.city_name,1,2)=b.city
left outer join 
(
  select
    province_code,
    province
  from csx_ods.sys_province_ods
)g on a.province_name=g.province
left join 
(select vendor_id,
  vendor_name 
 from dim.dim_vendor where edate='9999-12-31') p  on regexp_replace(vendor_code,'(^0*)','')= regexp_replace(vendor_id,'(^0*)','')
 left outer join 
(
-- 插入省区总信息
  select 
    sales_id as province_manager_id,
    sales_name as province_manager_name,
    work_no as province_manager_work_no,
    province_name
  from csx_dw.region_data_permission 
  where region_permission = 3
)h on a.province_name = h.province_name
;


set hive.map.aggr = true;
set hive.groupby.skewindata=false;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true; -- 开启动态分析
set hive.exec.dynamic.partition.mode=nonstrict; -- 动态分区模式
set hive.exec.max.dynamic.partitions.pernode=10000;
insert  overwrite table csx_dw.sale_goods_m partition(sdt,sales_type)
select
  channel ,
  channel_name ,
  shop_id ,
  shop_name ,
  sales_date ,
  sales_channel ,
  province_code ,
  province_name ,
  city_code ,
  city_name ,
  city_real ,
  cityjob ,
  customer_no ,
  customer_name ,
  sales_id,
    sales_name,
    sales_work_no ,
    sales_supervisor_id,--销售主管
  sales_supervisor_name,
  sales_supervisor_work_no,
  sales_manager_id,-- 销售经理
  sales_manager_name, 
  sales_manager_work_no,
  city_manager_id,
  city_manager_name, -- 城市经理
  city_manager_work_no,
  province_manager_id,
  province_manager_name,-- 省区经理
  province_manager_work_no,
  first_category,
  first_category_code,
  second_category,
  second_category_code,
  third_category,
  third_category_code,
  promotion_code ,
  goods_code ,
  goods_name ,
  bar_code ,
  brand ,
  brand_name ,
  bd_id ,
  bd_name ,
  category_code ,
  category_name ,
  firm_code ,
  firm_name ,
  dept_id ,
  dept_name ,
  category_large_code ,
  category_large_name ,
  category_middle_code ,
  category_middle_name ,
  category_small_code ,
  category_small_name ,
  price_zone ,
  price_zone_name ,
  standard ,
  unit ,
  sales_qty ,
  sales_value ,
  sales_cost ,
  profit ,
  promotion_deduction ,
  excluding_tax_sales ,
  excluding_tax_cost ,
  excluding_tax_profit ,
  excluding_tax_deduction ,
  tax_value ,
  sales_tax_rate ,
  cost_tax_rate ,
  vendor_code ,
  vendor_name ,
  run_type ,
  storage_location ,
  bill_type ,
  return_flag ,
  customer_group ,
  origin_shop_id ,
  sdt ,
  sales_type
from
  b2b_tmp.sale_goods_m_2

;