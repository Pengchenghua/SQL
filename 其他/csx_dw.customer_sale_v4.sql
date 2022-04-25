set mapreduce.job.queuename=caishixian;

-- 从DC W0B6 出去得算BBC，从W0H4出去且客户为门店得算供应链（S端）
--根据战报表逻辑生成近两个月的销售表，添加渠道，省区字段
drop table b2b_tmp.tmp_customer_sale_1;
create temporary table b2b_tmp.tmp_customer_sale_1
as
select
  shop_id,
  shop_name,
  customer_no,
  customer_name,
  sales_date,
  sales_channel,
  sales_id,
  promotion_code,
  goods_code,
  bar_code,
  unit,
  category_code,
  bd_name,
  category_large_code,
  category_middle_code,
  category_small_code,
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
  sales_type,
  channel,
  case when channel='1' then '大客户'
    when channel='2' then '商超（对内）'
    when channel='3' then '商超（对外）'
    when channel='4' then '大宗'
    when channel='5' then '供应链（食百）'
    when channel='6' then '供应链（生鲜）'
    when channel='7' then '企业购 '
    when channel='8' then '其他'
    when channel='-1' then '未定义'
    end as channel_name,
  case when channel ='5'  then '平台-食百采购' 
    when channel='6' then '平台-生鲜采购'
    when channel='4' then '平台-大宗'
    else aa.province_name end as province_name,
  city_name,
  region_city
from 
(
 select 
   a.shop_id,
   shop_name,
   a.customer_no,
   b.customer_name,
   a.sales_date,
   a.sales_channel,
   b.sales_id,
   a.promotion_code,
   a.goods_code,
   a.bar_code,
   a.unit,
   a.category_code,
   a.bd_name,
   a.category_large_code,
   a.category_middle_code,
   a.category_small_code,
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
   d.city_name,
   b.sales_city region_city,
   case when a.origin_shop_id = 'W0B6'  then '7' 
     when (a.origin_shop_id = 'W0H4' and a.customer_no like 'S%' and bd_name='食百') or (b.channel = '供应链（食百）' and bd_name='食百')then '5'
     when (a.origin_shop_id = 'W0H4' and a.customer_no like 'S%' and bd_name='生鲜') or (b.channel = '供应链（生鲜）' and bd_name='生鲜')then '6'
     when b.channel in ('商超（对内）', 'M端') then '2'
     when b.channel = '大客户' or b.channel='B端' or a.customer_no in ('S9962','S9951','S9952','S9955','S9958','S9961','S9985') then '1'
     when b.channel = '大宗' then '4'
     when b.channel='商超（对外）' then '3'
     when b.channel='其他' then '8'
     else '-1' end as channel,
   case when b.customer_no is not null and b.sales_province='BBC' then '福建省'
     when b.customer_no is not null and b.channel <> 'M端' then b.sales_province
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
     unit,
     category_code,
     case when category_code in ('10','11') then '生鲜' when category_code in ('12','13') then '食百' else '其他' end bd_name,
     category_large_code,
     category_middle_code,
     category_small_code,
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
   where sales_type in ('qyg', 'gc','anhui') 
 )a left outer join 
 (
   select 
     *
   from csx_dw.customer_m 
   where sdt = regexp_replace(date_sub(current_date,1),'-','')
 )b on a.customer_no = b.customer_no 
 left outer join 
 (
   select distinct
     shop_id,
     province_name
   from csx_dw.shop_m 
   where sdt = 'current'
 )c on a.customer_no = concat('S', c.shop_id)
 left outer join 
 (
   select distinct
     shop_id,
	 shop_name,
     province_name,
     case when province_name like '%市' then province_name else city_name end city_name 
   from csx_dw.shop_m 
   where sdt = 'current'
 )d on a.shop_id = d.shop_id
)aa
;


--福建省的要划分到市，其余省份无该需求
drop table b2b_tmp.tmp_customer_sale_2;
create temporary table b2b_tmp.tmp_customer_sale_2
as
select
  channel,
  shop_id,
  shop_name,
  customer_no,
  customer_name,
  sales_date,
  sales_channel,
  g.province_code,
  province_name,
  '' sales_city_code,
	case when channel_name='商超（对内）' then city_name 
    when channel_name='企业购' then '福州' 
    when (channel_name<>'商超（对内）')  then region_city else '-' end sales_city,
  case when a.province_name='福建省' then coalesce(b.city_real,'福州、宁德、三明')else '-' end city_real,
  case when a.province_name='福建省' then coalesce(b.cityjob,'沈锋')else '-' end cityjob,
  sales_id,
  i.sales_name,
  i.work_no,
  i.sales_supervisor_id,
  i.sales_supervisor_name,
  i.sales_supervisor_work_no
  h.province_manager_id,
  h.province_manager_name,
  h.province_manager_work_no,
  promotion_code,
  j.goods_name,
  a.goods_code,
  goodsname,
  bar_code,
	, brand
	, brand_name
	, standard
	, unit
	, firm_code
	, firm_name
	, category_code
	, div_name as category_name
	, category_large_code
	, catg_l_name as catg_large_name
	, category_middle_code
	, catg_m_name as  catg_middle_name
	, category_small_code
	, catg_s_name  as category_small_name
	, bd_id,bd_name,dept_id,dept_name,price_zone,price_zone_name,
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
    promotion_code,
    a.goods_code,
    bar_code,
	unit,
	category_code,
	category_large_code,
	category_middle_code,
	category_small_code,
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
    sales_type,
   case when channel is null or channel='' then '1' when province_name='平台-B' and channel='1' then '平台' else channel end channel,
   case when province_name ='成都' then '四川省' when channel='7' then '福建省'else province_name end province_name,
   province_name,
   city_name,
   case when channel='2' and province_name ='福建省' then city_name 
      when channel='7' then '福州' 
      when (channel<>'2' and province_name ='福建省')  then region_city else '-' 
      end region_city
  from b2b_tmp.tmp_customer_sale_1
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
)b on substr(a.region_city,1,2)=b.city
left outer join
(select goodsid,
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
    province_code,
    province
  from csx_ods.sys_province_ods
)g on a.province_name=g.province
left outer join 
(
  select 
    sales_id as province_manager_id,
    sales_name as province_manager_name,
    work_no as province_manager_work_no,
    province_name
  from csx_dw.region_data_permission 
  where region_permission = 3
)h on a.province_name = h.province_name
left outer join
(
  select
    sales_id,
    sales_name,
    work_no,
    sales_supervisor_id,
    sales_supervisor_name,
    sales_supervisor_work_no
  from csx_dw.sale_org_m
  where sdt='current'
)i on aa.sales_id=i.sales_id

;







insert overwrite table csx_dw.customer_sale_v4
select * from b2b_tmp.tmp_customer_sale_2;