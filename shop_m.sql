 CREATE TABLE `csx_dw.shop_m`(
  `shop_id` string COMMENT '门店编码(业务主键)', 
  `shop_name` string COMMENT '门店名称', 
  `sales_dist` string COMMENT '销售地区', 
  `sales_dist_name` string COMMENT '销售地区名称', 
  `company_code` string COMMENT '公司代码', 
  `company_name` string COMMENT '公司描述', 
  `shop_form` string COMMENT '门店业态', 
  `shop_form_desc` string COMMENT '门店业态描述', 
  `shop_status` string COMMENT '门店状态', 
  `shop_status_desc` string COMMENT '门店状态描述', 
  `open_date` string COMMENT '开店日期', 
  `region_id` string COMMENT '门店区域', 
  `region_name` string COMMENT '门店区域描述', 
  `province_code` string COMMENT '省份', 
  `province_name` string COMMENT '省份描述', 
  `city_code` string COMMENT '地级市', 
  `city_name` string COMMENT '地级市描述', 
  `town_code` string COMMENT '县级市', 
  `town_name` string COMMENT '县级市描述', 
  `old_shop_id` string COMMENT '旧门店编码',
  `purchase_clust` string COMMENT '进价店群',
  `purchase_clust_name` string COMMENT '进价店群描述',
  `shop_duration` string COMMENT '店龄', 
  `shop_duration_desc` string COMMENT '店龄描述', 
  `shop_type` string COMMENT '门店类型', 
  `shop_type_desc` string COMMENT '门店类型描述',
  `purchase_org` string COMMENT '采购组织', 
  `purchase_org_name` string COMMENT '采购组织描述', 
  `city_dist_id` string COMMENT '城市区域', 
  `city_dist_name` string COMMENT '城市区域名称',  
  `shop_belong` string COMMENT '门店归属', 
  `shop_belong_desc` string COMMENT '门店归属描述', 
  `addr_name` string COMMENT '门店地址', 
  `zone_id` string COMMENT '战区编码', 
  `zone_name` string COMMENT '战区名称', 
  `sales_belong_flag` string COMMENT '销售归属标识, 1_云超,2_云创会员店,3_云创超级物种,
    4_企业购,5_彩食鲜,6_云创到家,7_上蔬托管联华,8_云超MINI'
  `shop_run_type` string COMMENT '门店经营类型,1:商超, 2:实体DC, 3:虚拟DC',
  `shop_run_type_name` string COMMENT '门店类型名称',
  `financial_body` int COMMENT '1:彩食鲜 2:其它'
)partitioned by 
(
  sdt string COMMENT '日期分区'
);

set mapreduce.job.queuename=caishixian;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

drop table  b2b_tmp.temp_shops_current;
create temporary table b2b_tmp.temp_shops_current
as
select 
  shop_id,
  shop_name,
  coalesce(sales_dist_new, sales_dist) as sales_dist,
  coalesce(sales_dist_new_name, sales_dist) as sales_dist_name,
  comp_code as company_code,
  comp_name as company_name,
  shop_form,
  shop_form_desc,
  coalesce(shop_sts_new, shop_sts) as shop_status,
  shop_sts_desc as shop_status_desc,
  coalesce(open_date_new, open_date) as open_date,
  coalesce(region_id_new, region_id) as region_id,
  coalesce(region_name_new, region_name) as region_name,
  prov_code as province_code,
  prov_name as province_name,
  city_code,
  city_name,
  town_code,
  town_name,
  old_shop_id,
  pur_clust as purchase_clust,
  pur_clust_name as purchase_clust_name,
  shop_duration,
  shop_duration_desc,
  shop_type,
  shop_type_desc,
  pur_org as purchase_org,
  pur_org_name as purchase_org_name,
  city_dist_id,
  city_dist_name,
  shop_belong,
  shop_belong_desc,
  addr_name,
  zone_id,
  zone_name
from dim.dim_shop 
where edate='9999-12-31' and ((shop_name is not null and shop_id like '9%') or 
  (shop_id like 'W%' and (sales_dist_new in ('310000', '200000') or shop_name like 'YHO2O%' 
  	or sales_dist_new_name like '%彩食鲜%')));

insert into b2b_tmp.temp_shops_current
select 
  a.*,
  b.zone_id,
  b.zone_name
from 
(
  select 
    shop_id,
    shop_name,
    coalesce(sales_dist_new, sales_dist) as sales_dist,
    coalesce(sales_dist_new_name, sales_dist) as sales_dist_name,
    comp_code as company_code,
    comp_name as company_name,
    shop_form,
    shop_form_desc,
    coalesce(shop_sts_new, shop_sts) as shop_status,
    shop_sts_desc as shop_status_desc,
    coalesce(open_date_new, open_date) as open_date,
    coalesce(region_id_new, region_id) as region_id,
    coalesce(region_name_new, region_name) as region_name,
    prov_code as province_code,
    prov_name as province_name,
    city_code,
    city_name,
    town_code,
    town_name,
    old_shop_id,
    pur_clust as purchase_clust,
    pur_clust_name as purchase_clust_name,
    shop_duration,
    shop_duration_desc,
    shop_type,
    shop_type_desc,
    pur_org as purchase_org,
    pur_org_name as purchase_org_name,
    city_dist_id,
    city_dist_name,
    shop_belong,
    shop_belong_desc,
    addr_name
  from dim.dim_shop
  where edate='9999-12-31' and shop_id like 'W%'
    and sales_dist_new not in ('310000','200000','810000') and shop_name not like 'YHO2O%' 
    and sales_dist_new_name not like '%彩食鲜%'
)a
left join 
(
  select 
    distinct city_code,
    zone_id,
    zone_name 
  from b2b_tmp.temp_shops_current
  where zone_id is not null and zone_id<>''
)b
on coalesce(a.city_code,a.province_code)=b.city_code;


-- 刷选企业购
drop table b2b_tmp.dim_shops_current;
create temporary table b2b_tmp.dim_shops_current
as
select 
  a.*, 
  '4_企业购'as sales_belong_flag 
from b2b_tmp.temp_shops_current a 
where sales_dist_name like '%彩食鲜%' and 
  ((shop_belong_desc not like '%彩食鲜%' and shop_id like 'W%') or shop_id like '99%');

-- 筛选彩食鲜工厂
insert into table b2b_tmp.dim_shops_current 
select 
  a.*
from 
(
  select 
    a.*, 
    '5_彩食鲜'as sales_belong_flag 
  from b2b_tmp.temp_shops_current a  
  where sales_dist_name like '%彩食鲜%' and shop_belong_desc like '%彩食鲜%' and shop_id like 'W%'
)a left outer join b2b_tmp.dim_shops_current b 
on a.shop_id = b.shop_id 
where b.shop_id is null;

-- 筛选云创会员店
insert into table b2b_tmp.dim_shops_current 
select 
  a.*
from 
(
  select 
    a.*, 
    '2_云创会员店'as sales_belong_flag 
  from b2b_tmp.temp_shops_current a 
  where substr(shop_id,1,2) in ('9D') or 
   ((shop_name like 'YHO2O%' or shop_name like '%云创%') and shop_id like 'W%')  
)a left outer join b2b_tmp.dim_shops_current b 
on a.shop_id = b.shop_id 
where b.shop_id is null;

-- 刷选云创超级物种
insert into table b2b_tmp.dim_shops_current 
select 
  a.*
from 
(
  select 
    a.*, 
    '3_云创超级物种'as sales_belong_flag 
  from b2b_tmp.temp_shops_current a 
  where substr(shop_id,1,2) in ('9I','9K') or 
    (purchase_clust_name like '%超%物%' and shop_name not like 'YHO2O%')
)a left outer join b2b_tmp.dim_shops_current b 
on a.shop_id = b.shop_id 
where b.shop_id is null;

-- 筛选云创到家
insert into table b2b_tmp.dim_shops_current 
select 
  a.*
from 
(
  select 
    a.*,
    '6_云创到家'as sales_belong_flag 
  from b2b_tmp.temp_shops_current a 
  where substr(shop_id,1,2) in ('9L','9M')
    and shop_id not in ('9M20','9M21','9M22')
)a left outer join b2b_tmp.dim_shops_current b 
on a.shop_id = b.shop_id 
where b.shop_id is null;

-- 筛选上蔬托管联华
insert into table b2b_tmp.dim_shops_current 
select 
  a.*
from 
(
  select 
    a.*,
    '7_上蔬托管联华'as sales_belong_flag 
  from b2b_tmp.temp_shops_current a 
  where substr(shop_id,1,2) in ('9B','9F','9G')
)a left outer join b2b_tmp.dim_shops_current b 
on a.shop_id = b.shop_id 
where b.shop_id is null;

-- 筛选云创MINI
insert into table b2b_tmp.dim_shops_current 
select 
  a.*
from 
(
  select 
    a.*,
    '8_云超MINI' as sales_belong_flag 
  from b2b_tmp.temp_shops_current a 
  where shop_belong = '29' or purchase_clust_name like '%BRAVOMINI%'
)a left outer join b2b_tmp.dim_shops_current b 
on a.shop_id = b.shop_id 
where b.shop_id is null;

-- 筛选云超
insert into b2b_tmp.dim_shops_current
select 
  a.*, 
  '1_云超' as sales_belong_flag 
from 
(
	select * 
	from b2b_tmp.temp_shops_current
	where zone_id is not null and zone_id <> ''
)a 
left outer join b2b_tmp.dim_shops_current b on a.shop_id=b.shop_id 
where b.shop_id is null;

insert overwrite table csx_dw.shop_m partition(sdt) 
select 
  a.*,
  if(shop_id like 'W%' or shop_id like '99%', 2, 1) as shop_run_type,
  if(shop_id like 'W%' or shop_id like '99%', '实体DC', '商超') as shop_run_type_name,
  if(company_code in ('2115','2116','2126','2207','2210','2211',
  	  '2216','2304','2408','2814','3505','3506','3750','3751','8030'), 1, 2) as financial_body,
  regexp_replace(date_sub(current_date, 1), '-', '') as sdt
from b2b_tmp.dim_shops_current a;

insert overwrite table csx_dw.shop_m partition(sdt) 
select 
  a.*,
  if(shop_id like 'W%' or shop_id like '99%', 2, 1) as shop_run_type,
  if(shop_id like 'W%' or shop_id like '99%', '实体DC', '商超') as shop_run_type_name,
  if(company_code in ('2115','2116','2126','2207','2210','2211',
      '2216','2304','2408','2814','3505','3506','3750','3751','8030'), 1, 2) as financial_body,
  'current' as sdt
from b2b_tmp.dim_shops_current a;