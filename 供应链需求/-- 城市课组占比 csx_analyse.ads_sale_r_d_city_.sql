-- 城市课组占比 csx_analyse.ads_sale_r_d_city_dept_fr
set hive.exec.parallel=true; 
set hive.exec.parallel.thread.number=100;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set hive.exec.dynamic.partition.mode=nonstrict;


-- 明细
drop table if exists csx_analyse.csx_analyse_tmp_attribute_sale_01;
create temporary table csx_analyse.csx_analyse_tmp_attribute_sale_01
as 
select  province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    channel,
    channel_name,
    business_type_name,
    business_type_code,
    division_code ,
    division_name,
    purchase_group_code ,
    purchase_group_name,
    sum(coalesce(daily_sale_value,0))as daily_sale_value,
    sum(coalesce(daily_profit,0)) as daily_profit,
    sum(coalesce(month_sale,0)) month_sale,
    sum(coalesce(month_profit,0)) month_profit,
    sum(coalesce(month_sale_cust_num,0))as month_sale_cust_num,
    sum(coalesce(month_sales_sku,0))as month_sales_sku,
    sum(coalesce(last_month_sale,0)) as last_month_sale
from (
select
    performance_province_code as  province_code ,
    performance_province_name as province_name ,
    a.performance_city_code as city_group_code,
    a.performance_city_name as city_group_name,
    case when a.channel_code not in ('2','4','5','6') then '1'
        else a.channel_code
        end channel,
    case when a.channel_code not in ('2','4','5','6') then '大'
        else a.channel_name
        end channel_name,
        a.business_type_name as    business_type_name,
        a.business_type_code as business_type_code,
    division_code ,
    division_name,
    case when purchase_group_code like 'U%' then 'U01' else purchase_group_code end       purchase_group_code ,
    case when purchase_group_code like 'U%' then '加工课' else purchase_group_name end   purchase_group_name,
    sum(case when sdt = regexp_replace('${edate}','-','') then sale_amt end )as daily_sale_value,
    sum(case when sdt =  regexp_replace('${edate}','-','') then profit end) as daily_profit,
    sum(sale_amt) month_sale,
    sum(profit) month_profit,
    count(distinct a.customer_code )as month_sale_cust_num,
    count(distinct goods_code )as month_sales_sku,
    0 as last_month_sale
from
    csx_dws.dws_sale_r_d_detail a
where
    sdt >=   regexp_replace(trunc('${edate}','MM'),'-','')
    and sdt <=   regexp_replace('${edate}','-','')
  --  and  channel ='1'
  --  and  a.business_type_code in ('1','2') and a.order_kind!='WELFARE'
group by 
    performance_province_code ,
    performance_province_name ,
    a.performance_city_code,
    a.performance_city_name,
    division_code ,
    division_name,
    case when purchase_group_code like 'U%' then 'U01' else purchase_group_code end   , 
    case when purchase_group_code like 'U%' then '加工课' else purchase_group_name end ,
    case when a.channel_code not in ('2','4','5','6') then '1'
        else a.channel_code
        end ,
    case when a.channel_code not in ('2','4','5','6') then '大'
        else a.channel_name
        end ,
     a.business_type_code,
      a.business_type_name
union all 
select
    performance_province_code  province_code ,
    performance_province_name province_name ,
    a.performance_city_code   city_group_code,
    a.performance_city_name   city_group_name,
   case when a.channel_code not in ('2','4','5','6') then '1'
        else a.channel_code
        end channel,
    case when a.channel_code not in ('2','4','5','6') then '大'
        else a.channel_name
        end channel_name,
      a.business_type_name as business_type_name,
      a.business_type_code as  business_type_code,
    division_code ,
    division_name,
    case when purchase_group_code like 'U%' then 'U01' else purchase_group_code end  purchase_group_code  , 
    case when purchase_group_code like 'U%' then '加工课' else purchase_group_name end  purchase_group_name,
    0 as daily_sale_value,
    0 as daily_profit,
    0 month_sale,
    0 month_profit,
    0 month_sale_cust_num,
    0 month_sales_sku,
    sum(sale_amt)as last_month_sale
from
    csx_dws.dws_sale_r_d_detail a
where
    sdt >=  regexp_replace(trunc(add_months('${edate}',-1),'MM'),'-','')
    and sdt <=  regexp_replace(if('${edate}'=last_day('${edate}'),last_day(add_months('${edate}',-1)),add_months('${edate}',-1)),'-','')
   -- and  channel ='1'
   -- and  a.business_type_code in ('1','2') and a.order_kind!='WELFARE'
group by 
    performance_province_code,
    performance_province_name,
    a.performance_city_code  ,
    a.performance_city_name  ,
    division_code ,
    division_name,
    case when purchase_group_code like 'U%' then 'U01' else purchase_group_code end   , 
    case when purchase_group_code like 'U%' then '加工课' else purchase_group_name end ,
    case when a.channel_code not in ('2','4','5','6') then '1'
        else a.channel_code
        end ,
    case when a.channel_code not in ('2','4','5','6') then '大'
        else a.channel_name
        end ,
     a.business_type_code ,
     a.business_type_name
) a 
group by 
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    channel,
    channel_name,
    business_type_name,
    business_type_code,
    division_code ,
    division_name,
    purchase_group_code ,
    purchase_group_name    ;
 
-- select sum(month_sale) from  csx_analyse.csx_analyse_tmp_attribute_sale_02 where province_code='32' and business_type_code='1' and channel='1' and purchase_group_code='104' ;
 
-- 计算课组层级
drop table  if exists csx_analyse.csx_analyse_tmp_attribute_sale_02;
create temporary table csx_analyse.csx_analyse_tmp_attribute_sale_02 as
select
   '1' as level_id,
    region_code,
    region_name,
    a.province_code ,
    a.province_name ,
    a.city_group_code,
    a.city_group_name,
    a.channel,
    a.channel_name,
    a.business_type_code,
    business_type_name,
    case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end business_division_code,
    case when division_code in ('10','11') then '生鲜' when division_code in ('12','13','14') then '食百' else division_name end business_division_name,
    division_code ,
    division_name,
    purchase_group_code ,
    purchase_group_name,
    0 daily_plan_sale,
    daily_sale_value,
    0 daily_sale_fill_rate,
    daily_profit,
    coalesce(daily_profit/daily_sale_value,0) daily_profit_rate,
    0 as month_plan_sale,
    month_sale,
    0 as month_sale_fill_rate,
    last_month_sale,
   coalesce((month_sale-last_month_sale)/abs(last_month_sale),0) as mom_sale_growth_rate,
   coalesce(month_sale/sum(month_sale)over(partition by a.city_group_code,a.business_type_code,a.channel),0) month_sale_ratio,
   coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
   0 month_plan_profit,
    month_profit,
    0 month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust) as penetration_rate,  -- 渗透率
    (all_sale_cust) as all_sale_cust_num,
    row_number()over(partition by a.province_code ,a.business_type_code,a.channel order by month_sale desc) as row_num
from csx_analyse.csx_analyse_tmp_attribute_sale_01    a 
left join 
(
select
    performance_province_code province_code ,
    a.performance_city_code city_group_code,
    case when a.channel_code not in ('2','4','5','6') then '1'
        else a.channel_code
        end channel,
   a.business_type_code as  business_type_code,
    count(distinct a.customer_code )as all_sale_cust
from
    csx_dws.csx_dws_sale_detail_di a
where
    sdt >=   regexp_replace(trunc('${edate}','MM'),'-','')
    and sdt <=  regexp_replace('${edate}','-','')
    
group by 
    performance_province_code ,
    a.performance_city_code,
   case when a.channel_code not in ('2','4','5','6') then '1'
        else a.channel_code
        end ,
    a.business_type_code
   ) b on a.province_code=b.province_code and a.business_type_code=b.business_type_code and a.channel=b.channel and a.city_group_code=b.city_group_code
   left join 
   (select distinct performance_province_code as province_code,
        performance_province_name as  province_name,
        performance_region_code as region_code,
        performance_region_name as region_name
    from csx_dim.csx_dim_sales_area_belong_mapping 
    where sdt='current' ) c on a.province_code=c.province_code 
;

-- 插入数据表
insert overwrite table csx_analyse.ads_sale_r_d_city_dept_fr partition(months)
select 
    level_id,
    substr(regexp_replace('${edate}','-',''),1,6) as sales_month,
    region_code,
    region_name,
    a.province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    a.channel,
    a.channel_name,
    a.business_type_code,
    business_type_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    purchase_group_code ,
    purchase_group_name,
    daily_plan_sale,
    daily_sale_value,
    daily_sale_fill_rate,
    daily_profit,
    daily_profit_rate,
    month_plan_sale,
    month_sale,
    month_sale_fill_rate,
    last_month_sale,
    mom_sale_growth_rate,
    month_sale_ratio,
    month_avg_cust_sale,
    month_plan_profit,
    month_profit,
    month_profit_fill_rate,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    penetration_rate cust_penetration_rate,  -- 渗透率
    all_sale_cust_num,
    row_num,
    current_timestamp(),
    substr(regexp_replace('${edate}','-',''),1,6)
from  csx_analyse.csx_analyse_tmp_attribute_sale_02 a;

-- describe csx_analyse.ads_sale_r_d_zone_province_dept_fr ;
-- 插入汇总数据
insert into table csx_analyse.ads_sale_r_d_city_dept_fr partition(months)
select
    level_id,
    substr(regexp_replace('${edate}','-',''),1,6) as sales_month,
    region_code,
    region_name,
    province_code ,
    province_name ,
    city_group_code,
    city_group_name,
    a.channel,
    a.channel_name,
    a.business_type_code,
    business_type_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    purchase_group_code ,
    purchase_group_name,
    daily_plan_sale,
    daily_sale_value,
    coalesce(daily_sale_value/daily_plan_sale,0) daily_sale_fill_rate,
    daily_profit,
    coalesce(daily_profit/daily_sale_value,0) daily_profit_rate,
    month_plan_sale,
    month_sale,
    month_sale_fill_rate,
    last_month_sale,
   coalesce((month_sale-last_month_sale)/abs(last_month_sale),0) as mom_sale_growth_rate,
   coalesce(month_sale/sum(month_sale)over(partition by province_code,a.business_type_code,channel),0) month_sale_ratio,
   coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
    month_plan_profit,
    month_profit,
    coalesce(month_profit / month_plan_profit,0) month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust_num) as penetration_rate,  -- 渗透率
    all_sale_cust_num,
    row_number()over(partition by a.province_code ,a.business_type_code,a.channel order by month_sale desc) as row_num,
    current_timestamp(),
    substr(regexp_replace('${edate}','-',''),1,6)
from(
select
    '2' as level_id,
    region_code,
    region_name,
    province_code ,
    province_name ,
    '00' as city_group_code,
    '-' as city_group_name,
    channel,
    a.channel_name,
    a.business_type_code,
    business_type_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    purchase_group_code ,
    purchase_group_name,
    sum(daily_plan_sale)daily_plan_sale,
    sum(daily_sale_value)daily_sale_value,
    sum(daily_profit)daily_profit,
   --coalesce(sum(daily_profit)/sum(daily_sale_value),0) daily_profit_rate,
    sum(month_plan_sale) as month_plan_sale,
    sum(month_sale) month_sale,
    sum(month_sale_fill_rate) as month_sale_fill_rate,
    sum(last_month_sale)last_month_sale,
   --coalesce((sum(month_sale)-sum(last_month_sale))/abs(sum(last_month_sale)),0) as mom_sale_growth_rate,
   --coalesce(sum(month_sale)/sum(month_sale)over(partition by region_code,a.business_type_code),0) month_sale_ratio,
   --coalesce(sum(month_sale)/sum(month_sale_cust_num),0) as month_avg_cust_sale,
    sum(month_plan_profit)month_plan_profit,
    sum(month_profit) month_profit,
    -- coalesce(sum(month_profit) /sum( month_plan_profit),0) month_profit_fill_rate,
   -- sum(month_profit)/sum(month_sale) as month_profit_rate,
    sum(month_sales_sku)month_sales_sku,
    sum(month_sale_cust_num)month_sale_cust_num,
    -- sum(month_sale_cust_num)/sum(all_sale_cust_num) as penetration_rate,  -- 渗透率
    sum(all_sale_cust_num) as all_sale_cust_num
from  csx_analyse.csx_analyse_tmp_attribute_sale_02 a
group by 
    region_code,
    region_name,
    province_code ,
    province_name ,
    a.channel,
    a.channel_name,
    a.business_type_code,
    business_type_name,
    division_code ,
    division_name,
    purchase_group_code ,
    purchase_group_name,
    business_division_code,
    business_division_name
) a ;




CREATE TABLE `csx_analyse.csx_analyse_fr_sale_kanban_city_catg_di`(
	  `level_id` string COMMENT '汇总层级:1 城市课组汇总、2 城市部类汇总、3 城市采购部汇总 4 城市汇总、5、省区课组、部类、采购部汇总、6、战区课组、部类、采购部、汇总，00 代码表示汇总 ', 
	  `sales_month` string COMMENT '销售月份', 
	  `region_code` string COMMENT '战区编码', 
	  `region_name` string COMMENT '战区名称', 
	  `province_code` string COMMENT '省区编码 ', 
	  `province_name` string COMMENT '省区名称', 
	  `city_group_code` string COMMENT '城市组编码', 
	  `city_group_name` string COMMENT '城市组名称', 
	  `channel_code` string COMMENT '渠道编码', 
	  `channel` string COMMENT '渠道名称', 
	  `bd_id` string COMMENT '采购部编码', 
	  `bd_name` string COMMENT '采购部名称', 
	  `division_code` string COMMENT '部类编码', 
	  `division_name` string COMMENT '部类名称', 
	  `purchase_group_code` string COMMENT '课组编码', 
	  `purchase_group_name` string COMMENT '课组名称', 
	  `daily_plan_sale` decimal(26,6) COMMENT '昨日计划销售额', 
	  `daily_sale_value` decimal(26,6) COMMENT '昨日销售额', 
	  `daily_sale_fill_rate` decimal(26,6) COMMENT '昨日销售达成率', 
	  `daily_profit` decimal(26,6) COMMENT '昨日毛利额', 
	  `daily_profit_rate` decimal(26,6) COMMENT '昨日毛利率', 
	  `month_plan_sale` decimal(26,6) COMMENT '月至今销售预算', 
	  `month_sale` decimal(26,6) COMMENT '月至今销售额', 
	  `month_sale_fill_rate` decimal(26,6) COMMENT '月至今销售达成率', 
	  `last_month_sale` decimal(26,6) COMMENT '月环比销售额', 
	  `mom_sale_growth_rate` decimal(26,6) COMMENT '月环比增长率', 
	  `month_plan_profit` decimal(26,6) COMMENT '月度毛利计划', 
	  `month_profit` decimal(26,6) COMMENT '月至今毛利额', 
	  `month_profit_fill_rate` decimal(26,6) COMMENT '月度毛利完成率', 
	  `month_profit_rate` decimal(26,6) COMMENT '月至今毛利率', 
	  `month_sales_sku` bigint COMMENT '销售SKU数', 
	  `month_sale_cust_num` bigint COMMENT '课组成交数', 
	  `cust_penetration_rate` decimal(26,6) COMMENT '渗透率', 
	  `all_sale_cust_num` bigint COMMENT '合计数', 
	  `row_num` bigint COMMENT '行数', 
	  `update_time` timestamp COMMENT '更新时间')
	COMMENT '大课组销售与渗透率'
	PARTITIONED BY ( 
	  `months` string COMMENT '按月分区')
	STORED AS parquet 
