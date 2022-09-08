-- ******************************************************************** 
-- @功能描述：大区经营看板客户属性销售分析
-- @创建者： 彭承华 
-- @创建者日期：2022-08-24 16:13:07 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


-- set tez.queue.name=caishixian;
set hive.exec.parallel=true; 
set hive.exec.parallel.thread.number=100;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set hive.exec.dynamic.partition.mode=nonstrict;


-- 实际完成

drop table csx_analyse_tmp.csx_analyse_tmpcity_attribute_00;
create  table csx_analyse_tmp.csx_analyse_tmpcity_attribute_00 as 
select 
       performance_region_code,
       performance_region_name ,
       a.performance_province_code ,
       a.performance_province_name ,
       a.performance_city_code,
       a.performance_city_name,
       business_type_code,
       business_type_name,
       customer_code,     
       sum(yesterday_sales_value/10000 )as yesterday_sales_value,
       sum(yesterday_profit/10000) as yesterday_profit,
      -- sum(daily_profit)/sum(daily_sales_value) as days_profit_rate,
       sum(month_sale/10000 )month_sale,
       sum(last_month_sale/10000 ) as last_month_sale,
      -- (sum(month_sale)- coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as mom_sale_rate,
       sum(month_profit/10000)month_profit,
       sum(month_profit)/sum(month_sale) as profit_rate,
       count(distinct case when month_sale>0 then customer_code end )as month_sale_cust_num,
       -- sum(month_sale_cust_num-last_month_sale_cust_num) as diff_sale_cust,
       sum(last_month_profit/10000) as last_month_profit,
       count(distinct case when last_month_sale>0 then customer_code end ) as last_month_sale_cust_num
from (
   SELECT 
       performance_region_code,
       performance_region_name ,
       a.performance_province_code ,
       a.performance_province_name ,
       a.performance_city_code,
       a.performance_city_name,
       case when  c.shop_low_profit_flag=1 then '99' else business_type_code end as    business_type_code,
       case when  c.shop_low_profit_flag=1 then '联营仓' else business_type_name end as business_type_name,
       customer_code,
       sum(case when sdt= regexp_replace('${edate}','-','') then sale_amt end )as yesterday_sales_value,
       sum(case when sdt= regexp_replace('${edate}','-','') then profit end ) as  yesterday_profit,
       sum(sale_amt )month_sale,
       sum(profit )month_profit,
       0 as last_month_sale,
       0 as last_month_profit
   FROM csx_dws.csx_dws_sale_detail_di a 
	left join 
	(select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code
    where sdt>= regexp_replace(trunc('${edate}','MM'),'-','') 
    and sdt<= regexp_replace('${edate}','-','') 
    and a.channel_code in('1','7','9')
   group by 
       performance_region_code,
       performance_region_name ,
       a.performance_province_code ,
       a.performance_province_name ,
       a.performance_city_code,
       a.performance_city_name,
       case when  c.shop_low_profit_flag=1 then '99' else business_type_code end    ,
       case when  c.shop_low_profit_flag=1 then '联营仓' else business_type_name end,
       customer_code
 union all 
   SELECT 
       performance_region_code,
       performance_region_name ,
       a.performance_province_code ,
       a.performance_province_name ,
       a.performance_city_code,
       a.performance_city_name,
       case when  c.shop_low_profit_flag=1 then '99' else business_type_code end as    business_type_code,
       case when  c.shop_low_profit_flag=1 then '联营仓' else business_type_name end as business_type_name,
       customer_code,
       0 as yesterday_sales_value,
       0 as yesterday_profit,
       0 as month_sale,
       0 as month_profit,
       sum(sale_amt)as last_month_sale,
       sum(profit)as last_month_profit
   FROM csx_dws.csx_dws_sale_detail_di a    
	left join 
	(select shop_low_profit_flag,shop_code from csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code 
    where sdt>= regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','') 
    and  sdt<=regexp_replace(if('${edate}'=last_day('${edate}'),last_day(add_months('${edate}',-1)),add_months('${edate}',-1)),'-','')  
    and a.channel_code in('1','7','9')
   group by
       performance_region_code,
       performance_region_name ,
       a.performance_province_code ,
       a.performance_province_name ,
       a.performance_city_code,
       a.performance_city_name,
       case when  c.shop_low_profit_flag=1 then '99' else business_type_code end       ,
       case when  c.shop_low_profit_flag=1 then '联营仓' else business_type_name end    ,
       customer_code
 ) a 
 group by  
       performance_region_code,
       performance_region_name ,
       a.performance_province_code ,
       a.performance_province_name ,
       a.performance_city_code,
       a.performance_city_name,
       business_type_name,
       business_type_code,
       customer_code
  ;
  
  
-- 关联计划表
drop table if exists csx_analyse_tmp.csx_analyse_tmpcity_attribute_03;
create  table csx_analyse_tmp.csx_analyse_tmpcity_attribute_03
as 
select  
       performance_region_code,
       performance_region_name ,
       b.performance_province_code ,
       b.performance_province_name ,
       b.performance_city_code,
       b.performance_city_name,
       business_type_name,
       business_type_code,     
       sum(yesterday_sales_value )as yesterday_sales_value,
       sum(yesterday_profit) as yesterday_profit,
      --sum(daily_profit)/sum(daily_sales_value) as days_profit_rate,
       sum(month_sale )month_sale,
       sum(last_month_sale ) as last_month_sale,
      -- (sum(month_sale)- coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as mom_sale_rate,
       sum(month_profit)month_profit,
       sum(month_profit)/sum(month_sale) as profit_rate,
       sum(month_sale_cust_num )as month_sale_cust_num,
       -- sum(month_sale_cust_num-last_month_sale_cust_num) as diff_sale_cust,
       sum(last_month_profit) as last_month_profit,
       sum(last_month_sale_cust_num) as last_month_sale_cust_num,
       sum(daily_plan_sale) as daily_plan_sale,
       sum(month_plan_sale) as month_plan_sale ,
       sum(month_plan_profit) as month_plan_profit
from (
SELECT 
       a.performance_province_code ,
       performance_city_code,
        business_type_code, 
       business_type_name,
       sum(yesterday_sales_value)as yesterday_sales_value,
       sum(yesterday_profit) as     yesterday_profit,
       sum(month_sale )month_sale,
       sum(month_profit )month_profit,
       count(distinct  case when month_sale>0 then a.customer_code end)as month_sale_cust_num,
       sum(last_month_sale)as last_month_sale,
       sum(last_month_profit)as last_month_profit,
       count(distinct case when last_month_sale>0 then  a.customer_code end)as last_month_sale_cust_num ,
       0 as daily_plan_sale,
       0 as month_plan_sale ,
       0 as month_plan_profit 
   FROM csx_analyse_tmp.csx_analyse_tmpcity_attribute_00 a 
   group by 
       a.performance_province_code ,
       business_type_name,
       business_type_code,
       performance_city_code
  union all 
select province_code performance_province_code,
        city_group_code performance_city_code,
        customer_attribute_code as business_type_code,
        customer_attribute_name as business_type_name,
        0 as yesterday_sales_value,
        0 as yesterday_profit,
        0 as month_sale,
        0 as month_profit,
        0 as month_sale_cust_num,
        0 as last_month_sale,
        0 as last_month_profit,
        0 as last_month_sale_cust_num,
        0 as daily_plan_sale,
        sum(plan_sales_value)month_plan_sale ,
        sum(plan_profit)month_plan_profit 
  from csx_ods.csx_ods_data_analysis_prd_dws_csms_province_month_sale_plan_df
     where month=substr(regexp_replace('${edate}','-',''),1,6) 
		and sdt=substr(regexp_replace('${edate}','-',''),1,6) 
	    and  (weeknum is null or weeknum='')
		and customer_attribute_code !='00'
    -- and channel_name='大客户'
    group by  province_code,
        customer_attribute_code,
        customer_attribute_name,
        city_group_code
)a 
left join 
(select distinct 
    performance_region_code,
    performance_region_name ,
    performance_province_code ,
    performance_province_name ,
    performance_city_code,
    performance_city_name
  from csx_dim.csx_dim_sales_area_belong_mapping ) b on a.performance_province_code=b.performance_province_code  and a.performance_city_code=b.performance_city_code
group by 
       performance_region_code,
       performance_region_name ,
       b.performance_province_code ,
       b.performance_province_name ,
       b.performance_city_code,
       b.performance_city_name,
       business_type_name,
       business_type_code

;
-- select * from csx_analyse_tmp.csx_analyse_tmpcity_attribute_00 where zone_id='3' group by attribute;

-- 销售汇总
drop table if exists csx_analyse_tmp.csx_analyse_tmpcity_attribute_02;
create  table csx_analyse_tmp.csx_analyse_tmpcity_attribute_02 as 
select
       performance_region_code,
       performance_region_name ,
       coalesce(a.performance_province_code,'00') performance_province_code ,
       coalesce(performance_province_name,'-') performance_province_name ,
       coalesce(a.performance_city_code,'00') performance_city_code ,
       coalesce(performance_city_name,'-') performance_city_name ,
       coalesce(business_type_code,'00') business_type_code,
       coalesce(business_type_name,'-') business_type_code , 
       daily_plan_sale,
       yesterday_sales_value,
       coalesce(yesterday_sales_value/daily_plan_sale,0) as yesterday_sale_fill_rate,
       yesterday_profit,
       coalesce(yesterday_profit/yesterday_sales_value,0) as yesterday_profit_rate,
       month_plan_sale,
       month_sale,
       coalesce(month_sale/month_plan_sale,0) as month_sale_fill_rate,
       coalesce(last_month_sale,0)last_month_sale,
       coalesce((month_sale-last_month_sale)/abs(last_month_sale),0)  mom_sale_growth_rate,
       month_plan_profit,
       coalesce(month_profit,0) as month_profit,
       coalesce(month_profit/month_plan_profit,0) month_profit_fill_rate,
       coalesce(month_profit/month_sale,0) as month_profit_rate,
       coalesce(month_sale_cust_num,0) as month_sale_cust_num,
       coalesce(month_sale_cust_num-last_month_sale_cust_num,0)   as mom_diff_sale_cust,
       coalesce(last_month_profit,0) as last_month_profit,
       coalesce(last_month_sale_cust_num,0) as last_month_sale_cust_num,
       grouping__id
from (
select 
       performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name,
       business_type_name,
       business_type_code,
       sum(daily_plan_sale) as daily_plan_sale,
       sum(yesterday_sales_value) as yesterday_sales_value,
       sum(yesterday_profit) as yesterday_profit,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale) as month_sale,
       sum(coalesce(last_month_sale,0)) last_month_sale,
       sum(month_plan_profit) month_plan_profit,
       sum(coalesce(month_profit,0)) as month_profit,
       sum(coalesce(month_sale_cust_num,0)) as month_sale_cust_num,
       sum(coalesce(last_month_profit,0)) as last_month_profit,
       sum(coalesce(last_month_sale_cust_num,0)) as last_month_sale_cust_num,
       grouping__id
from csx_analyse_tmp.csx_analyse_tmpcity_attribute_03 a 
 group by  performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name,
       business_type_name,
       business_type_code
grouping sets 
(    ( performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name,
       business_type_name,
       business_type_code),  -- 1 城市客户属性
       ( performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name),  -- 2 城市汇总
     ( performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       business_type_name,
       business_type_code),  -- 3 省区客户属性
    ( performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ),  -- 4 省区汇总
      ( performance_region_code,
       performance_region_name,
       business_type_name,
       business_type_code),  -- 5 大区客户属性
    ( performance_region_code,
       performance_region_name ),  -- 6 大区汇总
     ()
       
       )
) a ;

select performance_region_code,
       performance_region_name ,
       coalesce(a.performance_province_code,'00') performance_province_code ,
       coalesce(performance_province_name,'-') performance_province_name ,
       coalesce(a.performance_city_code,'00') performance_city_code ,
       coalesce(performance_city_name,'-') performance_city_name ,
       coalesce(business_type_code,'00') business_type_code,
       coalesce(business_type_name,'-') business_type_name, 
       coalesce(month_sale_cust_num,0) as month_sale_cust_num,
       coalesce(month_sale_cust_num-last_month_sale_cust_num,0)   as mom_diff_sale_cust,
       coalesce(last_month_sale_cust_num,0) as last_month_sale_cust_num,
       grouping__id
from (
select 
       performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name,
       business_type_name,
       business_type_code,
       count(distinct case when month_sale>0 then a.customer_code end)as month_sale_cust_num,
       count(distinct case when last_month_sale>0 then  a.customer_code end)as last_month_sale_cust_num ,
       grouping__id
from csx_analyse_tmp.csx_analyse_tmpcity_attribute_00 a 
 group by performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name,
       business_type_name,
       business_type_code
grouping sets 
( (performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name,
       business_type_name,
       business_type_code),  -- 1 省区客户属性
    (performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name),  -- 2 省区汇总
     (performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       business_type_name,
       business_type_code),  -- 3 省区客户属性
    (performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ) , -- 4 大区汇总
     (performance_region_code,
       performance_region_name ,
       business_type_name,
       business_type_code),
       (performance_region_code,
       performance_region_name )
       ,
       ()
    )
) a
;

  insert overwrite table  csx_analyse.csx_analyse_fr_sale_cust_attribute_kanban partition(months)

 select 
      level_id,
     substr(regexp_replace('${edate}','-',''),1,6)as sales_month,
       performance_region_code,
       performance_region_name ,
       performance_province_code ,
       performance_province_name ,
       performance_city_code,
       performance_city_name,
       business_type_name,
       business_type_code
       daily_plan_sale,
       yesterday_sales_value,
       yesterday_sale_fill_rate,
       yesterday_profit,
       yesterday_profit_rate,
       month_plan_sale,
       month_sale,
       month_sale_fill_rate,
       last_month_sale,
       mom_sale_growth_rate,
       month_plan_profit,
       month_profit,
       month_profit_fill_rate,
       month_profit_rate,
       month_sale_cust_num,
       mom_diff_sale_cust,
       last_month_profit,
       last_month_sale_cust_num,
       current_timestamp(),
       substr(regexp_replace('${edate}','-',''),1,6)
from(
 select 
    case when  
          a.performance_city_name!='-' and a.business_type_name !='-'  then '1'
        when   a.performance_city_name!='-' and a.business_type_name ='-'  then '2'
        when a.performance_city_name ='-' and  a.business_type_name !='-'  then '4'
        when a.performance_province_name!='-' and a.business_type_name !='-'  then '5'
        when   a.performance_province_name!='-' and a.business_type_name ='-'  then '6'
        when a.performance_province_name ='-' and  a.business_type_name !='-'  then '7'
    else '8'
    end level_id,
       a.performance_region_code,
       a.performance_region_name ,
       a.performance_province_code ,
       a.performance_province_name ,
       a.performance_city_code,
       a.performance_city_name,
       a.business_type_name,
       a.business_type_code,
       a.daily_plan_sale,
       a.yesterday_sales_value,
       a.yesterday_sale_fill_rate,
       a.yesterday_profit,
       a.yesterday_profit_rate,
       a.month_plan_sale,
       a.month_sale,
       a.month_sale_fill_rate,
       a.last_month_sale,
       a.mom_sale_growth_rate,
       a.month_plan_profit,
       a.month_profit,
       a.month_profit_fill_rate,
       a.month_profit_rate,
      b.month_sale_cust_num,
      b.mom_diff_sale_cust,
       last_month_profit,
      b.last_month_sale_cust_num
from csx_analyse_tmp.csx_analyse_tmpcity_attribute_02 a 
left outer join csx_analyse_tmp.csx_analyse_tmpcity_attribute_01 b
    on a.performance_region_code=b.performance_region_code 
    and a.performance_region_name=b.performance_region_name
    and a.performance_province_name=b.performance_province_name
    and a.performance_province_code=b.performance_province_code
    and a.performance_city_name=b.performance_city_name
    and a.business_type_code=b.business_type_code
    and a.business_type_name=b.business_type_name
 
) a
;

-- 建表
	CREATE TABLE `csx_analyse.csx_analyse_fr_sale_cust_attribute_kanban`(
	  `level_id` string COMMENT '展示层级，1 明细、2 总计', 
	  `sales_month` string COMMENT '销售月份', 
	  `performance_region_code` string COMMENT '战区编码', 
	  `performance_region_name` string COMMENT '战区名称', 
	  `performance_province_code` string COMMENT '省区编码', 
	  `performance_province_name` string COMMENT '省区名称', 
 	  `performance_city_code` string COMMENT '城市组编码', 
	  `performance_city_name` string COMMENT '城市组编码',     
	  `business_type_code` int COMMENT '客户属性编码', 
	  `business_type_name` string COMMENT '客户属性名称', 
	  `daily_plan_sale` decimal(26,6) COMMENT '昨日计划销售额', 
	  `yesterday_sales_value` decimal(26,6) COMMENT '昨日销售额', 
	  `yesterday_sale_fill_rate` decimal(26,6) COMMENT '昨日销售达成率', 
	  `yesterday_profit` decimal(26,6) COMMENT '昨日毛利额', 
	  `yesterday_profit_rate` decimal(26,6) COMMENT '昨日毛利率', 
	  `month_plan_sale` decimal(26,6) COMMENT '月至今销售预算', 
	  `month_sale` decimal(26,6) COMMENT '月至今销售额', 
	  `month_sale_fill_rate` decimal(26,6) COMMENT '月至今销售达成率', 
	  `last_month_sale` decimal(26,6) COMMENT '月环比销售额', 
	  `mom_sale_growth_rate` decimal(26,6) COMMENT '月环比增长率', 
	  `month_plan_profit` decimal(26,6) COMMENT '月度毛利计划', 
	  `month_profit` decimal(26,6) COMMENT '月至今毛利额', 
	  `month_profit_fill_rate` decimal(26,6) COMMENT '月度毛利完成率', 
	  `month_profit_rate` decimal(26,6) COMMENT '月至今毛利率', 
	  `month_sale_cust_num` bigint COMMENT '月至今成交客户数', 
	  `mom_diff_sale_cust` bigint COMMENT '月至今成交客户差异数', 
	  `last_month_profit` decimal(26,6) COMMENT '环比毛利额', 
	  `last_month_sale_cust_num` bigint COMMENT '环比客户数', 
	  `update_time` timestamp COMMENT '更新时间')
	COMMENT '大区经营看板客户属性销售大客户'
	PARTITIONED BY ( 
	  `months` string COMMENT '按日分区')
	