



DROP table csx_tmp.ads_sale_r_d_city_cust_attribute_fr;
CREATE TABLE `csx_tmp.ads_sale_r_d_city_cust_attribute_fr`(
  `level_id` string COMMENT '展示层级，1 明细、2 城市组汇总、3、省区客户属性、4、省区汇总', 
  `sales_month` string COMMENT '销售月份', 
  `zone_id` string COMMENT '战区编码', 
  `zone_name` string COMMENT '战区名称', 
  `province_code` string COMMENT '省区编码', 
  `province_name` string COMMENT '省区名称', 
  `city_group_code` string COMMENT '城市组', 
  `city_group_name` string COMMENT '城市组名称', 
  `attribute_code` int COMMENT '客户属性编码', 
  `attribute` string COMMENT '客户属性名称', 
  `daily_plan_sale` decimal(26,6) COMMENT '昨日计划销售额', 
  `daily_sales_value` decimal(26,6) COMMENT '昨日销售额', 
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
  `month_sale_cust_num` bigint COMMENT '月至今成交客户数', 
  `mom_diff_sale_cust` bigint COMMENT '月至今成交客户差异数', 
  `last_month_profit` decimal(26,6) COMMENT '环比毛利额', 
  `last_month_sale_cust_num` bigint COMMENT '环比客户数', 
  `update_time` timestamp COMMENT '更新时间')
COMMENT '城市组客户属性销售大客户'
PARTITIONED BY ( `months` string COMMENT '按月分区')
STORED AS parquet 
;




set hive.execution.engine=tez;
set tez.queue.name=caishixian;
set hive.exec.parallel=true; 
set hive.exec.parallel.thread.number=100;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set hive.exec.dynamic.partition.mode=nonstrict;
-- 当前日期
SET edate= '${enddate}';
-- 月初
SET sdate=trunc(${hiveconf:edate},'MM');
-- 上月初
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
-- 上月当前日期
SET l_edate=add_months(${hiveconf:edate},-1);



-- 客户属性数据插入
drop table if exists csx_tmp.temp_city_attribute_01;
create temporary table csx_tmp.temp_city_attribute_01
as 
select  
       zone_id,zone_name ,
       a.province_code ,
       province_name ,
       a.city_group_code,
        b.city_group_name,
       attribute_code,
       case when attribute_code='7' then 'BBC'
            when attribute_code='3' then '贸易客户'
            when attribute_code='2' then '福利单'
            when attribute_code='5' then '合伙人客户'
            when attribute_code='1' then '日配单'
            else attribute_code
            end attribute,       
       sum(daily_sales_value/10000 )as daily_sales_value,
       sum(daily_profit/10000) as daily_profit,
      --sum(daily_profit)/sum(daily_sales_value) as days_profit_rate,
       sum(month_sale/10000 )month_sale,
       sum(last_month_sale/10000 ) as last_month_sale,
      -- (sum(month_sale)- coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as mom_sale_rate,
       sum(month_profit/10000)month_profit,
       sum(month_profit)/sum(month_sale) as profit_rate,
       sum(month_sale_cust_num )as month_sale_cust_num,
       --sum(month_sale_cust_num-last_month_sale_cust_num) as diff_sale_cust,
       sum(last_month_profit/10000) as last_month_profit,
       sum(last_month_sale_cust_num) as last_month_sale_cust_num,
       sum(0) as daily_plan_sale,
       sum(0)month_plan_sale ,
       sum(0)month_plan_profit
from (
   SELECT 
       province_code ,
       a.city_group_code,
       case when channel='7' then '7'
            when attribute_code='3' then '3'
            when order_kind='WELFARE' then '2'
            when attribute_code='5' then '5'
            else '1'
            end attribute_code,
       sum(case when sdt= regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sales_value,
       sum(case when sdt= regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
       sum(sales_value )month_sale,
       sum(profit )month_profit,
       count(distinct a.customer_no )as month_sale_cust_num,
       0 as last_month_sale,
       0 as last_month_profit,
       0 as last_month_sale_cust_num,
       0 as daily_plan_sale,
       0 as month_plan_sale ,
       0 as month_plan_profit 
   FROM csx_dw.dws_sale_r_d_customer_sale a 
    where sdt>=regexp_replace(${hiveconf:sdate},'-','') 
    and sdt<= regexp_replace(${hiveconf:edate},'-','') 
    and a.channel in('1','7','9')
   group by 
       case when channel='7' then '7'
            when attribute_code='3' then '3'
            when order_kind='WELFARE' then '2'
            when attribute_code='5' then '5'
            else '1' end ,
            province_code,
            city_group_code
 union all 
   SELECT 
       province_code ,
       city_group_code,
       case when channel='7' then '7'
            when attribute_code='3' then '3'
            when order_kind='WELFARE' then '2'
            when attribute_code='5' then '5'
            else '1'
            end attribute_code,
       0 as daily_sales_value,
       0 as daily_profit,
       0 as month_sale,
       0 as month_profit,
       0 as month_sale_cust_num,
       sum(sales_value)as last_month_sale,
       sum(profit)as last_month_profit,
       count(distinct a.customer_no)as last_month_sale_cust_num ,
        0 as daily_plan_sale,
        0 as month_plan_sale ,
        0 as month_plan_profit 
   FROM csx_dw.dws_sale_r_d_customer_sale a     
    where sdt>= regexp_replace(${hiveconf:l_sdate},'-','') 
    and sdt<= regexp_replace(${hiveconf:l_edate},'-','') 
    and a.channel in('1','7','9')
   group by
       case when channel='7' then '7'
            when attribute_code='3' then '3'
            when order_kind='WELFARE' then '2'
            when attribute_code='5' then '5'
            else '1'
            end  ,
       province_code,
       city_group_code

) a 
left join 
(select DISTINCT province_code,
    province_name ,
    region_code zone_id,
    region_name zone_name,
    case when length(city_group_code)=0 then '-' else city_group_code end city_group_code,
    city_group_name
from csx_dw.dim_area where area_rank='11') b on a.province_code=b.province_code and a.city_group_code=b.city_group_code
group by zone_id,zone_name ,
        a.province_code ,
        province_name,
        case when attribute_code='7' then 'BBC'
            when attribute_code='3' then '贸易客户'
            when attribute_code='2' then '福利单'
            when attribute_code='5' then '合伙人客户'
            when attribute_code='1' then '日配单'
            else attribute_code
            end ,
        attribute_code,
        a.city_group_code,
        b.city_group_name
;
drop table if exists csx_tmp.temp_city_attribute_02;
create temporary table csx_tmp.temp_city_attribute_02 as 
select 
       
       zone_id,
       zone_name ,
       coalesce(a.province_code,'00') province_code ,
       coalesce(province_name,'-') province_name ,
       coalesce(city_group_code,'00' ) city_group_code,
       coalesce(a.city_group_name,'-') city_group_name,
       coalesce(attribute_code,'00') attribute_code,
       coalesce(attribute,'-') attribute, 
       daily_plan_sale,
       daily_sales_value,
       coalesce(daily_sales_value/daily_plan_sale,0) as daily_sale_fill_rate,
       daily_profit,
       coalesce(daily_profit/daily_sales_value,0) as daily_profit_rate,
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
       zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       city_group_code,
       a.city_group_name,
       attribute_code,
       attribute, 
       sum(daily_plan_sale) as daily_plan_sale,
       sum(daily_sales_value) as daily_sales_value,
       sum(daily_profit) as daily_profit,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale) as month_sale,
       sum(coalesce(last_month_sale,0)) last_month_sale,
       sum(month_plan_profit) month_plan_profit,
       sum(coalesce(month_profit,0)) as month_profit,
       sum(coalesce(month_sale_cust_num,0)) as month_sale_cust_num,
       sum(coalesce(last_month_profit,0)) as last_month_profit,
       sum(coalesce(last_month_sale_cust_num,0)) as last_month_sale_cust_num,
       grouping__id
from csx_tmp.temp_city_attribute_01 a 
 group by zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       city_group_code,
       a.city_group_name,
       attribute_code,
       attribute
grouping sets 
((zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       city_group_code,
       a.city_group_name,
       attribute_code,
       attribute),   --1 城市组客户属性
        (zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       city_group_code,
       a.city_group_name), --2 城市组汇总
            (zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       attribute_code,
       attribute),  -- 3 省区客户属性
       (zone_id,
       zone_name ,
       a.province_code ,
       province_name )  -- 4 省区汇总
      
       )
) a ;

 insert overwrite table  csx_tmp.ads_sale_r_d_city_cust_attribute_fr partition(months)
 
 select 
      level_id,
     substr(regexp_replace(${hiveconf:edate},'-',''),1,6)as sales_month,
       zone_id,
       zone_name ,
       province_code ,
       province_name ,
       city_group_code,
       city_group_name,
       attribute_code,
       attribute, 
       daily_plan_sale,
       daily_sales_value,
       daily_sale_fill_rate,
       daily_profit,
       daily_profit_rate,
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
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from(
 select 
    case when attribute !='-' and a.city_group_name !='-' then '1'
        when attribute ='-' and city_group_name !='-'  then '2'
        when city_group_name ='-' and  province_name!='-' then '3'
    else '4'
    end level_id,
       zone_id,
       zone_name ,
       province_code ,
       province_name ,
       city_group_code,
       city_group_name,
       attribute_code,
       attribute, 
       daily_plan_sale,
       daily_sales_value,
       daily_sale_fill_rate,
       daily_profit,
       daily_profit_rate,
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
       last_month_sale_cust_num
from csx_tmp.temp_city_attribute_02 a)a;












    union all 
    select province_code,
        '' as city_group_code,
        customer_attribute_code as attribute_code,
        0 as daily_sales_value,
        0 as daily_profit,
        0 as month_sale,
        0 as month_profit,
        0 as month_sale_cust_num,
        0 as last_month_sale,
        0 as last_month_profit,
        0 as last_month_sale_cust_num,
        0 as daily_plan_sale,
        sum(plan_sales_value)month_plan_sale ,
        sum(plan_profit)month_plan_profit 
    from csx_tmp.dws_csms_province_month_sale_plan_tmp
         where month=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
				and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
    -- and channel_name='大客户'
    group by  province_code,
        customer_attribute_code,
       '' as  city_group_code;