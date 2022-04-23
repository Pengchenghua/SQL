-- 大区渠道销售报表【20220423】
--set hive.execution.engine=tez;
--set tez.queue.name=caishixian;
--set tez.am.speculation.enabled=true;  --是否开启推测执行，默认是false，在出现最后一个任务很慢的情况下，建议把这个参数设置为true
--set tez.am.resource.memory.mb=8000;  --am分配的内存大小，默认1024
--set tez.task.resource.memory.mb=8000;  --分配的内存，默认1024 ,出现内存不够时候，设置更大点
--set tez.am.resource.cpu.vcores=8;  -- am分配的cpu个数，默认1
set hive.exec.parallel=true; 
set hive.exec.parallel.thread.number=100;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.support.quoted.identifiers=none;

-- 当前日期
SET edate= '${enddate}';
-- 月初
SET sdate=trunc(${hiveconf:edate},'MM');
-- 上月初
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
-- 上月当前日期
--SET l_edate=add_months(${hiveconf:edate},-1);

-- 上月当前日期，判断 月末取月末最后一天，不等于月末则取当前日期


SET l_edate=  if(${hiveconf:edate}=last_day(${hiveconf:edate}),last_day(add_months(${hiveconf:edate},-1)),add_months(${hiveconf:edate},-1)) ;

-- select ${hiveconf:l_edate};
 -- 昨日\月销售数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    sum(yesterday_sales_value) as yesterday_sales_value, 
    sum(real_yesterday_sales_value)as real_yesterday_sales_value,
   -- last_day_sales,
    sum(yesterday_profit) as yesterday_profit,
   -- yesterday_negative_profit,
    sum(yesterday_new_customer_sale) as yesterday_new_customer_sale,
    sum(yesterday_often_customer_sale) as yesterday_often_customer_sale,
    count(DISTINCT CASE WHEN yesterday_sales_value>0  then a.customer_no end)  as yesterday_customer_num, 
    -- 月累计
    sum(months_sales_value)as months_sales_value, 
    sum(real_months_sales_value)as real_months_sales_value,
    sum(months_profit)as months_profit,
   -- negative_profit,
    sum(months_new_customer_sale) as months_new_customer_sale,
    sum(months_often_customer_sale) as  months_often_customer_sale,
    count(distinct case when months_sales_value>0 then  a.customer_no end ) as months_customer_num
FROM 
(
SELECT CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       a.province_code,
       a.province_name,
       a.customer_no,
    sum(CASE  WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value  END)AS yesterday_sales_value, 
     sum(CASE  WHEN sdt=regexp_replace(${hiveconf:edate},'-','') and a.business_type_name!='批发内购' THEN sales_value  END)AS real_yesterday_sales_value, --不含批发内购销售
    sum(CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN profit  END)AS yesterday_profit,
    sum(CASE WHEN substr(sdt,1,6)=first_sale_mon  AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value     END) AS yesterday_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value END) AS yesterday_often_customer_sale,
    count(DISTINCT CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN a.customer_no END) AS yesterday_customer_num, 
    -- 月累计
    sum(sales_value)AS months_sales_value, 
    sum(case when a.business_type_name!='批发内购' then sales_value end )AS  real_months_sales_value, 
    sum(profit)AS months_profit,
    -- sum(case when profit <0 then profit end ) as negative_profit,
    sum(CASE WHEN substr(sdt,1,6)=first_sale_mon THEN sales_value  END) AS months_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon THEN sales_value END) AS months_often_customer_sale,
    count(DISTINCT a.customer_no) AS months_customer_num
FROM csx_dw.dws_sale_r_d_detail a
left JOIN
  (SELECT customer_no,
          substr(first_order_date,1,6) AS first_sale_mon
   FROM csx_dw.dws_crm_w_a_customer_active
   WHERE sdt=regexp_replace(${hiveconf:edate},'-','')) b ON a.customer_no=b.customer_no
WHERE sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
GROUP BY province_code,
         province_name,
         CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END,
       a.customer_no
)a 
GROUP BY 
    a.channel_name,
    a.province_code,
    a.province_name
;


-- 关联上周环比数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_01;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_01 AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    yesterday_sales_value, 
    real_yesterday_sales_value,
    last_week_daily_sales,
    yesterday_profit,
    last_week_daily_profit,
   -- yesterday_negative_profit,
    yesterday_new_customer_sale,
    yesterday_often_customer_sale,
    yesterday_customer_num, 
    -- 月累计
    months_sales_value, 
    real_months_sales_value,
    months_profit,
   -- negative_profit,
    months_new_customer_sale,
    months_often_customer_sale,
    months_customer_num
FROM 
csx_tmp.temp_war_zone_sale  a 
left join 
(select CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       province_code,
       sum(a.sales_value)as last_week_daily_sales,      --上周同日销售额
       sum(profit) as last_week_daily_profit
    from csx_dw.dws_sale_r_d_detail a
    where sdt=regexp_replace(date_sub(${hiveconf:edate},7),'-','')
 	and  (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
    group by CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END ,
       province_code
     ) as c on a.province_code=c.province_code and a.channel_name=c.channel_name;

-- select regexp_replace(date_sub(${hiveconf:edate},7),'-','');
-- show create table csx_dw.ads_sale_w_d_ads_customer_sales_q;
-- 上月环比数据

DROP TABLE IF EXISTS csx_tmp.temp_ring_war_zone_sale;
CREATE TEMPORARY TABLE csx_tmp.temp_ring_war_zone_sale AS
SELECT CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       province_code,
       province_name,
       sum(CASE
               WHEN sdt=regexp_replace(${hiveconf:l_edate},'-','') THEN sales_value
           END)AS last_month_daily_sale,
        sum(CASE when sdt=regexp_replace(${hiveconf:l_edate},'-','') then profit end ) as last_month_daily_profit,
       sum(sales_value)AS last_month_sale,
       sum(profit) last_month_profit
FROM csx_dw.dws_sale_r_d_detail a
WHERE sdt>=regexp_replace(${hiveconf:l_sdate},'-', '')
  AND sdt<=regexp_replace(${hiveconf:l_edate},'-','')
	and  (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
GROUP BY province_code,
         province_name,
         CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END ;


-- 负毛利

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_02;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_02 AS

select
    '大客户' as channel_name,
    province_code ,
    province_name  ,
    count(distinct  1 )as sale_sku,
    sum(sale/10000)sale,
    sum(profit/10000 )profit,
    sum(profit) /sum(sale) as profit_rate,
    sum(case when profit<0 and sdt=regexp_replace(${hiveconf:edate},'-','') then profit end ) as negative_days_profit,
    sum(case when profit<0 then profit end ) as negative_profit
 from (
select
    province_code ,
    province_name ,
    sdt,
    a.customer_no,
    avg(cost_price )avg_cost,
    avg(sales_price )avg_sale,
    sum(sales_qty )qty,
    sum(sales_value) sale,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_detail a 
where
    sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    and channel_code in ('1','7','9')
    and return_flag!='X'
	and  (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
group by 
   province_code ,
    province_name ,
    sdt,
    a.customer_no
   ) a 
group by 
   province_code ,
   province_name ;
 
-- 计划表统计  
drop table if exists csx_tmp.temp_plan_sale;
create temporary table csx_tmp.temp_plan_sale
as 
select trim(province_code)province_code,
    channel_name,
    sum(daily_plan_sales_value)daily_plan_sales_value,
    sum(daily_plan_profit)  daily_plan_profit,
    sum(plan_sales_value)plan_sales_value ,
    sum(plan_profit)plan_profit 
   from 
   (select province_code,'大客户' as channel_name,0 daily_plan_sales_value,0 daily_plan_profit,(plan_sales_value)plan_sales_value ,(plan_profit)plan_profit 
   from csx_tmp.dws_csms_province_month_sale_plan_tmp
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
    and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
    and customer_attribute_name!='批发内购'
	and  (weeknum is null or weeknum='')
     union all 
    select province_code,'商超' as channel_name,0 daily_plan_sales_value,0 daily_plan_profit,(plan_sales_value)plan_sales_value ,(plan_profit)plan_profit 
    from csx_tmp.dws_ssms_province_month_sale_plan_tmp
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
     and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
	and  (weeknum is null or weeknum='')
	union all 
    select province_code,channel_name,0 daily_plan_sales_value,0 daily_plan_profit,(sales_value )plan_sales_value ,0 plan_profit 
    from csx_tmp.report_sale_r_m_province_target 
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
        and province_code not in ('32','23','24')
    ) d 
    group by 
province_code,
channel_name
;

-- select * from  csx_tmp.temp_plan_sale;

-- select * from csx_tmp.dws_daily_sales_plan;
-- 本期同期负毛利汇总 
-- INSERT overwrite table csx_tmp.ads_sale_r_d_zone_sales_fr partition(months)
drop table if exists  csx_tmp.temp_plan_sale_01;
create temporary table  csx_tmp.temp_plan_sale_01 as 
SELECT '1' level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       case when a.channel_name='大客户' then '1' when a.channel_name='商超' then '2' else channel_name end  as channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       --0 as daily_plan_sale,
       sum(yesterday_sales_value/10000 )AS daily_sales_value,
       --0 as daily_sale_fill_rate,
       sum(real_yesterday_sales_value/10000) AS real_daily_sales_value,
       sum(last_week_daily_sales/10000 ) as last_week_daily_sales,      -- 上周同日销售额
       (coalesce(sum(yesterday_sales_value),0)-coalesce(sum(last_week_daily_sales),0))/coalesce(sum(last_week_daily_sales),0) as daily_sale_growth_rate,
       --0 as daily_plan_profit,
       sum(last_week_daily_profit)/10000 last_week_daily_profit,  -- 上周毛利额
       sum(yesterday_profit/10000 )AS daily_profit,
        --0 as daily_profit_fill_rate,
       coalesce(sum(yesterday_profit)/sum(yesterday_sales_value),0) as daily_profit_rate,
       (negative_days_profit/10000) as daily_negative_profit,
       sum(yesterday_often_customer_sale/10000 )AS daily_often_cust_sale,
       sum(yesterday_new_customer_sale/10000 )AS daily_new_cust_sale,
       sum(yesterday_customer_num)AS daily_sale_cust_num,
       -- plan_sales_value as month_plan_sale,
       sum(months_sales_value/10000 )AS month_sale_value,
       sum(real_months_sales_value/10000) as real_months_sales_value,
       -- sum(months_sales_value/10000 )/plan_sales_value as month_sale_fill_rate,
       sum(last_month_sale/10000 )AS last_month_sale,
       sum(last_month_profit)/10000 as last_month_profit,
       (coalesce(sum(months_sales_value),0)-coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as mom_sale_growth_rate,
      -- plan_profit as month_plan_profit,
       sum(months_profit/10000 )AS month_profit,
      -- sum(months_profit/10000 )/plan_profit as month_proft_fill_rate,
       sum(months_profit)/sum(months_sales_value) as month_profit_rate,
       (negative_profit/10000) as month_negative_profit, 
       sum(months_often_customer_sale/10000 )AS month_often_cust_sale,
       sum(months_new_customer_sale/10000 )AS month_new_cust_sale,
       sum(months_customer_num)AS month_sale_cust_num,
       sum(last_month_daily_sale/10000 ) AS last_month_daily_sale
FROM
  (SELECT channel_name,
          province_code,
          province_name,
          yesterday_sales_value,
          real_yesterday_sales_value,
          yesterday_profit,
          yesterday_often_customer_sale,
          yesterday_new_customer_sale,
          yesterday_customer_num,
          months_sales_value,
          real_months_sales_value,
          months_profit,
          months_often_customer_sale,
          months_new_customer_sale,
          months_customer_num,
          last_week_daily_sales,
          last_week_daily_profit,
          0 AS last_month_daily_sale,
          0 last_month_daily_profit,
          0 AS last_month_sale,
          0 as last_month_profit
   FROM csx_tmp.temp_war_zone_sale_01
   UNION ALL SELECT channel_name,
                    province_code,
                    province_name,
                    0 AS yesterday_sales_value,
                    0  as real_yesterday_sales_value,
                    0 AS yesterday_profit,
                    0 AS yesterday_often_customer_sale,
                    0 AS yesterday_new_customer_sale,
                    0 AS yesterday_customer_num,
                    0 AS months_sales_value,
                    0 as real_months_sales_value,
                    0 AS months_profit,
                    0 AS months_often_customer_sale,
                    0 AS months_new_customer_sale,
                    0 AS months_customer_num,
                    0 as last_week_daily_sales,
                    0 last_week_daily_profit,
                    last_month_daily_sale ,
                    last_month_daily_profit  ,
                    last_month_sale  ,
                    last_month_profit 
   FROM csx_tmp.temp_ring_war_zone_sale
   ) a  
   left join 
   csx_tmp.temp_war_zone_sale_02 c on a.province_code=c.province_code and a.channel_name=c.channel_name
   left join 
   (select distinct province_code,province_name,region_code as zone_id,region_name as zone_name from csx_dw.dws_sale_w_a_area_belong ) b on 
    case when a.province_code in ('35','36') then '35' else a.province_code end =b.province_code
GROUP BY a.channel_name,
         a.province_code,
         a.province_name,
         zone_id,
         zone_name,
         negative_days_profit,
         negative_profit
    ;
    
    
    
    
--插入数据

-- INSERT overwrite table csx_tmp.ads_sale_r_d_zone_sales_fr partition(sdt)
drop table csx_tmp.temp_sale_into_01;
CREATE temporary table csx_tmp.temp_sale_into_01 as 
SELECT level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       daily_plan_sales_value as daily_plan_sale,
       daily_sales_value,
       real_daily_sales_value,
       coalesce(real_daily_sales_value/d.daily_plan_sales_value,0)    real_daily_sale_fill_rate,
       coalesce(daily_sales_value/d.daily_plan_sales_value,0)    daily_sale_fill_rate,
       last_week_daily_sales as last_day_sales,
       (coalesce((daily_sales_value),0)-coalesce((last_week_daily_sales),0))/coalesce((last_week_daily_sales),0) as daily_sale_growth_rate,
       d.daily_plan_profit,
       daily_profit,
       coalesce(daily_profit/d.daily_plan_profit,0) as daily_profit_fill_rate,
       coalesce((daily_profit)/(daily_sales_value),0) as daily_profit_rate,
       daily_negative_profit,
       daily_often_cust_sale,
       daily_new_cust_sale,
       daily_sale_cust_num,
       plan_sales_value as month_plan_sale,
       month_sale_value,
       real_months_sales_value real_month_sale_value,
       (real_months_sales_value/plan_sales_value) as real_month_sale_fill_rate,
       (month_sale_value/plan_sales_value) as month_sale_fill_rate,
       last_month_sale,
       last_month_profit,
       (coalesce((month_sale_value),0)-coalesce((last_month_sale),0))/coalesce((last_month_sale),0) as mom_sale_growth_rate,
       d.plan_profit as month_plan_profit,
       month_profit,
       (month_profit /d.plan_profit) as month_proft_fill_rate,
       (month_profit)/(month_sale_value) as month_profit_rate,
       month_negative_profit, 
       month_often_cust_sale,
       month_new_cust_sale,
       month_sale_cust_num,
       last_month_daily_sale as last_months_daily_sale
FROM
 csx_tmp.temp_plan_sale_01 a  
   left join 
   csx_tmp.temp_plan_sale d on a.province_code=d.province_code and trim(a.channel_name)=trim(d.channel_name)
   
;


-- 插入 渠道小计
drop table csx_tmp.temp_sale_into_02;
create temporary table csx_tmp.temp_sale_into_02 as 
select  
       sales_months,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       sum(daily_plan_sale) daily_plan_sale,
       sum(daily_sales_value) daily_sales_value,
       sum(real_daily_sales_value) real_daily_sales_value,
      -- sum()coalesce(real_daily_sales_value/d.daily_plan_sales_value,0)    real_daily_sale_fill_rate,
      -- sum()coalesce(daily_sales_value/d.daily_plan_sales_value,0)    daily_sale_fill_rate,
       sum(last_day_sales) last_day_sales,
      -- sum()(coalesce((daily_sales_value),0)-coalesce((last_week_daily_sales),0))/coalesce((last_week_daily_sales),0) as daily_sale_growth_rate,
       sum(daily_plan_profit) daily_plan_profit,
       sum(daily_profit)daily_profit,
      -- sum()coalesce(daily_profit/d.daily_plan_profit,0) as daily_profit_fill_rate,
      -- sum()coalesce((daily_profit)/(daily_sales_value),0) as daily_profit_rate,
       sum(daily_negative_profit)daily_negative_profit,
       sum(daily_often_cust_sale)daily_often_cust_sale,
       sum(daily_new_cust_sale)daily_new_cust_sale,
       sum(daily_sale_cust_num)daily_sale_cust_num,
       sum(month_plan_sale)month_plan_sale,
       sum(month_sale_value)month_sale_value,
       sum(real_month_sale_value) real_month_sale_value,
      -- sum()(real_months_sales_value/plan_sales_value) as real_month_sale_fill_rate,
      -- sum()(month_sale_value/plan_sales_value) as month_sale_fill_rate,
       sum(last_month_sale)last_month_sale,
       sum(last_month_profit)last_month_profit,
       -- sum()(coalesce((month_sale_value),0)-coalesce((last_month_sale),0))/coalesce((last_month_sale),0) as mom_sale_growth_rate,
       sum(month_plan_profit)month_plan_profit,
       sum(month_profit)month_profit,
      -- sum()(month_profit /d.plan_profit) as month_proft_fill_rate,
      -- sum()(month_profit)/(month_sale_value) as month_profit_rate,
       sum(month_negative_profit)month_negative_profit, 
       sum(month_often_cust_sale)month_often_cust_sale,
       sum(month_new_cust_sale) month_new_cust_sale,
       sum(month_sale_cust_num) month_sale_cust_num,
       sum(last_months_daily_sale) last_month_daily_sale,
       grouping__id
    from csx_tmp.temp_sale_into_01 a
    group by   
       sales_months,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name
     grouping sets
     (( sales_months,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name),
       ( sales_months,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name),
       ( sales_months,
       zone_id,
       zone_name),
       ( sales_months,
       zone_id,
       zone_name,
       a.province_code,
       a.province_name))
       ;
       
     select 
     case when grouping__id='127' then '1'
        when grouping__id='7' then '3' 
          when grouping__id='31' then '2'
          when grouping__id='103' then '4'
         else grouping__id end level_id,
       sales_months,
       zone_id,
       zone_name,
       coalesce(channel_code,'00')channel_code,
       coalesce(a.channel_name,'合计')channel_name,
       coalesce(a.province_code,'00')province_code,
       coalesce(a.province_name,'小计')province_name,
       daily_plan_sale,
       daily_sales_value,
       real_daily_sales_value,
       coalesce(real_daily_sales_value/daily_plan_sale,0)    real_daily_sale_fill_rate,
       coalesce(daily_sales_value/daily_plan_sale,0)    daily_sale_fill_rate,
        last_day_sales,
       (coalesce((daily_sales_value),0)-coalesce((last_day_sales),0))/coalesce((last_day_sales),0) as daily_sale_growth_rate,
       daily_plan_profit,
       daily_profit,
       coalesce(daily_profit/daily_plan_profit,0) as daily_profit_fill_rate,
       coalesce((daily_profit)/(daily_sales_value),0) as daily_profit_rate,
       daily_negative_profit,
       daily_often_cust_sale,
       daily_new_cust_sale,
       daily_sale_cust_num,
       month_plan_sale,
       month_sale_value,
       real_month_sale_value,
       (real_month_sale_value/month_plan_sale) as real_month_sale_fill_rate,
       (month_sale_value/month_plan_sale) as month_sale_fill_rate,
        last_month_sale,
       last_month_profit,
        (coalesce((month_sale_value),0)-coalesce((last_month_sale),0))/coalesce((last_month_sale),0) as mom_sale_growth_rate,
       month_plan_profit,
       month_profit,
       (month_profit /month_plan_profit) as month_proft_fill_rate,
      (month_profit)/(month_sale_value) as month_profit_rate,
       month_negative_profit, 
       month_often_cust_sale,
       month_new_cust_sale,
       month_sale_cust_num,
       last_month_daily_sale,
       grouping__id
    from csx_tmp.temp_sale_into_02 a;


-- 统计新签约客户数及签约客户金额

drop table if exists csx_tmp.temp_sale_01;
create temporary table csx_tmp.temp_sale_01 as 
select 
channel_code,
coalesce( region_code,'00')  as region_code,
coalesce( sales_province_code,'00') as province_code,
daily_sign_cust_num,
daily_sign_amount,
sign_cust_num,
sign_amount,
group_id
from 
(
select 
'1' as channel_code,
region_code,
sales_province_code,
count(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace(${hiveconf:edate},'-','') then customer_no end ) as daily_sign_cust_num,
sum(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace(${hiveconf:edate},'-','') then estimate_contract_amount end ) as daily_sign_amount,
count(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) then customer_no end ) as sign_cust_num,
sum(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) then estimate_contract_amount end ) as sign_amount,
grouping__id as group_id
from csx_dw.dws_crm_w_a_customer a 
join 
(select distinct region_code,city_group_code,province_code from csx_dw.dws_sale_w_a_area_belong ) b on a.city_group_code=b.city_group_code and a.sales_province_code=b.province_code
where sdt='current' 
-- and sales_province_code='15'
group by sales_province_code,
region_code
grouping sets
(
(
region_code,
sales_province_code
),
(
region_code),
())
)a
;

insert overwrite table  csx_tmp.ads_sale_r_d_zone_sales_fr partition(sdt)
SELECT a.*,
    daily_sign_cust_num,
    daily_sign_amount,
    sign_cust_num,
    sign_amount,
    current_timestamp(),
    regexp_replace(${hiveconf:edate},'-','') 
from(
SELECT * from csx_tmp.temp_sale_into_01
union all 
select * from csx_tmp.temp_sale_into_02
UNION all 
select * from csx_tmp.temp_sale_into_03
union all 
select * from csx_tmp.temp_sale_into_04
) a 
LEFT JOIN
csx_tmp.temp_sale_01 b on a.zone_id=b.region_code and a.province_code=b.province_code and a.channel_code=b.channel_code
;



