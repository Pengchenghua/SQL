-- 城市组销售

set hive.exec.dynamic.partition.mode=nonstrict;
-- 当前日期
SET edate= '${enddate}';
-- 月初
SET sdate=trunc(${hiveconf:edate},'MM');
-- 上月初
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
-- 上月当前日期

SET l_edate=  if(${hiveconf:edate}=last_day(${hiveconf:edate}),last_day(add_months(${hiveconf:edate},-1)),add_months(${hiveconf:edate},-1)) ;


-- 昨日\月销售数据

DROP TABLE IF EXISTS csx_tmp.temp_war_city_sale;


CREATE TEMPORARY TABLE csx_tmp.temp_war_city_sale AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    sum(yesterday_sales_value) as yesterday_sales_value, 
    sum(real_yesterday_sales_value) as real_yesterday_sales_value,
   -- last_day_sales,
    sum(yesterday_profit) as yesterday_profit,
   -- yesterday_negative_profit,
    sum(yesterday_new_customer_sale) as yesterday_new_customer_sale,
    sum(yesterday_often_customer_sale) as yesterday_often_customer_sale,
    count(DISTINCT case when yesterday_sales_value>0 then  a.customer_no end ) as  yesterday_customer_num, 
    -- 月累计
    sum(months_sales_value) as months_sales_value,
    sum(real_month_sale_value) as real_month_sale_value,
    sum(months_profit) as months_profit,
   -- negative_profit,
    sum(months_new_customer_sale) as months_new_customer_sale,
    sum(months_often_customer_sale) as months_often_customer_sale,
    count(DISTINCT case when months_sales_value>0 then  a.customer_no end ) as months_customer_num
FROM 
(
SELECT CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大' 
           ELSE a.channel_name
       END channel_name,
       a.province_code,
       a.province_name,
       a.city_group_code,
       a.city_group_name,
       a.customer_no,
       sum(CASE  WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value  END)AS yesterday_sales_value, 
    sum(CASE  WHEN sdt=regexp_replace(${hiveconf:edate},'-','') and a.business_type_name!='批发内购' THEN sales_value  END)AS real_yesterday_sales_value, --不含批发内购销售
     --  sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') and profit <0 then profit end ) as yesterday_negative_profit,
    -- sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') then sales_value end ) as last_day_sales,
    sum(CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN profit  END)AS yesterday_profit,
    sum(CASE WHEN substr(sdt,1,6)=first_sale_mon  AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value     END) AS yesterday_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value END) AS yesterday_often_customer_sale,
    count(DISTINCT CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN a.customer_no END) AS yesterday_customer_num, 
    -- 月累计
    sum(sales_value)AS months_sales_value,
    sum(case when a.business_type_name!='批发内购' then sales_value end )AS  real_month_sale_value, 
    sum(profit)AS months_profit,
    -- sum(case when profit <0 then profit end ) as negative_profit,
    sum(CASE WHEN substr(sdt,1,6) =first_sale_mon THEN sales_value  END) AS months_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon THEN sales_value END) AS months_often_customer_sale,
    count(DISTINCT a.customer_no) AS months_customer_num
FROM csx_dw.dws_sale_r_d_detail a
left JOIN
  (SELECT customer_no,
          substr(first_order_date,1,6) AS first_sale_mon
   FROM csx_dw.dws_crm_r_a_customer_active_info
   WHERE sdt=regexp_replace(${hiveconf:edate},'-','')) b ON a.customer_no=b.customer_no
WHERE sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
GROUP BY province_code,
         province_name,
         a.city_group_code,
         a.city_group_name,
         CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大' 
           ELSE a.channel_name
       END,
       a.customer_no
)a 
group by a.channel_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name
;

-- 关联上周环比数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_01;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_01 AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    yesterday_sales_value,
    real_yesterday_sales_value,
    last_day_sales,
    yesterday_profit,
   -- yesterday_negative_profit,
    yesterday_new_customer_sale,
    yesterday_often_customer_sale,
    yesterday_customer_num, 
    -- 月累计
    months_sales_value, 
    real_month_sale_value,
    months_profit,
   -- negative_profit,
    months_new_customer_sale,
    months_often_customer_sale,
    months_customer_num
FROM 
csx_tmp.temp_war_city_sale  a 
left join 
(select CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大' 
           ELSE a.channel_name
       END channel_name,
       province_code,
        a.city_group_code,
        a.city_group_name,
       sum(a.sales_value)as last_day_sales
    from csx_dw.dws_sale_r_d_detail a
    where sdt=regexp_replace(date_sub(${hiveconf:edate},7),'-','')
     and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
    group by CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大' 
           ELSE a.channel_name
       END ,
       a.city_group_code,
       a.city_group_name,
       province_code
     ) as c on a.province_code=c.province_code and a.channel_name=c.channel_name and a.city_group_code=c.city_group_code;

-- select regexp_replace(date_sub(${hiveconf:edate},7),'-','');
-- show create table csx_dw.ads_sale_w_d_ads_customer_sales_q;
-- 上月环比数据

DROP TABLE IF EXISTS csx_tmp.temp_ring_war_zone_sale;
CREATE TEMPORARY TABLE csx_tmp.temp_ring_war_zone_sale AS
SELECT CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大' 
           ELSE a.channel_name
       END channel_name,
       province_code,
       province_name,
    a.city_group_code,
    a.city_group_name,
       sum(CASE
               WHEN sdt=regexp_replace(${hiveconf:l_edate},'-','') THEN sales_value
           END)AS last_yesterday_sales_value,
       sum(sales_value)AS last_months_sales_value
FROM csx_dw.dws_sale_r_d_detail a
WHERE sdt>=regexp_replace(${hiveconf:l_sdate},'-', '')
  AND sdt<=regexp_replace(${hiveconf:l_edate},'-','')
GROUP BY province_code,
         province_name,
         a.city_group_code,
    a.city_group_name,
         CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大' 
           ELSE a.channel_name
       END ;


-- 负毛利

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_02;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_02 AS

select
    '大' as channel_name,
    province_code ,
    province_name  ,
    a.city_group_code,
    a.city_group_name,
    count(distinct  goods_code )as sale_sku,
    sum(sale/10000)sale,
    sum(profit/10000 )profit,
    sum(profit) /sum(sale) as profit_rate,
    sum(case when profit<0 and sdt=regexp_replace(${hiveconf:edate},'-','') then profit end ) as negative_days_profit,
    sum(case when profit<0 then profit end ) as negative_profit
 from (
select
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    sdt,
    a.customer_no,
    goods_code ,
    goods_name,
    division_code ,division_name ,
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
group by 
   province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    sdt,
    a.customer_no,
    goods_code ,
    goods_name,
    division_code ,division_name 
   ) a 
group by 
   province_code ,
   province_name ,
   a.city_group_code,
   a.city_group_name;
 
-- 计划表统计  
drop table if exists csx_tmp.temp_plan_sale;
create temporary table csx_tmp.temp_plan_sale
as 
select trim(province_code)province_code,
    channel_name,
    city_group_code,city_group_name,
    sum( daily_plan_sales_value)daily_plan_sales_value,
    sum( daily_plan_profit)  daily_plan_profit,
    sum(plan_sales_value)plan_sales_value ,
    sum(plan_profit)plan_profit 
   from 
   (select province_code,city_group_code,case when city_group_name like '攀枝花%' then '攀枝花市' else city_group_name end city_group_name,
        '大' as channel_name,
        0 daily_plan_sales_value,
        0 daily_plan_profit,
        (plan_sales_value)plan_sales_value ,
        (plan_profit)plan_profit 
   from csx_tmp.dws_csms_province_month_sale_plan_tmp
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
    and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
    and (weeknum is null or weeknum='')
    --and province_code='23'
     union all 
    select province_code,city_group_code,case when city_group_name like '攀枝花%' then '攀枝花市' else city_group_name end city_group_name,
        '商超' as channel_name,
        0 daily_plan_sales_value,
        0 daily_plan_profit,
        (plan_sales_value)plan_sales_value ,
        (plan_profit)plan_profit 
    from csx_tmp.dws_ssms_province_month_sale_plan_tmp
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
     and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
    and  (weeknum is null or weeknum='')
    ) d 
    group by 
province_code,
city_group_code,city_group_name,
channel_name
;


-- 本期同期负毛利汇总 
-- INSERT overwrite table csx_tmp.ads_sale_r_d_zone_sales_fr partition(months)
drop table if exists  csx_tmp.temp_plan_sale_01;
create temporary table  csx_tmp.temp_plan_sale_01 as 
SELECT '1' level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       case when a.channel_name='大' then '1' when a.channel_name='商超' then '2' else channel_name end  as channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       a.city_group_code,
       a.city_group_name,
       --0 as daily_plan_sale,
       sum(yesterday_sales_value/10000 )AS daily_sales_value,
        sum(real_yesterday_sales_value/10000) AS real_daily_sales_value,
       --0 as daily_sale_fill_rate,
       sum(last_day_sales/10000 ) as last_week_daily_sales,
       (coalesce(sum(yesterday_sales_value),0)-coalesce(sum(last_day_sales),0))/coalesce(sum(last_day_sales),0) as daily_sale_growth_rate,
       --0 as daily_plan_profit,
       sum(yesterday_profit/10000 )AS daily_profit,
        --0 as daily_profit_fill_rate,
       coalesce(sum(yesterday_profit)/sum(yesterday_sales_value),0) as daily_profit_rate,
       (negative_days_profit/10000) as daily_negative_profit,
       sum(yesterday_often_customer_sale/10000 )AS daily_often_cust_sale,
       sum(yesterday_new_customer_sale/10000 )AS daily_new_cust_sale,
       sum(yesterday_customer_num)AS daily_sale_cust_num,
       -- plan_sales_value as month_plan_sale,
       sum(months_sales_value/10000 )AS month_sale_value,
       sum(real_month_sale_value/10000) as real_month_sale_value,
       sum(ring_months_sale/10000 )AS last_month_sale,
       (coalesce(sum(months_sales_value),0)-coalesce(sum(ring_months_sale),0))/coalesce(sum(ring_months_sale),0) as mom_sale_growth_rate,
      -- plan_profit as month_plan_profit,
       sum(months_profit/10000 )AS month_profit,
      -- sum(months_profit/10000 )/plan_profit as month_proft_fill_rate,
       sum(months_profit)/sum(months_sales_value) as month_profit_rate,
       (negative_profit/10000) as month_negative_profit, 
       sum(months_often_customer_sale/10000 )AS month_often_cust_sale,
       sum(months_new_customer_sale/10000 )AS month_new_cust_sale,
       sum(months_customer_num)AS month_sale_cust_num,
       sum(ring_date_sale/10000 ) AS last_month_daily_sale
FROM
  (SELECT channel_name,
          province_code,
          province_name,
          a.city_group_code,
          city_group_name,
          yesterday_sales_value,
          real_yesterday_sales_value,
          yesterday_profit,
          yesterday_often_customer_sale,
          yesterday_new_customer_sale,
          yesterday_customer_num,
          months_sales_value,
          real_month_sale_value,
          months_profit,
          months_often_customer_sale,
          months_new_customer_sale,
          months_customer_num,
          last_day_sales,
          0 AS ring_date_sale,
          0 AS ring_months_sale
   FROM csx_tmp.temp_war_zone_sale_01 a
   UNION ALL SELECT channel_name,
                    province_code,
                    province_name,
                    a.city_group_code,
                    a.city_group_name,
                    0 AS yesterday_sales_value,
                    0 as real_yesterday_sales_value,
                    0 AS yesterday_profit,
                    0 AS yesterday_often_customer_sale,
                    0 AS yesterday_new_customer_sale,
                    0 AS yesterday_customer_num,
                    0 AS months_sales_value,
                    0 as real_month_sale_value,
                    0 AS months_profit,
                    0 AS months_often_customer_sale,
                    0 AS months_new_customer_sale,
                    0 AS months_customer_num,
                    0 as last_day_sales,
                    last_yesterday_sales_value AS ring_date_sale,
                    last_months_sales_value AS ring_months_sale
   FROM csx_tmp.temp_ring_war_zone_sale a
   ) a  
   left join 
   csx_tmp.temp_war_zone_sale_02 c on a.province_code=c.province_code and a.channel_name=c.channel_name and a.city_group_code= c.city_group_code
   left join 
   (select distinct province_code,province_name,region_code as zone_id,region_name as zone_name, province_manager_id , province_manager_name  from csx_dw.dws_sale_w_a_area_belong ) b on 
    case when a.province_code in ('35','36') then '35' else a.province_code end =b.province_code
GROUP BY a.channel_name,
         a.province_code,
         a.province_name,
         a.city_group_code,
         a.city_group_name,
         zone_id,
         zone_name,
         negative_days_profit,
         negative_profit
    ;
    
    
--插入数据
--INSERT overwrite table csx_tmp.ads_sale_r_d_city_sales_fr partition(sdt)
drop table if exists csx_tmp.temp_sale_00;
create temporary table csx_tmp.temp_sale_00 as 
select 
       case when level_id ='1' then '1'
            when city_group_code is null and channel_code is not null and province_code is not null then '2'
            when  channel_code is  null and province_code is not null  then '3'
            when  province_code is  null then '4'
            end level_id,       
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       coalesce(channel_code,'00')channel_code,
       coalesce(a.channel_name,'全渠道') channel_name,
       coalesce(a.province_code,'00') province_code,
       coalesce(a.province_name, '-') province_name,
       coalesce(city_group_code,'00')city_group_code,
       coalesce(city_group_name,'-')  city_group_name,
       coalesce(daily_plan_sale,0)daily_plan_sale,
       daily_sales_value,
       real_daily_sales_value,
       coalesce(real_daily_sales_value/daily_plan_sale,0)    real_daily_sale_fill_rate,
       coalesce(daily_sales_value/daily_plan_sale,0)    daily_sale_fill_rate,
       last_day_sales,
       coalesce((coalesce((daily_sales_value),0)-coalesce((last_day_sales),0))/coalesce((last_day_sales),0),0) as daily_sale_growth_rate,
       coalesce(daily_plan_profit,0)daily_plan_profit,
       daily_profit,
       coalesce(daily_profit/daily_plan_profit,0) as daily_profit_fill_rate,
       coalesce((daily_profit)/(daily_sales_value),0) as daily_profit_rate,
       daily_negative_profit,
       daily_often_cust_sale,
       daily_new_cust_sale,
       daily_sale_cust_num,
       coalesce(month_plan_sale,0)month_plan_sale,
       months_sales_value,
       (real_month_sale_value) as real_month_sale_value,
       (real_month_sale_value)/(month_plan_sale) as real_month_sale_fill_rate,
       (months_sales_value/month_plan_sale) as month_sale_fill_rate,
       last_month_sale,
       (coalesce((months_sales_value),0)-coalesce((last_month_sale),0))/coalesce((last_month_sale),0) as mom_sale_growth_rate,
       coalesce(month_plan_profit,0)month_plan_profit ,
       months_profit,
       (months_profit /month_plan_profit) as month_proft_fill_rate,
       (months_profit)/(months_sales_value) as month_profit_rate,
       month_negative_profit, 
       month_often_cust_sale,
       month_new_cust_sale,
       month_sale_cust_num,
       last_months_daily_sale,
       current_timestamp(),
      regexp_replace(${hiveconf:edate},'-','') 
from 
(SELECT level_id,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       a.city_group_code,
       a.city_group_name,
       sum(coalesce(daily_plan_sales_value,0)) as daily_plan_sale,
       sum(daily_sales_value )as daily_sales_value,
       sum(real_daily_sales_value) as real_daily_sales_value,
       sum(real_daily_sales_value)/sum(coalesce(daily_plan_sales_value,0)) as real_daily_sale_fill_rate,
       sum(daily_sales_value)/sum(coalesce(daily_plan_sales_value,0)) as daily_sale_fill_rate,
       sum(coalesce(last_week_daily_sales,0) ) as last_day_sales,
       (coalesce(sum(daily_sales_value),0)-coalesce(sum(last_week_daily_sales),0))/coalesce(sum(last_week_daily_sales),0) as daily_sale_rate,
       sum(daily_plan_profit) as daily_plan_profit,
       sum(daily_profit )AS daily_profit,
        sum(daily_profit)/sum(daily_plan_profit) as daily_profit_fill_rate,
       coalesce(sum(daily_profit)/sum(daily_sales_value),0) as daily_profit_rate,
       sum(daily_negative_profit) as daily_negative_profit,
       sum(daily_often_cust_sale )AS daily_often_cust_sale,
       sum(daily_new_cust_sale )AS daily_new_cust_sale,
       sum(daily_sale_cust_num)AS daily_sale_cust_num,
       sum(plan_sales_value)as month_plan_sale,
       sum(month_sale_value)AS months_sales_value,
       sum(real_month_sale_value) as real_month_sale_value,
       sum(real_month_sale_value)/sum(plan_sales_value) as real_month_sale_fill_rate,
       sum(month_sale_value)/sum(plan_sales_value) as month_sale_fill_rate,
       sum(last_month_sale )AS last_month_sale,
       (coalesce(sum(month_sale_value),0)-coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as months_sale_rate,
       sum(plan_profit)as month_plan_profit,
       sum(month_profit )as months_profit,
       sum(month_profit )/sum(plan_profit) as month_proft_fill_rate,
       sum(month_profit)/sum(month_sale_value) as months_profit_rate,
       sum(month_negative_profit) as month_negative_profit, 
       sum(month_often_cust_sale )AS month_often_cust_sale,
       sum(month_new_cust_sale )AS month_new_cust_sale,
       sum(month_sale_cust_num)AS month_sale_cust_num,
       sum(last_month_daily_sale ) AS last_months_daily_sale
FROM
 csx_tmp.temp_plan_sale_01 a  
left join 
   csx_tmp.temp_plan_sale d on a.province_code=d.province_code and trim(a.channel_name)=trim(d.channel_name) and a.city_group_code=d.city_group_code
   --where a.province_code='15'
 group by level_id,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       a.city_group_code,
       a.city_group_name
GROUPING SETS (
    (level_id,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       a.city_group_code,
       a.city_group_name),--一级
       (
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name), -- 省区渠道合计
       (
       zone_id,
       zone_name,
       a.province_code,
       a.province_name,
       a.city_group_code,
       a.city_group_name), -- 城市合计
       (
       zone_id,
       zone_name,
       a.province_code,
       a.province_name),--省区合计
       (
       zone_id,
       zone_name,
       channel_code,
       a.channel_name) , --战区渠道合计
        (
       zone_id,
       zone_name)  --战区合计
       )
) a ;



-- 统计新签约数及签约金额

drop table if exists csx_tmp.temp_sale_01;
create temporary table csx_tmp.temp_sale_01 as 
select 
channel_code,
coalesce( region_code,'00')  as region_code,
coalesce( sales_province_code,'00') as province_code,
coalesce( city_group_code,'00') as city_group_code,
daily_sign_cust_number,
daily_sing_amount,
sign_cust_number,
sing_amount,
group_id
from 
(
select 
'1' as channel_code,
region_code,
sales_province_code,
a.city_group_code,
count(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace(${hiveconf:edate},'-','') then customer_no end ) as daily_sign_cust_number,
sum(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace(${hiveconf:edate},'-','') then estimate_contract_amount end ) as daily_sing_amount,
count(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) then customer_no end ) as sign_cust_number,
sum(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) then estimate_contract_amount end ) as sing_amount,
grouping__id as group_id
from csx_dw.dws_crm_w_a_customer a 
join 
(select region_code,city_group_code,province_code from csx_dw.dws_sale_w_a_area_belong ) b on a.city_group_code=b.city_group_code and a.sales_province_code=b.province_code
where sdt='current' 
-- and sales_province_code='15'
group by sales_province_code,
a.city_group_code,
region_code
grouping sets
((
region_code,
sales_province_code,
a.city_group_code
),
(
region_code,
sales_province_code
),
(
region_code),
())
)a
;


--插入数据
INSERT overwrite table csx_tmp.ads_sale_r_d_city_sales_fr partition(sdt)
select 
       level_id,       
       sales_months,
       zone_id,
       zone_name,
       a.channel_code,
       channel_name,
       a.province_code,
       province_name,
       a.city_group_code,
       city_group_name,
       daily_plan_sale,
       daily_sales_value,
       real_daily_sales_value,
       real_daily_sale_fill_rate,
       daily_sale_fill_rate,
       last_day_sales,
       daily_sale_growth_rate,
       daily_plan_profit,
       daily_profit,
       daily_profit_fill_rate,
       daily_profit_rate,
       daily_negative_profit,
       daily_often_cust_sale,
       daily_new_cust_sale,
       daily_sale_cust_num,
       month_plan_sale,
       months_sales_value,
       real_month_sale_value,
       real_month_sale_fill_rate,
       month_sale_fill_rate,
       last_month_sale,
       mom_sale_growth_rate,
       month_plan_profit ,
       months_profit,
       month_proft_fill_rate,
       month_profit_rate,
       month_negative_profit, 
       month_often_cust_sale,
       month_new_cust_sale,
       month_sale_cust_num,
       last_months_daily_sale,
       daily_sign_cust_number,
       daily_sing_amount,
       sign_cust_number,
       sing_amount,
       current_timestamp(),
      regexp_replace(${hiveconf:edate},'-','') 
from  csx_tmp.temp_sale_00  a 
left outer join
csx_tmp.temp_sale_01 b on a.zone_id=b.region_code and a.province_code=b.province_code and a.city_group_code=b.city_group_code and a.channel_code=b.channel_code


;



drop table csx_tmp.ads_sale_r_d_city_sales_fr;
  CREATE TABLE `csx_tmp.ads_sale_r_d_city_sales_fr`(
  `level_id` string COMMENT '等级：1 城市汇总、2、省区汇总、3、省区全渠道、4、大区全渠道汇总', 
  `sales_months` string COMMENT '销售月份', 
  `zone_id` string COMMENT '战区编码', 
  `zone_name` string COMMENT '战区名称', 
  `channel_code` string COMMENT '渠道编码', 
  `channel_name` string COMMENT '渠道', 
  `province_code` string COMMENT '省区编码', 
  `province_name` string COMMENT '省区名称', 
  `city_group_code` string COMMENT '城市编码', 
  `city_group_name` string COMMENT '城市组名称', 
  `daily_plan_sale` decimal(26,6) COMMENT '昨日计划销售额', 
  `daily_sales_value` decimal(26,6) COMMENT '昨日销售额', 
  `real_daily_sales_value` decimal(26,6) COMMENT '昨日销售额(不含批发内购)', 
  `real_daily_sale_fill_rate` decimal(26,6) COMMENT '日销售达成率不含批发内购', 
  `daily_sale_fill_rate` decimal(26,6) COMMENT '日销售达成率', 
  `last_week_daily_sales` decimal(26,6) COMMENT '上周同日销售额 当前日期-7', 
  `daily_sale_growth_rate` decimal(26,6) COMMENT '昨日环比增长率', 
  `daily_plan_profit` decimal(26,6) COMMENT '昨日销售毛利计划', 
  `daily_profit` decimal(26,6) COMMENT '昨日毛利额', 
  `daily_profit_fill_rate` decimal(26,6) COMMENT '昨日毛利额完成率', 
  `daily_profit_rate` decimal(26,6) COMMENT '昨日毛利率', 
  `daily_negative_profit` decimal(26,6) COMMENT '负毛利金额', 
  `daily_often_cust_sale` decimal(26,6) COMMENT '昨日老客销售额，首次成交未在本月', 
  `daily_new_cust_sale` decimal(26,6) COMMENT '昨日新额销售额，首次成交在本月', 
  `daily_sale_cust_num` bigint COMMENT '昨日成交数', 
  `month_plan_sale` decimal(26,6) COMMENT '月至今销售预算', 
  `month_sale_value` decimal(26,6) COMMENT '月至今销售额', 
  `real_month_sale_value` decimal(26,6) COMMENT '月至今销售额不含批发内购', 
  `real_month_sale_fill_rate` decimal(26,6) COMMENT '月至今销售额不含批发内购', 
  `month_sale_fill_rate` decimal(26,6) COMMENT '月至今销售达成率', 
  `last_month_sale` decimal(26,6) COMMENT '月环比销售额', 
  `mom_sale_growth_rate` decimal(26,6) COMMENT '月环比增长率', 
  `month_plan_profit` decimal(26,6) COMMENT '月度毛利计划', 
  `month_profit` decimal(26,6) COMMENT '月毛利额', 
  `month_profit_fill_rate` decimal(26,6) COMMENT '月度毛利完成率', 
  `month_profit_rate` decimal(26,6) COMMENT '月毛利率', 
  `month_negative_profit` decimal(26,6) COMMENT '负毛利额', 
  `month_often_cust_sale` decimal(26,6) COMMENT '月老客销售额', 
  `month_new_cust_sale` decimal(26,6) COMMENT '新客销售额', 
  `month_sale_cust_num` bigint COMMENT '成交数', 
   `last_months_daily_sale` decimal(26,6) COMMENT '上月同日销售额', 
  `daily_sign_cust_num` bigint comment '昨日新签约数',
  `daily_sign_amount` decimal(26,6) COMMENT '昨日新签约合同金额',
  `sign_cust_num` bigint COMMENT '本月新签约数',
  `sign_amount` decimal(26,6) COMMENT '本月签约金额', 
  `update_time` timestamp COMMENT '更新时间戳')
COMMENT '城市组销售看板'
PARTITIONED BY (  `sdt` string COMMENT '日期分区')
STORED AS parquet
;

count(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace(${hiveconf:edate},'-','') then customer_no end ) as daily_sign_cust_num,
        sum(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace(${hiveconf:edate},'-','') then estimate_contract_amount end ) as daily_sign_amount,
        count(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) then customer_no end ) as sign_cust_num,
        sum(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) then estimate_contract_amount end ) as sign_amount,