-- 城市组销售

set hive.exec.dynamic.partition.mode=nonstrict;
-- 当前日期
SET edate= '${enddate}';
-- 月初
SET sdate=trunc(${hiveconf:edate},'MM');
-- 上月初
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
-- 上月当前日期
SET l_edate=add_months(${hiveconf:edate},-1);

-- 昨日\月销售数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    yesterday_sales_value, 
   -- last_day_sales,
    yesterday_profit,
   -- yesterday_negative_profit,
    yesterday_new_customer_sale,
    yesterday_often_customer_sale,
    yesterday_customer_num, 
    -- 月累计
    months_sales_value, 
    months_profit,
   -- negative_profit,
    months_new_customer_sale,
    months_often_customer_sale,
    months_customer_num
FROM 
(
SELECT CASE
           WHEN a.channel IN ('1','7','9') THEN '大客户'
 		   when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       a.province_code,
       a.province_name,
       sum(CASE  WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value  END)AS yesterday_sales_value, 
     --  sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') and profit <0 then profit end ) as yesterday_negative_profit,
    -- sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') then sales_value end ) as last_day_sales,
    sum(CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN profit  END)AS yesterday_profit,
    sum(CASE WHEN substr(sdt,1,6)=first_sale_mon  AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value     END) AS yesterday_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value END) AS yesterday_often_customer_sale,
    count(DISTINCT CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN a.customer_no END) AS yesterday_customer_num, 
    -- 月累计
    sum(sales_value)AS months_sales_value, 
    sum(profit)AS months_profit,
    -- sum(case when profit <0 then profit end ) as negative_profit,
    sum(CASE WHEN substr(sdt,1,6)=first_sale_mon THEN sales_value  END) AS months_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon THEN sales_value END) AS months_often_customer_sale,
    count(DISTINCT a.customer_no) AS months_customer_num
FROM csx_dw.dws_sale_r_d_customer_sale a
JOIN
  (SELECT customer_no,
          substr(first_sale_day,1,6) AS first_sale_mon
   FROM csx_dw.ads_sale_w_d_ads_customer_sales_q
   WHERE sdt=regexp_replace(${hiveconf:edate},'-','')) b ON a.customer_no=b.customer_no
WHERE sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
GROUP BY province_code,
         province_name,
         CASE
           WHEN a.channel IN ('1','7','9') THEN '大客户'
 		   when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END
)a 

;

-- 关联上周环比数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_01;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_01 AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    yesterday_sales_value, 
    last_day_sales,
    yesterday_profit,
   -- yesterday_negative_profit,
    yesterday_new_customer_sale,
    yesterday_often_customer_sale,
    yesterday_customer_num, 
    -- 月累计
    months_sales_value, 
    months_profit,
   -- negative_profit,
    months_new_customer_sale,
    months_often_customer_sale,
    months_customer_num
FROM 
csx_tmp.temp_war_zone_sale  a 
left join 
(select CASE
           WHEN a.channel IN ('1','7','9') THEN '大客户'
 		   when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       province_code,
       sum(a.sales_value)as last_day_sales
    from csx_dw.dws_sale_r_d_customer_sale a
    where sdt=regexp_replace(date_sub(${hiveconf:edate},7),'-','')
    group by CASE
           WHEN a.channel IN ('1','7','9') THEN '大客户'
 		   when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel in ('5','6') and a.customer_no not like 'S%' then '大客户' 
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
           WHEN a.channel IN ('1','7','9') THEN '大客户'
 		   when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       province_code,
       province_name,
       sum(CASE
               WHEN sdt=regexp_replace(${hiveconf:l_edate},'-','') THEN sales_value
           END)AS last_yesterday_sales_value,
       sum(sales_value)AS last_months_sales_value
FROM csx_dw.dws_sale_r_d_customer_sale a
WHERE sdt>=regexp_replace(${hiveconf:l_sdate},'-', '')
  AND sdt<=regexp_replace(${hiveconf:l_edate},'-','')
GROUP BY province_code,
         province_name,
         CASE
           WHEN a.channel IN ('1','7','9') THEN '大客户'
 		   when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END ;


-- 负毛利

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_02;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_02 AS

select
    '大客户' as channel_name,
    province_code ,
    province_name  ,
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
    csx_dw.dws_sale_r_d_customer_sale a 
where
    sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    and channel in ('1','7','9')
group by 
   province_code ,
    province_name ,
    sdt,
    a.customer_no,
    goods_code ,
    goods_name,
    division_code ,division_name 
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
    sum( daily_plan_sales_value)daily_plan_sales_value,
    sum( daily_plan_profit)  daily_plan_profit,
    sum(plan_sales_value)plan_sales_value ,
    sum(plan_profit)plan_profit 
   from 
   (select province_code,'大客户' as channel_name,0 daily_plan_sales_value,0 daily_plan_profit,(plan_sales_value)plan_sales_value ,(plan_profit)plan_profit 
   from csx_tmp.dws_csms_province_month_sale_plan_tmp
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
    and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
     union all 
    select province_code,'商超' as channel_name,0 daily_plan_sales_value,0 daily_plan_profit,(plan_sales_value)plan_sales_value ,(plan_profit)plan_profit 
    from csx_tmp.dws_ssms_province_month_sale_plan_tmp
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
     and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
     union all 
     select province_code,'大客户' as channel_name,coalesce(plan_sale_value,0) daily_plan_sales_value ,coalesce(plan_profit,0)daily_plan_profit,0 plan_sales_value,0 plan_profit 
      from csx_tmp.dws_daily_sales_plan
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
      and plan_sdt=${hiveconf:edate}
     and channel_code='1'
    ) d 
    group by 
province_code,
channel_name
;

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
       -- sum(months_sales_value/10000 )/plan_sales_value as month_sale_fill_rate,
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
          yesterday_sales_value,
          yesterday_profit,
          yesterday_often_customer_sale,
          yesterday_new_customer_sale,
          yesterday_customer_num,
          months_sales_value,
          months_profit,
          months_often_customer_sale,
          months_new_customer_sale,
          months_customer_num,
          last_day_sales,
          0 AS ring_date_sale,
          0 AS ring_months_sale
   FROM csx_tmp.temp_war_zone_sale_01
   UNION ALL SELECT channel_name,
                    province_code,
                    province_name,
                    0 AS yesterday_sales_value,
                    0 AS yesterday_profit,
                    0 AS yesterday_often_customer_sale,
                    0 AS yesterday_new_customer_sale,
                    0 AS yesterday_customer_num,
                    0 AS months_sales_value,
                    0 AS months_profit,
                    0 AS months_often_customer_sale,
                    0 AS months_new_customer_sale,
                    0 AS months_customer_num,
                    0 as last_day_sales,
                    last_yesterday_sales_value AS ring_date_sale,
                    last_months_sales_value AS ring_months_sale
   FROM csx_tmp.temp_ring_war_zone_sale
   ) a  
   left join 
   csx_tmp.temp_war_zone_sale_02 c on a.province_code=c.province_code and a.channel_name=c.channel_name
   left join 
   (select DISTINCT province_code ,region_code zone_id,region_name zone_name 
    from csx_dw.dim_area where area_rank='13') b on 
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

INSERT overwrite table csx_tmp.ads_sale_r_d_zone_sales_fr partition(months)
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
       coalesce(daily_sales_value/d.daily_plan_sales_value,0)    daily_sale_fill_rate,
       last_week_daily_sales,
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
       (month_sale_value/plan_sales_value) as month_sale_fill_rate,
       last_month_sale,
       (coalesce((month_sale_value),0)-coalesce((last_month_sale),0))/coalesce((last_month_sale),0) as mom_sale_growth_rate,
       d.plan_profit ,
       month_profit,
       (month_profit /d.plan_profit) as month_proft_fill_rate,
       (month_profit)/(month_sale_value) as month_profit_rate,
       month_negative_profit, 
       month_often_cust_sale,
       month_new_cust_sale,
       month_sale_cust_num,
       last_month_daily_sale,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM
 csx_tmp.temp_plan_sale_01 a  
   left join 
   csx_tmp.temp_plan_sale d on a.province_code=d.province_code and trim(a.channel_name)=trim(d.channel_name)
   
;


-- 插入 渠道小计

INSERT into table csx_tmp.ads_sale_r_d_zone_sales_fr partition(months)
SELECT '2' level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       channel_code,
       channel_name,
       '00' as province_code,
       '小计' as province_name,
       sum(daily_plan_sale) as daily_plan_sale,
       sum(daily_sales_value )as daily_sales_value,
       sum(daily_sales_value)/sum(daily_plan_sale) as daily_sale_fill_rate,
       sum(last_week_daily_sales ) as last_day_sales,
       (coalesce(sum(daily_sales_value),0)-coalesce(sum(last_week_daily_sales),0))/coalesce(sum(last_week_daily_sales),0) as daily_sale_rate,
       sum(daily_plan_profit) as daily_plan_profit,
       sum(daily_profit )AS daily_profit,
        sum(daily_profit)/sum(daily_plan_profit) as daily_profit_fill_rate,
       coalesce(sum(daily_profit)/sum(daily_sales_value),0) as daily_profit_rate,
       sum(daily_negative_profit) as daily_negative_profit,
       sum(daily_often_cust_sale )AS daily_often_cust_sale,
       sum(daily_new_cust_sale )AS daily_new_cust_sale,
       sum(daily_sale_cust_num)AS daily_sale_cust_num,
       sum(month_plan_sale)as month_plan_sale,
       sum(month_sale_value)AS months_sales_value,
       sum(month_sale_value)/sum(month_plan_sale) as month_sale_fill_rate,
       sum(last_month_sale )AS last_month_sale,
       (coalesce(sum(month_sale_value),0)-coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as months_sale_rate,
       sum(month_plan_profit)as month_plan_profit,
       sum(month_profit )as months_profit,
       sum(month_profit )/sum(month_plan_profit) as month_proft_fill_rate,
       sum(month_profit)/sum(month_sale_value) as months_profit_rate,
       sum(month_negative_profit) as month_negative_profit, 
       sum(month_often_cust_sale )AS month_often_cust_sale,
       sum(month_new_cust_sale )AS month_new_cust_sale,
       sum(month_sale_cust_num)AS month_sale_cust_num,
       sum(last_months_daily_sale ) AS last_months_daily_sale,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM csx_tmp.ads_sale_r_d_zone_sales_fr a
    where level_id ='1'
        and months=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
group by 
       zone_id,
       zone_name,
       channel_code,a.channel_name;
       
-- 插入省区小计
INSERT into table csx_tmp.ads_sale_r_d_zone_sales_fr partition(months)
SELECT '3' level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       '00'channel_code,
       '合计'channel_name,
       '00' as province_code,
       '小计' as province_name,
       sum(daily_plan_sale) as daily_plan_sale,
       sum(daily_sales_value )as daily_sales_value,
       sum(daily_sales_value)/sum(daily_plan_sale) as daily_sale_fill_rate,
       sum(last_week_daily_sales ) as last_day_sales,
       (coalesce(sum(daily_sales_value),0)-coalesce(sum(last_week_daily_sales),0))/coalesce(sum(last_week_daily_sales),0) as daily_sale_rate,
       sum(daily_plan_profit) as daily_plan_profit,
       sum(daily_profit )AS daily_profit,
        sum(daily_profit)/sum(daily_plan_profit) as daily_profit_fill_rate,
       coalesce(sum(daily_profit)/sum(daily_sales_value),0) as daily_profit_rate,
       sum(daily_negative_profit) as daily_negative_profit,
       sum(daily_often_cust_sale )AS daily_often_cust_sale,
       sum(daily_new_cust_sale )AS daily_new_cust_sale,
       sum(daily_sale_cust_num)AS daily_sale_cust_num,
       sum(month_plan_sale)as month_plan_sale,
       sum(month_sale_value)AS months_sales_value,
       sum(month_sale_value)/sum(month_plan_sale) as month_sale_fill_rate,
       sum(last_month_sale )AS last_month_sale,
       (coalesce(sum(month_sale_value),0)-coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as months_sale_rate,
       sum(month_plan_profit)as month_plan_profit,
       sum(month_profit )as months_profit,
       sum(month_profit )/sum(month_plan_profit) as month_proft_fill_rate,
       sum(month_profit)/sum(month_sale_value) as months_profit_rate,
       sum(month_negative_profit) as month_negative_profit, 
       sum(month_often_cust_sale )AS month_often_cust_sale,
       sum(month_new_cust_sale )AS month_new_cust_sale,
       sum(month_sale_cust_num)AS month_sale_cust_num,
       sum(last_months_daily_sale ) AS last_months_daily_sale,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM csx_tmp.ads_sale_r_d_zone_sales_fr a
where level_id ='1'
    and months=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
group by 
        zone_id,
       zone_name;


INSERT into table csx_tmp.ads_sale_r_d_zone_sales_fr partition(months)
SELECT '4' level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       '00'channel_code,
       '合计'channel_name,
        province_code,
       province_name,
       sum(daily_plan_sale) as daily_plan_sale,
       sum(daily_sales_value )as daily_sales_value,
       sum(daily_sales_value)/sum(daily_plan_sale) as daily_sale_fill_rate,
       sum(last_week_daily_sales ) as last_day_sales,
       (coalesce(sum(daily_sales_value),0)-coalesce(sum(last_week_daily_sales),0))/coalesce(sum(last_week_daily_sales),0) as daily_sale_rate,
       sum(daily_plan_profit) as daily_plan_profit,
       sum(daily_profit )AS daily_profit,
        sum(daily_profit)/sum(daily_plan_profit) as daily_profit_fill_rate,
       coalesce(sum(daily_profit)/sum(daily_sales_value),0) as daily_profit_rate,
       sum(daily_negative_profit) as daily_negative_profit,
       sum(daily_often_cust_sale )AS daily_often_cust_sale,
       sum(daily_new_cust_sale )AS daily_new_cust_sale,
       sum(daily_sale_cust_num)AS daily_sale_cust_num,
       sum(month_plan_sale)as month_plan_sale,
       sum(month_sale_value)AS months_sales_value,
       sum(month_sale_value)/sum(month_plan_sale) as month_sale_fill_rate,
       sum(last_month_sale )AS last_month_sale,
       (coalesce(sum(month_sale_value),0)-coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as months_sale_rate,
       sum(month_plan_profit)as month_plan_profit,
       sum(month_profit )as months_profit,
       sum(month_profit )/sum(month_plan_profit) as month_proft_fill_rate,
       sum(month_profit)/sum(month_sale_value) as months_profit_rate,
       sum(month_negative_profit) as month_negative_profit, 
       sum(month_often_cust_sale )AS month_often_cust_sale,
       sum(month_new_cust_sale )AS month_new_cust_sale,
       sum(month_sale_cust_num)AS month_sale_cust_num,
       sum(last_months_daily_sale ) AS last_months_daily_sale,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM csx_tmp.ads_sale_r_d_zone_sales_fr a
where level_id ='1'
    and months=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
group by 
        zone_id,
       zone_name,
       a.province_code,
       a.province_name;