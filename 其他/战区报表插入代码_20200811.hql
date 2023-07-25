-------------------------------------------------------------------分割线-------------------------------------------------------------------------------
-- set tez.queue.name= mr;
-- 首页销售省区
SET sdate=trunc(${hiveconf:edate},'MM');


SET edate= date_sub(CURRENT_DATE,1);


SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');


SET l_edate=add_months(${hiveconf:edate},-1);

-- select ${hiveconf:l_edate};
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
           WHEN a.channel IN ('1','7') THEN '大'
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
             WHEN a.channel IN ('1', '7') THEN '大'
             ELSE a.channel_name
         END
)a 

;

-- 关联上周环比数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_01;


CREATE
TEMPORARY TABLE csx_tmp.temp_war_zone_sale_01 AS

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
join 
(select CASE
           WHEN a.channel IN ('1','7') THEN '大'
           ELSE a.channel_name
       END channel_name,
       province_code,
       sum(a.sales_value)as last_day_sales
    from csx_dw.dws_sale_r_d_customer_sale a
    where sdt=regexp_replace(date_sub(${hiveconf:edate},7),'-','')
    group by CASE
           WHEN a.channel IN ('1','7') THEN '大'
           ELSE a.channel_name
       END ,
       province_code
     ) as c on a.province_code=c.province_code and a.channel_name=c.channel_name;

-- select regexp_replace(date_sub(${hiveconf:edate},7),'-','');
-- show create table csx_dw.ads_sale_w_d_ads_customer_sales_q;
-- 上月环比数据

drop table if exists csx_tmp.temp_ring_war_zone_sale;
create temporary table csx_tmp.temp_ring_war_zone_sale as
select case
           when a.channel in ('1', '7') then '大'
           else a.channel_name
       end channel_name,
       province_code,
       province_name,
       sum(case
               when sdt=regexp_replace(${hiveconf:l_edate},'-','') then sales_value
           end)as last_yesterday_sales_value,
       sum(sales_value)as last_months_sales_value
from csx_dw.dws_sale_r_d_customer_sale a
where sdt>=regexp_replace(${hiveconf:l_sdate},'-', '')
  and sdt<=regexp_replace(${hiveconf:l_edate},'-','')
group by province_code,
         province_name,
         case
             when a.channel in ('1','7') then '大'
             else a.channel_name
         end ;


-- 负毛利

drop table if exists csx_tmp.temp_war_zone_sale_02;


create temporary table csx_tmp.temp_war_zone_sale_02 as

select
    '大' as channel_name,
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
    and channel in ('1','7')
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
 
  

INSERT overwrite table csx_tmp.ads_sale_r_d_zone_sales_fr partition(months)
SELECT '1' level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       case when a.channel_name='大' then '1' when a.channel_name='商超' then '2' else channel_name end  as channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       0 as daily_plan_sale,
       sum(yesterday_sales_value/10000 )AS daily_sales_value,
       0 as daily_sale_fill_rate,
       sum(last_day_sales/10000 ) as last_week_daily_sales,
       (coalesce(sum(yesterday_sales_value),0)-coalesce(sum(last_day_sales),0))/coalesce(sum(last_day_sales),0) as daily_sale_growth_rate,
       0 as daily_plan_profit,
       sum(yesterday_profit/10000 )AS daily_profit,
        0 as daily_profit_fill_rate,
       coalesce(sum(yesterday_profit)/sum(yesterday_sales_value),0) as daily_profit_rate,
       (negative_days_profit/10000) as daily_negative_profit,
       sum(yesterday_often_customer_sale/10000 )AS daily_often_cust_sale,
       sum(yesterday_new_customer_sale/10000 )AS daily_new_cust_sale,
       sum(yesterday_customer_num)AS daily_sale_cust_num,
       plan_sales_value as month_plan_sale,
       sum(months_sales_value/10000 )AS month_sale_value,
       sum(months_sales_value/10000 )/plan_sales_value as month_sale_fill_rate,
       sum(ring_months_sale/10000 )AS last_month_sale,
       (coalesce(sum(months_sales_value),0)-coalesce(sum(ring_months_sale),0))/coalesce(sum(ring_months_sale),0) as mom_sale_growth_rate,
       plan_profit as month_plan_profit,
       sum(months_profit/10000 )AS month_profit,
       sum(months_profit/10000 )/plan_profit as month_proft_fill_rate,
       sum(months_profit)/sum(months_sales_value) as month_profit_rate,
       (negative_profit/10000) as month_negative_profit, 
       sum(months_often_customer_sale/10000 )AS month_often_cust_sale,
       sum(months_new_customer_sale/10000 )AS month_new_cust_sale,
       sum(months_customer_num)AS month_sale_cust_num,
       sum(ring_date_sale/10000 ) AS last_month_daily_sale,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
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
   (select province_code,channel_name,sum(plan_sales_value)plan_sales_value ,sum(plan_profit)plan_profit from csx_tmp.dws_csms_manager_month_sale_plan_tmp
     where month='202008' 
    group by  province_code,channel_name) d on d.province_code=a.province_code and trim(a.channel_name)=trim(d.channel_name)
   left join 
   (select DISTINCT province_code ,region_code zone_id,region_name zone_name from csx_dw.dim_area where area_rank='13') b on case when a.province_code in ('35','36') then '35' else a.province_code end =b.province_code
GROUP BY a.channel_name,
         a.province_code,
         a.province_name,
         zone_id,
         zone_name,
         negative_days_profit,
         negative_profit,
         plan_profit,
         plan_sales_value
    ;
    

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
group by 
       zone_id,
       zone_name,
       channel_code,a.channel_name;
       

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
group by 
        zone_id,
       zone_name,
       a.province_code,
       a.province_name;



------------------------------------------分割线------------------------



-- 属性数据插入
drop table if exists csx_tmp.temp_zone_attribute_01;
create temporary table csx_tmp.temp_zone_attribute_01
as 
select  
       zone_id,zone_name ,
       a.province_code ,
       province_name ,
       attribute_code,
       attribute,       
       sum(days_sale/10000 )as days_sale,
       sum(days_profit/10000) as days_profit,
       sum(days_profit)/sum(days_sale) as days_profit_rate,
       sum(sale/10000 )sale,
       sum(ring_sale/10000 ) as ring_sale,
       (sum(sale)- coalesce(sum(ring_sale),0))/coalesce(sum(ring_sale),0) as mom_sale_rate,
       sum(profit/10000)profit,
       sum(profit)/sum(sale) as profit_rate,
       sum(sale_cust )as sale_cust,
       sum(sale_cust-ring_sale_cust) as diff_sale_cust,
       sum(ring_profit/10000) as ring_profit,
       sum(ring_sale_cust) as ring_sale_cust
from (
   SELECT 
       province_code ,
       case when a.channel='7' then 'BBC'
            when b.attribute_code=3 then '贸易'
            when a.order_kind='WELFARE' then '福利'
            when b.attribute_code=5 then '合伙人'
            else '日配'
            end attribute,
       case when a.channel='7' then '7'
            when b.attribute_code=3 then '3'
            when a.order_kind='WELFARE' then '2'
            when b.attribute_code=5 then '5'
            else '1'
            end attribute_code,
       sum(case when sdt= regexp_replace(${hiveconf:edate},'-','') then sales_value end )as days_sale,
       sum(case when sdt= regexp_replace(${hiveconf:edate},'-','') then profit end) as days_profit,
       sum(sales_value )sale,
       sum(profit )profit,
       count(distinct a.customer_no )as sale_cust,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_cust
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code,
    first_category,
    first_category_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>=regexp_replace(${hiveconf:sdate},'-','') and sdt<= regexp_replace(${hiveconf:edate},'-','') and a.channel in('1','7')
   group by case when a.channel='7' then 'BBC'
            when b.attribute_code=3 then '贸易'
            when a.order_kind='WELFARE' then '福利'
            when b.attribute_code=5 then '合伙人'
            else '日配'   end ,
       case when a.channel='7' then '7'
            when b.attribute_code=3 then '3'
            when a.order_kind='WELFARE' then '2'
            when b.attribute_code=5 then '5'
            else '1' end ,
            province_code,
            province_name
 union all 
   SELECT 
       province_code ,
       case when a.channel='7' then 'BBC'
            when b.attribute_code=3 then '贸易'
            when a.order_kind='WELFARE' then '福利'
            when b.attribute_code=5 then '合伙人'
            else '日配'
            end attribute,
       case when a.channel='7' then '7'
            when b.attribute_code=3 then '3'
            when a.order_kind='WELFARE' then '2'
            when b.attribute_code=5 then '5'
            else '1'
            end attribute_code,
       0 as days_sale,
       0 as days_profit,
       0 as sale,
       0 as profit,
       0 as sale_cust,
       sum(sales_value)as ring_sale,
       sum(profit)as ring_profit,
       count(distinct a.customer_no)as ring_sale_cust       
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code,
    first_category,
    first_category_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>= regexp_replace(${hiveconf:l_sdate},'-','') and sdt<= regexp_replace(${hiveconf:l_edate},'-','') and a.channel in('1','7')
   group by case when a.channel='7' then 'BBC'
            when b.attribute_code=3 then '贸易'
            when a.order_kind='WELFARE' then '福利'
            when b.attribute_code=5 then '合伙人'
            else '日配'
            end ,
       case when a.channel='7' then '7'
            when b.attribute_code=3 then '3'
            when a.order_kind='WELFARE' then '2'
            when b.attribute_code=5 then '5'
            else '1'
            end ,
       province_code
) a 
join 
(select DISTINCT province_code,province_name ,region_code zone_id,region_name zone_name from csx_dw.dim_area where area_rank='13') b on a.province_code=b.province_code 
group by zone_id,zone_name ,
        a.province_code ,
        province_name,
        attribute,
        attribute_code
;

insert overwrite table csx_tmp.ads_sale_r_d_zone_cust_attribute_fr partition(months)
select '1'as level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)as sales_month,
       zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       attribute_code,
       attribute, 
       daily_plan_sale,
       days_sale as daily_sales_value,
       coalesce(days_sale/daily_plan_sale,0) as daily_sale_fill_rate,
       days_profit as daily_profit,
       days_profit_rate as daily_profit_rate,
       plan_sales_value as month_plan_sale,
       sale as month_sale,
       coalesce(sale/plan_sales_value,0) as month_sale_fill_rate,
       ring_sale as last_month_sale,
       mom_sale_rate as mom_sale_growth_rate,
       plan_profit as month_plan_profit,
       profit as month_profit,
      coalesce(profit/plan_profit,0) month_profit_fill_rate,
       profit_rate as month_profit_rate,
       sale_cust as month_sale_cust_num,
       diff_sale_cust as mom_diff_sale_cust,
       ring_profit as last_month_profit,
       ring_sale_cust as last_month_sale_cust_num,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from csx_tmp.temp_zone_attribute_01 a 
left join 
(select province_code,channel_name,customer_attribute_code,0 daily_plan_sale,sum(plan_sales_value)plan_sales_value ,sum(plan_profit)plan_profit from csx_tmp.dws_csms_manager_month_sale_plan_tmp
     where month=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) and channel_name='大'
    group by  province_code,channel_name,customer_attribute_code) b on a.province_code=b.province_code and a.attribute_code=b.customer_attribute_code 
    ;

insert into table csx_tmp.ads_sale_r_d_zone_cust_attribute_fr partition(months)
select '2'as level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)as sales_month,
       zone_id,
       zone_name ,
       '00'province_code ,
       zone_name as province_name ,
       attribute_code,
       attribute, 
       sum(daily_plan_sale) as daily_plan_sale,
       sum(daily_sales_value) as daily_sales_value,
       coalesce(sum(daily_sales_value)/sum(daily_plan_sale),0) as daily_sale_fill_rate,
       sum(daily_profit) as daily_profit,
       coalesce(sum(daily_profit) /sum(daily_sales_value),0) as daily_profit_rate,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale) as month_sale,
       coalesce(sum(month_sale)/sum(month_plan_sale),0) as month_sale_fill_rate,
       sum(last_month_sale) as last_month_sale ,
       coalesce((sum(month_sale)-sum(last_month_sale))/abs(sum(last_month_sale)),0)mom_sale_growth_rate,
       sum(month_plan_profit)month_plan_profit,
       sum(month_profit)month_profit,
      coalesce(sum(month_profit)/sum(month_plan_profit),0) month_profit_fill_rate,
       coalesce(sum(month_profit)/sum(month_sale),0) month_profit_rate,
       sum(month_sale_cust_num)month_sale_cust_num,
       sum(month_sale_cust_num)-sum(last_month_sale_cust_num) as mom_diff_sale_cust,
       sum(last_month_profit)last_month_profit,
       sum(last_month_sale_cust_num)last_month_sale_cust_num,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from csx_tmp.ads_sale_r_d_zone_cust_attribute_fr 
where substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
group by 
       zone_id,
       zone_name ,
       attribute_code,
       attribute;

---------------------------------------分割线----------------------------------------


-- 商超查询数据 20200730
drop table csx_tmp.temp_supper_sale;
create temporary table csx_tmp.temp_supper_sale as
select
    a.province_code ,
    province_name,
    process_type_code,
    process_type,
    coalesce(a.format_type,'其他') as format_type,
    sum(daily_sale_value )as daily_sale_value,
    sum(daily_profit) as daily_profit,
    sum(daily_profit)/sum(daily_sale_value ) as daily_profit_rate,
    sum(month_sale) month_sale,
    sum(last_month_sale)  as last_month_sale,
   (sum(month_sale)-sum(last_month_sale))/sum(last_month_sale) as mom_sale_growth_rate,
    sum(month_profit )month_profit ,
    sum(month_profit )/sum(month_sale )as month_profit_rate,
    sum(last_month_profit)  as last_month_profit
from
(
select
    province_code ,
    province_name,
    case when a.dc_code in('W0M6','W0S8','W0T7') then '2' else '1' end as process_type_code,
    case when a.dc_code in('W0M6','W0S8','W0T7') then '代加工' else '非代加工' end as process_type,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end format_type,
    sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then profit end )as daily_profit,
    sum(sales_value) month_sale,
    sum(profit )month_profit ,
    0 as last_month_sale,
    0 as last_month_profit
from
    csx_dw.dws_sale_r_d_customer_sale as a
left join (
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
where
    sdt >= regexp_replace(${hiveconf:sdate},'-','')
    and sdt <= regexp_replace(${hiveconf:edate},'-','')
    and channel = '2'
 --   and province_code in ('32','23','24')
  group by 
    province_code ,
    province_name,
    case when a.dc_code in('W0M6','W0S8','W0T7') then '2' else '1' end,
    case when a.dc_code in('W0M6','W0S8','W0T7') then '代加工' else '非代加工' end,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end  
union all 
select 
    province_code ,
    province_name,
    case when a.dc_code in('W0M6','W0S8','W0T7') then '2' else '1' end as process_type_code,
    case when a.dc_code in('W0M6','W0S8','W0T7') then '代加工' else '非代加工' end as process_type,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end  format_type,
    0 as daily_sale_value,
    0 as daily_profit,
    0 as month_sale,
    0 as month_profit ,
    sum(sales_value) last_month_sale,
    sum(profit ) last_month_profit 
from
    csx_dw.dws_sale_r_d_customer_sale as a
left join (
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
where
    sdt >= regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <= regexp_replace(${hiveconf:l_edate},'-','')
    and channel = '2'
    and province_code in ('32','23','24')
  group by 
    province_code ,
    province_name,
    case when a.dc_code in('W0M6','W0S8','W0T7') then '2' else '1' end, 
    case when a.dc_code in('W0M6','W0S8','W0T7') then '代加工' else '非代加工' end ,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end 
) a 
group by 
    a.province_code ,
    province_name,
    process_type_code,
    process_type,
    coalesce(a.format_type,'其他') ;
    

INSERT OVERWRITE table csx_tmp.ads_sale_r_d_zone_super_type_fr partition(months)
SELECT '1' as level_id,
        substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
        zone_id,
        zone_name,
       process_type_code,
       a.process_type,
       a.province_code,
       province_name,
       format_code as format_type_code,
       format_name as format_type,
       daily_plan_sale,
       daily_sale_value,
       coalesce(daily_sale_value/daily_plan_sale,0 ) as daily_sale_fill_rate,
       daily_profit,
       daily_profit_rate,
       plan_sales_value as month_plan_sale,
       month_sale,
       month_sale/plan_sales_value as month_sale_fill_rate,
       last_month_sale,
       (month_sale-last_month_sale) /abs(last_month_sale) mom_sale_growth_rate,
       plan_profit as month_plan_profit,
       month_profit,
       month_profit/plan_profit as month_profit_fill_rate,
       month_profit/month_sale as  month_profit_rate,
       last_month_profit,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM csx_tmp.temp_supper_sale a
LEFT JOIN
  (SELECT province_code,
          if(process_type='','非代加工', coalesce(process_type,'非代加工')) as process_type, 
          format_code,
          format_name,
          0 daily_plan_sale,
          sum(plan_sales_value)plan_sales_value,
          sum(plan_profit)plan_profit
   FROM csx_tmp.dws_ssms_province_month_sale_plan_tmp
   WHERE MONTH=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
   GROUP BY province_code,
            format_code,
            format_name,if(process_type='','非代加工', coalesce(process_type,'非代加工'))
    ) b ON a.province_code=b.province_code
AND trim(a.format_type)=trim(format_name) and a.process_type=b.process_type
LEFT JOIN
(select region_code as zone_id,region_name as zone_name,province_code from csx_dw.dim_area where area_rank='13') c on a.province_code=c.province_code;


-- 2 level_id 按照加工类型汇总
INSERT into table csx_tmp.ads_sale_r_d_zone_super_type_fr partition(months)
SELECT '2' as level_id,
        substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
        zone_id,
        zone_name,
       process_type_code,
        process_type,
       province_code,
       province_name,
       a.process_type_code as format_type_code,
       concat(process_type,'_小计') as format_type,
       sum(daily_plan_sale)daily_plan_sale,
       sum(daily_sale_value) daily_sale_value,
       coalesce(sum(daily_sale_value)/sum(daily_plan_sale),0 ) as daily_sale_fill_rate,
       sum(daily_profit)daily_profit,
       coalesce(sum(daily_profit)/sum(daily_sale_value),0) as daily_profit_rate,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale)month_sale,
       coalesce(sum(month_sale)/sum(month_plan_sale),0)  as month_sale_fill_rate,
       sum(last_month_sale)last_month_sale,
       coalesce((sum(month_sale)-sum(last_month_sale)) /abs(sum(last_month_sale)),0) mom_sale_growth_rate,
       sum(month_plan_profit) as month_plan_profit,
       sum(month_profit)month_profit,
       coalesce( sum(month_profit)/sum(month_plan_profit),0) as month_profit_fill_rate,
       coalesce(sum(month_profit)/sum(month_sale),0) as  month_profit_rate,
       sum(last_month_profit)last_month_profit,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM 
csx_tmp.ads_sale_r_d_zone_super_type_fr  a
where level_id='1'
and months=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
GROUP BY
        zone_id,
        zone_name,
       process_type_code,
        process_type,
       province_code,
       province_name,
       concat(process_type,'_小计') ;



-- 3 level_id 按照省区汇总
INSERT into table csx_tmp.ads_sale_r_d_zone_super_type_fr partition(months)
SELECT '3' as level_id,
        substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
        zone_id,
        zone_name,
       province_code as  process_type_code,
       concat(province_name,'_小计') process_type,
       province_code,
       province_name,
       province_code as format_type_code,
        concat(province_name,'_小计') as format_type,
       sum(daily_plan_sale)daily_plan_sale,
       sum(daily_sale_value) daily_sale_value,
       coalesce(sum(daily_sale_value)/sum(daily_plan_sale),0 ) as daily_sale_fill_rate,
       sum(daily_profit)daily_profit,
       coalesce(sum(daily_profit)/sum(daily_sale_value),0) as daily_profit_rate,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale)month_sale,
       coalesce(sum(month_sale)/sum(month_plan_sale),0)  as month_sale_fill_rate,
       sum(last_month_sale)last_month_sale,
       coalesce((sum(month_sale)-sum(last_month_sale)) /abs(sum(last_month_sale)),0) mom_sale_growth_rate,
       sum(month_plan_profit) as month_plan_profit,
       sum(month_profit)month_profit,
       coalesce( sum(month_profit)/sum(month_plan_profit),0) as month_profit_fill_rate,
       coalesce(sum(month_profit)/sum(month_sale),0) as  month_profit_rate,
       sum(last_month_profit)last_month_profit,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM 
csx_tmp.ads_sale_r_d_zone_super_type_fr  a
where level_id='1'
and months=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
GROUP BY
        zone_id,
        zone_name,
        concat(province_name,'_小计') ,
       province_code,
       province_name ;

-- select * from csx_tmp.ads_sale_r_d_zone_super_type_fr;


---------------------------------------------------分割线------------------------------------------------------------------------------

-- 大区部类、课组销售表 csx_tmp.ads_sale_r_d_zone_catg_sales_fr 
drop table if exists csx_tmp.temp_zone_bd_sale;
create temporary table csx_tmp.temp_zone_bd_sale as 
select
    c.zone_id,
    c.zone_name,
    a.division_code,
    a.division_name,
    department_code ,
    department_name,
    sum(daily_sale_value)as daily_sale_value,
    sum(daily_profit) as daily_profit,
    sum(daily_profit)/ sum(daily_sale_value) as daily_profit_rate,
    sum(month_sale) month_sale,
    sum(last_month_sale) as last_month_sale,
    sum(month_sale-last_month_sale)/sum(last_month_sale) as mom_sale_growth_rate,
    sum(month_profit) month_profit,
    sum(month_profit)/sum(month_sale)as month_profit_rate,
    sum(month_sales_sku)as month_sales_sku,
    sum(month_sale_cust_num)as month_sale_cust_num
   -- sum(month_sale_cust_num)/sum(all_sale_cust_num) as penetration_rate,  -- 渗透率
   -- sum(all_sale_cust_num) as all_sale_cust_num
from
(
select
    province_code,
    a.province_name  ,
    a.division_code,
    a.division_name,
    case when department_code like 'U%' then 'U01' else department_code end     department_code ,
    case when department_code like 'U%' then '加工课' else department_name end department_name,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
    sum(sales_value) month_sale,
    sum(profit) month_profit,
    count(distinct a.customer_no )as month_sale_cust_num,
    count(distinct goods_code )as month_sales_sku,
    0 as last_month_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
group by 
    province_code,
    a.province_name ,
    a.division_code,
    a.division_name,
    case when department_code like 'U%' then 'U01' else department_code end  ,  
    case when department_code like 'U%' then '加工课' else department_name end 
union all 
select
   province_code,
   a.province_name , 
   a.division_code,
    a.division_name,
    case when department_code like 'U%' then 'U01' else department_code end     department_code ,
    case when department_code like 'U%' then '加工课' else department_name end  department_name,
    0 as daily_sale_value,
    0 as daily_profit,
    0 month_sale,
    0 month_profit,
    0 month_sale_cust_num,
    0 month_sales_sku,
    sum(sales_value)as last_month_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  channel in ('1','7')
group by 
    province_code,
    a.province_name ,
    a.division_code,
    a.division_name,
    case when department_code like 'U%' then 'U01' else department_code end    ,
    case when department_code like 'U%' then '加工课' else department_name end 
) a 
  join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
group by c.zone_id,
    c.zone_name,
    a.division_code,
    a.division_name,
    department_code ,
    department_name
    
    ;
 
 -- 部类
drop table if exists csx_tmp.temp_zone_bd_sale_02; 
create temporary table csx_tmp.temp_zone_bd_sale_02 as 
select
    c.zone_id,
    c.zone_name,
    division_code,
    division_name,
    department_code ,
    department_name,
    sum(daily_sale_value)as daily_sale_value,
    sum(daily_profit) as daily_profit,
    sum(daily_profit)/ sum(daily_sale_value) as daily_profit_rate,
    sum(month_sale) month_sale,
    sum(last_month_sale) as last_month_sale,
    sum(month_sale-last_month_sale)/sum(last_month_sale) as mom_sale_growth_rate,
    sum(month_profit) month_profit,
    sum(month_profit)/sum(month_sale)as month_profit_rate,
    sum(month_sales_sku)as month_sales_sku,
    sum(month_sale_cust_num)as month_sale_cust_num
   -- sum(month_sale_cust_num)/sum(all_sale_cust_num) as penetration_rate,  -- 渗透率
   -- sum(all_sale_cust_num) as all_sale_cust_num
from
(
select
    province_code,a.province_name  ,
    a.division_code as  division_code,
    a.division_name ,
    '00'  as department_code ,
    '小计'as  department_name,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
    sum(sales_value) month_sale,
    sum(profit) month_profit,
    count(distinct a.customer_no )as month_sale_cust_num,
    count(distinct goods_code )as month_sales_sku,
    0 as last_month_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
group by 
province_code,a.province_name ,
    a.division_code  ,  
    a.division_name 
union all 
select
   province_code,
   a.province_name ,
   a.division_code as  division_code,
   a.division_name ,
    '00'  as department_code ,
    '小计'as  department_name,
    0 as daily_sale_value,
    0 as daily_profit,
    0 month_sale,
    0 month_profit,
    0 month_sale_cust_num,
    0 month_sales_sku,
    sum(sales_value)as last_month_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
where
   sdt >=  regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  channel in ('1','7')
group by 
    province_code,a.province_name ,
     a.division_code,  
a.division_name 
) a 
  join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
     group by c.zone_id,
    c.zone_name,
    division_code,
    division_name,
    department_code ,
    department_name
;
 
drop table if exists csx_tmp.temp_zone_bd_sale_03;
create temporary table csx_tmp.temp_zone_bd_sale_03 as 
select
    c.zone_id,
    c.zone_name,
    a.division_code,
    a.division_name,
    department_code ,
    department_name,
    sum(daily_sale_value)as daily_sale_value,
    sum(daily_profit) as daily_profit,
    sum(daily_profit)/ sum(daily_sale_value) as daily_profit_rate,
    sum(month_sale) month_sale,
    sum(last_month_sale) as last_month_sale,
    sum(month_sale-last_month_sale)/sum(last_month_sale) as mom_sale_growth_rate,
    sum(month_profit) month_profit,
    sum(month_profit)/sum(month_sale)as month_profit_rate,
    sum(month_sales_sku)as month_sales_sku,
    sum(month_sale_cust_num)as month_sale_cust_num
   -- sum(month_sale_cust_num)/sum(all_sale_cust_num) as penetration_rate,  -- 渗透率
   -- sum(all_sale_cust_num) as all_sale_cust_num
from
(
select
    province_code,
    a.province_name  ,
    case when  a.division_code in('11','10') then '11' when a.division_code in('12','13','14') then '12' else  division_code end as division_code,
    case when  a.division_code in('11','10') then '生鲜采购部' when a.division_code in('12','13','14') then '食百采购部' else  division_name end division_name,
    '00' as department_code ,
    '小计'  as  department_name,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
    sum(sales_value) month_sale,
    sum(profit) month_profit,
    count(distinct a.customer_no )as month_sale_cust_num,
    count(distinct goods_code )as month_sales_sku,
    0 as last_month_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
group by 
    province_code,
    a.province_name ,
    case when  a.division_code in('11','10') then '11' when a.division_code in('12','13','14') then '12' else  division_code end ,
    case when  a.division_code in('11','10') then '生鲜采购部' when a.division_code in('12','13','14') then '食百采购部' else  division_name end  
union all 
select
    province_code,
    a.province_name ,
    case when  a.division_code in('11','10') then '11' when a.division_code in('12','13','14') then '12' else  division_code end as division_code,
    case when  a.division_code in('11','10') then '生鲜采购部' when a.division_code in('12','13','14') then '食百采购部' else  division_name end division_name,
    '00' department_code ,
    '小计'  as  department_name,
    0 as daily_sale_value,
    0 as daily_profit,
    0 month_sale,
    0 month_profit,
    0 month_sale_cust_num,
    0 month_sales_sku,
    sum(sales_value)as last_month_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
where
   sdt >=  regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  channel in ('1','7')
group by 
    province_code,
    a.province_name ,
    case when  a.division_code in('11','10') then '11' when a.division_code in('12','13','14') then '12' else  division_code end ,
    case when  a.division_code in('11','10') then '生鲜采购部' when a.division_code in('12','13','14') then '食百采购部' else  division_name end 
) a 
  join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
group by c.zone_id,
    c.zone_name,
    division_code,
    division_name,
    department_code ,
    department_name
    ;


drop table if exists  csx_tmp.temp_zone_bd_sale_04;
create table csx_tmp.temp_zone_bd_sale_04 as 
select level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    a.zone_id,
    a.zone_name,
   '1' channel_code,
    '大'as channel,
    division_code,
    division_name,
    department_code ,
    department_name,
    0 as daily_plan_sale,
    daily_sale_value,
    0 as daily_sale_fill_rate,
    daily_profit,
    daily_profit_rate,
    0 month_plan_sale,
    month_sale,
    0 as month_sale_fill_rate,
    last_month_sale,
    mom_sale_growth_rate,
    0 as month_plan_profit,
    month_profit,
    0 as month_profit_fill_rate,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust_num) as cust_penetration_rate,  -- 渗透率
     all_sale_cust_num,
    row_num,
    current_timestamp(),
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from (
 select
    '1' as level_id,
    a.zone_id,
    a.zone_name,
    division_code,
    division_name,
    department_code ,
    department_name,
    daily_sale_value,
    daily_profit,
    daily_profit_rate,
    month_sale,
    last_month_sale,
    mom_sale_growth_rate,
    month_profit,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    row_number()over(partition by a.zone_id order by month_sale desc) as row_num
from  csx_tmp.temp_zone_bd_sale a 
 union all 
 select
  '2' as level_id,
    a.zone_id,
    a.zone_name,
    division_code,
    division_name,
    department_code ,
    department_name,
    daily_sale_value,
    daily_profit,
    daily_profit_rate,
    month_sale,
     last_month_sale,
    mom_sale_growth_rate,
    month_profit,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
   row_number()over(partition by a.zone_id order by month_sale desc) as row_num
from  csx_tmp.temp_zone_bd_sale_02 a 
union all 
select
    '3' as level_id,
    a.zone_id,
    a.zone_name,
    division_code,
    division_name,
    department_code ,
    department_name,
    daily_sale_value,
    daily_profit,
    daily_profit_rate,
    month_sale,
    last_month_sale,
    mom_sale_growth_rate,
    month_profit,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    row_number()over(partition by a.zone_id order by month_sale desc) as row_num
from  csx_tmp.temp_zone_bd_sale_03 a 
) a 
left  join 
(
select
    zone_id,
    zone_name,
    count(distinct a.customer_no )as all_sale_cust_num
from
    csx_dw.dws_sale_r_d_customer_sale a
join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
    group by 
    zone_id,
    zone_name
 ) b on a.zone_id=b.zone_id
 ;
 
INSERT OVERWRITE table csx_tmp.ads_sale_r_d_zone_catg_sales_fr PARTITION(months)
select * from csx_tmp.temp_zone_bd_sale_04 where months=substr(regexp_replace(${hiveconf:edate},'-',''),1,6);


----------------------------------------商超销售----------------------------------------------------------------

-------------------------------------------------------------------分割线-------------------------------------------------------------------------------
-- 说明 ： a.customer_no IN ('S9961','S99A0','S9996','SW098','S99A7') 归属云超 ；customer_no in ('103097', '103903','104842') 归属 '红旗/中百'
-- set tez.queue.name= mr;
-- 首页销售省区
set hive.exec.dynamic.partition.mode=nonstrict;

-- 当前日期
SET edate= '${enddate}';
-- 月初
SET sdate=trunc(${hiveconf:edate},'MM');
-- 上月初
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
-- 上月当前日期
SET l_edate=add_months(${hiveconf:edate},-1);


-- 商超查询数据 20200730
drop table if exists csx_tmp.temp_supper_sale;
create temporary table csx_tmp.temp_supper_sale
as
select
    a.province_code ,
    process_type_code,
    case when process_type_code='2' then '代加工' else '非代加工' end process_type,
    coalesce(a.format_type,'其他') as format_type,
    sum(daily_sale_value )as daily_sale_value,
    sum(daily_profit) as daily_profit,
    sum(daily_profit)/sum(daily_sale_value ) as daily_profit_rate,
    sum(month_sale) month_sale,
    sum(last_month_sale)  as last_month_sale,
    sum(month_profit )month_profit ,
    sum(month_profit )/sum(month_sale )as month_profit_rate,
    sum(last_month_profit)  as last_month_profit,
    sum(daily_plan_sale) daily_plan_sale,
    sum(month_plan_sale) as month_plan_sale,
    sum(month_plan_profit) as month_plan_profit
from

;
create temporary table csx_tmp.temp_supper_sale
as 
select
    a.province_code ,
    case when shop_name like '%代加工%' then '2' else '1' end  process_type_code,
    case when  shop_name like '%代加工%' then '代加工' else '非代加工' end process_type,
    coalesce(case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活'
        when (a.customer_no IN ('S9961','S99A0','S9996','SW098','S99A7') or sales_belong_flag='1_云超') then '1_云超'
        else sales_belong_flag
    end,'其他') as format_type,
    sum(daily_sale_value )as daily_sale_value,
    sum(daily_profit) as daily_profit,
    sum(daily_profit)/sum(daily_sale_value ) as daily_profit_rate,
    sum(month_sale) month_sale,
    sum(last_month_sale)  as last_month_sale,
    sum(month_profit )month_profit ,
    sum(month_profit )/sum(month_sale )as month_profit_rate,
    sum(last_month_profit)  as last_month_profit,
    sum(daily_plan_sale) daily_plan_sale
from
(
select
    province_code ,
    dc_code as process_type_code,
    customer_no as format_type,
    sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then profit end )as daily_profit,
    sum(sales_value) month_sale,
    sum(profit )month_profit ,
    0 as last_month_sale,
    0 as last_month_profit,
    0 as daily_plan_sale,
    0 as month_plan_sale,
    0 as month_plan_profit
from
    csx_dw.dws_sale_r_d_customer_sale as a
where
    sdt >= regexp_replace(${hiveconf:sdate},'-','')
    and sdt <= regexp_replace(${hiveconf:edate},'-','')
    and channel = '2'
   -- and province_code in ('32','23','24')
  group by 
    province_code ,
    province_name,
    a.dc_code ,
   -- case when a.dc_code in('W0M6','W0S8','W0T7') then '代加工' else '非代加工' end,
    customer_no 
union all 
select 
    province_code ,
     a.dc_code  as process_type_code,
    customer_no as format_type,
    0 as daily_sale_value,
    0 as daily_profit,
    0 as month_sale,
    0 as month_profit ,
    sum(sales_value) last_month_sale,
    sum(profit ) last_month_profit ,
    0 daily_plan_sale,
    0 as month_plan_sale,
    0 as month_plan_profit
from
    csx_dw.dws_sale_r_d_customer_sale as a

where
    sdt >= regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <= regexp_replace(${hiveconf:l_edate},'-','')
    and channel = '2'
   -- and province_code in ('32','23','24')
group by 
    province_code ,
     a.dc_code,
     customer_no 
)a 
left join 
(
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
left join 
(select shop_id,shop_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') c on a.dc_code=c.shop_id
group by   case when shop_name like '%代加工%' then '2' else '1' end  ,
    case when  shop_name like '%代加工%' then '代加工' else '非代加工' end ,
    coalesce(case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活'
        when (a.customer_no IN ('S9961','S99A0','S9996','SW098','S99A7') or sales_belong_flag='1_云超') then '1_云超'
        else sales_belong_flag
    end,'其他') 
;
union all 
SELECT province_code,
          case when process_type='代加工' then '2' else '1' end    as process_type_code, 
          format_name as format_type,
            0 as daily_sale_value,
            0 as daily_profit,
            0 as month_sale,
            0 as month_profit ,
            0 as last_month_sale,
            0 as last_month_profit ,
            0 as daily_plan_sale,
          sum(plan_sales_value)month_plan_sale,
          sum(plan_profit)month_plan_profit
   FROM csx_tmp.dws_ssms_province_month_sale_plan_tmp
   WHERE MONTH=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
   GROUP BY province_code,
            format_name,
            case when process_type='代加工' then '2' else '1' end
    
) a 
group by 
    a.province_code ,
    process_type_code,
    case when process_type_code='2' then '代加工' else '1' end ,
    coalesce(a.format_type,'其他') ;

drop table if exists csx_tmp.temp_super_type_fr   ;
create temporary table csx_tmp.temp_super_type_fr as 
SELECT '1' as level_id,
        substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
        zone_id,
        zone_name,
       process_type_code,
       a.process_type,
       a.province_code,
       province_name,
       case when a.format_type='1_云超' then '1'
            when a.format_type='2_云创永辉生活' then '2'
            when a.format_type='3_云创超级物种' then '3'
            when a.format_type='8_云超MINI' then '8'
            when a.format_type='红旗/中百' then '7'
            when a.format_type='其他' then '-1'
            else format_type 
            end as format_type_code,
       format_type,
       daily_plan_sale,
       daily_sale_value,
       coalesce(daily_sale_value/10000/daily_plan_sale,0 ) as daily_sale_fill_rate,
       daily_profit,
       daily_profit_rate,
       month_plan_sale,
       month_sale,
       coalesce(month_sale/10000/month_plan_sale,0) as month_sale_fill_rate,
       last_month_sale,
       (month_sale-last_month_sale) /abs(last_month_sale) mom_sale_growth_rate,
       month_plan_profit,
       month_profit,
       coalesce(month_profit/10000/month_plan_profit,0) as month_profit_fill_rate,
       coalesce(month_profit/month_sale,0) as  month_profit_rate,
       last_month_profit,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM csx_tmp.temp_supper_sale a
LEFT JOIN
(select region_code as zone_id,region_name as zone_name,province_code,province_name 
    from csx_dw.dim_area where area_rank='13') c on a.province_code=c.province_code ;


INSERT OVERWRITE table csx_tmp.ads_sale_r_d_zone_super_type_fr partition(months)
SELECT *
FROM csx_tmp.temp_super_type_fr  a
 ;


-- 2 level_id 按照加工类型汇总
INSERT into table csx_tmp.ads_sale_r_d_zone_super_type_fr partition(months)
SELECT '2' as level_id,
        substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
        zone_id,
        zone_name,
       process_type_code,
        process_type,
       province_code,
       province_name,
       a.process_type_code as format_type_code,
       concat(process_type,'_小计') as format_type,
       sum(daily_plan_sale)daily_plan_sale,
       sum(daily_sale_value) daily_sale_value,
       coalesce(sum(daily_sale_value)/10000/sum(daily_plan_sale),0 ) as daily_sale_fill_rate,
       sum(daily_profit)daily_profit,
       coalesce(sum(daily_profit)/sum(daily_sale_value),0) as daily_profit_rate,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale)month_sale,
       coalesce(sum(month_sale)/10000/sum(month_plan_sale),0)  as month_sale_fill_rate,
       sum(last_month_sale)last_month_sale,
       coalesce((sum(month_sale)-sum(last_month_sale)) /abs(sum(last_month_sale)),0) mom_sale_growth_rate,
       sum(month_plan_profit) as month_plan_profit,
       sum(month_profit)month_profit,
       coalesce( sum(month_profit)/10000/sum(month_plan_profit),0) as month_profit_fill_rate,
       coalesce(sum(month_profit)/sum(month_sale),0) as  month_profit_rate,
       sum(last_month_profit)last_month_profit,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM 
csx_tmp.temp_super_type_fr  a
where level_id='1'
GROUP BY
        zone_id,
        zone_name,
       process_type_code,
        process_type,
       province_code,
       province_name,
       concat(process_type,'_小计') ;



-- 3 level_id 按照省区汇总
INSERT into table csx_tmp.ads_sale_r_d_zone_super_type_fr partition(months)
SELECT '3' as level_id,
        substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
        zone_id,
        zone_name,
       province_code as  process_type_code,
       concat(province_name,'_小计') process_type,
       province_code,
       province_name,
       province_code as format_type_code,
        concat(province_name,'_小计') as format_type,
       sum(daily_plan_sale)daily_plan_sale,
       sum(daily_sale_value) daily_sale_value,
       coalesce(sum(daily_sale_value)/10000/sum(daily_plan_sale),0 ) as daily_sale_fill_rate,
       sum(daily_profit)daily_profit,
       coalesce(sum(daily_profit)/sum(daily_sale_value),0) as daily_profit_rate,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale)month_sale,
       coalesce(sum(month_sale)/10000/sum(month_plan_sale),0)  as month_sale_fill_rate,
       sum(last_month_sale)last_month_sale,
       coalesce((sum(month_sale)-sum(last_month_sale)) /abs(sum(last_month_sale)),0) mom_sale_growth_rate,
       sum(month_plan_profit) as month_plan_profit,
       sum(month_profit)month_profit,
       coalesce( sum(month_profit)/10000/sum(month_plan_profit),0) as month_profit_fill_rate,
       coalesce(sum(month_profit)/sum(month_sale),0) as  month_profit_rate,
       sum(last_month_profit)last_month_profit,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
FROM 
csx_tmp.temp_super_type_fr  a
where level_id='1'
GROUP BY
        zone_id,
        zone_name,
        concat(province_name,'_小计') ,
       province_code,
       province_name ;

