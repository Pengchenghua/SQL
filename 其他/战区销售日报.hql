
SET sdate=trunc(${hiveconf:edate},'MM');


SET edate= date_sub(CURRENT_DATE,1);


SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');


SET l_edate=add_months(${hiveconf:edate},-1);

-- select ${hiveconf:l_edate};
 -- 昨日\月销售数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale;


CREATE
TEMPORARY TABLE csx_tmp.temp_war_zone_sale AS
SELECT CASE
           WHEN a.channel IN ('1','7') THEN '大客户'
           ELSE a.channel_name
       END channel_name,
       province_code,
       province_name,
       sum(CASE
               WHEN sdt=regexp_replace(date_sub(${hiveconf:edate},0),'-','') THEN sales_value
           END)AS yesterday_sales_value, 
    sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') then sales_value end ) as last_day_sales,
    sum(CASE
         WHEN sdt=regexp_replace(date_sub(${hiveconf:edate},0),'-','') THEN profit
         END)AS yesterday_profit,
    sum(CASE
         WHEN substr(sdt,1,6)=first_sale_mon
              AND sdt=regexp_replace(date_sub(${hiveconf:edate},0),'-','') THEN sales_value
         END) AS yesterday_new_customer_sale,
    sum(CASE
         WHEN substr(sdt,1,6) !=first_sale_mon
              AND sdt=regexp_replace(date_sub(${hiveconf:edate},0),'-','') THEN sales_value
        END) AS yesterday_often_customer_sale,
    count(DISTINCT CASE
                    WHEN sdt=regexp_replace(date_sub(${hiveconf:edate},0),'-','') THEN a.customer_no
                END) AS yesterday_customer_num, 
                -- 月累计
    sum(sales_value)AS months_sales_value, 
    sum(profit)AS months_profit,
    sum(CASE
         WHEN substr(sdt,1,6)=first_sale_mon THEN sales_value
        END) AS months_new_customer_sale,
    sum(CASE
         WHEN substr(sdt,1,6) !=first_sale_mon THEN sales_value
        END) AS months_often_customer_sale,
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
             WHEN a.channel IN ('1', '7') THEN '大客户'
             ELSE a.channel_name
         END ;

-- 环比数据

DROP TABLE IF EXISTS csx_tmp.temp_ring_war_zone_sale;
CREATE TEMPORARY TABLE csx_tmp.temp_ring_war_zone_sale AS
SELECT CASE
           WHEN a.channel IN ('1', '7') THEN '大客户'
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
             WHEN a.channel IN ('1','7') THEN '大客户'
             ELSE a.channel_name
         END ;



  

SELECT channel_name,
       province_code,
       province_name,
       zone_id,
       zone_name,
       sum(yesterday_sales_value)AS yesterday_sales_value,
       sum(yesterday_profit)AS yesterday_profit,
       coalesce(sum(yesterday_profit)/sum(yesterday_sales_value),0) as yesterday_profit_rate,
       sum(yesterday_often_customer_sale)AS yesterday_often_customer_sale,
       sum(yesterday_new_customer_sale)AS yesterday_new_customer_sale,
       sum(yesterday_customer_num)AS yesterday_customer_num,
       sum(months_sales_value)AS months_sales_value,
       sum(months_profit)AS months_profit,
       sum(months_often_customer_sale)AS months_often_customer_sale,
       sum(months_new_customer_sale)AS months_new_customer_sale,
       sum(months_customer_num)AS months_customer_num,
       sum(ring_date_sale) AS ring_date_sale,
       sum(last_day_sales) as last_day_sales,
       (coalesce(sum(yesterday_sales_value),0)-coalesce(sum(last_day_sales),0))/coalesce(sum(last_day_sales),0) as daily_sale_rate,
       sum(ring_months_sale)AS ring_months_sale,
       (coalesce(sum(months_sales_value),0)-coalesce(sum(ring_months_sale),0))/coalesce(sum(ring_months_sale),0) as months_sale_rate
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
   FROM csx_tmp.temp_war_zone_sale
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
   FROM csx_tmp.temp_ring_war_zone_sale) a
   left join 
   (select DISTINCT dist_code ,zone_id,zone_name from csx_dw.csx_shop where sdt='current') b on case when a.province_code in ('35','36') then '35' else province_code end =b.dist_code
GROUP BY channel_name,
         province_code,
         province_name,
         zone_id,
       zone_name;

 