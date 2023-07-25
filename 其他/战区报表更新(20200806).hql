 -------------------------------------------------------------------分割线------------------------------------------------------------------------------- 
-- SET tez.queue.name= mr; -- 首页销售省区
SET sdate=trunc(${hiveconf:edate},'MM');
SET edate= date_sub(CURRENT_DATE,1);
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
SET l_edate=add_months(${hiveconf:edate},-1); 

-- SELECT  ${hiveconf:l_edate}; -- 昨日\月销售数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale;
CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale AS
SELECT  a.channel_name
       ,a.province_code
       ,a.province_name
       ,yesterday_sales_value
       ,-- last_day_sales yesterday_profit
       ,-- yesterday_negative_profit yesterday_new_customer_sale
       ,yesterday_often_customer_sale
       ,yesterday_customer_num
       ,-- 月累计 months_sales_value
       ,months_profit
       ,-- negative_profit months_new_customer_sale
       ,months_often_customer_sale
       ,months_customer_num
FROM 
(
	SELECT  CASE WHEN a.channel IN ('1','7') THEN '大' ELSE a.channel_name END channel_name
	       ,a.province_code
	       ,a.province_name
	       ,SUM(CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value END)AS yesterday_sales_value
	       ,-- SUM(case WHEN sdt=regexp_replace(date_sub(${hiveconf:edate} 1) '-' '') AND profit <0 THEN profit end ) AS yesterday_negative_profit -- SUM(case WHEN sdt=regexp_replace(date_sub(${hiveconf:edate} 1) '-' '') THEN sales_value end ) AS last_day_sales SUM(CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN profit END)AS yesterday_profit
	       ,SUM(CASE WHEN substr(sdt,1,6)=first_sale_mon AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value END)                AS yesterday_new_customer_sale
	       ,SUM(CASE WHEN substr(sdt,1,6) !=first_sale_mon AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value END)              AS yesterday_often_customer_sale
	       ,COUNT(DISTINCT CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN a.customer_no END)                                      AS yesterday_customer_num
	       ,-- 月累计 SUM(sales_value)AS months_sales_value
	       ,SUM(profit)AS months_profit
	       ,-- SUM(case WHEN profit <0 THEN profit end ) AS negative_profit SUM(CASE WHEN substr(sdt,1,6)=first_sale_mon THEN sales_value END) AS months_new_customer_sale
	       ,SUM(CASE WHEN substr(sdt,1,6) !=first_sale_mon THEN sales_value END)                                                               AS months_often_customer_sale
	       ,COUNT(DISTINCT a.customer_no)                                                                                                      AS months_customer_num
	FROM csx_dw.dws_sale_r_d_customer_sale a
	JOIN 
	(
		SELECT  customer_no
		       ,substr(first_sale_day,1,6) AS first_sale_mon
		FROM csx_dw.ads_sale_w_d_ads_customer_sales_q
		WHERE sdt=regexp_replace(${hiveconf:edate},'-','') 
	) b
	ON a.customer_no=b.customer_no
	WHERE sdt>=regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt<=regexp_replace(${hiveconf:edate},'-','') 
	GROUP BY  province_code
	         ,province_name
	         ,CASE WHEN a.channel IN ('1','7') THEN '大' ELSE a.channel_name END 
)a ; -- 关联上周环比数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_01;
CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_01 AS
SELECT  a.channel_name
       ,a.province_code
       ,a.province_name
       ,yesterday_sales_value
       ,last_day_sales
       ,yesterday_profit
       ,-- yesterday_negative_profit yesterday_new_customer_sale
       ,yesterday_often_customer_sale
       ,yesterday_customer_num
       ,-- 月累计 months_sales_value
       ,months_profit
       ,-- negative_profit months_new_customer_sale
       ,months_often_customer_sale
       ,months_customer_num
FROM csx_tmp.temp_war_zone_sale a
JOIN 
(
	SELECT  CASE WHEN a.channel IN ('1','7') THEN '大' ELSE a.channel_name END channel_name
	       ,province_code
	       ,SUM(a.sales_value)as last_day_sales
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt=regexp_replace(date_sub(${hiveconf:edate},7),'-','') 
	GROUP BY  CASE WHEN a.channel IN ('1','7') THEN '大' ELSE a.channel_name END 
	         ,province_code 
) AS c
ON a.province_code=c.province_code AND a.channel_name=c.channel_name; 

-- SELECT  regexp_replace(date_sub(${hiveconf:edate},7),'-',''); -- show

CREATE TABLE csx_dw.ads_sale_w_d_ads_customer_sales_q; -- 上月环比数据

DROP TABLE IF EXISTS csx_tmp.temp_ring_war_zone_sale;
CREATE TEMPORARY TABLE csx_tmp.temp_ring_war_zone_sale AS
SELECT  CASE WHEN a.channel IN ('1','7') THEN '大' ELSE a.channel_name END channel_name
       ,province_code
       ,province_name
       ,SUM(CASE WHEN sdt=regexp_replace(${hiveconf:l_edate},'-','') THEN sales_value END)AS last_yesterday_sales_value
       ,SUM(sales_value)AS last_months_sales_value
FROM csx_dw.dws_sale_r_d_customer_sale a
WHERE sdt>=regexp_replace(${hiveconf:l_sdate},'-', '') 
AND sdt<=regexp_replace(${hiveconf:l_edate},'-','') 
GROUP BY  province_code
         ,province_name
         ,CASE WHEN a.channel IN ('1','7') THEN '大' ELSE a.channel_name END ; -- 负毛利

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_02;
CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_02 AS
SELECT  '大'                                                                                     AS channel_name
       ,province_code 
       ,province_name 
       ,COUNT(distinct goods_code )as sale_sku
       ,SUM(sale/10000)sale
       ,SUM(profit/10000 )profit
       ,SUM(profit) /SUM(sale)                                                                    AS profit_rate
       ,SUM(case WHEN profit<0 AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN profit end ) AS negative_days_profit
       ,SUM(case WHEN profit<0 THEN profit end )                                                  AS negative_profit
FROM 
(
	SELECT  province_code 
	       ,province_name 
	       ,sdt
	       ,a.customer_no
	       ,goods_code 
	       ,goods_name
	       ,division_code 
	       ,division_name 
	       ,AVG(cost_price )avg_cost
	       ,AVG(sales_price )avg_sale
	       ,SUM(sales_qty )qty
	       ,SUM(sales_value) sale
	       ,SUM(profit) profit
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt>=regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt<=regexp_replace(${hiveconf:edate},'-','') 
	AND channel IN ('1','7') 
	GROUP BY  province_code 
	         ,province_name 
	         ,sdt
	         ,a.customer_no
	         ,goods_code 
	         ,goods_name
	         ,division_code 
	         ,division_name 
) a
GROUP BY  province_code 
         ,province_name ;

SELECT  a.channel_name
       ,a.province_code
       ,a.province_name
       ,zone_id
       ,zone_name
       ,0                                                                                                         AS daily_plan_sale
       ,SUM(yesterday_sales_value/10000 )AS yesterday_sales_value
       ,0                                                                                                         AS daily_sale_fill_rate
       ,SUM(last_day_sales/10000 )                                                                                AS last_day_sales
       ,(coalesce(SUM(yesterday_sales_value),0)-coalesce(SUM(last_day_sales),0))/coalesce(SUM(last_day_sales),0)  AS daily_sale_rate
       ,0                                                                                                         AS daily_plan_profit
       ,SUM(yesterday_profit/10000 )AS yesterday_profit
       ,0                                                                                                         AS daily_profit_fill_rate
       ,coalesce(SUM(yesterday_profit)/SUM(yesterday_sales_value),0)                                              AS yesterday_profit_rate
       ,(negative_days_profit/10000)                                                                              AS yesterday_negative_profit
       ,SUM(yesterday_often_customer_sale/10000 )AS yesterday_often_customer_sale
       ,SUM(yesterday_new_customer_sale/10000 )AS yesterday_new_customer_sale
       ,SUM(yesterday_customer_num)AS yesterday_customer_num
       ,plan_sales_value
       ,SUM(months_sales_value/10000 )AS months_sales_value
       ,SUM(months_sales_value/10000 )/plan_sales_value                                                           AS month_sale_fill_rate
       ,SUM(ring_months_sale/10000 )AS ring_months_sale
       ,(coalesce(SUM(months_sales_value),0)-coalesce(SUM(ring_months_sale),0))/coalesce(SUM(ring_months_sale),0) AS months_sale_rate
       ,plan_profit
       ,SUM(months_profit/10000 )AS months_profit
       ,SUM(months_profit/10000 )/plan_profit                                                                     AS month_proft_fill_rate
       ,SUM(months_profit)/SUM(months_sales_value)                                                                AS months_profit_rate
       ,(negative_profit/10000)                                                                                   AS negative_profit
       ,SUM(months_often_customer_sale/10000 )AS months_often_customer_sale
       ,SUM(months_new_customer_sale/10000 )AS months_new_customer_sale
       ,SUM(months_customer_num)AS months_customer_num
       ,SUM(ring_date_sale/10000 )                                                                                AS ring_date_sale
FROM 
(
	SELECT  channel_name
	       ,province_code
	       ,province_name
	       ,yesterday_sales_value
	       ,yesterday_profit
	       ,yesterday_often_customer_sale
	       ,yesterday_new_customer_sale
	       ,yesterday_customer_num
	       ,months_sales_value
	       ,months_profit
	       ,months_often_customer_sale
	       ,months_new_customer_sale
	       ,months_customer_num
	       ,last_day_sales
	       ,0 AS ring_date_sale
	       ,0 AS ring_months_sale
	FROM csx_tmp.temp_war_zone_sale_01 
	UNION ALL
	SELECT  channel_name
	       ,province_code
	       ,province_name
	       ,0                          AS yesterday_sales_value
	       ,0                          AS yesterday_profit
	       ,0                          AS yesterday_often_customer_sale
	       ,0                          AS yesterday_new_customer_sale
	       ,0                          AS yesterday_customer_num
	       ,0                          AS months_sales_value
	       ,0                          AS months_profit
	       ,0                          AS months_often_customer_sale
	       ,0                          AS months_new_customer_sale
	       ,0                          AS months_customer_num
	       ,0                          AS last_day_sales
	       ,last_yesterday_sales_value AS ring_date_sale
	       ,last_months_sales_value    AS ring_months_sale
	FROM csx_tmp.temp_ring_war_zone_sale 
) a
LEFT JOIN csx_tmp.temp_war_zone_sale_02 c
ON a.province_code=c.province_code AND a.channel_name=c.channel_name
LEFT JOIN 
(
	SELECT  province_code
	       ,channel_name
	       ,SUM(plan_sales_value)plan_sales_value 
	       ,SUM(plan_profit)plan_profit
	FROM csx_tmp.dws_csms_manager_month_sale_plan_tmp
	WHERE month='202008' 
	GROUP BY  province_code
	         ,channel_name
) d
ON d.province_code=a.province_code AND trim(a.channel_name)=trim(d.channel_name)
LEFT JOIN 
(
	SELECT  DISTINCT province_code 
	       ,region_code zone_id
	       ,region_name zone_name
	FROM csx_dw.dim_area
	WHERE area_rank='13' 
) b
ON CASE WHEN a.province_code IN ('35','36') THEN '35' else a.province_code end =b.province_code
GROUP BY  a.channel_name
         ,a.province_code
         ,a.province_name
         ,zone_id
         ,zone_name
         ,negative_days_profit
         ,negative_profit
         ,plan_profit
         ,plan_sales_value ; ------------------------------------------------------------------------------分割线---------------------------------------------------------------------------------- -- 商超查询数据 20200730 -- 商超查询数据 20200730

DROP TABLE csx_tmp.temp_supper_sale;
CREATE temporary TABLE csx_tmp.temp_supper_sale AS
SELECT  a.province_code 
       ,province_name
       ,mach_type
       ,coalesce(a.sales_belong_flag,'其他')        AS sales_belong_flag
       ,SUM(days_sale/10000 )as days_sale
       ,SUM(days_profit/10000)                    AS days_profit
       ,SUM(days_profit)/SUM(days_sale )          AS days_profit_rate
       ,SUM(sale/10000) sale
       ,SUM(ring_sale/10000)                      AS ring_sale
       ,(SUM(sale)-SUM(ring_sale))/SUM(ring_sale) AS ring_sale_ratio
       ,SUM(profit/10000 )profit 
       ,SUM(profit )/SUM(sale )as profit_rate
       ,SUM(ring_profit/10000)                    AS ring_profit
FROM 
(
	SELECT  province_code 
	       ,province_name
	       ,CASE WHEN a.dc_code in('W0M6','W0S8','W0T7') THEN '代加工' ELSE '非代加工' END AS mach_type
	       ,CASE WHEN customer_no IN ('103097','103903','104842') THEN '红旗/中百' 
	             WHEN sales_belong_flag IN ('2_云创会员店','6_云创到家') THEN '2_云创永辉生活' ELSE sales_belong_flag END sales_belong_flag
	       ,SUM(case WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value end )as days_sale
	       ,SUM(case WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN profit end )as days_profit
	       ,SUM(sales_value) sale
	       ,SUM(profit )profit 
	       ,0                                                                       AS ring_sale
	       ,0                                                                       AS ring_profit
	FROM csx_dw.dws_sale_r_d_customer_sale AS a
	LEFT JOIN 
	(
		SELECT  concat('S',shop_id)shop_id
		       ,sales_belong_flag
		FROM csx_dw.dws_basic_w_a_csx_shop_m a
		WHERE sdt = 'current' 
	) b
	ON a.customer_no = shop_id
	WHERE sdt >= regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:edate},'-','') 
	AND channel = '2' 
	AND province_code IN ('32','23','24') 
	GROUP BY  province_code 
	         ,province_name
	         ,CASE WHEN a.dc_code in('W0M6','W0S8','W0T7') THEN '代加工' ELSE '非代加工' END
	         ,CASE WHEN customer_no IN ('103097','103903','104842') THEN '红旗/中百' 
	             WHEN sales_belong_flag IN ('2_云创会员店','6_云创到家') THEN '2_云创永辉生活' ELSE sales_belong_flag END 
	UNION ALL
	SELECT  province_code 
	       ,province_name
	       ,CASE WHEN a.dc_code in('W0M6','W0S8','W0T7') THEN '代加工' ELSE '非代加工' END AS mach_type
	       ,CASE WHEN customer_no IN ('103097','103903','104842') THEN '红旗/中百' 
	             WHEN sales_belong_flag IN ('2_云创会员店','6_云创到家') THEN '2_云创永辉生活' ELSE sales_belong_flag END sales_belong_flag
	       ,0                                                                       AS days_sale
	       ,0                                                                       AS days_profit
	       ,0                                                                       AS sale
	       ,0                                                                       AS profit 
	       ,SUM(sales_value) ring_sale
	       ,SUM(profit ) ring_profit
	FROM csx_dw.dws_sale_r_d_customer_sale AS a
	LEFT JOIN 
	(
		SELECT  concat('S',shop_id)shop_id
		       ,sales_belong_flag
		FROM csx_dw.dws_basic_w_a_csx_shop_m a
		WHERE sdt = 'current' 
	) b
	ON a.customer_no = shop_id
	WHERE sdt >= regexp_replace(${hiveconf:l_sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:l_edate},'-','') 
	AND channel = '2' 
	AND province_code IN ('32','23','24') 
	GROUP BY  province_code 
	         ,province_name
	         ,CASE WHEN a.dc_code in('W0M6','W0S8','W0T7') THEN '代加工' ELSE '非代加工' END 
	         ,CASE WHEN customer_no IN ('103097','103903','104842') THEN '红旗/中百' 
	             WHEN sales_belong_flag IN ('2_云创会员店','6_云创到家') THEN '2_云创永辉生活' ELSE sales_belong_flag END 
) a
GROUP BY  a.province_code 
         ,province_name
         ,mach_type
         ,coalesce(a.sales_belong_flag,'其他'); 

-- SELECT  *
FROM csx_tmp.temp_supper_sale a;

SELECT  a.province_code
       ,province_name
       ,mach_type
       ,sales_belong_flag
       ,days_sale
       ,days_profit
       ,days_profit_rate
       ,plan_sales_value
       ,sale
       ,sale/plan_sales_value AS sale_fill_rate
       ,ring_sale
       ,ring_sale_ratio
       ,plan_sales_value
       ,profit
       ,profit/plan_profit    AS profit_fill_rate
       ,profit_rate
       ,ring_profit
FROM csx_tmp.temp_supper_sale a
LEFT JOIN 
(
	SELECT  province_code
	       ,if(process_type='','非代加工',coalesce(process_type,'非代加工')) AS process_type
	       ,format_code
	       ,format_name
	       ,SUM(plan_sales_value)plan_sales_value
	       ,SUM(plan_profit)plan_profit
	FROM csx_tmp.dws_ssms_province_month_sale_plan_tmp
	WHERE MONTH='202008' 
	GROUP BY  province_code
	         ,format_code
	         ,format_name
	         ,if(process_type='','非代加工',coalesce(process_type,'非代加工')) 
) b
ON a.province_code=b.province_code AND trim(a.sales_belong_flag)=trim(format_name) AND a.mach_type=b.process_type ; ----------------------------------------------------------------------分割线-------------------------------------------------------------------------------------------- -- 属性销售

CREATE temporary TABLE csx_tmp.temp_zone_attribute_01 AS
SELECT  zone_id
       ,zone_name 
       ,a.province_code 
       ,province_name 
       ,attribute_code
       ,attribute
       ,SUM(days_sale/10000 )as days_sale
       ,SUM(days_profit/10000)                                             AS days_profit
       ,SUM(days_profit)/SUM(days_sale)                                    AS days_profit_rate
       ,SUM(sale/10000 )sale
       ,SUM(ring_sale/10000 )                                              AS ring_sale
       ,(SUM(sale)- coalesce(SUM(ring_sale),0))/coalesce(SUM(ring_sale),0) AS mom_sale_rate
       ,SUM(profit/10000)profit
       ,SUM(profit)/SUM(sale)                                              AS profit_rate
       ,SUM(sale_cust )as sale_cust
       ,SUM(sale_cust-ring_sale_cust)                                      AS diff_sale_cust
       ,SUM(ring_profit/10000)                                             AS ring_profit
       ,SUM(ring_sale_cust)                                                AS ring_sale_cust
FROM 
(
	SELECT  province_code 
	       ,CASE WHEN a.channel='7' THEN 'BBC' 
	             WHEN b.attribute_code=3 THEN '贸易' 
	             WHEN a.order_kind='WELFARE' THEN '福利' 
	             WHEN b.attribute_code=5 THEN '合伙人' ELSE '日配' END attribute
	       ,CASE WHEN a.channel='7' THEN '7' 
	             WHEN b.attribute_code=3 THEN '3' 
	             WHEN a.order_kind='WELFARE' THEN '2' 
	             WHEN b.attribute_code=5 THEN '5' ELSE '1' END attribute_code
	       ,SUM(case WHEN sdt= regexp_replace(${hiveconf:edate},'-','') THEN sales_value end )as days_sale
	       ,SUM(case WHEN sdt= regexp_replace(${hiveconf:edate},'-','') THEN profit end) AS days_profit
	       ,SUM(sales_value )sale
	       ,SUM(profit )profit
	       ,COUNT(distinct a.customer_no )as sale_cust
	       ,0                                                                            AS ring_sale
	       ,0                                                                            AS ring_profit
	       ,0                                                                            AS ring_sale_cust
	FROM csx_dw.dws_sale_r_d_customer_sale a
	JOIN 
	(
		SELECT  customer_no 
		       ,attribute 
		       ,attribute_code
		       ,first_category
		       ,first_category_code
		FROM csx_dw.dws_crm_w_a_customer_m_v1
		WHERE sdt = 'current' 
	) AS b
	ON a.customer_no =b.customer_no
	WHERE sdt>=regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt<= regexp_replace(${hiveconf:edate},'-','') 
	AND a.channel in('1','7') 
	GROUP BY  CASE WHEN a.channel='7' THEN 'BBC' 
	             WHEN b.attribute_code=3 THEN '贸易' 
	             WHEN a.order_kind='WELFARE' THEN '福利' 
	             WHEN b.attribute_code=5 THEN '合伙人' ELSE '日配' END 
	         ,CASE WHEN a.channel='7' THEN '7' 
	             WHEN b.attribute_code=3 THEN '3' 
	             WHEN a.order_kind='WELFARE' THEN '2' 
	             WHEN b.attribute_code=5 THEN '5' ELSE '1' END 
	         ,province_code
	         ,province_name 
	UNION ALL
	SELECT  province_code 
	       ,CASE WHEN a.channel='7' THEN 'BBC' 
	             WHEN b.attribute_code=3 THEN '贸易' 
	             WHEN a.order_kind='WELFARE' THEN '福利' 
	             WHEN b.attribute_code=5 THEN '合伙人' ELSE '日配' END attribute
	       ,CASE WHEN a.channel='7' THEN '7' 
	             WHEN b.attribute_code=3 THEN '3' 
	             WHEN a.order_kind='WELFARE' THEN '2' 
	             WHEN b.attribute_code=5 THEN '5' ELSE '1' END attribute_code
	       ,0 AS days_sale
	       ,0 AS days_profit
	       ,0 AS sale
	       ,0 AS profit
	       ,0 AS sale_cust
	       ,SUM(sales_value)as ring_sale
	       ,SUM(profit)as ring_profit
	       ,COUNT(distinct a.customer_no)as ring_sale_cust
	FROM csx_dw.dws_sale_r_d_customer_sale a
	JOIN 
	(
		SELECT  customer_no 
		       ,attribute 
		       ,attribute_code
		       ,first_category
		       ,first_category_code
		FROM csx_dw.dws_crm_w_a_customer_m_v1
		WHERE sdt = 'current' 
	) AS b
	ON a.customer_no =b.customer_no
	WHERE sdt>= regexp_replace(${hiveconf:l_sdate},'-','') 
	AND sdt<= regexp_replace(${hiveconf:l_edate},'-','') 
	AND a.channel in('1','7') 
	GROUP BY  CASE WHEN a.channel='7' THEN 'BBC' 
	             WHEN b.attribute_code=3 THEN '贸易' 
	             WHEN a.order_kind='WELFARE' THEN '福利' 
	             WHEN b.attribute_code=5 THEN '合伙人' ELSE '日配' END 
	         ,CASE WHEN a.channel='7' THEN '7' 
	             WHEN b.attribute_code=3 THEN '3' 
	             WHEN a.order_kind='WELFARE' THEN '2' 
	             WHEN b.attribute_code=5 THEN '5' ELSE '1' END 
	         ,province_code 
) a
JOIN 
(
	SELECT  DISTINCT province_code
	       ,province_name 
	       ,region_code zone_id
	       ,region_name zone_name
	FROM csx_dw.dim_area
	WHERE area_rank='13' 
) b
ON a.province_code=b.province_code
GROUP BY  zone_id
         ,zone_name 
         ,a.province_code 
         ,province_name
         ,attribute
         ,attribute_code ;

SELECT  zone_id
       ,zone_name 
       ,a.province_code 
       ,province_name 
       ,attribute_code
       ,attribute
       ,days_sale
       ,days_profit
       ,days_profit_rate
       ,plan_sales_value
       ,sale
       ,ring_sale
       ,mom_sale_rate
       ,plan_profit
       ,profit
       ,profit_rate
       ,sale_cust
       ,diff_sale_cust
       ,ring_profit
       ,ring_sale_cust
FROM csx_tmp.temp_zone_attribute_01 a
LEFT JOIN 
(
	SELECT  province_code
	       ,channel_name
	       ,customer_attribute_code
	       ,SUM(plan_sales_value)plan_sales_value 
	       ,SUM(plan_profit)plan_profit
	FROM csx_tmp.dws_csms_manager_month_sale_plan_tmp
	WHERE month='202008' 
	AND channel_name='大' 
	GROUP BY  province_code
	         ,channel_name
	         ,customer_attribute_code
) b
ON a.province_code=b.province_code AND a.attribute_code=b.customer_attribute_code ; 
-----------------------------------分割线---------------------------------------------------------------------------------------------------------- 
-- 销售主管数据-- 销售主管数据
CREATE TEMPORARY TABLE csx_tmp.temp_manger_sale AS
select  region_name,
		province_name,
		first_supervisor_name,
		ALL_Md_sales_value,
        ALL_M_sales_value,
        ALL_M_profit,
        ALL_H_sales_value,
        ALL_M_prorate,
        ALL_H_sale_rate,
		old_Md_sales_value,
        old_M_sales_value,
        old_M_profit,
        old_H_sales_value,
        old_M_prorate,
        old_H_sale_rate,
        new_cust_count,
        new_Md_sales_value,
        new_M_sales_value,
        new_M_profit,
        new_H_sales_value,
        new_M_prorate,
		new_H_sale_rate,
		GROUPING__ID
from
    (select region_name,
        coalesce(province_name,'合计') as province_name,
        -- coalesce(city_group_name,'合计') as city_group_name,
        -- coalesce(channel_name_1,'合计') as channel_name_1,
        -- if(sale_group is null,'合计',third_supervisor_name) as third_supervisor_name,
        coalesce(first_supervisor_name,'合计') as first_supervisor_name,
        -- coalesce(sale_group,'合计') sale_group,
        old_Md_sales_value,
        old_M_sales_value,
        old_M_profit,
        old_H_sales_value,
        old_M_profit/old_M_sales_value as old_M_prorate,
        (old_M_sales_value/old_H_sales_value-1) as old_H_sale_rate,
        new_cust_count,
        new_Md_sales_value,
        new_M_sales_value,
        new_M_profit,
        new_H_sales_value,
        new_M_profit/new_M_sales_value as new_M_prorate,
        (new_M_sales_value/new_H_sales_value-1) as new_H_sale_rate,
        ALL_Md_sales_value,
        ALL_M_sales_value,
        ALL_M_profit,
        ALL_H_sales_value,
        ALL_M_profit/ALL_M_sales_value as ALL_M_prorate,
        (ALL_M_sales_value/ALL_H_sales_value-1) as ALL_H_sale_rate,
       -- case when city_group_name='-' and channel_name_1 is null then '是' else '否' end is_delete,
        GROUPING__ID 
    from
        (select region_name,
            province_name,
            -- city_group_name,
            -- channel_name_1,
            -- third_supervisor_name,
            first_supervisor_name,
            -- sale_group,
            coalesce(sum(case when smonth='本月' and is_new_sale='否' then Md_sales_value end)/10000,0) as old_Md_sales_value, --老客-昨日销售额
            coalesce(sum(case when smonth='本月' and is_new_sale='否' then sales_value end)/10000,0) as old_M_sales_value,  --老客-累计销售额
            coalesce(sum(case when smonth='本月' and is_new_sale='否' then profit end)/10000,0) as old_M_profit,  --老客-累计毛利额
            coalesce(sum(case when smonth='环比月' and is_new_sale='否' then sales_value end)/10000,0) as old_H_sales_value,  --老客-环比累计销售额
            coalesce(count(distinct case when smonth='本月' and is_new_sale='是' then customer_no end),0)as new_cust_count,  --新客-累计数
            coalesce(sum(case when smonth='本月' and is_new_sale='是' then Md_sales_value end)/10000,0) as new_Md_sales_value, --新客-昨日销售额
            coalesce(sum(case when smonth='本月' and is_new_sale='是' then sales_value end)/10000,0) as new_M_sales_value,  --新客-累计销售额
            coalesce(sum(case when smonth='本月' and is_new_sale='是' then profit end)/10000,0) as new_M_profit,  --新客-累计毛利额
            coalesce(sum(case when smonth='环比月' and is_new_sale='是' then sales_value end)/10000,0) as new_H_sales_value,  --新客-环比累计销售额
            coalesce(sum(case when smonth='本月' then Md_sales_value end)/10000,0) as ALL_Md_sales_value, --汇总-昨日销售额
            coalesce(sum(case when smonth='本月' then sales_value end)/10000,0) as ALL_M_sales_value,  --汇总-累计销售额
            coalesce(sum(case when smonth='本月' then profit end)/10000,0) as ALL_M_profit,  --汇总-累计毛利额
            coalesce(sum(case when smonth='环比月' then sales_value end)/10000,0) as ALL_H_sales_value , --汇总-环比累计销售额
            GROUPING__ID 
        from (SELECT region_code,
                     region_name,
                     province_code,
                     province_name,
                     city_group_code,
                     city_group_name,
                     channel,
                     channel_name,
                     third_supervisor_name,
                     coalesce(first_supervisor_name,'')as first_supervisor_name,
                     customer_no,
                     customer_name,
                     province_manager_id,
                     province_manager_name,
                     city_group_manager_id,
                     city_group_manager_name,
                     order_kind,
                     sales_belong_flag,
                     is_partner,
                     attribute_0,
                     ascription_type_name,
                     sale_group,
                     is_new_sale,
                     coalesce(Md_sales_value,0)as Md_sales_value, --昨日销售额
                     coalesce(sales_value,0)as sales_value,
                     coalesce(profit,0) as profit,
                     smonth,
                     sdt,
                     case when channel_name='商超' then 'M端'
							when channel_name='大' or channel_name like '企业购%' then 'B端'
							else '其他' end channel_name_1
                FROM csx_tmp.tmp_supervisor_day_detail
                )a
            where channel_name_1 ='B端'
            group by region_name,
                 province_name,
                 city_group_name,
                 -- channel_name_1,
                 -- third_supervisor_name,
                 first_supervisor_name
                 -- sale_group
             grouping sets((region_name),
                      (region_name,province_name),
                      (region_name,province_name,first_supervisor_name))
        )a
    )a  
where 1=1
  -- is_delete='否'
;


SELECT a.region_name,
       a.province_name,
       first_supervisor_name,
       ALL_Md_sales_value,
       plan_sales_value,
       ALL_M_sales_value,
       coalesce(ALL_M_sales_value/plan_sales_value,0) AS all_m_sale_fill_rate,
       ALL_H_sales_value,
       ALL_H_sale_rate,
       plan_profit,
       ALL_M_profit,
       coalesce(ALL_M_profit/plan_profit,0) AS all_m_profit_fill_rate,
       ALL_M_prorate,
       old_Md_sales_value,
       old_M_sales_value,
       old_H_sales_value,
       old_H_sale_rate,
       old_M_profit,
       old_M_prorate,
       new_cust_count,
       new_Md_sales_value,
       new_M_sales_value,
       new_H_sales_value,
       new_H_sale_rate,
       new_M_profit,
       new_M_prorate,
       GROUPING__ID
FROM csx_tmp.temp_manger_sale a
LEFT JOIN
  (SELECT province_code,
          province_name,
          coalesce(manager_name,'合计') manager_name,
          coalesce(plan_sales_value,0)plan_sales_value,
          plan_profit
   FROM
     ( SELECT province_code,
              province_name,
              channel_name,
              manager_name,
              coalesce(sum(plan_sales_value),0)plan_sales_value,
              sum(plan_profit)plan_profit
      FROM csx_tmp.dws_csms_manager_month_sale_plan_tmp
      WHERE MONTH='202008'
        AND channel_name='大'
      GROUP BY province_code,
               channel_name,
               manager_name,
               province_name
      GROUPING
      SETS ((province_code,
             province_name),(province_code,
                             province_name,
                             channel_name,
                             manager_name)) )b ) b ON a.province_name=trim(b.province_name)
AND a.first_supervisor_name=trim(b.manager_name) ;


--------------------------------------------------------------分割线----------------------------------------------------------------------------------------------- -- 省区课组销售 日配单

SELECT  zone_id
       ,zone_name
       ,a.province_code 
       ,a.province_name 
       ,division_code 
       ,division_name
       ,department_code 
       ,department_name
       ,-- SUM(days_sale/10000)as days_sale -- SUM(days_profit/10000) AS days_profit -- SUM(days_profit)/ SUM(days_sale) AS days_profit_rate SUM(sale/10000) sale
       ,SUM(ring_months_sale/10000)                                                                                      AS ring_months_sale
       ,SUM(sale-ring_months_sale)/SUM(ring_months_sale)                                                                 AS ring_sales_ratio
       ,SUM(profit/10000) profit
       ,SUM(profit)/SUM(sale)as profit_rate
       ,SUM(sale_sku)as sale_sku
       ,SUM(sale_cust)as sale_cust
       ,SUM(sale_cust)/SUM(all_sale_cust)                                                                                AS penetration_rate
       ,-- 渗透率 (all_sale_cust)                                                                                           AS all_sale_cust
FROM 
(
	SELECT  province_code 
	       ,province_name 
	       ,division_code 
	       ,division_name
	       ,CASE WHEN department_code like 'U%' THEN 'U01' ELSE department_code END department_code 
	       ,CASE WHEN department_code like 'U%' THEN '加工课' ELSE department_name END department_name
	       ,SUM(case WHEN sdt = ${hiveconf:edate} THEN sales_value end )as days_sale
	       ,SUM(case WHEN sdt = ${hiveconf:edate} THEN profit end) AS days_profit
	       ,SUM(sales_value) sale
	       ,SUM(profit) profit
	       ,COUNT(distinct a.customer_no )as sale_cust
	       ,COUNT(distinct goods_code )as sale_sku
	       ,0                                                      AS ring_months_sale
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:edate},'-','') 
	AND channel ='1' 
	AND a.attribute_code IN ('1','2') 
	AND a.order_kind!='WELFARE' 
	GROUP BY  province_code 
	         ,province_name 
	         ,division_code 
	         ,division_name
	         ,CASE WHEN department_code like 'U%' THEN 'U01' ELSE department_code END 
	         ,CASE WHEN department_code like 'U%' THEN '加工课' ELSE department_name END 
	UNION ALL
	SELECT  province_code 
	       ,province_name 
	       ,division_code 
	       ,division_name
	       ,CASE WHEN department_code like 'U%' THEN 'U01' ELSE department_code END department_code 
	       ,CASE WHEN department_code like 'U%' THEN '加工课' ELSE department_name END department_name
	       ,0 AS days_sale
	       ,0 AS days_profit
	       ,0 sale
	       ,0 profit
	       ,0 sale_cust
	       ,0 sale_sku
	       ,SUM(sales_value)as ring_months_sale
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:l_sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:l_edate},'-','') 
	AND channel ='1' 
	AND a.attribute_code IN ('1','2') 
	AND a.order_kind!='WELFARE' 
	GROUP BY  province_code 
	         ,province_name 
	         ,division_code 
	         ,division_name
	         ,CASE WHEN department_code like 'U%' THEN 'U01' ELSE department_code END 
	         ,CASE WHEN department_code like 'U%' THEN '加工课' ELSE department_name END 
) a
LEFT JOIN 
(
	SELECT  province_code 
	       ,province_name 
	       ,COUNT(distinct a.customer_no )as all_sale_cust
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:edate},'-','') 
	AND channel ='1' 
	AND a.attribute_code IN ('1','2') 
	AND a.order_kind!='WELFARE' 
	GROUP BY  province_code 
	         ,province_name 
) b
ON a.province_code=b.province_code
LEFT JOIN 
(
	SELECT  distinct dist_code
	       ,zone_id
	       ,zone_name
	FROM csx_dw.csx_shop
	WHERE sdt='current'  
) c
ON a.province_code=c.dist_code
GROUP BY  zone_id
         ,zone_name
         ,a.province_code 
         ,a.province_name 
         ,division_code 
         ,division_name
         ,department_code 
         ,department_name
         ,all_sale_cust ; 
         
--------------------------------------------------大区课组销售--------------------------------------------------------------------------------------------------- -- 大区课组销售 
-- SET sdate='20200701'; 
-- SET edate='20200727'; 
-- SET l_sdate='20200601'; 
-- SET l_edate='20200627';

CREATE temporary TABLE csx_tmp.temp_zone_bd_sale AS
SELECT  c.zone_id
       ,c.zone_name
       ,department_code 
       ,department_name
       ,SUM(days_sale/10000)as days_sale
       ,SUM(days_profit/10000)                                                                                           AS days_profit
       ,SUM(days_profit)/ SUM(days_sale)                                                                                 AS days_profit_rate
       ,SUM(sale/10000) sale
       ,SUM(ring_months_sale/10000)                                                                                      AS ring_months_sale
       ,SUM(sale-ring_months_sale)/SUM(ring_months_sale)                                                                 AS ring_sales_ratio
       ,SUM(profit/10000) profit
       ,SUM(profit)/SUM(sale)as profit_rate
       ,SUM(sale_sku)as sale_sku
       ,SUM(sale_cust)as sale_cust -- SUM(sale_cust)/SUM(all_sale_cust) AS penetration_rate -- 渗透率 -- SUM(all_sale_cust) AS all_sale_cust
FROM 
(
	SELECT  province_code
	       ,a.province_name 
	       ,CASE WHEN department_code like 'U%' THEN 'U01' ELSE department_code END department_code 
	       ,CASE WHEN department_code like 'U%' THEN '加工课' ELSE department_name END department_name
	       ,SUM(case WHEN sdt = regexp_replace(${hiveconf:edate},'-','') THEN sales_value end )as days_sale
	       ,SUM(case WHEN sdt = regexp_replace(${hiveconf:edate},'-','') THEN profit end) AS days_profit
	       ,SUM(sales_value) sale
	       ,SUM(profit) profit
	       ,COUNT(distinct a.customer_no )as sale_cust
	       ,COUNT(distinct goods_code )as sale_sku
	       ,0                                                                             AS ring_months_sale
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:edate},'-','') 
	AND channel IN ('1','7') 
	GROUP BY  province_code
	         ,a.province_name 
	         ,CASE WHEN department_code like 'U%' THEN 'U01' ELSE department_code END 
	         ,CASE WHEN department_code like 'U%' THEN '加工课' ELSE department_name END 
	UNION ALL
	SELECT  province_code
	       ,a.province_name 
	       ,CASE WHEN department_code like 'U%' THEN 'U01' ELSE department_code END department_code 
	       ,CASE WHEN department_code like 'U%' THEN '加工课' ELSE department_name END department_name
	       ,0 AS days_sale
	       ,0 AS days_profit
	       ,0 sale
	       ,0 profit
	       ,0 sale_cust
	       ,0 sale_sku
	       ,SUM(sales_value)as ring_months_sale
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:l_sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:l_edate},'-','') 
	AND channel IN ('1','7') 
	GROUP BY  province_code
	         ,a.province_name 
	         ,CASE WHEN department_code like 'U%' THEN 'U01' ELSE department_code END 
	         ,CASE WHEN department_code like 'U%' THEN '加工课' ELSE department_name END 
) a
JOIN 
(
	SELECT  distinct dist_code
	       ,zone_id
	       ,zone_name
	FROM csx_dw.csx_shop
	WHERE sdt='current'  
) c
ON a.province_code=c.dist_code
GROUP BY  c.zone_id
         ,c.zone_name
         ,department_code 
         ,department_name ;

CREATE temporary TABLE csx_tmp.temp_zone_bd_sale_02 AS
SELECT  c.zone_id
       ,c.zone_name
       ,department_code 
       ,department_name
       ,SUM(days_sale/10000)as days_sale
       ,SUM(days_profit/10000)                                                                                           AS days_profit
       ,SUM(days_profit)/ SUM(days_sale)                                                                                 AS days_profit_rate
       ,SUM(sale/10000) sale
       ,SUM(ring_months_sale/10000)                                                                                      AS ring_months_sale
       ,SUM(sale-ring_months_sale)/SUM(ring_months_sale)                                                                 AS ring_sales_ratio
       ,SUM(profit/10000) profit
       ,SUM(profit)/SUM(sale)as profit_rate
       ,SUM(sale_sku)as sale_sku
       ,SUM(sale_cust)as sale_cust -- SUM(sale_cust)/SUM(all_sale_cust) AS penetration_rate -- 渗透率 -- SUM(all_sale_cust) AS all_sale_cust
FROM 
(
	SELECT  province_code
	       ,a.province_name 
	       ,a.division_code                                                               AS department_code 
	       ,a.division_name                                                               AS department_name
	       ,SUM(case WHEN sdt = regexp_replace(${hiveconf:edate},'-','') THEN sales_value end )as days_sale
	       ,SUM(case WHEN sdt = regexp_replace(${hiveconf:edate},'-','') THEN profit end) AS days_profit
	       ,SUM(sales_value) sale
	       ,SUM(profit) profit
	       ,COUNT(distinct a.customer_no )as sale_cust
	       ,COUNT(distinct goods_code )as sale_sku
	       ,0                                                                             AS ring_months_sale
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:edate},'-','') 
	AND channel IN ('1','7') 
	GROUP BY  province_code
	         ,a.province_name 
	         ,a.division_code 
	         ,a.division_name 
	UNION ALL
	SELECT  province_code
	       ,a.province_name 
	       ,a.division_code AS department_code 
	       ,a.division_name AS department_name
	       ,0               AS days_sale
	       ,0               AS days_profit
	       ,0 sale
	       ,0 profit
	       ,0 sale_cust
	       ,0 sale_sku
	       ,SUM(sales_value)as ring_months_sale
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:l_sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:l_edate},'-','') 
	AND channel IN ('1','7') 
	GROUP BY  province_code
	         ,a.province_name 
	         ,a.division_code
	         ,a.division_name 
) a
JOIN 
(
	SELECT  distinct dist_code
	       ,zone_id
	       ,zone_name
	FROM csx_dw.csx_shop
	WHERE sdt='current'  
) c
ON a.province_code=c.dist_code
GROUP BY  c.zone_id
         ,c.zone_name
         ,department_code 
         ,department_name ;

CREATE temporary TABLE csx_tmp.temp_zone_bd_sale_03 AS
SELECT  c.zone_id
       ,c.zone_name
       ,department_code 
       ,department_name
       ,SUM(days_sale/10000)as days_sale
       ,SUM(days_profit/10000)                                                                                           AS days_profit
       ,SUM(days_profit)/ SUM(days_sale)                                                                                 AS days_profit_rate
       ,SUM(sale/10000) sale
       ,SUM(ring_months_sale/10000)                                                                                      AS ring_months_sale
       ,SUM(sale-ring_months_sale)/SUM(ring_months_sale)                                                                 AS ring_sales_ratio
       ,SUM(profit/10000) profit
       ,SUM(profit)/SUM(sale)as profit_rate
       ,SUM(sale_sku)as sale_sku
       ,SUM(sale_cust)as sale_cust -- SUM(sale_cust)/SUM(all_sale_cust) AS penetration_rate -- 渗透率 -- SUM(all_sale_cust) AS all_sale_cust
FROM 
(
	SELECT  province_code
	       ,a.province_name 
	       ,CASE WHEN a.division_code in('11','10') THEN '11' 
	             WHEN a.division_code in('12','13','14') THEN '12' ELSE division_code END    AS department_code 
	       ,CASE WHEN a.division_code in('11','10') THEN '生鲜采购部' 
	             WHEN a.division_code in('12','13','14') THEN '食百采购部' ELSE division_name END AS department_name
	       ,SUM(case WHEN sdt = regexp_replace(${hiveconf:edate},'-','') THEN sales_value end )as days_sale
	       ,SUM(case WHEN sdt = regexp_replace(${hiveconf:edate},'-','') THEN profit end)    AS days_profit
	       ,SUM(sales_value) sale
	       ,SUM(profit) profit
	       ,COUNT(distinct a.customer_no )as sale_cust
	       ,COUNT(distinct goods_code )as sale_sku
	       ,0                                                                                AS ring_months_sale
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:edate},'-','') 
	AND channel IN ('1','7') 
	GROUP BY  province_code
	         ,a.province_name 
	         ,CASE WHEN a.division_code in('11','10') THEN '11' 
	             WHEN a.division_code in('12','13','14') THEN '12' ELSE division_code END 
	         ,CASE WHEN a.division_code in('11','10') THEN '生鲜采购部' 
	             WHEN a.division_code in('12','13','14') THEN '食百采购部' ELSE division_name END 
	UNION ALL
	SELECT  province_code
	       ,a.province_name 
	       ,CASE WHEN a.division_code in('11','10') THEN '11' 
	             WHEN a.division_code in('12','13','14') THEN '12' ELSE division_code END    AS department_code 
	       ,CASE WHEN a.division_code in('11','10') THEN '生鲜采购部' 
	             WHEN a.division_code in('12','13','14') THEN '食百采购部' ELSE division_name END AS department_name
	       ,0                                                                                AS days_sale
	       ,0                                                                                AS days_profit
	       ,0 sale
	       ,0 profit
	       ,0 sale_cust
	       ,0 sale_sku
	       ,SUM(sales_value)as ring_months_sale
	FROM csx_dw.dws_sale_r_d_customer_sale a
	WHERE sdt >= regexp_replace(${hiveconf:l_sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:l_edate},'-','') 
	AND channel IN ('1','7') 
	GROUP BY  province_code
	         ,a.province_name 
	         ,CASE WHEN a.division_code in('11','10') THEN '11' 
	             WHEN a.division_code in('12','13','14') THEN '12' ELSE division_code END 
	         ,CASE WHEN a.division_code in('11','10') THEN '生鲜采购部' 
	             WHEN a.division_code in('12','13','14') THEN '食百采购部' ELSE division_name END 
) a
JOIN 
(
	SELECT  distinct dist_code
	       ,zone_id
	       ,zone_name
	FROM csx_dw.csx_shop
	WHERE sdt='current'  
) c
ON a.province_code=c.dist_code
GROUP BY  c.zone_id
         ,c.zone_name
         ,department_code 
         ,department_name ;

SELECT  a.zone_id
       ,a.zone_name
       ,department_code 
       ,department_name
       ,days_sale
       ,days_profit
       ,days_profit_rate
       ,sale
       ,ring_months_sale
       ,ring_sales_ratio
       ,profit
       ,profit_rate
       ,sale_sku
       ,sale_cust
       ,(sale_cust)/(all_sale_cust) AS penetration_rate
       ,-- 渗透率 row_num
       ,all_sale_cust
FROM 
(
	SELECT  a.zone_id
	       ,a.zone_name
	       ,department_code 
	       ,department_name
	       ,days_sale
	       ,days_profit
	       ,days_profit_rate
	       ,sale
	       ,ring_months_sale
	       ,ring_sales_ratio
	       ,profit
	       ,profit_rate
	       ,sale_sku
	       ,sale_cust
	       ,row_number()over(partition by a.zone_id ORDER BY sale desc) AS row_num
	FROM csx_tmp.temp_zone_bd_sale a 
	UNION ALL
	SELECT  a.zone_id
	       ,a.zone_name
	       ,department_code 
	       ,department_name
	       ,days_sale
	       ,days_profit
	       ,days_profit_rate
	       ,sale
	       ,ring_months_sale
	       ,ring_sales_ratio
	       ,profit
	       ,profit_rate
	       ,sale_sku
	       ,sale_cust
	       ,row_number()over(partition by a.zone_id ORDER BY sale desc) AS row_num
	FROM csx_tmp.temp_zone_bd_sale_02 a 
	UNION ALL
	SELECT  a.zone_id
	       ,a.zone_name
	       ,department_code 
	       ,department_name
	       ,days_sale
	       ,days_profit
	       ,days_profit_rate
	       ,sale
	       ,ring_months_sale
	       ,ring_sales_ratio
	       ,profit
	       ,profit_rate
	       ,sale_sku
	       ,sale_cust
	       ,row_number()over(partition by a.zone_id ORDER BY sale desc) AS row_num
	FROM csx_tmp.temp_zone_bd_sale_03 a 
) a
JOIN 
(
	SELECT  zone_id
	       ,zone_name
	       ,COUNT(distinct a.customer_no )as all_sale_cust
	FROM csx_dw.dws_sale_r_d_customer_sale a
	JOIN 
	(
		SELECT  distinct dist_code
		       ,zone_id
		       ,zone_name
		FROM csx_dw.csx_shop
		WHERE sdt='current'  
	) c
	ON a.province_code=c.dist_code
	WHERE sdt >= regexp_replace(${hiveconf:sdate},'-','') 
	AND sdt <= regexp_replace(${hiveconf:edate},'-','') 
	AND channel IN ('1','7') 
	GROUP BY  zone_id
	         ,zone_name 
) b
ON a.zone_id=b.zone_id ; 
