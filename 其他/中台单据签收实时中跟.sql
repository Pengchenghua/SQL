

-- 中台单据签收实时状况
SELECT
  province_name,
  city_group_name,
  sum(sales_value)sales_value,
  sum(profit)profit,
  sum(profit_rate)profit_rate,
  sum(self_sales_value)self_sales_value,
  sum(self_profit)self_profit,
  sum(self_profit_rate)self_profit_rate,
  sum(joint_sales_value)joint_sales_value,
  sum(joint_profit)joint_profit,
  sum(joint_profit_rate)joint_profit_rate,
  sum(today_sales_value)today_sales_value,
  sum(today_profit)today_profit,
  sum(today_profit_rate)today_profit_rate,
  sum(total_sale_value)total_sale_value,  
  sum(total_profit)total_profit,
  round(sum(total_profit)/sum(total_sale_value),4) total_profit_rate,
  sum(no_receive_amt)no_receive_amt,
  sum(able_sgin_value)able_sgin_value,
  sum(bbc_sales_value)bbc_sales_value,
  sum(bbc_profit)bbc_profit,
 round( sum(bbc_profit_rate),4)bbc_profit_rate,
  sum(today_bbc_sales_value)today_bbc_sales_value,
  sum(today_bbc_profit)today_bbc_profit,
  sum(today_bbc_profit_rate)today_bbc_profit_rate,
  sum(total_bbc_sale_value)total_bbc_sale_value,
  sum(total_bbc_profit) as total_bbc_profit,
  round(sum(total_bbc_profit_rate),4) as total_bbc_profit_rate,   --BBC毛利率
  sum(total_receive_performance_all) as total_receive_performance_all,   -- 已签收销售额
  sum(total_self_sales_value) as total_self_sales_value ,				-- 已签收自营销售额
  sum(total_self_profit) as total_self_profit ,							--已签收自营毛利额
  sum(total_self_profit)/ sum(total_self_sales_value) as total_self_profit_rate,
  sum(total_joint_sales_value)as total_joint_sales_value,				--已签收非自营销售额
  sum(total_joint_profit)as total_joint_profit,							--已签收非自营毛利额
  sum(total_joint_profit)/sum(total_joint_sales_value) as total_join_profit_rate,
  sum(no_receive_performance_all) as no_receive_performance_all,         -- 未签收销售额
  sum(no_receive_profit) as no_receive_profit,                       --未签收毛利额
  round(sum(no_receive_profit)/ sum(no_receive_performance_all),4) as no_receive_profit_rate,   --未签收毛利率
  sum(total_performance_all) as total_performance_all,                   -- 总销售额
  sum(total_performance_all_profit) as total_performance_all_profit,  --总毛利额
  sum(total_receive_performance_profit) as total_receive_performance_profit,
  round(sum(total_receive_performance_profit)/sum(total_receive_performance_all),4)as total_receive_performance_profit_rate ,  -- 已签收毛利率
  round(sum(send_amt),4) send_amt,
  round(sum(send_profit),4) send_profit,
  COALESCE (sum(send_profit)/sum(send_amt),0) as send_profit_rate,
  round(sum(purchase_amt),4) purchase_amt,
  update_time
FROM
( 
SELECT
  coalesce(t1.province_name, t2.province_name, t3.province_name, '') as province_name,
  coalesce(t1.city_group_name, t2.city_group_name, t3.city_group_name, '') as city_group_name,
  round(nvl(t1.sales_value/10000, 0),4) as sales_value,
  round(nvl(t1.profit,0)/10000,4) as profit,
  nvl(t1.profit_rate, 0) as profit_rate,
  round(nvl(t2.self_sales_value, 0)/10000,4) as self_sales_value,
  round(nvl(t2.self_profit,0)/10000,4) as self_profit,
  nvl(t2.self_profit_rate, 0) as self_profit_rate,
  round(nvl(t2.joint_sales_value, 0)/10000,4) as joint_sales_value,
  round(nvl(t2.joint_profit/10000, 0),4) as joint_profit,
  nvl(t2.joint_profit_rate, 0) as joint_profit_rate,
  round(nvl(t2.sales_value, 0)/10000,4) as today_sales_value,
  round(nvl(t2.profit/10000, 0),4) as today_profit,
  nvl(t2.profit_rate, 0) as today_profit_rate,
  round((nvl(t1.sales_value, 0) + nvl(t2.sales_value, 0))/10000,4) as total_sale_value,
  round(nvl((t1.profit+t2.profit)/10000, 0),4) as total_profit,
  round(nvl(t3.no_receive_amt, 0)/10000,2) no_receive_amt,
  round((nvl(t1.sales_value, 0) + nvl(t2.sales_value, 0) + nvl(t3.no_receive_amt, 0))/10000,2) able_sgin_value,
  round(nvl(t1.bbc_sales_value, 0)/10000,2) as bbc_sales_value,
  round(nvl(t1.bbc_profit,0)/10000,2) as bbc_profit,
  nvl(t1.bbc_profit_rate, 0) as bbc_profit_rate,
  round(nvl(t2.bbc_sales_value, 0)/10000,4) as today_bbc_sales_value,
  round(nvl(t2.bbc_profit,0)/10000,4) as today_bbc_profit,
  nvl(t2.bbc_profit_rate, 0) as today_bbc_profit_rate,
  round((nvl(t1.bbc_profit,0)+nvl(t2.bbc_profit,0))/10000,4) as total_bbc_profit,  --BBC总毛利额
  round((nvl(t1.bbc_profit,0)+nvl(t2.bbc_profit,0))/(nvl(t1.bbc_sales_value, 0) + nvl(t2.bbc_sales_value, 0)),4) as total_bbc_profit_rate,                   --BBC总毛利率
  round((nvl(t1.bbc_sales_value, 0) + nvl(t2.bbc_sales_value, 0))/10000,4) total_bbc_sale_value,   --BBC总销售额
  round((nvl(t1.sales_value, 0) + nvl(t2.sales_value, 0) + nvl(t1.bbc_sales_value, 0) + nvl(t2.bbc_sales_value, 0))/10000,4) as total_receive_performance_all,      -- 已签收销售额
  round((nvl(t1.bbc_profit, 0) + nvl(t2.bbc_profit, 0) + nvl(t1.profit, 0) + nvl(t2.profit, 0) )/10000,4) total_receive_performance_profit,         --已签收自营毛利额
  
  -- 已签收自营销售额
  round((nvl(t1.self_sales_value, 0) + nvl(t2.self_sales_value, 0) )/10000,4) as total_self_sales_value,      -- 已签收自营销售额
  round((nvl(t1.self_profit, 0) + nvl(t2.self_profit, 0) )/10000,4) total_self_profit,         --已签收自营毛利额
  --已签收非自营销售
  round((nvl(t1.joint_sales_value, 0) + nvl(t2.joint_sales_value, 0) )/10000,4) as total_joint_sales_value,      -- 已签收非自营销售额
  round((nvl(t1.joint_profit, 0) + nvl(t2.joint_profit, 0) )/10000,4) total_joint_profit,         --已签收非自营毛利额  
  round(nvl(t3.no_receive_amt, 0)/10000,4) no_receive_performance_all,      --未签收销售额
  round(nvl(t3.no_receive_profit, 0)/10000,4) no_receive_profit,            --未签收毛利额
  round((nvl(t1.sales_value, 0) + nvl(t2.sales_value, 0) + nvl(t1.bbc_sales_value, 0) + nvl(t2.bbc_sales_value, 0) + nvl(t3.no_receive_amt, 0))/10000,4) total_performance_all, --总销售
 -- round((nvl(t1.bbc_profit, 0) + nvl(t2.bbc_profit, 0) + nvl(t1.profit, 0) + nvl(t2.profit, 0) + nvl(t3.no_receive_profit, 0))/10000,2) total_performance_all_profit,  --总毛利额
  round((nvl(t1.bbc_profit, 0) + nvl(t2.bbc_profit, 0) + nvl(t1.profit, 0) + nvl(t2.profit, 0))/10000 ,4) total_performance_all_profit,  --总毛利额
  round(send_amt/10000,4) send_amt,
  round(send_profit/10000,4) send_profit,
  round(purchase_amt/10000,4) purchase_amt,
  
  t4.update_time
FROM
( -- 截止到昨天业绩城市粒度汇总
  SELECT
    province_name, city_group_name, -- B端大业绩
    sum(if(channel_code IN ('1', '9'), sales_value, 0)) AS sales_value,
    sum(if(channel_code IN ('1', '9'), profit, 0)) AS profit,
	--自营销售额
	sum(if(channel_code IN ('1', '9') AND business_type_code <> '4' AND dc_code <> 'W0K4', sales_value, 0)) AS self_sales_value,
    sum(if(channel_code IN ('1', '9') AND business_type_code <> '4' AND dc_code <> 'W0K4', profit, 0)) AS self_profit,
	-- B端大非自营业绩
    sum(if(channel_code IN ('1', '9') AND (business_type_code = '4' OR dc_code = 'W0K4'), sales_value, 0)) AS joint_sales_value,
    sum(if(channel_code IN ('1', '9') AND (business_type_code = '4' OR dc_code = 'W0K4'), profit, 0)) AS joint_profit,
    coalesce(round(sum(if(channel_code IN ('1', '9'), profit, 0)) / abs(sum(if(channel_code IN ('1', '9'), sales_value, 0))), 6), 0) AS profit_rate, -- BBC业绩
    sum(if(channel_code = '7', sales_value, 0)) AS bbc_sales_value,
    sum(if(channel_code = '7', profit, 0)) AS bbc_profit,
    coalesce(round(sum(if(channel_code = '7', profit, 0)) / abs(sum(if(channel_code = '7', sales_value, 0))), 6), 0) AS bbc_profit_rate
  FROM   csx_dw.dws_sale_r_d_detail
  WHERE sdt >= substr(regexp_replace(cast(trunc(now(), 'MM') AS string), '-', ''), 1, 8) AND sales_type <> 'sc' AND channel_code IN ('1', '7', '9')
   GROUP BY province_name, city_group_name
) t1 FULL JOIN
( -- 今日业绩城市粒度汇总
  SELECT
    province_name, city_group_name,
    -- B端大自营业绩
    sum(if(channel_code IN ('1', '9') AND business_type_code <> '4' AND dc_code <> 'W0K4', sales_value, 0)) AS self_sales_value,
    sum(if(channel_code IN ('1', '9') AND business_type_code <> '4' AND dc_code <> 'W0K4', profit, 0)) AS self_profit,
    coalesce(round(sum(if(channel_code IN ('1', '9') AND business_type_code <> '4' AND dc_code <> 'W0K4', profit, 0))
      / abs(sum(if(channel_code IN ('1', '9') AND business_type_code <> '4' AND dc_code <> 'W0K4', sales_value, 0))), 6), 0) AS self_profit_rate,
    -- B端大非自营业绩
    sum(if(channel_code IN ('1', '9') AND (business_type_code = '4' OR dc_code = 'W0K4'), sales_value, 0)) AS joint_sales_value,
    sum(if(channel_code IN ('1', '9') AND (business_type_code = '4' OR dc_code = 'W0K4'), profit, 0)) AS joint_profit,
    coalesce(round(sum(if(channel_code IN ('1', '9') AND (business_type_code = '4' OR dc_code = 'W0K4'), profit, 0))
      / abs(sum(if(channel_code IN ('1', '9') AND (business_type_code = '4' OR dc_code = 'W0K4'), sales_value, 0))), 6), 0) AS joint_profit_rate,
    -- B端大总业绩
    sum(if(channel_code IN ('1', '9'), sales_value, 0)) AS sales_value,
    sum(if(channel_code IN ('1', '9'), profit, 0)) AS profit,
    coalesce(round(sum(if(channel_code IN ('1', '9'), profit, 0)) / abs(sum(if(channel_code IN ('1', '9'), sales_value, 0))), 6), 0) AS profit_rate,
    -- BBC业绩
    sum(if(channel_code = '7', sales_value, 0)) AS bbc_sales_value,
    sum(if(channel_code = '7', profit, 0)) AS bbc_profit,
    coalesce(round(sum(if(channel_code = '7', profit, 0)) / abs(sum(if(channel_code = '7', sales_value, 0))), 6), 0) AS bbc_profit_rate
  FROM csx_dw.report_sale_r_h_today
  WHERE sales_type <> 'sc' AND channel_code IN ('1', '7', '9')
  GROUP BY province_name, city_group_name
) t2 ON t1.province_name = t2.province_name AND t1.city_group_name = t2.city_group_name
FULL JOIN
( -- 待发货+配送中+待确认数据统计 --CUTTED待出库 STOCKOUT配送中 
  SELECT
    e.province_name, f.city_group_name,
    SUM(coalesce(IF(order_status IN ('CUTTED', 'STOCKOUT'), send_amt, purchase_amt), 0)) AS no_receive_amt,   --CUTTED待出库 STOCKOUT配送中 
    sum(coalesce(case when order_status IN ('CUTTED', 'STOCKOUT') then  send_amt end,0 )) send_amt,                   --已发货未签收金额
    sum(coalesce(case when order_status IN ('CUTTED', 'STOCKOUT') then  send_cost end,0 )) send_cost,                   --已发货未签收成本金额
    sum(coalesce(case when order_status IN ('CUTTED', 'STOCKOUT') then  send_amt end,0 ))- sum(coalesce(case when order_status IN ('CUTTED', 'STOCKOUT') then  send_cost end,0 )) send_profit, -- 已发货未签收毛利金额
    
    sum(coalesce(case when order_status not IN ('CUTTED', 'STOCKOUT')  then  purchase_amt end,0 )) purchase_amt,       --未发货未订单金额
    sum(coalesce(IF(order_status IN ('CUTTED', 'STOCKOUT'), send_cost, purchase_cost), 0)) as no_receive_cost,
    SUM(coalesce(IF(order_status IN ('CUTTED', 'STOCKOUT'), send_amt, purchase_amt), 0))-sum(coalesce(IF(order_status IN ('CUTTED', 'STOCKOUT'), send_cost, purchase_cost), 0)) as no_receive_profit
  FROM
  (
    SELECT
      order_no, order_status, sap_cus_code
    FROM csx_dw.dwd_csms_r_h_yszx_order
    WHERE order_status IN ('PAID', 'CUTTED', 'STOCKOUT') AND cast(require_delivery_date as string) <= substr(regexp_replace(cast(now() as string), '-', ''), 1, 8)
  ) a LEFT JOIN
  (
    SELECT
      order_no,
      SUM(purchase_qty*promotion_price) AS purchase_amt, -- 下单金额
      sum(origin_cost_price*purchase_qty ) as purchase_cost,    --  下单成本
      SUM(send_qty*promotion_price) AS send_amt, -- 发货金额
      sum(origin_cost_price*send_qty ) as send_cost    --  发货成本
      
      
    FROM csx_dw.dwd_csms_r_h_yszx_order_item
    WHERE item_status <> 0
    GROUP BY order_no
  ) c ON a.order_no = c.order_no
  JOIN
  (
    SELECT
      customer_number, sales_province, sales_city
    FROM csx_ods.source_crm_w_a_customer
    WHERE sdt = substr(regexp_replace(cast(date_sub(now(), 1) AS string), '-', ''), 1, 8) AND channel_code IN ('1', '9')
  ) d ON a.sap_cus_code = d.customer_number
  LEFT JOIN
  (-- 获取省区信息
    SELECT DISTINCT
      province_code, province_name
    FROM csx_dw.dws_sale_w_a_area_belong
  ) e ON d.sales_province = e.province_code
  LEFT JOIN
  (-- 获取城市组信息
    SELECT
      province_code, city_code, city_group_code, city_group_name
    FROM csx_dw.dws_sale_w_a_area_belong
  ) f ON d.sales_city = f.city_code AND d.sales_province = f.province_code
  GROUP BY e.province_name, f.city_group_name
) t3 ON t1.province_name = t3.province_name AND t1.city_group_name = t3.city_group_name
JOIN
(
  SELECT max(update_time) AS update_time
  FROM csx_dw.report_sale_r_h_today
  WHERE sales_type <> 'sc' AND channel_code IN ('1', '7', '9')
) t4 ON 1 = 1
WHERE coalesce(t1.province_name, t2.province_name, t3.province_name, '') = '${prov}'
) a 
group by province_name,
  city_group_name,
  update_time
ORDER BY province_name,
CASE WHEN city_group_name= '福州市' THEN 1
    WHEN city_group_name = '厦门市' THEN 2
    WHEN city_group_name = '泉州市' THEN 3
    WHEN city_group_name = '莆田市' THEN 4
    WHEN city_group_name = '南平市' THEN 5
    WHEN city_group_name = '三明市' THEN 6
    WHEN city_group_name = '宁德市' THEN 7
    WHEN city_group_name = '龙岩市' THEN 8
    WHEN city_group_name = '杭州市' THEN 9
    WHEN city_group_name = '宁波市' THEN 10
    WHEN city_group_name = '舟山市' THEN 11
    WHEN city_group_name = '南京市' THEN 12
    WHEN city_group_name = '苏州市' THEN 13
    WHEN city_group_name = '石家庄市' THEN 14
    WHEN city_group_name = '沙坪坝区' THEN 15
    WHEN city_group_name = '万州区' THEN 16
    WHEN city_group_name = '黔江区' THEN 17
    ELSE 18 END ASC;

