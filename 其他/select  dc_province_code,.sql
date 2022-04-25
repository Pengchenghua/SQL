select  dc_province_code,
	    dc_province_name,
       dc_city_name,
	   supervisor_name,
city_manager_name,
       sales_name,
       customer_no,
       customer_name,
       first_category,
       second_category,
       sum(sales_days)sales_days,
       sum(all_sales_value) all_sales_value,
       sum(all_sales_sku) as all_sales_sku,
       sum(all_profit) as all_profit,
       sum(all_profit)/sum(all_sales_value) all_profit_rate,
       sum(price_sales_value) price_sales_value, --中台销售额
       sum(price_sales_value)/sum(all_sales_value) as price_sale_ratio, --销售占比
       sum(price_sales_sku) as price_sales_sku, --销售SKU
       sum(price_sales_sku)/sum(all_sales_sku)  as price_sku_ratio, --SKU占比
       sum(price_profit) price_profit,  --综合毛利
       sum(price_profit)/sum(price_sales_value)*1.00 as price_profitrate, --综合毛利率       
       sum(price_sales_cost) /sum(price_qty)*1.00 as midd_cost, --平均成本
       sum(price_sales_cost) as price_sales_cost,  -- 销售成本
       sum(price_midd_sale) price_midd_sale , --中台报价销售
        sum(price_midd_sale-price_sales_cost) as midd_profit,
    sum(price_midd_sale-price_sales_cost)/sum(price_sales_value)*1.00 as price_midd_profitrate,--中台毛利率（毛利额-报价销售额）
       sum(price_front_profit) as price_front_profit,  --前端毛利      
sum(price_front_profit)/  sum(price_sales_value)  price_front_profitrate    
       FROM (
SELECT dc_province_code,dc_province_name,
       dc_city_name,
	   supervisor_name,
	   city_manager_name,
       sales_name,
       customer_no,
       customer_name,
       first_category,
       second_category,
       count(DISTINCT sdt)sales_days,
       sum(sales_value) all_sales_value,
       sum(sales_qty) as all_sales_qty,
      0 as all_sales_sku,
       sum(profit) as all_profit,
       0 as  price_sales_value, --销售额
       0 as price_sales_sku, --销售SKU
       0 as price_sales_cost,  -- 销售成本
       0 as price_qty,
       0 as price_front_profit,  --前端毛利
       0 price_profit,  --综合毛利
       0 price_midd_sale 
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE  sdt>=regexp_replace(to_date('${sdate}'),'-','')
  AND sdt<=regexp_replace(to_date('${edate}'),'-','')
  ${IF(len(chann)==0,"","and channel in ('"+chann+"')")}
 -- ${IF(len(provcode)==0,"","and dc_province_code in ('"+provcode+"')")}
  ${IF(len(dc)==0,"","and dc_code in ('"+dc+"')")}
  ${IF(len(attribute)==0,"","and attribute in ('"+attribute+"')")}
   ${IF(len(ordertype)==0,"","and order_kind in ('"+ordertype+"')")}
  group by dc_province_code,dc_province_name,
       dc_city_name,supervisor_name,
city_manager_name,sales_name,
       customer_no,
       customer_name,
       first_category,
       second_category
 union all 
 SELECT dc_province_code,dc_province_name,
       dc_city_name,
	   supervisor_name,
city_manager_name,
       sales_name,
       customer_no,
       customer_name,
       first_category,
       second_category,
       0 sales_days,
       0 all_sales_value,
      0 as all_sales_qty,
      count(DISTINCT goods_code) as all_sales_sku,
       0 as all_profit,
       0 as  price_sales_value, --销售额
       0 as price_sales_sku, --销售SKU
       0 as price_sales_cost,  -- 销售成本
       0 as price_qty,
       0 as price_front_profit,  --前端毛利
       0 price_profit,  --综合毛利
       0 price_midd_sale 
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE  sdt>=regexp_replace(to_date('${sdate}'),'-','')
  AND sdt<=regexp_replace(to_date('${edate}'),'-','')
    ${IF(len(chann)==0,"","and channel in ('"+chann+"')")}
	--${IF(len(provcode)==0,"","and dc_province_code in ('"+provcode+"')")}
	${IF(len(dc)==0,"","and dc_code in ('"+dc+"')")}
	${IF(len(attribute)==0,"","and attribute in ('"+attribute+"')")}
	 ${IF(len(ordertype)==0,"","and order_kind in ('"+ordertype+"')")}
  group by dc_province_code,dc_province_name,
       dc_city_name,
	   supervisor_name,
city_manager_name,
       sales_name,
       customer_no,
       customer_name,
       first_category,
       second_category
union all 
SELECT dc_province_code,dc_province_name,
       dc_city_name,
	   supervisor_name,
city_manager_name,
       sales_name,
       customer_no,
       customer_name,
       first_category,
       second_category,
      0 as sales_days,
      0 as all_sales_value,
      0 as all_sales_qty,
      0 as all_sales_sku,
       0 as all_profit,
       sum(sales_value) price_sales_value, --销售额
       count(DISTINCT goods_code) as price_sales_sku, --销售SKU
       sum(sales_cost) as price_sales_cost,  -- 销售成本
       sum(sales_qty) as price_qty, --销售数量
       sum(front_profit) as price_front_profit,  --前端毛利
       sum(profit) price_profit,  --综合毛利
       sum(middle_office_price*sales_qty*1.00) price_midd_sale  --中台报价销售
       FROM csx_dw.dws_sale_r_d_customer_sale
WHERE sdt>=regexp_replace(to_date('${sdate}'),'-','')
  AND sdt<=regexp_replace(to_date('${edate}'),'-','')
    ${IF(len(chann)==0,"","and channel in ('"+chann+"')")}
	--${IF(len(provcode)==0,"","and dc_province_code in ('"+provcode+"')")}
	${IF(len(dc)==0,"","and dc_code in ('"+dc+"')")}
		${IF(len(attribute)==0,"","and attribute in ('"+attribute+"')")}
	 ${IF(len(ordertype)==0,"","and order_kind in ('"+ordertype+"')")}
 and REPORT_price=1
  group by dc_province_code,dc_province_name,
       dc_city_name,
	   supervisor_name,
city_manager_name,
       sales_name,
       customer_no,
       customer_name,
       first_category,
       second_category
       ) a 
       group by 
        dc_province_code,dc_province_name,
       dc_city_name,
	   supervisor_name,
city_manager_name,
       sales_name,
       customer_no,
       customer_name,
       first_category,
       second_category
       order by  dc_province_code,dc_province_name,
       dc_city_name,
       customer_no,
       customer_name,
       first_category,
       second_category;
