--sqoop export导入MYSQL
-- 全量导入
columns='level_id,sales_months,zone_id,zone_name,channel,channel_name,province_code,province_name,city_group_code,city_group_name,manager_no,manager_name,all_cust_count,all_daily_sale,all_plan_sale,all_month_sale,real_month_sale,real_sales_fill_rate,all_sales_fill_rate,all_last_month_sale,all_mom_sale_growth_rate,all_plan_profit,all_month_profit,all_month_profit_fill_rate,all_month_profit_rate,old_cust_count,old_daily_sale,old_plan_sale,old_month_sale,old_sales_fill_rate,old_last_month_sale,old_mom_sale_growth_rate,old_month_profit,old_month_profit_rate,new_plan_sale_cust_num,new_cust_count,new_cust_count_fill,new_daily_sale,new_plan_sale,new_month_sale,new_month_sale_fill_rate,new_last_month_sale,new_mom_sale_growth_rate,new_month_profit,new_month_profit_rate,new_sign_cust_num,new_sign_amount,daily_sign_cust_num,daily_sign_amount,update_time'

sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table ads_sale_r_d_zone_supervisor_fr \
--hcatalog-database csx_tmp \
--hcatalog-table ads_sale_r_d_zone_supervisor_fr \
--m 64 \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"


-- 按照分区导入 yesterday=`date -d "yesterday" +%Y%m%d`
columns='level_id,sales_months,zone_id,zone_name,channel,channel_name,province_code,province_name,city_group_code,city_group_name,manager_no,manager_name,all_cust_count,all_daily_sale,all_plan_sale,all_month_sale,real_month_sale,real_sales_fill_rate,all_sales_fill_rate,all_last_month_sale,all_mom_sale_growth_rate,all_plan_profit,all_month_profit,all_month_profit_fill_rate,all_month_profit_rate,old_cust_count,old_daily_sale,old_plan_sale,old_month_sale,old_sales_fill_rate,old_last_month_sale,old_mom_sale_growth_rate,old_month_profit,old_month_profit_rate,new_plan_sale_cust_num,new_cust_count,new_cust_count_fill,new_daily_sale,new_plan_sale,new_month_sale,new_month_sale_fill_rate,new_last_month_sale,new_mom_sale_growth_rate,new_month_profit,new_month_profit_rate,new_sign_cust_num,new_sign_amount,daily_sign_cust_num,daily_sign_amount,update_time,sdt'
day='2022-01-01'
yesterday=${day//-/}
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table ads_sale_r_d_zone_supervisor_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table ads_sale_r_d_zone_supervisor_fr \
--hive-partition-key sdt \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"



-- 按照分区导入设置动参 yesterday=`date -d "yesterday" +%Y%m%d`
columns='level_id,sales_months,zone_id,zone_name,channel,channel_name,province_code,province_name,city_group_code,city_group_name,manager_no,manager_name,all_cust_count,all_daily_sale,all_plan_sale,all_month_sale,real_month_sale,real_sales_fill_rate,all_sales_fill_rate,all_last_month_sale,all_mom_sale_growth_rate,all_plan_profit,all_month_profit,all_month_profit_fill_rate,all_month_profit_rate,old_cust_count,old_daily_sale,old_plan_sale,old_month_sale,old_sales_fill_rate,old_last_month_sale,old_mom_sale_growth_rate,old_month_profit,old_month_profit_rate,new_plan_sale_cust_num,new_cust_count,new_cust_count_fill,new_daily_sale,new_plan_sale,new_month_sale,new_month_sale_fill_rate,new_last_month_sale,new_mom_sale_growth_rate,new_month_profit,new_month_profit_rate,new_sign_cust_num,new_sign_amount,daily_sign_cust_num,daily_sign_amount,update_time,sdt'
day='2022-01-01'
yesterday=${day//-/}
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table ads_sale_r_d_zone_supervisor_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table ads_sale_r_d_zone_supervisor_fr \
--hive-partition-key sdt \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"

yesterday=`date -d yesterday +%Y%m%d`
columns='biz_id,province_code,province_name,city_code,city_name,dc_code,dc_name,goods_code,goods_name,unit_name,stock_qty,receive_qty_zero,receive_qty_three,receive_qty_five,receive_qty_seven,on_way_qty,sdt'
sqoop export \
  --connect "jdbc:mysql://10.0.74.77:7477/csx_data_market?useUnicode=true&characterEncoding=utf-8" \
  --username datagroup_app \
  --password 'Hoaerwsadr' \
  --table report_wms_r_a_niuyuebao_stock_goods \
  --hcatalog-database csx_dw \
  --hcatalog-table report_wms_r_a_niuyuebao_stock_goods \
  --hive-partition-key sdt \
  --hive-partition-value "$yesterday" \
  --input-null-string '\\N'  \
  --input-null-non-string '\\N' \
  --columns "${columns}"

  ;

  y='2022-01-01'
  echo ${y//-/} 
