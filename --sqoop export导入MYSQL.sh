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
day=2022-02-28
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
level_id,sales_months,zone_id,zone_name,channel_code,channel_name,province_code,province_name,daily_plan_sale,daily_sales_value,real_daily_sales_value,real_daily_sale_fill_rate,daily_sale_fill_rate,last_week_daily_sales,daily_sale_growth_rate,daily_plan_profit,daily_profit,daily_profit_fill_rate,daily_profit_rate,daily_negative_profit,daily_often_cust_sale,daily_new_cust_sale,daily_sale_cust_num,month_plan_sale,month_sale_value,real_month_sale_value,real_month_sale_fill_rate,month_sale_fill_rate,last_month_sale,mom_sale_growth_rate,month_plan_profit,month_profit,month_profit_fill_rate,month_profit_rate,month_negative_profit,month_often_cust_sale,month_new_cust_sale,month_sale_cust_num,last_months_daily_sale,daily_sign_cust_num,daily_sign_amount,sign_cust_num,sign_amount,update_time,sdt


-- 按照分区导入设置动参 yesterday=`date -d "yesterday" +%Y%m%d`
columns='level_id,sales_months,zone_id,zone_name,channel_code,channel_name,province_code,province_name,daily_plan_sale,daily_sales_value,real_daily_sales_value,real_daily_sale_fill_rate,daily_sale_fill_rate,last_week_daily_sales,daily_sale_growth_rate,daily_plan_profit,daily_profit,daily_profit_fill_rate,daily_profit_rate,daily_negative_profit,daily_often_cust_sale,daily_new_cust_sale,daily_sale_cust_num,month_plan_sale,month_sale_value,real_month_sale_value,real_month_sale_fill_rate,month_sale_fill_rate,last_month_sale,mom_sale_growth_rate,month_plan_profit,month_profit,month_profit_fill_rate,month_profit_rate,month_negative_profit,month_often_cust_sale,month_new_cust_sale,month_sale_cust_num,last_months_daily_sale,daily_sign_cust_num,daily_sign_amount,sign_cust_num,sign_amount,update_time,sdt'
day=${enddate}
yesterday=${day//-/}
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table ads_sale_r_d_zone_sales_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table ads_sale_r_d_zone_sales_fr \
--hive-partition-key sdt \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"


-- 按照分区导入设置动参 yesterday=`date -d "yesterday" +%Y%m%d`  day=${enddate}
columns='level_id,sales_month,zone_id,zone_name,province_code,province_name,channel,channel_name,attribute_code,attribute_name,business_division_code,business_division_name,division_code,division_name,classify_middle_code,classify_middle_name,daily_plan_sale,daily_sale_value,daily_sale_fill_rate,daily_profit,daily_profit_rate,month_plan_sale,month_sale,month_sale_fill_rate,mom_sale_growth_rate,month_sale_ratio,month_avg_cust_sale,month_plan_profit,month_profit,month_profit_fill_rate,month_profit_rate,month_sales_sku,month_sale_cust_num,cust_penetration_rate,all_sale_cust_num,last_month_sale,last_month_profit,last_profit_rate,last_cust_penetration_rate,last_month_sale_cust_num,last_all_sale_cust,last_month_sale_ratio,same_period_sale,same_period_profit,same_period_profit_rate,same_period_cust_penetration_rate,same_period_sale_cust_num,same_period_all_sale_cust,same_sale_ratio,row_num,updatetime,months,sdt'
day=${enddate}
yesterday=${day//-/}
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_sale_r_d_zone_classify_sale_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_sale_r_d_zone_classify_sale_fr \
--hive-partition-key sdt \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"


columns='level_id,sales_month,zone_id,zone_name,province_code,province_name,attribute_code,attribute,daily_plan_sale,daily_sales_value,daily_sale_fill_rate,daily_profit,daily_profit_rate,month_plan_sale,month_sale,month_sale_fill_rate,last_month_sale,mom_sale_growth_rate,month_plan_profit,month_profit,month_profit_fill_rate,month_profit_rate,month_sale_cust_num,mom_diff_sale_cust,last_month_profit,last_month_sale_cust_num,update_time,months'
day=2022-03-24
yesterday=`date -d ${day} +%Y%m`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table ads_sale_r_d_zone_cust_attribute_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table ads_sale_r_d_zone_cust_attribute_fr \
--hive-partition-key months \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"


columns='region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,customer_name,ring_sale_value,ring_profit,ring_profit_rate,sale_value,profit,profit_rate,diff_profit_rate,ring_rank_desc,rank_desc,update_time,months'
day=2022-02-28
yesterday=`date -d ${day} +%Y%m`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_fr_r_d_zone_sale_customer_top10 \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_fr_r_d_zone_sale_customer_top10 \
--hive-partition-key months \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"
;

columns='sdt,region_code,region_name,province_code,province_name,city_group_code,city_group_name,plan_sales,plan_profit,sales_value,profit,profit_rate,sales_fill_rate,profit_fill_rate,update_time,mon'
day=2022-02-28
yesterday=`date -d ${day} +%Y%m`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_fr_r_d_zone_sale_days_trend \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_fr_r_d_zone_sale_days_trend \
--hive-partition-key mon \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"


columns='level_id,years,smonth,channel_name,region_code,region_name,province_code,province_name,city_group_code,city_group_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,sales_value,profit,profit_rate,daily_sales_value,daily_profit,daily_profit_rate,last_sales_value,last_profit,last_profit_rate,last_daily_sales_value,last_daily_profit,last_daily_profit_rate,ring_b_sales_rate,ring_daily_sales_rate,diff_profit_rate,diff_daily_profit_rate,all_sales_value,all_profit,all_profit_rate,classify_sales_ratio,prov_daily_sales_value,last_prov_daily_sales_value,sales_qty_30day,sales_value_30day,profit_30day,final_qty,final_amt,daily_cust_number,last_daily_cust_number,b_daily_cust_number,last_b_daily_cust_number,daily_cust_penetration_rate,last_daily_cust_penetration_rate,diff_daily_cust_penetration_rate,daily_sales_ratio,last_daily_sales_ratio,diff_daily_ratio_rate,grouping__id,update_time,months'
day=2022-03-31
yesterday=`date -d ${day} +%Y%m`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_sale_r_d_classify_ratio_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_sale_r_d_classify_ratio_fr \
--hive-partition-key months \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"



-- 物流商品库存周转
columns='years,months,province_code,province_name,dist_code,dist_name,city_code,city_name,dc_code,dc_name,goods_id,goods_name,standard,unit_name,brand_name,dept_id,dept_name,business_division_code,business_division_name,division_code,division_name,category_large_code,category_large_name,category_middle_code,category_middle_name,category_small_code,category_small_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,joint_purchase_flag,valid_tag,valid_tag_name,goods_status_id,goods_status_name,sales_qty,sales_value,profit,sales_cost,period_inv_qty,period_inv_amt,final_qty,final_amt,days_turnover,cost_30day,sales_30day,qty_30day,dms,inv_sales_days,period_inv_qty_30day,period_inv_amt_30day,days_turnover_30,max_sale_sdt,no_sale_days,dc_type,entry_qty,entry_value,entry_sdt,entry_days,contain_transfer_entry_qty,contain_transfer_entry_value,contain_transfer_entry_sdt,contain_transfer_entry_days,receipt_amt,receipt_qty,material_take_amt,material_take_qty,dc_uses,update_time,sdt'
day=2022-04-13
yesterday=`date -d ${day} +%Y%m%d`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table ads_wms_r_d_goods_turnover \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table ads_wms_r_d_goods_turnover \
--hive-partition-key sdt \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"


columns='level_id,province_code,province_name,dc_code,dc_name,dc_uses,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,sku,total_amt,total_turnover_day,high_stock_amt,high_stock_sku,no_sales_stock_amt,no_sales_stock_sku,validity_amt,validity_sku,update_time,sdt'
day=2022-04-14
yesterday=`date -d ${day} +%Y%m%d`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_wms_r_d_turnover_classify_kanban_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_wms_r_d_turnover_classify_kanban_fr \
--hive-partition-key sdt \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}";

  REPORT_WMS_R_D_STOCK_KANBAN_FR