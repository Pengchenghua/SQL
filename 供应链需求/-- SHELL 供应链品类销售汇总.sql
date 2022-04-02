-- SHELL 供应链品类销售汇总
columns='level_id,years,channel_name,business_type_code,business_type,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,sales_plan,sales_value,profit,profit_rate,last_sales_value,last_profit,last_profit_rate,ring_sale_rate,group_id,update_time,months'
day=${enddate}
yesterday=`date -d"${day}" "+%Y%m"`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_sale_r_d_manage_sum \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_sale_r_d_manage_sum \
--hive-partition-key months \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"



-- 月度供应链品类销售汇总插入mysql
columns='level_id,years,channel_name,business_type_code,business_type,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,sales_plan,sales_value,profit,profit_rate,last_sales_value,last_profit,last_profit_rate,ring_sale_rate,group_id,update_time,months'
yesterday=202201
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_sale_r_d_manage_sum \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_sale_r_d_manage_sum \
--hive-partition-key months \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"