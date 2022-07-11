-- data_analysis_prd.source_r_m_province_month_category_target definition
-- 省区品类月销售预算导入
CREATE TABLE csx_tmp.`source_r_m_province_month_category_target` (
  `id` string  COMMENT '主键',
  `province_code` string comment'省区编码', 
  `province_name` string comment'省区名称', 
  `city_group_code` string comment'城市组',
  `city_group_name` string comment'城市组',
  `classify_middle_code` string comment'管理二级编码',
  `classify_middle_name` string comment'管理二级名称',
  `business_type_name` string comment '销售业务类型名称',
  `plan_sales_value` string COMMENT '计划销售额',
  `plan_profit` string COMMENT '计划毛利',
  `plan_profit_rate` string COMMENT '计划毛利率',
  `create_time` timestamp  COMMENT '创建时间',
  `create_by` string comment '创建者',
  `update_time` timestamp   COMMENT '更新时间',
  `update_by` string comment '更新者'
)  COMMENT'省区品类月销售预算'
partitioned by(months string comment'日期分区')
stored as parquet;



-- MYSQL同步HIVE 
    hive_database=csx_tmp
    table_name=source_r_m_province_month_category_target
    day='2022-06-30'
    month=`date -d ${day} +%Y%m`
    where="months = \"${month}\""
    username="dataanprd_all"
    password="slH25^672da"
    sqoop import \
    --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
    --username "$username" \
    --password "$password" \
    --table $table_name \
    --fields-terminated-by '\001' \
    --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/months=${month} \
    --delete-target-dir \
    --columns "id,province_code,province_name,city_group_code,city_group_name,classify_middle_code,classify_middle_name,business_type_name,plan_sales_value,plan_profit,plan_profit_rate,create_time,create_by,update_time,update_by " \
    --where "${where}" \
    --hive-drop-import-delims \
    --hive-import \
    --hive-database $hive_database \
    --hive-table $table_name \
    --hive-partition-key months \
    --hive-partition-value "$month" \
    --null-string '\\N'  \
    --null-non-string '\\N'