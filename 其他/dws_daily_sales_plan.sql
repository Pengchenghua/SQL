DROP TABLE csx_tmp.dws_all_format_plan_sales;
CREATE TABLE csx_tmp.dws_all_format_plan_sales (
  `id` string  COMMENT '',
  `province_code` string comment '省区编码',
  `province_name` string comment '省区名称',
  `city_group_code` string COMMENT '城市组',
  `city_group_name` string COMMENT '城市组省区',
  `channel_code` string COMMENT '渠道',
  `channel_name` string COMMENT '渠道名称',
  `attribute_code` string  COMMENT '客户属性及商超业态',
  `attribute_name` string COMMENT '客户属性及商超业态',
  `plan_sale_value` decimal(26,6) comment '销售计划',
  `plan_profit` decimal(26,6) comment '毛利计划',
  `plan_no_tax_sale` decimal(26,6) COMMENT '未税销售额',
  `plan_no_tax_profit` decimal(26,6)COMMENT '未税毛利额',
  `plan_profit_rate` decimal(26,6) COMMENT '毛利率',
  `plan_new_cust_num` BIGINT COMMENT '新客计划',
  `tmp_01` string COMMENT '预留字段',
  `tmp_02` string COMMENT '预留字段',
  `sdt` string  COMMENT '计划日期',
  create_time TIMESTAMP comment '创建日期',
  update_time TIMESTAMP comment '更新日期',
  update_ty string comment '更新人',
  create_by string comment '创建人'
) COMMENT '各业态销售计划含B端、M端、平台'
partitioned by (month string comment '日期分区');


hive_database=csx_tmp
table_name=dws_all_format_plan_sales
month=`date -d "yesterday" +%Y%m`
where="sdt = \"${month}\""
username="dataanprd_all"
password="slH25^672da"
sqoop import \
 --connect jdbc:mysql://10.0.74.10:3306/data_analysis_prd?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --table $table_name \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/month=${month} \
 --delete-target-dir \
 --columns 'id,province_code,province_name,city_group_code,city_group_name,channel_code,channel_name,attribute_code,attribute_name,plan_sale_value,plan_profit,plan_no_tax_sale,plan_no_tax_profit,plan_profit_rate,plan_new_cust_num,tmp_01,tmp_02,sdt,create_time,update_time,update_ty,create_by'\
 --where "${where}" \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --hive-partition-key month \
 --hive-partition-value "$month" \
 --null-string '\\N'  \
 --null-non-string '\\N'


 --query "select id,province_code,province_name,city_group_code,city_group_name,channel_code,channel_name,attribute_code,attribute_name,plan_sale_value,plan_profit,plan_no_tax_sale,plan_no_tax_profit,plan_profit_rate,plan_new_cust_num,tmp_01,tmp_02,sdt,create_time,update_time,update_ty,create_by from ${table_name} where \$CONDITIONS and sdt = ${month}" \


DROP TABLE csx_tmp.dws_daily_sales_plan;
CREATE TABLE `csx_tmp.dws_daily_sales_plan` (
  `id` string comment '',
  `plan_type` string comment '计划类型，1 大客户、2 商超',
  `province_code` string comment '省区',
  `province_name` string comment '省区',
  `channel_code` string comment '渠道',
  `channel_name` string comment '渠道',
  `plan_sale_value` decimal(26,6) comment '计划额',
  `plan_profit` decimal(26,6) comment '计划毛利额',
  `temp_01` string comment '预留字段',
  `temp_02` string comment '预留字段',
  `plan_sdt` string comment '每日日期',
  `months` string comment '月份',
  `create_time` string comment '创建日期',
  `create_by` string comment '创建人',
  `update_time` string comment '更新日期',
  `update_by` string comment '更新人'
) COMMENT '每日预算'
partitioned by (month string comment '日期分区');

hive_database=csx_tmp
table_name=dws_daily_sales_plan
month=`date -d "yesterday" +%Y%m`
where="sdt = \"${month}\""
username="dataanprd_all"
password="slH25^672da"
sqoop import \
 --connect jdbc:mysql://10.0.74.10:3306/data_analysis_prd?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --table $table_name \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/month=${month} \
 --delete-target-dir \
 --columns 'id,plan_type,province_code,province_name,channel_code,channel_name,plan_sale_value,plan_profit,temp_01,temp_02,sdt,months,create_time,create_by,update_time,update_by'\
 --where "${where}" \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --hive-partition-key month \
 --hive-partition-value "$month" \
 --null-string '\\N'  \
 --null-non-string '\\N'
