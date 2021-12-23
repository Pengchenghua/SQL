
hive_database=csx_tmp
table_name=dws_crm_r_d_customer_site_info
username="dataanprd_all"
password="slH25^672da"
months="current"
sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/sdt=${months} \
 --delete-target-dir \
 --query "select id,province_code,province_name,city_group_code,city_group_name,customer_code,customer_name,customer_child_code,customer_child_name,customer_site_code,customer_site_name,update_time,create_time,update_by,create_by,now()  from dws_crm_r_d_customer_site_info \$CONDITIONS" \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --hive-partition-key sdt \
 --hive-partition-value "$months" \
 --null-string '\\N'  \
 --null-non-string '\\N'

 --table $table_name \
 --columns 'id,province_code,province_name,city_group_code,city_group_name,customer_code,customer_name,customer_child_code,customer_child_name,customer_site_code,customer_site_name,update_time,create_time,update_by,create_by'\

;
sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
  --username "$username" \
 --password "$password" \
 --query " select id,province_code,province_name,city_group_code,city_group_name,customer_code,customer_name,customer_child_code,customer_child_name,customer_site_code,customer_site_name,update_time,create_time,update_by,create_by,insert_time from csx_tmp.dws_crm_r_d_customer_site_info" \
 --fields-terminated-by '\001' -m 1 \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/month=${month} \
 --delete-target-dir \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database csx_tmp \
 --hive-table dws_wms_r_m_inherent_cost  \
 --hive-partition-key months \
 --hive-partition-value "$month" \
 --null-string '\\N' \
 --null-non-string '\\N'



months="current"
sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
 --username dataanprd_all \
 --password slH25^672da \
 --query "select id,wms_storage_type_code,wms_storage_type_name,city_group_code,city_group_name,rental,fold_stand_amt,water_electricity_amt,personnel_amt,full_amt,update_time from dws_wms_r_m_inherent_cost where \$CONDITIONS "\
 --fields-terminated-by '\001'\
 --m 1 \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/dws_wms_r_m_inherent_cost/months=${months} \
 --delete-target-dir \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database csx_tmp \
 --hive-table dws_wms_r_m_inherent_cost \
 --hive-partition-key months \
 --hive-partition-value "$months"\
 --null-string '\\N' \
 --null-non-string '\\N'



hive_database=csx_tmp
table_name=dws_crm_r_d_customer_site_info
username="dataanprd_all"
password="slH25^672da"
months="current"
 sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
 --username dataanprd_all \
 --password slH25^672da \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/sdt=${months} \
 --delete-target-dir \
 --query "select id,province_code,province_name,city_group_code,city_group_name,customer_code,customer_name,customer_child_code,customer_child_name,customer_site_code,customer_site_name,update_time,create_time,update_by,create_by,now()  from dws_crm_r_d_customer_site_info \$CONDITIONS" \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --hive-partition-key sdt \
 --hive-partition-value "$months" \
 --null-string '\\N'  \
 --null-non-string '\\N'