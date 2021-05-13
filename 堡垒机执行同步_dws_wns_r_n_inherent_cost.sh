
hive_database=csx_tmp
table_name=dws_wms_r_m_inherent_cost
username="dataanprd_all"
password="slH25^672da"
months="current"
sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --table $table_name \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/months=${months} \
 --delete-target-dir \
 --columns 'id,wms_storage_type_code,wms_storage_type_name,city_group_code,city_group_name,rental,fold_stand_amt,water_electricity_amt,personnel_amt,full_amt,update_time'\
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --hive-partition-key months \
 --hive-partition-value "$months" \
 --null-string '\\N'  \
 --null-non-string '\\N'

;

sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
  --username "$username" \
 --password "$password" \
 --query "select id,wms_storage_type_code,wms_storage_type_name,city_group_code,city_group_name,rental,fold_stand_amt,water_electricity_amt,personnel_amt,full_amt,update_time from dws_wms_r_m_inherent_cost  where \$CONDITIONS " \
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