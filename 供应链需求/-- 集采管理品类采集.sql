-- 集采管理品类采集
drop table source_scm_w_a_group_purchase_classily;
create table csx_tmp.source_scm_w_a_group_purchase_classily
(
id BIGINT comment 'ID',
short_name string  comment '简称',
group_purchase string comment '集采品类',
classify_middle_code string comment '管理二级分类编码',
classify_middle_code string comment '管理二级分类编码',
classify_middle_name string comment '管理二级分类名称',
classify_small_code string comment '管理三级分类编码',
classify_small_name string comment '管理三级分类名称',
start_date string comment '生效日期',
end_date string comment '停用日期',
is_flag string comment '是否禁用 1 是 0 否',
`create_time` timestamp   COMMENT '创建时间',
`create_by` string   COMMENT '创建者',
`update_time` timestamp   COMMENT '更新时间',
`update_by` string   COMMENT '更新者' 
) comment= '集采管理分类采集'
;


hive_database=csx_tmp
table_name=source_scm_w_a_group_purchase_classily
username="dataanprd_all"
password="slH25^672da"
sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --table $table_name \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name} \
 --delete-target-dir \
 --columns 'id,short_name,group_purchase,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,start_date,end_date,is_flag,create_time,create_by,update_time,update_by'\
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --null-string '\\N'  \
 --null-non-string '\\N'



select * from csx_tmp.source_scm_w_a_group_purchase_classily