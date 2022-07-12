-- 供应链仓信息配置
CREATE TABLE csx_tmp.dws_basic_supply_dc (
  `id` string comment 'id',
  `shop_code` string comment'门店编码',
  `shop_name` string COMMENT '门店机构全称',
  `shop_status` string comment'门店状态',
  `shop_status_desc` string COMMENT '门店状态描述',
  `open_date` string COMMENT '开店日期',
  `province_code` string comment'省份',
  `province_name` string COMMENT '省份描述',
  `city_code` string comment'地级市',
  `city_name` string COMMENT '地级市描述',
  `purchase_org` string comment'采购组织',
  `purchase_org_name` string COMMENT '采购组织描述',
  `location_type_code` string comment'仓库类型编码',
  `location_type_name` string COMMENT '仓库类型名称',
  `location_status` string COMMENT '状态  1 开启 2 禁用',
  `financial_body` string COMMENT '1:彩食鲜 2:其它',
  `purpose` string comment'地点用途： 01大客户仓库 02商超仓库 03工厂 04寄售门店 05彩食鲜小店 06合伙人物流 07BBC物流 08代加工工厂',
  `purpose_name` string COMMENT '地点用途名称',
  `is_frozen_dc` string COMMENT '是否冻品仓',
  `is_purchase_dc` string COMMENT '是否采购入库仓',
  `update_time` timestamp  COMMENT '更新时间',
  `create_time` timestamp  COMMENT '创建时间',
  `create_by` string COMMENT '创建人',
  `update_by` string COMMENT '更新者'
)  COMMENT'供应链采购入库仓信息配置'
partitioned by (sdt string comment '日期分区');

-- 同步HIVE
    hive_database=csx_tmp
    table_name=dws_basic_supply_dc
    day='2022-07-12'
    month=`date -d ${day} +%Y%m%d`
    username="dataanprd_all"
    password="slH25^672da"
    sqoop import \
    --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
    --username "$username" \
    --password "$password" \
    --table $table_name \
    --fields-terminated-by '\001' \
    --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/sdt=${month} \
    --delete-target-dir \
    --columns "id,shop_code,shop_name,shop_status,shop_status_desc,open_date,province_code,province_name,city_code,city_name,purchase_org,purchase_org_name,location_type_code,location_type_name,location_status,financial_body,purpose,purpose_name,is_frozen_dc,is_purchase_dc,update_time,create_time,create_by,update_by" \
    --hive-drop-import-delims \
    --hive-import \
    --hive-database $hive_database \
    --hive-table $table_name \
    --hive-partition-key sdt \
    --hive-partition-value "$month" \
    --null-string '\\N'  \
    --null-non-string '\\N'
    ;


    hive_database=csx_tmp
    table_name=dws_basic_supply_dc
    month=`current`
    username="dataanprd_all"
    password="slH25^672da"
    sqoop import \
    --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false \
    --username "$username" \
    --password "$password" \
    --table $table_name \
    --fields-terminated-by '\001' \
    --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/sdt=${month} \
    --delete-target-dir \
    --columns "id,shop_code,shop_name,shop_status,shop_status_desc,open_date,province_code,province_name,city_code,city_name,purchase_org,purchase_org_name,location_type_code,location_type_name,location_status,financial_body,purpose,purpose_name,is_frozen_dc,is_purchase_dc,update_time,create_time,create_by,update_by" \
    --hive-drop-import-delims \
    --hive-import \
    --hive-database $hive_database \
    --hive-table $table_name \
    --hive-partition-key sdt \
    --hive-partition-value "$month" \
    --null-string '\\N'  \
    --null-non-string '\\N'