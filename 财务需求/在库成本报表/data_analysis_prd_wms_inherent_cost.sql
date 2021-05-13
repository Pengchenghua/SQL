drop table wms_inherent_cost ;
CREATE TABLE `dws_wms_r_m_inherent_cost` (
    id int(11) NOT NULL AUTO_INCREMENT ,
 `wms_storage_type_code` varchar(100) NOT NULL COMMENT '仓储类型编码',
  `wms_storage_type_name` varchar(100) NOT NULL COMMENT '仓储类型名称', 
  `city_group_code` varchar(100) DEFAULT NULL COMMENT '城市编码',
  `city_group_name` varchar(100) DEFAULT NULL COMMENT '城市名称',
  `rental` decimal(26,6) DEFAULT NULL COMMENT '租金',
  `fold_stand_amt` decimal(26,6) DEFAULT NULL COMMENT '折摊费',
  `water_electricity_amt` decimal(26,6) DEFAULT NULL COMMENT '水电费',
  `personnel_amt` decimal(26,6) DEFAULT NULL COMMENT '人员费用',
  `full_amt` decimal(26,6) DEFAULT NULL COMMENT '满储费',
  primary key (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='WMS仓储固有费用收集'

-- 删除
alter table dws_wms_r_m_inherent_cost drop  column id;
alter table dws_wms_r_m_inherent_cost add primary key (id) ;

-- 增加字段加主键
ALTER TABLE dws_wms_r_m_inherent_cost ADD id INT(16) NOT NULL
 PRIMARY KEY AUTO_INCREMENT FIRST;

-- hive 插入表
 CREATE TABLE `csx_tmp.dws_wms_r_m_inherent_cost` (
   id string comment '主键' ,
  `wms_storage_type_code` string COMMENT '仓储类型编码',
  `wms_storage_type_name` string  COMMENT '仓储类型名称', 
  `city_group_code`string  COMMENT '城市编码',
  `city_group_name`string COMMENT '城市名称',
  `rental` decimal(26,6)  COMMENT '租金',
  `fold_stand_amt` decimal(26,6) COMMENT '折摊费',
  `water_electricity_amt` decimal(26,6)COMMENT '水电费',
  `personnel_amt` decimal(26,6)  COMMENT '人员费用',
  `full_amt` decimal(26,6) COMMENT '满储费',
   update_time TIMESTAMP comment '更新日期'
) COMMENT'WMS仓储固有费用收集'
partitioned by (months string comment '月分区，取插入当前月');

