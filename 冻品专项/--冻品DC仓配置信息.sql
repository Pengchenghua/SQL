--冻品DC仓配置信息
CREATE TABLE `dws_basic_frozen_dc`(
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shop_id` varchar(32) NOT NULL DEFAULT '' COMMENT '门店编码(以底表中的location_code字段作为唯一编码取得,用于关联门店编码shop_code)',
  `shop_name` varchar(128) NOT NULL DEFAULT '' COMMENT '门店机构全称',
   `sales_dist` varchar(32) NOT NULL DEFAULT '' COMMENT '销售区域代码',
  `sales_dist_name` varchar(128) NOT NULL DEFAULT '' COMMENT '销售区域名称',
  `company_code` varchar(32) NOT NULL DEFAULT '' COMMENT '公司代码',
  `company_name` varchar(128) NOT NULL DEFAULT '' COMMENT '公司名称',
  `shop_status` varchar(32) NOT NULL DEFAULT '' COMMENT '门店状态',
  `shop_status_desc` varchar(128) NOT NULL DEFAULT '' COMMENT '门店状态描述',
   `open_date` varchar(128) NOT NULL DEFAULT '' COMMENT '开店日期',
  `province_code` varchar(32) NOT NULL DEFAULT '' COMMENT '省份',
  `province_name` varchar(128) NOT NULL DEFAULT '' COMMENT '省份描述',
  `city_code` varchar(32) NOT NULL DEFAULT '' COMMENT '地级市',
  `city_name` varchar(128) NOT NULL DEFAULT '' COMMENT '地级市描述',
  `town_code` varchar(32) NOT NULL DEFAULT '' COMMENT '县级市',
  `town_name` varchar(128) NOT NULL DEFAULT '' COMMENT '县级市描述',
  `sales_region_code` varchar(32) NOT NULL DEFAULT '' COMMENT '销售大区编码(业绩划分)',
  `sales_region_name` varchar(128) NOT NULL DEFAULT '' COMMENT '销售大区名称(业绩划分)',
  `sales_province_code` varchar(32) NOT NULL DEFAULT '' COMMENT '销售归属省区编码',
  `sales_province_name` varchar(128) NOT NULL DEFAULT '' COMMENT '销售归属省区名称',
  `city_group_code` varchar(32) NOT NULL DEFAULT '' COMMENT '城市组编码(业绩划分)',
  `city_group_name` varchar(128) NOT NULL DEFAULT '' COMMENT '城市组名称(业绩划分)',
   `purchase_org` varchar(32) NOT NULL DEFAULT '' COMMENT '采购组织',
  `purchase_org_name` varchar(128) NOT NULL DEFAULT '' COMMENT '采购组织描述',
  `location_type_code` varchar(32) NOT NULL DEFAULT '' COMMENT '仓库类型编码',
  `location_type_name` varchar(128) NOT NULL DEFAULT '' COMMENT '仓库类型名称',
  `location_status` int(11) NOT NULL DEFAULT '0' COMMENT '状态  1 开启 2 禁用',
  `financial_body` int(11) NOT NULL DEFAULT '0' COMMENT '1:彩食鲜 2:其它',
  `purpose` varchar(32) NOT NULL DEFAULT '' COMMENT '地点用途： 01大客户仓库 02商超仓库 03工厂 04寄售门店 05彩食鲜小店 06合伙人物流 07BBC物流 08代加工工厂',
  `purpose_name` varchar(128) NOT NULL DEFAULT '' COMMENT '地点用途名称',
   is_frozen_dc varchar(8) not null default '0' comment '是否冻品仓',
  PRIMARY KEY (`id`),
  KEY `purpose` (`purpose`,`province_code`,shop_id)
) ENGINE=InnoDB AUTO_INCREMENT=4345 DEFAULT CHARSET=utf8mb4 COMMENT='冻品仓信息配置'




-- HIVE 表
CREATE TABLE `source_basic_frozen_dc` (
  `id` string,
  `shop_id` string   COMMENT '门店编码(以底表中的location_code字段作为唯一编码取得,用于关联门店编码shop_code)',
  `shop_name` string COMMENT '门店机构全称',
  `shop_status` string COMMENT '门店状态',
  `shop_status_desc` string COMMENT '门店状态描述',
  `open_date` string COMMENT '开店日期',
  `province_code` string COMMENT '省份',
  `province_name` string COMMENT '省份描述',
  `city_code` string COMMENT '地级市',
  `city_name` string COMMENT '地级市描述',
  `purchase_org` string COMMENT '采购组织',
  `purchase_org_name` string COMMENT '采购组织描述',
  `location_type_code` string COMMENT '仓库类型编码',
  `location_type_name` string COMMENT '仓库类型名称',
  `location_status` int  COMMENT '状态  1 开启 2 禁用',
  `financial_body` int   COMMENT '1:彩食鲜 2:其它',
  `purpose` string COMMENT '地点用途： 01大客户仓库 02商超仓库 03工厂 04寄售门店 05彩食鲜小店 06合伙人物流 07BBC物流 08代加工工厂',
  `purpose_name` string COMMENT '地点用途名称',
  `is_frozen_dc` string COMMENT '是否冻品仓',
  `update_time` date COMMENT '更新时间',
  `create_time` date COMMENT '创建时间',
  `create_by` string  COMMENT '创建人',
  `update_by` string  COMMENT '更新者'
)   COMMENT '冻品仓信息配置' 
PARTITIONED BY (
`sdt` string COMMENT '日期分区')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orc tblproperties ("author"="wangkuiming");