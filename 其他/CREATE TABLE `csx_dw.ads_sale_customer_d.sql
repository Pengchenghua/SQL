CREATE TABLE `csx_dw.ads_sale_customer_division_level_sales_months` (
  `sales_year` string comment '销售年份',
  `sales_month` string comment '销售月份',
  `layer` string comment '层级 ：1 合计、2采购部、3课组',
  `business_division_code` string comment '采购部编码',
  `business_division_name` string comment '采购部名称',
  `purchase_group_code` string comment '课组编码',
  `purchase_group_name` string comment '课组名称',
  `customer_no` string comment '编码',
  `customer_name` string comment '名称',
  `attribute_code` int comment '属性编码',
  `attribute` string comment '属性名称',
  `first_category_code` string comment '企业类型编码一级',
  `first_category` string comment '企业类型类型名称一级',
  `second_category_code` string comment '企业类型类型编码二级',
  `second_category` string comment '企业类型类型名称二级',
  `third_category_code` string comment '企业类型类型编码三级',
  `third_category` string comment '企业类型类型名称三级',
  `sales_province_code` string comment '销售省区编码',
  `sales_province` string comment '销售省区名称',
  `is_copemate_order` int comment '是否合伙人（0.否  1.是）',
  `channel` string comment '战报渠道编码',
  `channel_name` string comment '战报渠道名称',
  `province_code` string comment '战报省区编码',
  `province_name` string comment '战报省区名称',
  `city_code` string comment '战报城市编码',
  `city_name` string comment '战报城市名称',
  `city_group_code` string comment '战报城市组编码',
  `city_group_name` string comment '战报城市组名称',
  `city_real` string comment '战报城市组名称',
  `cityjob` string comment '战报城市组负责人',
  `province_manager_id` string comment '战报省区负责人',
  `province_manager_work_no` string comment '战报省区负责人工号',
  `province_manager_name` string comment '战报省区负责人姓名',
  `sales_sku` bigint comment '销售sku',
  `sales_days` bigint comment '销售天数',
  `sales_value` decimal(36, 6) comment '销售额',
  `sales_qty` decimal(36, 6) comment '销量',
  `profit` decimal(36, 6) comment '毛利额',
  `front_profit` decimal(36, 6) comment '前端毛利额',
  `profit_rate` decimal(38, 6) comment '毛利率',
  `return_amt` decimal(36, 6) comment '退货额',
  `rank_num` int comment '销售排名'
) comment '月销售报表' partitioned by (sdt string comment '日期分区') STORED AS parquet LOCATION 'hdfs://nameservice1/user/hive/warehouse/csx_dw.db/ads_sale_customer_division_level_sales'
