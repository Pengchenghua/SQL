CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_direct_sales`(
	sales_months string comment '销售月份',
  `channel_code` string comment '渠道', 
  `channel_name` string comment '渠道', 
  `business_type_code` string comment '销售业务类型', 
  `business_type_name` string comment '销售业务类型', 
  `classify_large_code` string comment '管理一级分类', 
  `classify_large_name` string comment '管理一级分类', 
  `classify_middle_code` string comment '管理二级', 
  `classify_middle_name` string comment '管理二级', 
  `classify_small_code` string comment '管理三级', 
  `classify_small_name` string comment '管理三级', 
  `no_tax_sales_value` decimal(38,6) comment '未税销售额', 
  `no_tax_profit` decimal(38,6) comment '未税毛利额', 
  `no_tax_profit_rate` decimal(38,18) comment '未税毛利率',
   sales_value decimal(38,6) comment '未税销售额', 
   profit decimal(38,6) comment '毛利额', 
   profit_rate decimal(38,18) comment '毛利率',
   update_time timestamp comment '插入时间'
  ) comment '冻品财务-公司间交易销售'
  partitioned by (months string comment '月分区')

STORED AS parquet
; 