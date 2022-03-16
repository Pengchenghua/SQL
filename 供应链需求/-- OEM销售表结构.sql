  -- OEM销售表结构
  create table csx_dw.ads_sale_r_d_oem_goods_fr as 
  `sdt` string comment '销售日期', 
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_group_code` string comment '城市组编码', 
  `city_group_name` string comment '城市组名称', 
  `channel_code` string comment '渠道编码', 
  `channel_name` string comment '渠道名称', 
  `business_type_code` string comment '销售业务类型', 
  `business_type_name` string comment '销售业务类型名称', 
  `goods_code` string comment '商品编码', 
  `bar_code` string comment '商品条码', 
  `goods_name` string comment '商品名称', 
  `unit_name` string comment '销售单位', 
  `brand_name` string comment '品牌名称', 
  `standard` string comment '销售规格', 
  `classify_large_code` string comment '管理一级编码', 
  `classify_large_name` string comment '管理一级名称', 
  `classify_middle_code` string comment '管理二级编码', 
  `classify_middle_name` string comment '管理二级名称', 
  `classify_small_code` string comment '管理三级编码', 
  `classify_small_name` string comment '管理三级名称', 
  `division_code` string comment '部类编码', 
  `division_name` string comment '部类名称', 
  `department_id` string comment '课组编码', 
  `department_name` string comment '课组名称', 
  `category_small_code` string comment '小类编码', 
  `sales_qty` decimal(30,6) comment '销量', 
  `sales_value` decimal(30,6) comment '销售额', 
  `profit` decimal(30,6) comment '毛利额', 
  `sales_cost` decimal(38,6) comment '销售成本', 
  `excluding_tax_sales` decimal(38,6) comment '未税销售额', 
  `excluding_tax_cost` decimal(38,6) comment '未税销售成本', 
  `excluding_tax_profit` decimal(38,6) comment '未税毛利额', 
  `oem_sales_value` decimal(38,6), 
  `oem_sales_cost` decimal(38,6), 
  `oem_profit` decimal(38,6), 
  `oem_excluding_tax_sales` decimal(38,6), 
  `oem_excluding_tax_cost` decimal(38,6), 
  `oem_excluding_tax_profit` decimal(38,6))
  `goods_purchase_level` string comment '商品采购级别编码', 
  `goods_purchase_level_name` string comment '商品采购级别名称'
  )comment 'OEM商品销售'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
;


CREATE TEMPORARY TABLE `csx_tmp.report_fr_r_d_oem_classify_sale`(
  `sdt` string comment '销售日期', 
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_group_code` string comment '城市组编码', 
  `city_group_name` string comment '城市组名称', 
  `channel_code` string comment '渠道编码', 
  `channel_name` string comment '渠道名称', 
  `business_type_code` string comment '销售业务类型', 
  `business_type_name` string comment '销售业务类型名称', 
  `classify_large_code` string comment '管理一级编码', 
  `classify_large_name` string comment '管理一级名称', 
  `classify_middle_code` string comment '管理二级编码', 
  `classify_middle_name` string comment '管理二级名称', 
  `sales_qty` decimal(38,6) comment '销量', 
  `sales_cost` decimal(38,6) comment '销售成本', 
  `sales_value` decimal(38,6) comment '销售额', 
  `profit` decimal(38,6) comment '毛利额', 
  `oem_sales_qty` decimal(38,6) comment 'OEM销售量', 
  `oem_sales_value` decimal(38,6) comment 'OEM销售额', 
  `oem_sales_cost` decimal(38,6) comment 'OEM销售成本', 
  `oem_profit` decimal(38,6) comment 'OEM毛利额',
  `oem_sales_ratio` decimal(38,6) comment 'OEM销售占比'
  )comment 'OEM管理二级销售报表'
partitioned by (sdt string comment '销售日期分区') 
