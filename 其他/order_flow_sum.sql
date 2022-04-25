	CREATE TABLE `csx_dw.order_flow_sum`(
	  `order_date` string COMMENT '需求订单日期', 
	  `province_code` string COMMENT 'DC省区', 
	  `province_name` string COMMENT 'DC省区名称', 
	  `dc_code` string COMMENT 'DC编码', 
	  `shop_name` string COMMENT 'DC名称', 
	  `purchase_org_code` string COMMENT '采购组织', 
	  `super_class_name` string COMMENT '订单类型', 
	  `vender_code` string COMMENT '供应商编码', 
	  `vendor_name` string COMMENT '供应商名称', 
	  `org_vendor` string COMMENT '源供应商', 
	  `receive_location_code` string COMMENT '收货地点编码', 
	  `receive_name` string COMMENT '收货地址名称', 
	  `send_location_code` string COMMENT '发货地点编码', 
	  `send_location_name` string COMMENT '发货地点名称', 
	  `goods_code` string COMMENT '商品编码', 
	  `bar_code` string COMMENT '商品条码', 
	  `goodsname` string COMMENT '商品名称', 
	  `standard` string COMMENT '商品规格', 
	  `brand_name` string COMMENT '品牌', 
	  `unit_name` string COMMENT '单位', 
	  `bd_id` string COMMENT '事业部门编码', 
	  `bd_name` string COMMENT '事业部门名称', 
	  `category_code` string COMMENT '部类编码', 
	  `category_name` string COMMENT '部类名称', 
	  `dept_code` string COMMENT '课组编码', 
	  `dept_name` string COMMENT '课组名称', 
	  `category_large_code` string COMMENT '大类编码', 
	  `category_large_name` string COMMENT '大类名称',
      `category_middle_code` string COMMENT '中类编码', 
	  `category_middle_name` string COMMENT '中类名称', 	  
	  `category_small_code` string COMMENT '小类编码', 
	  `category_small_name` string COMMENT '小类名称', 
	  `order_qty` decimal(26,6) COMMENT '订单量', 
	  `order_amount` decimal(26,6) COMMENT '订单金额', 
	  `receive_qty` decimal(26,6) COMMENT '收货量', 
	  `receive_amount` decimal(26,6) COMMENT '收货金额', 
	  `shipped_qty` decimal(26,6) COMMENT '出库量', 
	  `shipped_amount` decimal(26,6) COMMENT '出库金额', 
	  `shipped_date` string COMMENT '出库日期', 
	  `receive_date` string COMMENT '收货日期', 
	  `order_status` string COMMENT '订单状态', 
	  `source_type_name` string COMMENT '来源订单')
	COMMENT '订单入库出库汇总'
	PARTITIONED BY ( 
	  `sdt` string COMMENT '日期分区按照需要订单日期')
	ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
	STORED AS parquet
	  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
	OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
	LOCATION
	  'hdfs://nameservice1/user/hive/warehouse/csx_dw.db/order_flow_sum'
	TBLPROPERTIES (
