CREATE TABLE `base_price_sync` (
  `id` bigint(20) DEFAULT NULL COMMENT 'id条数标识',
  `location_code` varchar(64) DEFAULT NULL COMMENT '库存DC编码',
  `location_name` varchar(64) DEFAULT NULL COMMENT '库存DC名称',
  `goods_code` varchar(64) DEFAULT NULL COMMENT '商品编码',
  `goods_name` varchar(100) DEFAULT NULL COMMENT '商品名称',
  `avg_prices_last_month` decimal(26,6) DEFAULT '0.000000' COMMENT '最近30天平均库存成本价',
  `last_in_price` decimal(26,6) DEFAULT '0.000000' COMMENT '最新进价',
  `last_purchase_prices` decimal(26,6) DEFAULT '0.000000' COMMENT '最近采购报价',
  `last_put_supplier` varchar(64) DEFAULT '0.000000' COMMENT '最近入库供应商',
  `last_put_time` datetime DEFAULT NULL COMMENT '最近一次入库时间',
  `is_default_flag` int(11) DEFAULT '2' COMMENT '是否有入库标识、0标识有1标识无 2标识其他',
  `update_time` datetime DEFAULT NULL COMMENT '数据同步时间',
  `sdt` varchar(11) DEFAULT NULL COMMENT 'hive任务时间',
  UNIQUE KEY `uq_location_goods` (`location_code`,`goods_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='仓库商品进价表';





---mysql建表语句

CREATE TABLE fixation_report_customer_sale_month (
  sales_province_code varchar(64) DEFAULT NULL COMMENT '销售省区编码',
  sales_province varchar(64) DEFAULT NULL COMMENT '销售省区',
  sales_city_code varchar(64) DEFAULT NULL COMMENT '城市编码',
  sales_city varchar(64) DEFAULT NULL COMMENT '城市',
  channel  varchar(64) DEFAULT NULL COMMENT '渠道',
  customer_no varchar(64) DEFAULT NULL COMMENT '编码',
  customer_name varchar(64) DEFAULT NULL COMMENT '名称',
  first_category_code varchar(64) DEFAULT NULL COMMENT '一级分类编码',
  first_category varchar(64) DEFAULT NULL COMMENT '一级分类',
  second_category_code varchar(64) DEFAULT NULL COMMENT '二级分类编码',
  second_category varchar(64) DEFAULT NULL COMMENT '二级分类',
  third_category_code varchar(64) DEFAULT NULL COMMENT '三级分类编码',
  third_category varchar(64) DEFAULT NULL COMMENT '三级分类',
  is_this_month  varchar(64) DEFAULT NULL COMMENT '是否为本月签约',
  sign_time varchar(64) DEFAULT NULL COMMENT '签约时间',
  sales_id varchar(64) DEFAULT NULL COMMENT '业务员id',
  sales_name varchar(64) DEFAULT NULL COMMENT '业务员名称',
  first_supervisor_work_no varchar(64) DEFAULT NULL COMMENT '主管工号',
  first_supervisor_name varchar(64) DEFAULT NULL COMMENT '主管名称',
  hz int(11) default '0' COMMENT '频次',
  sales_value decimal(26,6) DEFAULT '0.000000' COMMENT '销售额',
  prorate decimal(26,6) DEFAULT '0.000000' COMMENT '毛利率',
  year_month varchar(64) DEFAULT NULL COMMENT '月份',
  sum_hz int(11) default '0' COMMENT '合计频次',
  sum_sales_value decimal(26,6) DEFAULT '0.000000' COMMENT '合计销售额',
  sum_proate decimal(26,6) DEFAULT '0.000000' COMMENT '合计毛利率',
  sdt  varchar(64) DEFAULT NULL COMMENT 'hive更新时间'

)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='销售月汇总表';



--hive建表语句

drop  table csx_dw.fixation_report_customer_sale_month
CREATE TABLE csx_dw.fixation_report_customer_sale_month (
  sales_province_code string COMMENT '销售省区编码',
  sales_province string COMMENT '销售省区',
  sales_city_code string COMMENT '城市编码',
  sales_city string COMMENT '城市',
  channel  string COMMENT '渠道',
  customer_no string COMMENT '编码',
  customer_name string COMMENT '名称',
  first_category_code string COMMENT '一级分类编码',
  first_category string COMMENT '一级分类',
  second_category_code string COMMENT '二级分类编码',
  second_category string COMMENT '二级分类',
  third_category_code string COMMENT '三级分类编码',
  third_category string COMMENT '三级分类',
  is_this_month  string COMMENT '是否为本月签约',
  sign_time string COMMENT '签约时间',
  sales_id string COMMENT '业务员id',
  sales_name string COMMENT '业务员名称',
  first_supervisor_work_no string COMMENT '主管工号',
  first_supervisor_name string COMMENT '主管名称',
  hz int COMMENT '频次',
  sales_value decimal(26,6)  COMMENT '销售额',
  prorate decimal(26,6)  COMMENT '毛利率',
  year_month string  COMMENT '月份',
  sum_hz int COMMENT '合计频次',
  sum_sales_value decimal(26,6)  COMMENT '合计销售额',
  sum_proate decimal(26,6)  COMMENT '合计毛利率'
)  COMMENT '销售月汇总表'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE 
LOCATION 'hdfs://nameservice1/user/hive/warehouse/csx_dw.db/fixation_report_customer_sale_month';


INVALIDATE METADATA csx_dw.fixation_report_customer_sale_month;