create table csx_dw.provinces_kanban(

type                string 			  comment '类型',
months_sale         string            comment '销售月份',
province_code       string            comment '省区编码',
province_name       string            comment '省区名称',
workshop_code       string            comment '车间编码/部类编码/渠道',
workshop_name		string            comment '车间编码/部类编码/渠道名称',
sale_sku            decimal(26,6)     comment '销售SKU',
all_sku             decimal(26,6)     comment '总SKU',
pin_rate,           decimal(26,6)     comment '动销率=销售SKU/总SKU',
sale                decimal(26,6)     comment '销售额',
profit              decimal(26,6)     comment '毛利额',
profit_rate         decimal(26,6)     comment '毛利率',
mom_sale            decimal(26,6)     comment '环期销售额',
mom_profit          decimal(26,6)     comment '环期毛利额',
mom_profit_rate     decimal(26,6)     comment '环期毛利率',
yoy_sale            decimal(26,6)     comment '同期销售额',
yoy_profit          decimal(26,6)     comment '同期毛利额',
yoy_profit_rate     decimal(26,6)     comment '同期毛利率',
sale_ring_ratio     decimal(26,6)     comment '销售环比增长率',
profit_ring_ratio   decimal(26,6)     comment '毛利环比长率',
mom_gross_rate_diff decimal(26,6)     comment '毛利率环期率差',
sale_yoy_ratio      decimal(26,6)     comment '销售同比增长率',
profit_yoy_ratio    decimal(26,6)     comment '毛利同比增长率',
yoy_gross_rate_diff  decimal(26,6)     comment '毛利率同比率差',
final_amt           decimal(26,6)     comment '期间库存额',
period_qty          decimal(26,6)     comment '期末库存量',
period_amt          decimal(26,6)     comment '期末库存额',
day_turnover        decimal(26,6)     comment '周转天数',
negative_sku        decimal(26,6) 	  comment '负库存SKU',
receive_amt         decimal(26,6)     comment '入库金额',
shop_dept_cust     decimal(26,6)      comment '销售商超客户数',
big_dept_cust      decimal(26,6)      comment '销售大客户数',
sale_cust_ratio    decimal(26,6)      comment '大客户数占比 销售客户数/大客户总数',
big_cust           decimal(26,6)      comment '大客户总数'
) comment '省区销售看板'
partitioned by (sdt string comment'日期分区')
stored as parquet
;

CREATE TABLE csx_dw.dc_sale_inventory (
  years string comment'年份',
  months string   comment '月份',
  saledate STRING COMMENT '销售日期',
  province_code STRING COMMENT '省区编码',
  province_name STRING COMMENT '省区名称',
  dc_code STRING COMMENT 'DC编码',
  dc_name STRING COMMENT 'DC名称',
  goods_code STRING COMMENT '商品编码',
  goods_bar_code STRING COMMENT '条码',
  goods_name STRING COMMENT '商品名称',
  spec STRING COMMENT '规格',
  unit_name STRING COMMENT '单位',
  brand_name STRING COMMENT '品牌',
  bd_id STRING COMMENT '采购部编码',
  bd_name STRING COMMENT '采购部名称',
  dept_id STRING COMMENT '课组编码',
  dept_name STRING COMMENT '课组名称',
  div_id STRING COMMENT '部类编码',
  div_name STRING COMMENT '部类名称',
  category_large_code STRING COMMENT '大类编码',
  category_large_name STRING COMMENT '大类名称',
  category_middle_code STRING COMMENT '中类编码',
  category_middle_name STRING COMMENT '中类名称',
  category_small_code STRING COMMENT '小类编码',
  category_small_name STRING COMMENT '小类名称',
  valid_tag STRING COMMENT '有效标识',
  valid_tag_name STRING COMMENT '有效标识名称',
  goods_status_id STRING COMMENT '商品状态编码',
  goods_status_name STRING COMMENT '商品状态名称',
  sales_cost DECIMAL(26,6) COMMENT '销售成本',
  sales_qty DECIMAL(26,6) COMMENT '销售数量',
  sales_vlaue DECIMAL(26,6) COMMENT '销售金额',
  profit DECIMAL(26,6) COMMENT '毛利',
  inventory_qty DECIMAL(26,6) COMMENT '结余库存量',
  inventory_amt DECIMAL(26,6) COMMENT '结余库存额',
  vendor_code STRING COMMENT '供应商号',
  vendor_name STRING COMMENT '供应商名称',
  logistics_mode STRING COMMENT '物流模式',
  logistics_mode_name STRING COMMENT ' 物流模式名称'
)COMMENT 'DC销售与结余库存'
PARTITIONED BY (
  sdt STRING COMMENT '日期分区'
)
 
STORED AS PARQUET
LOCATION 'hdfs://nameservice1/user/hive/warehouse/csx_dw.db/dc_sale_inventory'

