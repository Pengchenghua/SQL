
drop table csx_tmp.ads_fr_r_d_frozen_financial_classify_sales;
CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_financial_classify_sales`(
  `sales_monts` string COMMENT '销售日期', 
  `channel_code` string COMMENT '渠道', 
  `channel_name` string COMMENT '渠道', 
  `business_type_code` string COMMENT '销售业务', 
  `business_type_name` string COMMENT '销售业务', 
  `classify_large_code` string COMMENT '管理一级分类', 
  `classify_large_name` string COMMENT '管理一级分类', 
  `classify_middle_code` string COMMENT '管理二级分类', 
  `classify_middle_name` string COMMENT '管理二级分类', 
  `classify_small_code` string COMMENT '管理三级', 
  `classify_small_name` string COMMENT '管理三级', 
  `sales_cost` decimal(38,6) COMMENT '销售成本含税', 
  `sales_value` decimal(38,6) COMMENT '销售额含税', 
  `profit` decimal(38,6) COMMENT '毛利额含税', 
  `profit_rate` decimal(38,6) COMMENT '毛利率', 
  `no_tax_sales_cost` decimal(38,6) COMMENT '未税成本', 
  `no_tax_sales` decimal(38,6) COMMENT '未税销售', 
  `no_tax_profit` decimal(38,6) COMMENT '未税毛利额', 
  `no_tax_profit_rate` decimal(38,6) COMMENT '未税毛利率', 
  `adj_no_tax_sum_value` decimal(38,6) COMMENT '未税调整成本', 
  `adj_sum_value` decimal(38,6) COMMENT '含税调整成本', 
  `no_tax_rebate_out_value` decimal(38,6) COMMENT '返利支出未税额', 
  `rebate_out_value` decimal(38,6) COMMENT '返利支出含税额', 
  `no_tax_rebate_in_value` decimal(38,6) COMMENT '返利收入未税额', 
  `rebate_in_value` decimal(38,6) COMMENT '返利收入含税额', 
  `no_tax_net_profit` decimal(38,6) COMMENT '净毛利额未税', 
  `net_profit` decimal(38,6) COMMENT '净毛利额含税', 
  `no_tax_net_profit_rate` decimal(38,6) COMMENT '未税净毛利率', 
  `net_profit_rate` decimal(38,6) COMMENT '净毛利率', 
    `purchase_qty` decimal(38,6)COMMENT '采购数量', 
  `purchase_amt` decimal(38,6) COMMENT'采购金额含税', 
  `no_tax_purchase_amt` decimal(38,6) COMMENT '采购金额未税', 
  `update_time` timestamp COMMENT '更新时间')
COMMENT '冻品财报-品类销售收入'
PARTITIONED BY ( 
  `months` string COMMENT '月分区')

STORED AS parquet 

;