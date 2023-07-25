CREATE TABLE `csx_tmp.dws_csms_province_month_sale_plan_tmp` (
  `id` bigint COMMENT '主键',
  `sdt` string COMMENT '日期',
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区名称',
  `customer_attribute_code` string COMMENT '属性编码',
  `customer_attribute_name` string COMMENT '属性名称',
  `plan_sales_value` decimal(19, 6) COMMENT '计划销售额',
  `plan_profit` decimal(19, 6) COMMENT '计划毛利',
  `create_time` string COMMENT '创建时间',
  `create_by` string COMMENT '创建者',
  `update_time` string COMMENT '更新时间',
  `update_by` string COMMENT '更新者'
) COMMENT '大省区月销售预算'
partitioned by (month string)  ;

CREATE TABLE `csx_tmp.dws_csms_province_day_sale_plan_tmp` (
  `id` bigint COMMENT '主键',
  `sdt` string COMMENT '日期',
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区名称',
  `plan_sales_value` decimal(19, 6) COMMENT '计划销售额',
  `plan_profit` decimal(19, 6) COMMENT '计划毛利',
  `create_time` string COMMENT '创建时间',
  `create_by` string COMMENT '创建者',
  `update_time` string COMMENT '更新时间',
  `update_by` string COMMENT '更新者'
) COMMENT '大省区日配销售预算'
partitioned by (month string)  ;

CREATE TABLE `csx_tmp.dws_ssms_province_month_sale_plan_tmp` (
  `id` bigint COMMENT '主键',
  `sdt` string COMMENT '日期',
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区名称',
  `process_type` string COMMENT '加工类型',
  `format_code` string COMMENT '业态编码',
  `format_name` string COMMENT '业态名称',
  `plan_sales_value` decimal(19, 6) COMMENT '计划销售额',
  `plan_profit` decimal(19, 6) COMMENT '计划毛利',
  `create_time` string COMMENT '创建时间',
  `create_by` string COMMENT '创建者',
  `update_time` string COMMENT '更新时间',
  `update_by` string COMMENT '更新者'
) COMMENT '商超省区月销售预算'
partitioned by (month string);

CREATE TABLE `csx_tmp.dws_csms_manager_month_sale_plan_tmp` (
  `id` bigint COMMENT '主键',
  `sdt` string COMMENT '日期',
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区名称',
  `channel` string COMMENT '渠道',
  `channel_name` string COMMENT '渠道名称',
  `city_manager_job_no` string COMMENT '城市经理工号',
  `city_manager_name` string COMMENT '城市经理名称',
  `manager_job_no` string COMMENT '主管工号',
  `manager_name` string COMMENT '主管名称',
  `customer_attribute_code` string COMMENT '属性编码',
  `customer_attribute_name` string COMMENT '属性名称',
  `customer_age_code` string COMMENT '客龄编码',
  `customer_age_name` string COMMENT '客龄名称',
  `customer_count` int COMMENT '数',
  `plan_sales_value` decimal(19, 6) COMMENT '计划销售额',
  `plan_profit` decimal(19, 6) COMMENT '计划毛利',
  `create_time` string COMMENT '创建时间',
  `create_by` string COMMENT '创建者',
  `update_time` string COMMENT '更新时间',
  `update_by` string COMMENT '更新者'
) COMMENT '企业购主管月销售计划'
partitioned by (month string);

