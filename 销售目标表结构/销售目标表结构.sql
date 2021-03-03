  
 drop table dws_csms_manager_month_sale_plan;

CREATE TABLE dws_csms_manager_month_sale_plan (
  `id` bigint(20) not null COMMENT '主键',
  `sdt` varchar(8) DEFAULT NULL COMMENT '日期',
  `province_code` varchar(8) DEFAULT NULL COMMENT '省区编码',
  `province_name` varchar(64) DEFAULT NULL COMMENT '省区名称',
  `channel` varchar(64) DEFAULT NULL COMMENT '渠道',
  `channel_name` varchar(64) DEFAULT NULL COMMENT '渠道名称',
  `city_group_code` varchar(64) DEFAULT NULL COMMENT '城市编码',
  `city_group_name` varchar(64) DEFAULT NULL COMMENT '城市名称',
  `city_manager_job_no` varchar(64) DEFAULT NULL COMMENT '城市经理工号',
  `city_manager_name` varchar(64) DEFAULT NULL COMMENT '城市经理名称',
  `manager_job_no` varchar(64) DEFAULT NULL COMMENT '主管工号',
  `manager_name` varchar(64) DEFAULT NULL COMMENT '主管名称',
  `customer_attribute_code` varchar(8) DEFAULT NULL COMMENT '客户属性编码',
  `customer_attribute_name` varchar(64) DEFAULT NULL COMMENT '客户属性名称',
  `customer_age_code` varchar(8) DEFAULT NULL COMMENT '客龄编码',
  `customer_age_name` varchar(8) DEFAULT NULL COMMENT '客龄名称：老客 1、 2新客',
  `customer_count` int(11) DEFAULT '0' COMMENT '客户数',
  `plan_sales_value` decimal(19,6) DEFAULT '0.000000' COMMENT '计划销售额',
  `plan_profit` decimal(19,6) DEFAULT '0.000000' COMMENT '计划毛利',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '创建者',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '更新者',
 --  month varchar(64) not null comment'月份',
   PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='企业购主管月销售计划'
;

drop table data_analysis_prd.dws_csms_province_day_sale_plan;
CREATE TABLE data_analysis_prd.dws_csms_province_day_sale_plan(
  `id` bigint  not null COMMENT '主键', 
  `sdt`  varchar(8) not null  COMMENT '日期', 
  `province_code`  varchar(8)  not null COMMENT '省区编码', 
  `province_name`  varchar(64) not null COMMENT '省区名称', 
  `plan_sales_value` decimal(19,6)  DEFAULT '0.000000' COMMENT '计划销售额', 
  `plan_profit` decimal(19,6)  DEFAULT '0.000000' COMMENT '计划毛利', 
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '创建者',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '更新者',
  `sale_months`  varchar(8)  not null COMMENT '月份',
  -- month varchar(64) not null comment'月份',
   PRIMARY KEY (`id`)
)
COMMENT '大客户省区日配销售预算';

drop table data_analysis_prd.dws_ssms_province_month_sale_plan;
CREATE TABLE data_analysis_prd.dws_ssms_province_month_sale_plan(
  `id` bigint  not null COMMENT '主键', 
  `sdt`  varchar(64) not null  COMMENT '日期', 
  weeknum varchar(64) default null comment '周',
  `province_code`  varchar(8)  not null COMMENT '省区编码', 
  `province_name`  varchar(64) not null COMMENT '省区名称',
  city_group_code varchar(64)  DEFAULT null COMMENT '城市组', 
  city_group_name varchar(64)  DEFAULT null COMMENT '城市组',
  `process_type` varchar(64) default null  COMMENT '加工类型', 
  `format_code` varchar(64)  not null COMMENT '业态编码', 
  `format_name` varchar(64)  not null COMMENT '业态名称', 
  `plan_sales_value` decimal(19,6) DEFAULT '0.000000' COMMENT '计划销售额', 
  `plan_profit` decimal(19,6) DEFAULT '0.000000' COMMENT '计划毛利', 
   `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '创建者',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '更新者',
  -- month varchar(64) not null comment'月份',
  primary key (id)
  )
COMMENT '商超省区月销售预算'
;
drop table data_analysis_prd.dws_csms_province_month_sale_plan;
CREATE TABLE data_analysis_prd.dws_csms_province_month_sale_plan(
   `id` bigint  not null COMMENT '主键', 
  `sdt`  varchar(64) not null  COMMENT '日期', 
  weeknum varchar(64) default null comment '周',
  `province_code`  varchar(8)  not null COMMENT '省区编码', 
  `province_name`  varchar(64) not null COMMENT '省区名称',
  city_group_code varchar(64)  DEFAULT null COMMENT '城市组', 
  city_group_name varchar(64)  DEFAULT null COMMENT '城市组',
  `customer_attribute_code` varchar(64)  DEFAULT null COMMENT '客户属性编码', 
  `customer_attribute_name` varchar(64)  DEFAULT null COMMENT '客户属性名称', 
  `plan_sales_value` decimal(19,6)  DEFAULT '0.000000' COMMENT '计划销售额', 
  `plan_profit` decimal(19,6)  DEFAULT '0.000000' COMMENT '计划毛利', 
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '创建者',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '更新者',
--   month varchar(64) not null comment'月份',
  primary key(id)
  )
COMMENT '大客户省区月销售预算'
;


drop table dws_csms_province_day_sale_plan;
select * from  dws_csms_province_day_sale_plan;
drop table dws_csms_province_day_sale_plan;
CREATE TABLE data_analysis_prd.dws_csms_province_day_sale_plan(
  `id` bigint  not null COMMENT '主键', 
  `sdt`  varchar(64) not null  COMMENT '日期', 
  `province_code`  varchar(8)  not null COMMENT '省区编码', 
  `province_name`  varchar(64) not null COMMENT '省区名称',
  city_group_code varchar(64)  DEFAULT null COMMENT '城市组', 
  city_group_name varchar(64)  DEFAULT null COMMENT '城市组',
  `plan_sales_value` decimal(19,6) COMMENT '计划销售额', 
  `plan_profit` decimal(19,6) COMMENT '计划毛利', 
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '创建者',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '更新者',
 -- month  varchar(64) not null  COMMENT '销售月份', 
  primary key(id)
  )
COMMENT '大客户省区日配销售预算'
;

-- 弃用
CREATE TABLE `csx_tmp.dws_daily_sales_plan`(
  `id` string COMMENT '', 
  `plan_type` string COMMENT '计划类型，1 大客户、2 商超', 
  `province_code` string COMMENT '省区', 
  `province_name` string COMMENT '省区', 
  `channel_code` string COMMENT '渠道', 
  `channel_name` string COMMENT '渠道', 
  `plan_sale_value` decimal(26,6) COMMENT '计划额', 
  `plan_profit` decimal(26,6) COMMENT '计划毛利额', 
  `temp_01` string COMMENT '预留字段', 
  `temp_02` string COMMENT '预留字段', 
  `plan_sdt` string COMMENT '每日日期', 
  `months` string COMMENT '月份', 
  `create_time` string COMMENT '创建日期', 
  `create_by` string COMMENT '创建人', 
  `update_time` string COMMENT '更新日期', 
  `update_by` string COMMENT '更新人')
COMMENT '每日预算'
;


