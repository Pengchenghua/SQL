 select distinct  sdt from
            data_center_report.account_age_dtl_fct_new a ;

ALTER TABLE data_analysis_prd.dws_csms_province_day_sale_plan MODIFY COLUMN id bigint(20) auto_increment NOT NULL;
        
        
select *from data_center_report.fixation_report_sale_data_center_plat_sale_gather;
select * from data_analysis_prd.dws_csms_province_day_sale_plan dcpdsp  ;
-- alter table dws_daily_sales_plan modify months varchar(10) after sdt;
-- truncate dws_ssms_province_month_sale_plan;

select * from data_analysis_prd.dws_csms_manager_month_sale_plan dcmmsp where sdt='202011' and channel ='1';
-- delete from  csx_data_market.dws_csms_manager_month_sale_plan dcmmsp where id >='735'or province_code='';
select * from data_analysis_prd.dws_csms_province_day_sale_plan dcpdsp where sale_months ='202008';
-- delete from  data_analysis_prd.dws_csms_province_month_sale_plan where customer_attribute_name like '%云%';
select * from data_analysis_prd.dws_csms_province_month_sale_plan  where sdt ='202011';
select * from dws_ssms_province_month_sale_plan dspmsp WHERE sdt='202011';

-- delete from  dws_csms_province_day_sale_plan where  sale_months ='202010';

-- 大计划
select * from data_analysis_prd.dws_csms_province_month_sale_plan  where sdt ='202102';
UPDATE data_analysis_prd.dws_csms_province_month_sale_plan set customer_attribute_name='福利业务'  where customer_attribute_name ='福利单业务' and sdt='202101';

-- 主管计划
select * from data_analysis_prd.dws_csms_manager_month_sale_plan dcmmsp where sdt='202102' ;
-- 商超计划
select * from dws_ssms_province_month_sale_plan dspmsp WHERE sdt='202102';
-- 日配每日销售
select * from dws_csms_province_day_sale_plan where sale_months ='202012';


select * from data_analysis_prd.dws_ssms_province_month_sale_plan dspmsp  where sdt ='202012';
select * from dws_all_format_plan_sales;


select * from fine_conf_entity fce ;
select * from FINE_RECORD_EXECUTE ;

drop table FINE_RECORD_EXECUTE;
update from data_analysis_prd.dws_csms_province_month_sale_plan SET province_code=trim(province_code) ;

--  Auto-generated SQL script. Actual values for binary/complex data types may differ - what you see is the default string representation of values.
UPDATE data_analysis_prd.dws_csms_province_day_sale_plan
    SET months=date_format(sdt,'%Y%m') 
    ;


   
   

select province_code ,province_name,sum(plan_sale_value),sum(plan_profit) from data_analysis_prd.dws_all_format_plan_sales 
group by province_code ,province_name;

select date_format(sdt,'%Y%m%d'),sale_months months from  data_analysis_prd.dws_csms_province_day_sale_plan;


 select
        province_code,
        0 sales_value,
        0 profit,
        0 last_sale,
        0 last_profit,
        sum(plan_sale_value)plan_sale_value,
        sum(plan_profit)    plan_profit
    from dws_all_format_plan_sales 
    where `sdt` BETWEEN '202007' and '202009'
    and province_code ='35'
    GROUP BY province_code;
   
  select sum(plan_sale_value),sum(plan_profit),sum(plan_no_tax_sale),sum(plan_no_tax_profit) from dws_all_format_plan_sales where sdt='202010' and sdt<='202012' ;
-- delete   from dws_all_format_plan_sales where sdt>='202010';

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
  `customer_attribute_code` varchar(8) DEFAULT NULL COMMENT '属性编码',
  `customer_attribute_name` varchar(64) DEFAULT NULL COMMENT '属性名称',
  `customer_age_code` varchar(8) DEFAULT NULL COMMENT '客龄编码',
  `customer_age_name` varchar(8) DEFAULT NULL COMMENT '客龄名称：老客 1、 2新客',
  `customer_count` int(11) DEFAULT '0' COMMENT '数',
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
drop table  data_analysis_prd.dws_csms_province_day_sale_plan_tmp ;
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
COMMENT '大省区日配销售预算';

drop table  data_analysis_prd.dws_ssms_province_month_sale_plan_tmp ;
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
select * from dws_csms_province_month_sale_plan;
drop table  data_analysis_prd.dws_csms_province_month_sale_plan_tmp;
CREATE TABLE data_analysis_prd.dws_csms_province_month_sale_plan(
   `id` bigint  not null COMMENT '主键', 
  `sdt`  varchar(64) not null  COMMENT '日期', 
  weeknum varchar(64) default null comment '周',
  `province_code`  varchar(8)  not null COMMENT '省区编码', 
  `province_name`  varchar(64) not null COMMENT '省区名称',
  city_group_code varchar(64)  DEFAULT null COMMENT '城市组', 
  city_group_name varchar(64)  DEFAULT null COMMENT '城市组',
  `customer_attribute_code` varchar(64)  DEFAULT null COMMENT '属性编码', 
  `customer_attribute_name` varchar(64)  DEFAULT null COMMENT '属性名称', 
  `plan_sales_value` decimal(19,6)  DEFAULT '0.000000' COMMENT '计划销售额', 
  `plan_profit` decimal(19,6)  DEFAULT '0.000000' COMMENT '计划毛利', 
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '创建者',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(64) NOT NULl DEFAULT 'sys' COMMENT '更新者',
--   month varchar(64) not null comment'月份',
  primary key(id)
  )
COMMENT '大省区月销售预算'
;
truncate table data_analysis_prd.dws_ssms_province_month_sale_plan;
truncate table data_analysis_prd.dws_csms_province_month_sale_plan;
truncate  table  dws_csms_province_day_sale_plan;
drop table data_analysis_prd.dws_csms_province_day_sale_plan_tmp ;
-- select * from  dws_csms_province_day_sale_plan_tmp;
drop table dws_csms_province_day_sale_plan;
CREATE TABLE data_analysis_prd.dws_csms_province_day_sale_plan (
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
  sale_months  varchar(64) not null  COMMENT '销售月份', 
  primary key(id)
  )
COMMENT '大省区日配销售预算'
;



drop table ads_fr_sale_r_d_year_plan;
create table ads_fr_sale_r_d_year_plan
(
id BIGINT not null auto_increment comment '主键',
year varchar(4) comment '年度',	
quarter varchar(6) comment '季度',	
month  varchar(6) comment '月度',	
region_code  varchar(6) comment '大区编码',		
region_name varchar(64) comment '大区名称',		
province_code varchar(6) comment '省区编码',	
province_name varchar(64) comment '省区名称',	
city_group_code  varchar(6) comment '城市编码',		
city_group_name  varchar(64) comment '城市名称',
all_sale_plan  decimal(19,6) comment '全渠道销售计划',		
b_sale_plan decimal(19,6) comment 'B端销售计划：自营计划+合伙计划+BBC计划',	
b_self_sale_plan decimal(19,6) comment 'B端自营计划',	
parter_sale_plan decimal(19,6) comment '合伙人销售计划',	
bbc_sale_plan decimal(19,6) comment 'BBC销售计划',
m_sale_plan  decimal(19,6) comment 'M端销售计划：M加工+M端代加工',	
m_self_sale_plan decimal(19,6) comment 'M端自营销售计划',	
m_oem_sale_plan decimal(19,6) comment 'M端代加工销售计划',	
all_profit_plan	  decimal(19,6) comment '全渠道毛利额计划',
b_profit_plan   decimal(19,6) comment 'B端销售计划：自营计划+合伙计划+BBC计划',
b_self_profit_plan   decimal(19,6) comment 'B端自营计划',	
parter_profit_plan   decimal(19,6) comment '合伙人销售计划',	
bbc_profit_plan   decimal(19,6) comment 'BBC销售计划',	
m_profit_plan   decimal(19,6) comment 'M端销售计划：M加工+M端代加工',	
m_self_profit_plan   decimal(19,6) comment 'M端自营销售计划',	
m_oem_profit_plan   decimal(19,6) comment 'M端代加工销售计划',
tmp_01   decimal(19,6) comment '预留字段',	
tmp_02   decimal(19,6) comment '预留字段',	
tmp_03   decimal(19,6) comment '预留字段',
 create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 create_by varchar(64) NOT NULl DEFAULT 'sys' COMMENT '创建者',
 update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
 update_by varchar(64) NOT NULl DEFAULT 'sys' COMMENT '更新者',
primary key(id)
)comment '年度销售计划'
;

