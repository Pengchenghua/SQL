-- 1.0 战区月销售跟踪
drop table csx_tmp.ads_sale_r_d_zone_sales_fr;
create table `csx_tmp.ads_sale_r_d_zone_sales_fr` (
    level_id string comment '展示层级，1 明细、2 渠道汇总、3总计',
    sales_months string comment '销售月份',
    `zone_id` string comment '战区编码',
    `zone_name` string comment '战区名称',
    `channel_code` string comment '渠道编码',
    `channel_name` string comment '渠道',
    `province_code` string comment '省区编码',
    `province_name` string comment '省区名称',
    daily_plan_sale decimal(26, 6) comment '昨日计划销售额',
    `daily_sales_value` decimal(26, 6) comment '昨日销售额',
    daily_sale_fill_rate decimal(26, 6) comment '日销售达成率',
    `last_week_daily_sales` decimal(26, 6) comment '上周同日销售额 当前日期-7',
    `daily_sale_growth_rate` decimal(26, 6) comment '昨日环比增长率',
    daily_plan_profit decimal(26, 6) comment '昨日销售毛利计划',
    `daily_profit` decimal(26, 6) comment '昨日毛利额',
    daily_profit_fill_rate decimal(26, 6) comment '昨日毛利额完成率',
    `daily_profit_rate` decimal(26, 6) comment '昨日毛利率',
    `daily_negative_profit` decimal(26, 6) comment '负毛利金额',
    `daily_often_cust_sale` decimal(26, 6) comment '昨日老客销售额，首次成交未在本月',
    `daily_new_cust_sale` decimal(26, 6) comment '昨日新额销售额，首次成交在本月',
    `daily_sale_cust_num` bigint comment '昨日成交数',
    `month_plan_sale` decimal(26, 6) comment '月至今销售预算',
    month_sale_value decimal(26, 6) comment '月至今销售额',
    month_sale_fill_rate decimal(26, 6) comment '月至今销售达成率',
    `last_month_sale` decimal(26, 6) comment '月环比销售额',
    `mom_sale_growth_rate` decimal(26, 6) comment '月环比增长率',
    month_plan_profit decimal(26, 6) comment '月度毛利计划',
    `month_profit` decimal(26, 6) comment '月毛利额',
    month_profit_fill_rate decimal(26, 6) comment '月度毛利完成率',
    `month_profit_rate` decimal(26, 6) comment '月毛利率',
    `month_negative_profit` decimal(26, 6) comment '负毛利额',
    `month_often_cust_sale` decimal(26, 6) comment '月老客销售额',
    `month_new_cust_sale` decimal(26, 6) comment '新客销售额',
    `month_sale_cust_num` bigint comment '成交数',
    `last_months_daily_sale` decimal(26, 6) comment '上月同日销售额',
    update_time timestamp comment '更新时间戳'
) comment '战区月累计销售跟踪' 
partitioned by(months string comment '按日分区') 
stored as parquet;

drop table  `csx_tmp.ads_sale_r_d_zone_cust_attribute_fr`;
-- 1.1 属性销售
create table `csx_tmp.ads_sale_r_d_zone_cust_attribute_fr`
	(   level_id string comment '展示层级，1 明细、2 总计',
		sales_month string comment '销售月份'                          ,
		`zone_id` string comment '战区编码'                       ,
		`zone_name` string comment '战区名称'                     ,
		`province_code` string comment '省区编码'                 ,
		`province_name` string comment '省区名称'                 ,
		`attribute_code` int comment '属性编码'                 ,
		`attribute` string comment '属性名称'                   ,
	 	 daily_plan_sale            decimal(26,6) comment '昨日计划销售额'  ,
		`daily_sales_value`        decimal(26,6) comment '昨日销售额'    ,
		daily_sale_fill_rate       decimal(26,6) comment '昨日销售达成率'  ,
		`daily_profit`             decimal(26,6) comment '昨日毛利额'    ,
		`daily_profit_rate`        decimal(26,6) comment '昨日毛利率'    ,
		 month_plan_sale            decimal(26,6) comment '月至今销售预算'  ,
		 month_sale                 decimal(26,6) comment '月至今销售额'   ,
		 month_sale_fill_rate       decimal(26,6) comment '月至今销售达成率' ,
		`last_month_sale`          decimal(26,6) comment '月环比销售额'   ,
		`mom_sale_growth_rate`     decimal(26,6) comment '月环比增长率'   ,
		month_plan_profit decimal(26, 6) comment '月度毛利计划',
		`month_profit`             decimal(26,6) comment '月至今毛利额'   ,
		 month_profit_fill_rate decimal(26, 6) comment '月度毛利完成率',
		`month_profit_rate`        decimal(26,6) comment '月至今毛利率'   ,
		`month_sale_cust_num`      bigint comment '月至今成交数'        ,
		`mom_diff_sale_cust`       bigint comment '月至今成交差异数'      ,
		`last_month_profit`        decimal(26,6) comment '环比毛利额'    ,
		`last_month_sale_cust_num` bigint comment '环比数',
		update_time timestamp comment '更新时间'
	)
	comment '属性销售大' 
	partitioned by	(months string comment '按日分区')
	stored as parquet
;


-- 1.2 商超业态销售
drop table  `csx_tmp.ads_sale_r_d_zone_super_type_fr`;
create table `csx_tmp.ads_sale_r_d_zone_super_type_fr`
	(   level_id string comment '汇总层级:1明细、2 加工类型小计、3 省区汇总、4 大区汇总',
		sales_month string comment '销售月份'     ,                                       
		`zone_id` string comment '战区编码' ,
		`zone_name` string comment '战区名称',
		process_type_code string comment '加工类型：1 非代加工、2 代加工',
		process_type string comment '加工类型名称',
		province_code string comment '省区编码'  ,
		province_name string comment '省区名称' ,		
		format_type_code string comment '业态编码' ,
		format_type string comment '业态名称'        ,
		daily_plan_sale        decimal(26,6) comment '昨日计划销售额'    ,
		`daily_sale_value`     decimal(26,6) comment '昨日销售额'      ,
		daily_sale_fill_rate   decimal(26,6) comment '昨日销售达成率'    ,
		`daily_profit`         decimal(26,6) comment '昨日毛利额'      ,
		`daily_profit_rate`    decimal(26,6) comment '昨日毛利率'      ,
		month_plan_sale        decimal(26,6) comment '月至今销售预算'    ,
		month_sale             decimal(26,6) comment '月至今销售额'     ,
		month_sale_fill_rate   decimal(26,6) comment '月至今销售达成率'   ,
		`last_month_sale`      decimal(26,6) comment '月环比销售额'     ,
		`mom_sale_growth_rate` decimal(26,6) comment '月环比增长率'     ,
		month_plan_profit decimal(26, 6) comment '月度毛利计划',
		`month_profit`         decimal(26,6) comment '月至今毛利额'     ,
		month_profit_fill_rate decimal(26, 6) comment '月度毛利完成率',
		`month_profit_rate`    decimal(26,6) comment '月至今毛利率'     ,
		`last_month_profit`    decimal(26,6) comment '环比毛利额'      ,
		update_time timestamp comment '更新时间'
	)
	comment '商超业态销售' 
	partitioned by(months string comment '按月分区')
	stored as parquet
;


-- 1.3 大区部类销售
drop table  `csx_tmp.ads_sale_r_d_zone_catg_sales_fr`;
create table `csx_tmp.ads_sale_r_d_zone_catg_sales_fr`
	(   level_id string comment '汇总层级:1明细、1 大区汇总',
		sales_month string comment '销售月份'     ,                                       
		`zone_id` string comment '战区编码' ,
		`zone_name` string comment '战区名称',
		channel_code string comment '渠道编码',
		channel string comment '渠道名称',
		division_code string comment '部类编码'  ,
		division_name string comment '部类名称' ,		
		department_code string comment '课组编码' ,
		department_name string comment '课组名称'        ,
		daily_plan_sale        decimal(26,6) comment '昨日计划销售额'    ,
		`daily_sale_value`     decimal(26,6) comment '昨日销售额'      ,
		daily_sale_fill_rate   decimal(26,6) comment '昨日销售达成率'    ,
		`daily_profit`         decimal(26,6) comment '昨日毛利额'      ,
		`daily_profit_rate`    decimal(26,6) comment '昨日毛利率'      ,
		month_plan_sale        decimal(26,6) comment '月至今销售预算'    ,
		month_sale             decimal(26,6) comment '月至今销售额'     ,
		month_sale_fill_rate   decimal(26,6) comment '月至今销售达成率'   ,
		`last_month_sale`      decimal(26,6) comment '月环比销售额'     ,
		`mom_sale_growth_rate` decimal(26,6) comment '月环比增长率'     ,
		month_plan_profit decimal(26, 6) comment '月度毛利计划',
		`month_profit`         decimal(26,6) comment '月至今毛利额'     ,
		month_profit_fill_rate decimal(26, 6) comment '月度毛利完成率',
		`month_profit_rate`    decimal(26,6) comment '月至今毛利率'     ,
		month_sales_sku bigint comment '销售SKU数',
		`month_sale_cust_num` bigint comment '课组成交数',		
		cust_penetration_rate decimal(26,6) comment '渗透率'      ,
		all_sale_cust_num bigint comment '合计数',
		row_num bigint comment '行数',
		update_time timestamp comment '更新时间'
	)
	comment '大大区部类、课组销售与渗透率' 
	partitioned by(months string comment '按月分区')
	stored as parquet
;




-- 1.4 省区课组销售——日配单渗透率
drop table  `csx_tmp.ads_sale_r_d_zone_province_dept_fr`;
create table `csx_tmp.ads_sale_r_d_zone_province_dept_fr`
	(   level_id string comment '汇总层级:1明细、1 大区汇总',
		sales_month string comment '销售月份'     ,                                       
		`zone_id` string comment '战区编码' ,
		`zone_name` string comment '战区名称',
		province_code string comment '省区编码',
		province_name string comment '省区名称',
		attribute_code string comment '属性编码：1、日配单，2、福利订单(WELFARE)，贸易(3)、合伙人(5)、BBC (7)',
		attribute_name string comment '属性名称',
		business_division_code string comment '采购部编码',
		business_division_name string comment '采购部名称',		
		division_code string comment '部类编码'  ,
		division_name string comment '部类名称' ,		
		department_code string comment '课组编码' ,
		department_name string comment '课组名称'        ,
		dail_plan_sale decimal(26,6) comment '昨日销售额'      ,
		`daily_sale_value`     decimal(26,6) comment '昨日销售额'      ,
		daily_sale_fill_rate   decimal(26,6) comment '昨日销售达成率'    ,
		`daily_profit`         decimal(26,6) comment '昨日毛利额'      ,
		`daily_profit_rate`    decimal(26,6) comment '昨日毛利率'      ,
		month_plan_sale        decimal(26,6) comment '月至今销售预算'    ,
		month_sale             decimal(26,6) comment '月至今销售额'     ,
		month_sale_fill_rate   decimal(26,6) comment '月至今销售达成率'   ,
		`last_month_sale`      decimal(26,6) comment '月环比销售额'     ,
		`mom_sale_growth_rate` decimal(26,6) comment '月环比增长率'     ,
		month_sale_ratio decimal(26,6) comment '月销售占比',
		month_avg_cust_sale decimal(26,6) comment '月客均销售额 销售额/数',
		month_plan_profit decimal(26, 6) comment '月度毛利计划',
		`month_profit`         decimal(26,6) comment '月至今毛利额'     ,
		month_profit_fill_rate decimal(26, 6) comment '月度毛利完成率',
		`month_profit_rate`    decimal(26,6) comment '月至今毛利率'     ,
		month_sales_sku bigint comment '销售SKU数',
		`month_sale_cust_num` bigint comment '课组成交数',
		cust_penetration_rate decimal(26,6) comment '渗透率'      ,
		all_sale_cust_num bigint comment '合计数',
		row_num bigint comment '行数',
		update_time timestamp comment '更新时间'
	)
	comment '省区课组日配销售表' 
	partitioned by(months string comment '按月分区')
	stored as parquet
;


-- 1.5 销售主管表
drop table  `csx_tmp.ads_sale_r_d_zone_supervisor_fr`;
create table `csx_tmp.ads_sale_r_d_zone_supervisor_fr`
	(   level_id string comment '汇总层级:1明细、2、省区汇总 1 大区汇总',
		sales_month string comment '销售月份'     ,                                       
		`zone_id` string comment '战区编码' ,
		`zone_name` string comment '战区名称',
		province_code string comment '省区编码',
		province_name string comment '省区名称',
		supervisor_no string comment '主管工号',
		supervisor_name string comment '主管名称',
		 daily_plan_sale decimal(26,6) comment '昨日销售计划'      ,
		`daily_sale_value`     decimal(26,6) comment '昨日销售额'      ,
		 daily_sale_fill_rate   decimal(26,6) comment '昨日销售达成率'    ,
		`daily_profit`         decimal(26,6) comment '昨日毛利额'      ,
		`daily_profit_rate`    decimal(26,6) comment '昨日毛利率'      ,
		month_plan_sale        decimal(26,6) comment '月至今销售预算'    ,
		month_sale             decimal(26,6) comment '月至今销售额'     ,
		month_sale_fill_rate   decimal(26,6) comment '月至今销售达成率'   ,
		`last_month_sale`      decimal(26,6) comment '月环比销售额'     ,
		`mom_sale_growth_rate` decimal(26,6) comment '月环比增长率'     ,
		month_sale_ratio decimal(26,6) comment '月销售占比',
		month_avg_cust_sale decimal(26,6) comment '月客均销售额 销售额/数',
		month_plan_profit decimal(26, 6) comment '月度毛利计划',
		`month_profit`         decimal(26,6) comment '月至今毛利额'     ,
		month_profit_fill_rate decimal(26, 6) comment '月度毛利完成率',
		`month_profit_rate`    decimal(26,6) comment '月至今毛利率'     ,
		month_sales_sku bigint comment '销售SKU数',
		`month_sale_cust_num` bigint comment '课组成交数',
		cust_penetration_rate decimal(26,6) comment '渗透率'      ,
		all_sale_cust_num bigint comment '合计数',
		row_num bigint comment '行数',
		update_time timestamp comment '更新时间'
	)
	comment '省区课组日配销售表' 
	partitioned by(months string comment '按月分区')
	stored as parquet
;


-- 1.4 省区课组销售——日配单渗透率
drop table  `csx_tmp.ads_sale_r_d_zone_province_dept_fr`;
create table `csx_tmp.ads_sale_r_d_zone_province_dept_fr`
	(   level_id string comment '汇总层级:1明细、1 大区汇总',
		sales_month string comment '销售月份'     ,                                       
		`zone_id` string comment '战区编码' ,
		`zone_name` string comment '战区名称',
		province_code string comment '省区编码',
		province_name string comment '省区名称',
		channel string COMMENT '渠道编码',
		channel_name string comment '渠道名称',
		attribute_code string comment '属性编码：1、日配单，2、福利订单(WELFARE)，贸易(3)、合伙人(5)、BBC (7)',
		attribute_name string comment '属性名称',
		business_division_code string comment '采购部编码',
		business_division_name string comment '采购部名称',		
		division_code string comment '部类编码'  ,
		division_name string comment '部类名称' ,		
		department_code string comment '课组编码' ,
		department_name string comment '课组名称'        ,
		dail_plan_sale decimal(26,6) comment '昨日销售额'      ,
		`daily_sale_value`     decimal(26,6) comment '昨日销售额'      ,
		daily_sale_fill_rate   decimal(26,6) comment '昨日销售达成率'    ,
		`daily_profit`         decimal(26,6) comment '昨日毛利额'      ,
		`daily_profit_rate`    decimal(26,6) comment '昨日毛利率'      ,
		month_plan_sale        decimal(26,6) comment '月至今销售预算'    ,
		month_sale             decimal(26,6) comment '月至今销售额'     ,
		month_sale_fill_rate   decimal(26,6) comment '月至今销售达成率'   ,
		`last_month_sale`      decimal(26,6) comment '月环比销售额'     ,
		`mom_sale_growth_rate` decimal(26,6) comment '月环比增长率'     ,
		month_sale_ratio decimal(26,6) comment '月销售占比',
		month_avg_cust_sale decimal(26,6) comment '月客均销售额 销售额/数',
		month_plan_profit decimal(26, 6) comment '月度毛利计划',
		`month_profit`         decimal(26,6) comment '月至今毛利额'     ,
		month_profit_fill_rate decimal(26, 6) comment '月度毛利完成率',
		`month_profit_rate`    decimal(26,6) comment '月至今毛利率'     ,
		month_sales_sku bigint comment '销售SKU数',
		`month_sale_cust_num` bigint comment '课组成交数',
		cust_penetration_rate decimal(26,6) comment '渗透率'      ,
		all_sale_cust_num bigint comment '合计数',
		row_num bigint comment '行数',
		update_time timestamp comment '更新时间'
	)
	comment '省区课组日配销售表' 
	partitioned by(months string comment '按月分区')
	stored as parquet
;
