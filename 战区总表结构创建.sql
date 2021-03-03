-- 1.0 战区月销售跟踪
create table csx_tmp.ads_sale_r_d_zone_sales_fr (
	months string comment '销售月份',
	channel_name string comment '渠道',
	province_code string comment '省区编码',
	province_name string comment '省区名称',
	zone_id string comment '战区编码',
	zone_name string comment '战区名称',
	daily_plan_sale decimal(26, 6) comment '昨日计划销售额',
	daily_sales_value decimal(26, 6) comment '昨日销售额',
	daily_sale_fill_rate decimal(26, 6) comment '月至今销售达成率',
	last_week_daily_sales decimal(26, 6) comment '上周同日销售额',
	daily_sale_growth_rate decimal(26, 6) comment '日环比增长率',
	daily_profit decimal(26, 6) comment '昨日毛利额',
	daily_profit_rate decimal(26, 6) comment '昨日毛利率',
	daily_negative_profit decimal(26, 6) comment '负毛利金额',
	daily_often_cust_sale decimal(26, 6) comment '昨日老客销售额，首次成交未在本月',
	daily_new_cust_sale decimal(26, 6) comment '昨日新额销售额，首次成交在本月',
	daily_sale_cust_num bigint comment '昨日成交客户数',
	month_plan_value decimal(26, 6) comment '月至今销售预算',
	month_sale_fill_rate decimal(26, 6) comment '月至今销售达成率',
	month_sale decimal(26, 6) comment '月至今销售额',
	last_month_sale decimal(26, 6) comment '月环比销售额',
	mom_sale_growth_rate decimal(26, 6) comment '月环比增长率',
	month_profit decimal(26, 6) comment '月毛利额',
	month_profit_rate decimal(26, 6) comment '月毛利率',
	month_negative_profit decimal(26, 6) comment '负毛利额',
	month_often_cust_sale decimal(26, 6) comment '月老客销售额',
	month_new_cust_sale decimal(26, 6) comment '新客销售额',
	month_sale_cust_num bigint comment '成交客户数',
	last_months_daily_sale decimal(26, 6) comment '上月日销售额'
) comment '战区月销售跟踪' 
partitioned by(sdt string comment '按日分区') 
stored as parquet;



-- 1.1 客户属性销售
create table csx_tmp.ads_sale_r_d_zone_cust_attribute_fr (
	months string comment '销售月份',
	zone_id string comment '战区编码',
	zone_name string comment '战区名称',
	province_code string comment '省区编码',
	province_name string comment '省区名称',
	attribute_code int comment '客户属性编码',
	attribute string comment '客户属性名称',
	daily_plan_sale decimal(26, 6) comment '昨日计划销售额',
	daily_sales_value decimal(26, 6) comment '昨日销售额',
	daily_sale_fill_rate decimal(26, 6) comment '昨日销售达成率',
	daily_profit decimal(26, 6) comment '昨日毛利额',
	daily_profit_rate decimal(26, 6) comment '昨日毛利率',
	month_plan_sale decimal(26, 6) comment '月至今销售预算',
	month_sale decimal(26, 6) comment '月至今销售额',
	month_sale_fill_rate decimal(26, 6) comment '月至今销售达成率',
	last_month_sale decimal(26, 6) comment '月环比销售额',
	mom_sale_growth_rate decimal(26, 6) comment '月环比增长率',
	month_profit decimal(26, 6) comment '月至今毛利额',
	month_profit_rate decimal(26, 6) comment '月至今毛利率',
	month_sale_cust_num bigint comment '月至今成交客户数',
	mom_diff_sale_cust bigint comment '月至今成交客户差异数',
	last_month_profit decimal(26, 6) comment '环比毛利额',
	last_month_sale_cust_num bigint comment '环比客户数'
) comment '客户属性销售' 
partitioned by (sdt string comment '按日分区') 
stored as parquet;


-- 1.2 商超业态销售
create table csx_tmp.ads_sale_r_d_zone_super_type_fr (
	months string comment '销售月份',
	zone_id string comment '战区编码' zone_name string comment '战区名称',
	province_code string comment '省区编码',
	province_name string comment '省区名称' super_type_code string comment '业态编码',
	super_type string comment '业态名称',
	daily_plan_sale decimal(26, 6) comment '昨日计划销售额',
	daily_sale_value decimal(26, 6) comment '昨日销售额',
	daily_sale_fill_rate decimal(26, 6) comment '昨日销售达成率',
	daily_profit decimal(26, 6) comment '昨日毛利额',
	daily_profit_rate decimal(26, 6) comment '昨日毛利率',
	month_plan_sale decimal(26, 6) comment '月至今销售预算',
	month_sale decimal(26, 6) comment '月至今销售额',
	month_sale_fill_rate decimal(26, 6) comment '月至今销售达成率',
	last_month_sale decimal(26, 6) comment '月环比销售额',
	mom_sale_growth_rate decimal(26, 6) comment '月环比增长率',
	month_profit decimal(26, 6) comment '月至今毛利额',
	month_profit_rate decimal(26, 6) comment '月至今毛利率',
	last_month_profit decimal(26, 6) comment '环比毛利额',
) comment '商超业态销售' 
	partitioned by(sdt string comment '按日分区')
	stored as parquet;


-- 1.3 大区部类销售
create table csx_tmp.ads_sale_r_d_zone_dept_fr 
(
	months string comment '销售月份',
	zone_id string comment '战区编码' zone_name string comment '战区名称',
	type_id string comment '课组识别,0 采购部 1 部类 2 课组',
	department_code string comment '课组,0 采购部 1 部类 2 课组',
	department_name string comment '课组名称,0 采购部 1 部类 2 课组',
	daily_plan_sale decimal(26, 6) comment '昨日计划销售额',
	daily_sale_value decimal(26, 6) comment '昨日销售额',
	daily_sale_fill_rate decimal(26, 6) comment '昨日销售达成率',
	daily_profit decimal(26, 6) comment '昨日毛利额',
	daily_profit_rate decimal(26, 6) comment '昨日毛利率',
	month_plan_sale decimal(26, 6) comment '月至今销售预算',
	month_sale decimal(26, 6) comment '月至今销售额',
	month_sale_fill_rate decimal(26, 6) comment '月至今销售达成率',
	last_month_sale decimal(26, 6) comment '月环比销售额',
	mom_sale_growth_rate decimal(26, 6) comment '月环比增长率',
	month_profit decimal(26, 6) comment '月至今毛利额',
	month_profit_rate decimal(26, 6) comment '月至今毛利率',
	last_month_profit decimal(26, 6) comment '环比毛利额',
	month_sale_sku bigint comment '月销售SKU',
	month_sale_cust_num bigint comment '月成交客户数',
	penetration_rate bigint comment '渗透率= month_sale_cust_num/ month_all_sale_cust ',
	month_all_sale_cust	bigint comment '大区成交客户总数'
	rank_sale bigint comment '月销售排名,根据type_id 排名'
)comment '大区课组\部类\采购部销售'
partitioned by (sdt string comment '日期分区')
stored as parquet
;

-- 1.4 大区/省区课组销售占比

create table csx_tmp.ads_sale_r_d_zone_province_dept_fr
(
	zone_id	  string comment '战区编码'	,
    zone_name string comment '战区名称',
    province_code string comment '省区编码',
    province_name string comment '省区名称',
    division_code string comment '部类编码',
    division_name string comment '部类名称',
    department_code string comment '课组编码',
    department_name string comment '课组名称',
    daily_plan_sale decimal(26, 6) comment '昨日计划销售额',
	daily_sale_value decimal(26, 6) comment '昨日销售额',
	daily_sale_fill_rate decimal(26, 6) comment '昨日销售达成率',
	daily_profit decimal(26, 6) comment '昨日毛利额',
	daily_profit_rate decimal(26, 6) comment '昨日毛利率',
	month_plan_sale decimal(26, 6) comment '月至今销售预算',
	month_sale decimal(26, 6) comment '月至今销售额',
	month_sale_fill_rate decimal(26, 6) comment '月至今销售达成率',
	last_month_sale decimal(26, 6) comment '月环比销售额',
	mom_sale_growth_rate decimal(26, 6) comment '月环比增长率',
	month_profit decimal(26, 6) comment '月至今毛利额',
	month_profit_rate decimal(26, 6) comment '月至今毛利率',
	last_month_profit decimal(26, 6) comment '环比毛利额',
	month_sale_sku bigint comment '月销售SKU',
	month_sale_cust_num bigint comment '月成交客户数',
	penetration_rate bigint comment '渗透率= month_sale_cust_num/ month_all_sale_cust ',
	month_all_sale_cust	bigint comment '大区成交客户总数'
	rank_sale bigint comment '月销售排名,根据type_id 排名'
)comment '省区课组销售占比及渗透率'
partitioned by (sdt string comment '日期分区')


