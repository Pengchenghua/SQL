set
mapreduce.job.queuename = caishixian;

set
hive.exec.dynamic.partition = true;
-- 开启动态分析
 set
hive.exec.dynamic.partition.mode = nonstrict;
-- 动态分区模式
 set
hive.exec.max.dynamic.partitions.pernode = 10000;
-- 表示每个maper或reducer可以允许创建的最大动态分区个数
-- 财务对帐周期
 drop table if exists b2b_tmp.bill_data;

CREATE TEMPORARY TABLE if not exists b2b_tmp.bill_data as
select
	'P001' payment_terms,
	'票到7天' payment_name
union all
select
	'P002' payment_terms,
	'票到15天' payment_name
union all
select
	'P003' payment_terms,
	'票到20天' payment_name
union all
select
	'P004' payment_terms,
	'票到30天' payment_name
union all
select
	'Y001' payment_terms,
	'月结10天' payment_name
union all
select
	'Y002' payment_terms,
	'月结15天' payment_name
union all
select
	'Y003' payment_terms,
	'月结30天' payment_name
union all
select
	'Y004' payment_terms,
	'月结45天' payment_name
union all
select
	'Y005' payment_terms,
	'月结60天' payment_name
union all
select
	'Y006' payment_terms,
	'月结90天' payment_name
union all
select
	'Z001' payment_terms,
	'双方另议' payment_name
union all
select
	'Z002' payment_terms,
	'票到7天' payment_name
union all
select
	'Z003' payment_terms,
	'票到15天' payment_name
union all
select
	'Z004' payment_terms,
	'票到30天' payment_name
union all
select
	'Z005' payment_terms,
	'票到45天' payment_name
union all
select
	'Z006' payment_terms,
	'票到60天' payment_name
union all
select
	'Z007' payment_terms,
	'预付货款' payment_name
union all
select
	'Z008' payment_terms,
	'票到8天' payment_name ;
-- 子客户数
 DROP TABLE IF EXISTS b2b_tmp.p_cust_info_01;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_cust_info_01 AS
select
	customer_no,
	concat_ws(',',COLLECT_set(agreement_dc_code))as agreement_dc_code ,
	'' as agreement_dc_name ,
	concat_ws(',', collect_set(inventory_dc_code))  as inventory_dc_code ,
	'' as inventory_dc_name ,
	sum(cust_number)cust_number,
	concat_ws(',',COLLECT_set(mark_shop_code)) as mark_shop_code 
from
	(
	select
		'1' note,
		sap_cus_code as customer_no,
		agreement_dc_code ,
		agreement_dc_name ,
		inventory_dc_code ,
		inventory_dc_name ,
		mark_shop_code ,
		COUNT(DISTINCT sap_sub_cus_code )cust_number
	from
		csx_ods.yszx_customer_relation_new_ods
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),	1)),'-','')
	group by
		sap_cus_code,
		agreement_dc_code ,
		agreement_dc_name ,
		inventory_dc_code ,
		inventory_dc_name,
		mark_shop_code
union all
	select
		'2' note,
		sap_cus_code as customer_no,
		agreement_dc_code ,
		agreement_dc_name ,
		inventory_dc_code ,
		inventory_dc_name ,
		mark_shop_code ,
		COUNT(DISTINCT sap_sub_cus_code )cust_number
	from
		csx_ods.yszx_customer_relation_ods
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),		1)),		'-',		'')
		and ( inventory_dc_name not like '%安徽%'
		and inventory_dc_name not like '%北京%')
	group by
		sap_cus_code,
		agreement_dc_code ,
		agreement_dc_name ,
		inventory_dc_code ,
		inventory_dc_name ,
		mark_shop_code)a
group by
	customer_no;
 --  select * from b2b_tmp.p_cust_info 
-- 客户属性表    sys_customer_category_ods
 DROP TABLE IF EXISTS b2b_tmp.p_cust_info;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_cust_info AS
SELECT
	a.id,
	a.customer_number,
	customer_name,
	agreement_dc_code ,
	agreement_dc_name ,
	inventory_dc_code ,
	inventory_dc_name ,
	mark_shop_code,
	attribute_name,
	j.cust_number,
	--子客户数
 channel,
	concat( first_category_code,
	'_',
	first_category )AS fist_category,
	concat( second_category_code,
	'_',
	second_category )AS second_category,
	concat( third_category_code,
	'_',
	third_category )AS third_category,
	archive_category,
	a.customer_type,
	social_credit_code,
	legal_person_name,
	legal_person_id_no,
	contacts,
	invoice_name,
	province_code,
	province_name,
	city_name,
	bank_no,
	bank_name,
	bank_account_name,
	bank_account_no,
	a.company_code,
	f.comp_name,
	a.payment_terms ,
	g.payment_name,
	pay_mode,
	payment_days,
	plan_sales_amount,
	sign_amount,
	taxes_category,
	sales_name,
	sales_id ,
	work_no,
	sales_province,
	sales_city,
	customer_level,
	credit_limit,
	temp_credit_limit,
	temp_begin_time,
	temp_end_time,
	credit_modulus,
	customer_status,
	channel_first_code,
	channel_second_code,
	org_code,
	org_name,
	distribution_channel,
	credit_range,
	contract_begin_time,
	contract_end_time,
	invoice_address,
	creator,
	create_time
FROM
	(
	SELECT
		id,
		customer_no AS customer_number,
		customer_name as customer_name,
		CASE
			when attribute = '1' then '1_日配客户'
			when attribute = '2' then '2_福利客户'
			when attribute = '3' then '3_贸易客户'
			when attribute = '4' then '4_战略客户'
			when attribute = '5' then '5_合伙人客户'
			else attribute
	end attribute_name,
		channel,
		first_category_code,
		first_category,
		second_category_code,
		second_category,
		third_category_code,
		third_category,
		-- 档案分类：01法人，02政府/事业单位，03军队，04其他非法人，05一次性客户
 archive_category,
		-- 合作模式：01长期客户；02临时客户
 customer_type ,
		social_credit_code,
		legal_person_name,
		legal_person_id_no,
		contacts,
		invoice_name,
		bank_no,
		bank_name,
		bank_account_name,
		bank_account_no,
		company_code,
		payment_terms,
		-- 01转账、02现金（刷卡）、03支票、04信控、05账扣
 case
			when pay_mode = '01' THEN '01_转账'
			when pay_mode = '02' THEN '02_现金(刷卡)'
			when pay_mode = '03' THEN '03_支票'
			when pay_mode = '04' THEN '04_信控 '
			when pay_mode = '05' THEN '05_帐扣'
	end pay_mode ,
		-- 账期：01（无账期），02（7天），03（15天），04（30天），05（45天），06（60天），07（90天）
 CASE WHEN payment_days = '01' THEN '01_无账期'
		WHEN payment_days = '02' THEN '02_7天'
		WHEN payment_days = '03' THEN '03_15天'
		WHEN payment_days = '04' THEN '04_30天'
		WHEN payment_days = '05' THEN '05_45天'
		WHEN payment_days = '06' THEN '06_60天'
		WHEN payment_days = '07' THEN '07_90天'
END payment_days,
	plan_sales_amount,
	sign_amount,
	--：0免税、1一般纳税人、2小规模纳税人
 case
		when taxes_category = '0' then '0_免税'
		when taxes_category = '1' then '1_一般纳税人'
		when taxes_category = '2' then '1_小规模纳税人'
		else taxes_category
end taxes_category,
	sales_name,
	sales_id,
	work_no,
	sales_province,
	sales_city,
	province_code,
	province_name,
	city_name,
	CASE WHEN customer_level = 'AA' THEN 'AA优质客户'
	WHEN customer_level = 'A' THEN 'A良好客户'
	WHEN customer_level = 'B' THEN 'B良好客户'
	WHEN customer_level = 'C' THEN 'C一般客户'
	WHEN customer_level = 'D' THEN 'D风险客户'
END customer_level,
credit_limit,
temp_credit_limit,
to_date(temp_begin_time) temp_begin_time ,
to_date(temp_end_time) temp_end_time,
credit_modulus,
CASE WHEN customer_status = '01' THEN '01_待审批'
WHEN customer_status = '02' THEN '02_审批中'
WHEN customer_status = '03' THEN '03_审批拒绝'
WHEN customer_status = '04' THEN '04：审批通过'
END customer_status,
channel_first_code,
channel_second_code,
org_code,
org_name,
distribution_channel,
credit_range,
to_date(contract_begin_time) contract_begin_time,
to_date(contract_end_time) contract_end_time,
invoice_address,
creator,
to_date(create_time) create_time
FROM
csx_dw.customer_m a
WHERE
sdt = regexp_replace( to_date( date_sub( CURRENT_TIMESTAMP(),
1 ) ),
'-',
'' )
and source = 'crm'
and channel != '商超(对内)' ) a
LEFT outer JOIN (
	SELECT
		company_code comp_code,
		company_name comp_name
	FROM
		csx_dw.shop_m
	WHERE
		sdt = 'current'
	GROUP BY
		company_code ,
		company_name) f ON
	a.company_code = f.comp_code
left JOIN b2b_tmp.bill_data g on
	a.payment_terms = g.payment_terms
left join b2b_tmp.p_cust_info_01 j on
	a.customer_number = j.customer_no ;
--  SELECT * from csx_dw.customer_m where sdt='20190826';
--  签约日期与新签约成交日期 无签约日期为空
 DROP TABLE IF EXISTS b2b_tmp.p_sign_info;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_sign_info AS
SELECT
	id,
	customer_no as cust_id,
	regexp_replace(to_date(sign_time),
	'-',
	'') AS sign_date
FROM
	csx_dw.customer_m
WHERE
	sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),
	1)),
	'-',
	'') ;
--	select * from csx_dw.customer_simple_info_v2 WHERE	sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),1)),	'-','');
-- 查询新签约成交时间
 DROP TABLE IF EXISTS b2b_tmp.p_min_sale_sdt;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_min_sale_sdt as
SELECT
	id,
	a.cust_id,
	sign_date,
	min_sale_sdt
FROM
	b2b_tmp.p_sign_info a
left JOIN (
	SELECT
		customer_no cust_id,
		min(sdt) as min_sale_sdt
	from
		csx_dw.sale_goods_m
	GROUP by
		customer_no) b on
	a.cust_id = b.cust_id ;
--  获取拜访最近日期 ,计算未拜访天数、及拜访模式      b2b_tmp.temp_visit 
 DROP TABLE IF EXISTS b2b_tmp.temp_visit ;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.temp_visit AS
select
	a.customer_id,
	a.customer_no,
	a.customer_name,
	a.max_visit_time as max_visit_time,
	visitor as sales_name,
	visitor_id ,
	customer_type,
	DATEDIFF( date_sub( CURRENT_TIMESTAMP(),
	1 ),
	to_date(a.max_visit_time) )AS days_not_visit,
	min_visit_time
from
	(
	SELECT
		customer_id,
		customer_no ,
		customer_name ,
		visitor_id,
		visit_time max_visit_time,
		visitor,
		customer_type
	FROM
		csx_dw.customer_visit_info_m a
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),
		1)),
		'-',
		'' )
	group by
		customer_no ,
		customer_id,
		customer_name ,
		visitor_id,
		visit_time ,
		visitor,
		customer_type )a
join (
	SELECT
		customer_id,
		customer_no,
		customer_name,
		MAX(visit_time)max_visit_time,
		min(visit_time)min_visit_time
	FROM
		csx_dw.customer_visit_info_m
	WHERE
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),
		1)),
		'-',
		'' )
	group by
		customer_id,
		customer_no,
		customer_name )b on
	a.customer_no = b.customer_no
	and a.customer_name = b.customer_name
	and a.max_visit_time = b.max_visit_time ;
-- select * from b2b_tmp.temp_max_visit where customer_name like '%北京贝黎诗美容有限公司亚运村分公司%'
-- 销售最大日期
 DROP TABLE IF EXISTS b2b_tmp.p_sale_dt;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_sale_dt AS
SELECT
	customer_no cust_id,
	sdt,
	SUM(b.sales_value) sales_value,
	SUM(profit) profit
FROM
	csx_dw.sale_goods_m b
WHERE
	sdt<regexp_replace( to_date( CURRENT_TIMESTAMP() ),
	'-',
	'' )
GROUP BY
	customer_no ,
	sdt;
-- 2.0计算最近销售客户
 DROP TABLE IF EXISTS b2b_tmp.p_max_sale;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_max_sale AS
SELECT
	regexp_replace( customer_no ,'(^0*)','' ) AS cust_id,
	sdt,
	SUM(sales_value) sales_value,
	datediff( to_date( date_sub( CURRENT_TIMESTAMP(),1 ) ),from_unixtime( unix_timestamp( sdt,'yyyymmdd' ),'yyyy-mm-dd' ) )days_not_sale
FROM
	csx_dw.sale_goods_m a
JOIN (
	SELECT
		a.cust_id,
		MAX(sdt)max_sdt
	FROM
		b2b_tmp.p_sale_dt a
	WHERE
		sales_value>0
	GROUP BY
		cust_id ) b ON
	a.sdt = b.max_sdt
	AND regexp_replace( customer_no ,'(^0*)','' )= regexp_replace( b.cust_id,'(^0*)','' )
GROUP BY
	a.customer_no ,
	sdt;
-- 3.1 30天至一年销售下单次数、金额、客单价 b2b_tmp.p_order_data
 DROP TABLE IF EXISTS b2b_tmp.p_order_data;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_order_data AS
SELECT
	cust_id,
	order_num_30,
	order_sale_30,
	order_sale_30 / order_num_30*1.00 order_avg_30,
	order_profit_30,
	order_num_90,
	order_sale_90,
	order_sale_90 / order_num_90*1.00 order_avg_90,
	order_profit_90,
	order_num_180,
	order_sale_180,
	order_sale_180 / order_num_180*1.00 order_avg_180,
	order_profit_180,
	order_num_365,
	order_sale_365,
	order_sale_365 / order_num_365*1.00 order_avg_365,
	order_profit_365
FROM
	(
	SELECT
		cust_id,
		COUNT(CASE WHEN sales_value != 0 AND sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 30)), '-', '') THEN sdt END ) order_num_30,
		SUM(CASE WHEN sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 30)), '-', '') THEN sales_value END ) order_sale_30,
		SUM(CASE WHEN sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 30)), '-', '') THEN profit END ) order_profit_30,
		COUNT(CASE WHEN sales_value != 0 AND sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 90)), '-', '') THEN sdt END ) order_num_90,
		SUM(CASE WHEN sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 90)), '-', '') THEN sales_value END ) order_sale_90,
		SUM(CASE WHEN sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 90)), '-', '') THEN profit END ) order_profit_90,
		COUNT(CASE WHEN sales_value != 0 AND sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 180)), '-', '') THEN sdt END ) order_num_180,
		SUM(CASE WHEN sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 180)), '-', '') THEN sales_value END ) order_sale_180,
		SUM(CASE WHEN sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 180)), '-', '') THEN profit END ) order_profit_180,
		COUNT(CASE WHEN sales_value != 0 AND sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 365)), '-', '') THEN sdt END ) order_num_365,
		SUM(CASE WHEN sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 365)), '-', '') THEN sales_value END ) order_sale_365,
		SUM(CASE WHEN sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 365)), '-', '') THEN profit END ) order_profit_365
	FROM
		b2b_tmp.p_sale_dt
	GROUP BY
		cust_id )a;
-- 3.2 拜访量30、60、90、365天数据  b2b_tmp.p_visit_data;
 DROP TABLE IF EXISTS b2b_tmp.p_visit_data;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_visit_data AS
SELECT
	customer_id,
	customer_no,
	customer_name,
	SUM(visit_offline_30)visit_offline_30,
	SUM(visit_phone_30)visit_phone_30,
	SUM(visit_offline_90)visit_offline_90,
	SUM(visit_phone_90)visit_phone_90,
	SUM(visit_offline_180)visit_offline_180,
	SUM(visit_phone_180)visit_phone_180,
	SUM(visit_offline_365)visit_offline_365,
	SUM(visit_phone_365)visit_phone_365
FROM
	(
	SELECT
		customer_id,
		customer_no,
		customer_name,
		visitor,
		visitor_id,
		COUNT(DISTINCT CASE WHEN customer_type = 1 AND regexp_replace(to_date(visit_time), '-', '')>= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 30)), '-', '') THEN regexp_replace(to_date(visit_time), '-', '') END ) visit_offline_30,
		COUNT(DISTINCT CASE WHEN customer_type = 2 AND regexp_replace(to_date(visit_time), '-', '')>= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 30)), '-', '') THEN regexp_replace(to_date(visit_time), '-', '') END ) visit_phone_30,
		COUNT(DISTINCT CASE WHEN customer_type = 1 AND regexp_replace(to_date(visit_time), '-', '')>= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 90)), '-', '') THEN regexp_replace(to_date(visit_time), '-', '') END ) visit_offline_90,
		COUNT(DISTINCT CASE WHEN customer_type = 2 AND regexp_replace(to_date(visit_time), '-', '')>= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 90)), '-', '') THEN regexp_replace(to_date(visit_time), '-', '') END ) visit_phone_90,
		COUNT(DISTINCT CASE WHEN customer_type = 1 AND regexp_replace(to_date(visit_time), '-', '')>= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 180)), '-', '') THEN regexp_replace(to_date(visit_time), '-', '') END ) visit_offline_180,
		COUNT(DISTINCT CASE WHEN customer_type = 2 AND regexp_replace(to_date(visit_time), '-', '')>= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 180)), '-', '') THEN regexp_replace(to_date(visit_time), '-', '') END ) visit_phone_180,
		COUNT(DISTINCT CASE WHEN customer_type = 1 AND regexp_replace(to_date(visit_time), '-', '')>= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 365)), '-', '') THEN regexp_replace(to_date(visit_time), '-', '') END ) visit_offline_365,
		COUNT(DISTINCT CASE WHEN customer_type = 2 AND regexp_replace(to_date(visit_time), '-', '')>= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 365)), '-', '') THEN regexp_replace(to_date(visit_time), '-', '') END ) visit_phone_365
	FROM
		csx_dw.customer_visit_info_m
	WHERE
		sdt = regexp_replace( to_date( date_sub( CURRENT_TIMESTAMP(),
		1 ) ),
		'-',
		'' )
	GROUP BY
		customer_no,
		customer_id,
		customer_name,
		visitor,
		visitor_id )a
GROUP BY
	customer_no,
	customer_id,
	customer_name;
-- 统计汇总数据	
 
 insert
	overwrite table csx_dw.p_customer_visit PARTITION (sdt)
SELECT
	a.id,	
	a.customer_number ,
	a.customer_name ,
	attribute_name ,
	channel ,
	fist_category ,
	second_category ,
	third_category ,
	province_code ,
	province_name ,
	city_name ,
	a.sales_name ,
	work_no ,
	sales_province ,
	sales_city ,
	company_code ,
	a.comp_name ,
	archive_category ,
	a.customer_type ,
	social_credit_code ,
	legal_person_name ,
	legal_person_id_no ,
	contacts ,
	invoice_name ,
	bank_no ,
	bank_name ,
	bank_account_name ,
	bank_account_no ,
	payment_name ,
	pay_mode ,
	-- 账期：01（无账期），02（7天），03（15天），04（30天），05（45天），06（60天），07（90天）
 payment_days ,
	taxes_category ,
	plan_sales_amount ,
	sign_amount ,
	customer_level ,
	credit_limit ,
	temp_credit_limit ,
	temp_begin_time ,
	temp_end_time ,
	credit_modulus ,
	customer_status ,
	-- channel_first_code,
	-- channel_second_code,
	-- sale_organization,
	-- distribution_channel,
	-- sale_region,
	-- credit_range --信控范围,
 org_code ,
	org_name ,
	contract_begin_time ,
	contract_end_time ,
	invoice_address ,
	creator ,
	create_time ,
	e.sign_date ,
	e.min_sale_sdt ,
	regexp_replace(to_date(max_visit_time),
	'-',
	'')as max_visit_time ,
	visitor_id ,
	b.sales_name visitor ,
	CASE WHEN b.customer_type = '1' THEN '1_上门'
	WHEN b.customer_type = '2' THEN '2_电话'
END visit_type ,
days_not_visit ,
e.sdt as max_sdt ,
sales_value ,
days_not_sale ,
nvl(d.order_num_30,0)order_num_30 ,
nvl(order_sale_30,0)order_sale_30 ,
nvl(order_avg_30,0)order_avg_30 ,
nvl(order_profit_30,0)order_profit_30 ,
nvl(order_num_90,0)order_num_90 ,
nvl(order_sale_90,0)order_sale_90 ,
nvl(order_avg_90,0)order_avg_90 ,
nvl(order_profit_90,0)order_profit_90 ,
nvl(order_num_180,0)order_num_180 ,
nvl(order_sale_180,0)order_sale_180 ,
nvl(order_avg_180,0)order_avg_180 ,
nvl(order_profit_180,0)order_profit_180 ,
nvl(order_num_365,0)order_num_365 ,
nvl(order_sale_365,0)order_sale_365 ,
nvl(order_avg_365,0)order_avg_365 ,
nvl(order_profit_365,0)order_profit_365 ,
nvl(f.visit_offline_30,0)visit_offline_30 ,
nvl(visit_phone_30,0)visit_phone_30 ,
nvl(visit_offline_90,0)visit_offline_90 ,
nvl(visit_phone_90,0)visit_phone_90 ,
nvl(visit_offline_180,0)visit_offline_180 ,
nvl(visit_phone_180,0)visit_phone_180 ,
nvl(visit_offline_365,0)visit_offline_365 ,
nvl(visit_phone_365,0)visit_phone_365 ,
nvl(a.cust_number,0)cust_number
-- 子客户数
,
regexp_replace(to_date(min_visit_time),'-','') as min_visit_time
-- 最早拜访日期
,
	agreement_dc_code ,
	agreement_dc_name ,
	inventory_dc_code ,
	inventory_dc_name ,
	mark_shop_code,
regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') sdt
FROM
b2b_tmp.p_cust_info a
LEFT JOIN (
SELECT
	customer_id,
	customer_no ,
	customer_name ,
	max_visit_time ,
	min_visit_time,
	visitor_id,
	sales_name,
	customer_type,
	days_not_visit
FROM
	b2b_tmp.temp_visit a )b ON
cast (id as string) = b.customer_id
LEFT outer JOIN (
SELECT
	e.id as id_s,
	e.cust_id,
	sign_date,
	min_sale_sdt,
	sdt,
	sales_value,
	days_not_sale
FROM
	b2b_tmp.p_min_sale_sdt e
LEFT JOIN (
	SELECT
		cust_id,
		sdt,
		sales_value,
		days_not_sale
	FROM
		b2b_tmp.p_max_sale )c ON
	e.cust_id = c.cust_id )e on
a.id = e.id_s
LEFT JOIN (
SELECT
	cust_id,
	order_num_30,
	order_sale_30,
	order_avg_30,
	order_profit_30,
	order_num_90,
	order_sale_90,
	order_avg_90,
	order_profit_90,
	order_num_180,
	order_sale_180,
	order_avg_180,
	order_profit_180,
	order_num_365,
	order_sale_365,
	order_avg_365,
	order_profit_365
FROM
	b2b_tmp.p_order_data )d ON
a.customer_number = d.cust_id
LEFT JOIN (
SELECT
	customer_no,
	customer_name,
	visit_offline_30,
	visit_phone_30,
	visit_offline_90,
	visit_phone_90,
	visit_offline_180,
	visit_phone_180,
	visit_offline_365,
	visit_phone_365
FROM
	b2b_tmp.p_visit_data )f ON
a.customer_number = f.customer_no
AND a.customer_name = f.customer_name;