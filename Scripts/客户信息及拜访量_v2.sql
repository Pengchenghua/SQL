set
mapreduce.job.queuename = caishixian;

set
hive.exec.dynamic.partition = true;
-- ������̬����
 set
hive.exec.dynamic.partition.mode = nonstrict;
-- ��̬����ģʽ
 set
hive.exec.max.dynamic.partitions.pernode = 10000;
-- ��ʾÿ��maper��reducer���������������̬��������
-- �����������
 drop table if exists b2b_tmp.bill_data;

CREATE TEMPORARY TABLE if not exists b2b_tmp.bill_data as
select
	'P001' payment_terms,
	'Ʊ��7��' payment_name
union all
select
	'P002' payment_terms,
	'Ʊ��15��' payment_name
union all
select
	'P003' payment_terms,
	'Ʊ��20��' payment_name
union all
select
	'P004' payment_terms,
	'Ʊ��30��' payment_name
union all
select
	'Y001' payment_terms,
	'�½�10��' payment_name
union all
select
	'Y002' payment_terms,
	'�½�15��' payment_name
union all
select
	'Y003' payment_terms,
	'�½�30��' payment_name
union all
select
	'Y004' payment_terms,
	'�½�45��' payment_name
union all
select
	'Y005' payment_terms,
	'�½�60��' payment_name
union all
select
	'Y006' payment_terms,
	'�½�90��' payment_name
union all
select
	'Z001' payment_terms,
	'˫������' payment_name
union all
select
	'Z002' payment_terms,
	'Ʊ��7��' payment_name
union all
select
	'Z003' payment_terms,
	'Ʊ��15��' payment_name
union all
select
	'Z004' payment_terms,
	'Ʊ��30��' payment_name
union all
select
	'Z005' payment_terms,
	'Ʊ��45��' payment_name
union all
select
	'Z006' payment_terms,
	'Ʊ��60��' payment_name
union all
select
	'Z007' payment_terms,
	'Ԥ������' payment_name
union all
select
	'Z008' payment_terms,
	'Ʊ��8��' payment_name ;
-- �ӿͻ���
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
		and ( inventory_dc_name not like '%����%'
		and inventory_dc_name not like '%����%')
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
-- �ͻ����Ա�    sys_customer_category_ods
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
	--�ӿͻ���
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
			when attribute = '1' then '1_����ͻ�'
			when attribute = '2' then '2_�����ͻ�'
			when attribute = '3' then '3_ó�׿ͻ�'
			when attribute = '4' then '4_ս�Կͻ�'
			when attribute = '5' then '5_�ϻ��˿ͻ�'
			else attribute
	end attribute_name,
		channel,
		first_category_code,
		first_category,
		second_category_code,
		second_category,
		third_category_code,
		third_category,
		-- �������ࣺ01���ˣ�02����/��ҵ��λ��03���ӣ�04�����Ƿ��ˣ�05һ���Կͻ�
 archive_category,
		-- ����ģʽ��01���ڿͻ���02��ʱ�ͻ�
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
		-- 01ת�ˡ�02�ֽ�ˢ������03֧Ʊ��04�ſء�05�˿�
 case
			when pay_mode = '01' THEN '01_ת��'
			when pay_mode = '02' THEN '02_�ֽ�(ˢ��)'
			when pay_mode = '03' THEN '03_֧Ʊ'
			when pay_mode = '04' THEN '04_�ſ� '
			when pay_mode = '05' THEN '05_�ʿ�'
	end pay_mode ,
		-- ���ڣ�01�������ڣ���02��7�죩��03��15�죩��04��30�죩��05��45�죩��06��60�죩��07��90�죩
 CASE WHEN payment_days = '01' THEN '01_������'
		WHEN payment_days = '02' THEN '02_7��'
		WHEN payment_days = '03' THEN '03_15��'
		WHEN payment_days = '04' THEN '04_30��'
		WHEN payment_days = '05' THEN '05_45��'
		WHEN payment_days = '06' THEN '06_60��'
		WHEN payment_days = '07' THEN '07_90��'
END payment_days,
	plan_sales_amount,
	sign_amount,
	--��0��˰��1һ����˰�ˡ�2С��ģ��˰��
 case
		when taxes_category = '0' then '0_��˰'
		when taxes_category = '1' then '1_һ����˰��'
		when taxes_category = '2' then '1_С��ģ��˰��'
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
	CASE WHEN customer_level = 'AA' THEN 'AA���ʿͻ�'
	WHEN customer_level = 'A' THEN 'A���ÿͻ�'
	WHEN customer_level = 'B' THEN 'B���ÿͻ�'
	WHEN customer_level = 'C' THEN 'Cһ��ͻ�'
	WHEN customer_level = 'D' THEN 'D���տͻ�'
END customer_level,
credit_limit,
temp_credit_limit,
to_date(temp_begin_time) temp_begin_time ,
to_date(temp_end_time) temp_end_time,
credit_modulus,
CASE WHEN customer_status = '01' THEN '01_������'
WHEN customer_status = '02' THEN '02_������'
WHEN customer_status = '03' THEN '03_�����ܾ�'
WHEN customer_status = '04' THEN '04������ͨ��'
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
and channel != '�̳�(����)' ) a
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
--  ǩԼ��������ǩԼ�ɽ����� ��ǩԼ����Ϊ��
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
-- ��ѯ��ǩԼ�ɽ�ʱ��
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
--  ��ȡ�ݷ�������� ,����δ�ݷ����������ݷ�ģʽ      b2b_tmp.temp_visit 
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
-- select * from b2b_tmp.temp_max_visit where customer_name like '%��������ʫ�������޹�˾���˴�ֹ�˾%'
-- �����������
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
-- 2.0����������ۿͻ�
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
-- 3.1 30����һ�������µ����������͵��� b2b_tmp.p_order_data
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
-- 3.2 �ݷ���30��60��90��365������  b2b_tmp.p_visit_data;
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
-- ͳ�ƻ�������	
 
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
	-- ���ڣ�01�������ڣ���02��7�죩��03��15�죩��04��30�죩��05��45�죩��06��60�죩��07��90�죩
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
	-- credit_range --�ſط�Χ,
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
	CASE WHEN b.customer_type = '1' THEN '1_����'
	WHEN b.customer_type = '2' THEN '2_�绰'
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
-- �ӿͻ���
,
regexp_replace(to_date(min_visit_time),'-','') as min_visit_time
-- ����ݷ�����
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