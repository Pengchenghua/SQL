 DROP TABLE IF EXISTS b2b_tmp.p_temp_sdt;

CREATE TEMPORARY TABLE IF NOT EXISTS b2b_tmp.p_temp_sdt AS
SELECT
	sdt ,
	province_code ,
	province_name,
	channel_name,
	customer_no ,
	customer_name,
	SUM(sale)sale ,
	SUM(profit)profit ,
	COUNT( DISTINCT CASE WHEN sale != 0 THEN a.customer_no END )sales_frequency
FROM
	temp.p_cust_sale a
GROUP BY
	sdt ,
	province_code ,
	province_name,
	customer_no,
	customer_name,
	channel_name ;

DROP TABLE IF EXISTS b2b_tmp.p_temp_01;

CREATE TEMPORARY TABLE b2b_tmp.p_temp_01 AS
SELECT
	a.customer_no ,
	customer_name ,
	first_category,
	second_category,
	channel ,
	sales_province_code,
	sales_province ,
	sales_city,
	to_date(sign_time) sign_time,
	sales_id ,
	sales_name,
	first_supervisor_name,
	CASE
		WHEN a.customer_no IN ('PF0649',
		'102784',
		'102215',
		'104267',
		'104172',
		'104751',
		'104746',
		'104745',
		'103145',
		'103151',
		'103154',
		'103156',
		'103204',
		'103207',
		'103245',
		'103160',
		'103243',
		'103247',
		'103283',
		'104099',
		'104122',
		'104617',
		'103135',
		'103155',
		'103174',
		'104340',
		'104705',
		'103140',
		'103146',
		'103165',
		'103170',
		'103194',
		'103246',
		'103250',
		'104697',
		'104742') THEN '兼职合伙人'
		ELSE note
END note
FROM
	(
	SELECT
		b.customer_no ,
		b.customer_name ,
		b.first_category,
		b.second_category,
		b.channel ,
		b.sales_province_code,
		b.sales_province ,
		b.sales_city,
		to_date(b.sign_time) sign_time,
		sales_id ,
		sales_name,
		first_supervisor_name
	FROM
		csx_dw.customer_m b
	WHERE
		sdt = '20191105'
		AND customer_no <> '')a
LEFT OUTER JOIN (
	SELECT
		DISTINCT customer_no,
		'项目合伙人' note
	FROM
		csx_dw.csx_partner_list) b ON
	a.customer_no = b.customer_no;
	
