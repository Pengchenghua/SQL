
SET edate='2019-10-31';
-- ��Ʒ����SKU
 DROP TABLE b2b_tmp.p_temp_goods;

CREATE TEMPORARY TABLE b2b_tmp.p_temp_goods AS
SELECT
	sdt,
	sgm.province_code,
	sgm.province_name,
	CASE
		WHEN province_name LIKE 'ƽ̨%' THEN 'ƽ̨'
		WHEN channel IN('1','7') THEN '��ͻ�'
		WHEN channel IN('2','3','8') THEN '�̳�'
		ELSE channel_name
END channel_name,
	customer_no,
	customer_name,
	goods_code,
	SUM(sales_value)sale,
	SUM(profit)profit
FROM
	csx_dw.sale_goods_m AS sgm
WHERE
	sdt >= '20190101'
	AND sdt <=regexp_replace(${hiveconf:edate},'-','')
	AND channel IN('1',	'7')
	AND sgm.province_name NOT LIKE 'ƽ̨%'
GROUP BY
	sdt,
	customer_no,
	goods_code,
	customer_name,
	sgm.province_code,
	sgm.province_name,
	CASE
		WHEN province_name LIKE 'ƽ̨%' THEN 'ƽ̨'
		WHEN channel IN('1','7') THEN '��ͻ�'
		WHEN channel IN('2','3','8') THEN '�̳�'
		ELSE channel_name
END ;
-- select channel_name,province_name, sum(sale) from b2b_tmp.p_temp_goods group by  channel_name,province_name;
 DROP TABLE temp.p_cust_sale;

CREATE TEMPORARY TABLE temp.p_cust_sale AS
SELECT
	sdt ,
	province_code ,
	province_name,
	channel_name,
	customer_no ,
	customer_name,
	SUM(sale)sale ,
	SUM(profit)profit
FROM
	b2b_tmp.p_temp_goods a
GROUP BY
	sdt ,
	customer_no,
	customer_name,
	province_code ,
	province_name,
	channel_name;
-- �����������
 DROP TABLE IF EXISTS temp.p_max_sdt;

CREATE TEMPORARY TABLE temp.p_max_sdt AS
SELECT
	province_code ,
	province_name,
	customer_no ,
	customer_name,
	max_sdt,
	from_unixtime(unix_timestamp(max_sdt,'yyyymmdd'),'yyyy-mm-dd') AS max_sdt1,
	datediff(to_date(${hiveconf:edate}),	from_unixtime(unix_timestamp(max_sdt,'yyyymmdd'),'yyyy-mm-dd'))AS diff_day
FROM
	(
	SELECT
		customer_no ,
		customer_name,
		province_code ,
		province_name,
		MAX(sdt)max_sdt
	FROM
		temp.p_cust_sale
	GROUP BY
		customer_no ,
		customer_name,
		province_code ,
		province_name) a ;
-- select * from  temp.p_max_sdt limit 1000;
-- ��������Ƶ��
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


-- �·�ת����
-- �ͻ���ʶ���弰˵����   
--     ��ǩԼ�ͻ���ǩԼʱ���ڱ��µĿͻ���                                                              
--     ��Ծ�ͻ���ÿ���µ������һ���������<=2�죩��                                                
--     ��Ƶ�ͻ������һ���µ�7�죨���һ���������>2����<=7�죩��                                  
--     ��Ĭ�ͻ������һ���µ��������30�죨���һ���������>7����<=30�죩��                
--     Ԥ��ʧ�ͻ������һ���µ��������60�죨���һ���������>30����<=60�죩��         

--     ��ʧ�ͻ������һ���µ��������60�����ϣ����һ���������>60�죩��     
 SELECT
	channel,
	channel_name,
	b.sales_province_code ,
	b.sales_province,
	b.sales_city,
	b.customer_no ,
	b.customer_name ,
	b.note,
	regexp_replace(to_date(b.sign_time) ,
	'-' ,
	'')create_date ,
	b.first_category ,
	b.second_category,
	sales_id,
	sales_name,
	first_supervisor_name,
	max_sdt,
	diff_day,
	CASE
		WHEN SUBSTRING(regexp_replace(to_date(b.sign_time) , '-' , ''), 1, 6)= SUBSTRING(regexp_replace(${hiveconf:edate},'-',''), 1, 6) THEN '����ǩԼ'
		WHEN diff_day <= 2 THEN '��Ծ�ͻ�'
		WHEN diff_day>2
		AND diff_day <= 7 THEN '��Ƶ�ͻ�'
		WHEN diff_day>7
		AND diff_day <= 30 THEN '��Ĭ�ͻ�'
		WHEN diff_day>30
		AND diff_day <= 60 THEN 'Ԥ��ʧ�ͻ�'
		WHEN diff_day>60 THEN '��ʧ�ͻ�'
		ELSE diff_day
END AS note,
	(sale)sale ,
	(profit)profit ,
	profit / sale*1.00 prorate,
	(sales_frequency)sales_frequency ,
	--avg_sku,
 mon_s,
	SIZE (split(mon_s ,
	',')) mon_s
	-- ��������·���
	-- min_sdt ,min_sale,max_sdt,max_sale,	
	--CEIL (months_between(from_unixtime(unix_timestamp(max_sdt,'yyyymmdd'),'yyyy-mm-dd'),from_unixtime(unix_timestamp(min_sdt,'yyyymmdd'),'yyyy-mm-dd') ))diff_date

	FROM (
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
		first_supervisor_name,
		note
	FROM
		csx_dw.view_customer b
	WHERE
		customer_no <> '' )b
LEFT JOIN (
	SELECT
		customer_no cust_id,
		province_code ,
		province_name ,
		channel_name,
		SUM(sale)sale ,
		SUM(profit)profit ,
		SUM(sales_frequency)sales_frequency ,
		-- concat_ws(',' ,	collect_set(substr(sdt ,1 ,	6))) 
 substr(sdt ,
		1 ,
		6) mon_s
	FROM
		b2b_tmp.p_temp_sdt a
	WHERE
		1 = 1
	GROUP BY
		customer_no,
		substr(sdt ,1 ,6),
		province_code ,
		province_name,
		channel_name)a ON
	b.customer_no = a.cust_id
LEFT JOIN temp.p_max_sdt c ON
	b.customer_no = c.customer_no ;