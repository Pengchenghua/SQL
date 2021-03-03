-- CONNECTION: name=Hadoop - HIVE
-- SET hive.exec.dynamic.partition.mode = nonstrict;
-- set mapred.job.queue.name=default;
set  sdate=20190101;
set  edate=20190930;

-- ��Ʒ����SKU
 drop table
	b2b_tmp.p_temp_goods;

create temporary table
	b2b_tmp.p_temp_goods as select
		sdt ,sgm.province_code ,sgm.province_name ,channel_name ,
		customer_no ,
		customer_name ,
		goods_code ,
		SUM(sales_value)sale ,
		SUM(profit)profit
	from
		csx_dw.sale_goods_m as sgm 
	where
		sdt >= '20190101'
		and sdt <= '20190930'
		and channel ='1'
	GROUP by
		sdt ,
		customer_no ,
		goods_code,customer_name ,sgm.province_code ,sgm.province_name ,channel_name ;
--	select * from b2b_tmp.p_temp_goods 
-- REFRESH csx_dw.sale_warzone01_detail_dtl 
-- select * from  csx_dw.sale_warzone01_detail_dtl a limit 100
--	select * from  b2b_tmp.p_temp_sku where cust_id='0000103044' and mon='201807'
 drop table
	temp.p_cust_sale;
	create TEMPORARY table temp.p_cust_sale
as 
select		sdt ,province_code ,province_name,channel_name,
			customer_no ,customer_name,
			sum(sale)sale ,
			sum(profit)profit
		from
			b2b_tmp.p_temp_goods a
		group by
			sdt ,
			customer_no,customer_name,province_code ,province_name,channel_name;
-- �����������
drop table if exists temp.p_max_sdt;
create temporary table temp.p_max_sdt
as 
select province_code ,province_name,customer_no ,customer_name,max_sdt,from_unixtime(unix_timestamp(max_sdt,'yyyymmdd'),'yyyy-mm-dd') as max_sdt1,
	datediff(to_date('2019-09-30'),from_unixtime(unix_timestamp(max_sdt,'yyyymmdd'),'yyyy-mm-dd'))as diff_day
from (
select customer_no ,customer_name,province_code ,province_name,max(sdt)max_sdt from temp.p_cust_sale group by  customer_no ,customer_name,province_code ,province_name) a
; 
-- select * from  temp.p_max_sdt limit 1000;
-- ��������Ƶ��
 drop table if exists 
	b2b_tmp.p_temp_sdt;

create temporary table if not exists
	b2b_tmp.p_temp_sdt as select
		sdt ,province_code ,province_name,channel_name,
		customer_no ,customer_name,
		sum(sale)sale ,
		sum(profit)profit ,
		COUNT( DISTINCT CASE when sale != 0 then a.customer_no end )sales_frequency
	from
		temp.p_cust_sale a
	group by
		sdt ,
		province_code ,province_name,customer_no,customer_name,channel_name ;
	
DROP TABLE IF EXISTS b2b_tmp.p_temp_01;

CREATE TEMPORARY TABLE  b2b_tmp.p_temp_01 AS
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
		'104742') THEN '��ְ�ϻ���'
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
		to_date(b.sign_time) sign_time,sales_id ,sales_name,first_supervisor_name
	FROM
		csx_dw.customer_m b
	WHERE
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),	1)),'-','')
		AND customer_no <> '')a
LEFT OUTER  JOIN (
	SELECT
		DISTINCT customer_no,
		'��Ŀ�ϻ���' note
	FROM
		csx_dw.csx_partner_list) b ON
	a.customer_no = b.customer_no;	
	
-- �·�ת����
-- �ͻ���ʶ���弰˵����   
 --     ��ǩԼ�ͻ���ǩԼʱ���ڱ��µĿͻ���                                                              
 --     ��Ծ�ͻ���ÿ���µ������һ���������<=2�죩��                                                
 --     ��Ƶ�ͻ������һ���µ�7�죨���һ���������>2����<=7�죩��                                  
 --     ��Ĭ�ͻ������һ���µ��������30�죨���һ���������>7����<=30�죩��                
 --     Ԥ��ʧ�ͻ������һ���µ��������60�죨���һ���������>30����<=60�죩��         
 --     ��ʧ�ͻ������һ���µ��������60�����ϣ����һ���������>60�죩��     
 select
 channel,
 	b.sales_province_code ,
	b.sales_province,
	b.sales_city,
	b.customer_no ,
	b.customer_name ,
	note,
	regexp_replace(to_date(b.sign_time) ,'-' ,'')create_date ,
	b.first_category ,
	b.second_category,
	sales_id,
	sales_name,
	first_supervisor_name,
	max_sdt,
	diff_day,
	case when substring(regexp_replace(to_date(b.sign_time) ,'-' ,''),1,6)= substring(regexp_replace(to_date(date_sub(current_timestamp(),1)) ,'-' ,''),1,6)
	then '����ǩԼ'
	when diff_day<=2 then '��Ծ�ͻ�'
	when diff_day>2 and diff_day<=7 then '��Ƶ�ͻ�'
	when diff_day>7 and diff_day<=30 then '��Ĭ�ͻ�'
	when diff_day>30 and diff_day<=60 then 'Ԥ��ʧ�ͻ�'
	when diff_day>60  then '��ʧ�ͻ�'
	else diff_day end as note,
	(sale)sale ,
	(profit)profit ,
	profit/sale*1.00 prorate,
	(sales_frequency)sales_frequency ,
	--avg_sku,
	mon_s,
	SIZE (split(mon_s ,',')) mon_s-- ��������·���
	-- min_sdt ,min_sale,max_sdt,max_sale,	
	--CEIL (months_between(from_unixtime(unix_timestamp(max_sdt,'yyyymmdd'),'yyyy-mm-dd'),from_unixtime(unix_timestamp(min_sdt,'yyyymmdd'),'yyyy-mm-dd') ))diff_date
from
(select
		b.customer_no ,
		b.customer_name ,
		b.first_category,
		b.second_category,
		b.channel ,
		b.sales_province_code,
		b.sales_province ,
		b.sales_city,
		to_date(b.sign_time) sign_time,sales_id ,sales_name,first_supervisor_name,note
	from
		 b2b_tmp.p_temp_01 b where customer_no <>'' 
	)b 
	left join 
	(
		select customer_no cust_id,province_code ,province_name ,channel_name,
		sum(sale)sale ,
		sum(profit)profit ,
		sum(sales_frequency)sales_frequency ,
		-- concat_ws(',' ,	collect_set(substr(sdt ,1 ,	6))) 
		substr(sdt ,1 ,	6) mon_s
	from
		b2b_tmp.p_temp_sdt a
		where 1=1
	group by
		customer_no,substr(sdt ,1 ,	6),province_code ,province_name,channel_name)a
 on
	b.customer_no = a.cust_id 
left join 
temp.p_max_sdt  c
on b.customer_no=c.customer_no 
	

;

select * from csx_dw.customer_m where sdt='20190830' and customer_no='103735';

select qdflag,cust_id,customer_name,dist,SUM(xse)sale from csx_dw.sale_warzone01_detail_dtl where dist like '����%' and sdt>='20190801' and sdt<='20190830' 
group by 
cust_id,customer_name,dist,qdflag;
-- select * from csx_dw.customer_m where customer_no='103995' limit 1;

