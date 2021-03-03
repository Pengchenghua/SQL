-- CONNECTION: name=Hadoop - HIVE
 set mapreduce.job.queuename=caishixian;
 SET hive.exec.dynamic.partition.mode = nonstrict;
set mapred.job.queue.name=default;
set mapreduce.map.memory.mb=5120;
set hive.exec.parallel=true;
set  sdate=20180701;
set  edate=20190731;

-- 单品计算SKU 增加部类
 drop table
	b2b_tmp.p_temp_goods;

create temporary table
	b2b_tmp.p_temp_goods as select
		sdt ,
		customer_no  cust_id,
		category_code ,
		goods_code goodsid ,
		SUM(sales_value )sale ,
		SUM(profit )profit
	from
		csx_dw.sale_b2b_item 
	where
		sdt >= '${hiveconf:sdate}'
		and sdt <= '${hiveconf:edate}'
		and shop_id != 'W098'
		and sales_type in('qyg','gc')
	GROUP by
		sdt ,
		customer_no ,
		goods_code,
	category_code;
--	select * from b2b_tmp.p_temp_goods 
--  计算最早销售日期 、最大销售日期
 DROP table
	if EXISTS b2b_tmp.p_temp_min;

create TEMPORARY TABLE
	if not EXISTS b2b_tmp.p_temp_min as select
		a.cust_id,
		min_sale,
		min_sdt,
		max_sale,
		max_sdt
	from
		(
			select regexp_replace(a.cust_id,'(^0*)','')cust_id,
			sum(sale)min_sale ,
			sdt as min_sdt
		from
			b2b_tmp.p_temp_goods a
		JOIN (
				select cust_id,
				min(sdt)min_sdt
			from
				b2b_tmp.p_temp_goods
			group by
				cust_id)b on
			a.cust_id = b.cust_id
			and sdt = min_sdt
		group by
			a.cust_id,
			sdt)a
	left OUTER JOIN (
			select regexp_replace(a.cust_id,'(^0*)','')cust_id,
			sum(sale)max_sale ,
			sdt as max_sdt
		from
			b2b_tmp.p_temp_goods a
		JOIN (
				select cust_id,
				max(sdt)max_sdt
			from
				b2b_tmp.p_temp_goods
			group by
				cust_id)b on
			a.cust_id = b.cust_id
			and sdt = max_sdt
		group by
			a.cust_id,
			sdt)b on
		a.cust_id = b.cust_id ;
-- 计算SKU数
 drop table
	if EXISTS b2b_tmp.p_temp_sku;

create temporary table
	if not EXISTS b2b_tmp.p_temp_sku as select
		substr(sdt,1,6) mon,
		cust_id,
		count(distinct goodsid)sku
	from
		b2b_tmp.p_temp_goods
	group by
		cust_id,substr(sdt,1,6);
--	select * from  b2b_tmp.p_temp_sku where cust_id='0000103044' and mon='201807'
-- 计算销售频次
 drop table
	b2b_tmp.p_temp_sdt;

create temporary table
	b2b_tmp.p_temp_sdt as select
		sdt ,
		cust_id ,
		sum(sale)sale ,
		sum(profit)profit ,
		COUNT( DISTINCT CASE when sale != 0 then a.cust_id end )sales_frequency
	from
		(
		select
			sdt ,
			cust_id ,
			sum(sale)sale ,
			sum(profit)profit
		from
			b2b_tmp.p_temp_goods a
		group by
			sdt ,
			cust_id) a
	group by
		sdt ,
		cust_id ;
-- 月份转横向
 select
	b.region_province_name ,
	a.cust_id ,
	b.customer_name ,
	b.sflag ,
	regexp_replace(to_date(b.create_time) ,'-' ,'')create_date ,
	c.source_name ,
	c.type_1_name ,
	c.type_2_name ,
		case when sales_frequency >='16' then '16以上' 
		when sales_frequency<16 and sales_frequency>5 then '6~15'
		when sales_frequency<6 and sales_frequency>=2 then '2~5'
		when sales_frequency=1 then '1'
	else sales_frequency end 	frequency_note,
	case when atv>=100000 then'10万以上' 
		when atv<100000 and atv>=50000 then '5万~10万'
		when atv<50000 and atv>=10000 then '1万~5万'
		when  atv<10000 and atv>=5000 then '5千~1万'
		when atv<5000 then '5千以下'
		ELSE 
		atv
		end atv_note,
	case when 	sales_frequency=1 and atv>=100000 then '客户福利单' end weal_note,
	-- avg_sku,
	(sale)sale ,
	(profit)profit ,
	profit/sale*1.00 prorate,
	sales_frequency,
	atv, 
	mon_s,
	SIZE (split(mon_s ,',')) mon_s-- 数组计算月份数
	 ,min_sdt ,min_sale,max_sdt,max_sale,	
	CEIL (months_between(from_unixtime(unix_timestamp(max_sdt,'yyyymmdd'),'yyyy-mm-dd'),from_unixtime(unix_timestamp(min_sdt,'yyyymmdd'),'yyyy-mm-dd') ))diff_date
from
	(
		select cust_id,
		sum(sale)sale ,
		sum(profit)profit ,
		sum(sales_frequency)sales_frequency ,
		sum(sale)/sum(sales_frequency) atv
		-- ,concat_ws(',' ,	collect_set(substr(sdt ,1 ,	6))) 
		,substr(sdt ,1 ,	6) mon_s
	from
		b2b_tmp.p_temp_sdt a
	group by
		cust_id
		,substr(sdt ,1 ,	6)
		)a
 join (
	select
		customer_number ,
		b.customer_name ,
		b.sflag ,
		b.region_province_name ,
		b.create_time,sdt
	from
		csx_dw.customer_simple_info_v2 b
	where
		sdt =${hiveconf:edate} )b on
	regexp_replace(a.cust_id ,	'(^0*)' ,'')= regexp_replace(b.customer_number ,'(^0*)' ,'')
 join (
	select
		cust_id ,
		source_name ,
		type_1_name ,
		type_2_name
	from
		csx_ods.b2b_customer_new )c on
	regexp_replace(a.cust_id ,	'(^0*)' ,'')= regexp_replace(c.cust_id ,'(^0*)' ,'')

LEFT 	join (
		select *
	from
		b2b_tmp.p_temp_sku )j on
	a.cust_id = j.cust_id 	and a.mon_s=j.mon
left join 
(select cust_id ,min_sdt ,min_sale,max_sdt,max_sale from b2b_tmp.p_temp_min) d 
on regexp_replace(a.cust_id,'(^0*)','')=regexp_replace(d.cust_id,'(^0*)','') 
 ;

-- -----------------------------------------------------------------------
set mapreduce.map.memory.mb=5120;
set hive.exec.parallel=true;
-- 按月查询
 select
	a.mon,
	b.region_province_name ,
	a.cust_id ,
	b.customer_name ,
	b.sflag ,
	regexp_replace(to_date(b.create_time) ,'-' ,'')create_date ,
	c.source_name ,
	c.type_1_name ,
	c.type_2_name ,
	(sale)sale ,
	(profit)profit ,
	(sales_frequency)sales_frequency,
	(sku)sku
from
	(
		select substr(a.sdt,1,6)mon,
		cust_id,
		sum(sale)sale ,
		sum(profit)profit ,
		sum(sales_frequency)sales_frequency
	from
		b2b_tmp.p_temp_sdt a
	group by
		substr(a.sdt,1,6),cust_id
		)a
join (
	select
		customer_number ,
		b.customer_name ,
		b.sflag ,
		b.region_province_name ,
		b.create_time 
	from
		csx_dw.customer_simple_info_v2 b
	where
		sdt = '20190730' )b on
	regexp_replace(a.cust_id ,'(^0*)' ,'')= regexp_replace(b.customer_number ,'(^0*)' ,'')
join (
	select
		cust_id ,
		source_name ,
		type_1_name ,
		type_2_name
	from
		csx_ods.b2b_customer_new )c on
	regexp_replace(a.cust_id ,'(^0*)' ,'')= regexp_replace(c.cust_id ,'(^0*)' ,'')
left join (
		select *
	from
		b2b_tmp.p_temp_sku)j on
	a.cust_id = j.cust_id
	and a.mon = j.mon ;
-- 单笔金额大于10万
 select
	sdt,
	sflag,
	a.cust_id,
	b.cust_name,
	b.source_name,
	b.type_1_name,
	b.type_2_name,
	sale,
	profit
from
	b2b_tmp.p_temp_sdt a
join (
		select cust_id,
		cust_name,
		b.source_name,
		b.type_1_name,
		b.type_2_name,
		sflag
	from
		csx_ods.b2b_customer_new b)b on
	regexp_replace(a.cust_id,
	'(^0*)',
	'')= regexp_replace(b.cust_id,
	'(^0*)',
	'')
	and sale >= 100000;
	-- select sum(sale) from b2b_tmp.p_temp_sdt where cust_id='0000103135'

