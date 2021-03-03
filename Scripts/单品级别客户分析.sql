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
	b2b_tmp.p_temp_goods 
as
select sdt,case when shopid_orig='W0B6' then 'BBC' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' else c.sflag end qdflag,
case when c.dist is not null and c.sflag<>'M端' then substr(dist,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end dist,region_no,region_manage,b.city_name,region_city,
--manage,manage_no,暂时先不添加，直接按照表格给的福州沈锋，泉州胡永水
a.cust_id,c.cust_name,goods_code ,category_code , coalesce(dept_id,div_id)dept_id,coalesce(dept_name,div_name)dept_name,
sum(sales)sales,
sum(profit)profit
from 
(select shop_id,customer_no cust_id,origin_shop_id shopid_orig,sdt,goods_code ,category_code ,category_large_code 
       ,sum(sales_value) sales
       ,sum(profit) profit
from  csx_dw.sale_b2b_item 
where sdt>='${hiveconf:sdate}'
and sdt<='${hiveconf:edate}'
and shop_id<>'W098'
and sales_type in('qyg','gc') 
group by shop_id,customer_no,origin_shop_id,sdt,category_code ,category_large_code ,goods_code)a
left join (select DISTINCT div_id,div_name,dept_id,dept_name,catg_l_id from dim.dim_catg WHERE sdt ='${hiveconf:edate}') f on a.category_large_code=catg_l_id
left join csx_ods.b2b_customer_new c on lpad(a.cust_id,10,'0')=lpad(c.cust_id,10,'0')
left join (select shop_id,prov_name from dim.dim_shop where edate='9999-12-31')d on a.shop_id=d.shop_id
left join
(select shop_id,case when shop_id in ('W055','W056') then '上海市' else prov_name end prov_name,case when prov_name like '%市' then prov_name else city_name end city_name from dim.dim_shop where edate='9999-12-31' )b 
on a.cust_id=concat('S',b.shop_id)
group by sdt,case when shopid_orig='W0B6' then 'BBC' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' else c.sflag end,
case when c.dist is not null and c.sflag<>'M端' then substr(dist,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end,region_no,region_manage,b.city_name,region_city,a.cust_id,goods_code ,category_code ,c.cust_name
, coalesce(dept_id,div_id),coalesce(dept_name,div_name);

--	
select case when SUBSTRING(sdt,1,6) between '201901' and '201903' then 'Q1' when SUBSTRING(sdt,1,6) between '201904' and '201906' then 'Q2' else'Q3' end quarter
	,qdflag,dist,cust_id,cust_name,category_code,dept_id,dept_name
	,COUNT(DISTINCT goods_code)goods_cn,sum(sales)sale,sum(profit)profit
from b2b_tmp.p_temp_goods where sdt>='20190101'
and cust_id in ('103306','104417','103912','103304','103297','103296','104114','104459','103307','PF0326','104343','103284','104160','104169',
'102734','104214','102828','103830','103311','103994','103253','103998','100883','104080','104232','104507','104379','104206','104494','104380',
'102635','104081','104420','104156','104157','104009','104153','104084','104503','104283','103989','104468','104569','103781','104567','104320',
'104022','103314','104297','103152','103845','104304','104350','103197','102806','104226','104315','104514','103649','104485','104521','104224','102719',
'100799','PF0065','102829','104484','104251','104259','104048','103215','104289','103993','104266','103126','104414','102893','102738','103704','103068',
'103775','104036','103358','104195','103218','104311','104229','104287','103945','104030','103406','104513','104222','103057','104321','103855','104033',
'103240','103267','104241','103211','103137','103188','103179','104407','104586','102735','103198','104600','102704','103764','102859','104238','102484',
'102878','104516','104128','103371','103236','103229','103853','103286','103140','102730','102166','104267','104337','103230','104517','103193','104342',
'103173','104239','104560','104100','104401','104007','103092','104255','104422','103859','104274','104524','104150','103370','104547','PF0094','104111',
'103075','103136','104358','104529','103209','PF0099','103908','103875','103770','102790','103883','103096','103927','100563','104141','104165','102827',
'103823','102229','103719','100361','102565','104398','102692','104375','104307','103217','103206','103885','104355','104430','104219','103316','104371',
'PF0320','104460','101653','103281','103835','102215','101897','103906','103332','104492','103309','103995','101916','PF0431','104519','103355','102865',
'101928','101870','101482','102687','103926','102975','104523','103825','104324','103954','103374','104346','104179','104410','103235','PF0365','104217',
'103094','103874','PF0500','102757','104370','104268','103344','103138','103346','104361','103772','PF4096','103334','104363','104353','103784','103868',
'103810','104332','103898','104538','102682','PF0526','103086','PF0085','104164','103283','102680','103867','103759','PF0319','PF0328','102251','PF0462',
'103194','104052','101884','104075','PF0345','104322','104448','103195','104172','100360','104335','104065','PF1265','104464','103320','102633','104333',
'PF1209','103808','103751','103167','104504','103034','PF1206','104478','103670','104035','PF0143','103824','102844','102890','103964','103199','103250',
'104508','103247','104318','101543','102784','102202','102580','104296','103106','102523','102691','104469','102798','102754','103174','102508','104225',
'PF0937','104116','102534','104596','102583','102632','103765','103062','103070','103887','103849','101585','104532','103156','104209','103135','PF0458',
'100794','103204','103899','103245','104151','103160','104386','104055','104095','102901','103782','103904','100326','103145','103256','102662','103155',
'104023','PF0424','103183','102686','102879','103056','103207','103010','PF0649','104362','103154','104531','103271','103175','104477','PF0129','102646',
'102894','103243','103717','104402','102998','103369','103151','104444','101988','103141','103072','102751','102661','104099','103709','102942','103189',
'104340','103058','103997','103259','102225','104122','103928','104086','104275','103876','104527','PF0548','PF1205','102755','104192','102955'
)
group by case when SUBSTRING(sdt,1,6) between '201901' and '201903' then 'Q1' when SUBSTRING(sdt,1,6) between '201904' and '201906' then 'Q2' else 'Q3' end,
qdflag,dist,cust_id,cust_name,category_code,dept_id,dept_name;

select case when SUBSTRING(sdt,1,6) between '201901' and '201903' then 'Q1' when SUBSTRING(sdt,1,6) between '201904' and '201906' then 'Q2' else'Q3' end quarter
	,qdflag,category_code,dept_id,dept_name
	,COUNT(DISTINCT goods_code)goods_cn,sum(sales)sale,sum(profit)profit
from b2b_tmp.p_temp_goods where sdt>='20190101'
and cust_id in ('103306','104417','103912','103304','103297','103296','104114','104459','103307','PF0326','104343','103284','104160','104169',
'102734','104214','102828','103830','103311','103994','103253','103998','100883','104080','104232','104507','104379','104206','104494','104380',
'102635','104081','104420','104156','104157','104009','104153','104084','104503','104283','103989','104468','104569','103781','104567','104320',
'104022','103314','104297','103152','103845','104304','104350','103197','102806','104226','104315','104514','103649','104485','104521','104224','102719',
'100799','PF0065','102829','104484','104251','104259','104048','103215','104289','103993','104266','103126','104414','102893','102738','103704','103068',
'103775','104036','103358','104195','103218','104311','104229','104287','103945','104030','103406','104513','104222','103057','104321','103855','104033',
'103240','103267','104241','103211','103137','103188','103179','104407','104586','102735','103198','104600','102704','103764','102859','104238','102484',
'102878','104516','104128','103371','103236','103229','103853','103286','103140','102730','102166','104267','104337','103230','104517','103193','104342',
'103173','104239','104560','104100','104401','104007','103092','104255','104422','103859','104274','104524','104150','103370','104547','PF0094','104111',
'103075','103136','104358','104529','103209','PF0099','103908','103875','103770','102790','103883','103096','103927','100563','104141','104165','102827',
'103823','102229','103719','100361','102565','104398','102692','104375','104307','103217','103206','103885','104355','104430','104219','103316','104371',
'PF0320','104460','101653','103281','103835','102215','101897','103906','103332','104492','103309','103995','101916','PF0431','104519','103355','102865',
'101928','101870','101482','102687','103926','102975','104523','103825','104324','103954','103374','104346','104179','104410','103235','PF0365','104217',
'103094','103874','PF0500','102757','104370','104268','103344','103138','103346','104361','103772','PF4096','103334','104363','104353','103784','103868',
'103810','104332','103898','104538','102682','PF0526','103086','PF0085','104164','103283','102680','103867','103759','PF0319','PF0328','102251','PF0462',
'103194','104052','101884','104075','PF0345','104322','104448','103195','104172','100360','104335','104065','PF1265','104464','103320','102633','104333',
'PF1209','103808','103751','103167','104504','103034','PF1206','104478','103670','104035','PF0143','103824','102844','102890','103964','103199','103250',
'104508','103247','104318','101543','102784','102202','102580','104296','103106','102523','102691','104469','102798','102754','103174','102508','104225',
'PF0937','104116','102534','104596','102583','102632','103765','103062','103070','103887','103849','101585','104532','103156','104209','103135','PF0458',
'100794','103204','103899','103245','104151','103160','104386','104055','104095','102901','103782','103904','100326','103145','103256','102662','103155',
'104023','PF0424','103183','102686','102879','103056','103207','103010','PF0649','104362','103154','104531','103271','103175','104477','PF0129','102646',
'102894','103243','103717','104402','102998','103369','103151','104444','101988','103141','103072','102751','102661','104099','103709','102942','103189',
'104340','103058','103997','103259','102225','104122','103928','104086','104275','103876','104527','PF0548','PF1205','102755','104192','102955'
)
group by case when SUBSTRING(sdt,1,6) between '201901' and '201903' then 'Q1' when SUBSTRING(sdt,1,6) between '201904' and '201906' then 'Q2' else 'Q3' end,
qdflag,category_code,dept_id,dept_name;
;
--  计算最早销售日期 、最大销售日期
 DROP table
	if EXISTS b2b_tmp.p_temp_min;

create TEMPORARY TABLE
	if not EXISTS b2b_tmp.p_temp_min as select
		a.dist,a.qdflag,a.cust_id,
		min_sale,
		min_sdt,
		max_sale,
		max_sdt
	from
		(
			select a.dist,a.qdflag,a.cust_id,
			sum(sales)min_sale ,
			sdt as min_sdt
		from
			b2b_tmp.p_temp_goods a
		JOIN (
				select dist,qdflag,cust_id,
				min(sdt)min_sdt
			from
				b2b_tmp.p_temp_goods
			group by
				dist,qdflag,cust_id)b on
			a.cust_id = b.cust_id and a.dist=b.dist and a.qdflag=b.qdflag
			and sdt = min_sdt
		group by
			a.dist,a.qdflag,a.cust_id,a.sdt)a
	left OUTER JOIN (
			select a.dist,a.qdflag,a.cust_id,
			sum(sales)max_sale ,
			sdt as max_sdt
		from
			b2b_tmp.p_temp_goods a
		JOIN (
				select a.dist,a.qdflag,a.cust_id,
				max(sdt)max_sdt
			from
				b2b_tmp.p_temp_goods a
			group by
				dist,qdflag,cust_id)b on
			a.cust_id = b.cust_id and a.dist=b.dist and a.qdflag=b.qdflag
			and sdt = max_sdt
		group by
			a.dist,a.qdflag,a.cust_id,
			a.sdt)b on
		a.cust_id = b.cust_id and a.dist=b.dist and a.qdflag=b.qdflag;
-- 计算SKU数
 drop table
	if EXISTS b2b_tmp.p_temp_sku;

create temporary table
	if not EXISTS b2b_tmp.p_temp_sku as select
		substr(sdt,1,6) mon,
		a.dist,a.qdflag,a.cust_id,
		category_code ,
		count(distinct goods_code)sku
	from
		b2b_tmp.p_temp_goods a
	group by
		a.dist,a.qdflag,a.cust_id,substr(sdt,1,6),category_code;
--	select * from  b2b_tmp.p_temp_sku where cust_id='0000103044' and mon='201807'
-- 计算销售频次
 drop table
	b2b_tmp.p_temp_sdt;

create temporary table
	b2b_tmp.p_temp_sdt as select
		sdt ,dist,qdflag,
		cust_id,cust_name, category_code ,
		sum(sale)sale ,
		sum(profit)profit ,
		COUNT( DISTINCT CASE when sale != 0 then a.cust_id end )sales_frequency
	from
		(
		select
			sdt ,dist,qdflag,
		cust_id,cust_name, category_code ,
			sum(sales)sale ,
			sum(profit)profit
		from
			b2b_tmp.p_temp_goods a
		group by
			dist,qdflag,
		cust_id,cust_name, category_code,sdt  ) a
	group by
		dist,qdflag,
		cust_id,cust_name, category_code,sdt  ;
-- 月份转横向
 select
	b.region_province_name ,
	a.dist,
	a.cust_id ,
	b.customer_name ,
	b.sflag ,
	a.qdflag,
	a.category_code,
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
	sku,
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
		select a.qdflag,dist,cust_id,cust_name,
		category_code,
		sum(sale)sale ,
		sum(profit)profit ,
		sum(sales_frequency)sales_frequency ,
		sum(sale)/sum(sales_frequency) atv
		-- ,concat_ws(',' ,	collect_set(substr(sdt ,1 ,	6))) 
		,substr(sdt ,1 ,	6) mon_s
	from
		b2b_tmp.p_temp_sdt a
	group by
		 a.qdflag,dist,cust_id,cust_name,
		category_code		,substr(sdt ,1 ,	6)
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
	a.cust_id = j.cust_id 	and a.mon_s=j.mon and a.category_code=j.category_code and a.dist=j.dist and a.qdflag=j.qdflag
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
	category_code,
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

-- 部类销售分析
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

