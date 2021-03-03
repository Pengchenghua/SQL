-- CONNECTION: name= HVIE
drop table if EXISTS b2b_tmp.p_csx_visit;
create TEMPORARY table if not EXISTS b2b_tmp.p_csx_visit
as 
-- 拜访客户数、签约客户数
select a.customer_no,a.customer_name,customer_status,visit_cn,min_visit_time,customer_create_time,customer_sign_time,sale,min_order_sdt
from 
 (select
	customer_no,
	customer_name,
	customer_status,
	count( DISTINCT case when regexp_replace(to_date(visit_time),	'-',	'')>= 			regexp_replace	(to_date	(trunc	(date_sub	(CURRENT_TIMESTAMP(),1),	'MM')),	'-',	'') 
	and regexp_replace(to_date(visit_time),	'-',	'')<=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
		AND regexp_replace(to_date(customer_create_time),	'-',	'')>= regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),	'MM')),	'-','') then  customer_no end ) visit_cn,	
	min(regexp_replace(to_date(visit_time),	'-',	'')) as min_visit_time	,
	regexp_replace(to_date(customer_create_time),	'-',	'') as customer_create_time	,
	regexp_replace(to_date(customer_sign_time),'-','')as customer_sign_time
from
	csx_dw.customer_visit_info
where
	sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by customer_no,
	customer_name,
	customer_status,
	regexp_replace(to_date(customer_create_time),	'-',	''),
	regexp_replace(to_date(customer_sign_time),'-',''))a
left join 
--下单客户数	
(select regexp_replace(sold_to,'(^0*)','')sold_to,customer_name,sum(tax_sale_val)sale,min(a.sdt)min_order_sdt from csx_dw.csx_order_item a
JOIN
(select * from csx_dw.customer_visit_info a where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	and regexp_replace(to_date(customer_create_time),'-','')>=regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),	'MM')),	'-',''))b
on regexp_replace(sold_to,'(^0*)','')=b.customer_no
where a.sdt<=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	and a.sdt>=regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),	'MM')),	'-','')
group by 
sold_to,customer_name)b
on a.customer_no=b.sold_to and a.customer_name=b.customer_name;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table  csx_dw.report_display_visit_cn PARTITION (dt)
select regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') sdt,
	COUNT(case when visit_cn !=0 then customer_no end )visit_cn,
		COUNT(case when customer_create_time>=regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),	'MM')),	'-','') and customer_status='04' then customer_no end  )sign_cn,
		COUNT(case when sale!=0 then customer_no end ) order_cn
		,regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') dt
from  b2b_tmp.p_csx_visit;

select * from csx_dw.report_display_visit_cn