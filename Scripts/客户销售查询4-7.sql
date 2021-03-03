select
	mon,
	a.dist
	, channel,
	qdflag
	, a.cust_id
	, customer_name
	--,concat(a.cust_id ,cust_name )full_cust
	, b.first_category
	, second_category
	, third_category
	, sign_time
	, sign_amount
	, sales_name
	, work_no
	, first_supervisor_name
	, sum(sale)sale
	, sum(profit)profit
	, sum(hz)hz
from
	(
	select
		a.qdflag
		, dist
		, substr( sdt, 1, 6)mon
		, cust_id
		, sum(sale)sale
		, SUM (profit) profit
		, COUNT
		(case
			when sale != 0 then cust_id
		end )hz
	from
		(
		select
			qdflag
			, a.dist
			, a.sdt
			, a.cust_id
			, sum(xse)sale
			, SUM (mle) profit
		from
			csx_dw.sale_warzone01_detail_dtl a
		where
			sdt >= '20190101'
			and sdt <= '20190731' and qdflag ='B¶Ë'
		group by
			qdflag
			, a.dist
			, a.sdt
			, a.cust_id )a
	group by
		a.qdflag
		, dist
		, substr( sdt
		, 1
		, 6)
		, cust_id )a
 JOIN (
	select
		customer_no
		, customer_name
		, channel
		, sales_id
		, sales_name
		, work_no
		, b.first_supervisor_name
		, b.second_supervisor_name
		, b.third_supervisor_name
		, b.first_category
		, b.second_category
		, b.third_category
		, regexp_replace(to_date(b.sign_time), '-', '')sign_time
		, b.sign_amount
		,org_name
		,sales_province
	from
		csx_dw.customer_m b
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP()
		, 1))
		, '-'
		, '')) b on
	a.cust_id = b.customer_no 
group by
	a.dist
	, channel
	,qdflag
	, a.cust_id
	, customer_name
	, b.first_category
	, second_category
	, third_category
	, sign_time
	, sign_amount
	, sales_name
	, work_no
	, first_supervisor_name
	,mon
	;
	

select cust_id ,concat(cust_id ,' ',cust_name )full_cust 
from csx_dw.sale_warzone01_detail_dtl where
sdt>=CONCAT (SUBSTRING (regexp_replace(to_date(date_sub(current_timestamp(),1)),'-',''),1,6),'01')
group by cust_id ,concat(cust_id ,' ',cust_name );

select * from csx_dw.sale_org_m WHERE sales_name like 'ÓáĞ¡Æ½';

select qdflag ,cust_id,cust_name ,sum(xse )sale from csx_dw.sale_warzone01_detail_dtl WHERE sdt>='20190801'and sdt<='20190818'  group by qdflag ,cust_id,cust_name;
select * from csx_dw.report_big_cust_sale_v2  WHERE sdt ='20190818';



select
		*
	from
		csx_dw.customer_m b where customer_no ='102787' ;
	
	
select
		channel_name ,
		SUM(sales_value)sale ,
		SUM(profit)profit
	from
		csx_dw.sale_goods_m as sgm 
	where
		sdt >= '20190101'
		and sdt <= '20190930'and province_code !='33'
	GROUP by
		channel_name ;
	
select * from csx_dw.sale_goods_m as sgm  where customer_no ='1'
