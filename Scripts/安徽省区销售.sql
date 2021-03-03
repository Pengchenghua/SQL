
select
	channel_name,
	sales_supervisor_name,
	sum(case when mon='201909' then sale end ) curr_sale,
	sum(case when mon='201909' then profit end ) curr_profit,
	sum(case when mon='201908' then sale end ) last_sale,
	sum(case when mon='201908' then profit end ) last_profit,
	count(case when mon='201909' then customer_name end ) curr_cust_cn,
	COUNT(case when mon='201908' then customer_name end) last_cust_cn,
	COUNT(DISTINCT sales_name) sales_cn
from
	(
	SELECT
		channel_name,
		sales_supervisor_name,
		sales_supervisor_work_no,
		SUBSTRING(sdt,1,6)mon,
		sales_name,
		customer_name,
		sum(sales_value)/10000 sale,
		SUM(profit)/10000 profit
	FROM
		csx_dw.sale_goods_m
	where
		sdt >= '20190901' and
		sdt <='20190924'
		 and province_name like '安徽%'
	group by
		channel_name,
		sales_supervisor_name,
		sales_supervisor_work_no,
		SUBSTRING(sdt,1,6),
		sales_name,
		customer_name
	union all 
	SELECT
		channel_name,
		sales_supervisor_name,
		sales_supervisor_work_no,
		SUBSTRING(sdt,1,6)mon,
		sales_name,
		customer_name,
		sum(sales_value)/10000 sale,
		SUM(profit)/10000 profit
	FROM
		csx_dw.sale_goods_m
	where
		sdt >= '20190801' and sdt<='20190824'
		 and province_name like '安徽%'
	group by
		channel_name,
		sales_supervisor_name,
		sales_supervisor_work_no,
		SUBSTRING(sdt,1,6),
		sales_name,
		customer_name
	) a
group by
	channel_name,
		sales_supervisor_name;

select case when channel ='M端' then '商超（对内）' else channel end channelh,COUNT(customer_no)cust_cn,COUNT(case when to_date(sign_time)>='2019-09-01' then customer_no end )new_cust_cn from csx_dw.customer_m 
where sales_province like '安徽%' and ((customer_status='04' and `source`='crm') or `source`='sap')  and sdt='20190924'
group by case when channel ='M端' then '商超（对内）' else channel end ;

select a.*,b.prov_name from csx_dw.customer_m  a 
JOIN
dim.dim_shop_latest b 
on a.customer_no=CONCAT('S',b.shop_id)
and   a.sdt='20190924' and b.prov_name='安徽省' ;



	SELECT
		channel_name,
		sales_supervisor_name,
		sales_supervisor_work_no,
		SUBSTRING(sdt,1,6)mon,
		sum(sales_value)/10000 sale,
		SUM(profit)/sum(sales_value) profit
	FROM
		csx_dw.sale_goods_m
	where
		-- sdt >= '20190901' and
		sdt ='20190924'
		 and province_name like '安徽%'
	group by
		channel_name,
		sales_supervisor_name,
		sales_supervisor_work_no,
		SUBSTRING(sdt,1,6)
		;
	
	select case when channel ='M端' then '商超（对内）' else channel end channelh,first_supervisor_name,COUNT(customer_no)cust_cn,COUNT(case when to_date(sign_time)>='2019-09-01' then customer_no end )new_cust_cn from csx_dw.customer_m 
where sales_province like '安徽%' and ((customer_status='04' and `source`='crm') or `source`='sap')  and sdt='20190924'
group by first_supervisor_name,case when channel ='M端' then '商超（对内）' else channel end ;

select * from csx_dw.customer_m where sdt='20190924' and `source`='crm';
select * from csx_dw.sale_item_m LIMIT 1000
;


-- SELECT count(*) from csx_dw.goods_prices_m where price_begin_time>='20190914' and price_end_time<='20190920' and sdt ='20190924';

