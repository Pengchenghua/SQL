业务类型（新逻辑20201217）
-- INVALIDATE METADATA csx_tmp.sale_01;
--业绩-业务类型 1211
SET hive.execution.engine=spark; 
-- 相关字段：数、总数
drop table csx_tmp.sale_01;
create table csx_tmp.sale_01 as 
select
    a.sdt,
	a.province_name,
	province_code,
	a.city_group_name,
	a.customer_no,
	b.customer_name,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and b.attribute='合伙人' then '城市服务商' 
		when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
		when a.channel in ('1','9') and b.attribute='贸易' and d.order_profit_rate<=0.015 then '批发内购' 
		when a.channel in ('1','9') and b.attribute='贸易' and (d.order_profit_rate>0.015 or d.order_profit_rate is null) then '省区大宗'
		when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
		else '日配单' end sale_group, 
	a.channel,
	channel_name,
	goods_code,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	a.category_small_code,
	a.category_small_name,
	sum(sales_cost) as sales_cost,
	sum(a.sales_qty)sales_qty,
	sum(sales_value)as sales_value,
	sum(profit)as profit,
	sum(front_profit) as front_profit
from 
	(select 
	    sdt,
	    a.channel_name,
		channel,
		a.city_group_name,
		province_name,
		province_code,
		substr(sdt,1,6) smonth,
		origin_order_no,
		order_no,
		goods_code,
		classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name,
	    a.category_small_code,
	    a.category_small_name,
		coalesce(origin_order_no,order_no) order_no_new,
		customer_no,
		order_kind,
		sum(sales_cost) as sales_cost,
		sum(a.sales_qty)sales_qty,
		sum(sales_value)as sales_value,
		sum(profit)as profit,
		sum(front_profit) as front_profit
	from csx_dw.dws_sale_r_d_customer_sale a
	join 
	(select goods_id,
	    classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
	where sdt >= '20190101' and sdt < '20201211' 
	and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
	and channel in('1','7','9')	
	and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)	
	group by 
	    a.sdt,
	    channel,
	    a.city_group_name,
	    province_code,
	    province_name,
	    origin_order_no,
	    order_no,
	    goods_code,
	    coalesce(origin_order_no,order_no),
	    customer_no,
	    order_kind,
	    classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name,
	    a.category_small_code,
	    a.category_small_name,
	    a.channel_name
	)a  
left join   
	(select *
		from csx_dw.dws_crm_w_a_customer_m_v1 
		where sdt=regexp_replace(date_sub(current_date,1),'-','')
	)b on b.customer_no=a.customer_no
left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
	(
	select coalesce(origin_order_no,order_no) order_no_new, 
	sum(profit)/abs(sum(sales_value)) order_profit_rate
	from csx_dw.dws_sale_r_d_customer_sale
	where sdt >= '20190101' 
	    and sdt < '20201211' 
	    and channel in ('1','7','9')
	group by 
	    coalesce(origin_order_no,order_no)
	)d on a.order_no_new = d.order_no_new 	
group by a.province_name,
        channel_name,
        province_code,
        a.customer_no,
        a.city_group_name,
        goods_code,
        a.channel,
        b.customer_name,
        classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and b.attribute='合伙人' then '城市服务商' 
		when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
		when a.channel in ('1','9') and b.attribute='贸易' and d.order_profit_rate<=0.015 then '批发内购' 
		when a.channel in ('1','9') and b.attribute='贸易' and (d.order_profit_rate>0.015 or d.order_profit_rate is null) then '省区大宗'
		when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
		else '日配单' end
		,a.sdt,
		a.category_small_code,
	    a.category_small_name;
		
		
		
		

-- INVALIDATE METADATA csx_tmp.sale_01;
--业绩-业务类型 1222
SET hive.execution.engine=spark; 
set sdt='20190101';
set edt='20201221';
-- 相关字段：数、总数
drop table csx_tmp.sale_01;
create table csx_tmp.sale_01 as 
select
    a.sdt,
    a.dc_code,
	a.province_name,
	province_code,
	a.city_group_name,
	a.customer_no,
	b.customer_name,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and b.attribute='合伙人' then '城市服务商' 
		when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
		when a.channel in ('1','9') and b.attribute='贸易' and coalesce(d.order_profit_rate,a.order_profit_rate)<=0.015 then '批发内购' 
		when a.channel in ('1','9') and b.attribute='贸易' and (coalesce(d.order_profit_rate,a.order_profit_rate)>0.015 or coalesce(d.order_profit_rate,a.order_profit_rate) is null) then '省区大宗'
		when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
		else '日配单' end sale_group, 	
	a.channel,
	channel_name,
	goods_code,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	a.category_small_code,
	a.category_small_name,
	sum(sales_cost) as sales_cost,
	sum(a.sales_qty)sales_qty,
	sum(sales_value)as sales_value,
	sum(profit)as profit,
	sum(front_profit) as front_profit
from 
	(select 
	    sdt,
	    a.channel_name,
		channel,
		a.city_group_name,
		province_name,
		province_code,
		a.dc_code,
		substr(sdt,1,6) smonth,
		origin_order_no,
		order_no,
		goods_code,
		classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name,
	    a.category_small_code,
	    a.category_small_name,
		coalesce(origin_order_no,order_no) order_no_new,
		customer_no,
		order_kind,
		sum(sales_cost) as sales_cost,
		sum(a.sales_qty)sales_qty,
		sum(sales_value)as sales_value,
		sum(profit)as profit,
		sum(front_profit) as front_profit,
		sum(profit)/abs(sum(sales_value)) order_profit_rate
	from csx_dw.dws_sale_r_d_customer_sale a
	join 
	(select goods_id,
	    classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
	where sdt >=  ${hiveconf:sdt} and sdt <=  ${hiveconf:edt}
	and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
	and channel in('1','7','9')	
	and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)	
	group by 
	    a.sdt,
	    channel,
	    a.city_group_name,
	    province_code,
	    province_name,
	    origin_order_no,
	    order_no,
	    goods_code,
	    coalesce(origin_order_no,order_no),
	    customer_no,
	    order_kind,
	    classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name,
	    a.category_small_code,
	    a.category_small_name,
	    a.channel_name,
	    	a.dc_code
	)a  
left join   
	(select *
		from csx_dw.dws_crm_w_a_customer_m_v1 
		where sdt=regexp_replace(date_sub(current_date,1),'-','')
	)b on b.customer_no=a.customer_no
left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
	(
	select coalesce(origin_order_no,order_no) order_no_new, 
	sum(profit)/abs(sum(sales_value)) order_profit_rate
	from csx_dw.dws_sale_r_d_customer_sale
	where sdt >=  ${hiveconf:sdt}
	    and sdt <=  ${hiveconf:edt}
	    and channel in ('1','7','9')
	group by 
	    coalesce(origin_order_no,order_no)
	)d on a.order_no_new = d.order_no_new 	
group by a.province_name,
        channel_name,
        province_code,
        a.customer_no,
        a.dc_code,
        a.city_group_name,
        goods_code,
        a.channel,
        b.customer_name,
        classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and b.attribute='合伙人' then '城市服务商' 
		when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
		when a.channel in ('1','9') and b.attribute='贸易' and coalesce(d.order_profit_rate,a.order_profit_rate)<=0.015 then '批发内购' 
		when a.channel in ('1','9') and b.attribute='贸易' and (coalesce(d.order_profit_rate,a.order_profit_rate)>0.015 
		    or coalesce(d.order_profit_rate,a.order_profit_rate) is null) then '省区大宗'
		when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
		else '日配单' end , 	
		a.sdt,
		a.category_small_code,
	    a.category_small_name;
		
		
		

select
    region_code,
    region_name,
    province_code,
    a.province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
    sales_value ,
	profit ,
	profit/sales_value as profit_rate,
	per_ticket_sales ,
	sale_cust/all_sale_cust penetration_rate,
	sale_cust,
	all_sale_cust
from 
(select province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	sum(sales_value)/count(distinct customer_no) per_ticket_sales ,
	count(distinct customer_no) as sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
) a
left join
(select province_name,
	count(distinct customer_no) as all_sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name
) b on a.province_name=b.province_name
left join
(select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank=13) c on a.province_name=c.province_name
;


select
    region_code,
    region_name,
    province_code,
    a.province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sale_group,
    sales_value ,
	profit ,
	profit/sales_value as profit_rate,
	per_ticket_sales ,
	sale_cust/all_sale_cust penetration_rate,
	sale_cust,
	all_sale_cust
from 
(select province_name,
	city_group_name,
	sale_group,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	sum(sales_value)/count(distinct customer_no) per_ticket_sales ,
	count(distinct customer_no) as sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name,
	sale_group,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
) a
left join
(select province_name,
	count(distinct customer_no) as all_sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name
) b on a.province_name=b.province_name
left join
(select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank=13) c on a.province_name=c.province_name
;



--业绩-业务类型 1222
select
	a.province_name,a.smonth,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and b.attribute='合伙人' then '城市服务商' 
		when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
		when a.channel in ('1','9') and b.attribute='贸易' and coalesce(d.order_profit_rate,a.order_profit_rate)<=0.015 then '批发内购' 
		when a.channel in ('1','9') and b.attribute='贸易' and (coalesce(d.order_profit_rate,a.order_profit_rate)>0.015 or coalesce(d.order_profit_rate,a.order_profit_rate) is null) then '省区大宗'
		when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
		else '日配单' end sale_group, 	
	sum(a.sales_value)as sales_value,
	sum(a.profit)as profit,
	sum(a.front_profit) as front_profit
from 
	(select 
		channel,province_name,substr(sdt,1,6) smonth,origin_order_no,order_no,coalesce(origin_order_no,order_no) order_no_new,
		customer_no,order_kind,
		sum(sales_value)as sales_value,
		sum(profit)as profit,
		sum(front_profit) as front_profit,
		sum(profit)/abs(sum(sales_value)) order_profit_rate
	from csx_dw.dws_sale_r_d_customer_sale 
	where sdt >= '20201201' and sdt < '20201221' 
	and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
	and channel in('1','7','9')	
	and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)	
	group by channel,province_name,substr(sdt,1,6),origin_order_no,order_no,coalesce(origin_order_no,order_no),customer_no,order_kind
	)a  
left join   
	(select *
		from csx_dw.dws_crm_w_a_customer_m_v1 
		where sdt=regexp_replace(date_sub(current_date,1),'-','')
	)b on b.customer_no=a.customer_no
left join --尽量消除退货单误归到批发内购影响，算毛利率往前找近60天原单
	(
	select coalesce(origin_order_no,order_no) order_no_new, sum(profit)/abs(sum(sales_value)) order_profit_rate
	from csx_dw.dws_sale_r_d_customer_sale
	where sdt >= '20201001' and sdt < '20201221' and channel in ('1','7','9')
	group by coalesce(origin_order_no,order_no)
	)d on a.order_no_new = d.order_no_new 	
group by a.province_name,a.smonth,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and b.attribute='合伙人' then '城市服务商' 
		when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
		when a.channel in ('1','9') and b.attribute='贸易' and coalesce(d.order_profit_rate,a.order_profit_rate)<=0.015 then '批发内购' 
		when a.channel in ('1','9') and b.attribute='贸易' and (coalesce(d.order_profit_rate,a.order_profit_rate)>0.015 or coalesce(d.order_profit_rate,a.order_profit_rate) is null) then '省区大宗'
		when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
		else '日配单' end;