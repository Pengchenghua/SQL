--业绩-业务类型 1211
select
	a.province_name,a.smonth,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
		when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
		when a.channel in ('1','9') and b.attribute='贸易客户' and d.order_profit_rate<=0.015 then '批发内购' 
		when a.channel in ('1','9') and b.attribute='贸易客户' and (d.order_profit_rate>0.015 or d.order_profit_rate is null) then '省区大宗'
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
		sum(front_profit) as front_profit
	from csx_dw.dws_sale_r_d_customer_sale 
	where sdt >= '20201101' and sdt < '20201201' 
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
	where sdt >= '20201001' and sdt < '20201201' and channel in ('1','7','9')
	group by coalesce(origin_order_no,order_no)
	)d on a.order_no_new = d.order_no_new 	
group by a.province_name,a.smonth,
	case when a.channel ='7' then 'BBC'	
		when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
		when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
		when a.channel in ('1','9') and b.attribute='贸易客户' and d.order_profit_rate<=0.015 then '批发内购' 
		when a.channel in ('1','9') and b.attribute='贸易客户' and (d.order_profit_rate>0.015 or d.order_profit_rate is null) then '省区大宗'
		when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
		else '日配单' end;