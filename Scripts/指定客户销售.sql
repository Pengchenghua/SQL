with sale as (
select city_group_name,
	city_group_code ,
	first_category ,
	second_category_code ,
	second_category ,
	sum(current_sale)current_sale,
	sum(current_profit)current_profit,
	sum(current_front_profit)current_front_profit,
	sum(daily_order_sale)daily_order_sale,
	sum(daily_order_profit) daily_order_profit,
	sum(daily_order_front_profit)daily_order_front_profit,
	sum(ring_sale) ring_sale,
	sum(ring_profit) ring_profit,
	sum(ring_front_profit) ring_front_profit,
	sum(ring_daily_order_sale) ring_daily_order_sale,
	sum(ring_daily_order_profit) ring_daily_order_profit,
	sum(ring_daily_order_front_profit) ring_daily_order_front_profit
from (
select
	city_group_code ,
	city_group_name,
	first_category ,
	second_category_code ,
	second_category ,
	sum(sales_value)current_sale,
	sum(profit)current_profit,
	sum(front_profit)current_front_profit,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then sales_value 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then sales_value end ) as daily_order_sale,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then profit 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then profit end ) as daily_order_profit,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then front_profit 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then front_profit end ) as daily_order_front_profit,
	0 ring_sale,
	0 ring_profit,
	0 ring_front_profit,
	0 ring_daily_order_sale,
	0 ring_daily_order_profit,
	0 ring_daily_order_front_profit
from
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt >= '20201101'
	and sdt <= '20201126'
	and province_code = '15'
	and channel !='2'
	and second_category_code in ('304','305','306','307','313')
group by city_group_name,
	city_group_code ,
	second_category,
	first_category,
	second_category_code
union all 
select
	city_group_code ,
	city_group_name,
	first_category ,
	second_category_code ,
	second_category ,
	0 current_sale,
	0 current_profit,
	0 current_front_profit,
	0 daily_order_sale,
	0 daily_order_profit,
	0 daily_order_front_profit,
	sum(sales_value) ring_sale,
	sum(profit) ring_profit,
	sum(front_profit) ring_front_profit,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then sales_value 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then sales_value end ) as ring_daily_order_sale,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then profit 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then profit end ) as ring_daily_order_profit,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then front_profit 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then front_profit end ) as ring_daily_order_front_profit
from
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt >= '20201001'
	and sdt <= '20201026'
	and province_code = '15'
	and channel !='2'
	and second_category_code in ('304','305','306','307','313')
group by city_group_name,
	city_group_code ,	
	second_category,
	first_category,
	second_category_code
) a 
group by city_group_name,
	second_category,
	first_category,
	city_group_code ,
	second_category_code
grouping set 
((city_group_name,
	second_category,
	first_category,
	city_group_code ,
	second_category_code),
	())
	)
	select city_group_code ,
	city_group_name,
	first_category ,
	second_category_code ,
	second_category ,
	current_sale,
	current_profit,
	current_front_profit,
    coalesce(current_profit/current_sale ,0) as current_profit_rate,
    coalesce(current_front_profit/current_sale,0) as current_front_profit_rate,
	daily_order_sale,
	daily_order_profit,
	daily_order_front_profit,
    coalesce(daily_order_profit/daily_order_sale,0) as current_daily_order_profit_rate,
    coalesce(daily_order_front_profit/daily_order_sale,0) as current_daily_front_profit_rate,
	ring_sale,
	ring_profit,
	ring_front_profit,
    coalesce(ring_profit/ring_sale,0) as ring_profit,
    coalesce(ring_front_profit/ring_sale,0) as ring_front_profit,
	ring_daily_order_sale,
	ring_daily_order_profit,
	ring_daily_order_front_profit,
    coalesce(ring_daily_order_profit/ring_daily_order_sale,0) as ring_daily_order_profit_rate,
    coalesce(ring_daily_order_front_profit/ring_daily_order_sale,0) as ring_daily_front_profit_rate,
    coalesce(current_profit/current_sale ,0)-coalesce(ring_profit/ring_sale,0) as diff_profit,
    coalesce(current_front_profit/current_sale,0)-coalesce(ring_front_profit/ring_sale,0) as diff_front_profit,
    coalesce(daily_order_profit/daily_order_sale,0)-coalesce(ring_daily_order_profit/ring_daily_order_sale,0) as diff_daily_order_profit_rate,
    coalesce(daily_order_front_profit/daily_order_sale,0)-coalesce(ring_daily_order_front_profit/ring_daily_order_sale,0) as diff_daily_front_profit_rate,
    coalesce((current_sale-ring_sale)/ring_sale,0) sale_growth_rate,
    coalesce((daily_order_sale-ring_daily_order_sale)/ring_daily_order_sale,0) daily_sale_growth_rate
 from sale a 
;
	


select
	city_group_name,
	first_category ,
	b.second_category_code ,
	b.second_category_name ,
	sum(sales_value)current_sale,
	sum(profit)current_profit,
	sum(front_profit)current_front_profit
from
	csx_dw.dws_sale_r_d_customer_sale a 
	join 
	(select
	customer_no ,
	attribute_code,
	second_category_name ,
	second_category_code
from
	csx_dw.dws_crm_w_a_customer_20200924
where
	sdt = 'current') b on a.customer_no =b.customer_no
where
	sdt >= '20201101'
	and sdt <= '20201125'
	and province_code = '15'
	and channel !='2'
--  and division_code in ('12','13' )
--	and attribute_code in (1,2)  
   and order_kind ='WELFARE'
	and b.second_category_code in ('307' )
group by city_group_name,
	b.second_category_name,
	first_category,
	b.second_category_code
grouping sets ((city_group_name,
	b.second_category_name,
	first_category,
	b.second_category_code),())
	;
	

select
	city_group_name,
	first_category ,
	second_category_code ,
	second_category ,
	sum(sales_value)current_sale,
	sum(profit)current_profit,
	sum(front_profit)current_front_profit,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then sales_value 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then sales_value end ) as daily_order_sale,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then profit 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then profit end ) as daily_order_profit,
	sum(case when second_category_code ='304' and customer_no in ('105150','105156','105164','105165','105177','105181','105182','106423','106721','106805','107404') then front_profit 
		when second_category_code !='304' and attribute_code in (1,2) and order_kind !='WELFARE' then front_profit end ) as daily_order_front_profit,
	0 ring_sale,
	0 ring_profit,
	0 ring_front_profit
from
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt >= '20201101'
	and sdt <= '20201125'
	and province_code = '15'
	and channel ='1'
	and second_category_code in ('304','305','306','307','313')
group by city_group_name,
	second_category,
	first_category,
	second_category_code