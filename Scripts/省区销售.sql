--销售主管	销售员数	销售频次	销售额	环比销售额	环比率	销售毛利额	毛利额	毛利率	销售频次	数	成交数	签约数 
												
--销售员	销售额	环比销售额	环比率	销售毛利额	毛利额	毛利率	SKU数	销售天数	销售频次  	数	成交数	签约数
												
-- 销售员	编码	名称	签约日期	销售额	环比销售额	环比率	销售毛利额	毛利额	毛利率	SKU数	销售频次  	

-- 主管数
drop table if exists temp.p_sales_01;
create temporary table if not EXISTS temp.p_sale_01 
as 
select channel,sales_province,first_supervisor_name,first_supervisor_work_no,
count(DISTINCT customer_no)cust_all,
count(distinct 
	case when SUBSTRING(regexp_replace(to_date(sign_time),'-',''),1,6)=SUBSTRING(regexp_replace(to_date(date_sub(current_timestamp(),1)),'-',''),1,6) then customer_no end )new_cust_cn
from csx_dw.customer_m
where sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')and sales_province like '福建%'
and (customer_status='04' and `source`='crm')  
group by channel,sales_province,first_supervisor_name,first_supervisor_work_no;

-- 主管销售数据
drop table if exists temp.p_sales_02;
create temporary table if not EXISTS temp.p_sale_02 
as 
select 
	a.province_name,
	channel_name,
	a.sales_supervisor_name,
	sales_supervisor_work_no,
	sum(sale)sale,
	sum(profit)profit,
	sum( goods_cn)goods_cn,
	sum(sale_day)sale_day,
	sum(cust_cn)cust_cn,
	sum(ring_sale) ring_sale
from 
(
select
	a.province_name,
	channel_name,
	a.sales_supervisor_name,
	sales_supervisor_work_no,
	sum(sales_value)sale,
	sum(profit)profit,
	COUNT(DISTINCT goods_code)goods_cn,
	COUNT(DISTINCT sdt)sale_day,
	COUNT(DISTINCT customer_no)cust_cn,
	0 ring_sale
from
	csx_dw.sale_goods_m a
where sdt>='20190901' and sdt<='20190908'  
group by a.province_name,
	a.sales_supervisor_name,
	sales_supervisor_work_no,
	channel_name
union all 
select
	a.province_name,
	channel_name,
	a.sales_supervisor_name,
	sales_supervisor_work_no,
	0 sale,
	0 profit,
	0 goods_cn,
	0 sale_day,
	0 cust_cn,
	sum(sales_value) ring_sale
from
	csx_dw.sale_goods_m a
where sdt>='20190801' and sdt<='20190808'  
group by a.province_name,
	a.sales_supervisor_name,
	sales_supervisor_work_no,
	channel_name
)a 
group by
a.province_name,
	a.sales_supervisor_name,
	sales_supervisor_work_no,
	channel_name
;
select channel,sales_province,first_supervisor_name,first_supervisor_work_no,
	sale,profit,profit/sale*1.00 prorate,(sale-ring_sale)/ring_sale*1.00 ring_rate,
	goods_cn,
	sale_day,
	ring_sale,cust_all,cust_cn,new_cust_cn  from   temp.p_sale_01 a 
left join 
temp.p_sale_02 b 
on a.channel=b.channel_name 
	and a.sales_province=b.province_name 
	and a.first_supervisor_name=b.sales_supervisor_name
	and a.first_supervisor_work_no=b.sales_supervisor_work_no
;

select * from temp.p_sale_01 b  where first_supervisor_name like '企业购';
