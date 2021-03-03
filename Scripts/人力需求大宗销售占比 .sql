
select
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name,
	sale,
	sale/sum(sale)over() as sale_ratio,
	profit
from (select
	department_code,
	department_name,
	category_middle_code,
	category_middle_name,
	category_large_code,
	category_large_name,
	sum(sales_value)/10000 as  sale,
	sum(profit)/10000 as profit
from
	csx_dw.customer_sale_m
where
	channel_name like '%大宗%'
	and sdt <= '20191231'
	and sdt >= '20190101'
	group by department_code,
	department_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name
) a  
order by sale_ratio desc ;

-- 客户分类销售占比
select
	customer_no,
	customer_name,
	first_category,
	second_category,
	sale,
	sale/sum(sale)over() as sale_ratio,
	profit
from (

select
	customer_no,
	customer_name,
	first_category,
	second_category,
	sum(sales_value)/10000 as  sale,
	sum(profit)/10000 as profit
from
	csx_dw.customer_sale_m
where
	channel_name like '%大宗%'
	and sdt <= '20191231'
	and sdt >= '20190101'
	group by customer_no,
	customer_name,
	first_category,
	second_category
)a order by sale_ratio desc ;

-- 逾期客户情况 
select * from csx_dw.receivables_collection where sdt='20191231' and channel='大宗'