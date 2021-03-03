select
	province_code ,
	province_name,
	customer_no,
	customer_name,
	b.new_id ,
	b.new_classify_name ,
	a.second_category_code ,
	a.second_category,
	sum(sales_value)sale,
	sum(profit)proft
from
	csx_dw.dws_sale_r_d_customer_sale a
join csx_tmp.new_customer_classify b on
	a.second_category_code = b.second_category
where
	sdt >= '20201101'
	and sdt <= '20201130'
	and channel in ('1','3','7','9')
group by 
province_code ,
	province_name,
	customer_no,
	customer_name,
	b.new_id ,
	b.new_classify_name ,
	a.second_category_code ,
	a.second_category;
	
with cust_sale as 
(select
	province_code ,
	province_name,
	customer_no,
	customer_name,
	b.new_id ,
	b.new_classify_name ,
	a.second_category_code ,
	a.second_category,
	sum(sales_value)sale,
	sum(profit)profit
from
	csx_dw.dws_sale_r_d_customer_sale a
join csx_tmp.new_customer_classify b on
	a.second_category_code = b.second_category
where
	sdt >= '20201101'
	and sdt <= '20201130'
	and channel in ('1','3','7','9')
group by 
province_code ,
	province_name,
	customer_no,
	customer_name,
	b.new_id ,
	b.new_classify_name ,
	a.second_category_code ,
	a.second_category
)
select 
	province_code,
	province_name,
	new_id,
	new_classify_name,
	cust_num,
	cust_num/sum(cust_num)over(partition by  province_code) as cust_ratio,
	sale,
	sale/sum(sale)over(partition by  province_code) as sale_ratio,
	profit,
	profit/sale profit_rate
from 
(select
	province_code,
	province_name,
	new_id,
	new_classify_name,
	count(distinct customer_no) cust_num,
	sum(sale)sale,
	sum(profit)profit
from cust_sale
group by 
	province_code,
	province_name,
	new_id,
	new_classify_name
) a ;

select * from csx_tmp.new_customer_classify;