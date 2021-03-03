--大宗二部销售
select substr(sdt,1,6) as mon,
	province_code,
	province_name,
	customer_no,
	customer_name,
	category_large_code ,
	category_large_name,
	classify_middle_code ,
	classify_middle_name ,
	sum(sales_value)sales_value ,
	sum(profit)profit 
from
	csx_dw.dws_sale_r_d_detail
where
	sdt >= '20200101'
	and sdt <= '20201231'
	and channel_code in ('5','6')
group by substr(sdt,1,6),
province_code,
	province_name,
	customer_no,
	customer_name,
	category_large_code ,
	category_large_name,
	classify_middle_code ,
	classify_middle_name 