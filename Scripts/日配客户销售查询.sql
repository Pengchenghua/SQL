create temporary table temp.csx_sale_01
as 
select
	province_code,
	province_name,
	case when a.division_code in ('12','13') then '12' else a.division_code end bd_id,
	case when a.division_code in ('12','13') then '食百' else a.division_name end bd_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	sum(sales_qty)qty,
	sum(sales_value)sale,
	sum(profit)profit
from
	csx_dw.customer_sale_m a
join (
	select
		customer_no,
		ATTRIBUTE
	from
		csx_dw.customer_m
	where
		sdt = '20200226'
		and attribute ='日配' and first_category_code in ('21','23'))b on
	a.customer_no = b.customer_no
	and sdt >= '20200101'
	and sdt <= '20200131'
	and order_kind !='WELFARE'
	GROUP by 
	province_code,
	province_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	case when a.division_code in ('12','13') then '12' else a.division_code end ,
	case when a.division_code in ('12','13') then '食百' else a.division_name end 
	;


select 	province_code,
	province_name,
	bd_id,
	bd_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	qty,
	sale,
	profit,
	sale_ratio,
	all_sale_ratio
from(
select 	province_code,
	province_name,
	bd_id,
	bd_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	qty,
	sale,
	profit,
	sale/sum(sale)over(partition by bd_id,province_code )sale_ratio,
	sale/sum(sale)over(partition by bd_id )all_sale_ratio
	from  temp.csx_sale_01
	where province_name not like '平台%'
union all 
select 	province_code,
	province_name,
	bd_id,
	bd_name,
	''division_code,
	'小计'division_name,
	''department_code,
	''department_name,
	''category_large_code,
	''category_large_name,
	sum(qty)qty,
	sum(sale)sale,
	sum(profit)profit,
	0 sale_ratio,
	0 all_sale_ratio
	from  temp.csx_sale_01
		where province_name not like '平台%'
	group by 
	province_code,
	province_name,
	bd_id,
	bd_name
union all 
select 	province_code,
	province_name,
	''bd_id,
	'合计'bd_name,
	''division_code,
	''division_name,
	''department_code,
	''department_name,
	''category_large_code,
	''category_large_name,
	sum(qty)qty,
	sum(sale)sale,
	sum(profit)profit,
	0 sale_ratio,
	0 all_sale_ratio
	from  temp.csx_sale_01
		where province_name not like '平台%'
	group by 
	province_code,
	province_name
)a 	order by province_code,bd_id;
	

select 	''province_code,
	''province_name,
	bd_id,
	bd_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	qty,
	sale,
	profit,
sale_ratio,
    all_sale_ratio
	from 
	(
select 	''province_code,
	''province_name,
	bd_id,
	bd_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	qty,
	sale,
	profit,
	sale/sum(sale)over(partition by bd_id,province_code )sale_ratio,
    ''all_sale_ratio
	from 
(select 	''province_code,
	''province_name,
	bd_id,
	bd_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	sum(qty)qty,
	sum(sale)sale,
	sum(profit)profit
	--sale/sum(sale)over(partition by bd_id,province_code )sale_ratio,
--	sale/sum(sale)over(partition by bd_id )all_sale_ratio
	from  temp.csx_sale_01
		where province_name not like '平台%'
	group by
		bd_id,
	bd_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name
	)a
union all 
select 	''province_code,
	''province_name,
	bd_id,
	bd_name,
	''division_code,
	''division_name,
	''department_code,
	''department_name,
	''category_large_code,
	''category_large_name,
	sum(qty)qty,
	sum(sale)sale,
	sum(profit)profit,
	0 sale_ratio,
	0 all_sale_ratio
	from  temp.csx_sale_01
		where province_name not like '平台%'
	group by 
	bd_id,
	bd_name
union all 
select 	''province_code,
	''province_name,
	''bd_id,
	''bd_name,
	''division_code,
	''division_name,
	''department_code,
	''department_name,
	''category_large_code,
	''category_large_name,
	sum(qty)qty,
	sum(sale)sale,
	sum(profit)profit,
	0 sale_ratio,
	0 all_sale_ratio
	from  temp.csx_sale_01
		where province_name not like '平台%'
	
)a order by province_code,bd_id