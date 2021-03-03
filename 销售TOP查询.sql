select
	province_code      ,
	province_name      ,
	goods_code         ,
	goods_name         ,
	division_code      ,
	division_name      ,
	category_large_code,
	category_large_name,
	sale               ,
	profit             ,
	sales_qty          ,
	NUM
from
	(
		select
			province_code      ,
			province_name      ,
			goods_code         ,
			goods_name         ,
			division_code      ,
			division_name      ,
			category_large_code,
			category_large_name,
			sale               ,
			profit             ,
			sales_qty          ,
			rank()over(PARTITION by category_large_code order by
					   sale desc) as NUM
		from
			(
				select
					province_code          ,
					province_name          ,
					goods_code             ,
					goods_name             ,
					division_code          ,
					division_name          ,
					category_large_code    ,
					category_large_name    ,
					sum(sales_value)sale   ,
					sum(profit)     profit ,
					sum(sales_qty)  sales_qty
				from
					csx_dw.customer_sale_m a
					join
						(
							select
								customer_no,
								cm.`attribute`
							from
								csx_dw.customer_m cm
							where
								sdt                =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
								${if (len(types==0),"","and cm.`attribute`!='5'")}
								and customer_no   !=''
						)
						as b
						on
							a.customer_no=b.customer_no
				where
					sdt    >='20190101'
					and sdt<='20191231'
				group by
					goods_code         ,
					goods_name         ,
					division_code      ,
					division_name      ,
					category_large_code,
					category_large_name,
					province_code      ,
					province_name
			)
			a
	)
	a
where
	num <31
	and division_code in ('10',
						  '11')
;