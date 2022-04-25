SELECT
	a.channel_name              ,
	province_code               ,
	province_name               ,
	city_name                   ,
	a.customer_no               ,
	customer_name               ,
	cust_name                   ,
	first_category              ,
	second_category             ,
	sales_name                  ,
	a.work_no                   ,
	full_sales_name             ,
	avg_sale                    ,
	goods_cn                    ,
	sale                        ,
	profit                      ,
	profit/sale*1.00 as prorate ,
	sales_cn                    ,
	max_date                    ,
	return_sale
from
	(
		SELECT
			a.channel_name                                      ,
			province_code                                       ,
			province_name                                       ,
			city_name                                           ,
			a.customer_no                                       ,
			customer_name                                       ,
			concat(a.customer_no,' ',customer_name)as cust_name ,
			first_category                                      ,
			second_category                                     ,
			sales_name                                          ,
			a.work_no                                           ,
			concat(a.work_no,' ',sales_name)    full_sales_name    ,
			sale/sales_cn                    as avg_sale           ,
			goods_cn                                               ,
			sale                                                   ,
			profit                                                 ,
			profit/sale*1.00 as prorate                            ,
			sales_cn                                               ,
			max_date                                               ,
			return_sale
		from
			(
				SELECT
					channel_name                          ,
					province_code                         ,
					province_name                         ,
					city_name                             ,
					customer_no                           ,
					customer_name                         ,
					sales_name                            ,
					sales_work_no             as work_no  ,
					COUNT(DISTINCT goods_code)AS goods_cn ,
					sum(sales_value)             sale     ,
					sum(profit)                  profit   ,
					abs(sum
						(
							case
								when return_flag='X'
									then sales_value
							end
						)
					)return_sale
				FROM
					csx_dw.sale_goods_m
				WHERE
					channel in('1' ,'5' ,'6' ,'4' ,'7')
					and
					(
						sdt    >=regexp_replace(to_date(date_sub('${sdt}',1)),'-','')
						and sdt<=regexp_replace(to_date('${edt}'),'-','')
					)
				GROUP BY
					channel_name  ,
					province_code ,
					province_name ,
					city_name     ,
					customer_no   ,
					sales_name    ,
					work_no       ,
					customer_name
			)
			a
			left join
				(
					select
						channel_name ,
						customer_no  ,
						count(distinct sales_date) sales_cn
					from
						csx_dw.sale_goods_m
					WHERE
						(
							sdt    >=regexp_replace(to_date(date_sub('${sdt}',1)),'-','')
							and sdt<=regexp_replace(to_date('${edt}'),'-','')
						)
					group by
						channel_name ,
						customer_no
				)
				b
				on
					a.channel_name   =b.channel_name
					and a.customer_no=b.customer_no
			left join
				(
					select
						channel_name ,
						customer_no  ,
						max(sales_date) max_date
					from
						csx_dw.sale_goods_m
					where
						sdt<=regexp_replace(to_date('${edt}'),'-','')
					group by
						channel_name ,
						customer_no
				)
				c
				on
					a.channel_name   =c.channel_name
					and a.customer_no=c.customer_no
			left join
				(
					select
						customer_no    ,
						first_category ,
						second_category
					from
						csx_dw.customer_m
					where
						sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				)
				d
				on
					a.customer_no=d.customer_no
	)
	a
where
	1=1 
	${if(len(channel)==0,"","and channel_name in ('"+channel+"')")} 
	${if(len(province)==0,"","and province_name in ('"+province+"')")} 
	${if(len(cust)==0,"","and customer_no in ('"+cust+"')")} 
	${if(len(salename)==0,"","and a.work_no in ('"+salename+"')")}
order by
	channel_name ,
	province_code
;