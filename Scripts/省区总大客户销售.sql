 INVALIDATE METADATA csx_dw.sale_goods_m;
-- 
select * from csx_dw.sale_goods_m as sgm where category_large_code=''

SELECT
	channel ,
	first_supervisor_code ,
	first_supervisor_work_no ,
	first_supervisor_name ,
	COUNT (
		DISTINCT sales_name
	)
from
	csx_dw.customer_m as cm
where
	sdt = '20191009'
	and `source` = 'crm'
	and customer_no <> ''
	and first_supervisor_code <> ''
group by
	channel ,
	first_supervisor_code,
	first_supervisor_name,
	first_supervisor_work_no ;

select
	cm.channel ,
	first_supervisor_code ,
	first_supervisor_name ,
	first_supervisor_work_no ,
	sales_province ,
	sales_province_code ,
	sum(new_cust_cn)new_cust_cn,
	sum(ring_cust_cn)new_cust_cn ,
	sum(sale)sale,
	sum(profit)profit ,
	sum(ring_sale)ring_sale ,
	sum(ring_profit)ring_profit
from
	(
		select
			channel ,
			first_supervisor_code ,
			first_supervisor_name ,
			first_supervisor_work_no ,
			sales_province ,
			sales_province_code ,
			customer_no ,
			COUNT (
				case
					when SUBSTRING (
						regexp_replace(
							to_date(sign_time),
							'-',
							''
						),
						1,
						6
					)= SUBSTRING (
						regexp_replace(
							to_date(
								date_sub(
									CURRENT_TIMESTAMP (),
									1
								)
							),
							'-',
							''
						),
						1,
						6
					) then 1
				end
			) as new_cust_cn,
			COUNT (
				case
					when SUBSTRING (
						regexp_replace(
							to_date(sign_time),
							'-',
							''
						),
						1,
						6
					)= SUBSTRING (
						regexp_replace(
							to_date(
								add_months(
									date_sub(
										CURRENT_TIMESTAMP (),
										1
									),
									-1
								)
							),
							'-',
							''
						),
						1,
						6
					) then 1
				end
			) as ring_cust_cn
		from
			csx_dw.customer_m as cm
		where
			sdt = '20191009'
			and customer_no <> ''
			and `source` = 'crm'
		group by
			channel ,
			first_supervisor_code ,
			first_supervisor_name ,
			first_supervisor_work_no ,
			sales_province ,
			sales_province_code,
			customer_no
	)cm
left join (
		select
			channel_name,
			customer_no ,
			sum(case when sdt >= '20191001' then sales_value end ) sale,
			sum(case when sdt >= '20191001' then profit end ) profit,
			sum(case when sdt >= '20190901' and sdt <= '20190909' then sales_value end ) as ring_sale,
			sum(case when sdt >= '20190901' and sdt <= '20190909' then profit end ) as ring_profit
		from
			csx_dw.sale_goods_m as sgm
		where
			sdt >= '20190901'
			and sdt <= '20191009'
		group by
			customer_no ,
			channel_name
	)sgm on
	cm.customer_no = sgm.customer_no
	and channel = channel_name
GROUP by
	cm.channel ,
	first_supervisor_code ,
	first_supervisor_name ,
	first_supervisor_work_no ,
	sales_province ,
	sales_province_code;

select
	channel ,
	sales_id ,
	sales_name ,
	work_no ,
	sales_province ,
	sales_province_code ,
	new_cust_cn,
	ring_cust_cn ,
	diff_cust_cn,
	sale,
	profit ,
	ifnull((profit)/(sale), 0) as prorate,
	ring_sale ,
	(
		sale-ring_sale
	)/(ring_sale) as ring_sale_rate,
	ring_profit,
	sale_cust_data,
	rank() over(
		partition by sales_province,
		channel
	order by
		sale desc
	)
from
	(
		select
			cm.channel ,
			sales_id ,
			sales_name ,
			work_no ,
			sales_province ,
			sales_province_code ,
			sum(new_cust_cn)new_cust_cn,
			sum(ring_cust_cn)ring_cust_cn ,
			sum(new_cust_cn)-sum(ring_cust_cn) as diff_cust_cn,
			sum(ifnull(sale, 0)) as sale,
			sum(IFNULL (profit, 0))as profit ,
			sum(ifnull(ring_sale, 0))ring_sale ,
			sum(ifnull(ring_profit, 0))ring_profit,
			count(distinct case when sale <> 0 then sgm.customer_no end) as sale_cust_data
		from
			(
				select
					channel ,
					sales_id ,
					sales_name ,
					work_no ,
					sales_province ,
					sales_province_code ,
					customer_no ,
					COUNT (
						case
							when SUBSTRING (
								regexp_replace(
									to_date(sign_time),
									'-',
									''
								),
								1,
								6
							)= SUBSTRING (
								regexp_replace(
									to_date(
										date_sub(
											CURRENT_TIMESTAMP (),
											1
										)
									),
									'-',
									''
								),
								1,
								6
							) then 1
						end
					) as new_cust_cn,
					COUNT (
						case
							when SUBSTRING (
								regexp_replace(
									to_date(sign_time),
									'-',
									''
								),
								1,
								6
							)= SUBSTRING (
								regexp_replace(
									to_date(
										add_months(
											date_sub(
												CURRENT_TIMESTAMP (),
												1
											),
											-1
										)
									),
									'-',
									''
								),
								1,
								6
							) then 1
						end
					) as ring_cust_cn
				from
					csx_dw.customer_m as cm
				where
					sdt = '20191009'
					and customer_no <> ''
					and `source` = 'crm'
					--${if(len(province)==0,"","and sales_province  in ('"+province+"')")}
					--${if(len(check)==0,"","and channel  in ('"+check+"')")}

					group by channel ,
					sales_id ,
					sales_name ,
					work_no ,
					sales_province ,
					sales_province_code,
					customer_no
			)cm
		left join (
				select
					channel_name,
					customer_no ,
					sum(case when sdt >= '20191001' then sales_value end )* 1.00 sale,
					sum(case when sdt >= '20191001' then profit end ) profit,
					sum(case when sdt >= '20190901' and sdt <= '20190909' then sales_value end ) as ring_sale,
					sum(case when sdt >= '20190901' and sdt <= '20190909' then profit end ) as ring_profit
				from
					csx_dw.sale_goods_m as sgm
				where
					sdt >= '20190901'
					and sdt <= '20191009'
				group by
					customer_no ,
					channel_name
			)sgm on
			cm.customer_no = sgm.customer_no
			and channel = channel_name
		GROUP by
			cm.channel ,
			sales_id ,
			sales_name ,
			work_no ,
			sales_province ,
			sales_province_code
	)a
order by
	sale desc;
-- 客户top10；
 select
	channel ,
	channel_name ,
	customer_no ,
	customer_name ,
	province_code ,
	province_name ,
	sum(sales_value) sale,
	sum(profit) profit,
	COUNT(DISTINCT  sdt,customer_no ) sale_cust_cn
from
	csx_dw.sale_goods_m as sgm
where sdt>='20190901' and sdt<='20190930'
group by 
channel ,
	channel_name ,
	customer_no ,
	customer_name ,
	province_code ,
	province_name
	;

select
	a.channel ,
	channel_name ,
	a.province_code ,
	province_name ,
	category_code ,
	category_name ,
	category_large_code ,
	category_large_name ,
	sale,
	profit ,
	all_sale,
	sale / all_sale as sale_ratio
from
	(
		select
			sgm.channel ,
			sgm.channel_name ,
			sgm.province_code ,
			sgm.province_name ,
			sgm.category_code ,
			sgm.category_name ,
			category_large_code ,
			category_large_name ,
			sum(sales_value) sale,
			sum(profit) profit
		from
			csx_dw.sale_goods_m as sgm
		where
			sgm .sdt >= '20190901'
			and sgm.sdt <= '20190930'
		group by
			sgm.channel ,
			sgm.channel_name ,
			sgm.province_code ,
			sgm.province_name ,
			sgm.category_code ,
			sgm.category_name ,
			category_large_code ,
			category_large_name
	) a
left join (
		select
			channel ,
			province_code ,
			sum(sales_value) all_sale
		from
			csx_dw.sale_goods_m as a
		where
			sdt >= '20190901'
			and sdt <= '20190930'
		group by
			channel ,
			province_code
	) b on
	a.channel = b.channel
	and a.province_code = b.province_code
order by
	sale_ratio desc 
;

-- 企业属性销售表查询
select
	channel ,
	channel_name,
	province_code,
	province_name ,
	first_category ,
	first_category_code ,
	sum(sales_value) sale
from
	csx_dw.sale_goods_m as sgm
where
	sdt >= '20190901'
	and sdt <= '20191010'
group by
	channel ,
	channel_name,
	province_code,
	province_name ,
	first_category ,
	first_category_code 
	;

select sgm.channel ,channel_name ,substring(sdt,5)sdt ,sum(sales_value )/10000 sale,sum(profit )/10000 profit,sum(profit )/sum(sales_value ) profit_rate  from csx_dw.sale_goods_m as sgm
where sdt>='20190901' and sdt<='20190930' 
--${if(len(province)==0,"","and province_name in ('"+province+"')")}
--${if(len(check)==0,"","and channel_name in ('"+check+"')")}
group by sgm.channel ,channel_name,sdt
order by sdt;


select
	case when province_name like '平台-B' then '平台' else channel_name end channel_name,
	province_code,
	province_name ,
	sum(sales_value) sale
from
	csx_dw.sale_goods_m as sgm
where
	sdt >= '20190901'
	and sdt <= '20191010'
group by
	case when province_name like '平台-B' then '平台' else channel_name end,
	province_code,
	province_name 
	;
select a.customer_no,a.customer_name,b.sale ,b.sdt,channel_name from 
(select * from csx_dw.customer_m  where sdt='20191010'  and channel ='其它')a
 join 
(select sdt,channel_name,customer_no,sum(sales_value) sale from csx_dw.sale_goods_m  group by sdt,customer_no ,channel_name)b on a.customer_no=b.customer_no;

select * from csx_dw.customer_m where sdt='20191010'and sales_province like '福建%'  and customer_no ='' and sales_city like '南平%';