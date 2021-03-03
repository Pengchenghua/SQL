
 select
	sdt,
	customer_number,
	customer_name,
	sflag,
	round(sum(sale_order), 2)sale_order,
	SUM (ret_order) ret_order,
	sum(sale_ods)sale_ods,
	SUM (ret_sale) ret_sale,
	case
		when round(sum(sale_order), 2)>sum(sale_ods) then concat('sale_ODS表数据缺失||', ifnull(cast(round(sum(sale_order)-sum(sale_ods), 2) as string), ''))
		when round(sum(sale_order), 2)<sum(sale_ods) then concat('sale_ODS表数据多于order_item||', ifnull(cast(round(sum(sale_order)-sum(sale_ods), 2)as string), ''))
		else '数据一致'
	end note
from
	(
		select
			sdt,
			customer_number,
			customer_name,
			sflag,
			sum(case when bill_type not like 'ZR%' THEN tax_sale_val end )sale_order,
			sum(case when bill_type like 'ZR%' THEN tax_sale_val end )ret_order,
			0 sale_ods,
			0 ret_sale
		from
			csx_dw.csx_order_item a
		join (
				select
					customer_number,
					customer_name,
					sflag
				from
					csx_dw.customer_simple_info_v2
				where
					sflag = '大宗'
					and customer_number != '910001'
					and sdt = regexp_replace(
						to_date(
							date_sub(
								CURRENT_TIMESTAMP(),
								1
							)
						),
						'-',
						''
					)
			) b on
			regexp_replace(
				a.sold_to,
				'(^0*)',
				''
			)= customer_number
			and sdt >= regexp_replace(
				to_date(
					trunc(
						date_sub(
							CURRENT_TIMESTAMP(),
							1
						),
						'MM'
					)
				),
				'-',
				''
			)
			and sdt <= regexp_replace(
				to_date(
					date_sub(
						CURRENT_TIMESTAMP(),
						1
					)
				),
				'-',
				''
			)
		group by
			sdt,
			customer_number,
			customer_name,
			sflag
	union all
		select
			sdt,
			customer_number,
			customer_name,
			b.sflag,
			0 sale_order,
			0 ret_order,
			sum(case WHEN retflag != 'X' THEN tax_salevalue END)sale_ods,
			sum(case WHEN retflag = 'X' THEN tax_salevalue END )ret_sale
		from
			csx_ods.sale_b2b_dtl_fct a
		join (
				select
					customer_number,
					customer_name,
					sflag
				from
					csx_dw.customer_simple_info_v2
				where
					sflag = '大宗'
					and customer_number != '910001'
					and sdt = regexp_replace(
						to_date(
							date_sub(
								CURRENT_TIMESTAMP(),
								1
							)
						),
						'-',
						''
					)
			) b on
			regexp_replace(
				a.cust_id,
				'(^0*)',
				''
			)= b.customer_number
			and sdt >= regexp_replace(
				to_date(
					trunc(
						date_sub(
							CURRENT_TIMESTAMP(),
							1
						),
						'MM'
					)
				),
				'-',
				''
			)
			and sdt <= regexp_replace(
				to_date(
					date_sub(
						CURRENT_TIMESTAMP(),
						1
					)
				),
				'-',
				''
			)
			and a.sflag != 'md'
		group by
			sdt,
			customer_number,
			customer_name,
			b.sflag
	) a
where
	sflag is not null
group by
	sdt,
	customer_number,
	customer_name,
	sflag
order by
	sdt desc;
-- 增加毛利额差异
-- CONNECTION: name=Hadoop-IMpala
 select
	sdt,
	sflag,
	round(sum(sale_order), 2)sale_order,
	sum(sale_ods)sale_ods,
	SUM(profit_order)profit_order,
	SUM(profit_ods)profit_ods,
	case
		when round(sum(sale_order), 2)>sum(sale_ods) then concat('sale_ODS表数据缺失||', ifnull(cast(round(sum(sale_order)-sum(sale_ods), 2) as string), ''))
		when round(sum(sale_order), 2)<sum(sale_ods) then concat('sale_ODS表数据多于order_item||', ifnull(cast(abs(round(sum(sale_order)-sum(sale_ods), 2))as string), ''))
		else '数据一致'
	end note
from
	(
		select
			sdt,
			sflag ,
			sum(tax_sale_val)sale_order,
			sum(tax_profit)profit_order,
			0 sale_ods,
			0 profit_ods
		from
			csx_dw.csx_order_item a
		join (
				select
					customer_number,
					customer_name,
					sflag
				from
					csx_dw.customer_simple_info_v2
				where
					sflag in('${sflag}')
					and customer_number != '910001'
					and sdt = regexp_replace(
						to_date(
							date_sub(
								CURRENT_TIMESTAMP(),
								1
							)
						),
						'-',
						''
					)
			) b on
			regexp_replace(
				a.sold_to,
				'(^0*)',
				''
			)= customer_number
			and sdt >= regexp_replace(
				to_date(
					trunc(
						date_sub(
							CURRENT_TIMESTAMP(),
							1
						),
						'MM'
					)
				),
				'-',
				''
			)
			and sdt <= regexp_replace(
				to_date(
					date_sub(
						CURRENT_TIMESTAMP(),
						1
					)
				),
				'-',
				''
			)
		group by
			sdt,
			sflag
	union all
		select
			sdt,
			b.sflag,
			0 sale_order,
			0 profit_order,
			sum(tax_salevalue)sale_ods,
			SUM(tax_profit)profit_ods
		from
			csx_ods.sale_b2b_dtl_fct a
		join (
				select
					customer_number,
					customer_name,
					sflag
				from
					csx_dw.customer_simple_info_v2
				where
					sflag in('${sflag}')
					and customer_number != '910001'
					and sdt = regexp_replace(
						to_date(
							date_sub(
								CURRENT_TIMESTAMP(),
								1
							)
						),
						'-',
						''
					)
			) b on
			regexp_replace(
				a.cust_id,
				'(^0*)',
				''
			)= b.customer_number
			and sdt >= regexp_replace(
				to_date(
					trunc(
						date_sub(
							CURRENT_TIMESTAMP(),
							1
						),
						'MM'
					)
				),
				'-',
				''
			)
			and sdt <= regexp_replace(
				to_date(
					date_sub(
						CURRENT_TIMESTAMP(),
						1
					)
				),
				'-',
				''
			)
			and a.sflag != 'md'
		group by
			sdt,
			b.sflag
	) a
where
	sflag is not null
group by
	sdt,
	sflag
order by
	sdt desc;