SELECT
	qstype as stype ,
	case
		when qstype in(
			'大宗' ,
			'供应链(生鲜)' ,
			'供应链(食百)' ,
			'平台' ,
			'BBC' ,
			'企业购 '
		) then '-'
		else province_code
	end province_code ,
	case
		when qstype in(
			'大宗' ,
			'供应链(生鲜)' ,
			'供应链(食百)' ,
			'平台' ,
			'BBC' ,
			'企业购 '
		) then '-'
		else prov_name
	end prov_name ,
	case
		when qstype in(
			'大宗' ,
			'供应链(生鲜)' ,
			'供应链(食百)' ,
			'平台' ,
			'BBC' ,
			'企业购 '
		) then '-'
		else coalesce(
			province_name ,
			prov_name
		)
	end province_name ,
	case
		when qstype in(
			'大宗' ,
			'供应链(生鲜)' ,
			'供应链(食百)' ,
			'平台' ,
			'BBC' ,
			'企业购 '
		) then '-'
		else district_manager_name
	end manage ,
	s_2019 ,
	s_201912 ,
	s_201911 ,
	s_201910 ,
	s_201909 ,
	s_201908 ,
	s_201907 ,
	s_201906 ,
	s_201905 ,
	s_201904 ,
	s_201903 ,
	s_201902 ,
	s_201901 ,
	Q4_sale ,
	Q3_sale ,
	Q2_sale ,
	Q1_sale ,
	s_2018 ,
	s_2019 / s_2018*1.00-1 s_rate ,
	p_2019 ,
	p_201912 ,
	p_201911 ,
	p_201910 ,
	p_201909 ,
	p_201908 ,
	p_201907 ,
	p_201906 ,
	p_201905 ,
	p_201904 ,
	p_201903 ,
	p_201902 ,
	p_201901 ,
	q4_fit ,
	q3_fit ,
	q2_fit ,
	q1_fit ,
	p_2019 / s_2019*1.00 p_rate ,
	p_201912 / s_201912*1.00 p_12_rate ,
	p_201911 / s_201911*1.00 p_11_rate ,
	p_201910 / s_201910*1.00 p_10_rate ,
	p_201909 / s_201909*1.00 p_09_rate ,
	p_201908 / s_201908*1.00 p_08_rate ,
	p_201907 / s_201907*1.00 p_07_rate ,
	p_201906 / s_201906*1.00 p_06_rate ,
	p_201905 / s_201905*1.00 p_05_rate ,
	p_201904 / s_201904*1.00 p_04_rate ,
	p_201903 / s_201903*1.00 p_03_rate ,
	p_201902 / s_201902*1.00 p_02_rate ,
	p_201901 / s_201901*1.00 p_01_rate ,
	q4_fit / Q4_sale*1.00 q4_profitrate ,
	q3_fit / Q3_sale*1.00 q3_profitrate ,
	q2_fit / Q2_sale*1.00 q2_profitrate ,
	q1_fit / Q1_sale*1.00 q1_profitrate
FROM
	(
		SELECT
			case
				when stype in (
					'M端',
					'商品（对内）'
				) then '商超（对内）'
				when stype in(
					'B端',
					'大客户'
				) then '大客户'
				when stype like '%S%' then '供应链'
				when stype in(
					'BBC',
					'企业购'
				) then '企业购'
				else stype
			end qstype ,
			prov_name ,
			sum(case WHEN sdt >= '201901' THEN sale END )s_2019 ,
			sum(case WHEN sdt = '201912' THEN sale END )s_201912 ,
			sum(case WHEN sdt = '201911' THEN sale END )s_201911 ,
			sum(case WHEN sdt = '201910' THEN sale END )s_201910 ,
			sum(case WHEN sdt = '201909' THEN sale END )s_201909 ,
			sum(case WHEN sdt = '201908' THEN sale END )s_201908 ,
			sum(case WHEN sdt = '201907' THEN sale END )s_201907 ,
			sum(case WHEN sdt = '201906' THEN sale END )s_201906 ,
			sum(case WHEN sdt = '201905' THEN sale END )s_201905 ,
			sum(case WHEN sdt = '201904' THEN sale END )s_201904 ,
			sum(case WHEN sdt = '201903' THEN sale END )s_201903 ,
			sum(case WHEN sdt = '201902' THEN sale END )s_201902 ,
			sum(case WHEN sdt = '201901' THEN sale END )s_201901 ,
			sum(case WHEN sdt >= '201910' and sdt <= '201912' THEN sale END )Q4_sale ,
			sum(case WHEN sdt >= '201907' and sdt <= '201909' THEN sale END )Q3_sale ,
			sum(case WHEN sdt >= '201904' and sdt <= '201906' THEN sale END )Q2_sale ,
			sum(case WHEN sdt >= '201901' and sdt <= '201903' THEN sale END )Q1_sale ,
			sum(case WHEN sdt >= '201801' AND sdt <= substr(regexp_replace(to_date(add_months(date_sub(current_timestamp(), 1),-12)), '-', ''), 1, 6) THEN h_sale END )s_2018 ,
			sum(case WHEN sdt >= '201901' THEN profit END )p_2019 ,
			sum(case WHEN sdt = '201912' THEN profit END )p_201912 ,
			sum(case WHEN sdt = '201911' THEN profit END )p_201911 ,
			sum(case WHEN sdt = '201910' THEN profit END )p_201910 ,
			sum(case WHEN sdt = '201909' THEN profit END )p_201909 ,
			sum(case WHEN sdt = '201908' THEN profit END )p_201908 ,
			sum(case WHEN sdt = '201907' THEN profit END )p_201907 ,
			sum(case WHEN sdt = '201906' THEN profit END )p_201906 ,
			sum(case WHEN sdt = '201905' THEN profit END )p_201905 ,
			sum(case WHEN sdt = '201904' THEN profit END )p_201904 ,
			sum(case WHEN sdt = '201903' THEN profit END )p_201903 ,
			sum(case WHEN sdt = '201902' THEN profit END )p_201902 ,
			sum(case WHEN sdt = '201901' THEN profit END )p_201901 ,
			sum(case WHEN sdt >= '201910' and sdt <= '201912' THEN profit END )q4_fit ,
			sum(case WHEN sdt >= '201907' and sdt <= '201909' THEN profit END )q3_fit ,
			sum(case WHEN sdt >= '201904' and sdt <= '201906' THEN profit END )q2_fit ,
			sum(case WHEN sdt >= '201901' and sdt <= '201903' THEN profit END )q1_fit
		FROM
			(
				SELECT
					case
						when province_name like '平台-B%' then '平台'
						else a.channel_name
					end as stype ,
					case
						when channel in (
							'4' ,
							'5' ,
							'6',
							'7'
						) then '-'
						else province_name
					end prov_name ,
					substr(
						sdt,
						1,
						6
					)sdt ,
					province_manager_name manage ,
					sum(a.sales_value)/ 10000 * 1.00 sale ,
					sum(a.profit)/ 10000 * 1.00 profit ,
					0 h_sale
				FROM
					csx_dw.sale_goods_m a
				where
					sdt <= '20190930'
					AND sdt >= regexp_replace(
						to_date(
							trunc(
								date_sub(
									current_timestamp(),
									1
								),
								'YY'
							)
						),
						'-',
						''
					)
				GROUP BY
					case
						when province_name like '平台-B%' then '平台'
						else a.channel_name
					end ,
					case
						when channel in (
							'4' ,
							'5' ,
							'6',
							'7'
						) then '-'
						else province_name
					end ,
					sdt ,
					a.province_manager_name
			union all
				select
					case
						when province_name like '平台-B%' then '平台'
						else a.channel_name
					end as stype ,
					case
						when channel in (
							'4' ,
							'5' ,
							'6',
							'7'
						) then '-'
						else province_name
					end prov_name ,
					substr(
						sdt,
						1,
						6
					)sdt ,
					province_manager_name manage ,
					0 sale ,
					0 profit ,
					sum(sales_value)/ 10000 * 1.00 h_sale
				FROM
					csx_dw.sale_goods_m a
				where
					sdt <= '20180930'
					AND sdt >= regexp_replace(
						to_date(
							trunc(
								date_sub(
									current_timestamp() ,
									366
								) ,
								'YY'
							)
						) ,
						'-' ,
						''
					)
				GROUP BY
					case
						when province_name like '平台-B%' then '平台'
						else a.channel_name
					end ,
					case
						when channel in (
							'4' ,
							'5' ,
							'6',
							'7'
						) then '-'
						else province_name
					end ,
					sdt ,
					a.province_manager_name
			)a
		GROUP BY
			stype ,
			prov_name
	)a
left join (
		select
			DISTINCT province_name ,
			district_manager_name ,
			province_code
		from
			csx_dw.sale_org_m
		where
			sdt = regexp_replace(
				to_date(
					date_sub(
						current_timestamp(),
						1
					)
				),
				'-' ,
				''
			)
			and org_name in(
				'区域本部' ,
				'平台-B',
				'平台-大宗',
				'平台BBC'
			)
			and district_manager_name != ''
			and province_name is not null
	) b on
	substr(
		a.prov_name,
		1,
		LENGTH (prov_name)-3
	) = substr(
		b.province_name,
		1,
		LENGTH (b.province_name)-3
	)
ORDER BY
	case
		when qstype = '大客户' then 1
		when qstype = '商超(对内)' then 2
		when qstype = '商超(对外)' then 3
		when qstype in(
			'供应链(食百)' ,
			'供应链(生鲜)'
		)then 4
		when qstype = '大宗' then 5
		else 6
	end ,
	province_code ;
-- select * from csx_dw.sale_goods_m as sgm where sgm .province_name like '企业购%';
-- 企业属性销售情况
select first_category,channel ,cust_cn,sale,profit , round(sale/sum(sale)over(partition by channel ),4) as sale_rate
from (
 select
	first_category ,
	case
		when province_name = '平台-B' then '平台'
		else channel_name
	end channel,
	COUNT (
		DISTINCT customer_no
	)cust_cn ,
	sum(sales_value) sale,
	sum(profit) profit
from
	csx_dw.sale_goods_m as sgm
where
	sdt >= '20190101'
	and sdt <= '20190930'
group by
	first_category ,
	case
		when province_name = '平台-B' then '平台'
		else channel_name
	end
	) a
;

select * from csx_dw.sale_item_m as sim   where first_category  is null and customer_no ='104152'

select * from csx_dw.customer_m as cm  where customer_number ='104152';