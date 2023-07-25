select
	qstype as stype                                    ,
	province_code                                      ,
	prov_name                                          ,
	coalesce(province_name, prov_name) as province_name,
	district_manager_name              as manage       ,
	s_2019                                             ,
	s_201912                                           ,
	s_201911                                           ,
	s_201910                                           ,
	s_201909                                           ,
	s_201908                                           ,
	s_201907                                           ,
	s_201906                                           ,
	s_201905                                           ,
	s_201904                                           ,
	s_201903                                           ,
	s_201902                                           ,
	s_201901                                           ,
	q4_sale                                            ,
	q3_sale                                            ,
	q2_sale                                            ,
	q1_sale                                            ,
	s_2018                                             ,
	s_2019 / s_2018 * 1.00 -1 s_rate                   ,
	p_2019                                             ,
	p_201912                                           ,
	p_201911                                           ,
	p_201910                                           ,
	p_201909                                           ,
	p_201908                                           ,
	p_201907                                           ,
	p_201906                                           ,
	p_201905                                           ,
	p_201904                                           ,
	p_201903                                           ,
	p_201902                                           ,
	p_201901                                           ,
	q4_fit                                             ,
	q3_fit                                             ,
	q2_fit                                             ,
	q1_fit                                             ,
	p_2019   / s_2019 * 1.00   p_rate                      ,
	p_201912 / s_201912 * 1.00 p_12_rate                   ,
	p_201911 / s_201911 * 1.00 p_11_rate                   ,
	p_201910 / s_201910 * 1.00 p_10_rate                   ,
	p_201909 / s_201909 * 1.00 p_09_rate                   ,
	p_201908 / s_201908 * 1.00 p_08_rate                   ,
	p_201907 / s_201907 * 1.00 p_07_rate                   ,
	p_201906 / s_201906 * 1.00 p_06_rate                   ,
	p_201905 / s_201905 * 1.00 p_05_rate                   ,
	p_201904 / s_201904 * 1.00 p_04_rate                   ,
	p_201903 / s_201903 * 1.00 p_03_rate                   ,
	p_201902 / s_201902 * 1.00 p_02_rate                   ,
	p_201901 / s_201901 * 1.00 p_01_rate                   ,
	q4_fit   / q4_sale * 1.00  q4_profitrate               ,
	q3_fit   / q3_sale * 1.00  q3_profitrate               ,
	q2_fit   / q2_sale * 1.00  q2_profitrate               ,
	q1_fit   / q1_sale * 1.00  q1_profitrate
from
	(
		select
			case
				when stype in ('M端', '商品（对内）') then '商超'
				when stype in('B端', '大')then '大'
				when stype like '%s%'	then '供应链'
				when stype in('BBC', '企业购')	then '企业购'
					else stype
			end qstype,
			prov_name ,
			sum
				(
					case
						when substr(sdt, 1, 4) = substr(regexp_replace('${edate}', '-', ''), 1, 4)
							then sale
					end
				)
			s_2019,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '12' ) -- concat(from_timestamp(date_sub('${edate}',1),'yyyy'),'4')
							then sale
					end
				)
			s_201912,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '11' )
							then sale
					end
				)
			s_201911,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '10' )
							then sale
					end
				)
			s_201910,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '09' )
							then sale
					end
				)
			s_201909,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '08' )
							then sale
					end
				)
			s_201908,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '07' )
							then sale
					end
				)
			s_201907,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '06' )
							then sale
					end
				)
			s_201906,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '05' )
							then sale
					end
				)
			s_201905,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '04' )
							then sale
					end
				)
			s_201904,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '03' )
							then sale
					end
				)
			s_201903,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '02' )
							then sale
					end
				)
			s_201902,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '01' )
							then sale
					end
				)
			s_201901,
			sum
				(
					case
						when b.calquarter = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '4' )
							then sale
					end
				)
			q4_sale,
			sum
				(
					case
						when b.calquarter = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '3' )
							then sale
					end
				)
			q3_sale,
			sum
				(
					case
						when b.calquarter = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '2' )
							then sale
					end
				)
			q2_sale,
			sum
				(
					case
						when b.calquarter = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '1' )
							then sale
					end
				)
			q1_sale,
			sum
				(
					case
						when sdt    >= concat( substr( regexp_replace( to_date(add_months('${edate}', -12)), '-','' ), 1, 4 ), '01' )
							and sdt <= regexp_replace( to_date(add_months('${edate}', -12)), '-', '' )
							then h_sale
					end
				)
			s_2018,
			sum
				(
					case
						when substr(sdt, 1, 4) = substr(regexp_replace('${edate}', '-', ''), 1, 4)
							then profit
					end
				)
			p_2019,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '12' )
							then profit
					end
				)
			p_201912,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '11' )
							then profit
					end
				)
			p_201911,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '10' )
							then profit
					end
				)
			p_201910,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '09' )
							then profit
					end
				)
			p_201909,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '08' )
							then profit
					end
				)
			p_201908,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '07' )
							then profit
					end
				)
			p_201907,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '06' )
							then profit
					end
				)
			p_201906,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '05' )
							then profit
					end
				)
			p_201905,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '04' )
							then profit
					end
				)
			p_201904,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '03' )
							then profit
					end
				)
			p_201903,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '02' )
							then profit
					end
				)
			p_201902,
			sum
				(
					case
						when sdt = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '01' )
							then profit
					end
				)
			p_201901,
			sum
				(
					case
						when b.calquarter = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '4' )
							then profit
					end
				)
			q4_fit,
			sum
				(
					case
						when b.calquarter = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '3' )
							then profit
					end
				)
			q3_fit,
			sum
				(
					case
						when b.calquarter = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '2' )
							then profit
					end
				)
			q2_fit,
			sum
				(
					case
						when b.calquarter = concat( substr(regexp_replace('${edate}', '-', ''), 1, 4), '1' )
							then profit
					end
				)
			q1_fit
		from
			(
				select
					case
						when province_name like '平台-B%'	then '平台'
						when channel in ('1', '3')		then '大'
						when channel in ('2')	then '商超'
						else a.channel_name
					end as stype,
					case
						when channel in ('4', '5', '6')	then '-'
						else province_name		
						end  prov_name,
					month as sdt      ,
					sum(a.sales_value) / 10000 * 1.00 sale  ,
					sum(a.profit)      / 10000 * 1.00 profit,
					0                                 h_sale
				from
					csx_dw.ads_sale_r_m_customer_goods_sale  a
				where
					month     <= substr(regexp_replace(to_date('${edate}'),'-',''),1,6)
					and month >= substr(regexp_replace(to_date(trunc('${edate}', 'yy')), '-', ''),1,6)
				group by
					case when province_name like '平台-B%' then '平台'
						when channel in ('1', '3')	then '大'
						when channel in ('2')	then '商超'
						else a.channel_name	end,
					case
						when channel in ('4', '5', '6')	then '-'
						else province_name
					end,
					sdt 
				union all
				select
					case
						when province_name like '平台-B%'	then '平台'
						when channel in ('1','3')then '大'
						when channel in ('2')	then '商超'
						else a.channel_name
					end as stype,
					case
						when channel in ('4', '5', '6')	then '-'
						else province_name
					end as   prov_name,
					substr(sdt, 1, 6) sdt      ,
					-- province_manager_name manage,
					0                  as             sale  ,
					0                   as            profit,
					sum(sales_value) / 10000 * 1.00 as h_sale
				from
					csx_dw.customer_sales a
				where
					month     <= substr(regexp_replace( to_date(add_months('${edate}', -12)), '-', '' ),1,6)
					and month >= substr(regexp_replace( to_date(trunc(add_months('${edate}', -12), 'yy')), '-', '' ),1,6)
				group by
					case
						when province_name like '平台%'	then '平台'
						when channel in ('1','3')then '大'
						when channel in ('2')then '商超'
						else a.channel_name
					end,
					case
						when channel in ('4', '5', '6')then '-'
						else province_name
					end,
					sdt
			)
			a
			join
				(
				select distinct
					year      ,
					month     ,
					quarter   ,
					calquarter,
					calmonth
				from
					dim.dim_time
				)
				b
				on
					a.sdt = b.calmonth
		group by
			stype,
			prov_name
	)
	a
	left join
		(
			select  distinct
				province_name        ,
				province_manager_name as district_manager_name,
				province_code
			from
				csx_dw.dim_area
			where
				area_rank = 13
		)
		b
		on
			substr(a.prov_name, 1, length (prov_name) -3) = substr(b.province_name, 1, length (b.province_name) -3)
order by
	case
		when qstype = '大' then 1
		when qstype in('商超') then 2
		when qstype like '企业购%' then 3
		when qstype in('供应链(食百)', '供应链(生鲜)')	then 4
		when qstype = '大宗'then 5
		else 6
	end,
	province_code
;