insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select coalesce(sale_group, '总计') sale_group,
	coalesce(region_name, '全国') region_name,
	coalesce(province_name, '合计') province_name,
	coalesce(smonth, '合计') smonth,
	sum(sales_value) / 10000 sales_value,
	sum(profit) / 10000 profit,
	sum(sale_cust_num) sale_cust_num,
	GROUPING__ID
from (
		select d.region_name,
			a.province_name,
			a.smonth,
			case
				when a.channel = '7' then 'BBC'
				--when a.channel in ('1') and b.attribute='合伙人' then '合伙人'
				when a.channel in ('1')
				and c.customer_no is not null then '合伙人'
				when a.channel in ('1')
				and a.smonth <= '202006'
				and c.customer_no in(
					'PF0129',
					'108087',
					'112505',
					'102755',
					'104371',
					'110601',
					'111130',
					'102998',
					'104348',
					'104362',
					'104523',
					'107758',
					'104319',
					'105366',
					'107539',
					'110683',
					'103010',
					'104346',
					'104332',
					'104342',
					'104507',
					'111056',
					'104375',
					'104504',
					'111114',
					'111368',
					'107945',
					'106311',
					'111874',
					'107694'
				) then '合伙人'
				when a.channel in ('1')
				and b.attribute = '贸易' then '贸易'
				when a.channel in ('1')
				and a.order_kind = 'WELFARE' then '福利单' - -
				when a.channel in ('1')
				and (
					b.attribute_code not in('3', '5')
					or b.attribute_code is null
				)
				and (
					a.order_kind <> 'WELFARE'
					or order_kind is null
				) then '日配单'
				when a.channel in ('1')
				and (
					a.order_kind <> 'WELFARE'
					or order_kind is null
				) then '日配单'
				else '其他'
			end sale_group,
			sum(sales_value) sales_value,
			sum(profit) profit,
			count(distinct customer_no) as sale_cust_num
		from (
				select channel,
					province_code,
					province_name,
					sdt,
					substr(sdt, 1, 6) smonth,
					customer_no,
					order_kind,
					sum(sales_value) as sales_value,
					sum(profit) as profit,
					sum(sales_qty) as sales_qty,
					sum(front_profit) as front_profit
				from csx_dw.dws_sale_r_d_customer_sale
				where sdt >= '20200101'
					and sdt < '20201001'
					and sales_type in ('qyg', 'sapqyg', 'sapgc', 'sc', 'bbc')
					and (
						order_no not in (
							'OC200529000043',
							'OC200529000044',
							'OC200529000045',
							'OC200529000046'
						)
						or order_no is null
					)
					and channel in('1', '7')
					and province_name not like '平台%'
				group by channel,
					province_code,
					province_name,
					sdt,
					substr(sdt, 1, 6),
					customer_no,
					order_kind
			) a
			left join --CRM信息取每月最后一天 剔除合伙人
			(
				select substr(sdt, 1, 6) smonth,
					customer_no,
					customer_name,
					attribute,
					attribute_code
				from csx_dw.dws_crm_w_a_customer_m_v1
				where sdt >= regexp_replace(trunc(date_sub(current_date, 1), 'YY'), '-', '') --昨日所在年第1天
					and sdt = if(substr(sdt, 1, 6) = substr(	regexp_replace(date_sub(current_date, 1), '-', ''),	1,6),regexp_replace(date_sub(current_date, 1), '-', ''),
						regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt, 'yyyyMMdd')))),'-',''	)
					) --sdt为每月最后一天
			) b on b.customer_no = a.customer_no
			and b.smonth = a.smonth
			left join csx_tmp.tmp_cust_partner2 c on c.customer_no = a.customer_no
			and c.sdt = a.smonth
			left join (
				select province_code,
					province_name,
					region_code,
					region_name
				from csx_dw.dim_area
				where area_rank = '13'
			) d on d.province_code = a.province_code
		group by d.region_name,
			a.province_name,
			a.smonth,
			case
				when a.channel = '7' then 'BBC' --when a.channel in ('1') and b.attribute='合伙人' then '合伙人'
				when a.channel in ('1')
				and c.customer_no is not null then '合伙人'
				when a.channel in ('1')
				and a.smonth <= '202006'
				and c.customer_no in(
					'PF0129',
					'108087',
					'112505',
					'102755',
					'104371',
					'110601',
					'111130',
					'102998',
					'104348',
					'104362',
					'104523',
					'107758',
					'104319',
					'105366',
					'107539',
					'110683',
					'103010',
					'104346',
					'104332',
					'104342',
					'104507',
					'111056',
					'104375',
					'104504',
					'111114',
					'111368',
					'107945',
					'106311',
					'111874',
					'107694'
				) then '合伙人'
				when a.channel in ('1')
				and b.attribute = '贸易' then '贸易'
				when a.channel in ('1')
				and a.order_kind = 'WELFARE' then '福利单' - -
				when a.channel in ('1')
				and (
					b.attribute_code not in('3', '5')
					or b.attribute_code is null
				)
				and (
					a.order_kind <> 'WELFARE'
					or order_kind is null
				) then '日配单'
				when a.channel in ('1')
				and (
					a.order_kind <> 'WELFARE'
					or order_kind is null
				) then '日配单'
				else '其他'
			end
	) a
group by sale_group,
	region_name,
	province_name,
	smonth grouping sets(
		smonth,
(smonth, region_name),
(smonth, region_name, province_name),
(sale_group, smonth),
(sale_group, region_name, smonth),
(sale_group, region_name, province_name, smonth)
	)
order by GROUPING__ID;