SELECT
	stype,
		case
		when stype in('大宗',
		'供应链',
		'平台',
		'BBC') then '-'
		else
	province_code
	end province_code,
	prov_name,
		case
		when stype in('大宗',
		'供应链',
		'平台',
		'BBC') then '-'
		else province_name end
	province_name,
	case
		when stype in('大宗',
		'供应链(S端)',
		'平台',
		'BBC') then '-'
		else district_manager_name
	end manage,
	s_2019,
	s_201912,
	s_201911,
	s_201910,
	s_201909,
	s_201908,
	s_201907,
	s_201906,
	s_201905,
	s_201904,
	s_201903,
	s_201902,
	s_201901,
	Q4_sale,
	Q3_sale,
	Q2_sale,
	Q1_sale,
	s_2018,
	s_2019 / s_2018*1.00-1 s_rate,
	p_2019,
	p_201912,
	p_201911,
	p_201910,
	p_201909,
	p_201908,
	p_201907,
	p_201906,
	p_201905,
	p_201904,
	p_201903,
	p_201902,
	p_201901,
	q4_fit,
	q3_fit,
	q2_fit,
	q1_fit,
	p_2019 / s_2019*1.00 p_rate,
	p_201912 / s_201912*1.00 p_12_rate,
	p_201911 / s_201911*1.00 p_11_rate,
	p_201910 / s_201910*1.00 p_10_rate,
	p_201909 / s_201909*1.00 p_09_rate,
	p_201908 / s_201908*1.00 p_08_rate,
	p_201907 / s_201907*1.00 p_07_rate,
	p_201906 / s_201906*1.00 p_06_rate,
	p_201905 / s_201905*1.00 p_05_rate,
	p_201904 / s_201904*1.00 p_04_rate,
	p_201903 / s_201903*1.00 p_03_rate,
	p_201902 / s_201902*1.00 p_02_rate,
	p_201901 / s_201901*1.00 p_01_rate,
	q4_fit / Q4_sale*1.00 q4_profitrate,
	q3_fit / Q3_sale*1.00 q3_profitrate,
	q2_fit / Q2_sale*1.00 q2_profitrate,
	q1_fit / Q1_sale*1.00 q1_profitrate
FROM
	(
	SELECT
		case
		when stype = 'M端' then '商超'
		when stype = 'B端' then '大客户'
		when stype like '%S%' then '供应链'
		else stype
	end 
		stype,
		case
			when prov_name in('北京',	'上海','重庆') then concat(prov_name, '市')
			when prov_name like '%平台%' then '-'
			when 
			else concat(prov_name, '省')
		end prov_name,
		manage,
		sum(case WHEN sdt >= '201901' THEN sale END )s_2019 ,
		sum(case WHEN sdt = '201912' THEN sale END )s_201912,
		sum(case WHEN sdt = '201911' THEN sale END )s_201911,
		sum(case WHEN sdt = '201910' THEN sale END )s_201910,
		sum(case WHEN sdt = '201909' THEN sale END )s_201909,
		sum(case WHEN sdt = '201908' THEN sale END )s_201908,
		sum(case WHEN sdt = '201907' THEN sale END )s_201907,
		sum(case WHEN sdt = '201906' THEN sale END )s_201906,
		sum(case WHEN sdt = '201905' THEN sale END )s_201905,
		sum(case WHEN sdt = '201904' THEN sale END )s_201904,
		sum(case WHEN sdt = '201903' THEN sale END )s_201903,
		sum(case WHEN sdt = '201902' THEN sale END )s_201902,
		sum(case WHEN sdt = '201901' THEN sale END )s_201901,
		sum(case WHEN sdt >= '201910' and sdt <= '201912' THEN sale END )Q4_sale,
		sum(case WHEN sdt >= '201907' and sdt <= '201909' THEN sale END )Q3_sale,
		sum(case WHEN sdt >= '201904' and sdt <= '201906' THEN sale END )Q2_sale,
		sum(case WHEN sdt >= '201901' and sdt <= '201903' THEN sale END )Q1_sale,
		sum(case WHEN sdt >= '201801' AND sdt <= substr(regexp_replace(to_date(add_months(date_sub(current_timestamp(), 1),-12)), '-', ''), 1, 6) THEN h_sale END )s_2018 ,
		sum(case WHEN sdt >= '201901' THEN profit END )p_2019 ,
		sum(case WHEN sdt = '201912' THEN profit END )p_201912,
		sum(case WHEN sdt = '201911' THEN profit END )p_201911,
		sum(case WHEN sdt = '201910' THEN profit END )p_201910,
		sum(case WHEN sdt = '201909' THEN profit END )p_201909,
		sum(case WHEN sdt = '201908' THEN profit END )p_201908,
		sum(case WHEN sdt = '201907' THEN profit END )p_201907,
		sum(case WHEN sdt = '201906' THEN profit END )p_201906,
		sum(case WHEN sdt = '201905' THEN profit END )p_201905,
		sum(case WHEN sdt = '201904' THEN profit END )p_201904,
		sum(case WHEN sdt = '201903' THEN profit END )p_201903,
		sum(case WHEN sdt = '201902' THEN profit END )p_201902,
		sum(case WHEN sdt = '201901' THEN profit END )p_201901,
		sum(case WHEN sdt >= '201910' and sdt <= '201912' THEN profit END )q4_fit,
		sum(case WHEN sdt >= '201907' and sdt <= '201909' THEN profit END )q3_fit,
		sum(case WHEN sdt >= '201904' and sdt <= '201906' THEN profit END )q2_fit,
		sum(case WHEN sdt >= '201901' and sdt <= '201903' THEN profit END )q1_fit
	FROM
		(
		SELECT
			a.qdflag stype,
			a.dist prov_name,
			substr(sdt,
			1,
			6)sdt,
			a.manage,
			sum(a.xse)/ 10000 * 1.00 sale,
			sum(a.mle)/ 10000 * 1.00 profit,
			0 h_sale
		FROM
			csx_dw.sale_warzone02_detail_dtl a
		where
			sdt <= regexp_replace(to_date(date_sub(current_timestamp(),
			1)),
			'-',
			'')
			AND sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(),
			1),
			'YY')),
			'-',
			'')
		GROUP BY
			a.qdflag,
			a.dist,
			sdt,
			a.manage
	union all
		SELECT
			a.qdflag stype,
			a.dist prov_name,
			substr(sdt,
			1,
			6)sdt,
			a.manage,
			0 sale,
			0 profit,
			sum(xse)/ 10000 * 1.00 h_sale
		FROM
			csx_dw.sale_warzone02_detail_dtl a
		where
			sdt <= regexp_replace(to_date(add_months(date_sub(current_timestamp(),
			1),
			-12)),
			'-',
			'')
			AND sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(),366),'YY')),'-','')
		GROUP BY
			a.qdflag,
			a.dist,
			sdt,
			a.manage )a
	GROUP BY
		stype ,
		prov_name ,
		manage )a
left join (
	select DISTINCT 		
		province_name,
		district_manager_name,
		province_code
	from
		csx_dw.sale_org_m 
	where
		sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')and  org_name in('区域本部','平台-B','平台-大宗','平台BBC','大客户')) b on
	substr(a.prov_name,1,LENGTH (prov_name)-3) = substr(b.province_name,1,LENGTH (b.province_name)-6)
ORDER BY
	case
		when stype = '商超' then 1
		when stype = '大客户' then 2
		when stype = '大宗' then 3
		else 4
	end 
   ,	province_code ;
	
SELECT *,substr(b.province_name,1,LENGTH (b.province_name)-6) FROM csx_dw.sale_org_m b WHERE sdt ='20190802';

-- csx_dw.sale_customer_say
SELECT
	stype,
		case
		when stype in('大宗',
		'供应链',
		'平台',
		'BBC') then '-'
		else
	province_code
	end province_code,
	prov_name,
		case
		when stype in('大宗',
		'供应链',
		'平台',
		'BBC') then '-'
		else coalesce(province_name,prov_name )end
	province_name,
	case
		when stype in('大宗',
		'供应链(S端)',
		'平台',
		'BBC') then '-'
		else district_manager_name
	end manage,
	s_2019,
	s_201912,
	s_201911,
	s_201910,
	s_201909,
	s_201908,
	s_201907,
	s_201906,
	s_201905,
	s_201904,
	s_201903,
	s_201902,
	s_201901,
	Q4_sale,
	Q3_sale,
	Q2_sale,
	Q1_sale,
	s_2018,
	s_2019 / s_2018*1.00-1 s_rate,
	p_2019,
	p_201912,
	p_201911,
	p_201910,
	p_201909,
	p_201908,
	p_201907,
	p_201906,
	p_201905,
	p_201904,
	p_201903,
	p_201902,
	p_201901,
	q4_fit,
	q3_fit,
	q2_fit,
	q1_fit,
	p_2019 / s_2019*1.00 p_rate,
	p_201912 / s_201912*1.00 p_12_rate,
	p_201911 / s_201911*1.00 p_11_rate,
	p_201910 / s_201910*1.00 p_10_rate,
	p_201909 / s_201909*1.00 p_09_rate,
	p_201908 / s_201908*1.00 p_08_rate,
	p_201907 / s_201907*1.00 p_07_rate,
	p_201906 / s_201906*1.00 p_06_rate,
	p_201905 / s_201905*1.00 p_05_rate,
	p_201904 / s_201904*1.00 p_04_rate,
	p_201903 / s_201903*1.00 p_03_rate,
	p_201902 / s_201902*1.00 p_02_rate,
	p_201901 / s_201901*1.00 p_01_rate,
	q4_fit / Q4_sale*1.00 q4_profitrate,
	q3_fit / Q3_sale*1.00 q3_profitrate,
	q2_fit / Q2_sale*1.00 q2_profitrate,
	q1_fit / Q1_sale*1.00 q1_profitrate
FROM
	(
	SELECT
		case
		when stype = 'M端' then '商超'
		when stype = 'B端' then '大客户'
		when stype like '%S%' then '供应链'
		else stype
	end 
		stype,
		case
			when prov_name in('北京',	'上海','重庆') then concat(prov_name, '市')
			when prov_name like '%平台%' then '-'
			when stype in ('大宗','BBC','平台','供应链(S端)') then '-'
			else concat(prov_name, '省')
		end prov_name,
		
		sum(case WHEN sdt >= '201901' THEN sale END )s_2019 ,
		sum(case WHEN sdt = '201912' THEN sale END )s_201912,
		sum(case WHEN sdt = '201911' THEN sale END )s_201911,
		sum(case WHEN sdt = '201910' THEN sale END )s_201910,
		sum(case WHEN sdt = '201909' THEN sale END )s_201909,
		sum(case WHEN sdt = '201908' THEN sale END )s_201908,
		sum(case WHEN sdt = '201907' THEN sale END )s_201907,
		sum(case WHEN sdt = '201906' THEN sale END )s_201906,
		sum(case WHEN sdt = '201905' THEN sale END )s_201905,
		sum(case WHEN sdt = '201904' THEN sale END )s_201904,
		sum(case WHEN sdt = '201903' THEN sale END )s_201903,
		sum(case WHEN sdt = '201902' THEN sale END )s_201902,
		sum(case WHEN sdt = '201901' THEN sale END )s_201901,
		sum(case WHEN sdt >= '201910' and sdt <= '201912' THEN sale END )Q4_sale,
		sum(case WHEN sdt >= '201907' and sdt <= '201909' THEN sale END )Q3_sale,
		sum(case WHEN sdt >= '201904' and sdt <= '201906' THEN sale END )Q2_sale,
		sum(case WHEN sdt >= '201901' and sdt <= '201903' THEN sale END )Q1_sale,
		sum(case WHEN sdt >= '201801' AND sdt <= substr(regexp_replace(to_date(add_months(date_sub(current_timestamp(), 1),-12)), '-', ''), 1, 6) THEN h_sale END )s_2018 ,
		sum(case WHEN sdt >= '201901' THEN profit END )p_2019 ,
		sum(case WHEN sdt = '201912' THEN profit END )p_201912,
		sum(case WHEN sdt = '201911' THEN profit END )p_201911,
		sum(case WHEN sdt = '201910' THEN profit END )p_201910,
		sum(case WHEN sdt = '201909' THEN profit END )p_201909,
		sum(case WHEN sdt = '201908' THEN profit END )p_201908,
		sum(case WHEN sdt = '201907' THEN profit END )p_201907,
		sum(case WHEN sdt = '201906' THEN profit END )p_201906,
		sum(case WHEN sdt = '201905' THEN profit END )p_201905,
		sum(case WHEN sdt = '201904' THEN profit END )p_201904,
		sum(case WHEN sdt = '201903' THEN profit END )p_201903,
		sum(case WHEN sdt = '201902' THEN profit END )p_201902,
		sum(case WHEN sdt = '201901' THEN profit END )p_201901,
		sum(case WHEN sdt >= '201910' and sdt <= '201912' THEN profit END )q4_fit,
		sum(case WHEN sdt >= '201907' and sdt <= '201909' THEN profit END )q3_fit,
		sum(case WHEN sdt >= '201904' and sdt <= '201906' THEN profit END )q2_fit,
		sum(case WHEN sdt >= '201901' and sdt <= '201903' THEN profit END )q1_fit
	FROM
		(
		SELECT
			a.qdflag stype,
			a.dist prov_name,
			substr(sdt,
			1,
			6)sdt,
			a.manage,
			sum(a.xse)/ 10000 * 1.00 sale,
			sum(a.mle)/ 10000 * 1.00 profit,
			0 h_sale
		FROM
			csx_dw.sale_customer_day a
		where
			sdt <= regexp_replace(to_date(date_sub(current_timestamp(),
			1)),
			'-',
			'')
			AND sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(),
			1),
			'YY')),
			'-',
			'')
		GROUP BY
			a.qdflag,
			a.dist,
			sdt,
			a.manage
	union all
		SELECT
			a.qdflag stype,
			a.dist prov_name,
			substr(sdt,
			1,
			6)sdt,
			a.manage,
			0 sale,
			0 profit,
			sum(xse)/ 10000 * 1.00 h_sale
		FROM
			csx_dw.sale_customer_day a
		where
			sdt <= regexp_replace(to_date(add_months(date_sub(current_timestamp(),
			1),
			-12)),
			'-',
			'')
			AND sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(),366),'YY')),'-','')
		GROUP BY
			a.qdflag,
			a.dist,
			sdt,
			a.manage )a
	GROUP BY
		stype ,
		prov_name 
		 )a
left join (
	select DISTINCT 		
		province_name,
		district_manager_name,
		province_code
	from
		csx_dw.sale_org_m 
	where
		sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')and  org_name in('区域本部','平台-B','平台-大宗','平台BBC','大客户')) b on
	substr(a.prov_name,1,LENGTH (prov_name)-3) = substr(b.province_name,1,LENGTH (b.province_name)-6)
ORDER BY
	case
		when stype = '商超' then 1
		when stype = '大客户' then 2
		when stype = '大宗' then 3
		else 4
	end 
   ,	province_code ;
   
  
  
  SELECT
	case
		when stype = 'M端' then '商超'
		when stype = 'B端' then '大客户'
		when stype like '%S%' then '供应链'
		else stype
	end stype,
	cast(province_code as BIGINT ) province_code,
	prov_name,
	case
		when stype in('大宗',
		'供应链(S端)',
		'平台',
		'BBC') then '-'
		else manage
	end manage,
	s_2019,
	s_201912,
	s_201911,
	s_201910,
	s_201909,
	s_201908,
	s_201907,
	s_201906,
	s_201905,
	s_201904,
	s_201903,
	s_201902,
	s_201901,
	Q4_sale,
	Q3_sale,
	Q2_sale,
	Q1_sale,
	s_2018,
	s_2019 / s_2018*1.00-1 s_rate,
	p_2019,
	p_201912,
	p_201911,
	p_201910,
	p_201909,
	p_201908,
	p_201907,
	p_201906,
	p_201905,
	p_201904,
	p_201903,
	p_201902,
	p_201901,
	q4_fit,
	q3_fit,
	q2_fit,
	q1_fit,
	p_2019 / s_2019*1.00 p_rate,
	p_201912 / s_201912*1.00 p_12_rate,
	p_201911 / s_201911*1.00 p_11_rate,
	p_201910 / s_201910*1.00 p_10_rate,
	p_201909 / s_201909*1.00 p_09_rate,
	p_201908 / s_201908*1.00 p_08_rate,
	p_201907 / s_201907*1.00 p_07_rate,
	p_201906 / s_201906*1.00 p_06_rate,
	p_201905 / s_201905*1.00 p_05_rate,
	p_201904 / s_201904*1.00 p_04_rate,
	p_201903 / s_201903*1.00 p_03_rate,
	p_201902 / s_201902*1.00 p_02_rate,
	p_201901 / s_201901*1.00 p_01_rate,
	q4_fit / Q4_sale*1.00 q4_profitrate,
	q3_fit / Q3_sale*1.00 q3_profitrate,
	q2_fit / Q2_sale*1.00 q2_profitrate,
	q1_fit / Q1_sale*1.00 q1_profitrate
FROM
	(
		SELECT stype,
		case
			when prov_name in('北京',
			'上海',
			'重庆') then concat(prov_name,
			'市')
			when prov_name like '%平台%' then prov_name
			else concat(prov_name,
			'省')
		end prov_name,
		manage,
		sum(case WHEN sdt >= '201901' THEN sale END )s_2019 ,
		sum(case WHEN sdt = '201912' THEN sale END )s_201912,
		sum(case WHEN sdt = '201911' THEN sale END )s_201911,
		sum(case WHEN sdt = '201910' THEN sale END )s_201910,
		sum(case WHEN sdt = '201909' THEN sale END )s_201909,
		sum(case WHEN sdt = '201908' THEN sale END )s_201908,
		sum(case WHEN sdt = '201907' THEN sale END )s_201907,
		sum(case WHEN sdt = '201906' THEN sale END )s_201906,
		sum(case WHEN sdt = '201905' THEN sale END )s_201905,
		sum(case WHEN sdt = '201904' THEN sale END )s_201904,
		sum(case WHEN sdt = '201903' THEN sale END )s_201903,
		sum(case WHEN sdt = '201902' THEN sale END )s_201902,
		sum(case WHEN sdt = '201901' THEN sale END )s_201901,
		sum(case WHEN sdt >= '201910' and sdt <= '201912' THEN sale END )Q4_sale,
		sum(case WHEN sdt >= '201907' and sdt <= '201909' THEN sale END )Q3_sale,
		sum(case WHEN sdt >= '201904' and sdt <= '201906' THEN sale END )Q2_sale,
		sum(case WHEN sdt >= '201901' and sdt <= '201903' THEN sale END )Q1_sale,
		sum(case WHEN sdt >= '201801' AND sdt <= substr(regexp_replace(to_date(add_months(date_sub(current_timestamp(), 1),-12)), '-', ''), 1, 6) THEN h_sale END )s_2018 ,
		sum(case WHEN sdt >= '201901' THEN profit END )p_2019 ,
		sum(case WHEN sdt = '201912' THEN profit END )p_201912,
		sum(case WHEN sdt = '201911' THEN profit END )p_201911,
		sum(case WHEN sdt = '201910' THEN profit END )p_201910,
		sum(case WHEN sdt = '201909' THEN profit END )p_201909,
		sum(case WHEN sdt = '201908' THEN profit END )p_201908,
		sum(case WHEN sdt = '201907' THEN profit END )p_201907,
		sum(case WHEN sdt = '201906' THEN profit END )p_201906,
		sum(case WHEN sdt = '201905' THEN profit END )p_201905,
		sum(case WHEN sdt = '201904' THEN profit END )p_201904,
		sum(case WHEN sdt = '201903' THEN profit END )p_201903,
		sum(case WHEN sdt = '201902' THEN profit END )p_201902,
		sum(case WHEN sdt = '201901' THEN profit END )p_201901,
		sum(case WHEN sdt >= '201910' and sdt <= '201912' THEN profit END )q4_fit,
		sum(case WHEN sdt >= '201907' and sdt <= '201909' THEN profit END )q3_fit,
		sum(case WHEN sdt >= '201904' and sdt <= '201906' THEN profit END )q2_fit,
		sum(case WHEN sdt >= '201901' and sdt <= '201903' THEN profit END )q1_fit
	FROM
		(
			SELECT a.qdflag stype,
			a.dist prov_name,
			substr(sdt,
			1,
			6)sdt,
			a.manage,
			sum(a.xse)/ 10000 * 1.00 sale,
			sum(a.mle)/ 10000 * 1.00 profit,
			0 h_sale
		FROM
			 csx_dw.sale_customer_day a
		where
			sdt <= regexp_replace(to_date(date_sub(current_timestamp(),
			1)),
			'-',
			'')
			AND sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(),
			1),
			'YY')),
			'-',
			'')
		GROUP BY
			a.qdflag,
			a.dist,
			sdt,
			a.manage
	union all
		SELECT
			a.qdflag stype,
			a.dist prov_name,
			substr(sdt,
			1,
			6)sdt,
			a.manage,
			0 sale,
			0 profit,
			sum(xse)/ 10000 * 1.00 h_sale
		FROM
			 csx_dw.sale_customer_day a
		where
			sdt <= regexp_replace(to_date(add_months(date_sub(current_timestamp(),
			1),
			-12)),
			'-',
			'')
			AND sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(),
			366),
			'YY')),
			'-',
			'')
		GROUP BY
			a.qdflag,
			a.dist,
			sdt,
			a.manage )a
	GROUP BY
		stype ,
		prov_name ,
		manage )a
left join (
		select province_code,
		province
	from
		csx_ods.sys_province_ods_v2
	where
		sdt = regexp_replace(to_date(date_sub(current_timestamp(),
		1)),
		'-',
		'') ) b on
	a.prov_name = b.province
ORDER BY
	case
		when stype = '商超' then 1
		when stype = '大客户' then 2
		when stype = '大宗' then 3
		else 4
	end ,
	province_code ;