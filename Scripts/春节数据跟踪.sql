set s_date='20200201';
set e_date='20200205';
set l_date='20191130';
set l_e_date='20191203';

SELECT
	province_code,
	province_name,
	sum(sale)/10000 sale,
	sum(year_sale)/10000 year_sale,
	sum(lunar_y_sale)/10000  lunar_y_sale,
	sum(mon_sale)/10000  mon_sale,
	round(coalesce(sum(sale)/sum(year_sale)-1,0),4) as year_sale_rate,
	round(coalesce(sum(sale)/sum(lunar_y_sale)-1,0),4) as lunar_y_sale_rate,
	round(coalesce(sum(sale)/sum(mon_sale)-1,0),4) as mon_sale_rate,
	sum(profit)/10000 profit,	
	sum(year_profit)/10000  year_profit,	
	sum(lunar_y_profit)/10000  lunar_y_profit,	
	sum(mon_profit)/10000  mon_profit,
	sum(profit)/sum(sale) as profit_rate,
	sum(cust_num) cust_num,
	sum(year_cust_num) year_cust_num,
	sum(lunar_y_cust_num) lunar_y_cust_num,
	sum(mon_cust_num) mon_cust_num,
	sum(cust_num)-sum(year_cust_num) as diff_year_cust_num,
	sum(cust_num)-sum(lunar_y_cust_num) as diff_lunar_y_cust_num,
	sum(cust_num)-sum(mon_cust_num) as diff_mon_cust_num
FROM
	(
	SELECT
		province_code,
		province_name,
		sum(sales_value)sale,
		sum(profit)profit,
		count(DISTINCT customer_no) cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${s_date}'
		AND sdt <= '${e_date}'
		AND channel IN('1',	'3','7')
	GROUP BY
		province_code,
		province_name
UNION ALL
	SELECT
		province_code,
		province_name,
		0 sale,
		0 profit,
		0 cust_num,
		sum(sales_value)year_sale,
		sum(profit)year_profit,
		count(DISTINCT customer_no) year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${y_s_date}'
		AND sdt <= '${y_e_date}'
		AND channel IN('1',	'3','7')
	GROUP BY
		province_code,
		province_name
UNION ALL
	SELECT
		province_code,
		province_name,
		0 sale,
		0 profit,
		0 cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		sum(sales_value) lunar_y_sale,
		sum(profit) lunar_y_profit,
		count(DISTINCT customer_no) lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${l_y_s_date}'
		AND sdt <= '${l_y_e_date}'
		AND channel IN('1',	'3','7')
	GROUP BY
		province_code,
		province_name
UNION ALL
	SELECT
		province_code,
		province_name,
		0 sale,
		0 profit,
		0 cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		sum(sales_value) mon_sale,
		sum(profit) mon_profit,
		count(DISTINCT customer_no) mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${m_s_date}'
		AND sdt <= '${m_e_date}'
		AND channel IN('1',	'3','7')
	GROUP BY
		province_code,
		province_name)a
GROUP BY
	province_code,
	province_name
order by 
case when 
substr(province_name ,1,6) ='福建' then 1 
when  substr(province_name ,1,6) ='北京' then 2
when  substr(province_name ,1,6) ='重庆' then 3
when  substr(province_name ,1,6) ='四川' then 4
when  substr(province_name ,1,6) ='上海' then 5
when  substr(province_name ,1,6) ='江苏' then 6
when  substr(province_name ,1,6) ='安徽' then 7
when  substr(province_name ,1,6) ='浙江' then 8
when  substr(province_name ,1,6) ='广东' then 9
when  substr(province_name ,1,6) ='河北' then 10
end asc;

-- 全国
SELECT
	'00'province_code,
	'全国'province_name,
	sum(sale)/10000 sale,
	sum(year_sale)/10000 year_sale,
	sum(lunar_y_sale)/10000  lunar_y_sale,
	sum(mon_sale)/10000  mon_sale,
	round(coalesce(sum(sale)/sum(year_sale)-1,0),4) as year_sale_rate,
	round(coalesce(sum(sale)/sum(lunar_y_sale)-1,0),4) as lunar_y_sale_rate,
	round(coalesce(sum(sale)/sum(mon_sale)-1,0),4) as mon_sale_rate,
	sum(profit)/10000 profit,	
	sum(year_profit)/10000  year_profit,	
	sum(lunar_y_profit)/10000  lunar_y_profit,	
	sum(mon_profit)/10000  mon_profit,
	sum(profit)/sum(sale) as profit_rate,
	sum(cust_num) cust_num,
	sum(year_cust_num) year_cust_num,
	sum(lunar_y_cust_num) lunar_y_cust_num,
	sum(mon_cust_num) mon_cust_num,
	sum(cust_num)-sum(year_cust_num) as diff_year_cust_num,
	sum(cust_num)-sum(lunar_y_cust_num) as diff_lunar_y_cust_num,
	sum(cust_num)-sum(mon_cust_num) as diff_mon_cust_num
FROM
	(
	SELECT
		sum(sales_value)sale,
		sum(profit)profit,
		count(DISTINCT customer_no) cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${s_date}'
		AND sdt <= '${e_date}'
		AND channel IN('1',	'3','7')
UNION ALL
	SELECT
		0 sale,
		0 profit,
		0 cust_num,
		sum(sales_value)year_sale,
		sum(profit)year_profit,
		count(DISTINCT customer_no) year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${y_s_date}'
		AND sdt <= '${y_e_date}'
		AND channel IN('1',	'3','7')
UNION ALL
	SELECT
		0 sale,
		0 profit,
		0 cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		sum(sales_value) lunar_y_sale,
		sum(profit) lunar_y_profit,
		count(DISTINCT customer_no) lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${l_y_s_date}'
		AND sdt <= '${l_y_e_date}'
		AND channel IN('1',	'3','7')
UNION ALL
	SELECT
		0 sale,
		0 profit,
		0 cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		sum(sales_value) mon_sale,
		sum(profit) mon_profit,
		count(DISTINCT customer_no) mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${m_s_date}'
		AND sdt <= '${m_e_date}'
		AND channel IN('1',	'3','7')
)a
;

-- 明细
SELECT
	province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date,
	sum(sale)/10000 sale,
	sum(year_sale)/10000 year_sale,
	sum(lunar_y_sale)/10000  lunar_y_sale,
	sum(mon_sale)/10000  mon_sale,
	round(coalesce(sum(sale)/sum(year_sale)-1,0),4) as year_sale_rate,
	round(coalesce(sum(sale)/sum(lunar_y_sale)-1,0),4) as lunar_y_sale_rate,
	round(coalesce(sum(sale)/sum(mon_sale)-1,0),4) as mon_sale_rate,
	sum(profit)/10000 profit,	
	sum(year_profit)/10000  year_profit,	
	sum(lunar_y_profit)/10000  lunar_y_profit,	
	sum(mon_profit)/10000  mon_profit,
	sum(profit)/sum(sale) as profit_rate,
	sum(sdt_num) sdt_num,
	sum(year_sdt_num) year_sdt_num,
	sum(lunar_y_sdt_num) lunar_y_sdt_num,
	sum(mon_sdt_num) mon_sdt_num,
	sum(sdt_num)-sum(year_sdt_num) as diff_year_sdt_num,
	sum(sdt_num)-sum(lunar_y_sdt_num) as diff_lunar_y_sdt_num,
	sum(sdt_num)-sum(mon_sdt_num) as diff_mon_sdt_num
FROM
	(
	SELECT	
		province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date,
		sum(sales_value)sale,
		sum(profit)profit,
		count(DISTINCT sdt) sdt_num,
		0 year_sale,
		0 year_profit,
		0 year_sdt_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_sdt_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_sdt_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${s_date}'
		AND sdt <= '${e_date}'
		AND channel IN('1',	'3','7')
group by 
province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date
UNION ALL
	SELECT
	province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date,
		0 sale,
		0 profit,
		0 sdt_num,
		sum(sales_value)year_sale,
		sum(profit)year_profit,
		count(DISTINCT sdt) year_sdt_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_sdt_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_sdt_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${y_s_date}'
		AND sdt <= '${y_e_date}'
		AND channel IN('1',	'3','7')
	group by 
	province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date
UNION ALL
	SELECT
	province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date,
		0 sale,
		0 profit,
		0 sdt_num,
		0 year_sale,
		0 year_profit,
		0 year_sdt_num,
		sum(sales_value) lunar_y_sale,
		sum(profit) lunar_y_profit,
		count(DISTINCT sdt) lunar_y_sdt_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_sdt_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${l_y_s_date}'
		AND sdt <= '${l_y_e_date}'
		AND channel IN('1',	'3','7')
		group by province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date
UNION ALL
	SELECT
	province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date,
		0 sale,
		0 profit,
		0 sdt_num,
		0 year_sale,
		0 year_profit,
		0 year_sdt_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_sdt_num,
		sum(sales_value) mon_sale,
		sum(profit) mon_profit,
		count(DISTINCT sdt) mon_sdt_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${m_s_date}'
		AND sdt <= '${m_e_date}'
		AND channel IN('1',	'3','7')
		group by 
		province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date
)a
group by province_code,
		province_name,
		customer_no,customer_name,first_category,first_category_code,sign_date;
-- 福州城市企业属性
SELECT
	province_code,
	province_name,first_category,
	sum(sale)/10000 sale,
	sum(year_sale)/10000 year_sale,
	sum(lunar_y_sale)/10000  lunar_y_sale,
	sum(mon_sale)/10000  mon_sale,
	round(coalesce(sum(sale)/sum(year_sale)-1,0),4) as year_sale_rate,
	round(coalesce(sum(sale)/sum(lunar_y_sale)-1,0),4) as lunar_y_sale_rate,
	round(coalesce(sum(sale)/sum(mon_sale)-1,0),4) as mon_sale_rate,
	sum(profit)/10000 profit,	
	sum(year_profit)/10000  year_profit,	
	sum(lunar_y_profit)/10000  lunar_y_profit,	
	sum(mon_profit)/10000  mon_profit,
	sum(profit)/sum(sale) as profit_rate,
	sum(cust_num) cust_num,
	sum(year_cust_num) year_cust_num,
	sum(lunar_y_cust_num) lunar_y_cust_num,
	sum(mon_cust_num) mon_cust_num,
	sum(cust_num)-sum(year_cust_num) as diff_year_cust_num,
	sum(cust_num)-sum(lunar_y_cust_num) as diff_lunar_y_cust_num,
	sum(cust_num)-sum(mon_cust_num) as diff_mon_cust_num
FROM
	(
	SELECT
		province_code,
		province_name,
		first_category,
		sum(sales_value)sale,
		sum(profit)profit,
		count(DISTINCT customer_no) cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${s_date}'
		AND sdt <= '${e_date}'
		AND channel IN('1',	'3','7')
		and city_name like '福州%'
	GROUP BY
		province_code,
		province_name,first_category
UNION ALL
	SELECT
		province_code,
		province_name,first_category,
		0 sale,
		0 profit,
		0 cust_num,
		sum(sales_value)year_sale,
		sum(profit)year_profit,
		count(DISTINCT customer_no) year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${y_s_date}'
		AND sdt <= '${y_e_date}'
		AND channel IN('1',	'3','7')
		and city_name='福州市'
	GROUP BY
		province_code,
		province_name,first_category
UNION ALL
	SELECT
		province_code,
		province_name,first_category,
		0 sale,
		0 profit,
		0 cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		sum(sales_value) lunar_y_sale,
		sum(profit) lunar_y_profit,
		count(DISTINCT customer_no) lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${l_y_s_date}'
		AND sdt <= '${l_y_e_date}'
		AND channel IN('1',	'3','7')
		and city_name='福州市'
	GROUP BY
		province_code,
		province_name,first_category
UNION ALL
	SELECT
		province_code,
		province_name,first_category,
		0 sale,
		0 profit,
		0 cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		sum(sales_value) mon_sale,
		sum(profit) mon_profit,
		count(DISTINCT customer_no) mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${m_s_date}'
		AND sdt <= '${m_e_date}'
		AND channel IN('1',	'3','7')
		and city_name='福州市'
	GROUP BY
		province_code,
		province_name,first_category)a
GROUP BY
	province_code,
	province_name,first_category
order by 
case when 
substr(province_name ,1,6) ='福建' then 1 
when  substr(province_name ,1,6) ='北京' then 2
when  substr(province_name ,1,6) ='重庆' then 3
when  substr(province_name ,1,6) ='四川' then 4
when  substr(province_name ,1,6) ='上海' then 5
when  substr(province_name ,1,6) ='江苏' then 6
when  substr(province_name ,1,6) ='安徽' then 7
when  substr(province_name ,1,6) ='浙江' then 8
when  substr(province_name ,1,6) ='广东' then 9
when  substr(province_name ,1,6) ='河北' then 10
end asc;

-- 省区企业属性
SELECT
	province_code,
	province_name,first_category,second_category,
	sum(sale)/10000 sale,
	sum(year_sale)/10000 year_sale,
	sum(lunar_y_sale)/10000  lunar_y_sale,
	sum(mon_sale)/10000  mon_sale,
	round(coalesce(sum(sale)/sum(year_sale)-1,0),4) as year_sale_rate,
	round(coalesce(sum(sale)/sum(lunar_y_sale)-1,0),4) as lunar_y_sale_rate,
	round(coalesce(sum(sale)/sum(mon_sale)-1,0),4) as mon_sale_rate,
	sum(profit)/10000 profit,	
	sum(year_profit)/10000  year_profit,	
	sum(lunar_y_profit)/10000  lunar_y_profit,	
	sum(mon_profit)/10000  mon_profit,
	sum(profit)/sum(sale) as profit_rate,
	sum(cust_num) cust_num,
	sum(year_cust_num) year_cust_num,
	sum(lunar_y_cust_num) lunar_y_cust_num,
	sum(mon_cust_num) mon_cust_num,
	sum(cust_num)-sum(year_cust_num) as diff_year_cust_num,
	sum(cust_num)-sum(lunar_y_cust_num) as diff_lunar_y_cust_num,
	sum(cust_num)-sum(mon_cust_num) as diff_mon_cust_num
FROM
	(
	SELECT
		province_code,
		province_name,
		first_category,
		second_category,
		sum(sales_value)sale,
		sum(profit)profit,
		count(DISTINCT customer_no) cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${s_date}'
		AND sdt <= '${e_date}'
		AND channel IN('1',	'3','7')
		and city_name like '福州%'
	GROUP BY
		province_code,
		province_name,	
		second_category,
first_category
UNION ALL
	SELECT
		province_code,
		province_name,first_category,
		second_category,
		0 sale,
		0 profit,
		0 cust_num,
		sum(sales_value)year_sale,
		sum(profit)year_profit,
		count(DISTINCT customer_no) year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${y_s_date}'
		AND sdt <= '${y_e_date}'
		AND channel IN('1',	'3','7')
		and city_name='福州市'
	GROUP BY
		province_code,
		province_name,second_category,first_category
UNION ALL
	SELECT
		province_code,
		province_name,first_category,
		second_category,
		0 sale,
		0 profit,
		0 cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		sum(sales_value) lunar_y_sale,
		sum(profit) lunar_y_profit,
		count(DISTINCT customer_no) lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${l_y_s_date}'
		AND sdt <= '${l_y_e_date}'
		AND channel IN('1',	'3','7')
		and city_name='福州市'
	GROUP BY
		province_code,
		province_name,second_category,first_category
UNION ALL
	SELECT
		province_code,
		province_name,first_category,
		second_category,
		0 sale,
		0 profit,
		0 cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		sum(sales_value) mon_sale,
		sum(profit) mon_profit,
		count(DISTINCT customer_no) mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${m_s_date}'
		AND sdt <= '${m_e_date}'
		AND channel IN('1',	'3','7')
		and city_name='福州市'
	GROUP BY
		province_code,
		province_name,second_category,first_category)a
GROUP BY
	province_code,
	province_name,second_category,first_category
order by 
case when 
first_category  ='企事业单位' then 1 
when  first_category  ='食品加工企业' then 2
when  first_category='餐饮企业'  then 3
when   first_category='个人及其他'  then 4
end asc;

	select 
	sdt,from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd') sdate,
	case when dayofweek(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))=7 then 6
	when dayofweek(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))=1 then 7
	else dayofweek(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))-1 end week,
	DAYNAME(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd')) as weekname,
	week(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))as weekof,
	province_code,
		province_name,first_category,
		second_category,customer_no,customer_name,sum(sales_value)sale 
	from csx_dw.customer_sale_m 
where sdt>='20191101' and sdt<='20200205' and second_category='监狱' and city_name='福州市'
group by sdt,province_code,
		province_name,first_category,
		second_category,customer_no,customer_name;
	


		select next_day('2018-02-27 10:03:01', 'TU');
select DAYNAME(to_date(now()))
;


	SELECT
		province_code,
		province_name,
		sum(sales_value)sale,
		sum(profit)profit,
		count(DISTINCT customer_no) cust_num,
		0 year_sale,
		0 year_profit,
		0 year_cust_num,
		0 lunar_y_sale,
		0 lunar_y_profit,
		0 lunar_y_cust_num,
		0 mon_sale,
		0 mon_profit,
		0 mon_cust_num
	FROM
		csx_dw.customer_sale_m
	WHERE
		sdt >= '${s_date}'
		AND sdt <= '${e_date}'
		AND channel IN('1',	'3','7')
	GROUP BY
		province_code,
		province_name;
	
	refresh csx_dw.provinces_kanban ;
select * from csx_dw.provinces_kanban where province_code='500000';

select category_large_code,category_large_name,goods_code,goods_name,unit_name,spec,sum(sales_qty)qty,sum(case when unit_name !='KG' then sales_qty*300 else sales_qty end )t_qty from csx_dw.dc_sale_inventory where sdt >='20200124' and sdt<='20200211' and category_large_code in ('1105','1103','1104','') or category_middle_code in('110130')
or category_small_code in ('11080301','11080302','11080401','11080402')
group by category_large_code,category_large_name,goods_code,goods_name,unit_name,spec;


--淡水、海水 鱼类、家禽、猪肉、蛋类、蔬菜
 select
	category_large_code,
	category_large_name,
	goods_code,
	goods_name,
	unit_name,
	spec,
	sum(sales_qty)qty,
	sum(case when unit_name != 'KG' then sales_qty*300 else sales_qty end )t_qty
from
	csx_dw.dc_sale_inventory
where
	(sdt >= '20200124'
	and sdt <= '20200211')
	and (category_large_code in ('1105','1103','1104')
	or category_middle_code in('110130')
	or category_small_code in ('11080301',
	'11080302',
	'11080401',
	'11080402'))
group by
	category_large_code,
	category_large_name,
	goods_code,
	goods_name,
	unit_name,
	spec;

--淡水、海水 鱼类、家禽、猪肉、蛋类、蔬菜
select category_large_code,category_large_name,sum(sales_qty)qty,sum(case when unit_name !='KG' then sales_qty*350/1000000 else sales_qty/1000 end ) t_qty ,
sum(case when unit_name !='KG' then sales_qty*350/1000000 else sales_qty/1000 end ) /()
from csx_dw.dc_sale_inventory where (sdt >='20200124' and sdt<='20200211' )
--and province_name like '福建%') 
and (category_large_code in ('1105','1103','1104') or category_middle_code in('110130')
or category_small_code in ('11080301','11080302','11080401','11080402'))

group by category_large_code,category_large_name;


select * from dim.dim_catg where sdt='20200211' and div_id='11' and catg_l_id='1103'