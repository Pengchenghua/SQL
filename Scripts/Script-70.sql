refresh csx_dw.customer_sales;
		SELECT
			CASE
				WHEN channel ='7' THEN '大客户'
				ELSE a.channel_name
		END AS STYPE ,
			province_name as prov_name ,
			substr(sdt,	1,6) sdt ,
			province_manager_name MANAGE ,
			SUM(a.sales_value)/ 10000 * 1.00 sale ,
			SUM(a.profit) / 10000 * 1.00 profit ,
			0 h_sale
		FROM
			csx_dw.customer_sales a
		WHERE
			sdt <= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),	1)),'-','')
			AND sdt >= regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'YY')),	'-',	'')
		GROUP BY
			CASE
				WHEN channel ='7' THEN '大客户'
				ELSE a.channel_name
		END  ,
			 province_name,
			sdt ,
			a.province_manager_name
	UNION ALL
		SELECT
			CASE
				WHEN channel ='7' THEN '大客户'
				ELSE a.channel_name
		END  AS STYPE ,
			province_name as prov_name ,
			substr(sdt,1,6) sdt ,
			province_manager_name MANAGE ,
			0 sale ,
			0 profit ,
			SUM(sales_value)/ 10000 * 1.00 h_sale
		FROM
			csx_dw.customer_sales a
		WHERE
			sdt <= regexp_replace(to_date(add_months(date_sub(CURRENT_TIMESTAMP(),1),-12)),	'-','')
			AND sdt >= regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),366),'YY')),'-','')	
			GROUP BY
			CASE
				WHEN channel ='7' THEN '大客户'
				ELSE a.channel_name
		END ,
			 province_name ,
			sdt ,
			a.province_manager_name 