SELECT
	sdt,
	SUM(sale_order)sale_order,
	SUM(sale_ods)sale_ods,
	SUM(profit_order)profit_order,
	SUM(profit_ods)profit_ods,
	CASE WHEN SUM(sale_order)>SUM(sale_ods)
			THEN concat('customer_sale_m表数据缺失||',	ifnull(CAST(SUM(sale_order)-SUM(sale_ods) AS string),	''))
		WHEN SUM(sale_order)< SUM(sale_ods) 
			THEN concat('sale_b2b_item表数据多于customer_sale_m||',ifnull(CAST(ABS(SUM(sale_order)-SUM(sale_ods))AS string),	''))
	ELSE '数据一致'
END note
FROM
(
SELECT
	sdt,
	round(SUM(sales_value ),0)sale_order,
	round(SUM(profit ),0)profit_order,
	0 sale_ods,
	0 profit_ods
FROM
	csx_dw.sale_b2b_item a
	where
	sdt>='20190101'
	and  sales_type in('qyg','gc','anhui','sc')
GROUP BY sdt
UNION ALL
SELECT
	sdt,
	0 sale_order,
	0 profit_order,
	round(SUM(sales_value),0)sale_ods,
	round(SUM(profit),0)profit_ods
FROM
	csx_dw.customer_sale_m a
	where 
	sdt>='20190101'
	--and  sales_type in('qyg','gc','anhui','sc')
GROUP BY
	sdt
	) a
	where sdt!='repair'
GROUP BY
sdt
--HAVING SUM(sale_order)-SUM(sale_ods)>1
-- or (SUM(sale_order)-SUM(sale_ods)<-1))
ORDER BY
sdt DESC;