SELECT
	sdt,sales_type,
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
    sales_type,
	round(SUM(sales_value ),0)sale_order,
	round(SUM(profit ),0)profit_order,
	0 sale_ods,
	0 profit_ods
FROM
	csx_dw.sale_b2b_item a
	where
	sdt>='20190101'
	and  sales_type in('qyg','gc','anhui','sc','bbc')
GROUP BY sdt,sales_type
UNION ALL
SELECT
	sdt,
    sales_type,
	0 sale_order,
	0 profit_order,
	round(SUM(sales_value),0)sale_ods,
	round(SUM(profit),0)profit_ods
FROM
	csx_dw.customer_sale_m a
	where 
	sdt>='20190101'
	and sales_type in('qyg','gc','anhui','sc','bbc')
GROUP BY
	sdt,sales_type
	) a
	where sdt!='repair'
GROUP BY
sdt,sales_type
--HAVING SUM(sale_order)-SUM(sale_ods)>1
-- or (SUM(sale_order)-SUM(sale_ods)<-1))
ORDER BY
sdt DESC;



select  mon,if((SUM(sale1)-sum(sale))!=0,'是','否')  as diff_sale from (
select substr(sdt,1,6)mon,round(sum(COALESCE (sales_value,0))+sum(COALESCE (profit,0)) ,0)*1.0  sale ,0 sale1 from csx_dw.supple_goods_sale_dtl 
where sdt>=regexp_replace(to_date(trunc(add_months(date_sub(CURRENT_TIMESTAMP(),1),-3),'MM')),'-','')
      AND sdt<=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
group by  substr(sdt,1,6)
union all
select  substr(sdt,1,6) mon,0 sale ,round(sum(COALESCE (sales_value,0))+sum(COALESCE (profit,0)) ,0)*1.0 as  sale1 from csx_dw.customer_sale_m
where sdt>=regexp_replace(to_date(trunc(add_months(date_sub(CURRENT_TIMESTAMP (),1),-3),'MM')),'-','')
      AND sdt<=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
group by  substr(sdt,1,6)) as a 
group by mon 
order by mon
;
