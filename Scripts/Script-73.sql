select
	case when first_category='企事业单位' then '企事业单位' when first_category ='餐饮企业' then '餐饮企业' else '个人批发加工' end note,
	SUM(sales_value)/10000 sale,
	SUM (profit)/10000 profit,
	COUNT(DISTINCT customer_no )cust_cn
from
	csx_dw.customer_sales
where
	sdt >= '20190101'
	and sdt <= '20191031'
	and channel in ('1','7')
	and province_code not in ('34',	'35','36','33')
group by
	case when first_category='企事业单位' then '企事业单位' when first_category ='餐饮企业' then '餐饮企业' else '个人批发加工' end  ;

select
	DISTINCT province_code,
	province_name
from
	csx_dw.customer_sales
where
	sdt >= '20190101';

-- 企业属性销售占比
select mon,note,sale,sale/sum(sale)over(PARTITION by mon ORDER  by mon ) as saleratio,profit/sale prorate,cust_cn
from 
(
select
	SUBSTRING(sdt,1,6)mon ,case when first_category='企事业单位' then '企事业单位' when first_category ='餐饮企业' then '餐饮企业' else '个人批发加工' end note ,
	SUM(sales_value)/10000 sale,
	SUM (profit)/10000 profit,
	COUNT(DISTINCT customer_no )cust_cn
from
	csx_dw.customer_sales
where
	sdt >= '20190101'
	and sdt <= '20191031'
	and channel in ('1','7')
	and province_code not in ('34',	'35','36','33')
group by
	SUBSTRING(sdt,1,6),case when first_category='企事业单位' then '企事业单位' when first_category ='餐饮企业' then '餐饮企业' else '个人批发加工' end  
)a order by mon
;

-- 每月销售大客户
select mon,
--note,
sale sale ,
-- sale/sum(sale)over(PARTITION by mon ORDER  by mon ) as saleratio,
profit/sale prorate,
cust_cn
from 
(
select
	SUBSTRING(sdt,1,6)mon ,
	-- case when first_category='企事业单位' then '企事业单位' when first_category ='餐饮企业' then '餐饮企业' else '个人批发加工' end note ,
	SUM(sales_value)/10000 sale,
	SUM (profit)/10000 profit,
	COUNT(DISTINCT customer_no )cust_cn
from
	csx_dw.customer_sales
where
	sdt >= '20180701'
	and sdt <= '20191031'
	and channel in ('1','7')
	and province_code not in ('34',	'35','36','33')
group by
	SUBSTRING(sdt,1,6)
	-- case when first_category='企事业单位' then '企事业单位' when first_category ='餐饮企业' then '餐饮企业' else '个人批发加工' end  
)a order by mon;


select DISTINCT  channel_name ,province_name from csx_dw.customer_sales  