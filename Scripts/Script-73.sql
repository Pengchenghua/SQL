select
	case when first_category='����ҵ��λ' then '����ҵ��λ' when first_category ='������ҵ' then '������ҵ' else '���������ӹ�' end note,
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
	case when first_category='����ҵ��λ' then '����ҵ��λ' when first_category ='������ҵ' then '������ҵ' else '���������ӹ�' end  ;

select
	DISTINCT province_code,
	province_name
from
	csx_dw.customer_sales
where
	sdt >= '20190101';

-- ��ҵ��������ռ��
select mon,note,sale,sale/sum(sale)over(PARTITION by mon ORDER  by mon ) as saleratio,profit/sale prorate,cust_cn
from 
(
select
	SUBSTRING(sdt,1,6)mon ,case when first_category='����ҵ��λ' then '����ҵ��λ' when first_category ='������ҵ' then '������ҵ' else '���������ӹ�' end note ,
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
	SUBSTRING(sdt,1,6),case when first_category='����ҵ��λ' then '����ҵ��λ' when first_category ='������ҵ' then '������ҵ' else '���������ӹ�' end  
)a order by mon
;

-- ÿ�����۴�ͻ�
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
	-- case when first_category='����ҵ��λ' then '����ҵ��λ' when first_category ='������ҵ' then '������ҵ' else '���������ӹ�' end note ,
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
	-- case when first_category='����ҵ��λ' then '����ҵ��λ' when first_category ='������ҵ' then '������ҵ' else '���������ӹ�' end  
)a order by mon;


select DISTINCT  channel_name ,province_name from csx_dw.customer_sales  