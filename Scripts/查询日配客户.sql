select * from  csx_dw.sale_item_m where sdt='20200219' and dc_code='W0A8';
select * from dw.sale_sap_dtl_fct where sdt='20200219' and shop_id='W0A8';
select DISTINCT workshop_code,workshop_name from csx_dw.factory_bom where sdt='20200220';
select * from csx_dw.customer_sales where sdt>='20200201' and customer_name ='江津区消防救援支队';

select small_workshop_code workshop_code,small_workshop_name  workshop_name from csx_dw.workshop_m ;
refresh csx_dw.customer_sale_m;
SELECT first_category,sum(sales_value) FROM csx_dw.customer_sale_m where sdt>='20200201' and sales_name ='欧启' GROUP by sales_name;
select DISTINCT `attribute`,attribute_code from csx_dw.customer_m where sdt='20200221';

select * from csx_dw.temp_sale_goods;

SELECT years,months,province_code,province_name,goods_code,goods_name,sum(sales_value)sale,sum(sales_qty)qty,sum(profit)profit
FROM csx_dw.dc_sale_inventory where goods_code='1191891' and sdt>='20190101'
GROUP by years,months,province_code,province_name,goods_code,goods_name;

select
	province_code,
	province_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	sum(sales_qty)qty,
	sum(sales_value)sale,
	sum(profit)profit
from
	csx_dw.customer_sale_m a
join (
	select
		customer_no,
		ATTRIBUTE
	from
		csx_dw.customer_m
	where
		sdt = '20200226'
		and attribute ='日配客户')b on
	a.customer_no = b.customer_no
	and sdt >= '20200101'
	and sdt <= '20200131'
	and order_kind !='WELFARE'
	--and (division_code is null or division_code ='')
	GROUP by 
	province_code,
	province_name,
	division_code,
	division_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name
	;
refresh csx_dw.customer_sale_m;
	select dc from csx_dw.customer_sale_m where sdt>='20200201' and channel='4' ;
	

select min(sdt) from csx_dw.dc_sale_inventory;

