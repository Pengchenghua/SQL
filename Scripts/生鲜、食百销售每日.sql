-- refresh csx_dw.sale_goods_m;
-- 19年截止至9月15日，食百商品销售情况，字段，省区，商品（编码+品名），销售数量，销售金额
select mon, channel_name,
	case when a.province_code is null or a.province_code='' then b.province_code else a.province_code end province_code,
	case when a.province_name is null or a.province_name='' then b.province_name else a.province_name end province_name,
	dept_id,
	dept_name,
	category_large_code,
	category_large_name,
	sum(qty)qty,
	sum(sale)sale,
	sum(profit)profit
from 
(
select
	SUBSTRING(sdt, 1, 6)mon,
	channel_name,
	shop_id,
	province_code,
	province_name,
	dept_id,
	dept_name,
	category_large_code,
	category_large_name,
	sum(sales_qty)qty,
	sum(sales_value)sale,
	sum(profit)profit
from
	csx_dw.sale_goods_m a
where
	sdt>='20190101' and sdt<='20190915'
	and category_small_code between '10000000' and '11999999'
group by
category_large_code,
	category_large_name,
	SUBSTRING(sdt, 1, 6),
	channel_name,
	goods_code,
	goods_name,
	dept_id,
	dept_name,
	shop_id,
	province_code,
	province_name
) a 
left OUTER join  
(select shop_id ,b.province_name,province_code from csx_dw.shop_m b where sdt='20190915') b on 
a.shop_id=b.shop_id 
group by 
mon, channel_name,
	case when a.province_code is null or a.province_code='' then b.province_code else a.province_code end ,
	case when a.province_name is null or a.province_name='' then b.province_name else a.province_name end ,
	dept_id,
	dept_name,
	category_large_code,
	category_large_name
;