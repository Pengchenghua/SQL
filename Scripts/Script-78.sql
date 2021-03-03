SELECT
	substr(sdt,
	1,
	6)mon,
	0 sale,
	0 sale1,
	0 sale2,
	sum(sales_value)sale3
FROM
	csx_dw.sale_item_m
WHERE
	sales_type IN('qyg',
	'gc',
	'anhui',
	'sc')
	AND sdt <= '20191010'
	AND sdt >= '20191001'
GROUP BY
	substr(sdt,
	1,
	6);

refresh csx_dw.sale_goods_m;

select
	*
from
	csx_dw.sale_goods_m a
where
	a.goods_code = '626'
	and shop_id = 'W0H4'
	and sdt >= '20191101' ;

SELECT
	*
from
	dw.shop_goods_fct
where
	goodsid = '626'
	and shop_id = 'W0H4'
	and sdt >= '20191101';

select
	regexp_replace(to_date(CURRENT_DATE ()),
	'-',
	'')