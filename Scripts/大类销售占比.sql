SELECT
	division_code,
	division_name,
	channel_name,
	sale_sku,
	sale,
	profit,
	profit/sale as profitrate,
	sale/sum(sale)over(partition by channel_name) as sale_raito
	from (
SELECT
	division_code,
	division_name,channel_name,
	COUNT(DISTINCT goods_code) sale_sku,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel in ('1','2','7')
	--AND cha
	group by 
	division_code,
	division_name,
	channel_name
)a order by sale_raito desc
;

--工厂销售

SELECT
	is_factory_goods_code,
	is_factory_goods_name,
	province_code,province_name,
	sale,
	profit,
	sale/sum(sale)over(partition by province_code) as sale_raito,
	sale_sku
	from (
SELECT
	is_factory_goods_code,
	is_factory_goods_name,
	province_code,province_name,
	COUNT(DISTINCT goods_code)sale_sku,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel in ('1','2','7')
	group by 
	is_factory_goods_code,
	is_factory_goods_name,province_code,province_name
)a order by sale_raito desc
;

-- 课组销售占比
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	--channel_name,
	sale_sku,
	sale,
	profit,
	profit/sale as profitrate,
	sale/sum(sale)over() as sale_raito,
	 fac_sale_sku AS fac_sale_sku,
	 fac_sale AS fac_sale,
	 fac_profit AS  fac_profit
	from (
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	--channel_name,
	sum(sale_sku)sale_sku,
	sum(sale)sale,
	sum(profit)profit,
	sum(profit)/sum(sale) as profitrate,
	--sale/sum(sale)over() as sale_raito,
	 sum(fac_sale_sku)fac_sale_sku,
	 sum(fac_sale)fac_sale,
	 sum(fac_profit)fac_profit
	from (
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,--channel_name,
	COUNT(DISTINCT goods_code) sale_sku,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit,
	0 fac_sale_sku,
	0 fac_sale,
	0 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301' 
	and sdt <= '20200321'
	AND channel in ('1','2','7')
	group by 
	category_large_code,
	category_large_name,
	department_code,
	department_name
union all SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	0 sale_sku, 0 sale,0 profit,
	COUNT(DISTINCT goods_code) fac_sale_sku,
	sum(sales_value)/10000 fac_sale,
	sum(profit)/10000 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel in ('1','2','7')
	and is_factory_goods_code=1
	group by 
	category_large_code,
	category_large_name,
	department_code,
	department_name
)a 
group by department_code,
	department_name,
	category_large_code,
	category_large_name
	)a
order by sale_raito desc
;


SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	
	sale,
		sale/sum(sale)over(partition by channel_name) as sale_raito,
	profit,
	profit/sale as profitrate,
	sale_sku,	
	 fac_sale AS fac_sale,
	   fac_sale_sku AS fac_sale_sku,
	 fac_profit AS  fac_profit,
	 fac_profit/fac_sale as fac_profitrate,
	 fac_sale/sale as fac_sale_ratio,
	fac_sale_sku/sale_sku as sku_ratio
	from (
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	sum(sale_sku)sale_sku,
	sum(sale)sale,
	sum(profit)profit,
	sum(profit)/sum(sale) as profitrate,
	--sale/sum(sale)over() as sale_raito,
	 sum(fac_sale_sku)fac_sale_sku,
	 sum(fac_sale)fac_sale,
	 sum(fac_profit)fac_profit
	from (
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	COUNT(DISTINCT goods_code) sale_sku,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit,
	0 fac_sale_sku,
	0 fac_sale,
	0 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel_name in ('大客户','企业购 ','商超')
	group by 
	category_large_code,
	category_large_name,
	channel_name,
	department_code,
	department_name
union all SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	0 sale_sku, 0 sale,0 profit,
	COUNT(DISTINCT goods_code) fac_sale_sku,
	sum(sales_value)/10000 fac_sale,
	sum(profit)/10000 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel_name in ('大客户','企业购 ','商超')
	and is_factory_goods_code=1
	group by 
	category_large_code,
	category_large_name,
	channel_name,
	department_code,
	department_name
)a 
group by department_code,
	department_name,
	channel_name,
	category_large_code,
	category_large_name
	)a
order by sale_raito desc
;

--省区
SELECT
	province_code,
	province_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,	
	sale,
	sale/sum(sale)over(partition by channel_name,province_code) as sale_raito,
	profit,
	profit/sale as profitrate,
	sale_sku,	
	fac_sale AS fac_sale,
	fac_sale_sku AS fac_sale_sku,
	 fac_profit AS  fac_profit,
	 fac_profit/fac_sale as fac_profitrate,
	 fac_sale/sale as fac_sale_ratio,
	fac_sale_sku/sale_sku as sku_ratio
	from (
SELECT
	province_code,
	province_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	sum(sale_sku)sale_sku,
	sum(sale)sale,
	sum(profit)profit,
	sum(profit)/sum(sale) as profitrate,
	--sale/sum(sale)over() as sale_raito,
	 sum(fac_sale_sku)fac_sale_sku,
	 sum(fac_sale)fac_sale,
	 sum(fac_profit)fac_profit
	from (
SELECT
	province_code,
	province_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	COUNT(DISTINCT goods_code) sale_sku,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit,
	0 fac_sale_sku,
	0 fac_sale,
	0 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel_name in ('大客户','企业购 ','商超')
	group by 
	category_large_code,
	category_large_name,
	channel_name,
	department_code,
	province_code,
	province_name,
	department_name
union all SELECT
	province_code,
	province_name,
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	0 sale_sku, 0 sale,0 profit,
	COUNT(DISTINCT goods_code) fac_sale_sku,
	sum(sales_value)/10000 fac_sale,
	sum(profit)/10000 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel_name in ('大客户','企业购 ','商超')
	and is_factory_goods_code=1
	group by 
	province_code,
	province_name,
	category_large_code,
	category_large_name,
	channel_name,
	department_code,
	department_name
)a 
group by department_code,
	department_name,
	channel_name,
	province_code,
	province_name,
	category_large_code,
	category_large_name
	)a
order by sale_raito desc
;


SELECT
	
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,	
	goods_code,
	goods_name,
	sale,
	sale/sum(sale)over(partition by channel_name) as sale_raito,
	profit,
	profit/sale as profitrate,
--	sale_sku,	
	fac_sale AS fac_sale,
--	fac_sale_sku AS fac_sale_sku,
	 fac_profit AS  fac_profit,
	 fac_profit/fac_sale as fac_profitrate,
	 fac_sale/sale as fac_sale_ratio
	--fac_sale_sku/sale_sku as sku_ratio
	from (
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	goods_code,
	goods_name,
	--sum(sale_sku)sale_sku,
	sum(sale)sale,
	sum(profit)profit,
	sum(profit)/sum(sale) as profitrate,
	--sale/sum(sale)over() as sale_raito,
	-- sum(fac_sale_sku)fac_sale_sku,
	 sum(fac_sale)fac_sale,
	 sum(fac_profit)fac_profit
	from (
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	goods_code,
	goods_name,
--	COUNT(DISTINCT goods_code) sale_sku,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit,
--	0 fac_sale_sku,
	0 fac_sale,
	0 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel_name in ('大客户','企业购 ','商超')
	group by 
	category_large_code,
	category_large_name,
	channel_name,
	goods_code,
	goods_name,
	department_code,
	department_name
union all SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	goods_code,
	goods_name,
	--0 sale_sku, 
	0 sale,
	0 profit,
--	COUNT(DISTINCT goods_code) fac_sale_sku,
	sum(sales_value)/10000 fac_sale,
	sum(profit)/10000 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel_name in ('大客户','企业购 ','商超')
	and is_factory_goods_code=1
	group by 
	goods_code,
	goods_name,
	category_large_code,
	category_large_name,
	channel_name,
	department_code,
	department_name
)a 
group by department_code,
	department_name,
	channel_name,
	goods_code,
	goods_name,
	category_large_code,
	category_large_name
	)a
order by sale_raito desc
;


SELECT
	
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	--channel_name,	
	goods_code,
	goods_name,
	sale,
	sale/sum(sale)over() as sale_raito,
	profit,
	profit/sale as profitrate,
--	sale_sku,	
	fac_sale AS fac_sale,
--	fac_sale_sku AS fac_sale_sku,
	 fac_profit AS  fac_profit,
	 fac_profit/fac_sale as fac_profitrate,
	 fac_sale/sale as fac_sale_ratio
	--fac_sale_sku/sale_sku as sku_ratio
	from (
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	--channel_name,
	goods_code,
	goods_name,
	--sum(sale_sku)sale_sku,
	sum(sale)sale,
	sum(profit)profit,
	sum(profit)/sum(sale) as profitrate,
	--sale/sum(sale)over() as sale_raito,
	-- sum(fac_sale_sku)fac_sale_sku,
	 sum(fac_sale)fac_sale,
	 sum(fac_profit)fac_profit
	from (
SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	goods_code,
	goods_name,
--	COUNT(DISTINCT goods_code) sale_sku,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit,
--	0 fac_sale_sku,
	0 fac_sale,
	0 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel_name in ('大客户','企业购 ','商超')
	group by 
	category_large_code,
	category_large_name,
	channel_name,
	goods_code,
	goods_name,
	department_code,
	department_name
union all SELECT
	department_code,
	department_name,
	category_large_code,
	category_large_name,
	channel_name,
	goods_code,
	goods_name,
	--0 sale_sku, 
	0 sale,
	0 profit,
--	COUNT(DISTINCT goods_code) fac_sale_sku,
	sum(sales_value)/10000 fac_sale,
	sum(profit)/10000 fac_profit
from
	csx_dw.customer_sale_m
where
	sdt >= '20200301'
	and sdt <= '20200321'
	AND channel_name in ('大客户','企业购 ','商超')
	and is_factory_goods_code=1
	group by 
	goods_code,
	goods_name,
	category_large_code,
	category_large_name,
	channel_name,
	department_code,
	department_name
)a 
group by department_code,
	department_name,
	--channel_name,
	goods_code,
	goods_name,
	category_large_code,
	category_large_name
	)a
order by sale_raito desc
--limit 20
;
SELECT * from csx_dw.customer_sale_m where sdt='20200320' and goods_code='707' and dc_code='W0H4';

SELECT * from csx_dw.sale_item_m where sdt='20200320' and goods_code='707' and dc_code='W0H4';
