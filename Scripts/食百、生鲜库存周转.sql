
-- 企业购归属大
-- 期末库存,以业务时间为时间节点
-- 新系统库存，需要将旧系统的sales_dist not in ('612000','613000') 北京、安徽剔除 
-- 云超库取未切换新系统的库存
-- 旧系统剔除 inv_place NOT IN ('B997','B999') 新系统剔除 reservoir_area_code not in ('PD01','PD02','TS01')
 SET
mapreduce.job.queuename = caishixian;

SET
edate = '2020-01-11';

SET
sdate = '2020-01-01';

DROP TABLE IF EXISTS temp.p_invt_1;

CREATE TEMPORARY TABLE IF NOT EXISTS temp.p_invt_1 AS
-- 库存查询
 select dc_code,goods_code goodsid, sum(qty)inv_qty,sum(amt)period_inv_amt,
sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then qty end )final_qty,
sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then amt end )qm_amt
from csx_dw.wms_accounting_stock_m where sdt>=regexp_replace(${hiveconf:sdate},'-','')  and sdt<=regexp_replace(${hiveconf:edate},'-','') 
and reservoir_area_code not in ('B999','B997','PD01','PD02','TS01')
group  by dc_code,goods_code
;
--select * from temp.p_invt_1 a where  shop_id='W0A2';
--关联库存与销售
-- SELECT prov_code,prov_name,bd_id,bd_name,dept_id,dept_name,sum(sale)sale,sum
-- FROM 
-- (
 DROP TABLE IF EXISTS temp.p_invt_2;

CREATE TEMPORARY TABLE IF NOT EXISTS temp.p_invt_2 AS
SELECT
	b.prov_code,
	b.prov_name,
	a.shop_id,
	b.shop_name,
	a.goodsid,
	c.goodsname,
	c.standard ,
	c.unit_name ,
	c.brand_name ,
	c.dept_id,
	c.dept_name,
	c.bd_id,
	c.bd_name,
	c.div_id,
	c.div_name,
	c.catg_l_id,
	c.catg_l_name,
	product_status_id ,
	product_status_name,
	SUM(qty) qty,
	SUM(a.sale)sale,
	SUM(profit)profit,
	COALESCE(SUM(sale-profit),	0) AS sales_cost,
	SUM(inv_qty)inv_qty,
	SUM(period_inv_amt)period_inv_amt,
	SUM(final_qty)final_qty,
	SUM(qm_amt)qm_amt,
	COALESCE(SUM(period_inv_amt)/ SUM(sale-profit),0) AS days_turnover
FROM
	(
	SELECT
		shop_id,
		goods_code goodsid,
		SUM(sales_qty)qty,
		SUM(sales_value)sale,
		SUM(profit)profit,
		0 inv_qty,
		0 period_inv_amt,
		0 final_qty,
		0 qm_amt
	FROM
		csx_dw.sale_goods_m1
	WHERE
		sdt >= regexp_replace(${hiveconf:sdate},'-','')
		AND sdt <= regexp_replace(${hiveconf:edate},'-','')
	GROUP BY
		shop_id,
		goods_code
UNION ALL
	SELECT
		a.dc_code AS shop_id,
		a.goodsid,
		0 qty,
		0 sale,
		0 profit,
		a.inv_qty,
		a.period_inv_amt,
		a.final_qty,
		a.qm_amt
	FROM
		temp.p_invt_1 a) a
JOIN (
	SELECT
		shop_id ,
		shop_name ,
		CASE WHEN a.shop_id = 'W0H4' THEN 'W0H4'
		ELSE a.province_code
END prov_code,
	CASE WHEN a.shop_id = 'W0H4' THEN '供应链平台'
	ELSE a.province_name
END prov_name
FROM
csx_dw.shop_m a
WHERE
sdt = 'current' ) b ON
	regexp_replace(a.shop_id,
	'^E',
	'9')= b.shop_id
JOIN dim.dim_goods_latest c ON
	a.goodsid = c.goodsid
LEFT OUTER JOIN (
	SELECT
		shop_code AS shop_id,
		product_code goodsid,
		product_status_name ,
		des_specific_product_status AS product_status_id
	FROM
		csx_ods.csx_product_info
	WHERE
		sdt = regexp_replace(${hiveconf:edate},
		'-',
		''))d ON
	a.shop_id = d.shop_id
	AND a.goodsid = d.goodsid
GROUP BY
	b.prov_code,
	b.prov_name,
	a.shop_id,
	b.shop_name,
	a.goodsid,
	c.goodsname,
	c.standard ,
	c.unit_name ,
	c.brand_name ,
	c.dept_id,
	c.dept_name,
	c.bd_id,
	c.bd_name,
	c.div_id,
	c.div_name,
	c.catg_l_id,
	c.catg_l_name,
	product_status_id ,
	product_status_name ;
drop table if exists csx_dw.supply_turnover_01;

create table csx_dw.supply_turnover_01
as 
SELECT
	*
FROM
	temp.p_invt_2
	;

-- DROP TABLE IF EXISTS temp.p_invt_2;
--
--CREATE TEMPORARY TABLE IF NOT EXISTS temp.p_invt_2 AS
--SELECT
--	b.prov_code,
--	b.prov_name,
--	dc_code shop_id,
--	dc_name shop_name,
--	goods_code goodsid,
--	goods_name goodsname,
--	spec as standard ,
--	unit_name ,
--	brand_name ,
--	dept_id,
--	dept_name,
--	bd_id,
--	bd_name,
--	div_id,
--	div_name,
--	category_large_code as catg_l_id,
--	category_large_name as  catg_l_name,
--	product_status_id ,
--	product_status_name,
--	SUM(qty) qty,
--	SUM(a.sale)sale,
--	SUM(profit)profit,
--	sum(sales_cost) as  sales_cost,
--	--SUM(inv_qty)inv_qty,
--	SUM(period_inv_amt)period_inv_amt,
--	SUM(final_qty)final_qty,
--	SUM(qm_amt)qm_amt,
--	COALESCE(SUM(period_inv_amt)/ SUM(sales_cost),0) AS days_turnover
--FROM
--	(select province_code,
--	province_name,
--	dc_code,dc_name,
--	goods_code,
--	goods_name,
--	spec,unit_name,brand_name,dept_id,dept_name,bd_id,bd_name,div_id,div_name,category_large_code,category_large_name,
--	sum(sales_qty)qty,
--	sum(sales_value) as sale,
--	sum(profit)profit,
--	sum(sales_sales_cost)sales_cost,
--	sum(inventory_amt)period_inv_amt,
--	sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then inventory_qty end )final_qty,
--	sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then inventory_amt end )qm_amt
--	from csx_dw.dc_sale_inventory where sdt>=regexp_replace(${hiveconf:sdate},'-','') and sdt<=regexp_replace(${hiveconf:edate},'-','')
--	group by province_code,
--	province_name,
--	dc_code,
--	dc_name,
--	goods_code,
--	goods_name,spec,unit_name,brand_name,dept_id,dept_name,bd_id,bd_name,div_id,div_name,category_large_code,category_large_name) a
--JOIN (
--	SELECT
--		shop_id ,
--		shop_name ,
--		CASE WHEN a.shop_id = 'W0H4' THEN 'W0H4'
--		ELSE a.province_code
--END prov_code,
--	CASE WHEN a.shop_id = 'W0H4' THEN '供应链平台'
--	ELSE a.province_name
--END prov_name
--FROM
--csx_dw.shop_m a
--WHERE
--sdt = 'current' ) b ON
--	regexp_replace(a.dc_code,'^E','9')= b.shop_id
--LEFT OUTER JOIN (
--	SELECT
--		shop_code AS shop_id,
--		product_code goodsid,
--		product_status_name ,
--		des_specific_product_status AS product_status_id
--	FROM
--		csx_ods.csx_product_info
--	WHERE
--		sdt = regexp_replace(date_sub(CURRENT_DATE(),1),	'-',''))d ON
--	a.dc_code = d.shop_id
--	AND a.goods_code = d.goodsid
--GROUP BY
--	prov_code,
--	prov_name,
--	dc_code,
--	dc_name,
--	goods_code,
--	goods_name,
--	spec ,
--	unit_name ,
--	brand_name ,
--	dept_id,
--	dept_name,
--	bd_id,
--	bd_name,
--	div_id,
--	div_name,
--	category_large_code,
--	category_large_name,
--	product_status_id ,
--	product_status_name ;
--食百明细
SELECT
	*
FROM
	temp.p_invt_2
WHERE
	bd_id = '12';
--生鲜明细
SELECT
	*
FROM
	temp.p_invt_2
WHERE
	bd_id = '11' ;
-- 查询食百课组明细无小计
-- SELECT
--	prov_code,
--	prov_name,
--	shop_id,
--	shop_name,
--	bd_id,
--	bd_name,
--	dept_id,
--	dept_name,
--	qty,
--	sale,
--	profit,
--	profit_rate,
--	sales_cost,
--	period_inv_amt,
--	qm_qmt,
--	final_qty,
--	days_turnover,
--	goods_sku,
--	sale_sku,
--	negative_inventory,
--	negative_amt,
--	highet_sku,
--	highet_amt,
--	b.location_type
--FROM
--	(
--	SELECT
--		prov_code,
--		prov_name,
--		shop_id,
--		shop_name,
--		bd_id,
--		bd_name,
--		dept_id,
--		dept_name,
--		SUM(qty) qty,
--		SUM(sale)/ 10000 * 1.00 sale,
--		SUM(profit)/ 10000 * 1.00 profit,
--		COALESCE(SUM(profit)/ SUM(sale),
--		0)* 1.00 AS profit_rate,
--		SUM(sales_cost)/ 10000 * 1.00 AS sales_cost,
--		SUM(period_inv_amt)/ 10000 * 1.00 period_inv_amt,
--		SUM(qm_amt)/ 10000 * 1.00 qm_qmt,
--		SUM(final_qty)/ 10000 * 1.00 final_qty,
--		round(COALESCE(SUM(period_inv_amt)/ SUM(sales_cost),
--		0),
--		2) AS days_turnover,
--		COUNT(DISTINCT goodsid)goods_sku,
--		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END)sale_sku,
--		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END)negative_inventory,
--		SUM(CASE WHEN qm_amt<0 THEN qm_amt END) negative_amt,
--		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND div_id = '12' AND qm_amt>500 ) THEN goodsid WHEN (days_turnover>45 AND div_id IN('13', '14') AND qm_amt>500 ) THEN goodsid END)highet_sku,
--		SUM(CASE WHEN (days_turnover>30 AND div_id = '12' AND qm_amt>500 ) THEN qm_amt WHEN (days_turnover>45 AND div_id IN('13', '14') AND qm_amt>500 ) THEN qm_amt END) highet_amt
--	FROM
--		temp.p_invt_2
--	WHERE
--		bd_id = '12'
--	GROUP BY
--		prov_code,
--		prov_name,
--		shop_id,
--		shop_name,
--		bd_id,
--		bd_name,
--		dept_id,
--		dept_name )a
--LEFT JOIN (
--	SELECT
--		*
--	FROM
--		csx_ods.md_all_shop_info_ods
--	WHERE
--		sdt = regexp_replace(${hiveconf:edate},
--		'-',
--		'')) b ON
--	regexp_replace(a.shop_id,
--	'E',
--	'9')= regexp_replace(b.rt_shop_code,
--	'E',
--	'9') ;
---- set mapreduce.job.queuename=caishixian;

-- 生鲜库存分析
 SELECT
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name,
	SUM(qty)/ 10000 qty,
	SUM(sale)/ 10000 * 1.00 sale,
	SUM(profit)/ 10000 * 1.00 profit ,
	COALESCE(SUM(profit)/ SUM(sale),
	0)* 1.00 AS profit_rate,
	SUM(sale-profit)/ 10000 * 1.00 AS sales_cost,
	SUM(period_inv_amt)/ 10000 * 1.00 period_inv_amt,
	SUM(qm_amt)/ 10000 * 1.00 qm_qmt,
	SUM(final_qty)/ 10000 * 1.00 final_qty,
	SUM(days_turnover)days_turnover,
	SUM(goods_sku)goods_sku,
	SUM(sale_sku)sale_sku,
	round(SUM(sale_sku)/ SUM(goods_sku),
	4)* 1.00 pin_rate,
	SUM(negative_inventory) negative_inventory,
	--负库存数
 SUM(negative_amt)/ 10000 * 1.00 negative_amt,
	SUM(highet_sku) AS highet_sku,
	SUM(highet_amt)/ 10000 * 1.00 AS highet_amt
FROM
	(
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		'00' AS bd_id,
		'小计' AS bd_name,
		'00' AS dept_id,
		'小计' AS dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(COALESCE(a.sale, 0)) sale,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.qm_amt, 0)) qm_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
UNION ALL
	SELECT
		'00' prov_code,
		'全国' prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(a.sale)sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
	GROUP BY
		bd_id,
		bd_name,
		dept_id,
		dept_name
UNION ALL
-- 省份明细
	SELECT
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name,
		a.dept_id,
		a.dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty) final_qty,
		round( COALESCE(SUM(period_inv_amt)/ SUM(sales_cost),
		0),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
	GROUP BY
		a.prov_code ,
		a.prov_name ,
		a.bd_id,
		a.bd_name,
		a.dept_id,
		a.dept_name
UNION ALL
-- 省份部类汇总
	SELECT
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name,
		'00' dept_id,
		'小计' dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
	GROUP BY
		a.prov_code ,
		a.prov_name ,
		a.bd_id,
		a.bd_name
UNION ALL
-- 省分汇总
	SELECT
		prov_code,
		prov_name,
		'00' bd_id,
		'小计' bd_name,
		'00' dept_id,
		'小计' dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
	GROUP BY
		prov_code,
		prov_name )a
GROUP BY
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name
ORDER BY
	prov_code,
	bd_id,
	dept_id;

-- 食百库存分析
 SELECT
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name,
	SUM(qty)/ 10000 * 1.00 qty,
	SUM(sale)/ 10000 * 1.00 sale,
	SUM(profit)/ 10000 * 1.00 profit ,
	COALESCE(SUM(profit)/ SUM(sale),
	0)* 1.00 AS profit_rate,
	SUM(sale-profit)/ 10000 * 1.00 AS sales_cost,
	SUM(period_inv_amt)/ 10000 * 1.00 period_inv_amt,
	SUM(qm_amt)/ 10000 * 1.00 qm_qmt,
	SUM(final_qty)/ 10000 * 1.00 final_qty,
	SUM(days_turnover)days_turnover,
	SUM(goods_sku)goods_sku,
	SUM(sale_sku)sale_sku,
	round(SUM(sale_sku)/ SUM(goods_sku),
	4)* 1.00 pin_rate,
	SUM(negative_inventory) negative_inventory ,
	--负库存数
 SUM(negative_amt)/ 10000 * 1.00 AS negative_amt,
	SUM(highet_sku)AS highet_sku,
	SUM(highet_amt)/ 10000 * 1.00 highet_amt
FROM
	(
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		'00' AS bd_id,
		'小计' AS bd_name,
		'00' AS dept_id,
		'小计' AS dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(COALESCE(a.sale, 0)) sale,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.qm_amt, 0)) qm_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
UNION ALL
	SELECT
		'00' prov_code,
		'全国' prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty)final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
	GROUP BY
		bd_id,
		bd_name,
		dept_id,
		dept_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name,
		a.dept_id,
		a.dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty)final_qty,
		round( COALESCE(SUM(period_inv_amt)/ SUM(sales_cost),
		0),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
	GROUP BY
		prov_code,
		prov_name ,
		a.bd_id,
		a.bd_name,
		a.dept_id,
		a.dept_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name,
		'00' dept_id,
		'小计' dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty)final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
	GROUP BY
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		'00' bd_id,
		'小计' bd_name,
		'00' dept_id,
		'小计' dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty)final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
	GROUP BY
		prov_code,
		prov_name )a
GROUP BY
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name
ORDER BY
	prov_code,
	bd_id,
	dept_id;


-- 联营小店库存质量（生鲜）
SELECT
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name,
	SUM(qty)/ 10000 qty,
	SUM(sale)/ 10000 * 1.00 sale,
	SUM(profit)/ 10000 * 1.00 profit ,
	COALESCE(SUM(profit)/ SUM(sale),
	0)* 1.00 AS profit_rate,
	SUM(sale-profit)/ 10000 * 1.00 AS sales_cost,
	SUM(period_inv_amt)/ 10000 * 1.00 period_inv_amt,
	SUM(qm_amt)/ 10000 * 1.00 qm_qmt,
	SUM(final_qty)/ 10000 * 1.00 final_qty,
	SUM(days_turnover)days_turnover,
	SUM(goods_sku)goods_sku,
	SUM(sale_sku)sale_sku,
	round(SUM(sale_sku)/ SUM(goods_sku),
	4)* 1.00 pin_rate,
	SUM(negative_inventory) negative_inventory,
	--负库存数
 SUM(negative_amt)/ 10000 * 1.00 negative_amt,
	SUM(highet_sku) AS highet_sku,
	SUM(highet_amt)/ 10000 * 1.00 AS highet_amt
FROM
	(
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		'00' AS bd_id,
		'小计' AS bd_name,
		'00' AS dept_id,
		'小计' AS dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(COALESCE(a.sale, 0)) sale,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.qm_amt, 0)) qm_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11' 
		and shop_id like 'E%'
UNION ALL
	SELECT
		'00' prov_code,
		'全国' prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(a.sale)sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
		and shop_id like 'E%'
	GROUP BY
		bd_id,
		bd_name,
		dept_id,
		dept_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name,
		a.dept_id,
		a.dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty) final_qty,
		round( COALESCE(SUM(period_inv_amt)/ SUM(sales_cost),
		0),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
		and shop_id like 'E%'
	GROUP BY
		a.prov_code ,
		a.prov_name ,
		a.bd_id,
		a.bd_name,
		a.dept_id,
		a.dept_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name,
		'00' dept_id,
		'小计' dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
		and shop_id like 'E%'
	GROUP BY
		a.prov_code ,
		a.prov_name ,
		a.bd_id,
		a.bd_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		'00' bd_id,
		'小计' bd_name,
		'00' dept_id,
		'小计' dept_name,
		SUM(COALESCE(a.qty, 0)) qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN days_turnover>15 AND qm_amt>500 THEN goodsid END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND qm_amt>500 THEN qm_amt END ) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '11'
		and shop_id like 'E%'
	GROUP BY
		prov_code,
		prov_name )a
GROUP BY
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name
ORDER BY
	prov_code,
	bd_id,
	dept_id;

-- 食百库存分析
 SELECT
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name,
	SUM(qty)/ 10000 * 1.00 qty,
	SUM(sale)/ 10000 * 1.00 sale,
	SUM(profit)/ 10000 * 1.00 profit ,
	COALESCE(SUM(profit)/ SUM(sale),
	0)* 1.00 AS profit_rate,
	SUM(sale-profit)/ 10000 * 1.00 AS sales_cost,
	SUM(period_inv_amt)/ 10000 * 1.00 period_inv_amt,
	SUM(qm_amt)/ 10000 * 1.00 qm_qmt,
	SUM(final_qty)/ 10000 * 1.00 final_qty,
	SUM(days_turnover)days_turnover,
	SUM(goods_sku)goods_sku,
	SUM(sale_sku)sale_sku,
	round(SUM(sale_sku)/ SUM(goods_sku),
	4)* 1.00 pin_rate,
	SUM(negative_inventory) negative_inventory ,
	--负库存数
 SUM(negative_amt)/ 10000 * 1.00 AS negative_amt,
	SUM(highet_sku)AS highet_sku,
	SUM(highet_amt)/ 10000 * 1.00 highet_amt
FROM
	(
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		'00' AS bd_id,
		'小计' AS bd_name,
		'00' AS dept_id,
		'小计' AS dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(COALESCE(a.sale, 0)) sale,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.qm_amt, 0)) qm_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
		and shop_id like 'E%'
UNION ALL
	SELECT
		'00' prov_code,
		'全国' prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty)final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
		and shop_id like 'E%'
	GROUP BY
		bd_id,
		bd_name,
		dept_id,
		dept_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name,
		a.dept_id,
		a.dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty)final_qty,
		round( COALESCE(SUM(period_inv_amt)/ SUM(sales_cost),
		0),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
		and shop_id like 'E%'
	GROUP BY
		prov_code,
		prov_name ,
		a.bd_id,
		a.bd_name,
		a.dept_id,
		a.dept_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name,
		'00' dept_id,
		'小计' dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty)final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
		and shop_id like 'E%'
	GROUP BY
		prov_code,
		prov_name,
		a.bd_id,
		a.bd_name
UNION ALL
	SELECT
		prov_code,
		prov_name,
		'00' bd_id,
		'小计' bd_name,
		'00' dept_id,
		'小计' dept_name,
		SUM(COALESCE(a.qty, 0))qty,
		SUM(a.sale) sale,
		SUM(a.profit) profit,
		SUM(a.period_inv_amt) period_inv_amt,
		SUM(a.qm_amt) qm_amt,
		SUM(final_qty)final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),
		2) AS days_turnover,
		COUNT(DISTINCT goodsid )goods_sku,
		COUNT(DISTINCT CASE WHEN (sale)!= 0 THEN goodsid END )sale_sku,
		COUNT(DISTINCT CASE WHEN qm_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN qm_amt<0 THEN qm_amt END ) negative_amt,
		COUNT(DISTINCT CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN goodsid WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND qm_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN qm_amt WHEN (days_turnover>45 AND qm_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN qm_amt END) highet_amt
	FROM
		temp.p_invt_2 a
	WHERE
		a.bd_id = '12'
		and shop_id like 'E%'
	GROUP BY
		prov_code,
		prov_name )a
GROUP BY
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name
ORDER BY
	prov_code,
	bd_id,
	dept_id;


--食百明细
SELECT
	*
FROM
	csx_dw.supply_turnover
WHERE
	bd_id = '11'
		and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
--and  shop_id like 'E%'
;
--生鲜明细
refresh csx_dw.supply_turnover;
SELECT
	*
FROM
	csx_dw.supply_turnover
WHERE
	bd_id = '11' 
		and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
--and  shop_id like 'E%'
;

INVALIDATE METADATA csx_dw.supply_turnover;


SELECT
	prov_code,
	prov_name,
	shop_id,
	shop_name,
	goodsid,
	goodsname,
	standard,
	unit_name,
	brand_name,
	dept_id ,
	dept_name,
	bd_id,
	bd_name,
	div_id,
	div_name,
	catg_l_id,
	catg_l_name,
	catg_m_id,
	catg_m_name,
	catg_s_id,
	catg_s_name,
	valid_tag,
	valid_tag_name,
	goods_status_id,
	goods_status_name,
	sales_qty,
	sales_value,
	profit,
	sales_cost,
	period_inv_qty,
	period_inv_amt,
	final_qty,
	final_amt,
	days_turnover,
	sale_30day,
	qty_30day,
	days_sale,
	max_sale_sdt,
	no_sale_days
FROM
	csx_dw.supply_turnover_province
WHERE
	bd_id = '11'
	and sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')