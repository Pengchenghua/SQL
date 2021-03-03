SELECT
	*
FROM
	csx_dw.order_flow;

select
	a.*
from
	(
	select
		distinct *
	from
		csx_ods.wms_accounting_stock_detail_view_ods
	where
		sdt = regexp_replace(date_sub(current_date(),
		1),
		'-',
		'') ) a
join (
	select
		max(id) as max_id,
		product_code ,
		location_code ,
		reservoir_area_code
	from
		csx_ods.wms_accounting_stock_detail_view_ods
	where
		sdt = regexp_replace(date_sub(current_date(),
		1),
		'-',
		'')
		and regexp_replace(to_date(biz_time),
		'-',
		'')< '20191119'
		--	and purchase_org_code ='A10'

		group by product_code ,
		location_code,
		reservoir_area_code ) b on
	a.id = b.max_id ;

show CREATE TABLE dim.dim_shop_goods_latest ;

--110406
--110408类别
--全国所有省区，19年按省区、按月、单品、销售量、销售额 
SELECT 
--mon,
zone_id,
zone_name,sales_dist,sales_dist_name,
	b.prov_code,
	b.prov_name,
	a.shop_id,
	b.shop_name,
 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
	a.vendor_id,
	b.vendor_name,
	b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name,
	sum(qty)qty,
	sum(sales_cost)sales_cost,
	sum(sale)sale,
	sum(profit)profit
FROM
	(
	SELECT
		substr(sdt,1,6)mon,
		shop_id,
		a.goodsid,
		a.vendor_id,
		sum(a.sales_qty)qty,
		sum(cost_amt) sales_cost,
		sum(a.tax_value + a.sales_val - a.subtatal_5) sale,
		sum(a.tax_value + a.sales_val - a.subtatal_5 - a.cost_amt + a.pro_chg_amt) profit
	FROM
		dw.sale_sap_dtl_fct a
	WHERE
		a.bill_type IN ('',
		'S1',
		'S2',
		'ZF1',
		'ZF2',
		'ZR1',
		'ZR2',
		'ZFP',
		'ZFP1')
		AND sdt <= '20191231'
		AND a.sdt >= '20190101'
		--AND a.div_id IN ('12')
		and catg_l_id in ('1406')
	GROUP BY
		shop_id,
		a.goodsid,
		a.vendor_id,substr(sdt,1,6))a
JOIN dim.dim_shop_goods_latest b ON
	a.shop_id = b.shop_id
	AND b.zone_id = '1'
	AND a.goodsid = b.goodsid
group by
	b.prov_code,
	b.prov_name,
	a.shop_id,
	b.shop_name,
 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
	a.vendor_id,
	b.vendor_name,
	b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name,
--mon,
zone_id,
zone_name,
sales_dist,sales_dist_name;
	

SELECT * from dim.dim_shop_new_goods  where shop_id ='W0A2' and goodsid ='1132351';

select regexp_replace('E0M3' ,'(^[A-Z]*)','') ;
SELECT * from dim.dim_goods_latest where goodsid ='1038739';

select * from csx_dw.shop_m where sdt='current' and sales_dist_name like '彩食鲜%';



--全国所有省区，19年按省区、按月、单品、销售量、销售额 
set APPX_COUNT_DISTINCT=true;
SELECT --mon,
--zone_id,
--zone_name,
sales_dist,sales_dist_name,
	b.prov_code,
	b.prov_name,
	--a.shop_id,
	--b.shop_name,
 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
	--a.vendor_id,
	--b.vendor_name,
	b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name,
	sum(qty)qty,
	sum(sales_cost)sales_cost,
	sum(sale)sale,
	sum(profit)profit,
	count(distinct a.shop_id)sale_shop,
	count(DISTINCT b.shop_id ) all_shop
FROM
	(
	SELECT
		substr(sdt,1,6)mon,
		shop_id,
		a.goodsid,
		a.vendor_id,
		sum(a.sales_qty)qty,
		sum(cost_amt) sales_cost,
		sum(a.tax_value + a.sales_val - a.subtatal_5) sale,
		sum(a.tax_value + a.sales_val - a.subtatal_5 - a.cost_amt + a.pro_chg_amt) profit
	FROM
		dw.sale_sap_dtl_fct a
	WHERE
		a.bill_type IN ('',
		'S1',
		'S2',
		'ZF1',
		'ZF2',
		'ZR1',
		'ZR2',
		'ZFP',
		'ZFP1')
		AND sdt >= '20190101'
		AND a.sdt <= '20191231'
		--AND a.shop_id IN ('W0A8')
		--and catg_m_id in ('123202')
		--AND goodsid ='0002516'
		and sales_dist not like '6%'
	GROUP BY
		shop_id,
		a.goodsid,
		a.vendor_id,substr(sdt,1,6))a
JOIN   dim.dim_shop_goods_latest b ON
	a.shop_id = b.shop_id
	--AND b.zone_id = '3'
	AND a.goodsid = b.goodsid
	and b.brand='0002516'
group by
	b.prov_code,
	b.prov_name,
	--a.shop_id,
	--b.shop_name,
 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
	--a.vendor_id,
	--b.vendor_name,
	b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name,
--mon,
--zone_id,
--zone_name,
sales_dist,
sales_dist_name
;
-- 全国查询
SELECT --mon,
--zone_id,
--zone_name,
--sales_dist,sales_dist_name,
--	b.prov_code,
--	b.prov_name,
	--a.shop_id,
	--b.shop_name,
	 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
	--a.vendor_id,
	--b.vendor_name,
--	b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name,
	sum(qty)qty,
	sum(sales_cost)sales_cost,
	sum(sale)sale,
	sum(profit)profit,
	count(distinct a.shop_id)sale_shop,
	count(DISTINCT b.shop_id ) all_shop
FROM
	(
	SELECT
		substr(sdt,1,6)mon,
		shop_id,
		a.goodsid,
		a.vendor_id,
		sum(a.sales_qty)qty,
		sum(cost_amt) sales_cost,
		sum(a.tax_value + a.sales_val - a.subtatal_5) sale,
		sum(a.tax_value + a.sales_val - a.subtatal_5 - a.cost_amt + a.pro_chg_amt) profit
	FROM
		dw.sale_sap_dtl_fct a
	WHERE
		a.bill_type IN ('',
		'S1',
		'S2',
		'ZF1',
		'ZF2',
		'ZR1',
		'ZR2',
		'ZFP',
		'ZFP1')
		AND sdt >= '20190101'
		AND a.sdt <= '20191231'
		--AND a.shop_id IN ('W0A8')
		and catg_l_id BETWEEN '1401' and '1499'
		--AND goodsid ='0002516'
		and sales_dist not like '6%'
	GROUP BY
		shop_id,
		a.goodsid,
		a.vendor_id,substr(sdt,1,6))a
JOIN   dim.dim_shop_goods_latest b ON
	a.shop_id = b.shop_id
	--AND b.zone_id = '3'
	AND a.goodsid = b.goodsid
	--and b.brand='0002516'
group by
--	b.prov_code,
--	b.prov_name,
	--a.shop_id,
	--b.shop_name,
 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
	--a.vendor_id,
	--b.vendor_name,
--	b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name
--mon,
--zone_id,
--zone_name,
--sales_dist,
--sales_dist_name
;

-- 每月销售明细 省区
SELECT
mon,
	b.prov_code,
	b.prov_name,
	--a.shop_id,
	--b.shop_name,
 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
	a.vendor_id,
	b.vendor_name,
	b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name,
	sum(qty)qty,
	sum(sales_cost)sales_cost,
	sum(sale)sale,
	sum(profit)profit
FROM
	(
	SELECT
	SUBSTRING(sdt,1,6) mon,
		shop_id,
		a.goodsid,
		a.vendor_id,
		sum(a.sales_qty)qty,
		sum(cost_amt) sales_cost,
		sum(a.tax_value + a.sales_val - a.subtatal_5) sale,
		sum(a.tax_value + a.sales_val - a.subtatal_5 - a.cost_amt + a.pro_chg_amt) profit
	FROM
		dw.sale_sap_dtl_fct a
	WHERE
		a.bill_type IN ('',
		'S1',
		'S2',
		'ZF1',
		'ZF2',
		'ZR1',
		'ZR2',
		'ZFP',
		'ZFP1')
		AND sdt <= '20190319'
		AND a.sdt >= '20190101'
		AND a.catg_s_id IN ('12400101',
'12400102',
'12400103',
'12400104',
'12400401',
'12400402',
'12400501',
'12410501',
'12410502',
'12410504',
'12410506',
'12410603',
'12410607',
'12410608',
'12410701',
'12410702',
'12410902',
'12410904',
'12440101',
'12440102',
'12440103',
'12440104',
'12440105',
'12440106',
'12440202',
'12440301',
'12440302',
'12440404',
'12440501',
'12440502')
	GROUP BY
		shop_id,
		a.goodsid,
		a.vendor_id,SUBSTRING(sdt,1,6))a
JOIN dim.dim_shop_goods_latest b ON
	a.shop_id = b.shop_id
	AND b.zone_id is not null 	AND a.goodsid = b.goodsid
group by
mon,
	b.prov_code,
	b.prov_name,
	--a.shop_id,
	--b.shop_name,
 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
	a.vendor_id,
	b.vendor_name,
	b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name;
--select * from dim.dim_goods_latest where goodsname LIKE '老才臣%'
select * from dim.dim_goods_latest;