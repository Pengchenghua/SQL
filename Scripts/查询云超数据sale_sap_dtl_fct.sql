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

SELECT
	b.prov_code,
	b.prov_name,
	--a.shop_id,
	--b.shop_name,
 a.goodsid,
	b.goodsname,
	b.bar_code,
	b.unit,
	b.brand_name,
--	a.vendor_id,
--	b.vendor_name,
	--b.prod_area,
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
		AND sdt < '20200601'
		AND a.sdt >= '20200101'
	--	AND a.catg_l_id IN ('1013')
	GROUP BY
		shop_id,
		a.goodsid,
		a.vendor_id)a
JOIN 
(select * from dim.dim_shop_goods_latest where zone_id IN('1','2','3','4','5','6','7') and goodsid in ('128','223','197','198','5283'))b ON
	a.shop_id = b.shop_id
	--AND b.zone_id = '3'
	AND a.goodsid = b.goodsid
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
--b.prod_area,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	b.catg_m_id,
	b.catg_m_name,
	b.catg_s_id,
	b.catg_s_name;
	
	
SELECT
    b.prov_code,
    b.prov_name,
    --a.shop_id,
    --b.shop_name,
-- a.goodsid,
--    b.goodsname,
--    b.bar_code,
--    b.unit,
--    b.brand_name,
--  a.vendor_id,
--  b.vendor_name,
    --b.prod_area,
  b.div_id,
  b.div_name,
--    b.catg_l_id,
--    b.catg_l_name,
--    b.catg_m_id,
--    b.catg_m_name,
--    b.catg_s_id,
--    b.catg_s_name,
    sum(qty)qty,
    sum(sales_cost)sales_cost,
    sum(sale)sale,
    sum(profit)profit
FROM
    (
    SELECT
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
        a.bill_type IN ('','S1', 'S2','ZF1','ZF2','ZR1','ZR2','ZFP','ZFP1')
        AND sdt <= '20200430'
        AND a.sdt >= '20200401'
        AND a.div_id IN ('11','10')
    GROUP BY
        shop_id,
        a.goodsid,
        a.vendor_id)a
JOIN 
(select * from dim.dim_shop_goods_latest where sales_dist ='250000') b  ON
    a.shop_id = b.shop_id
    --AND b.zone_id = '3'
    AND a.goodsid = b.goodsid
group by
    b.prov_code,
    b.prov_name,
    --a.shop_id,
    --b.shop_name,
-- a.goodsid,
--    b.goodsname,
--    b.bar_code,
--    b.unit,
--    b.brand_name,
    --a.vendor_id,
    --b.vendor_name,
--b.prod_area,
 b.div_id,
  b.div_name
--    b.catg_l_id,
--    b.catg_l_name,
--    b.catg_m_id,
--    b.catg_m_name,
--    b.catg_s_id,
--    b.catg_s_name
;	
	
	
	select * from dim.dim_catg_latest ;
	select * from csx_dw.goods_m where sdt='20200520' and goods_name  like '%饺子' and division_code in ('11','10');
	select * from dim.dim_goods where edate ='9999-12-31' and goodsid ='P137104';
	
	select DISTINCT sales_dist,sales_dist_name from dim.dim_shop_goods_latest 
	
	;
    SELECT
    SUBSTRING(sdt,1,6)as mon, 
        b.sales_dist ,sales_dist_new_name,
        category_large_code ,c.category_large_name ,c.category_middle_code ,c.category_middle_name, c.category_small_code ,c.category_small_name ,
--        a.vendor_id,
        sum(a.sales_qty)qty,
        sum(cost_amt) sales_cost,
        sum(a.tax_value + a.sales_val - a.subtatal_5) sale,
        sum(a.tax_value + a.sales_val - a.subtatal_5 - a.cost_amt + a.pro_chg_amt) profit
    FROM
        dw.sale_sap_dtl_fct a
        join 
       ( select sales_dist ,sales_dist_new_name ,shop_id from dim.dim_shop where edate ='9999-12-31' and sales_dist_new in('120000','110000') and shop_type ='1'
)b on a.shop_id=b.shop_id 
join 
(select m.category_large_code ,m.category_large_name ,m.category_middle_code ,m.category_middle_name, m.category_small_code ,m.category_small_name 
    from csx_dw.dws_basic_w_a_category_m as m where sdt='current') c on a.catg_s_id =c.category_small_code
    and 
        a.bill_type IN ('',
        'S1',
        'S2',
        'ZF1',
        'ZF2',
        'ZR1',
        'ZR2',
        'ZFP',
        'ZFP1')
        AND sdt <= '20200625'
        AND a.sdt >= '20200101'
       -- AND a.div_id IN ('11','10')
       -- and shop_id in  ()

    GROUP BY
    SUBSTRING(sdt,1,6),
        b.sales_dist ,sales_dist_new_name,
        category_large_code ,c.category_large_name ,c.category_middle_code ,c.category_middle_name, c.category_small_code ,c.category_small_name ;
--        a.goodsid,
--        a.vendor_id

