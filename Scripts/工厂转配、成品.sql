
-- CONNECTION: name=Hadoop - HIVE
-- 日期：20191008
 /* drop
	TABLE
		if exists b2b_tmp.temp_sale;
 INVALIDATE METADATA csx_dw.sale_goods_m;
CREATE
	TEMPORARY TABLE
		b2b_tmp.temp_sale AS SELECT
			case when shopid_orig='W0B6' then '企业购' 
			when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
			else c.sflag
			END qdflag ,
			CASE
				WHEN c.dist IS NOT NULL
				AND c.sflag <> 'M端' THEN SUBSTR(dist ,	1 ,	2)
				WHEN a.cust_id LIKE 'S%'
				AND SUBSTR(	b.prov_name ,1,	2) IN (
					'重庆' ,
					'四川' ,
					'北京' ,
					'福建' ,
					'上海' ,
					'浙江' ,
					'江苏' ,
					'安徽',
					'广东'
				) THEN SUBSTR(b.prov_name ,	1 ,2)
				ELSE SUBSTR(
					d.prov_name ,
					1 ,
					2
				)
			END dist ,
			shopid_orig ,
			goodsid ,
			SUM(profit)profit ,
			SUM(sale_ytd)sale_ytd
		FROM
			(
				SELECT
					shop_id ,
					customer_no  cust_id,
					origin_shop_id  shopid_orig ,
					goods_code goodsid ,
					SUM (
						CASE
							WHEN sdt >= '20190701'and sdt<='20190731' THEN sales_value 
							ELSE 0
						END
					) sale_mtd ,
					SUM(sales_value ) sale_ytd,
					sum(profit )profit
				FROM
					csx_dw.sale_goods_m as sgm 
				WHERE
					sdt >= '20190101' and sdt<='20190731'
					AND shop_id <> 'W098'
					AND sales_type IN(
						'qyg' ,
						'gc'
					)
				GROUP BY
					shop_id ,
					customer_no ,
					origin_shop_id ,
					goods_code 
			) a
	LEFT JOIN (
				SELECT
					shop_id ,
					prov_name
				FROM
					dim.dim_shop
				WHERE
					edate = '9999-12-31'
			) d ON
			a.shop_id = d.shop_id
		LEFT JOIN (
				SELECT
					shop_id ,
					CASE
						WHEN shop_id IN (
							'W055' ,
							'W056'
						) THEN '上海市'
						ELSE prov_name
					END prov_name ,
					CASE
						WHEN prov_name LIKE '%市' THEN prov_name
						ELSE city_name
					END city_name
				FROM
					dim.dim_shop
				WHERE
					edate = '9999-12-31'
			) b ON
			a.cust_id = concat(
				'S' ,
				b.shop_id
			)
		GROUP BY
			case when shopid_orig='W0B6' then 'BBC' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' else c.sflag end ,
			CASE
				WHEN c.dist IS NOT NULL
				AND c.sflag <> 'M端' THEN SUBSTR(
					dist ,
					1 ,
					2
				)
				WHEN a.cust_id LIKE 'S%'
				AND SUBSTR(
					b.prov_name ,
					1 ,
					2
				) IN (
					'重庆' ,
					'四川' ,
					'北京' ,
					'福建' ,
					'上海' ,
					'浙江' ,
					'江苏' ,
					'安徽',
					'广东'
				) THEN SUBSTR(
					b.prov_name ,
					1 ,
					2
				)
				ELSE SUBSTR(
					d.prov_name ,
					1 ,
					2
				)
			END ,
			shopid_orig ,
			goodsid ; 
*/

-- 商超的成品/转配
 SELECT
	a.channel_name ,
	province_name ,
	CASE
		WHEN b.mat_type = '成品'
		AND a.goodsid NOT IN ( '5990' ,
		'877589' ) THEN '成品'
		ELSE '转配'
END mat_type ,
	COUNT(DISTINCT a.goodsid)sku ,
	SUM(profit) profit ,
	SUM(sales) sales
FROM
	(
	SELECT
		channel ,
		channel_name,
		sgm2.province_name ,
		sgm2.goods_code goodsid ,
		sgm2.sap_origin_dc_code origin_shop_id ,
		sum(sales_value) sales,
		sum(sgm2.profit) profit
	FROM
		csx_dw.customer_sale_m as sgm2
	WHERE
		sdt >= '20190101'
		and sdt <= '20191130'
		and channel_name like '商超%'
	group by
		channel ,
		channel_name ,
		goods_code ,
		sap_origin_dc_code ,
		province_name ) a
LEFT JOIN 
	(
	select
		DISTINCT shop_id,
		goodsid ,
		mat_type
	from
		(
		SELECT
			DISTINCT shop_id,
			goodsid,
			mat_type
		FROM
			csx_ods.marc_ecc where shop_id not in (select factory_location_code from csx_dw.factory_bom where sdt='current')
	union all
		select
			DISTINCT a.factory_location_code shop_id,
			a.goods_code as goodsid,
			'成品' mat_type
		from
			csx_dw.factory_bom as a
		WHERE
			a.sdt = 'current' ) b) b ON
	a.origin_shop_id = b.shop_id
	AND a.goodsid = b.goodsid
GROUP BY
	a.channel_name ,
	province_name ,
	CASE
		WHEN b.mat_type = '成品'
		AND a.goodsid NOT IN ( '5990' ,	'877589' ) THEN '成品'
		ELSE '转配'
END
UNION ALL
SELect
	channel_name ,
	province_name ,
	CASE
		WHEN b.goodsid IS NOT NULL THEN '成品'
		ELSE '采购'
END mat_type ,
	COUNT(DISTINCT a.goodsid)sku ,
	SUM(profit) profit ,
	SUM(sales) sales
FROM
	(
	SELECT
		channel ,
		channel_name,
		sgm2.province_name ,
		sgm2.goods_code goodsid ,
		sap_origin_dc_code origin_shop_id ,
		sum(sales_value) sales,
		sum(sgm2.profit) profit
	FROM
		csx_dw.customer_sale_m as sgm2
	WHERE
		sdt >= '20190101'
		and sdt <= '20191031'
		and channel_name not like '商超%'
	group by
		channel ,
		channel_name ,
		goods_code ,
		sap_origin_dc_code ,
		province_name ) a
LEFT JOIN (
	select
		DISTINCT shop_id,
		goodsid ,
		mat_type
	from
		(
		SELECT
			DISTINCT shop_id,
			goodsid,
			mat_type
		FROM
			csx_ods.marc_ecc where shop_id not in (select factory_location_code from csx_dw.factory_bom where sdt='current')
	union all
		select
			DISTINCT a.factory_location_code shop_id,
			a.goods_code as goodsid,
			'成品' mat_type
		from
			csx_dw.factory_bom as a
		WHERE
			a.sdt = 'current' ) b) b ON
	a.goodsid = b.goodsid
GROUP BY
	channel_name ,
	province_name ,
	CASE
		WHEN b.goodsid IS NOT NULL THEN '成品'
		ELSE '采购'
END ;



-- 无省区
 SELECT
	a.channel ,
	CASE
		WHEN b.mat_type in('成品')
		AND a.goodsid NOT IN ('5990' ,
		'877589') THEN '成品'
		ELSE '转配'
END mat_type ,
	COUNT(DISTINCT a.goodsid)sku ,
	SUM(profit)/10000 profit ,
	SUM(sales)/10000 sales
FROM
	(
	SELECT
		case
			when (channel_name like '商超%'
			OR channel_name = '其它') then '商超'
	END channel,
		sgm2.province_name ,
		sgm2.goods_code goodsid ,
		sap_origin_dc_code origin_shop_id ,
		sum(sales_value) sales,
		sum(sgm2.profit) profit
	FROM
		csx_dw.customer_sale_m as sgm2
	WHERE
		sdt >= '20190101'
		and sdt <= '20191222'
		and channel_name like '商超%'
		and sales_type in ('qyg','gc','anhui','sc') 
	group by
		case
			when (channel_name like '商超%'
			OR channel_name = '其它' )then '商超'
	END ,
		goods_code ,
		sap_origin_dc_code ,
		province_name ) a
LEFT JOIN (
	select
		DISTINCT shop_id,
		goodsid ,
		mat_type
	from
		(
		SELECT
			DISTINCT shop_id,
			goodsid,
			mat_type
		FROM
			csx_ods.marc_ecc a
			where 
			    a.shop_id not in (select DISTINCT factory_location_code from csx_dw.factory_bom where sdt='current')
	union all
		select
			DISTINCT a.factory_location_code shop_id,
			a.goods_code as goodsid,
			'成品' mat_type
		from
			csx_dw.factory_bom as a
		WHERE
			a.sdt = 'current' ) b) b ON
	a.origin_shop_id = b.shop_id
	AND a.goodsid = b.goodsid
GROUP BY
	a.channel ,
	CASE
		WHEN b.mat_type in('成品')
		AND a.goodsid NOT IN ('5990' ,
		'877589') THEN '成品'
		ELSE '转配'
END
UNION ALL
SELECT 
	channel ,
	CASE
		WHEN b.goodsid is not null THEN '成品'
		ELSE '采购'
END mat_type ,
	COUNT(DISTINCT a.goodsid)sku ,
	SUM(profit)/10000 profit ,
	SUM(sales)/10000 sales
FROM
	(
	SELECT
			case
			when channel in('1','7') then '大客户'
			WHEN province_name in ('大客户平台','商超平台') then province_name 	else channel_name
	END 
		 channel,
		sgm2.province_name ,
		sgm2.goods_code goodsid ,
		sap_origin_dc_code origin_shop_id ,
		sum(sales_value) sales,
		sum(sgm2.profit) profit ,
		dc_code shop_id
	FROM
		csx_dw.customer_sale_m as sgm2
	WHERE
		sdt >= '20190101'
		and sdt <= '20191222'
		and channel_name not like '商超%'
		and sales_type in ('qyg','gc','anhui','sc') 
	group by
		case
			when channel in('1','7')  then '大客户'
			WHEN province_name in ('大客户平台','商超平台') then province_name 	else channel_name
	END ,
		goods_code ,
		province_name,
		dc_code ,
		sap_origin_dc_code ) a
LEFT JOIN (
	select
		DISTINCT goodsid
	from
		(
		SELECT
			DISTINCT goodsid
		FROM
			csx_ods.marc_ecc a
		WHERE
			mat_type = '成品' 
			AND a.shop_id not in (select DISTINCT factory_location_code from csx_dw.factory_bom where sdt='current')
			AND goodsid NOT IN ('5990' ,
			'877589')
	union all
		select
			DISTINCT a.goods_code goodsid
		from
			csx_dw.factory_bom as a
		WHERE
			a.sdt = 'current'
			and a.product_code != '5990' ) b) b ON
	a.goodsid = b.goodsid
GROUP BY
	channel ,
	CASE
		WHEN b.goodsid is not null THEN '成品'
		ELSE '采购'
END ;
--select * from csx_ods.marc_ecc as me where shop_id 
--select sdt,channel_name ,province_name ,customer_no ,customer_name ,sum(sales_value )from csx_dw.sale_goods_m as sgm where province_name is null group by province_name ,customer_no ,customer_name ,channel_name ,sdt
 sale_warzone01_detail_dtl
--
--  select
-- 	*
-- from
-- 	csx_dw.customer_m
-- where
-- 	sdt = '20191007'
-- 	and customer_no = '103748'
-- INVALIDATE METADATA csx_dw.sale_goods_m ;
 select
	*
from
	csx_dw.factory_bom
where
	sdt = 'current'
	and goods_type_name = '分解型';

-- 省区销售
 SELECT
	a.channel ,
	province_code ,province_name ,
	CASE
		WHEN b.mat_type in('成品')
		AND a.goodsid NOT IN ('5990' ,
		'877589') THEN '成品'
		ELSE '转配'
END mat_type ,
	COUNT(DISTINCT a.goodsid)sku ,
	SUM(profit)/10000 profit ,
	SUM(sales)/10000 sales
FROM
	(
	SELECT
		case
			when channel_name like '商超%'
			OR channel_name = '其它' then '商超'
	END channel,
		sgm2.province_name ,province_code ,
		sgm2.goods_code goodsid ,
		sap_origin_dc_code origin_shop_id ,
		sum(sales_value) sales,
		sum(sgm2.profit) profit
	FROM
		csx_dw.customer_sale_m as sgm2
	WHERE
		sdt >= '20190101'
		and sdt <= '20191031'
		and channel_name like '商超%'
		and sales_type in ('qyg','gc','anhui','sc') 
	group by
		case
			when channel_name like '商超%'
			OR channel_name = '其它' then '商超'
	END ,
		goods_code ,
		sap_origin_dc_code ,province_code,
		province_name ) a
LEFT JOIN (
	select
		DISTINCT shop_id,
		goodsid ,
		mat_type
	from
		(
		SELECT
			DISTINCT shop_id,
			goodsid,
			mat_type
		FROM
			csx_ods.marc_ecc where shop_id not in ('W0A3','W080','W082','W048')
	union all
		select
			DISTINCT a.factory_location_code shop_id,
			a.goods_code as goodsid,
			'成品' mat_type
		from
			csx_dw.factory_bom as a
		WHERE
			a.sdt = 'current' ) b) b ON
	a.origin_shop_id = b.shop_id
	AND a.goodsid = b.goodsid
GROUP BY
	a.channel ,
	CASE
		WHEN b.mat_type in('成品')
		AND a.goodsid NOT IN ('5990' ,
		'877589') THEN '成品'
		ELSE '转配'
END,province_code ,province_name 
UNION ALL
SELECT 
	channel ,province_code ,province_name ,
	CASE
		WHEN b.goodsid is not null THEN '成品'
		ELSE '采购'
END mat_type ,
	COUNT(DISTINCT a.goodsid)sku ,
	SUM(profit)/10000 profit ,
	SUM(sales)/10000 sales
FROM
	(
	SELECT
			case
			when channel_name  like '企业购%' then '大客户'
			WHEN province_name in ('大客户平台','商超平台') then province_name 	else channel_name
	END 
		 channel,
		sgm2.province_name ,province_code ,
		sgm2.goods_code goodsid ,
		sap_origin_dc_code origin_shop_id ,
		sum(sales_value) sales,
		sum(sgm2.profit) profit ,
		dc_code shop_id
	FROM
		csx_dw.customer_sale_m as sgm2
	WHERE
		sdt >= '20190101'
		and sdt <= '20191031'
		and channel_name not like '商超%'
		and sales_type in ('qyg','gc','anhui','sc') 
	group by
		case
			when channel_name  like '企业购%' then '大客户'
			WHEN province_name in ('大客户平台','商超平台') then province_name 	else channel_name
	END ,
		goods_code ,
		province_name,
		dc_code ,
		sap_origin_dc_code,province_code ) a
LEFT JOIN (
	select
		DISTINCT goodsid
	from
		(
		SELECT
			DISTINCT goodsid
		FROM
			csx_ods.marc_ecc
		WHERE
			mat_type = '成品' AND shop_id not in ('W0A3','W080','W082','W048')
			AND goodsid NOT IN ('5990' ,
			'877589')
	union all
		select
			DISTINCT a.goods_code goodsid
		from
			csx_dw.factory_bom as a
		WHERE
			a.sdt = 'current'
			and a.product_code != '5990' ) b) b ON
	a.goodsid = b.goodsid
GROUP BY
	channel ,province_code ,province_name ,
	CASE
		WHEN b.goodsid is not null THEN '成品'
		ELSE '采购'
END ;


-- 省区销售每日销售
 SELECT sdt,
	a.channel ,
	province_code ,province_name ,
	CASE
		WHEN b.mat_type in('成品')
		AND a.goodsid NOT IN ('5990' ,
		'877589') THEN '成品'
		ELSE '转配'
END mat_type ,
	COUNT(DISTINCT a.goodsid)sku ,
	SUM(profit)/10000 profit ,
	SUM(sales)/10000 sales
FROM
	(
	SELECT sdt,
		case
			when channel_name like '商超%'
			OR channel_name = '其它' then '商超'
	END channel,
		sgm2.province_name ,province_code ,
		sgm2.goods_code goodsid ,
		sap_origin_dc_code origin_shop_id ,
		sum(sales_value) sales,
		sum(sgm2.profit) profit
	FROM
		csx_dw.customer_sale_m as sgm2
	WHERE
		sdt >= '20190101'
		and sdt <= '20191222'
		and channel_name like '商超%'
		and sales_type in ('qyg','gc','anhui','sc') 
	group by
		case
			when channel_name like '商超%'
			OR channel_name = '其它' then '商超'
	END ,sdt,
		goods_code ,
		sap_origin_dc_code ,province_code,
		province_name ) a
LEFT JOIN (
	select
		DISTINCT shop_id,
		goodsid ,
		mat_type
	from
		(
		SELECT
			DISTINCT shop_id,
			goodsid,
			mat_type
		FROM
			csx_ods.marc_ecc where shop_id not in ('W0A3','W080','W082','W048')
	union all
		select
			DISTINCT a.factory_location_code shop_id,
			a.goods_code as goodsid,
			'成品' mat_type
		from
			csx_dw.factory_bom as a
		WHERE
			a.sdt = 'current' ) b) b ON
	a.origin_shop_id = b.shop_id
	AND a.goodsid = b.goodsid
GROUP BY
	a.channel ,sdt,
	CASE
		WHEN b.mat_type in('成品')
		AND a.goodsid NOT IN ('5990' ,
		'877589') THEN '成品'
		ELSE '转配'
END,province_code ,province_name 
UNION ALL
SELECT 
	sdt,channel ,province_code ,province_name ,
	CASE
		WHEN b.goodsid is not null THEN '成品'
		ELSE '采购'
END mat_type ,
	COUNT(DISTINCT a.goodsid)sku ,
	SUM(profit)/10000 profit ,
	SUM(sales)/10000 sales
FROM
	(
	SELECT
			case
			when channel_name  like '企业购%' then '大客户'
			WHEN province_name in ('大客户平台','商超平台') then province_name 	else channel_name
	END 
		 channel,
		 sdt,
		sgm2.province_name ,province_code ,
		sgm2.goods_code goodsid ,
		sap_origin_dc_code origin_shop_id ,
		sum(sales_value) sales,
		sum(sgm2.profit) profit ,
		dc_code shop_id
	FROM
		csx_dw.customer_sale_m as sgm2
	WHERE
		sdt >= '20190101'
		and sdt <= '20191031'
		and channel_name not like '商超%'
		and sales_type in ('qyg','gc','anhui','sc') 
	group by
		case
			when channel_name  like '企业购%' then '大客户'
			WHEN province_name in ('大客户平台','商超平台') then province_name 	else channel_name
	END ,
		goods_code ,sdt,
		province_name,
		dc_code ,
		sap_origin_dc_code,province_code ) a
LEFT JOIN (
	select
		DISTINCT goodsid
	from
		(
		SELECT
			DISTINCT goodsid
		FROM
			csx_ods.marc_ecc
		WHERE
			mat_type = '成品' AND shop_id not in ('W0A3','W080','W082','W048')
			AND goodsid NOT IN ('5990' ,
			'877589')
	union all
		select
			DISTINCT a.goods_code goodsid
		from
			csx_dw.factory_bom as a
		WHERE
			a.sdt = 'current'
			and a.product_code != '5990' ) b) b ON
	a.goodsid = b.goodsid
GROUP BY
	channel ,province_code ,province_name ,sdt,
	CASE
		WHEN b.goodsid is not null THEN '成品'
		ELSE '采购'
END ;
