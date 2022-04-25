-- 汇总数据
SELECT
	
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
LEFT JOIN ( SELECT DISTINCT shop_id,
                    goodsid,
                    mat_type
   FROM
     ( SELECT DISTINCT shop_id,
                       goodsid,
                       mat_type
      FROM csx_ods.marc_ecc a
      WHERE a.shop_id NOT IN
          (SELECT DISTINCT factory_location_code
           FROM csx_dw.factory_bom
           WHERE sdt='current')
      UNION ALL SELECT DISTINCT a.location_code shop_id,
                                a.product_code AS goodsid,
                                '成品' mat_type
      FROM csx_ods.factory_task_order_ods AS a
      WHERE a.sdt >= '20190901'
        AND sdt<='20191222') b) b ON
	a.origin_shop_id = b.shop_id
	AND a.goodsid = b.goodsid
GROUP BY

	CASE
		WHEN b.mat_type in('成品')
		AND a.goodsid NOT IN ('5990' ,
		'877589') THEN '成品'
		ELSE '转配'
END
UNION ALL
SELECT 

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
LEFT JOIN ( SELECT DISTINCT goodsid
   FROM
     ( SELECT DISTINCT goodsid
      FROM csx_ods.marc_ecc a
      WHERE mat_type = '成品'
        AND a.shop_id NOT IN
          (SELECT DISTINCT factory_location_code
           FROM csx_dw.factory_bom
           WHERE sdt='current')
        AND goodsid NOT IN ('5990',
                            '877589')
      UNION ALL 
      SELECT DISTINCT        a.product_code AS goodsid
      FROM csx_ods.factory_task_order_ods AS a
      WHERE a.sdt >= '20190901'
        AND sdt<='20191222') b) b ON
	a.goodsid = b.goodsid
GROUP BY
	CASE
		WHEN b.goodsid is not null THEN '成品'
		ELSE '采购'
END ;


--增加省区
CREATE
TEMPORARY TABLE csx_dw.temp_sku_01 AS
SELECT palce,
       dc_code,
       dc_name,
       goods_code,
       qty,
       amt,
       if(b.goodsid IS NULL,
                       '否',
                       '是') AS TYPE
FROM
  ( SELECT CASE
               WHEN shop_belong_desc = '彩食鲜工厂' THEN '彩食鲜工厂'
               ELSE '物流'
           END palce,
           dc_code,
           dc_name,
           goods_code,
           sum(qty)qty,
           sum(amt)amt
   FROM csx_dw.wms_accounting_stock_m AS a
   JOIN
     ( SELECT *
      FROM csx_dw.shop_m
      WHERE sdt = 'current' ) b ON regexp_replace(a.dc_code,'^E','9')= b.shop_id
   AND a.sdt = '20191222'
   AND qty != 0
   GROUP BY CASE
                WHEN shop_belong_desc = '彩食鲜工厂' THEN '彩食鲜工厂'
                ELSE '物流'
            END,
            goods_code,
            dc_code,
            dc_name) AS a
LEFT JOIN
  ( SELECT DISTINCT goodsid,
                    mat_type
   FROM
     (SELECT DISTINCT shop_id,
                      goodsid,
                      mat_type
      FROM csx_ods.marc_ecc a
      WHERE a.shop_id NOT IN
          (SELECT DISTINCT factory_location_code
           FROM csx_dw.factory_bom
           WHERE sdt='current')
        AND mat_type='成品'
      UNION ALL SELECT DISTINCT a.location_code shop_id,
                                a.product_code AS goodsid,
                                '成品' mat_type
      FROM csx_ods.factory_task_order_ods AS a
      WHERE a.sdt >= '20191122'
        AND sdt<='20191222') b) b ON a.goods_code=b.goodsid ;


CREATE
TEMPORARY TABLE csx_dw.temp_sku_02 AS
SELECT dc_code,
       dc_name,
       goods_code,
       sales_qty,
       sales_value,
       if(b.goodsid IS NULL,
                       '否',
                       '是') AS TYPE
FROM
  ( SELECT 
             shop_id as  dc_code,
           shop_name  as dc_name,
           goods_code,
           sum(sales_qty)sales_qty,
           sum(sales_value)sales_value
   FROM csx_dw.sale_goods_m1 AS a
   where a.sdt <= '20191222' and a.sdt>='20191122'
  -- AND qty != 0
   GROUP BY 
            goods_code,
            shop_id,
            shop_name) AS a
LEFT JOIN
  ( SELECT DISTINCT goodsid,
                    mat_type
   FROM
     (SELECT DISTINCT shop_id,
                      goodsid,
                      mat_type
      FROM csx_ods.marc_ecc a
      WHERE a.shop_id NOT IN
          (SELECT DISTINCT factory_location_code
           FROM csx_dw.factory_bom
           WHERE sdt='current')
        AND mat_type='成品'
      UNION ALL SELECT DISTINCT a.location_code shop_id,
                                a.product_code AS goodsid,
                                '成品' mat_type
      FROM csx_ods.factory_task_order_ods AS a
      WHERE a.sdt >= '20191122'
        AND sdt<='20191222') b) b ON a.goods_code=b.goodsid ;
        
        
--工厂调拨

SELECT  palce,province_code,province_name,
               count(DISTINCT goods_code) as sku,
               count(DISTINCT case when qty!=0 then goods_code end )inv_sku,
               count(distinct case when sales_value!=0 then goods_code end )sale_sku
               
FROM
  (SELECT palce,
          dc_code,
          goods_code,
          sum(qty)qty,
          0 sales_value
   FROM csx_dw.temp_sku_01
   WHERE palce='彩食鲜工厂'
   GROUP BY 
   palce,
          dc_code,goods_code
   UNION ALL 
   SELECT DISTINCT '彩食鲜工厂' palce,
                                     location_code AS dc_code,
                                     product_code AS goods_code,
                                     0 qty,
                                     0 sales_value
   FROM csx_dw.factory_out_rate
   WHERE sdt >='20191122'
     AND sdt<='20191222'
     union all 
     select '彩食鲜工厂'palce,dc_code,goods_code,0 qty,
     sum(sales_value)sales_value from  csx_dw.temp_sku_02 where type='是' group by dc_code,goods_code)a
     join 
     ( SELECT province_code,province_name,shop_id
      FROM csx_dw.shop_m
      WHERE sdt = 'current' ) b ON regexp_replace(a.dc_code,'^E','9')= b.shop_id
     GROUP BY palce,province_code,province_name
     ;




SELECT  palce,province_code,province_name,
               count(DISTINCT goods_code) as sku,
               count(DISTINCT case when qty!=0 then goods_code end )inv_sku,
               count(distinct case when sales_value!=0 then goods_code end )sale_sku
               
FROM
  (SELECT palce,
          dc_code,
          goods_code,
          sum(qty)qty,
          0 sales_value
   FROM csx_dw.temp_sku_01
   WHERE palce='物流'
   GROUP BY 
   palce,
          dc_code,goods_code
     union all 
     select '物流'palce,dc_code,goods_code,0 qty,
     sum(sales_value)sales_value from  csx_dw.temp_sku_02 where type='否' group by dc_code,goods_code)a
     join
     ( SELECT *
      FROM csx_dw.shop_m
      WHERE sdt = 'current' ) b ON regexp_replace(a.dc_code,'^E','9')= b.shop_id
     GROUP BY palce,province_code,province_name
     ;

SELECT  palce,
               count(DISTINCT goods_code) as sku
FROM
  (SELECT palce,
          dc_code,
          goods_code
   FROM csx_dw.temp_sku_01
   WHERE palce='彩食鲜工厂'
   UNION ALL 
   SELECT DISTINCT '彩食鲜工厂' palce,
                                     location_code AS dc_code,
                                     product_code AS goods_code
   FROM csx_dw.factory_out_rate
   WHERE sdt >='20191122'
     AND sdt<='20191222')a
     
     GROUP BY palce
     ;