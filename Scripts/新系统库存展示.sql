--获取库存

DROP TABLE IF EXISTS csx_ods.wms_h_stock_temp01 ;


CREATE
TEMPORARY TABLE csx_ods.wms_h_stock_temp01 AS
SELECT a.*,
       b.edate
FROM
  (SELECT *
   FROM csx_ods.wms_accounting_stock_detail_view_ods
   WHERE sdt = regexp_replace(date_sub(CURRENT_DATE, 1), '-','') ) a
JOIN
  (SELECT max(id) AS max_id,
          product_code,
          location_code,
          reservoir_area_code,
          regexp_replace(to_date(date_sub(current_date(),1)), '-','') AS edate
   FROM csx_ods.wms_accounting_stock_detail_view_ods
   WHERE sdt = regexp_replace(date_sub(CURRENT_DATE, 1), '-','')
     AND regexp_replace(to_date(biz_time),'-','')< '20191121'
   GROUP BY product_code,
            location_code,
            reservoir_area_code) b ON a.id = b.max_id ;

--获取入库商品每日最后一次入库日期

DROP TABLE IF EXISTS csx_ods.wms_h_stock_temp02 ;


CREATE
TEMPORARY TABLE csx_ods.wms_h_stock_temp02 AS
SELECT a. product_code,
       a.location_code,
       txn_amt,
       txn_qty,
       a.reservoir_area_code,
       regexp_replace(to_date(biz_time),
                      '-',
                      '')AS biz_date
FROM
  (SELECT product_code,
          location_code,
          txn_amt,
          txn_qty,
          reservoir_area_code,
          biz_time
   FROM csx_ods.wms_accounting_stock_detail_view_ods
   WHERE sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (), 1)),'-','') ) a
JOIN
  (SELECT max(biz_time) AS max_sdt,
          product_code,
          location_code,
          reservoir_area_code
   FROM csx_ods.wms_accounting_stock_detail_view_ods
   WHERE sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (), 1)),'-','')
     AND regexp_replace(to_date(biz_time),'-','')< '20191121'
     AND in_or_out = 0
   GROUP BY product_code,
            location_code,
            reservoir_area_code) b ON a.biz_time = b.max_sdt
AND a.location_code = b.location_code
AND a.product_code = b.product_code
AND a.reservoir_area_code = b.reservoir_area_code ;


CREATE
TEMPORARY TABLE csx_ods.wms_h_stock_temp03 AS
;
SELECT a.location_code shop_code,
       c.shop_name,
       a.product_code goods_code,
       c.goods_bar_code,
       c.goods_name,
       bd_id,
       bd_name,
       category_code,
       category_name,
       category_big_code,
       category_big_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       dept_code,
       dept_name,
       brand_name,
       unit,
       spec,
       manufacturer,
       delivery_type,
       business_type,
       product_status_name,
       vendor_code,
       vendor_name,
       logistics_mode_name,
       valid_tag_name,
       sales_return_tag,
       location_name,
       a.reservoir_area_code,
       a.reservoir_area_name,
       after_price,
       after_qty,
       after_amt,
       biz_date,
       b.txn_qty,
       b.txn_amt
FROM
  (SELECT a.location_code,
          product_code,
          a.reservoir_area_code,
          a.reservoir_area_name,
          after_price,
          after_qty,
          after_amt
   FROM csx_ods.wms_h_stock_temp01 AS a
   UNION ALL
   SELECT b.shop_id AS location_code,
                    b.goodsid AS product_code,
                    b.inv_place AS reservoir_area_code,
                    '' AS reservoir_area_name,
                    b.cycle_unit_price AS after_price,
                    b.inv_qty AS after_qty,
                    b.period_inv_amt AS after_amt
   FROM csx_dw.inv_sap_setl_dly_fct b
   WHERE sdt='20191120'
     AND b.sales_dist NOT IN ('612000',
                              '613000'))AS a
LEFT JOIN
  (SELECT a.shop_code,
          a.shop_name,
          a.product_code AS goods_code,
          a.product_name AS goods_name,
          product_bar_code AS goods_bar_code,
          case when root_category_code in ('10','11') then '10'  when root_category_code in ('12','13','14') then '11' else '15' end bd_id,
          case when root_category_code in ('10','11') then '生鲜供应链'  when root_category_code in ('12','13','14') then '食百供应链' else '易耗品' end bd_name,
          root_category_code AS category_code,
          root_category_name AS category_name,
          big_category_code AS category_big_code,
          big_category_name AS category_big_name,
          middle_category_code AS category_middle_code,
          middle_category_name AS category_middle_name,
          small_category_code AS category_small_code,
          small_category_name AS category_small_name,
          purchase_group_code AS dept_code,
          purchase_group_name AS dept_name,
          brand_name,
          unit,
          spec,
          manufacturer, --生产厂商
 CASE
     WHEN delivery_type='1' THEN '整件'
     WHEN delivery_type='2' THEN '小包装'
     WHEN delivery_type='3' THEN '散装'
     ELSE delivery_type
 END delivery_type , --配送方式
 CASE
     WHEN business_type='0' THEN '自营'
     WHEN business_type='1' THEN '联营'
     ELSE business_type
 END business_type, --经营方式
 product_status_name, --商品状态名称
 supplier_code AS vendor_code,
 supplier_name AS vendor_name,
 logistics_mode_name, --物流模式名称
 valid_tag_name, --有效标识名称
 CASE
     WHEN sales_return_tag='0' THEN '不可退'
     WHEN sales_return_tag='1' THEN '可退'
     ELSE sales_return_tag
 END sales_return_tag , --退货标识
 location_name --地点类型名称
   FROM csx_ods.csx_product_info AS a) c ON a.location_code = c.shop_code
AND a.product_code = c.goods_code
LEFT JOIN csx_ods.wms_h_stock_temp02 AS b ON a.location_code = b.location_code
AND a.product_code = b.product_code
AND a.reservoir_area_code = b.reservoir_area_code;

