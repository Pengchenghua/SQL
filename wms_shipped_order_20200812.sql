
SET hive.exec.dynamic.partition=TRUE;


SET hive.exec.dynamic.partition.mode=nonstrict;

--  set mapreduce.job.queuename=caishixian;
SET hive.support.quoted.identifiers=NONE;


SET hive.exec.max.dynamic.partitions=20000;


SET hive.exec.max.dynamic.partitions.pernode =20000;

-- 过滤彩食鲜物流入库数据

DROP TABLE csx_tmp.tmp_ship_order_sap_v9;


CREATE
TEMPORARY TABLE csx_tmp.tmp_ship_order_sap_v9 AS
SELECT t.*,
       t1.shop_name AS shop_out_name
FROM
  ( SELECT *
   FROM csx_dw.shop_m
   WHERE sdt = 'current'
     AND sales_belong_flag IN ('4_企业购',
                               '5_彩食鲜') )t1
JOIN
  ( SELECT *
   FROM b2b.ord_orderflow_t
   WHERE sdt> regexp_replace(to_date(date_sub(current_timestamp(),30)),'-','')
     AND length(shop_id_out) = 4 )t ON t.shop_id_out = t1.shop_id;


INSERT overwrite TABLE csx_dw.wms_shipped_order partition(sdt,send_sdt)
SELECT a.id,
       batch_id,
       order_id,
       header_id,
       batch_code,
       order_no,
       link_scm_order_no,
       split_order_no,
       goods_code,
       goods_bar_code,
       goods_name,
       b.division_code,
       b.division_name,
       b.department_id,
       b.department_name,
       b.category_large_code,
       b.category_large_name,
       b.category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       unit_name unit,
       sale_unit,
       split_group_code,
       split_group_name,
       plan_qty,
       order_shipped_qty,
       shipped_qty,
       picking_side_qty,
       store_location_qty,
       receive_qty,
       pass_qty,
       reject_qty,
       price,
       add_price_percent,
       amount,
       direct_price,
       direct_amount,
       shipped_location_code,
       shipped_location_name,
       shipped_area_code,
       shipped_area_name,
       shipped_store_location_code,
       shipped_store_location_name,
       all_send_flag,
       running_model,
       picking_type,
       tc_picking_flag,
       remark,
       specs_remark,
       handle_remark,
       run_type,
       assess_type,
       assess_type_name,
       tax_type,
       tax_rate,
       tax_code,
       price_type,
       source_system,
       super_class,
       a.shipped_type_code,
       c.wms_order_type AS shipped_type,
       a.business_type_code,
       c.business_type,
       return_flag,
       direct_flag,
       shipper_code,
       shipper_name,
       supplier_code,
       supplier_name,
       receive_location_code,
       receive_location_name,
       transfer_location_code,
       transfer_location_name,
       customer_code,
       customer_name,
       sub_customer_code,
       sub_customer_address,
       shop_type,
       shop_code,
       shop_name,
       shop_address,
       station_code,
       station_name,
       receive_name,
       receive_phone_number,
       receive_province_code,
       receive_province_name,
       receive_city_code,
       receive_city_name,
       receive_area_code,
       receive_area_name,
       receive_address,
       delivery_code,
       delivery_name,
       settlement_dc,
       settlement_dc_name,
       status,
       wave_code,
       distribute_shortage_flag,
       all_received_flag,
       print_times,
       packages_number,
       link_operate_order_no,
       origin_order_no,
       link_in_out_order_no,
       link_order_no,
       external_order_no,
       send_time,
       auto_status,
       sale_channel,
       compensation_type,
       plan_date,
       order_type,
       finish_time,
       a.create_time,
       a.create_by,
       a.update_time,
       a.update_by,
       'new'sys,
            a.sdt,
        regexp_replace(substr(a.send_time, 1, 10), '-', '') AS send_sdt
FROM
  (SELECT id,
          batch_id,
          order_id,
          header_id,
          batch_code,
          order_no,
          link_scm_order_no,
          split_order_no,
          goods_code,
          goods_bar_code,
          goods_name,
          unit,
          sale_unit,
          split_group_code,
          split_group_name,
          plan_qty,
          order_shipped_qty,
          shipped_qty,
          picking_side_qty,
          store_location_qty,
          receive_qty,
          pass_qty,
          reject_qty,
          price,
          add_price_percent,
          coalesce(price*shipped_qty,0) AS amount,
          direct_price,
          direct_amount,
          shipped_location_code,
          shipped_location_name,
          shipped_area_code,
          shipped_area_name,
          shipped_store_location_code,
          shipped_store_location_name,
          all_send_flag,
          running_model,
          picking_type,
          tc_picking_flag,
          remark,
          specs_remark,
          handle_remark,
          run_type,
          assess_type,
          assess_type_name,
          tax_type,
          tax_rate,
          tax_code,
          price_type,
          source_system,
          super_class,
          shipped_type AS shipped_type_code,
          business_type AS business_type_code,
          return_flag,
          direct_flag,
          shipper_code,
          shipper_name,
          supplier_code,
          supplier_name,
          receive_location_code,
          receive_location_name,
          transfer_location_code,
          transfer_location_name,
          customer_code,
          customer_name,
          sub_customer_code,
          sub_customer_address,
          shop_type,
          shop_code,
          shop_name,
          shop_address,
          station_code,
          station_name,
          receive_name,
          receive_phone_number,
          receive_province_code,
          receive_province_name,
          receive_city_code,
          receive_city_name,
          receive_area_code,
          receive_area_name,
          receive_address,
          delivery_code,
          delivery_name,
          settlement_dc,
          settlement_dc_name,
          status,
          wave_code,
          distribute_shortage_flag,
          all_received_flag,
          print_times,
          packages_number,
          link_operate_order_no,
          origin_order_no,
          link_in_out_order_no,
          link_order_no,
          external_order_no,
          send_time,
          auto_status,
          sale_channel,
          compensation_type,
          plan_date,
          order_type,
          finish_time,
          create_time,
          create_by,
          update_time,
          update_by,
          sdt
   FROM --csx_dw.wms_shipped_order_m
 csx_dw.dwd_wms_r_d_shipped_order_detail
   WHERE (sdt> regexp_replace(to_date(date_sub(current_timestamp(),60)),'-','')
          OR sdt='19990101')
     AND status<>9)a
LEFT JOIN
  (SELECT goods_id,
          unit_name,
          division_code,
          division_name,
          category_large_code,
          category_large_name,
          category_middle_code,
          category_middle_name,
          category_small_code,
          category_small_name,
          department_id,
          department_name
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_code=b.goods_id
LEFT JOIN
  ( SELECT *
   FROM csx_ods.source_wms_r_d_bills_config
   WHERE sdt = regexp_replace(date_sub(CURRENT_DATE,1),'-','') ) c ON a.business_type_code= c.business_type_code
AND a.shipped_type_code=c.type_code
UNION ALL
SELECT concat('O',pur_doc_id, t.sdt,goodsid) AS id,
       '' AS batch_id,
       '' AS order_id,
       '' AS header_id,
       '' AS batch_code,
       pur_doc_id AS order_no,
       pur_app_id AS link_scm_order_no,
       '' AS split_order_no,
       goodsid AS goods_code,
       bar_code AS goods_bar_code,
       goods_name goods_name,
       division_code,
       division_name,
       department_id,
       department_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       unit_name AS unit,
       unit_name AS sale_unit,
       '' AS split_group_code,
       '' AS split_group_name,
       t.purchase_qty AS plan_qty,
       t.pur_order_qty AS order_shipped_qty,
       t.pur_qty_out AS shipped_qty,
       qty_picked AS picking_side_qty,
       0 AS store_location_qty,
       t.recpt_qty AS receive_qty,
       0 AS pass_qty,
       0 AS reject_qty,
       cast(pur_doc_net_price*(1+coalesce(taxrate, 0)/100) AS decimal(10, 6)) AS price,
       0 AS add_price_percent,
       t.tax_pur_val_out AS amount,
       0 AS direct_price,
       0 AS direct_amount,
       t.shop_id_out AS shipped_location_code,
       t.shop_out_name AS shipped_location_name,
       '' AS shipped_area_code,
       '' AS shipped_area_name,
       '' AS shipped_store_location_code,
       '' AS shipped_store_location_name,
       1 AS all_send_flag,
       '' AS running_model,
       '' AS picking_type,
       '' AS tc_picking_flag,
       '' AS remark,
       '' AS specs_remark,
       '' AS handle_remark,
       99 AS run_type,
       '' AS assess_type,
       '' AS assess_type_name,
       22 AS tax_type,
       taxrate AS tax_rate,
       '' AS tax_code,
       99 AS price_type,
       'SAP' AS source_system,
       1 AS super_class,
       '999' AS shipped_type_code,
       '未定义(old)' AS shipped_type,
       CASE
           WHEN t.pur_doc_type LIKE 'Z%' THEN substr(t.pur_doc_type,1,3)
       END AS business_type_code,
       CASE
           WHEN t.pur_doc_type LIKE 'ZN0%' THEN '采购出库(old)'
           WHEN t.pur_doc_type LIKE 'ZNR%' THEN '退货出库(old)'
           WHEN ( t.pur_doc_type LIKE 'ZU0%'
                 OR t.pur_doc_type LIKE 'ZC0%' ) THEN '调拨出库(old)'
           WHEN ( t.pur_doc_type LIKE 'ZUR%'
                 OR t.pur_doc_type LIKE 'ZCR%' ) THEN '返配出库(old)'
           WHEN ( t.pur_doc_type LIKE 'ZX%' ) THEN '申偿出库(old)'
       END AS business_type,
       '' AS return_flag,
       '' AS direct_flag,
       'YHCSX' AS shipper_code,
       '永辉彩食鲜' AS shipper_name,
       t.vendor_id AS supplier_code,
       t3.vendor_name AS supplier_name,
       t.shop_id_in AS receive_location_code,
       t5.shop_name AS receive_location_name,
       transfer_order_supp_loc AS transfer_location_code,
       t7.shop_name AS transfer_location_name,
       t.acct_id AS customer_code,
       T6.SHOP_NAME AS customer_name,
       '' AS sub_customer_code,
       '' AS sub_customer_address,
       '' AS shop_type,
       '' AS shop_code,
       '' AS shop_name,
       '' AS shop_address,
       '' AS station_code,
       '' AS station_name,
       '' AS receive_name,
       '' AS receive_phone_number,
       '' AS receive_province_code,
       '' AS receive_province_name,
       '' AS receive_city_code,
       '' AS receive_city_name,
       '' AS receive_area_code,
       '' AS receive_area_name,
       '' AS receive_address,
       '' AS delivery_code,
       '' AS delivery_name,
       '' AS settlement_dc,
       '' AS settlement_dc_name,
       7 AS status,
       '' AS wave_code,
       1 AS distribute_shortage_flag,
       1 AS all_received_flag,
       0 AS print_times,
       0 AS packages_number,
       nvl(org_doc_id,'') AS link_operate_order_no,
       nvl(pur_doc_id_app,'') AS origin_order_no,
       nvl(org_doc_id,'') AS link_in_out_order_no,
       nvl(pur_app_id,'') AS link_order_no,
       '' AS external_order_no,
       from_unixtime(unix_timestamp(t.min_pstng_date_out,'yyyymmdd'),'yyyy-mm-dd 00:00:00.0') AS send_time,
       0 AS auto_status,
       '' AS sale_channel,
       '' AS compensation_type,
       plan_delivery_date AS plan_date,
       ordertype AS order_type,
       from_unixtime(unix_timestamp(t.sdt,'yyyymmdd'),'yyyy-mm-dd 00:00:00.0') AS finish_time,
       '' AS create_time,
       '' AS create_by,
       '' AS update_time,
       '' AS update_by,
       'old' AS sys,
       t.sdt,
       t.sdt AS send_sdt
FROM
  ( SELECT *
   FROM csx_tmp.tmp_ship_order_sap_v9
   WHERE shop_id_out<>'W098' ) t
LEFT OUTER JOIN
  ( SELECT goods_id,
           goods_name,
           a.bar_code,
           unit_name,
           division_code,
           division_name,
           category_large_code,
           category_large_name,
           category_middle_code,
           category_middle_name,
           category_small_code,
           category_small_name,
           department_id,
           department_name
   FROM csx_dw.goods_m a
   WHERE sdt = 'current' ) t2 ON t.goodsid = t2.goods_id
LEFT OUTER JOIN
  ( SELECT vendor_id,
           vendor_name
   FROM csx_dw.vendor_m
   WHERE sdt = 'current' ) t3 ON regexp_replace(t.vendor_id, '^0*', '') = t3.vendor_id
LEFT OUTER JOIN
  ( SELECT shop_id,
           shop_name
   FROM csx_dw.shop_m
   WHERE sdt = 'current' ) t4 ON t.shop_id_out = t4.shop_id
LEFT OUTER JOIN
  ( SELECT shop_id,
           shop_name
   FROM csx_dw.shop_m
   WHERE sdt = 'current' ) t5 ON t.shop_id_in = t5.shop_id
LEFT OUTER JOIN
  ( SELECT shop_id,
           shop_name
   FROM csx_dw.shop_m
   WHERE sdt = 'current' ) t6 ON t.acct_id = concat('S',t6.shop_id)
LEFT OUTER JOIN
  ( SELECT shop_id,
           shop_name
   FROM csx_dw.shop_m
   WHERE sdt = 'current' ) t7 ON t.transfer_order_supp_loc = t7.shop_id ;

