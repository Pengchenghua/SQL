SELECT product_code,
             location_code,
             shipper_code,
             after_qty,
             after_amt,
             after_price,
             regexp_replace(to_date(posting_time), '-', '')posting_time,
             id,
             reservoir_area_code,
             reservoir_area_name
      FROM csx_ods.wms_accounting_stock_detail_ods
      WHERE sdt = '20191024' AND location_code ='W0A3'
      ;
      
  SELECT a.product_code goodsid,
                    a.location_code as shop_id,
                    -- a.shipper_code,
                    a.reservoir_area_code,
                    a.reservoir_area_name,
                    sum(after_qty) inv_qty,
                    sum(after_amt) period_inv_amt,
                   -- sum(after_price) qm_price,
                    posting_time
   FROM
     (SELECT product_code,
             location_code,
             shipper_code,
             after_qty,
             after_amt,
             after_price,
             regexp_replace(to_date(posting_time), '-', '')posting_time,
             id,
             reservoir_area_code,
             reservoir_area_name
      FROM csx_ods.wms_accounting_stock_detail_view_ods
      WHERE sdt = '20191024' ) a
   JOIN
     (SELECT product_code,
             location_code,
             shipper_code,
             max(id)max_id,
             reservoir_area_code,
             reservoir_area_name
      FROM csx_ods.wms_accounting_stock_detail_view_ods
      WHERE 
         regexp_replace(to_date(update_time),'-','')<='20190924'
        -- AND regexp_replace(to_date(posting_time),'-','')<='20190924'
         AND sdt = '20191024'
      GROUP BY product_code,
               location_code,
               shipper_code,
               reservoir_area_code,
               reservoir_area_name) b ON a.product_code = b.product_code
   AND a.location_code = b.location_code
   AND a.shipper_code = b.shipper_code
   AND A.reservoir_area_code = b.reservoir_area_code
   AND a.id = b.max_id
   GROUP BY a.product_code,
            a.location_code,
            a.shipper_code,
            a.reservoir_area_code,
            a.reservoir_area_name,posting_time
            ;