CREATE TEMPORARY TABLE csx_tmp.peng_vendor_01 AS
SELECT a.tax_num,
  sum(qty) qty,
  sum(amt) amt,
  sum(csx_qty) csx_qty,
  sum(csx_amt) csx_amt
FROM (
    SELECT tax_num,
      sum(pur_qty_in) qty,
      sum(tax_pur_val_in) amt,
      0 AS csx_qty,
      0 AS csx_amt
    FROM b2b.ord_orderflow_t a
      JOIN (
        SELECT trim(vat_regist_num) AS tax_num,
          vendor_name,
          vendor_id
        FROM csx_dw.dws_basic_w_a_csx_supplier_m
        WHERE sdt = 'current'
      ) b ON a.vendor_id = b.vendor_id
      JOIN (
        SELECT location_code
        FROM csx_dw.csx_shop
        WHERE sdt = 'current'
          AND table_type = 2
      ) c ON a.shop_id_in = c.location_code
    WHERE sdt >= '20200101'
      AND sdt < '20200801'
      AND ordertype = '直送'
    GROUP BY tax_num
    UNION ALL
    SELECT tax_num,
      0 AS qty,
      0 amt,
      sum(receive_qty) csx_qty,
      sum(amount) csx_amt
    FROM csx_dw.wms_entry_order a
      JOIN (
        SELECT trim(vat_regist_num) AS tax_num,
          vendor_name,
          vendor_id
        FROM csx_dw.dws_basic_w_a_csx_supplier_m
        WHERE sdt = 'current'
      ) b ON a.supplier_code = b.vendor_id
      JOIN (
        SELECT location_code
        FROM csx_dw.csx_shop
        WHERE sdt = 'current'
          AND table_type = 1
      ) c ON a.receive_location_code = c.location_code
    WHERE sdt >= '20200101'
      AND sdt < '20200801'
      AND entry_type LIKE '采购%'
    GROUP BY tax_num
  ) a
GROUP BY tax_num;



drop table csx_tmp.peng_vendor_01;


CREATE TEMPORARY TABLE csx_tmp.peng_vendor_01 AS
SELECT a.tax_num,
  csx_qty,
  csx_amt,
  qty,
  amt
FROM (
    SELECT tax_num,
      sum(receive_qty) csx_qty,
      sum(amount) csx_amt
    FROM csx_dw.wms_entry_order a
      JOIN (
        SELECT trim(vat_regist_num) AS tax_num,
          vendor_name,
          vendor_id
        FROM csx_dw.dws_basic_w_a_csx_supplier_m
        WHERE sdt = 'current'
      ) b ON a.supplier_code = b.vendor_id
      JOIN (
        SELECT location_code
        FROM csx_dw.csx_shop
        WHERE sdt = 'current'
          AND table_type = 1
      ) c ON a.receive_location_code = c.location_code
    WHERE sdt >= '20200101'
      AND sdt < '20200801'
      AND entry_type LIKE '采购%'
    GROUP BY tax_num
  ) a
  JOIN (
    SELECT tax_num,
      sum(pur_qty_in) qty,
      sum(tax_pur_val_in) amt
    FROM b2b.ord_orderflow_t a
      JOIN (
        SELECT trim(vat_regist_num) AS tax_num,
          vendor_name,
          vendor_id
        FROM csx_dw.dws_basic_w_a_csx_supplier_m
        WHERE sdt = 'current'
      ) b ON a.vendor_id = b.vendor_id
      JOIN (
        SELECT location_code
        FROM csx_dw.csx_shop
        WHERE sdt = 'current'
          AND table_type = 2
      ) c ON a.shop_id_in = c.location_code
    WHERE sdt >= '20200101'
      AND sdt < '20200801'
      AND ordertype = '直送'
    GROUP BY tax_num
  ) b ON a.tax_num = b.tax_num;
SELECT tax_num,
  supplier_code,
  vendor_name,
  sum(receive_qty) csx_qty,
  sum(amount) csx_amt,
  substr(sdt, 1, 6) as mon
FROM csx_dw.wms_entry_order a
  JOIN (
    SELECT trim(vat_regist_num) AS tax_num,
      vendor_name,
      vendor_id
    FROM csx_dw.dws_basic_w_a_csx_supplier_m a
      join csx_tmp.peng_vendor_01 b on trim(vat_regist_num) = tax_num
    WHERE sdt = 'current'
  ) b ON a.supplier_code = b.vendor_id
where sdt >= '20200101'
  AND sdt < '20200801'
  AND entry_type LIKE '采购%'
GROUP BY tax_num,
  supplier_code,
  vendor_name,
  substr(sdt, 1, 6);
