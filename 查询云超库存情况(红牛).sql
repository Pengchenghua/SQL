SELECT a.shop_id,
       shop_name,
       b.sales_dist_name,
       b.sales_dist,
       prov_code,
       prov_name,
       zone_id,
       zone_name,
       a.goodsid,
       goodsname,
       inv_qty,
       inv_amt
FROM
  (SELECT shop_id,
          goodsid,
          inv_qty,
          inv_amt
   FROM dw.inv_sap_setl_dly_fct a
   WHERE sdt='20191230'
     AND goodsid IN ('7260',
                     '7265',
                     '14360',
                     '152270',
                     '683229',
                     '770073',
                     '793834',
                     '793835',
                     '793836',
                     '793837',
                     '837926',
                     '837927',
                     '907909',
                     '907910',
                     '926285',
                     '994131',
                     '1015768',
                     '1044800'))a
JOIN
  (SELECT shop_id,
          shop_name,
          sales_dist_name,
          sales_dist,
          prov_code,
          prov_name,
          zone_id,
          zone_name
   FROM dim.dim_shop
   WHERE edate='9999-12-31'
     AND zone_name IS NOT NULL) b ON a.shop_id=b.shop_id
JOIN
  (SELECT goods_uid,goodsid,goodsname
   FROM dim.dim_goods
   WHERE edate='9999-12-31') c ON a.goodsid=c.goodsid ;
