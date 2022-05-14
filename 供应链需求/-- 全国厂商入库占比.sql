-- 全国厂商入库占比
SELECT b.province_name,
    a.receive_location_code,
    a.receive_location_name,
    a.supplier_code,
    a.supplier_name,
    sum(a.receive_qty) qty,
    sum(a.amount) amt
FROM csx_dw.dws_wms_r_d_entry_batch a
JOIN
  (SELECT shop_id,shop_name,
        province_code,
        province_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
    AND table_type=1
    AND purpose IN ('01','03')
    and purchase_org !='P620'
    ) b on a.receive_location_code=b.shop_id
WHERE sdt>='20220401'
  AND sdt<'20220501'
  and a.order_type_code like 'P%'
  AND a.business_type='01'
  and a.receive_status='2'
  GROUP BY b.province_name,
    a.receive_location_code,
    a.receive_location_name,
    a.supplier_code,
    a.supplier_name;
    
    
    SELECT  DISTINCT purpose,purpose_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
    AND table_type=1
    AND purpose IN ('01','07')
'20023670','20055149','20043965','20030895','20055847','20056045','20055111','20047971','20056311','20055800','20056009','20055950','20055653','20052301','20028053','20043203','20041365','20024248','20038251','20029976','20042204','20055891','20055687','20051662','20048472','20046634','20056731','20056359','20056056','20055624','20055689','20055750','20055789','20056195','20054206','20054270','20054478','20052504','20054518','20054739','20054778','20054287','116816BJ','109436CQ','20053812','20053405','20054624','20054909','20054578','20054422','20054211','20051311','20051950','