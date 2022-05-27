-- 新旧系统入库统计【重庆】
SELECT sdt,a.province_code,
       a.province_name,
      dc_code,
       dc_name,
       settlement_dc,
       a.settlement_dc_name,
       a.supplier_code,
       a.supplier_name,
       order_code,
       origin_order_code,
       goods_code,
       goods_name,
          unit_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          classify_small_code,
          classify_small_name,
       price,
       qty,
       amount,
       shipped_qty,
       shipped_amount
FROM
(

SELECT sdt,
        a.province_code,
       a.province_name,
       receive_location_code as dc_code,
       a.receive_location_name as dc_name,
       settlement_dc,
       a.settlement_dc_name,
       a.supplier_code,
       a.supplier_name,
       order_code,
       origin_order_code,
       goods_code,
       price,
       case when order_type_name='退货' then receive_qty*-1 else a.receive_qty end qty,
      case when order_type_name='退货' then a.amount*-1 else amount end  AS amount,
      0 shipped_qty,
      0 shipped_amount
FROM csx_dw.dws_wms_r_d_entry_detail a

WHERE supplier_code IN ('20056924',
                        '20027362',
                        '20055223')
    and a.receive_status in (1,2)
 union all 
 
SELECT sdt,
        a.province_code,
       a.province_name,
       a.shipped_location_code dc_code,
       a.shipped_location_name dc_name,
       a.settlement_dc,
       a.settlement_dc_name,
       a.supplier_code,
       a.supplier_name,
       a.order_no,
       a.origin_order_no,
       goods_code,
       price,
       0 qty,
       0 amount,
       shipped_qty  shipped_qty,
       a.shipped_amount   AS shipped_amount
FROM csx_dw.dws_wms_r_d_ship_detail a

WHERE supplier_code IN ('20056924',
                        '20027362',
                        '20055223') 
and a.status!=9
and a.business_type_code='05'
)a  
JOIN
  (SELECT goods_id,
          goods_name,
          unit_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          classify_small_code,
          classify_small_name
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b on a.goods_code=b.goods_id