
SELECT dc_code,province_code,province_name,
      a.goods_code,
       division_code,
       final_qty,
       final_amt,
       period_qty,
       period_amt,
       receive_amt,
       if(c.goods_code is not null, 'ÊÇ', '·ñ') as label
       from 
(
SELECT dc_code,
      a.goods_code,
       division_code,
       sum(CASE
               WHEN sdt='20191206'THEN qty
           END) AS final_qty,
       sum(CASE
               WHEN sdt='20191206' THEN amt
           END) AS final_amt,
       sum(qty) AS period_qty,
       sum(amt) AS period_amt
FROM csx_dw.wms_accounting_stock_m a 
WHERE sdt>='20191201'
  AND sdt<='20191206'
  AND reservoir_area_code NOT IN ('B999',
                                  'B997',
                                  'PD01',
                                  'PD02',
                                  'TS01')
GROUP BY dc_code,
         a.goods_code,
         division_code )a
join
(select shop_id,province_code,province_name from csx_dw.shop_m where sdt='current')b on regexp_replace(a.dc_code,'(^E)','9')=b.shop_id
LEFT OUTER JOIN
(select DISTINCT factory_location_code,goods_code from csx_dw.factory_bom where sdt='20191206')c 
on a.goods_code=c.goods_code AND A.dc_code=factory_location_code
LEFT JOIN
(select receive_location_code,goods_code,sum(receive_qty*price) as receive_amt from csx_dw.wms_entry_order_m where sdt='20191206'
    GROUP BY receive_location_code,goods_code)d  on a.dc_code=d.receive_location_code and a.goods_code=d.goods_code

;