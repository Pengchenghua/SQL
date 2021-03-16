--耗材入库查询
SELECT sales_province_code,sales_province_name,
        receive_location_code,
       receive_location_name,
       goods_bar_code,
       goods_code,
       goods_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       sum(receive_qty) as qty,
       sum(price*receive_qty) as amt,
       supplier_code,
       supplier_name
FROM csx_dw.dws_wms_r_d_entry_detail a 
join 
(select shop_id,sales_province_code,sales_province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and table_type='1') b  on a.receive_location_code=b.shop_id
WHERE sdt>='20200101'
  AND sdt<'20210101'
   -- AND a.division_code='15' 
  AND category_middle_code IN ('150602',
                               '150105',
                               '150102',
                               '150101',
                               '150601',
                               '150601',
                               '150118',
                               '150704',
                               '150131',
                               '150133',
                               '150702',
                               '150106',
                               '150108',
                               '150701',
                               '150122',
                               '150120',
                               '150107'
                               )
group by 
        receive_location_code,
       receive_location_name,
       goods_bar_code,
       goods_code,
       goods_name,
       category_large_code,
       category_large_name,
       supplier_code,
       supplier_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       sales_province_code,sales_province_name;

