--指定供应商入库【财务】&旧系统  
SELECT province_name,
       receive_location_code,
       receive_location_name,
       supplier_code,
       supplier_name,
       sdt,
       goods_code,
       goods_name,
       sum(CASE WHEN business_type='ZNR1' THEN receive_qty*-1 ELSE receive_qty END) qty,
       sum(CASE WHEN business_type='ZNR1' THEN price*receive_qty*-1  ELSE price*receive_qty END) amount
FROM csx_dw.dws_wms_r_d_entry_detail
where supplier_code in ('20014608','20034399','20055065','20034399','20034399','20034399',
                        '20043536','20045813','20047386','20049806','20052361','20052380',
                        '20043536','20014608','20014608','20014608','20055065','20014608',
                        '20043536','20014608')
    and sys='old'
    and business_type like 'ZN%'
GROUP BY province_name,
       receive_location_code,
       receive_location_name,
       sdt,
       goods_code,
       goods_name,
       supplier_code,
       supplier_name
;

show create table csx_dw.dws_wms_r_d_entry_detail;


create temporary table csx_tmp.temp_01 as 
select province_name,
        order_code,
        origin_order_code,
       receive_location_code,
       receive_location_name,
       supplier_code,
       supplier_name,
       sdt,
       goods_code,
       goods_name,
       sum( receive_qty  ) receive_qty,
       sum( receive_amount  )     receive_amount,
       sum(shipp_qty ) shipp_qty,
       sum(shipp_amount ) shipp_amount
from (
SELECT province_name,
       order_code,
       origin_order_code,
       receive_location_code,
       receive_location_name,
       supplier_code,
       supplier_name,
       sdt,
       goods_code,
       goods_name,
       ( receive_qty  ) receive_qty,
       (  amount  )     receive_amount,
       0 shipp_qty,
       0 shipp_amount
FROM csx_dw.dws_wms_r_d_entry_detail
where supplier_code in ('20014608','20034399','20055065','20034399','20034399','20034399',
                        '20043536','20045813','20047386','20049806','20052361','20052380',
                        '20043536','20014608','20014608','20014608','20055065','20014608',
                        '20043536','20014608')
    and sys='new'
    and order_type_code like 'P%'
   -- and business_type like 'ZN%'

union all 
SELECT province_name,
        order_no order_code,
        origin_order_no origin_order_code,
       shipped_location_code receive_location_code,
       shipped_location_name receive_location_name,
       supplier_code,
       supplier_name,
       sdt,
       goods_code,
       goods_name,
      0 receive_qty,
      0  receive_amount,
       (shipped_qty ) shipp_qty,
       (amount ) shipp_amount
FROM csx_dw.dws_wms_r_d_ship_batch
where supplier_code in ('20014608','20034399','20055065','20034399','20034399','20034399',
                        '20043536','20045813','20047386','20049806','20052361','20052380',
                        '20043536','20014608','20014608','20014608','20055065','20014608',
                        '20043536','20014608')

    and order_type_code like 'P%'
   -- and business_type like 'ZN%'

) a   

group by province_name,
        order_code,
        origin_order_code,
       receive_location_code,
       receive_location_name,
       supplier_code,
       supplier_name,
       sdt,
       goods_code,
       goods_name;
       
select * from csx_tmp.temp_01 ;