--工厂调拨及销售商超金额
select substr(sdt,1,6) mon,
    province_name,
    send_dc_code,
    send_dc_name,
    goods_code,
    goods_name,
    sum(shipped_qty)shipped_qty,
    sum(shipped_amt)shipped_amt,
    business_type_name,
    shop_code,
    shop_code,
    receive_dc_code,
    receive_dc_name
from   csx_dws.csx_dws_wms_shipped_detail_di 
where
    ((sdt>='20220501' 
    and sdt<'20220601')
        or (sdt>='20230201' 
    and sdt<'20230301')
    )
   -- and send_dc_code='W039'
    and shipped_type in ('S07','T06')
    -- and shipped_type like 'T%'
    and send_dc_code in ('W0T5',
'WA93',
'W080',
'W0R1',
'W048',
'W053',
'WB03',
'W0S2',
'W088',
'W0BZ',
'W079',
'W0T7',
'W039',
'W0AZ',
'WA98',
'W0AR',
'W0F5',
'W0BT',
'W0F6',
'W082',
'W0E7',
'WB01',
'W0P3',
'W0K3',
'WA99',
'W0T0',
'W0P6',
'W0S9',
'W0Q1',
'W0Q4',
'W0T6',
'W0Q8',
'W0R8',
'WB00',
'W0X1',
'W0R7'
)
group by province_name,
    send_dc_code,
    send_dc_name,
    goods_code,
    goods_name,
     business_type_name,
    shop_code,
    shop_code,
    receive_dc_code,
    receive_dc_name
    , substr(sdt,1,6)