--工厂调拨及销售商超金额
select substr(sdt, 1, 6) mon,
    province_name,
    send_dc_code,
    send_dc_name,
    goods_code,
    goods_name,
    sum(shipped_qty) shipped_qty,
    sum(shipped_amt) shipped_amt,
    business_type_name,
    shop_code,
    shop_name,
    receive_dc_code,
    receive_dc_name
from   csx_dws.csx_dws_wms_shipped_detail_di
where (
        (
            sdt >= '20230701'
            and sdt < '20231001'
        )
        or (
            sdt >= '20240701'
            and sdt < '20241001'
        )
    ) -- and send_dc_code='W039'
    and shipped_type in ('S07', 'T06') 
    -- and shipped_type like 'T%'
    and send_dc_code in (
        'WA93',
'W080',
'WB04',
'W048',
'WB03',
'W053',
'W088',
'W0BZ',
'W079',
'WB98',
'W0AZ',
'W039',
'WB03',
'W053',
'W0AR',
'W079',
'WB03',
'W053',
'W0BT',
'W0AZ',
'WB04',
'W053',
'W0AZ',
'W0P3',
'W0P6',
'W0S9',
'W0T6',
'W0Q8',
'W0R8',
'WB00',
'W0X1',
'W0Z8',
'W0AZ',
'W039',
'W053',
'W0AZ',
'WC56',
'W039',
'W039',
'W039',
'WB03',
'WB03',
'W053',
'WB03',
'WD18',
'W0BJ',
'WD51'
    )
group by province_name,
    send_dc_code,
    send_dc_name,
    goods_code,
    goods_name,
    business_type_name,
    shop_code,
    shop_name,
    receive_dc_code,
    receive_dc_name,
    substr(sdt, 1, 6)