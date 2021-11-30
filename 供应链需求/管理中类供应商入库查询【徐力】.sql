-- 管理中类供应商入库查询 classify_middle_code in('B0603','B0701') 徐力需求
select a.province_code,
    a.province_name,
    a.receive_location_code,
   a.receive_location_name,
    supplier_code,
    a.supplier_name,
    joint_purchase,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    a.goods_code,
    a.goods_name,
    a.brand_name,
    a.unit,
    product_status_name,
    sum(a.receive_qty) qty,
    sum(a.amount) as amt
from csx_dw.dws_wms_r_d_entry_batch as a
left join 
(select vendor_id,joint_purchase from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id
left join
(select product_code,shop_code,product_status_name from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') c on a.receive_location_code=c.shop_code and a.goods_code=c.product_code
where sdt>='20210101' and sdt<='20211125'
    and a.classify_middle_code in('B0603','B0701')
    and business_type_name like '供应商配送'
    and a.receive_status='2'
    and a.receive_location_code in ('WA93','W0A2','W080','W0K7','W0L4','W0AW','W0J8','W048','WB04','W0A3','WB11',
        'W0A8','WB03','W053','W0F4','W0G9','W0K6','W0AH','W0AJ','W0J2','W0F7','W0G6','WA96','W0K1','W0AU','W0L3',
        'W0BK','W0AL','W0S9','W0Q2','W0Q9','W0Q8','W0BS','W0BH','W0BR','W0R9','WB00','W0R8','W088','W0BZ','W0A5',
        'W0P8','WA94','W0AS','W0AR','WA99','W0N1','W079','W0A6','W0BD','W0N0','WB01','W0P3','W0W7','W0X1','W0X2',
        'W0Z8','W0Z9','W0AZ','W039','W0A7')
    and receive_status='2'
group by  a.province_code,
    a.province_name,
    a.receive_location_code,
   a.receive_location_name,
    supplier_code,
    a.supplier_name,
    joint_purchase,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    a.goods_code,
    a.goods_name,
    a.brand_name,
    a.unit,
    product_status_name;