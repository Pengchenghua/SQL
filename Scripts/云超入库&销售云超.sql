
select mon,
    dist_code,
    dist_name,
    receive_location_code,
    shop_name,
    supplier_code,
    vendor_name,
    goods_code,
    goods_name,
    department_id,
    department_name,
    qty,
    amount,
    no_tax_amount,
    business_type ,
    entry_type 
from 
(select substr(sdt,1,6)mon, 
    receive_location_code,
    supplier_code ,
    goods_code,
    business_type ,
    entry_type ,
    sum(receive_qty)qty,
    sum(receive_qty*price)amount,
    sum(amount/(1+tax_rate/100)) as no_tax_amount
from csx_dw.wms_entry_order 
where sdt>='20200101' 
    and (business_type like '调拨%' or business_type like '云超%')
    AND department_id in ('A03','A05')
group by
receive_location_code,
    goods_code,
    supplier_code,
     business_type ,
    entry_type ,
    substr(sdt,1,6)
) a 
left join 
(select location_code,shop_name,dist_code,dist_name
    from csx_dw.csx_shop where sdt='current') b on a.receive_location_code=location_code 
left join 
(select goods_id,
    goods_name,
    department_id,
    department_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_code=c.goods_id
left join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') d on a.supplier_code=d.vendor_id
;