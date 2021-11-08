-- 酒水WB09入库明细
select      order_code,
    batch_code,
    origin_order_code,
    receive_location_code,
    receive_location_name,
    send_location_code,
    send_location_name,
    supplier_code,
    supplier_name,
    goods_bar_code,
    goods_code,
    goods_name,
    unit,
    receive_qty,
     price/(1+tax_rate/100) as no_price,
    price/(1+tax_rate/100)*receive_qty as no_tax_amt,
    price,
    amount,
    receive_time,
    sdt
from csx_dw.dws_wms_r_d_entry_batch 
    where receive_location_code='WB09' 
    and department_code='A10' 
    and (sdt>='20210901'  or sdt='19990101')
    and receive_status in (1,2)
    -- and supplier_code in ('20014815','20032297')

;

--入库明细

select  credential_no,   
   a.order_code,
   a.batch_code,
   a.origin_order_code,
   a.receive_location_code,
   a.receive_location_name,
   a.send_location_code,
   a.send_location_name,
   a.supplier_code,
   a.supplier_name,
   a.goods_bar_code,
   a.goods_code,
   a.goods_name,
   a.unit,
    receive_qty,
    price/(1+tax_rate/100) as no_price,
    price/(1+tax_rate/100)*receive_qty as no_tax_amt,
    price,
    amount,
    sdt
from csx_dw.dws_wms_r_d_entry_batch a 
left join
(select 
    credential_no,wms_batch_no,goods_code,batch_no,qty as out_qty,amt_no_tax as out_amt,move_name
    from csx_dw.dws_wms_r_d_batch_detail 
        where move_type='104A'
        and sdt>='20210101') b on a.batch_code=b.wms_batch_no and a.goods_code=b.goods_code
where settlement_dc='WB09' and department_code='A10' and sdt>='20210901';

--酒水销售明细
select a.order_no,
    a.origin_order_no,
    a.province_code,
    a.province_name,
    dc_code,
    a.dc_name,
    a.perform_dc_code,
    a.perform_dc_name,
    a.goods_code,
    a.goods_name,
    a.unit,
    a.sales_qty,
    a.sales_price,
    a.cost_price,
    a.excluding_tax_cost,
    a.excluding_tax_sales,
    a.excluding_tax_profit,
    sdt
from csx_dw.dws_sale_r_d_detail a 
join 
(select  distinct
   a.goods_code
from csx_dw.dws_wms_r_d_entry_batch a 
where a.settlement_dc='WB09' and department_code='A10' ) b on a.goods_code=b.goods_code 

where sdt>='20210901' and a.department_code='A10';

