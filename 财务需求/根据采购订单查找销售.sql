
--根据采购单号查找销售【财务】
-- 采购订单入库
select a.goods_code,
    a.goods_name,
    receive_location_code,
    origin_order_code,
    order_code,
    receive_qty,
    receive_amt,
    c.order_no,
    customer_no,
    customer_name,
    (sale_qty) sale_qty,
    (sales_value) sales_value,
    (profit) profit,
    b.credential_no
from 
(select goods_code,goods_name,receive_location_code,origin_order_code,order_code,sum(receive_qty) receive_qty,sum(amount) receive_amt
from csx_dw.dws_wms_r_d_entry_batch where 
   origin_order_code  in 
                        ('POW0A2211228002456',
                        'POW0A2211228002455',
                        'POW0K7210729038657',
                        'POW0K7210729001438',
                        'POW0K7210729000762',
                        'POW0K7210623002657',
                        'POW0K7210524002236',
                        'POW0A2210522000330',
                        'POW0A2210521000327',
                        'POW0G8210520000304',
                        'POW0K7210428002183',
                        'POW0K7210406001731',
                        'POW0K7210308001484',
                        'POW0K7210130001103',
                        'RPW0K7210130000004',
                        'POW0K7210125002845',
                        'POW0K7201218001678',
                        'POW0K7201121000682',
                        'POW0K7201030001473')
   -- and receive_status in (1,2)
    group by  goods_code,goods_name,receive_location_code,origin_order_code,order_code 
)a 
left join
(select b.credential_no,b.wms_order_no,b.goods_code from csx_dw.dws_wms_r_d_batch_detail b 
    where b.move_type='107A'
)b  on a.order_code=wms_order_no and a.goods_code=b.goods_code
left join
(select split_part(id,'&',1) credential_no ,order_no,customer_no,customer_name,goods_code,goods_name,sum(sales_qty) sale_qty,
    sum(sales_value) sales_value,
    sum(profit) profit
from csx_dw.dws_sale_r_d_detail 
   group by split_part(id,'&',1)  ,order_no,customer_no,customer_name,goods_code,goods_name) c on b.credential_no=c.credential_no and b.goods_code=c.goods_code
    ;
    