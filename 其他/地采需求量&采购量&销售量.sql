
select * from csx_tmp.tmp_11 ;
create temporary table csx_tmp.tmp_11 as 
SELECT receive_location_code,
       link_order_code,
       sale_order_code,
       local_purchase_order_code,
       product_code,
       product_name,
       product_price,
       qty,
       req_qty,
       sales_price,
       order_qty,
       order_price,
       receive_qty,
       receive_price 
       
FROM
(SELECT receive_location_code,
       link_order_code,
       sale_order_code,
       local_purchase_order_code,
       product_code,
       product_name,
       product_price,
       qty,
       req_qty
FROM csx_ods.source_scm_r_a_scm_local_purchase_request
WHERE sdt='20210202'
  AND create_time>='2021-01-01 00:00:00'
  AND create_time<'2021-02-01 00:00:00'
  and status=2
  ) a 
  left join 
  (select order_code,location_code,goods_code,order_qty,order_price,receive_qty,receive_price from  csx_dw.ads_supply_order_flow where sdt>='20201231')b on a.link_order_code=b.order_code and a.product_code=b.goods_code
   left join 
  (SELECT order_no,goods_code,sales_price from csx_dw.dws_sale_r_d_detail where sdt>='20201231')c on a.sale_order_code=order_no and a.product_code=c.goods_code;
  
  