
select sum(sales_value ) from csx_dw.sale_goods_m where customer_no='104840' and sdt='20191106';
select * from b2b.ord_orderflow_t where factory_customer_id ='0000104840' and sdt='20191103';