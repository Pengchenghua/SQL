select * from csx_b2b_scm.scm_product_sales_dtl where sale_order_code ='OM1990100000697';

select * from csx_b2b_scm.scm_purchase_request_item where sale_order_code ='OY190903000553';

select * from csx_b2b_scm.scm_partner_price_select 
;
select * from csx_b2b_scm.scm_order_product_price 

select * from csx_b2b_scm.scm_product_shipped_dtl 
;
-- scm_order_header ������ͷ
-- scm_order_items ������ϸ
-- scm_order_product_price	�۸��
-- scm_product_received_dtl  �����ϸ
-- scm_product_shipped_dtl ������ϸ

select * from csx_b2b_accounting.accounting_stock_detail_view  where update_time >='2019-11-19 00:00:00'