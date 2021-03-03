select * from csx_b2b_wms.wms_bills_config as wbc 

select * from csx_b2b_wms.wms_shipped_order_header where order_code like '%18191019000015%'
select * from csx_b2b_wms.wms_entry_order_item  where order_code ='IN191019000052';
select * from csx_b2b_wms.wms_entry_order_header  where order_code ='IN191019000052';

SELECT * FROM csx_b2b_wms.wms_material_product_item where order_code ='TK191024012041';

select * from csx_b2b_accounting.accounting_stock_detail_view  where product_code ='10028' and location_code ='W0A3'