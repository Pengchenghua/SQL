select  * from csx_b2b_accounting.accounting_stock_detail  where location_code ='W0A3' and update_time >='2019-10-20 23:11:57' and product_code ='959568' 
 and wms_batch_no ='TK191025004950'

;
select * from csx_b2b_accounting.accounting_stock_log_item  where link_wms_batch_no ='TK191025004950';

select * from csx_b2b_accounting.accounting_stock_log_item  where batch_no ='CB20191020000221';