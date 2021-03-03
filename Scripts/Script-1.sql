select * from csx_b2b_crm.customer c where customer_number ='113383';
select * from csx_basic_data.md_product_info ;

 select * from csxprd_s2b.bshop_order_shipped_goods where order_code = '2002190300578455';
 select * from csx_b2b_accounting.accounting_credential_item where source_order_no = '0300578455';
 select * from csx_b2b_accounting.accounting_stock_log_item where credential_no = 'PZ0000000001909248' and product_code = '639992';
  select * from csx_b2b_accounting.accounting_stock_log_item where batch_no = 'CB20200304009695';
  
 
 select * from csx_b2b_accounting.accounting_stock_detail where location_code='E218' and product_code='894890';
 
SELECT * FROM csx_b2b_accounting.accounting_last_in_stock alis

;

SELECT * from csx_b2b_accounting.accounting_stock_detail asd where location_code='9992' 
;
select * from csx_basic_data.md_all_shop_info masi where rt_shop_code='9992';

select * from data_sync.data_relation_cas_sale_credential drcsc where drcsc.posting_time>'2020-03-01 00:00:00' and posting_time<'2020-03-11 00:00:00' and product_code='4917' 
and location_code='W0H4';
SELECT * FROM csx_basic_data.md_shop_configuration msc ;
SELECT * FROM csx_basic_data.base_address_info bai ;

select * from csx_b2b_wms.wms_bills_config wbc;


select count(DISTINCT msi.supplier_code),count(DISTINCT msi.bank_name) from csx_basic_data.md_supplier_info msi;

select * from data_sync.data_relation_cas_sale_credential drcsc  where purchase_group_code IS NULL and posting_time>='2020-04-01 01:18:44' ;
select * from  csx_basic_data.csx_product_info cpi where shop_code ='W0A3' AND product_code ='1164548';

select * from data_sync.data_relation_cas_sale_credential where
location_code='W0H4' AND source_order_no='OY200401000826';

select * from csx_b2b_wms.wms_entry_order_header weoh where order_code='TD191028000017';

select * from  csx_b2b_accounting.accounting_transfer_config atc;
select * from csx_basic_data.md_dic md  ;
select * from csx_basic_data.md_shop_info msi2 ;

select * from csxprd_common.

select * from csx_basic_data.md_supplier_info msi where supplier_name like '%旺龙顺%';
select 

select * from csx_b2b_wms.wms_entry_order_header weoh;


SELECT * FROM c.accounting_file af ;
select * from csx_b2b_wms.wms_task wt where task_type ='6'and finish_time >='2020-10-01 00:00:00' and warehouse_code ='W0A8';

select * from csx_b2b_wms.wms_bills_config wbc ;
select * from csx_basic_data.md_dic md ;
SELECT * FROM csx.stock_center sc ;
select * from csx_b2b_wms.wms_product_stock_detail wpsd  where product_code ='967511' and warehouse_code ='W0A8';
