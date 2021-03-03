select
	*
from
	csx_b2b_wms.wms_bills_config ;
where
	date_format (create_time,'%Y%m%d')>='20191020';
	
SELECT * FROM csx_b2b_wms.wms_entry_order_item as a where order_code ='IN190930000218';