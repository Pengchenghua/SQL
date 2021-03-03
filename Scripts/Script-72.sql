SELECT
	*
FROM
	csx_b2b_scm.scm_order_header
WHERE
	order_code = 'TO99B2191023000095';

SELECT
	*
FROM
	csx_b2b_scm.scm_product_shipped_dtl
WHERE
	batch_order_code = 'TK191105009008';

SELECT
	*
FROM
	csx_b2b_scm.scm_order_product_price
WHERE
	order_code = 'TO99B2191023000095';


SELECT
	*
FROM
	csx_basic_data.csx_product_info
WHERE
	shop_code = 'W048'
	AND product_code = '841896';

SELECT
	*
FROM
	csx_b2b_wms.wms_shipped_order_item
WHERE
	order_code IN ('OU191121000077',
	'OU191121000076',
	'OU191120000105',
	'OU191120000106') ;

SELECT
	*
FROM
	csx_b2b_accounting.accounting_stock_detail
WHERE
	DATE_FORMAT (biz_time ,
	'yyyy%MM%dd')
	!= DATE_FORMAT (posting_time ,
	'yyyy%MM%dd')
	AND posting_time >= '2019-11-20 00:00:00';

SELECT
	*
FROM
	csx_b2b_factory.factory_setting_general_bom ;

SELECT
	a.*,
	b.*
FROM
	csx_b2b_wms.wms_bills_config a
LEFT JOIN csx_b2b_wms.wms_entry_order_header b ON
	a.business_type_code = b.business_type ;

SELECT
	DISTINCT
FROM
	csx_b2b_factory.factory_setting_bom;
	
select * from csx_b2b_wms.wms_bills_config ;

SELECT * FROM csx_b2b_sell.apply_order where apply_no ='OY191128001004';

select * from data_sync.data_sync_inventory_item where posting_time>='2020-05-01 00:00:00' and posting_time<='2020-05-31 23:59:00' and location_code='W053' ;