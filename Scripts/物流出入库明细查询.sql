SELECT
	a.sdt,
	close_time,
	dc_code,
	dc_name ,
	receive_time,
	order_code ,
	origin_order_code,
	goods_code,
	goods_name,
	division_code,
	division_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name,
	category_small_code,
	category_small_name,
	department_name,
	department_id,
	unit,
	price,
	plan_qty,
	receive_qty,
	amount,
	supplier_code,
	supplier_name,
	entry_type,
	b.wms_order_type,
	a.business_type as business_type_code,
	b.business_type,
	--b.business_type_code,
 send_dc_code,
	send_dc_name
from
	(
	SELECT
		sdt,
		receive_location_code dc_code,
		receive_location_name dc_name ,
		send_location_code as send_dc_code,
		send_location_name as send_dc_name,
		receive_time,
		close_time,
		order_code ,
		origin_order_code,
		goods_code,
		goods_name,
		unit,
		price,
		plan_qty,
		receive_qty,
		amount,
		entry_type,
		business_type,
		supplier_code,
		supplier_name
	from
		csx_dw.wms_entry_order_all_m
	where
		sdt >= '${sdate}'
		and sdt <= '${edate}') as a
join (
	select
		*
	from
		csx_ods.source_wms_r_d_bills_config
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),
		1)),
		'-',
		'')
		and system_code = 'CG'
		AND order_type_code like 'IN%') b on
	a.business_type = b.business_type_code
	and type_code = a.entry_type
join (
	select
		goods_id,
		division_code,
		division_name,
		category_large_code,
		category_large_name,
		gs.category_middle_code,
		gs.category_middle_name,
		gs.category_small_code,
		gs.category_small_name,
		department_name,
		department_id
	from
		csx_dw.goods_m gs
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),
		1)),
		'-',
		''))c on
	a.goods_code = c.goods_id ;


SELECT x.* FROM csx_ods.wms_storage_location_ods x
WHERE x.storage_location_code IN ('BZ01014055')
;
select
	location_code,
	shop_name,
	province_code,
	province_name,
	location_type_code,
	location_type
from
	csx_dw.csx_shop
where
	sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),
	1)),
	'-',
	'');

select
	order_no,
	shm.receive_city_code,
	shm.receive_city_name,
	shm.business_type,
	shm.shipped_type,
from
	csx_dw.wms_shipped_order_m shm;

SELECT
	a.sdt,
	close_time,
	dc_code,
	dc_name ,
	receive_time,
	order_code ,
	origin_order_code,
	goods_code,
	goods_name,
	unit,
	price,
	plan_qty,
	receive_qty,
	amount,
	supplier_code,
	supplier_name,
	entry_type,
	b.wms_order_type,
	a.business_type as business_type_code,
	b.business_type,
	--b.business_type_code,
 send_dc_code,
	send_dc_name
from
	(
	SELECT
		sdt,
		receive_location_code dc_code,
		receive_location_name dc_name ,
		send_location_code as send_dc_code,
		send_location_name as send_dc_name,
		receive_time,
		close_time,
		order_code ,
		origin_order_code,
		goods_code,
		goods_name,
		unit,
		price,
		plan_qty,
		receive_qty,
		amount,
		entry_type,
		business_type,
		supplier_code,
		supplier_name
	from
		csx_dw.wms_entry_order_all_m
	where
		sdt >= '20200316'
		and sdt <= '20200316'
		and receive_location_code in ('W0A3') ) as a
join (
	select
		*
	from
		csx_ods.source_wms_r_d_bills_config
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),
		1)),
		'-',
		'')) b on
	a.business_type = b.business_type_code
	and type_code = a.entry_type;
	



select DISTINCT wms_order_type,business_type from csx_ods.source_wms_r_d_bills_config where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') and system_code='CG'AND order_type_code like 'IN%'
order by wms_order_type desc;


select * from csx_dw.customer_m where customer_name like '眉%' and sdt='20200317';

select * from csx_dw.wms_shipped_order_m where sdt='20200317'