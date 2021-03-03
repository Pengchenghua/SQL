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
		receive_city_code,
		receive_city_name,
		customer_code,
		customer_name,
		goods_code,
		goods_name,
		price,	
		shipped_qty,
		shipped_qty*price as shipped_amt,
		shipped_type,
		entry_type,
		business_type,
		supplier_code,
		supplier_name
	from
		csx_dw.wms_shipped_order_m
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
	
select
	id,
	batch_id,
	order_id,
	header_id,
	batch_code,
	order_no,
	link_scm_order_no,
	split_order_no,
	goods_code,
	goods_bar_code,
	goods_name,
	unit,
	sale_unit,
	split_group_code,
	split_group_name,
	plan_qty,
	order_shipped_qty,
	shipped_qty,
	picking_side_qty,
	store_location_qty,
	receive_qty,
	pass_qty,
	reject_qty,
	price,
	add_price_percent,
	amount,
	direct_price,
	direct_amount,
	shipped_location_code,
	shipped_location_name,
	shipped_area_code,
	shipped_area_name,
	shipped_store_location_code,
	shipped_store_location_name,
	all_send_flag,
	running_model,
	picking_type,
	tc_picking_flag,
	remark,
	specs_remark,
	handle_remark,
	run_type,
	assess_type,
	assess_type_name,
	tax_type,
	tax_rate,
	tax_code,
	price_type,
	source_system,
	super_class,
	shipped_type,
	business_type,
	return_flag,
	direct_flag,
	shipper_code,
	shipper_name,
	supplier_code,
	supplier_name,
	receive_location_code,
	receive_location_name,
	transfer_location_code,
	transfer_location_name,
	customer_code,
	customer_name,
	sub_customer_code,
	sub_customer_address,
	shop_type,
	shop_code,
	shop_name,
	shop_address,
	station_code,
	station_name,
	receive_name,
	receive_phone_number,
	receive_province_code,
	receive_province_name,
	receive_city_code,
	receive_city_name,
	receive_area_code,
	receive_area_name,
	receive_address,
	delivery_code,
	delivery_name,
	settlement_dc,
	settlement_dc_name,
	status,
	wave_code,
	distribute_shortage_flag,
	all_received_flag,
	print_times,
	packages_number,
	link_operate_order_no,
	origin_order_no,
	link_in_out_order_no,
	link_order_no,
	external_order_no,
	send_time,
	auto_status,
	sale_channel,
	compensation_type,
	plan_date,
	order_type,
	finish_time,
	create_time,
	create_by,
	update_time,
	update_by,
	sdt
from
	csx_dw.wms_shipped_order_m
where
	sdt> = '20200301' and shipped_location_code='W0A3' and status =7;

select * from csx_ods.source_wms_r_d_storage_location where warehouse_code = 'W0A3' and sdt= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),
		1)),
		'-',
		'');
		
	select * from csx_dw.goods_m where sdt='20200317';
	
select send_time,receive_city_code,receive_city_name,
COUNT(DISTINCT customer_code)as cust_num,
COUNT(DISTINCT order_no)as order_num,
COUNT(DISTINCT goods_code)as  goods_num,
sum(shipped_qty)as qty,
sum(shipped_qty*price)as  shipped_amt,
COUNT(DISTINCT case when division_code in ('12','13','14') then goods_code end ) as food_goods_num, --食百SKU数
COUNT(DISTINCT case when division_code in ('12','13','14') then order_no end ) as food_order_num,	--食百订单数
sum(case when division_code in ('12','13','14') then shipped_qty end ) as food_qty,	--食百出库数量
sum(case when division_code in ('12','13','14') then shipped_qty*price end ) as food_amt	--食百出库额
from csx_dw.wms_shipped_order;

refresh  csx_dw.wms_shipped_order ;
select * from csx_ods.source_wms_r_d_storage_location;

select * from csx_dw.wms_shipped_order where sdt>='20200301' and status in (7,8) and shipped_location_code='W0H4' and shipped_type like '%采购出库%';

SELECT shipped_type,send_date,shipped_location_code,shipped_location_name,source_system,cust_num,order_num,goods_num,qty,shipped_amt,food_goods_num,food_order_num,food_qty,food_amt,sdt
FROM csx_dw.wms_shipped_day_report;



	select
		DISTINCT wms_order_type
	from
		csx_ods.source_wms_r_d_bills_config
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),
		1)),
		'-',
		'')
		--and system_code = 'CG'
		AND order_type_code like 'OU%';
		
	
	select * from csx_dw.csx_shop