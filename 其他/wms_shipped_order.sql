set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.job.queuename=caishixian;
set hive.support.quoted.identifiers=none;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode =20000;


-- 过滤彩食鲜物流入库数据
drop table b2b_tmp.tmp_ship_order_sap_v9;
create temporary table b2b_tmp.tmp_ship_order_sap_v9
as 
select 
  t.*,
  t1.shop_name as shop_out_name
from 
(
  select 
     *
  from csx_dw.shop_m 
  where sdt = 'current' and sales_belong_flag in ('4_企业购', '5_彩食鲜')
)t1 
 join 
(
  select 
    *
  from b2b.ord_orderflow_t 
  where sdt< '20200101'and sdt>='20190601'
    and length(shop_id_out) = 4 
)t on t.shop_id_out = t1.shop_id;

insert overwrite table csx_dw.wms_shipped_order partition(sdt,send_sdt)
select 
	a.id,
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
	b.division_code,
	b.division_name,
	b.department_id,
	b.department_name,
	b.category_large_code,
	b.category_large_name,
	b.category_middle_code,
	category_middle_name,
	category_small_code,
	category_small_name,
   unit_name	unit,
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
	a.shipped_type_code,
	c.wms_order_type as shipped_type,
	a.business_type_code,
	c.business_type,
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
	a.create_time,
	a.create_by,
	a.update_time,
	a.update_by,
	'new'sys,
	a.sdt,
	regexp_replace(substr(a.send_time, 1, 10), '-', '') as send_sdt
from
(select
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
	shipped_type as shipped_type_code,
	business_type as business_type_code,
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
	--csx_dw.wms_shipped_order_m
	csx_dw.dwd_wms_r_d_shipped_order_detail
where  sdt< '20190801' and sdt>='20190601' and status<>9)a
left join 
(SELECT goods_id,
        unit_name,
       division_code,
       division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       department_id,
       department_name
FROM csx_dw.goods_m
WHERE sdt='current') b on a.goods_code=b.goods_id 
left join 
(
	select
		*
	from
		csx_ods.source_wms_r_d_bills_config
	where
		sdt = regexp_replace(date_sub(current_date,1),'-','')
 ) c  on a.business_type_code= c.business_type_code and a.shipped_type_code=c.type_code
 union all 
 select
	concat('O',pur_doc_id, t.sdt,goodsid) as id                ,
	''                                    as batch_id          ,
	''                                    as order_id          ,
	''                                    as header_id         ,
	''                                    as batch_code        ,
	pur_doc_id                            as order_no          ,
	pur_app_id                            as link_scm_order_no ,
	''                                    as split_order_no    ,
	goodsid                               as goods_code        ,
	bar_code as goods_bar_code    ,
	goods_name                               goods_name        ,
	division_code                                              ,
	division_name                                              ,
	department_id                                              ,
	department_name                                            ,
	category_large_code                                        ,
	category_large_name                                        ,
	category_middle_code                                       ,
	category_middle_name                                       ,
	category_small_code                                        ,
	category_small_name                                        ,
	unit_name                                                              as unit                                          ,
	unit_name                                                              as sale_unit                                     ,
	''                                                                     as split_group_code                              ,
	''                                                                     as split_group_name                              ,
	t.purchase_qty                                                         as plan_qty                                      ,
	t.pur_order_qty                                                        as order_shipped_qty                             ,
	t.pur_qty_out                                                          as shipped_qty                                   ,
	qty_picked                                                             as picking_side_qty                              ,
	0                                                                      as store_location_qty                            ,
	t.recpt_qty                                                            as receive_qty                                   ,
	0                                                                      as pass_qty                                      ,
	0                                                                      as reject_qty                                    ,
	cast(pur_doc_net_price*(1+coalesce(taxrate, 0)/100) as decimal(10, 6)) as price                                         ,
	0                                                                      as add_price_percent                             ,
	t.tax_pur_val_out                                                      as amount                                        ,
	0                                                                      as direct_price                                  ,
	0                                                                      as direct_amount                                 ,
	t.shop_id_out                                                          as shipped_location_code                         ,
	t.shop_out_name                                                        as shipped_location_name                         ,
	''                                                                     as shipped_area_code                             ,
	''                                                                     as shipped_area_name                             ,
	''                                                                     as shipped_store_location_code                   ,
	''                                                                     as shipped_store_location_name                   ,
	1                                                                      as all_send_flag                                 ,
	''                                                                     as running_model                                 ,
	''                                                                     as picking_type                                  ,
	''                                                                     as tc_picking_flag                               ,
	''                                                                     as remark                                        ,
	''                                                                     as specs_remark                                  ,
	''                                                                     as handle_remark                                 ,
	99                                                                      as  run_type                                      ,
	''                                                                     as assess_type                                   ,
	''                                                                     as assess_type_name                              ,
	22                                                                     as   tax_type                                      ,
	taxrate                                                                as tax_rate                                      ,
	''                                                                     as tax_code                                      ,
	99                                                                     as price_type                                    ,
	'SAP'                                                                  as source_system                                 ,
	1                                                                      as super_class                                   ,
	'999'                                                                  as shipped_type_code                             ,
	'未定义(old)'                                                          as shipped_type                                  ,
	case
		when t.pur_doc_type like 'Z%'
			then substr(t.pur_doc_type,1,3)
	end as business_type_code ,
	case
		when t.pur_doc_type like 'ZN0%'
			then '采购出库(old)'
		when t.pur_doc_type like 'ZNR%'
			then '退货出库(old)'
		when (
				t.pur_doc_type    like 'ZU0%'
				OR t.pur_doc_type like 'ZC0%'
			)
			then '调拨出库(old)'
		when (
				t.pur_doc_type    like 'ZUR%'
				OR t.pur_doc_type like 'ZCR%'
			)
			then '返配出库(old)'
		when (
				t.pur_doc_type like 'ZX%'
			)
			then '申偿出库(old)'
	end                                                                                  as  business_type               ,
	''                                                                                     as return_flag              ,
	''                                                                                     as   direct_flag              ,
	'YHCSX'                                                                                as shipper_code             ,
	'永辉彩食鲜'                                                                                as shipper_name             ,
	t.vendor_id                                                                            as supplier_code            ,
	t3.vendor_name                                                                         as supplier_name            ,
	t.shop_id_in                                                                            as  receive_location_code    ,
	t5.shop_name                                                                            as  receive_location_name    ,
	transfer_order_supp_loc                                                                as transfer_location_code   ,
	t7.shop_name                                                                            as  transfer_location_name   ,
	t.acct_id                                                                              as customer_code            ,
	T6.SHOP_NAME                                                                           as customer_name            ,
	''                                                                                     as sub_customer_code        ,
	''                                                                                     as sub_customer_address     ,
	''                                                                                     as shop_type                ,
	''                                                                                     as shop_code                ,
	''                                                                                     as shop_name                ,
	''                                                                                     as shop_address             ,
	''                                                                                     as station_code             ,
	''                                                                                     as station_name             ,
	''                                                                                     as receive_name             ,
	''                                                                                     as receive_phone_number     ,
	''                                                                                     as receive_province_code    ,
	''                                                                                     as receive_province_name    ,
	''                                                                                     as receive_city_code        ,
	''                                                                                     as receive_city_name        ,
	''                                                                                     as receive_area_code        ,
	''                                                                                     as receive_area_name        ,
	''                                                                                     as receive_address          ,
	''                                                                                     as delivery_code            ,
	''                                                                                     as delivery_name            ,
	''                                                                                     as settlement_dc            ,
	''                                                                                     as settlement_dc_name       ,
	7                                                                                      as status                   ,
	''                                                                                     as wave_code                ,
	1                                                                                      as distribute_shortage_flag ,
	1                                                                                      as all_received_flag        ,
	0                                                                                      as print_times              ,
	0                                                                                      as packages_number          ,
	nvl(org_doc_id,'')                                                                     as link_operate_order_no    ,
	nvl(pur_doc_id_app,'')                                                                 as origin_order_no          ,
	nvl(org_doc_id,'')                                                                     as link_in_out_order_no     ,
	nvl(pur_app_id,'')                                                                     as link_order_no            ,
	''                                                                                     as external_order_no        ,
	from_unixtime(unix_timestamp(t.min_pstng_date_out,'yyyymmdd'),'yyyy-mm-dd 00:00:00.0') as send_time                ,
	0                                                                                      as auto_status              ,
	''                                                                                     as sale_channel             ,
	''                                                                                     as compensation_type        ,
	plan_delivery_date                                                                     as plan_date                ,
	ordertype                                                                              as order_type               ,
	from_unixtime(unix_timestamp(t.sdt,'yyyymmdd'),'yyyy-mm-dd 00:00:00.0')                as finish_time              ,
	''                                                                                     as create_time              ,
	''                                                                                     as create_by                ,
	''                                                                                     as update_time              ,
	''                                                                                     as update_by                ,
	'old'                                                                                  as sys                      ,
	t.sdt                                                                                                              ,
	t.sdt as send_sdt
from
	(
		select *
		from
			b2b_tmp.tmp_ship_order_sap_v9
		where
			shop_id_out<>'W098' and sdt< '20200801'
	)
	t
	left outer join
		(
			select  goods_id,goods_name,
			a.bar_code,
        unit_name,
       division_code,
       division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       department_id,
       department_name
			from
				csx_dw.goods_m a
			where
				sdt = 'current'
		)
		t2
		on
			t.goodsid = t2.goods_id
	left outer join
		(
			select    vendor_id,vendor_name
			from
				csx_dw.vendor_m
			where
				sdt = 'current'
		)
		t3
		on
			regexp_replace(t.vendor_id, '^0*', '') = t3.vendor_id
	left outer join
		(
			select    shop_id,shop_name
			from
				csx_dw.shop_m
			where
				sdt = 'current'
		)
		t4
		on
			t.shop_id_out = t4.shop_id
	left outer join
		(
			select    shop_id,shop_name
			from
				csx_dw.shop_m
			where
				sdt = 'current'
		)
		t5
		on
			t.shop_id_in = t5.shop_id
	left outer join
		(
			select    shop_id,shop_name
			from
				csx_dw.shop_m
			where
				sdt = 'current'
		)
		t6
		on
			t.acct_id = concat('S',t6.shop_id)
	left outer join
		(
			select    shop_id,shop_name
			from
				csx_dw.shop_m
			where
				sdt = 'current'
		)
		t7
		on
			t.transfer_order_supp_loc = t7.shop_id
;

