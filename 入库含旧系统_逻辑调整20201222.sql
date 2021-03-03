set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- set mapreduce.job.queuename=caishixian;
set hive.support.quoted.identifiers=none;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode =20000;
set mapreduce.job.reduces = 80;


insert overwrite table csx_dw.wms_entry_order partition (sdt,sys)
select
	a.id                         ,
	order_code                 ,
	batch_code                 ,
	goods_code                 ,
	bar_code as goods_bar_code             ,
	goods_name                 ,
	division_code              ,
	division_name              ,
	department_id              ,
	department_name            ,
	category_large_code        ,
	category_large_name        ,
	category_middle_code       ,
	category_middle_name       ,
	category_small_code        ,
	category_small_name        ,
	unit                       ,
	produce_date               ,
	plan_qty                   ,
	receive_qty                ,
	shipped_qty                ,
	shelf_qty                  ,
	pass_qty                   ,
	reject_qty                 ,
	price                      ,
	add_price_percent          ,
	amount                     ,
	direct_flag                ,
	direct_price               ,
	direct_amount              ,
	shipper_code               ,
	shipper_name               ,
	supplier_code              ,
	supplier_name              ,
	send_location_code         ,
	send_location_name         ,
	plan_receive_date          ,
	return_flag                ,
	super_class                ,
	receive_location_code      ,
	receive_location_name      ,
	receive_area_code          ,
	receive_area_name          ,
	receive_store_location_code,
	receive_store_location_name,
	shelf_store_location_type  ,
	shelf_area_code            ,
	shelf_area_name            ,
	shelf_store_location_code  ,
	shelf_store_location_name  ,
	receive_status             ,
	shelf_status               ,
	all_receive_flag           ,
	all_shelf_flag             ,
	print_times                ,
	link_operate_order_code    ,
	origin_order_code          ,
	link_order_code            ,
	receive_time               ,
	close_time                 ,
	close_by                   ,
	auto_status                ,
	sale_channel               ,
	compensation_type          ,
	outside_order_code         ,
	settlement_dc              ,
	settlement_dc_name         ,
	run_type                   ,
	case when a.sys ='old' and a.business_type_code in ('ZN01','ZN02','ZN03','ZX05') then 'P01' 
	     when a.sys ='old' and a.business_type_code like 'ZU0%' then 'T06'
	     when a.sys ='old' and a.business_type_code in ('ZC01','ZC02','ZCR1','ZCR2') then 'P02'
	     when a.sys ='old' and a.business_type_code in('ZX04','ZXR4') then 'T03'
	    else  a.entry_type_code 
    end  entry_type_code ,
    case when a.sys ='old' and (a.business_type_code like 'ZN0%' or a.business_type_code ='ZX05') then '采购入库' 
	     when a.sys ='old' and a.business_type_code like 'ZU0%' then '调拨入库'
	     when a.sys ='old' and a.business_type_code like 'ZC0%' then '采购入库'
	     when a.sys ='old' and a.business_type_code like 'ZX04' then '申偿入库'
	    else  wms_order_type 
    end  as entry_type ,
    case when a.sys= 'old' and a.business_type_code like 'ZN0%' then '供应商配送' 
	     when a.sys= 'old' and a.business_type_code like 'ZU0%' then '仓间调拨入库'
	     when a.sys= 'old' and a.business_type_code like 'ZC0%' then '云超配送'
	     when a.sys= 'old' and a.business_type_code like 'ZX0%' then '申偿入库'
	    else  a.business_type_code 
    end  business_type_code ,
    case when a.business_type_code like 'ZN0%' then '采购入库' when a.business_type_code like 'ZNR%' then '退货出库'
        when (a.business_type_code like 'ZU0%' OR a.business_type_code like 'ZC0%' ) then '调拨入库(old)'
        when (a.business_type_code like 'ZUR%' OR a.business_type_code like 'ZCR%' ) then '返配入库(old)' 
        when (a.business_type_code like 'ZX%') then '申偿入库(old)' 
        else c.business_type    end  business_type         ,
	assess_type                ,
	assess_type_name           ,
	tax_type                   ,
	tax_rate                   ,
	tax_code                   ,
	price_type                 ,
	source_system              ,
	a.create_time                ,
	a.create_by                  ,
	a.update_time                ,
	a.update_by                  ,
	a.sdt                        ,
	sys
from
(select
	id                         ,
	order_code                 ,
	batch_code                 ,
	goods_code                 ,
	produce_date               ,
	plan_qty                   ,
	receive_qty                ,
	shipped_qty                ,
	shelf_qty                  ,
	pass_qty                   ,
	reject_qty                 ,
	price                      ,
	add_price_percent          ,
	coalesce(price*receive_qty,0) as amount                     ,
	direct_flag                ,
	direct_price               ,
	direct_amount              ,
	shipper_code               ,
	shipper_name               ,
	regexp_replace(supplier_code,'(^0*)','') as supplier_code              ,
	supplier_name              ,
	send_location_code         ,
	send_location_name         ,
	plan_receive_date          ,
	return_flag                ,
	super_class                ,
	receive_location_code      ,
	receive_location_name      ,
	receive_area_code          ,
	receive_area_name          ,
	receive_store_location_code,
	receive_store_location_name,
	shelf_store_location_type  ,
	shelf_area_code            ,
	shelf_area_name            ,
	shelf_store_location_code  ,
	shelf_store_location_name  ,
	receive_status             ,
	shelf_status               ,
	all_receive_flag           ,
	all_shelf_flag             ,
	print_times                ,
	link_operate_order_code    ,
	origin_order_code          ,
	link_order_code            ,
	case when sys='old' then from_unixtime(unix_timestamp(receive_time,'yyyyMMdd'),'yyyy-MM-dd HH:mm:ss') else receive_time end       as receive_time  ,
	close_time                 ,
	close_by                   ,
	auto_status                ,
	sale_channel               ,
	compensation_type          ,
	outside_order_code         ,
	settlement_dc              ,
	settlement_dc_name         ,
	run_type                   ,
	entry_type   as entry_type_code               ,
	business_type    as 	business_type_code           ,
	assess_type                ,
	assess_type_name           ,
	tax_type                   ,
	tax_rate                   ,
	tax_code                   ,
	price_type                 ,
	case when sys='old' then 'SAP' else 	source_system    end     source_system      ,
	create_time                ,
	create_by                  ,
	update_time                ,
	update_by                  ,
	sdt                        ,
	sys
from
	csx_dw.dws_wms_r_d_entry_order_all_detail 
where 1=1 
and  ((sdt>regexp_replace(date_sub(current_date,90),'-','') 
		and  sdt<regexp_replace(current_date,'-','') )
	or sdt='19990101')
	 --   and order_code ='RH20112400000494'
)a 
 join 
(SELECT goods_id,
	    goods_name,
	    bar_code,
        unit_name unit,
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
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') b on a.goods_code=b.goods_id 
left join 
(
	select
		business_type_code,
		business_type,
		type_code,
		wms_order_type
	from
		csx_ods.source_wms_r_d_bills_config
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
		and wms_order_type like '%入库%'
		and entity_flag=0
 ) c  on a.business_type_code= c.business_type_code and a.entry_type_code=c.type_code
;
