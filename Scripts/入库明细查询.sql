refresh  csx_dw.dws_wms_r_d_entry_order_all_detail;
select
	a.id                         ,
	order_code                 ,
	batch_code                 ,
	goods_code                 ,
	goods_bar_code             ,
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
	a.entry_type_code            ,
	wms_order_type  as entry_type        ,
	a.business_type_code         ,
	c.business_type              ,
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
	goods_bar_code             ,
	goods_name                 ,
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
	regexp_replace(supplier_code,'(^0)','') as supplier_code              ,
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
	entry_type   as entry_type_code               ,
	business_type    as 	business_type_code           ,
	assess_type                ,
	assess_type_name           ,
	tax_type                   ,
	tax_rate                   ,
	tax_code                   ,
	price_type                 ,
	source_system              ,
	create_time                ,
	create_by                  ,
	update_time                ,
	update_by                  ,
	sdt                        ,
	sys
from
	csx_dw.dws_wms_r_d_entry_order_all_detail where sdt<='20200331' and sdt>='20200301' )a 
left join 
(SELECT goods_id,
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
FROM csx_dw.goods_m
WHERE sdt='current') b on a.goods_code=b.goods_id 
left join 
(
	select
		*
	from
		csx_ods.source_wms_r_d_bills_config
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
 ) c  on a.business_type_code= c.business_type_code and a.entry_type_code=c.type_code
;


select distinct wms_order_type from csx_ods.source_wms_r_d_bills_config where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') and system_code='CG'AND order_type_code like 'IN%'
union all 
select '未定义(old)' 
order by wms_order_type desc;
select * from csx_dw.dwd_wms_r_d_entry_order_detail;

refresh  csx_dw.dws_wms_r_d_entry_order_all_detail;
select

	goods_code                 ,
	goods_bar_code             ,
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
	sum(receive_qty )qty               ,
	sum(amount)aamount                     ,
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
	a.entry_type_code            ,
	wms_order_type  as entry_type        ,
	a.business_type_code         ,
	c.business_type              ,
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
	goods_bar_code             ,
	goods_name                 ,
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
	regexp_replace(supplier_code,'(^0)','') as supplier_code              ,
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
	entry_type   as entry_type_code               ,
	business_type    as 	business_type_code           ,
	assess_type                ,
	assess_type_name           ,
	tax_type                   ,
	tax_rate                   ,
	tax_code                   ,
	price_type                 ,
	source_system              ,
	create_time                ,
	create_by                  ,
	update_time                ,
	update_by                  ,
	sdt                        ,
	sys
from
	csx_dw.dws_wms_r_d_entry_order_all_detail where sdt<='20200331' and sdt>='20200301' )a 
left join 
(SELECT goods_id,
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
FROM csx_dw.goods_m
WHERE sdt='current') b on a.goods_code=b.goods_id 
left join 
(
	select
		*
	from
		csx_ods.source_wms_r_d_bills_config
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
 ) c  on a.business_type_code= c.business_type_code and a.entry_type_code=c.type_code
where division_code in ('10','11');

refresh csx_dw.dws_wms_r_d_entry_order_all_detail;
select DISTINCT business_type,entry_type,SUBSTRING(order_code,1,2) from csx_dw.dws_wms_r_d_entry_order_all_detail where sdt<='20200331' and sdt>='20190301'and sys='old';

select DISTINCT SUBSTRING(pur_doc_id,1,2),pur_doc_type,order_type from b2b.ord_orderflow_t where pur_org like 'P6%' AND SDT>='20190101' ;

SUBSTRING(pur_doc_id,1,2) in ('47') then '调拨入库' when SUBSTRING(pur_doc_id,1,2) in ('48') then '配送入库';


select * from b2b.ord_orderflow_t where pur_org like 'P6%' AND SDT>='20190101' and pur_doc_type='ZC01' and order_type is 	NULL;

select DISTINCT shipped_location_code,shop_code from csx_dw.dwd_wms_r_d_shipped_order_detail ;

select * from csx_dw.dws_wms_r_d_shipped_order_all_detail where order_no='OY200401000826' ;

refresh csx_dw.wms_shipped_order ;
select * from csx_dw.wms_shipped_order  where sdt>='20200301' and sys='old';

refresh csx_dw.dws_sale_r_d_sale_item_m;
select
*
from csx_dw.dws_sale_r_d_sale_item_m;


SELECT
    
	receive_location_code dc_code,
	receive_location_name dc_name ,
	send_location_code as send_dc_code,
	send_location_name as send_dc_name,
	goods_code,
	goods_name,
	unit,
	sum(PRICE*receive_qty)/sum(receive_qty) as price,
	sum(plan_qty)plan_qty,
	sum(receive_qty)receive_qty,
	sum(PRICE*receive_qty)*1.00 as receive_amt,
	sum(amount) as amount
from
	csx_dw.wms_entry_order 
	where sdt='20200401'
group by receive_location_code,unit,
	receive_location_name  ,
	send_location_code,
	send_location_name ,
	goods_code,
	goods_name;
	

SELECT
	 dc_code,
	dc_name ,
	goods_code,
	goods_name,
	unit,
	division_code,
	division_name,
	category_large_code,
	category_large_name,
	category_small_code,category_small_name,
	price,
	plan_qty,
	receive_qty,
	receive_amt,
	amount,
	supplier_code,
	supplier_name,
	--b.business_type_code,
	send_dc_code,
	send_dc_name
from 
(SELECt
	receive_location_code dc_code,
	receive_location_name dc_name ,
	send_location_code as send_dc_code,
	send_location_name as send_dc_name,
	goods_code,
	goods_name,
	unit,
	division_code,
	division_name,
	category_large_code,
	category_large_name,
	category_small_code,category_small_name,
	sum(PRICE*receive_qty)/sum(receive_qty) as price,
	sum(plan_qty)plan_qty,
	sum(receive_qty)receive_qty,
	sum(PRICE*receive_qty)*1.00 as receive_amt,
	sum(amount) as amount,
	supplier_code,
	supplier_name
from
	csx_dw.wms_entry_order 
	where 
	sdt>='${sdate}' and sdt<='${edate}'
${if(len(tree)==0,"","and receive_location_code in ('"+SUBSTITUTE(tree,",","','")+"')")}
${if(len(tree_c)==0,"","and send_location_code in ('"+SUBSTITUTE(tree_c,",","','")+"')")}
${if(len(supplier)==0,"","and supplier_code in ('"+REPLACE(supplier,",","','")+"')")}
${if(len(entry_check) ==0,"","and entry_type in('"+entry_check+"') ")}
${if(len(busin_type) ==0,"","and business_type in('"+busin_type+"') ")}
${if(len(goodsid)==0,"","and goods_code in ('"+REPLACE(goodsid,",","','")+"')")}
	group by receive_location_code,unit,
	division_code,
	division_name,
	category_large_code,
	category_large_name,
	category_small_code,category_small_name,
	receive_location_name  ,
	send_location_code,
	send_location_name ,
	goods_code,
	goods_name,
	supplier_code,
	supplier_name
	) as a 
;

select DISTINCT entry_type,entry_type_code from csx_dw.wms_entry_order ;

select sdt,receive_location_code as dc_code,receive_location_name as dc_name ,
division_code,
division_name,
category_large_code,
category_large_name,
entry_type,
count(DISTINCT goods_code) sku,
sum(receive_qty)in_qty,
sum(receive_qty*price) amount
from csx_dw.wms_entry_order where sdt>='20200401'
group by sdt,receive_location_code ,receive_location_name ,
division_code,
division_name,
category_large_code,
category_large_name,
entry_type;

refresh csx_dw.wms_entry_order;


	select
		*
	from
		csx_ods.source_wms_r_d_bills_config
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','');
		
	select *
		from
			ods_ecc.ecc_ytbcustomer where sdt='20200402' and budat>='20200401' and prctr='W0A7';
			
		csx_dw.account_age_dtl_fct;
		
	
			select distinct
				shop_id,
				shop_name
			from
				dim.dim_shop
			where
				edate             ='9999-12-31'
				and sales_dist_new='610000';
			
			
			select
	a.bukrs comp_code                                                                ,--公司代码
	a.kunnr                                                                          ,--客户编码
	a.budat                                                                          ,--日期
	regexp_extract(a.prctr, '(0|^)([^0].*)',2) prctr                                 ,--利润中心代码
	e.shop_name                                                                      ,--利润中心名称
	a.dmbtr                                                                          ,--金额
	c.zterm                                                                          ,--帐期
	d.diff                                                                           ,--帐期天数
	concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))                              sdate,
	date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(d.diff,0)) edate,
	sdt
from
	(
		select *
		from
			ods_ecc.ecc_ytbcustomer
		where
			hkont like '1122%'
			and sdt  ='20200403'
			and budat>='20200401'
			and mandt='800'
			and prctr='W0A7'
	)
	a
	left join
		(
			select distinct
				shop_id,
				shop_name
			from
				dim.dim_shop
			where
				edate             ='9999-12-31'
				and sales_dist_new LIKE '6%'
		)
		e
		on
			regexp_extract(a.prctr, '(0|^)([^0].*)',2)=e.shop_id
	left join
		(
			select
				bukrs,
				kunnr,
				zterm
			from
				ods_ecc.ecc_knb1
			where
				kunnr not like 'S%'
			group by
				bukrs,
				kunnr,
				zterm
		)
		c
		on
			a.bukrs    =c.bukrs
			and a.kunnr=c.kunnr
	left join
		(
			select
				zterm,
				cast(ztag1 as int) diff
			from
				ods_ecc.ecc_t052
			where
				mandt='800'
		)
		d
		on
			c.zterm=d.zterm
;
	
			select
				bukrs,
				kunnr,
				zterm
			from
				ods_ecc.ecc_knb1
			WHERE=
				kunnr='0000103097'
			;
				
			
			
			select
				zterm,
				cast(ztag1 as int) diff
			from
				ods_ecc.ecc_t052
			where
				mandt='800'