



--旧系统 增加  and (entry_type like 'P%'	or business_type in ('ZN01','ZN02','ZN03','ZC01'))

--金龙鱼+元宝、鲁花、福临门+福掌柜+福之泉+东海明珠
 select
	mon,
	dist_code ,
	dist_name ,
	coalesce(business_type,business_type_name)business_type_name,
	p.shop_name,
	dc_code,
	goods_code,
	goods_name,
	unit_name,
	brand_name,
	category_large_code ,
	category_large_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	regexp_replace(b.supplier_code,'^0*','')supplier_code,
	vendor_name,
	 receive_qty,
	amount,
	shipped_qty ,
	shipped_amt
from
	(
	select
		 mon,
		  business_type_name ,
	   dc_code,
		goods_code ,
		goods_name,
		unit_name,
		brand_name,
		category_large_code ,
		category_large_name ,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		category_small_code,
		category_small_name,
		supplier_code,
		sum(receive_qty) as receive_qty,
		sum(amount) as amount,
		sum(shipped_qty) shipped_qty ,
		sum(shipped_amt) shipped_amt
		from
(
	select
		substr(sdt,1,6)mon,
		case
			when business_type like 'ZN0%' then '供应商配送'
			when business_type like 'ZC0%' then '云超配送'
			else business_type
		end business_type_name ,
		e.receive_location_code as dc_code,
		goods_code ,
		b.goods_name,
		unit_name,
		brand_name,
		b.category_large_code ,
		b.category_large_name ,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		b.category_small_code,
		b.category_small_name,
		supplier_code,
		sum(receive_qty) as receive_qty,
		sum(price*receive_qty) as amount,
		0 shipped_qty ,
		0 shipped_amt
	from
		csx_dw.dws_wms_r_d_entry_order_all_detail e
	join (
		select
			goods_id,
			goods_name,
			unit_name,
			brand_name,
			classify_large_code,
			classify_large_name,
			classify_middle_code,
			classify_middle_name,
			classify_small_code,
			classify_small_name,
			category_small_code,
			category_small_name,
			division_code ,
			department_id ,
			department_name ,
			category_large_code ,
			category_large_name
		from
			csx_dw.dws_basic_w_a_csx_product_m a
		where
			sdt = 'current'
			and division_code = '15')as b on
		e.goods_code = b.goods_id
	where
		sdt >= '20201201'
		and sdt <= '20201231'
		and (entry_type like 'P%'
		or business_type in ('ZN01','ZN02','ZN03','ZC01'))
	group by
		case
			when business_type like 'ZN0%' then '供应商配送'
			when business_type like 'ZC0%' then '云超配送'
			else business_type
		end ,
		substr(sdt,	1,6),
		e.receive_location_code ,
		goods_code ,
		b.goods_name,
		unit_name,
		brand_name,
		category_large_code ,
		category_large_name ,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		b.category_small_code,
		b.category_small_name,
		supplier_code
	union all 
	
	select
		substr(sdt,1,6)mon,
		case
			when business_type like 'ZN0%' then '供应商配送'
			when business_type like 'ZC0%' then '云超配送'
			else business_type
		end business_type_name ,
		e.shipped_location_code as dc_code,
		goods_code ,
		b.goods_name,
		unit_name,
		brand_name,
		b.category_large_code ,
		b.category_large_name ,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		b.category_small_code,
		b.category_small_name,
		supplier_code,
		0 as receive_qty,
		0 as amount,
		sum(e.shipped_qty) as shipped_qty,
		sum(e.shipped_qty*price) as shippde_amt
	from
		csx_dw.dws_wms_r_d_shipped_order_all_detail e
	join (
		select
			goods_id,
			goods_name,
			unit_name,
			brand_name,
			classify_large_code,
			classify_large_name,
			classify_middle_code,
			classify_middle_name,
			classify_small_code,
			classify_small_name,
			category_small_code,
			category_small_name,
			division_code ,
			department_id ,
			department_name ,
			category_large_code ,
			category_large_name
		from
			csx_dw.dws_basic_w_a_csx_product_m a
		where
			sdt = 'current'
			and division_code = '15')as b on
		e.goods_code = b.goods_id
	where
		sdt >= '20201201'
		and sdt <= '20201231'
		and (e.shipped_type like 'P%'
		or business_type in ('ZNR1','ZNR2',	'ZNR3',	'ZCR1'))
	group by
		case
			when business_type like 'ZN0%' then '供应商配送'
			when business_type like 'ZC0%' then '云超配送'
			else business_type
		end ,
		substr(sdt,	1,6),
		e.shipped_location_code ,
		goods_code ,
		b.goods_name,
		unit_name,
		brand_name,
		category_large_code ,
		category_large_name ,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		b.category_small_code,
		b.category_small_name,
		supplier_code
		
)t group by
		business_type_name ,
		mon,
		dc_code ,
		goods_code ,
		goods_name,
		unit_name,
		brand_name,
		category_large_code ,
		category_large_name ,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		category_small_code,
		category_small_name,
		supplier_code) as b
left join (
	select
		w.business_type ,
		w.business_type_code ,
		w.wms_order_type,
		type_code
	from
		csx_ods.source_wms_r_d_bills_config w
	where
		sdt = '20210106'
		and wms_order_type like '采购入库%') w on b.business_type_name=w.business_type_code 
left join (
	select
		s.vendor_id,
		s.vendor_name
	from
		csx_dw.dws_basic_w_a_csx_supplier_m s
	where
		sdt = 'current')as s on
	regexp_replace(b.supplier_code,'^0*','') = s.vendor_id
left join (
	select
		p.location_code ,
		p.shop_name,
		dist_code ,
		dist_name
	from
		csx_dw.csx_shop p
	where
		sdt = 'current' )p on
	b.dc_code = p.location_code