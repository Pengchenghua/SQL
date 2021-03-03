




--金龙鱼+元宝、鲁花、福临门+福掌柜+福之泉+东海明珠

select
	sdt,
	dist_code ,
	dist_name ,
	business_type_name,
	order_code,
	p.shop_name,
	dc_code,
	goods_code,
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
	supplier_code,
	vendor_name,
	price,
	receive_qty,
    amount
from
(
	select
		sdt,
		case when business_type_code like 'ZN0%' then '供应商配送'
 		when business_type_code like 'ZC0%' then '云超配送'
 		else business_type end business_type_name ,
		e.order_code ,
		e.receive_location_code  as dc_code,
		goods_code ,
		b.goods_name,
		unit_name,
		brand_name,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		b.category_small_code,
		b.category_small_name,
		supplier_code,
		price ,
		sum(receive_qty) as receive_qty,
		sum(price*receive_qty) as amount
	from
		csx_dw.wms_entry_order e
	join 
	(
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
		category_small_name
	from
		csx_dw.dws_basic_w_a_csx_product_m a
	where
		sdt = 'current'
		and classify_middle_code in ('B0602',
		'B0603',
		'B0102')
		and (goods_name like '%金龙鱼%'
		or goods_name like '%元宝%'
		or goods_name like '%鲁花%'
		or goods_name like '%福临门%'
		or goods_name like '%福掌柜%'
		or goods_name like '%福之泉%'
		or goods_name like '%东海明珠%'
		or goods_name like '%海天%' ) )as b on e.goods_code=b.goods_id
	where
		sdt >= '20200101'
		and sdt <= '20201231'
		and (entry_type_code like 'P%' or  business_type_code in ('ZN01','ZN02','ZN03','ZC01'))
		group by 
		case when business_type_code like 'ZN0%' then '供应商配送'
 		when business_type_code like 'ZC0%' then '云超配送'
 		else business_type end ,
 		sdt,
		e.order_code ,
		e.receive_location_code ,
		goods_code ,
		b.goods_name,
		unit_name,
		brand_name,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		b.category_small_code,
		b.category_small_name,
		supplier_code,
		price) as b  
left join 
(select s.vendor_id,s.vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m s where sdt='current')as  s on b.supplier_code=s.vendor_id 
left join 
(select p.location_code ,p.shop_name,dist_code ,dist_name  from csx_dw.csx_shop p where sdt='current' )p on b.dc_code=p.location_code