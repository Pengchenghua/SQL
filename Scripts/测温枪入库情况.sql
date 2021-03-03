
select
	a.sdt,
	dist_code,
	dist_name,
	receive_location_code,
	shop_name ,
	if (supplier_code ='' ,send_location_code,supplier_code ) as supplier_code_01,
	if (supplier_name='' ,send_location_name,supplier_name )as supplier_name_01,
	goods_code,
	b.goods_name,
	brand_name,
	price,
	sum(receive_qty),
	sum(receive_qty * price) receive_amt,
	business_type 
from
	csx_dw.wms_entry_order a
join (
	select
		goods_id,
		goods_name,
		brand_name
	from
		csx_dw.dws_basic_w_a_csx_product_m
	where
		goods_name like '%温枪%'
		and sdt = 'current') b on
	a.goods_code = b.goods_id
join (
	select
		*
	from
		csx_dw.csx_shop
	where
		sdt = 'current') c on
	a.receive_location_code = c.location_code
where
	a.sdt >= '20200101'
	and a.sdt <= '20200430'
--	and business_type
group by
	dist_code,
	dist_name,
	receive_location_code,
	shop_name ,
	goods_code,
	b.goods_name,
	brand_name,
	price,
	a.sdt,
business_type,
if (supplier_code ='' ,send_location_code,supplier_code ) ,
if (supplier_name='' ,send_location_name,supplier_name );	
