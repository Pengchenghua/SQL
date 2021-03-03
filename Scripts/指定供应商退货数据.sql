--出库

select
	substr(sdt,1,6)mon,
	receive_location_code ,
	b.shop_name ,
	goods_code ,
	goods_name ,
	supplier_code ,
	supplier_name ,
	sum(shipped_qty) shipped_qty ,
	sum(amount) amt
from
	csx_dw.wms_shipped_order a
	 join 
	(select location_code,shop_name,company_code from csx_dw.csx_shop where sdt='current' and company_code='2304') b on a.shipped_location_code =b.location_code
where
	supplier_code in (
		'20020588',
		'20020295',
		'B10008')
	and business_type like '%退供%'
	and send_sdt >='20200901'
	group by 
	substr(sdt,1,6),
	shipped_location_code ,
	b.shop_name ,
	goods_code ,
	goods_name ,
	supplier_code ,
	supplier_name 
	;
	

--入库
select
	substr(sdt,1,6)mon,
	receive_location_code ,
	b.shop_name ,
	goods_code ,
	goods_name ,
	supplier_code ,
	supplier_name ,
	sum(receive_qty) shipped_qty ,
	sum(amount) amt
from
	csx_dw.wms_entry_order a
	 join 
	(select location_code,shop_name,company_code from csx_dw.csx_shop where sdt='current' and company_code='2304') b on a.receive_location_code =b.location_code
where
	supplier_code in (
		'20020588',
		'20020295',
		'B10008')
	and business_type like '%供应商%'
	and sdt >='20200901'
	and receive_status =2
	group by 
	substr(sdt,1,6),
	receive_location_code ,
	b.shop_name ,
	goods_code ,
	goods_name ,
	supplier_code ,
	supplier_name 
	;