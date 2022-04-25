
-- 指定供应商入库
select substr(sdt,1,4)years,substr(sdt,1,6)mon,company_code,receive_location_code ,shop_name,supplier_code,supplier_name,goods_code ,
	goods_name,
	sum(receive_qty)qty ,sum(amount)amount 
from csx_dw.wms_entry_order a 
join 
(select company_code,location_code,shop_name from csx_dw.csx_shop where sdt='current' and company_code ='2304') b on a.receive_location_code =b.location_code
where sdt>='20190101' and supplier_code  in ('B10008','20020295','20020588') 
	and a.receive_status =2
group by 
substr(sdt,1,4),substr(sdt,1,6),company_code,receive_location_code ,shop_name,supplier_code,supplier_name,goods_code ,goods_name
;

-- 指定供应商退货
-- 指定供应商入库
select substr(sdt,1,4)years,substr(sdt,1,6)mon,company_code,
	shipped_location_code ,
	b.shop_name,
	supplier_code,
	supplier_name,goods_code ,
	goods_name,
	sum(shipped_qty)qty ,sum(amount)amount 
from csx_dw.wms_shipped_order a 
join 
(select company_code,location_code,shop_name from csx_dw.csx_shop where sdt='current' and company_code ='2304') b on a.shipped_location_code =b.location_code
where sdt>='20190101' and supplier_code  in ('B10008','20020295','20020588') 
	-- and a.status =7
group by 
substr(sdt,1,4),substr(sdt,1,6),company_code,shipped_location_code ,b.shop_name,supplier_code,supplier_name,goods_code ,goods_name
;