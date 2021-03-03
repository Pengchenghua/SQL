
--云超配送入库情况 7月1号20201112


select dist_code,
	dist_name,	
	bd_id,
	supplier_code,
	vendor_name,
	--supplier_name ,
	count(DISTINCT goods_code ) as sku,
	sum(amount)amt,
	sum(qty)qty,
	sum(shipped_qty) shipped_qty ,
	sum(shipped_amt) shipped_amt,
	sum(qty)-coalesce(sum(shipped_qty),0) as receive_qty ,
	sum(amount)-coalesce(sum(shipped_amt),0) as receive_amt
from (
 select
	receive_location_code dc_code ,
	supplier_code,
	--supplier_name ,
	goods_code,
	sum(amount)amount,
	sum(receive_qty)qty,
	0 shipped_qty ,
	0 shipped_amt,
	entry_type as type ,
	business_type  
from
	csx_dw.wms_entry_order
where
	sdt >= '20200701'
	and sdt<'20201113'
	and business_type_code ='02'
	and entry_type ='采购入库'
	and receive_qty !=0
	--and supplier_code like'G%'	
group by 
	receive_location_code ,
	supplier_code,
	supplier_name ,
	goods_code,
	entry_type ,
	business_type
union all 
select shipped_location_code dc_code,
	supplier_code,
	goods_code,
	0 amount,
	0 qty,
	sum(shipped_qty)*-1 shipped_qty,
	sum(amount)*-1 shipped_amt,
	shipped_type  type ,
	business_type 
from csx_dw.wms_shipped_order 
where send_sdt>='20200701'
and send_sdt<'20201113'
and business_type_code ='70'
and shipped_type ='采购出库'
and shipped_qty!=0
group by 
	shipped_location_code,
	supplier_code,
	goods_code,
	shipped_type,
	business_type
) a 
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current') b on a.dc_code=b.location_code 
join 
(select vendor_id ,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code=c.vendor_id
join 
(select goods_id,case when division_code in ('10','11') then '生鲜' when  division_code in ('12','13','14') then '食百' else '易耗' end bd_id
	from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') d on a.goods_code=d.goods_id
group by 
	dist_code,
	dist_name,	
	supplier_code,
	vendor_name,
	bd_id
;


-- 云超统计

select dist_code,
	dist_name,	
	bd_id,
	count(distinct supplier_code) supplier_num,
	count(DISTINCT goods_code ) as sku,
	sum(amount)amt,
	sum(qty)qty,
	sum(shipped_qty) shipped_qty ,
	sum(shipped_amt) shipped_amt,
	sum(qty)-coalesce(sum(shipped_qty),0) as receive_qty ,
	sum(amount)-coalesce(sum(shipped_amt),0) as receive_amt
from (
 select
	receive_location_code dc_code ,
	supplier_code,
	--supplier_name ,
	goods_code,
	sum(amount)amount,
	sum(receive_qty)qty,
	0 shipped_qty ,
	0 shipped_amt,
	entry_type as type ,
	business_type  
from
	csx_dw.wms_entry_order
where
	sdt >= '20200701'
	and sdt<'20201113'
	and business_type_code ='02'
	and entry_type ='采购入库'
	and receive_qty !=0
	--and supplier_code like'G%'	
group by 
	receive_location_code ,
	supplier_code,
	supplier_name ,
	goods_code,
	entry_type ,
	business_type
union all 
select shipped_location_code dc_code,
	supplier_code,
	goods_code,
	0 amount,
	0 qty,
	sum(shipped_qty)*-1 shipped_qty,
	sum(amount)*-1 shipped_amt,
	shipped_type  type ,
	business_type 
from csx_dw.wms_shipped_order 
where send_sdt>='20200701'
and send_sdt<'20201113'
and business_type_code ='70'
and shipped_type ='采购出库'
and shipped_qty!=0
group by 
	shipped_location_code,
	supplier_code,
	goods_code,
	shipped_type,
	business_type
) a 
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current') b on a.dc_code=b.location_code 
join 
(select vendor_id ,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code=c.vendor_id
join 
(select goods_id,case when division_code in ('10','11') then '生鲜' when  division_code in ('12','13','14') then '食百' else '易耗' end bd_id
	from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') d on a.goods_code=d.goods_id
group by 
	dist_code,
	dist_name,
	bd_id
;