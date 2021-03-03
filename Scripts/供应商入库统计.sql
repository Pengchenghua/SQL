
--供应商入库情况 7月1号20201112

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
	and business_type_code !='02'
	and entry_type ='采购入库'
	and receive_qty !=0
	and receive_status =2
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
and business_type_code ='05'
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


--- 统计供应商数

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
	and business_type_code !='02'
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
and business_type_code ='05'
and shipped_type='采购出库'
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


 select
	receive_location_code dc_code ,
	supplier_code,
	supplier_name ,
	goods_code,
	goods_name ,
	sum(amount)amount,
	sum(receive_qty)qty,
	0 shipped_qty ,
	0 shipped_amt,
	entry_type as type ,
	business_type  
from
	csx_dw.wms_entry_order a 
	join 
	(select location_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current' and dist_code='32') b on a.receive_location_code=b.location_code 
where
	sdt >= '20200701'
	and sdt<'20201113'
	and business_type_code !='02'
	and entry_type ='采购入库'
	and receive_qty !=0
	and division_code in ('10','11')
	--and receive_status =2
	--and supplier_code like'G%'	
group by 
	receive_location_code ,
	supplier_code,
	supplier_name ,
	goods_code,
	goods_name,
	entry_type ,
	business_type;
	
select
	*
from
	csx_dw.dws_wms_r_d_entry_order_all_detail
where 1=1
	-- goods_code = '266'
	and receive_location_code = 'W039'
	and sdt >= '20200701'
	and sdt <= '20201113'
	and supplier_code ='C05013';
	

 select
--	receive_location_code dc_code ,
--	supplier_code,
	--supplier_name ,
	goods_code,
	goods_name ,
	sum(amount)amount,
	sum(receive_qty)qty
from
	csx_dw.wms_entry_order
where
	sdt >= '20200701'
	and sdt<'20201113'
	and business_type_code !='02'
	and entry_type ='采购入库'
	and receive_qty !=0
	and receive_status =2
	and division_code ='15'
	--and supplier_code like'G%'	
group by 
	-- receive_location_code ,
	supplier_code,
	supplier_name ,
	goods_name ,
	goods_code;
	


select
    dist_code,
    dist_name,
	bd_id,
	--count(distinct supplier_code) supplier_num,
	count(DISTINCT goods_code ) as sku,
	sum(amount)/10000 amt,
	sum(qty)qty,
	sum(shipped_qty) shipped_qty ,
	sum(shipped_amt)/10000 shipped_amt,
	sum(qty)+coalesce(sum(shipped_qty),0) as receive_qty ,
	(sum(amount)+coalesce(sum(shipped_amt),0))/10000 as receive_amt
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
	and business_type_code !='02'
	and entry_type ='采购入库'
	and receive_qty !=0
	and receive_status=2
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
and business_type_code ='05'
and shipped_type='采购出库'
and shipped_qty!=0
group by 
	shipped_location_code,
	supplier_code,
	goods_code,
	shipped_type,
	business_type
) a 
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current' and dist_code    in('35','34')) b on a.dc_code=b.location_code 
join 
(select vendor_id ,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code=c.vendor_id
join 
(select goods_id,case when division_code in ('10','11') then '生鲜' when  division_code in ('12','13','14') then '食百' else '易耗' end bd_id
	from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') d on a.goods_code=d.goods_id
group by  dist_code,
    dist_name,
	bd_id
	order by dist_code ,case when bd_id ='生鲜' then 1 when bd_id='食百' then 2 else 3 end 
;


select shipped_location_code dc_code,
	supplier_code,
	goods_code,
	0 amount,
	0 qty,
	sum(shipped_qty)*-1 shipped_qty,
	sum(amount)*-1 shipped_amt,
		shipped_type_code ,

	shipped_type  type ,
	business_type 
from csx_dw.wms_shipped_order 
where send_sdt>='20200701'
and send_sdt<'20201113'
-- and business_type_code ='05'
and shipped_type='采购出库'
and shipped_qty!=0
and supplier_code ='G2204'
group by 
	shipped_location_code,
	supplier_code,
	goods_code,
	shipped_type_code ,
	shipped_type,
	business_type
