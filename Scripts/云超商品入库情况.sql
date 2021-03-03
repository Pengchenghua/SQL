--业务逻辑清晰，20201218
-- 云超进货入库明细
-- 
-- ZN0  采购入库   
-- ZC、ZCR  跨公司调拨入库
-- ZU 、ZUR 公司内调拨

--
--返配申偿	ZXR4		F
--配送	ZU01		F
--退货	ZNR1		F
--直送	ZN01	0	F
--配送	ZU01	B	F
--配送	ZC01	B	F
--退货	ZNR1	B	F
--直送	ZN01	B	F
--配送	ZC01		F
--直送	ZN02		F
--退货	ZNR2		F
--直送	ZN01		F
--返配	ZCR1	B	F
--直通	ZC01	B	F
--配送申偿	ZX04	B	F
--返配	ZCR1		F
--直送	ZN01	-1	F
-- 新系统入库
 select
	substr(sdt,1,6) mon,
	dist_code,
	dist_name,
	dc_code,
	s.shop_name,
	goods_code ,
	p.goods_name,
	p.division_code,
	p.division_name,
	p.department_id,
	p.department_name,
	sum(receive_qty)receive_qty,
	sum(receive_amt)receive_amt,
	sum(shipped_qty)shipped_qty,
	sum(shipped_amt)shipped_amt,
	type
from
	(
	select
		sdt,
		receive_location_code as dc_code,
		goods_code ,
		sum(receive_qty)receive_qty,
		sum(price*receive_qty) receive_amt,
		0 shipped_qty ,
		0 shipped_amt ,
		'new' type
	from
		csx_dw.dws_wms_r_d_entry_detail 
	where
		sdt >= '20200101'
		and sdt <= '20201231'		
		and sys = 'new'
		-- and receive_location_code ='W0M4'
		and order_type_code like 'P%'
		and business_type = '02'
	group by
		sdt,
		receive_location_code,
		goods_code
--	union all 
--	SELECT
--		sdt,
--		shop_id as dc_code,
--		goodsid as goods_code,
--		sum(pur_qty_in) receive_qty,
--		sum(tax_pur_val_in)receive_amt,
--		0 shipped_qty ,
--		0 shipped_amt,
--		'old' type
--	FROM
--		b2b.ord_orderflow_t a 
--	where
--		sdt>='20200101'
--		and sdt<='20201217'
--		and pur_org like 'P6%'
--		and pur_doc_type in ('ZN01','ZN02','ZN03')
--		and delivery_finish_flag='X'
--	group by sdt,
--		shop_id,
--		goodsid
	union all 
	select sdt,
		shipped_location_code as dc_code,
		goods_code ,
		0 receive_qty ,
		0 receive_amt,
		sum(shipped_qty)shipped_qty,
		sum(price*shipped_qty) shipped_amt,
		'new' type
	from csx_dw.dws_wms_r_d_shipped_order_all_detail 
	where sdt>='20200101'
	and sdt<='20201231'
	-- and business_type ='70'
	group by sdt,
		shipped_location_code ,
		goods_code 
--	union all 
--	select sdt,
--		shopid as dc_code,
--		goodsid as goods_code,
--		0 receive_qty ,
--		0 receive_amt,
--		sum(pur_qty_in)shipped_qty,
--		sum(tax_pur_val_in) shipped_amt,
--		'old' type
--	from b2b.ord_orderflow_t a
--	where 	
--		sdt>='20200101'
--		and sdt<='20201217'
--		and pur_org like 'P6%'
--		and pur_doc_type in ('ZNR1','ZNR2','ZNR3')
--		and delivery_finish_flag='X'
--	group by sdt,
--		shopid,
--		goodsid
		) a
join (
	select
		s.location_code,
		s.shop_name,
		dist_code,
		dist_name
	from
		csx_dw.csx_shop s
	where
		sdt = 'current') s on
	a.dc_code = s.location_code
join (
	select
		p.goods_id ,
		p.goods_name,
		p.division_code,
		p.division_name,
		p.department_id,
		p.department_name
	from
		csx_dw.dws_basic_w_a_csx_product_m p
	where
		sdt = 'current' ) p on a.goods_code=p.goods_id
	 where a.dc_code !='W098'
group by 
	substr(sdt,1,6) ,
	dist_code,
	dist_name,
	dc_code,
	s.shop_name,
	goods_code ,
	p.goods_name,
	p.division_code,
	p.division_name,
	p.department_id,
	p.department_name,
	type
;

-- SAP 跨公司配送 ZC01

select
	substr(sdt,1,6) ,
	send_location_code,
	dist_code,
	dist_name,
	dc_code,
	s.shop_name,
	goods_code ,
	p.goods_name,
	p.division_code,
	p.division_name,
	p.department_id,
	p.department_name,
	sum(receive_qty)receive_qty,
	sum(receive_amt)receive_amt,
	sum(shipped_qty)shipped_qty,
	sum(shipped_amt)shipped_amt,
	type
from
	(
	SELECT
		sdt,
		receive_location_code as dc_code,
		goods_code,
		send_location_code ,
		sum(receive_qty) receive_qty,
		sum(amount)receive_amt,
		0 shipped_qty ,
		0 shipped_amt,
		'old' type
	FROM
		csx_dw.dws_wms_r_d_entry_order_all_detail
	where
		sdt>='20200101'
		and sys ='old'
		and business_type in ('ZC01')	 
	group by sdt,
		receive_location_code,
		goods_code,
		send_location_code
	UNION all 
	select sdt,
		d.shipped_location_code as dc_code,
		goods_code  ,
		receive_location_code as send_location_code ,
		0 receive_qty ,
		0 receive_amt,
		sum(shipped_qty)shipped_qty,
		sum(amount) shipped_amt,
		'old' type
	FROM
		csx_dw.dws_wms_r_d_shipped_order_all_detail d
	where
		sdt>='20200101'
		and sys ='old'
		and business_type in ('ZCR1')	 
	group by sdt,
		shipped_location_code,
		goods_code,
		receive_location_code
	)a
join (
	select
		s.location_code,
		s.shop_name,
		dist_code,
		dist_name
	from
		csx_dw.csx_shop s
	where
		sdt = 'current' 
		and table_type=1) s on
	a.dc_code = s.location_code
join (
	select
		p.goods_id ,
		p.goods_name,
		p.division_code,
		p.division_name,
		p.department_id,
		p.department_name
	from
		csx_dw.dws_basic_w_a_csx_product_m p
	where
		sdt = 'current' ) p on a.goods_code=p.goods_id
		where a.dc_code!='W098'
group by 
	substr(sdt,1,6) ,
	dist_code,
	dist_name,
	dc_code,
	s.shop_name,
	goods_code ,
	p.goods_name,
	p.division_code,
	p.division_name,
	p.department_id,
	p.department_name,
	type,
	send_location_code
;


select * from csx_dw.wms_entry_order  
where sdt>='20200101' and sdt<='20201217' and business_type_code = '67';
 ;
 
select * from csx_dw.csx_shop where sdt='current' and location_code='9PO4';
select * from csx_dw.dws_wms_r_d_entry_order_all_detail  where receive_location_code ='W0K6' and goods_code ='1004710' and sdt>='20200101' and sdt<'20200201';

select 
	receive_location_code ,
	goods_code ,
	sum(receive_qty)qty,
  substr(sdt, 1, 6) as month,
  sum(receive_qty * price) as total
from csx_dw.dws_wms_r_d_entry_order_all_detail 
where sdt = '20200101' and sys = 'old' and (business_type like 'ZN0%' OR business_type like 'ZC0%' OR business_type like 'ZU0%')
group by receive_location_code,substr(sdt, 1, 6),goods_code order by month;