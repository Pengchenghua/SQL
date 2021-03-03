select
	
	sum(receive_qty)
from
	csx_dw.ads_supply_order_flow
where
	order_update_time >= '2020-11-01 00:00:00.0'
	and order_update_time <= '2020-11-30 00:00:00.0'
	--and order_status = 4
	and receive_status = '2'
	and source_type = 1
	and location_code ='W0A7'
;


-- 根据入库单查询关联订单
select
	b.pur_order_code
	,source_type_name
	,origin_order_code
	,a.link_order_code
	, b.order_class
	, a.order_code
	, status
	, receive_location_code
	,receive_location_name 
	, supplier_code
	, supplier_name 
	,send_location_code
	,send_location_name
	, goods_code
	, c.goods_name
	, standard
	, c.unit_name
	, c.department_id
	, c.department_name
	, c.category_large_code
	, c.category_large_name
	, c.category_middle_code
	, c.category_middle_name
	, price
	, plan_qty
	, plan_qty*price plan_amt
	, receive_qty
	, price*receive_qty as amount
	, case when local_purchase_flag='1' then '是' else '否' end local_purchase_name
	, to_date(a.create_time ) create_date
	, to_date(update_time) update_date
	, a.sdt
from
	csx_dw.dws_wms_r_d_entry_order_all_detail a
join
(select
	goods_id
	, goods_name
	, m.unit_name
	, m.standard 
	, m.department_id
	, m.department_name
	, m.category_large_code
	, m.category_large_name
	, m.category_middle_code
	, m.category_middle_name
from
	csx_dw.dws_basic_w_a_csx_product_m m
where
	sdt = 'current') c on a.goods_code=c.goods_id
left join 
(select
	h.order_code as pur_order_code
	, link_order_code
	, case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end as order_class
    , received_order_code
    , shipped_order_code
    ,source_type_name
    ,status
    ,local_purchase_flag
from
	csx_dw.dws_scm_r_d_scm_order_info h
where
	sdt >= '20200101'	 
	-- and h.source_type = 1
	-- and status = 4
	
		and super_class != '2'
	group by 
	h.order_code 
	, link_order_code
	, super_class
    , received_order_code
    , source_type_name
    ,shipped_order_code
    ,status
    ,local_purchase_flag ) b on a.order_code =b.received_order_code
where
	sdt >= '20201101'
	and sdt <= '20201130'
	and a.sys ='new'
	and a.status=2
	--and order_code ='IN201111001721'
	--and a.receive_location_code ='W0A7'
;


select * from csx_dw.ads_supply_order_flow  where order_code ='TOW0A7201111000290';
select max(sdt) from csx_dw.dwd_scm_r_d_scm_order_header where sdt<='20201209'  where received_order_code='IN200126000003';

select  a.order_code
	, receive_location_code
	, supplier_code
	, goods_code
	, price
	, plan_qty
	, plan_qty*price plan_amt
	, receive_qty
	, amount from csx_dw.dws_wms_r_d_entry_order_all_detail a where sdt >= '20201101'
	and sdt <= '20201130' and order_code ='IN201111001721';


select
	*
from
	csx_dw.dws_scm_r_d_scm_order_m h
where
	
	 order_code ='TOW0A7201111000290'
	and sdt>='20200101';
	;
	
select order_code,sum(receive_qty) from csx_dw.ads_supply_order_flow 
where sdt>='20200101' 
and receive_close_date>='20201101' 
and receive_close_date <='20201130'
and location_code ='W0A7' 
and source_type=1
and order_status =4
group by order_code 
;

select * from csx_dw.ads_supply_order_flow where order_code ='TOW0A7201121000217';
select * from csx_dw.wms_entry_order where order_code ='IN201121001524';

select * from csx_dw.dws_scm_r_d_scm_order_info where order_code in ('POW0A7201103000288','TOW0A7201121000217','');

select * from csx_dw.wms_shipped_order where order_no='OU201121000277';



with entry as  (
select
    supplier_code
from
	csx_dw.wms_entry_order a
where
	sdt >= '20190101'
	and sdt <= '20201130'
	and  a.category_large_code  in ('1101')
	and ((entry_type_code like 'P%' and business_type_code !='02') or business_type ='采购入库(old)')
group by
	 supplier_code
)select a.supplier_code ,vendor_name,acct_grp,vat_regist_num,s.fixed_credit_line ,s.vendor_pur_lvl,s.vendor_pur_lvl_name  from entry a 
join 
(select  vendor_id,vendor_name,acct_grp,vat_regist_num,s.fixed_credit_line ,s.vendor_pur_lvl,s.vendor_pur_lvl_name 
from csx_dw.dws_basic_w_a_csx_supplier_m s where sdt='current') s on a.supplier_code=s.vendor_id 

;
refresh csx_dw.ads_supply_order_flow ;
select * from csx_dw.ads_supply_order_flow where order_code ='POW0A7201103000288';




-- 月度入库报表
select
	substr(a.sdt,1,6) mon,
	 receive_location_code
	,receive_location_name 
	, REGEXP_REPLACE(supplier_code,'^0*','') as supplier_code 
	, supplier_name 
	,send_location_code
	,send_location_name
	, goods_code
	, c.goods_name
	, standard
	, c.unit_name
	, c.department_id
	, c.department_name
	, c.category_large_code
	, c.category_large_name
	, c.category_middle_code
	, c.category_middle_name
	, sum(amount)/sum(receive_qty) avg_price
	, sum(receive_qty)qty
	, sum(amount) amount
	
from
	csx_dw.dws_wms_r_d_entry_order_all_detail a
join 
(select * from csx_dw.csx_shop where sdt='current' and dist_code='15') g on a.receive_location_code =location_code 
join
(select
	goods_id
	, goods_name
	, m.unit_name
	, m.standard 
	, m.department_id
	, m.department_name
	, m.category_large_code
	, m.category_large_name
	, m.category_middle_code
	, m.category_middle_name
from
	csx_dw.dws_basic_w_a_csx_product_m m
where
	sdt = 'current') c on a.goods_code=c.goods_id
	where 1=1
	--receive_location_code='W0A8'
	and a.sdt>='20200101' and a.sdt<='20201130'
group by 
 receive_location_code
	,receive_location_name 
	, supplier_code
	, supplier_name 
	,send_location_code
	,send_location_name
	, goods_code
	, c.goods_name
	, standard
	, c.unit_name
	, c.department_id
	, c.department_name
	, c.category_large_code
	, c.category_large_name
	, c.category_middle_code
	, c.category_middle_name
	,substr(a.sdt,1,6) ;


select * from csx_dw.dws_wms_r_d_entry_order_all_detail where goods_code ='853509' and receive_location_code ='W0B6' and sdt>='20200101' and sdt<='20200131';

select * from b2b.ord_orderflow_t where goodsid ='853509' and shop_id ='W0B6' and sdt>='20200101' and sdt<='20200131';

select * from csx_ods.source_scm_r_d_scm_order_trace_header
where 
	update_time >='2020-10-01 00:00:00'
	and update_time <'2020-12-01 00:00:00'
	and sdt='19990101'
	;
	
select * from csx_dw.dws_scm_r_d_scm_order_m where order_code='POW0R8201015002395' ;
select * from csx_ods.source_scm_r_d_scm_order_header where sdt='19990101';
select * from csx_ods.source_scm_r_d_scm_product_received_dtl  where sdt='19990101'  ;


select count(*) from (
select
	a.order_code,
	super_class,
	order_type_name,
	original_order_code,
	link_order_code,
	shipped_order_code,
	received_order_code,
	if (direct_flag='0','否','是') direct_flag,
	if (zm_direct_flag='0','否','是') zm_direct_flag,
	if (customer_direct_flag='0','否','是') customer_direct_flag,
	if (local_purchase_flag='0','否','是') local_purchase_flag,
	case when order_status = 1 then '已创建'
		when order_status = 2 then '已发货'
		when order_status = 3 then '部分入库'
		when order_status = 4 then '已完成'
		when order_status = 5 then '已取消'
		end order_status_name,  --状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)
	case when system_status = 1 then '订单已提交'
		when system_status = 2 then '已同步WMS'
		when system_status = 3 then 'WMS已回传'
		when system_status = 4 then '修改已提交'
		when system_status = 5 then '修改已同步WMS'
		when system_status=6 then '修改成功'
		when system_status=7 then '修改失败'		
		end system_status_name ,    --系统状态(1-订单已提交、2-已同步WMS、3-WMS已回传、4-修改已提交、5-修改已同步WMS、6-修改成功、7-修改失败)
	source_type, 
	source_type_name,
	supplier_code,
	supplier_name,
	source_location_code,
	source_location_name,
	target_location_code,
	target_location_name,
	settle_location_code,
	settle_location_name,
	purchase_org_code,
	purchase_org_name,
	if (is_compensation=0,'否','是')is_compensation_name,
	last_delivery_date,
	remark,
	create_time,
	create_by,
	update_time,
	update_by,
	if (addition_order_flag=0,'否','是') addition_order_name,  --是否加配单(0-否、1-是)
	product_code,
	goods_name,
	m.standard ,
	m.unit_name ,
	m.department_id ,
	m.department_name,
	m.division_code,
	m.division_name,
	m.category_large_code ,
	m.category_large_name ,
	tax_rate,
	cost,
	no_tax_cost,
	plan_qty,
	cost*plan_qty as plan_amt,
	goods_status ,  
	receive_qty,
	receive_amout,
	shipped_qty,
	shipped_amount
from
 (
	select
		id,
		a.order_code,
		super_class,
		original_order_code,
		link_order_code,
		shipped_order_code,
		received_order_code,
		direct_flag,
		zm_direct_flag,
		customer_direct_flag,
		local_purchase_flag,
		a.status as order_status,
		system_status,
		source_type,
		supplier_code,
		supplier_name,
		source_location_code,
		source_location_name,
		target_location_code,
		target_location_name,
		settle_location_code,
		settle_location_name,
		purchase_org_code,
		purchase_org_name,
		is_compensation,
		-- extra_flag,  --缺失字段
		
		last_delivery_date,
		remark,
		create_time,
		create_by,
		update_time,
		update_by,
		addition_order_flag,
		product_code,
		tax_rate,
		cost,
		no_tax_cost,
		plan_qty,
		cost*plan_qty as plan_amt,
		goods_status
	from
		csx_ods.source_scm_r_d_scm_order_header a
	join (
		select
			s.order_code,
			s.product_code,
			qty as plan_qty,
			case when status = 1 then '已创建'
		when status = 2 then '已发货'
		when status = 3 then '入库中'
		when status = 4 then '已完成'
		when status = 5 then '已取消'
		end  as goods_status,     --商品状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)
			return_reason_code,
			return_reason_name,
			tax_rate,
			cost,
			no_tax_cost
		from
			csx_ods.source_scm_r_d_scm_order_items s 
	   join 
		(select
			order_code,
			product_code,
			tax_rate,
			price1_include_tax as cost,
			price1_free_tax as no_tax_cost
		from
			csx_ods.source_scm_r_d_scm_order_product_price
		where
			sdt = '19990101') t on s.order_code =t.order_code and s.product_code =t.product_code
		where sdt = '19990101' 
 )b on
		a.order_code = b.order_code

	where
		sdt = '19990101'
		and a.update_time >= '2020-11-01 00:00:00'
		and a.update_time <'2020-12-01 00:00:00'
		--and a.order_code ='TOW0A8201001000314'
) a
left join (
	select
		order_code,
		b.goods_code,
		b.outside_order_code ,
		b.price ,
		sum(b.receive_qty) receive_qty,
		sum(b.price*b.receive_qty) receive_amout
	from
		csx_dw.dws_wms_r_d_entry_order_all_detail b
	where 1=1
		and sdt >= '20200101' 
		or sdt='19990101'
	--	and sdt<='20201217'
	group by
		order_code,
		b.goods_code ,
		b.outside_order_code,
		b.price) c on	a.received_order_code = c.order_code	and a.product_code = c.goods_code
left join (
	select
		order_no,
		goods_code,
		sum(shipped_qty) shipped_qty,
		sum(shipped_qty*price) shipped_amount
	from
		csx_dw.dws_wms_r_d_shipped_order_all_detail
	where
		sdt >= '20200101'
	group by
		order_no,
		goods_code
		) d on
	a.shipped_order_code = d.order_no
	and a.product_code = d.goods_code
left join (
	select
		distinct
		source_type_code,
		source_type_name
	from
		csx_ods.source_scm_w_a_scm_order_type_config) g on
	a.source_type = g.source_type_code 
left join 
(
	select
		distinct		 
		order_type_code,
		order_type_name 
	from
		csx_ods.source_scm_w_a_scm_order_type_config) j on cast(a.super_class as int)= j.order_type_code
left join 
(select
	goods_id,
	goods_name,
	m.standard ,
	m.unit_name ,
	m.department_id ,
	m.department_name,
	m.division_code,
	m.division_name,
	m.category_large_code ,
	m.category_large_name 
from
	csx_dw.dws_basic_w_a_csx_product_m m
where
	sdt = 'current') m on a.product_code=m.goods_id 
) a ;


(
	select
		distinct
		source_type_code,
		source_type_name,
		order_type_code,
		order_type_name,
		wms_order_type_code,
		wms_return_flag
	from
		csx_ods.source_scm_w_a_scm_order_type_config)

