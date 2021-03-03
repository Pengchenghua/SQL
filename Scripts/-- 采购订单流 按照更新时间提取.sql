-- 采购订单流 按照更新时间提取
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
