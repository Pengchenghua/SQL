
-- select version();
-- 单据类型(1-供应商订单、2-供应商退货订单、3-配送订单、4-返配订单
-- source_type COMMENT '来源类型(1-采购导入、2-直送、3-一键代发、4-项目合伙人、5-无单入库、6-寄售调拨、7-自营调拨、8-云超采购、9-工厂采购)'
-- status 状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)
-- system_status 系统状态(1-订单已提交、2-已同步WMS、3-WMS已回传、4-修改已提交、5-修改已同步WMS、6-修改成功、7-修改失败)
-- source_type 来源类型(1-采购导入、2-直送、3-一键代发、4-项目合伙人、5-无单入库、6-寄售调拨、7-自营调拨、8-云超采购、9-工厂采购)

-- system_status  COMMENT '系统状态(1-订单已提交、2-已同步WMS、3-WMS已回传、4-修改已提交、5-修改已同步WMS、6-修改成功、7-修改失败)',
 select
	a.order_code,
	super_class ,
	source_type,
	supplier_code,
	supplier_name,
	local_purchase_flag,
	a.customer_direct_flag,
	shipped_order_code,
	received_order_code,
	source_location_code,
	source_location_name,
	target_location_code,
	target_location_name,
	settle_location_code,
	settle_location_name,
	source_order_code,
	fixed_price_type,
	a.category_code,
	a.category_name,
	a.product_code,
	a.product_bar_code,
	a.product_name,
	spec,
	system_price,
	unit,
	pack_qty,
	batch_price,
	order_price,
	qty,
	amount1_include_tax,
	c.in_qty,
	in_price* in_qty as in_value,
	out_qty,
	case
		when out_price = 0 then in_price
		else out_price
	end out_price,
	out_qty*(
		case
			when out_price = 0 then in_price
			else out_price
		end
	) as out_value,
	c.close_flag,
	c.order_code,
	d.order_code,
	c.batch_order_code
from
	(
		select
			update_time,
			a.order_code,
			case
				when a.super_class = 1 then '1-供应商订单'
				when a.super_class = 2 then '2-供应商退货订单'
				when a.super_class = 3 then '3-配送订单'
				when a.super_class = 4 then '4-返配订单'
				else a.super_class
			end super_class ,
			source_type,
			supplier_code,
			supplier_name,
			local_purchase_flag,
			a.customer_direct_flag,
			shipped_order_code,
			received_order_code,
			source_location_code,
			source_location_name,
			target_location_code,
			target_location_name,
			settle_location_code,
			settle_location_name,
			source_order_code,
			fixed_price_type,
			a.category_code,
			a.category_name,
			b.product_code,
			b.product_bar_code,
			b.product_name,
			spec,
			system_price,
			unit,
			pack_qty,
			qty,
			batch_price,
			amount1_include_tax,
			price2_include_tax,
			price1_include_tax,
			order_price
		from
			(
				select
					update_time,
					a.order_code,
					a.super_class,
					original_order_code,
					link_order_code,
					shipped_order_code,
					received_order_code,
					direct_flag,
					zm_direct_flag,
					customer_direct_flag,
					local_purchase_flag,
					status,
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
					purchase_group_code,
					purchase_group,
					category_code,
					category_name,
					is_compensation,
					last_delivery_date,
					remark
				from
					csx_b2b_scm.scm_order_header a
				where
					status = 4
					and date_format(create_time, '%Y%m%d')<'20190917'
			) a
		join(
				select
					b.order_code,
					source_order_code,
					fixed_price_type,
					category_code,
					category_name,
					b.product_code,
					b.product_bar_code,
					b.product_name,
					system_price,
					spec,
					status,
					unit,
					pack_qty,
					qty,
					batch_price,
					amount1_include_tax,
					price2_include_tax,
					price1_include_tax,
					order_price
				from
					(
						select
							id,
							order_code,
							source_order_code,
							line_no,
							product_code,
							product_name,
							product_bar_code,
							spec,
							pack_qty,
							unit,
							qty,
							purchase_group_code,
							purchase_group_name,
							category_code,
							category_name,
							big_classify_code,
							big_classify_name,
							small_classify_code,
							small_classify_name,
							status,
							return_reason_code,
							return_reason_name,
							remark
						from
							csx_b2b_scm.scm_order_items
						where
							date_format(create_time, '%Y%m%d')<'20190917'
					)b
				join (
						select
							order_code,
							line_no,
							product_code,
							product_name,
							fixed_price_type,
							tax_code,
							tax_rate,
							tax_rate2,
							tax_code2,
							system_price,
							manual_price,
							system_transfer_price,
							manual_transfer_price,
							batch_price,
							price1_include_tax,
							price1_free_tax,
							price2_include_tax,
							price2_free_tax,
							case
								when price1_include_tax = 0 then price2_include_tax
								else price1_include_tax
							end as order_price,
							amount1_include_tax,
							amount1_free_tax,
							amount2_include_tax,
							amount2_free_tax,
							create_time,
							create_by,
							update_time,
							update_by,
							price1_enable_type,
							price2_enable_type,
							price_markup_proportion
						from
							csx_b2b_scm.scm_order_product_price
					)c on
					b.order_code = c.order_code
					and b.product_code = c.product_code
			)b on
			a.order_code = b.order_code
	)a
left join (
		select
			update_time,
			order_code,
			purchase_order_code,
			batch_order_code,
			product_code,
			qty in_qty,
			case
				when unit_price1 = 0 then unit_price2
				else unit_price1
			end in_price,
			close_flag
		from
			csx_b2b_scm.scm_product_received_dtl
	)c on
	a.order_code = c.purchase_order_code
	and a.product_code = c.product_code
left join (
		select
			update_time,
			order_code ,
			purchase_order_code,
			product_code,
			qty out_qty,
			unit_price ,
			unit_price2,
			case
				when unit_price = 0 then unit_price2
				else unit_price
			end out_price
		from
			scm_product_shipped_dtl
	) d on
	a.order_code = d.purchase_order_code
	and a.product_code = d.product_code ;
-- select date_format(now(),'%Y%m%d')

-- select * from scm_product_received_dtl where order_code='IN190910000199';
 select
	*
from
	csx_b2b_accounting.accounting_stock_detail
where
	product_code = '1' ;

select
	a.product_code ,
	a.location_code ,
	a.shipper_code ,
	a.reservoir_area_code,
	a.reservoir_area_name,
	product_name ,
	0 qc_qty ,
	0 qc_amt ,
	0 qc_price ,
	sum(after_qty) final_qty ,
	sum(after_amt) qm_amt ,
	sum(after_price) qm_price
from
	(
		select
			product_code ,
			location_code ,
			shipper_code ,
			product_name ,
			after_qty ,
			after_amt ,
			after_price ,
			date_format (
				posting_time,
				'%Y%m%d'
			) posting_time ,
			id ,
			reservoir_area_code,
			reservoir_area_name
		from
			accounting_stock_detail_view
	) a
join (
		select
			product_code ,
			location_code ,
			shipper_code ,
			max(id)max_id ,
			reservoir_area_code
		from
			accounting_stock_detail_view
		where
			date_format (
				posting_time,
				'%Y%m%d'
			) >= '20190901'
			and date_format (
				posting_time,
				'%Y%m%d'
			)<= '20190930'
		group by
			product_code ,
			location_code ,
			shipper_code ,
			reservoir_area_code
	) b on
	a.product_code = b.product_code
	and a.location_code = b.location_code
	and a.shipper_code = b.shipper_code
	and A.reservoir_area_code = b.reservoir_area_code
	and a.id = b.max_id
group by
	a.product_code ,
	a.location_code ,
	a.shipper_code,
	a.reservoir_area_code,
	product_name ,
	a.reservoir_area_name;