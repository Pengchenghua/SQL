select 
			produce_month,
			province_name,
			a.order_code,
			a.goods_code,
			worker_cost, -- 人工
			machine_cost, -- 机时
			support_material_cost -- 辅材
		from 
			(
			select 
				produce_month,province_name,order_code,goods_code
			from 
				(
				select 
					produce_month,province_name,order_code,goods_code,
					row_number() over(partition by goods_code,produce_month,province_name order by order_code asc) as ranks
				from 
					csx_dw.dws_mms_r_a_factory_order
				where 
					sdt >= '20210201' and sdt <= '20210430'
				) a 
			where 
				ranks = 1
			) a 
			left join 
				(
				select 
					order_code,product_code,
					worker_cost/reckon_factor  as worker_cost,
					machine_cost/reckon_factor as machine_cost,
					support_material_cost/reckon_factor as support_material_cost
				from 
					csx_dw.dws_mms_w_a_setting_order_craft   --工厂工单工艺路线
				where 
					sdt = 'current'
				) b on a.order_code = b.order_code and a.goods_code = b.product_code