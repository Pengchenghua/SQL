-- 冻品周转
select
	years,
	months,
	province_code,
	province_name,
	dist_code,
	dist_name,
	dc_code,
	dc_name,
	goods_id,
	goods_name,
	standard,
	unit_name,
	brand_name,
	dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name,
	a.category_small_code,
	a.category_small_name,
	valid_tag,
	valid_tag_name,
	goods_status_id,
	goods_status_name,
	sales_qty,
	sales_value,
	profit,
	sales_cost,
	period_inv_qty,
	period_inv_amt,
	final_qty,
	final_amt,
	days_turnover,
	cost_30day,
	sales_30day,
	qty_30day,
	qty as entry_30_qty,
	entry_amt as entry_30_amt,
	dms,
	inv_sales_days,
	period_inv_qty_30day,
	period_inv_amt_30day,
	days_turnover_30,
	max_sale_sdt,
	no_sale_days,
	dc_type,
	entry_qty,
	entry_value,
	entry_sdt,
	entry_days,
	dc_uses
from
	csx_tmp.ads_wms_r_d_goods_turnover a
left join (
	select
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		category_small_code
	from
		csx_dw.dws_basic_w_a_manage_classify_m
	where
		sdt = 'current' ) b on
	a.category_small_code = b.category_small_code

left join 
(select d.receive_location_code,goods_code,sum(d.receive_qty) qty,sum(price* receive_qty) entry_amt 
	from csx_dw.dws_wms_r_d_entry_detail d 
	where sdt>='20210203'
		and order_type_code like 'P%'
	group by receive_location_code
	,goods_code) d on a.dc_code =d.receive_location_code  and a.goods_id =d.goods_code 	
where
	sdt = '20210203'
;



-- 冻品销售
select
	order_no ,
	province_code ,
	province_name ,
	city_group_code ,
	city_group_name ,
	dc_code ,
	dc_name ,
	supplier_code ,
	supplier_name ,
	customer_no ,
	customer_name ,
	classify_small_code ,
	classify_small_name ,
	category_small_code ,
	category_small_name ,
	goods_code ,
	goods_name ,
	spec ,
	unit ,
	sdt,
	cost_price ,
	sales_price ,
	(sales_cost) sales_cost,
	(sales_qty) sales_qty,
	(sales_value) sales_value ,
	(profit)/(sales_value) as profit_rate
from
	csx_dw.dws_sale_r_d_detail
where
	sdt >= '20210101'
	and classify_middle_code = 'B0304'
;


select
	sdt,
	create_time,
	last_delivery_date,
	order_code ,
	target_location_code,
	target_location_name,
	case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end as order_class ,
	received_order_code,
	local_purchase_flag,
	header_status,
	source_type,
	source_type_name,
	supplier_code,
	supplier_name,
	goods_code,
	goods_name,
	unit,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	 order_qty ,
	 amt,
	items_status
from
	csx_dw.dws_scm_r_d_header_item_price a
join (
	select
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name,
		classify_small_code,
		classify_small_name,
		category_small_code
	from
		csx_dw.dws_basic_w_a_manage_classify_m
	where
		sdt = 'current'
		and classify_middle_code ='B0304') b on	a.category_small_code = b.category_small_code
where
	sdt>='20210101'
	and items_status=1
group by sdt,
	target_location_code,
	target_location_name,
	case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end  ,
	received_order_code,
	local_purchase_flag,
	header_status,
	source_type,
	source_type_name,
	supplier_code,
	supplier_name,
	goods_code,
	goods_name,
	unit,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name;