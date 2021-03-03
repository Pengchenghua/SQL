select substr(sdt,1,6) mon ,
	local_purchase_flag,
	receive_type ,
	receive_business_type ,
	dist_code,
	dist_name,
	a.location_code ,
	location_name ,
	supplier_code ,
	supplier_name ,
	category_code ,
	category_name ,
	category_large_code ,
	category_large_name ,
	goods_code ,
	goods_name ,
	sum(receive_qty)qty ,
	sum(receive_amt)amt ,
	sum(receive_amt)/sum(receive_qty) as avg_price
from csx_dw.ads_supply_order_flow  a 
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current' and zone_id='3') b on a.location_code =b.location_code
where sdt>='20200701'
	and receive_type='采购入库'
group by 
substr(sdt,1,6)  ,
	local_purchase_flag,
	receive_type ,
	receive_business_type ,
	dist_code,
	dist_name,
	a.location_code ,
	location_name ,
	supplier_code ,
	supplier_name ,
	category_code ,
	category_name ,
	category_large_code ,
	category_large_name ,
	goods_code ,
	goods_name ;
	

-- 高周转商品
select
	years,
	months,
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
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name,
	category_small_code,
	category_small_name,
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
	cost_30day,
	sales_30day,
	qty_30day,
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
	entry_days
from
	csx_tmp.ads_wms_r_d_goods_turnover a 
join 
(select location_code from csx_dw.csx_shop where sdt='current' and zone_id='3') b on a.dc_code =b.location_code
	where
	sdt in ('20200731','20200831','20200930')	
	and period_inv_amt_30day != 0
;