-- CONNECTION: name= HVIE
select sdt,
    stock_dc_code,stock_dc_name,
	require_delivery_date,
	order_date,
	sap_cus_code,
	sap_cus_name,
	receive_address,
	creator,
	product_code,
	bar_code,
	product_name ,
	purchase_unit,
	unit,
	purchase_qty ,
	real_value,
	spec_remarks ,
	category_large_name,
	stock_loc_code ,
	spec,
	pick_status ,
	is_print_pick_order,
	ratio_1_to_2
	from
	(
SELECT
 sdt,
    stock_dc_code,stock_dc_name,
	regexp_replace(to_date(require_delivery_time),
	'-',
	'')require_delivery_date,
	regexp_replace(to_date(order_time),
	'-',
	'')order_date,
	sap_cus_code,
	sap_cus_name,
	receive_address,
	creator,
	product_code,
	bar_code,
	product_name ,
	purchase_unit,
	unit,
	sum(purchase_qty)purchase_qty ,
	sum(real_value)real_value,
	spec_remarks ,
	category_large_name,
	stock_loc_code ,
	spec,
	pick_status ,
	is_print_pick_order
from
	csx_dw.order_m a
where 
	 sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),
	7)),
	'-',
	'')
	and stock_dc_code in ('W0J2')
GROUP by
	sdt,regexp_replace(to_date(order_time),
	'-',
	''),
	regexp_replace(to_date(require_delivery_time),
	'-',
	''),
	sap_cus_code,
	sap_cus_name,
	receive_address,
	creator,
	product_code,
	bar_code,
	product_name ,
	purchase_unit,
	unit,
	spec_remarks ,
	category_large_name,
	stock_loc_code ,
	category_large_code ,
	spec,
	pick_status ,
	is_print_pick_order,  stock_dc_code,stock_dc_name
	)a
	left  JOIN 
	-- 查询物流件装数
    (select
c.wl_area_id
,c.wl_area_name
,b.sku_id
,c.config_id
,c.client_id
,c.ratio_1_to_2 -- 箱到商品数比例
from 
(select * from ods_hc_oracle.sku_sku_config where main_config_id = 'Y' and wl_area_id='11' )b
join ods_hc_oracle.sku_config c on b.config_id=c.config_id and b.wl_area_id=c.wl_area_id and b.client_id=c.client_id 
) b 
	on a.product_code=b.sku_id
order by
	sap_cus_code