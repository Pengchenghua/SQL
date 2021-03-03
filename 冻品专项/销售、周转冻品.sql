
set enddate='${dt}';

drop table csx_tmp.temp_dp_01 ;
create temporary table csx_tmp.temp_dp_01
as 
select
	years,
	months,
	dist_code,
	dist_name,
	a.dc_code,
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
	all_qty,
	occupy_qty,
	available_qty ,
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
	where sdt>=regexp_replace(date_sub(${hiveconf:enddate},30),'-','') and sdt<=regexp_replace( ${hiveconf:enddate},'-','')
		and order_type_code like 'P%'
	group by receive_location_code
	,goods_code) d on a.dc_code =d.receive_location_code  and a.goods_id =d.goods_code 	
left join 
(select dc_code,goods_code,sum(occupy_qty) as occupy_qty,sum(available_qty) as available_qty,sum(qty) as all_qty 
from csx_dw.dws_wms_r_a_product_stock_m 
where sdt=regexp_replace( ${hiveconf:enddate},'-','')
and reservoir_area_code not in ('PD01','PD02','TS01')
group by dc_code,goods_code ) f on a.dc_code=f.dc_code and a.goods_id=f.goods_code
where
	sdt = regexp_replace( ${hiveconf:enddate},'-','')
--	and a.category_middle_code='B0304'
;
insert overwrite directory '/tmp/pengchenghua/aa' row format delimited fields terminated by '\t'
select *,joint_purchase_flag from  csx_tmp.temp_dp_01 a 
left join 
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' ) b on a.goods_id=b.product_code and a.dc_code=b.shop_code
where classify_middle_code='B0304';

insert overwrite directory '/tmp/pengchenghua/aa' row format delimited fields terminated by '\t'
select * from  csx_tmp.temp_dp_01 where classify_middle_code='B0304';






-- 冻品销售
insert overwrite directory '/tmp/pengchenghua/bb' row format delimited fields terminated by '\t'
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
	sdt>=regexp_replace(date_sub(${hiveconf:enddate},30),'-','') and sdt<=regexp_replace( ${hiveconf:enddate},'-','')
	and classify_middle_code = 'B0304'
	and business_type_code='1'
;



-- 在途订单
insert overwrite directory '/tmp/pengchenghua/cc' row format delimited fields terminated by '\t'
select
	sdt,
	create_time,
	last_delivery_date,
	order_code ,
	target_location_code,
	target_location_name,
	case when super_class='1' then '供应商订单'
		when  super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else  super_class end as order_class ,
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
    sdt>='20201201'
	and  items_status=1
;