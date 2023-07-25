refresh csx_dw.dws_sale_r_d_customer_sale;

select
	province_code,
	province_name,
	dc_code,
	dc_name,
	a.goods_code,
	goods_name,
	unit_name,
	div_id,
	div_name,
	dept_id,
	dept_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name,
	category_small_code,
	category_small_name,
	product_status_name,
	valid_tag_name,
	sale,
	profit,
	qty,
	inv_amt,
	inv_qty,
	if(c.goods_code is null ,0,1) as note
from (
select
	province_code,
	province_name,
	dc_code,
	dc_name,
	goods_code,
	goods_name,
	unit_name,
	div_id,
	div_name,
	dept_id,
	dept_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name,
	category_small_code,
	category_small_name,
	product_status_name,
	b.valid_tag_name,
	sum(sales_value)sale,
	sum(profit)profit,
	sum(sales_qty)qty,
	sum(case when sdt = '20200423' then inventory_amt end )inv_amt,
	sum(case when sdt = '20200423' then inventory_qty end )inv_qty
from
	csx_dw.dc_sale_inventory a
left join (
	select
		cpi.shop_code,
		cpi.product_code,
		cpi.product_status_name,
		cpi.valid_tag_name
	from
		csx_dw.dws_basic_w_a_csx_product_info cpi
	where
		sdt = '20200423') as b on
	a.dc_code = b.shop_code
	and a.goods_code = b.product_code
where
	sdt >= '20191201'
	and sdt <= '20200423'
GROUP by
	province_code,
	province_name,
	dc_code,
	dc_name,
	goods_code,
	goods_name,
	unit_name,
	div_id,
	div_name,
	dept_id,
	dept_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name,
	category_small_code,
	category_small_name,
	product_status_name,
	b.valid_tag_name
	)a left join 
	(select DISTINCT goods_code from csx_dw.dws_sale_r_d_customer_sale where sdt >= '20191201'
	and sdt <= '20200423' and channel_name='大')c  on a.goods_code=c.goods_code
	where product_status_name in ('B 正常商品','A 新品')