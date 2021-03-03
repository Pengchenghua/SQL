with goods_tmp as (
select
	shop_code,
	b.classify_large_code,
	b.classify_large_name,
	count(product_code) as goods_sku
from
	csx_dw.dws_basic_w_a_csx_product_info a
	join 
	(select * from csx_tmp.temp_goods) g on a.product_code=g.goods_code and a.shop_code=g.dc_code
left join (
	select
		b.classify_large_code,
		b.classify_large_name,
		b.category_small_code
	from
		csx_dw.dws_basic_w_a_manage_classify_m b
	where
		sdt = 'current')b on
	a.small_category_code = b.category_small_code
where
	sdt = regexp_replace(to_date('${edate}'),'-','')
	and shop_code = 'W0A8'
	-- and stock_properties_name = '存储'
group by
	shop_code,
	b.classify_large_code,
	b.classify_large_name ),
sale_tmp as (
-- 动销SKU
select
	dc_code,
	b.classify_large_code,
	b.classify_large_name,
	count(distinct goods_code) sale_sku
from
	csx_dw.dws_sale_r_d_detail a
left join (
	select
		b.classify_large_code,
		b.classify_large_name,
		b.category_small_code
	from
		csx_dw.dws_basic_w_a_manage_classify_m b
	where
		sdt = 'current')b on
	a.category_small_code = b.category_small_code
where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <= regexp_replace(to_date('${edate}'),'-','')
	and dc_code = 'W0A8'
group by
	dc_code,
	b.classify_large_code,
	b.classify_large_name ),
turnover_tmp as (
select
	a.dc_code ,
	b.classify_large_code,
	b.classify_large_name,
	count(case when a.final_qty!=0 or qty_30day!=0 then  a.goods_id end ) as sku,
	sum(a.period_inv_amt_30day)as period_inv_amt_30day,
	sum(a.cost_30day) as cost_30day,
	sum(a.period_inv_amt_30day)/ sum(a.cost_30day) as turnover_day
from
	csx_tmp.ads_wms_r_d_goods_turnover a
join (
	select
		b.classify_large_code,
		b.classify_large_name,
		b.category_small_code
	from
		csx_dw.dws_basic_w_a_manage_classify_m b
	where
		sdt = 'current') b on
	a.category_small_code = b.category_small_code
where
	sdt = regexp_replace(to_date('${edate}'),'-','')
	and dc_code = 'W0A8'
group by
	a.dc_code ,
	b.classify_large_code,
	b.classify_large_name
	),
stock_out as 	(
--缺货率问题：库存为0的商品，我取的口径是近3个月所在dc有销售记录的商品（排除从20201001后已经不在售卖的商品）
 select

	a.dc_code,
	division_code,
	count( goods_code) as stock_sku,
	count( if(qq = '缺货', goods_code, null)) stock_out_sku,
	count( if(qq = '缺货', goods_code, null))/ count( goods_code) as stock_out_rate
	--缺货率

	from (
	select
		a.dc_code,
		a.dc_name,
		a.sdt,
		a.goods_code,
		a.goods_name,
		division_code,
		if(a.qty = 0 or a.qty<c.sales_qty,'缺货','正常') qq
	from
		(
		select
			a.dc_code,
			dc_name,
			sdt,
			a.goods_code,
			goods_name,
			a.division_code ,
			sum(qty) qty
		from
			csx_dw.dws_wms_r_d_accounting_stock_m  a
			join (select * from csx_tmp.temp_goods)j on
		j.dc_code = a.dc_code
		and j.goods_code = a.goods_code
--	left join 
--	(
--	select
--		b.classify_large_code,
--		b.classify_large_name,
--		b.category_small_code
--	from
--		csx_dw.dws_basic_w_a_manage_classify_m b
--	where
--		sdt = 'current') b on
--	a.category_small_code = b.category_small_code
	where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <=  regexp_replace(to_date('${edate}'),'-','')
	and a.division_code in ('10','11','12','13')
	group by
			a.dc_code,
			dc_name,
			sdt,
			a.goods_code,
			goods_name,
			a.division_code) a
	join (
		select
			dc_code,
			goods_code
		from
			csx_dw.dws_sale_r_d_detail 
		where
			sdt >= regexp_replace(to_date('${sdate}'),'-','')
		and sdt <=  regexp_replace(to_date('${edate}'),'-','')
		group by
			dc_code,
			goods_code ) cc on
		a.dc_code = cc.dc_code
		and a.goods_code = cc.goods_code
	join (select * from csx_tmp.temp_goods)b on
		b.dc_code = a.dc_code
		and b.goods_code = a.goods_code
	left join (
		select
			dc_code,
			goods_code,
			goods_name,
			sdt,
			sum(sales_qty) as sales_qty
		from
			csx_dw.dws_sale_r_d_detail 
		where
			sdt >= regexp_replace(to_date('${sdate}'),'-','')
			-- and sdt <=  regexp_replace(to_date('${edate}'),'-','')
			and sales_value > 0
			and return_flag = ''
			and substr(order_no,1,2) <> 'OC'
			--退货单排除
			and logistics_mode_code = '2'
			--物流模式：2-配送,1-直送，2-自提，3-直通
			and operation_mode = 1
			--自营，联营(非自营)，1自营，0联营(非自营)

			group by dc_code,
			goods_code,
			goods_name,
			sdt ) c on
		a.dc_code = c.dc_code
		and a.goods_code = c.goods_code
		and cast(a.sdt as int) + 1 = cast(c.sdt as int) )a
	where a.dc_code in('W0A8','W0A7','W0A3')
group by
	a.dc_code,
	division_code 
)
select
	a.shop_code,
	a.classify_large_code,
	a.classify_large_name,
	goods_sku,	
	b.sale_sku,
	sku,
	b.sale_sku/sku as sale_pin_rate,
	c.period_inv_amt_30day,
	cost_30day,
	turnover_day,
	d.stock_sku,
	d.stock_out_sku,
	d.stock_out_sku/d.stock_sku as stock_out_rate
from
	goods_tmp as a
left join sale_tmp as b on
	a.shop_code = b.dc_code
	and a.classify_large_code = b.classify_large_code
left join turnover_tmp as c on
	a.shop_code = c.dc_code
	and a.classify_large_code = c.classify_large_code 
left join 
stock_out d on a.shop_code = d.dc_code
	and a.classify_large_code = d.classify_large_code 
WHERE  a.classify_large_code!='B09';

select *from  csx_tmp.temp_goods;






--缺货率问题：库存为0的商品，我取的口径是近3个月所在dc有销售记录的商品（排除从20201001后已经不在售卖的商品）
 select
	substr(sdt,1,6)mon,
	a.dc_code,
	division_code,
	count( goods_code) as stock_sku,
	count( if(qq = '缺货', goods_code, null)) stock_out_sku,
	count( if(qq = '缺货', goods_code, null))/ count( goods_code) as stock_out_rate
	--缺货率

	from (
	select
		a.dc_code,
		a.dc_name,
		a.sdt,
		a.goods_code,
		a.goods_name,
		division_code,
		if(a.qty = 0 or a.qty<c.sales_qty,'缺货','正常') qq
	from
		(
		select
			a.dc_code,
			dc_name,
			sdt,
			a.goods_code,
			goods_name,
			a.division_code ,
			sum(qty) qty
		from
			csx_dw.dws_wms_r_d_accounting_stock_m  a
			join (select * from csx_tmp.temp_goods)j on
		j.dc_code = a.dc_code
		and j.goods_code = a.goods_code
--	left join 
--	(
--	select
--		b.classify_large_code,
--		b.classify_large_name,
--		b.category_small_code
--	from
--		csx_dw.dws_basic_w_a_manage_classify_m b
--	where
--		sdt = 'current') b on
--	a.category_small_code = b.category_small_code
	where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <=  regexp_replace(to_date('${edate}'),'-','')
	and a.division_code in ('10','11','12','13')
	group by
			a.dc_code,
			dc_name,
			sdt,
			a.goods_code,
			goods_name,
			a.division_code) a
	join (
		select
			dc_code,
			goods_code
		from
			csx_dw.dws_sale_r_d_detail 
		where
			sdt >= regexp_replace(to_date('${sdate}'),'-','')
		and sdt <=  regexp_replace(to_date('${edate}'),'-','')
		group by
			dc_code,
			goods_code ) cc on
		a.dc_code = cc.dc_code
		and a.goods_code = cc.goods_code
	join (select * from csx_tmp.temp_goods)b on
		b.dc_code = a.dc_code
		and b.goods_code = a.goods_code
	left join (
		select
			dc_code,
			goods_code,
			goods_name,
			sdt,
			sum(sales_qty) as sales_qty
		from
			csx_dw.dws_sale_r_d_detail 
		where
			sdt >= regexp_replace(to_date('${sdate}'),'-','')
			-- and sdt <=  regexp_replace(to_date('${edate}'),'-','')
			and sales_value > 0
			and return_flag = ''
			and substr(order_no,1,2) <> 'OC'
			--退货单排除
			and logistics_mode_code = '2'
			--物流模式：2-配送,1-直送，2-自提，3-直通
			and operation_mode = 1
			--自营，联营(非自营)，1自营，0联营(非自营)

			group by dc_code,
			goods_code,
			goods_name,
			sdt ) c on
		a.dc_code = c.dc_code
		and a.goods_code = c.goods_code
		and cast(a.sdt as int) + 1 = cast(c.sdt as int) )a
	where a.dc_code in('W0A8','W0A7','W0A3')
group by
	a.dc_code,
	division_code ,
	substr(sdt,1,6);



-- 动销SKU
select
substr(sdt,1,6),
	a.dc_code,
		case when division_code in ('10','11') then '11' 
	when division_code in ('12','13') then '12'
	end 
	business_division_code ,
	count(distinct a.goods_code) sale_sku
from
	csx_dw.dws_sale_r_d_detail a
	join (select * from csx_tmp.temp_goods_01)b on
		b.dc_code = a.dc_code
		and b.goods_code = a.goods_code
where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <= regexp_replace(to_date('${edate}'),'-','')
	
	and a.dc_code in ('W0A8',	'W0A3',	'W0A7')
group by
	a.dc_code,
		case when division_code in ('10','11') then '11' 
	when division_code in ('12','13') then '12'
	end 
	,substr(sdt,1,6) ;
	

--周转天数

select
 sdt,
	a.dc_code ,
	case when division_code in ('10','11') then '11' 
	when division_code in ('12','13') then '12'
	end 
	business_division_code ,
	count(goods_id),
	sum(final_amt)as amt,
	sum(period_inv_amt_30day)/sum(cost_30day)
	
from
	csx_tmp.ads_wms_r_d_goods_turnover a
		join (select * from  csx_tmp.temp_goods_01)b on
	a.dc_code=b.dc_code
	and b.goods_code = a.goods_id
where
	sdt in ('20201031',	'20201130',	'20201231')
	and a.dc_code in ('W0A8',	'W0A3',	'W0A7')
	and division_code  in ('10','11','12','13')
	and final_qty !=0
	group by 
		sdt,
		a.dc_code ,
	case when division_code in ('10','11') then '11' 
	when division_code in ('12','13') then '12'
	end ;
	


select
 	sdt,
	a.dc_code ,
	dc_name ,
	a.goods_id,
	goods_name,
	division_code ,
	division_name ,
	category_large_code ,
	category_large_name ,
	final_qty ,
	(final_amt)as amt,
	days_turnover_30 
	
from
	 csx_tmp.ads_wms_r_d_goods_turnover a
	join (select * from  csx_tmp.temp_goods_01)b on
	a.dc_code=b.dc_code
	and b.goods_code = a.goods_id
where
	sdt in ('20201031',	'20201130',	'20201231')
	and a.dc_code in ('W0A8',	'W0A3',	'W0A7')
	and division_code  in ('10','11','12','13')
	and final_qty !=0
	;
	


select
	sdt,
	a.dc_code,
		a.dc_name,
		a.sdt,
		a.goods_code,
		a.goods_name,
		division_code,
		qq,
		qty,
		sales_qty

-- count( goods_code) as stock_sku,
--	 (if(qq = '缺货', goods_code, null)) stock_out_sku,
--	( if(qq = '缺货', goods_code, null))/ count( goods_code) as stock_out_rate
--缺货率

	from (
	select
		a.dc_code,
		a.dc_name,
		a.sdt,
		a.goods_code,
		a.goods_name,
		division_code,
		qty,
		c.sales_qty,
		if(a.qty = 0 or a.qty<c.sales_qty,'缺货','正常') qq
	from
		(
		select
			a.dc_code,
			dc_name,
			sdt,
			a.goods_code,
			goods_name,
			a.division_code ,
			sum(qty) qty
		from
			csx_dw.dws_wms_r_d_accounting_stock_m  a
			join (select * from csx_tmp.temp_goods_01)j on
		j.dc_code = a.dc_code
		and j.goods_code = a.goods_code
--	left join 
--	(
--	select
--		b.classify_large_code,
--		b.classify_large_name,
--		b.category_small_code
--	from
--		csx_dw.dws_basic_w_a_manage_classify_m b
--	where
--		sdt = 'current') b on
--	a.category_small_code = b.category_small_code
	where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <=  regexp_replace(to_date('${edate}'),'-','')
	and a.division_code in ('10','11','12','13')
	group by
			a.dc_code,
			dc_name,
			sdt,
			a.goods_code,
			goods_name,
			a.division_code) a
	join (
		select
			dc_code,
			goods_code
		from
			csx_dw.dws_sale_r_d_detail 
		where
			sdt >= regexp_replace(to_date('${sdate}'),'-','')
		and sdt <=  regexp_replace(to_date('${edate}'),'-','')
		group by
			dc_code,
			goods_code ) cc on
		a.dc_code = cc.dc_code
		and a.goods_code = cc.goods_code
	join (select * from csx_tmp.temp_goods_01)b on
		b.dc_code = a.dc_code
		and b.goods_code = a.goods_code
	left join (
		select
			dc_code,
			goods_code,
			goods_name,
			sdt,
			sum(sales_qty) as sales_qty
		from
			csx_dw.dws_sale_r_d_detail 
		where
			sdt >= regexp_replace(to_date('${sdate}'),'-','')
			-- and sdt <=  regexp_replace(to_date('${edate}'),'-','')
			and sales_value > 0
			and return_flag = ''
			and substr(order_no,1,2) <> 'OC'
			--退货单排除
			--and logistics_mode_code = '2'
			--物流模式：2-配送,1-直送，2-自提，3-直通
			and operation_mode = 1
			--自营，联营(非自营)，1自营，0联营(非自营)

			group by dc_code,
			goods_code,
			goods_name,
			sdt ) c on
		a.dc_code = c.dc_code
		and a.goods_code = c.goods_code
		and cast(a.sdt as int) + 1 = cast(c.sdt as int) )a
	where a.dc_code in('W0A8','W0A7','W0A3')
	and qq='缺货'
group by
	a.dc_code,
		a.dc_name,
		a.sdt,
		a.goods_code,
		a.goods_name,
		division_code;