
--缺货率问题：库存为0的商品，我取的口径是近3个月所在dc有销售记录的商品（排除从20201001后已经不在售卖的商品）
 select
	province_name,
	a.dc_code,
	a.dc_name,
	a.sdt,
	count( goods_code),
	count( if(qq = '缺货', goods_code, null)),
	count( if(qq = '缺货', goods_code, null))/ count(distinct goods_code)
	--缺货率

	from (
	select
		a.dc_code,
		a.dc_name,
		a.sdt,
		a.goods_code,
		a.goods_name,
		if(a.qty = 0 or a.qty<c.sales_qty,'缺货','正常') qq
	from
		(
		select
			dc_code,
			dc_name,
			sdt,
			goods_code,
			goods_name,
			sum(qty) qty
		from
			csx_dw.dws_wms_r_d_accounting_stock_m
		where
			sdt >= '20201101'
		group by
			dc_code,
			dc_name,
			sdt,
			goods_code,
			goods_name ) a
	join (
		select
			dc_code,
			goods_code
		from
			csx_dw.dws_sale_r_d_customer_sale
		where
			sdt >= '20201001'
		group by
			dc_code,
			goods_code ) cc on
		a.dc_code = cc.dc_code
		and a.goods_code = cc.goods_code
	join (
		select
			shop_code,
			product_code
		from
			csx_ods.source_basic_w_a_md_product_shop
		where
			sdt = '20201221'
			and stock_properties = '1'
			--1存储 DC，2 TC 货到即配
		)b on
		b.shop_code = a.dc_code
		and b.product_code = a.goods_code
	left join (
		select
			dc_code,
			goods_code,
			goods_name,
			sdt,
			sum(sales_qty) as sales_qty
		from
			csx_dw.dws_sale_r_d_customer_sale
		where
			sdt >= '20201101'
			and sales_value > 0
			and return_flag = ''
			and substr(order_no,1,2) <> 'OC'
			--退货单排除
			and order_mode = 0
			--订单模式：0-配送,1-直送，2-自提，3-直通
			and is_self_sale = 1
			--自营，联营(非自营)，1自营，0联营(非自营)

			group by dc_code,
			goods_code,
			goods_name,
			sdt ) c on
		a.dc_code = c.dc_code
		and a.goods_code = c.goods_code
		and cast(a.sdt as int) + 1 = cast(c.sdt as int) )a
left join
	--省区
(
	select
		shop_id,
		shop_name,
		province_code,
		province_name,
		city_code,
		city_name
	from
		csx_dw.dws_basic_w_a_csx_shop_m
	where
		sdt = 'current') b on
	b.shop_id = a.dc_code
	where a.dc_code='W0A8'
group by
	province_name,
	a.dc_code,
	a.dc_name,
	a.sdt;
