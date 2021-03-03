-- 生鲜入库查询 按月  w0h4 调整大客户 20191031
-- set sdt=2019-09-01;
-- set edt=2019-09-15;
refresh dim.dim_vendor;
select
	stype,
	pur_org,
	prov_code,
	prov_name,
	a.shop_id,
	shop_name,
	shop_id_out,
	shop_name_out,
	vendor_id,
	vendor_name,
	a.goodsid,
	goodsname,
	dept_id,
	dept_name,
	catg_l_id,
	catg_l_name,
	catg_m_id,
	catg_m_name,
	a.catg_s_id,
	catg_s_name,
	unit_name,
	pur_qty_in,
	pur_val_in,
	pur_qty_out,
	pur_val_out,
	last_pur_val_in,
	last_pur_qty_in
from
	(
	select
		stype,
		pur_org,
		prov_code,
		prov_name,
		a.shop_id,
		b.shop_name,
		shop_id_out,
		h.shop_name shop_name_out,
		a.vendor_id,
		vendor_name,
		a.goodsid,
		goodsname,
		dept_id,
		dept_name,
		catg_l_id,
		catg_l_name,
		catg_m_id,
		catg_m_name,
		a.catg_s_id,
		catg_s_name,
		unit_name,
		sum(pur_qty_in)pur_qty_in,
		sum(pur_val_in)* 1.00 pur_val_in,
		sum(pur_qty_out)* 1.00 pur_qty_out,
		sum(pur_val_out)* 1.00 pur_val_out,
		sum(last_pur_val_in)* 1.00 last_pur_val_in,
		sum(last_pur_qty_in)* 1.00 last_pur_qty_in
	from
		(
		SELECT
			a.pur_org,
			a.shop_id_in as shop_id,
			shop_id_out,
			case
				when a.vendor_id = '' then a.org_vendor
				else a.vendor_id
			end vendor_id,
			goodsid,
			a.goods_catg as catg_s_id ,
			sum(case when a.ordertype in ('配送', '直送', '直通', '货到c即配', 'UD') then a.pur_qty_in end ) pur_qty_in,
			sum(case when a.ordertype in ('配送', '直送', '直通', '货到即配', 'UD') then a.tax_pur_val_in end ) pur_val_in,
			sum(case when a.ordertype in ('配送', '直送', '直通', '货到即配', 'UD') then a.pur_val_in end ) notax_pur_val_in,
			sum(case when a.ordertype in ('退货', '返配') and (a.max_pstng_date_out >= '20190901' and a.max_pstng_date_out <='20190930') then a.pur_qty_out end ) pur_qty_out,
			sum(case when a.ordertype in ('退货', '返配') and (a.max_pstng_date_out >= '20190901' and a.max_pstng_date_out <='20190930' ) then a.tax_pur_val_out end ) pur_val_out,
			0 last_pur_qty_in,
			0 last_notax_pur_val_in,
			0 last_pur_val_in
		FROM
			b2b.ord_orderflow_t a
		 join (
			select
				shop_id,
				shop_name,
				prov_name,
				prov_code,
				case
					when a.shop_id='W0H4' then '平台'
					when a.shop_belong = '27' then 'B端'
					else 'M端'
				end stype
			from
				dim.dim_shop a
			where
				edate = '9999-12-31'
				and sales_dist_new_name like '%彩食鲜%'
				and prov_name is not null ) b on
			a.shop_id = b.shop_id
			and goods_catg between '10000000' and '14999999'
			and (max_pstng_date_in >= '20190901'
			and max_pstng_date_in <='20190930')
			and (sdt >='20190601'
			and sdt <='20190930' )
		group by
			a.shop_id_in,
			a.vendor_id,
			a.goodsid,
			goods_catg,
			pur_org,
			shop_id_out,
			org_vendor
	union all
		SELECT
			a.pur_org,
			a.shop_id_in as shop_id,
			shop_id_out,
			case
				when a.vendor_id = '' then a.org_vendor
				else a.vendor_id
			end vendor_id,
			goodsid,
			a.goods_catg as catg_s_id ,
			0 pur_qty_in,
			0 pur_val_in,
			0 notax_pur_val_in,
			0 pur_qty_out,
			0 pur_val_out,
			sum(case when a.ordertype in ('配送', '直送', '直通', '货到即配', 'UD') then a.pur_qty_in end ) last_pur_qty_in,
			sum(case when a.ordertype in ('配送', '直送', '直通', '货到即配', 'UD') then a.pur_val_in end ) last_notax_pur_val_in,
			sum(case when a.ordertype in ('配送', '直送', '直通', '货到即配', 'UD') then a.tax_pur_val_in end ) last_pur_val_in
		FROM
			b2b.ord_orderflow_t a
		join (
			select
				shop_id,
				shop_name,
				prov_name,
				prov_code,
				case
					when a.shop_id='W0H4' then '平台'
					when a.shop_belong = '27' then 'B端'					
					else 'M端'
				end stype
			from
				dim.dim_shop a
			where
				edate = '9999-12-31'
				and sales_dist_new_name like '%彩食鲜%'
				and prov_name is not null ) b on
			a.shop_id = b.shop_id
			and goods_catg between '10000000' and '14999999'
			and (max_pstng_date_in >= '20190901'
			and max_pstng_date_in <='20190930')
			and (sdt >='20190601'
			and sdt <='20190930')
		group by
			a.shop_id_in,
			a.vendor_id,
			a.goodsid,
			goods_catg,
			pur_org,
			shop_id_out,
			org_vendor ) a
	left join (
		select
			shop_id,
			shop_name,
			prov_name,
			prov_code,
			case
				when a.shop_id='W0H4' then '平台'
				when a.shop_belong = '27' then 'B端'
				else 'M端'
			end stype
		from
			dim.dim_shop a
		where
			edate = '9999-12-31') b on
		a.shop_id = b.shop_id
	left join (
		select
			goodsid,
			goodsname,
			a.dept_id,
			a.dept_name,
			a.catg_l_id,
			a.catg_l_name,
			a.catg_m_id,
			a.catg_m_name,
			a.catg_s_id,
			a.catg_s_name,
			a.unit_name
		from
			dim.dim_goods a
		where
			a.edate = '9999-12-31') d on
		a.goodsid = d.goodsid
	left join (
		select
			shop_id,
			shop_name
		from
			dim.dim_shop
		where
			edate = '9999-12-31')h on
		a.shop_id_out = h.shop_id
	left join (
		select
			vendor_id,
			vendor_name
		from
			dim.dim_vendor
		where
			edate = '9999-12-31')f on
		regexp_replace(a.vendor_id,'(^0*)',	'')= regexp_replace(f.vendor_id,'(^0*)','')
	group by
		stype,
		pur_org,
		prov_code,
		prov_name,
		a.shop_id,
		b.shop_name,
		shop_id_out,
		h.shop_name,
		a.vendor_id,
		vendor_name,
		a.goodsid,
		goodsname,
		dept_id,
		dept_name,
		catg_l_id,
		catg_l_name,
		catg_m_id,
		catg_m_name,
		a.catg_s_id,
		catg_s_name,
		unit_name ) a
where 	1 = 1
and a.catg_s_id between '100000000' and '11999999'
order by
stype,
prov_code,
a.shop_id,
dept_id;

