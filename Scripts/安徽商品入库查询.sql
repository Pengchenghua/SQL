select dc_code,
	dc_name,
	shop_code,
	case when SUBSTRING(c.pur_org,1,2 )='P3' then 'ÔÆ´´' when SUBSTRING(c.pur_org,1,2 )='P6' then '²ÊÊ³ÏÊ' ELSE 'ÔÆ³¬' end supertype,
	product_code,
	product_name,
	unit_name,
	bd_id,
	b.bd_name,
	b.dept_id,
	b.dept_name,
	b.catg_l_id,
	b.catg_l_name,
	total_price,
	apply_qty,
	accept_qty
from 
(select
	dc_code,
	dc_name,
	shop_code,
	product_code,
	product_name,
	sum(price* accept_qty)total_price,
	sum(apply_qty)apply_qty,
	sum(accept_qty)accept_qty
from
	csx_ods.apply_order_item_ods
where sdt>='20190901' and sdt<='20190917'
and regexp_replace(to_date(accept_time),'-','')>='20190901'
and regexp_replace(to_date(accept_time),'-','')<='20190917'
group by 	dc_code,
	dc_name,
	shop_code,
	product_code,
	product_name)a
left outer join 
dim.dim_goods_latest b on a.product_code=b.goodsid
left outer join
dim.dim_shop_latest c on a.shop_code=c.shop_id
left outer join 
(select DISTINCT goods_code from csx_dw.factory_bom where sdt='20190917') d 
on a.product_code=d.goods_code;



select * from dim.dim_shop_latest c;
