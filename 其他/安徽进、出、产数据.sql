
-- 期初库存
drop table if EXISTS temp.p_wms_01;
create TEMPORARY TABLE if not EXISTS temp.p_wms_01
as

select
	a.product_code,
	goodsname,
	bar_code,
	bd_id,
	bd_name,
	unit_name,
	catg_l_id,
	catg_l_name,
	dept_id,
	dept_name ,
	a.location_code,
	a.shipper_code,
	sum( qc_qty) qc_qty,
	sum(qc_amt) qc_amt,
	sum(qc_amt)/sum( qc_qty) qc_price,
	sum(qm_qty) qm_qty,
	sum(qm_amt) qm_amt,
	sum(qm_amt) /sum(qm_qty) qm_price
	from (
 select
	a.product_code,
	a.location_code,
	a.shipper_code,
	sum( after_qty) qc_qty,
	sum(after_amt) qc_amt,
	sum( after_price) qc_price,
	0 qm_qty,
	0 qm_amt,
	0 qm_price
from
	(
	select
		product_code,
		location_code,
		shipper_code,
		after_qty,
		after_amt,
		after_price,
		to_date(posting_time)posting_time,
		id,
		reservoir_area_code
	from
		csx_ods.wms_accounting_stock_detail_ods  where sdt='20190910') a
join (
	select
		product_code,
		location_code,
		shipper_code,
		max(id)max_id ,
		reservoir_area_code,
		to_date(posting_time)posting_time
	from
		csx_ods.wms_accounting_stock_detail_ods
	where
		to_date(posting_time)in('2019-09-01') and sdt='20190910'
	group by
		product_code,
		location_code,
		shipper_code,
		to_date(posting_time),reservoir_area_code )b on
	a.product_code = b.product_code
	and a.location_code = b.location_code
	and a.shipper_code = b.shipper_code
	and a.posting_time = b.posting_time
	and a.id = b.max_id
group by 
	a.product_code,
	a.location_code,
	a.shipper_code
union all
select
	a.product_code,
	a.location_code,
	a.shipper_code,
	0 qc_qty,
	0 qc_amt,
	0 qc_price,
	sum( after_qty) qm_qty,
	sum(after_amt) qm_amt,
	sum( after_price) qm_price
from
	(
	select
		product_code,
		location_code,
		shipper_code,
		after_qty,
		after_amt,
		after_price,
		to_date(posting_time)posting_time,
		id,
		reservoir_area_code
	from
		csx_ods.wms_accounting_stock_detail_ods where sdt='20190910' ) a
join (
	select
		product_code,
		location_code,
		shipper_code,
		max(id)max_id ,
		reservoir_area_code
	from
		csx_ods.wms_accounting_stock_detail_ods
	where
		to_date(posting_time) >='2019-09-01'and to_date(posting_time)<='2019-09-10' and sdt='20190910'
	group by
		product_code,
		location_code,
		shipper_code,
		reservoir_area_code )b on
	a.product_code = b.product_code
	and a.location_code = b.location_code
	and a.shipper_code = b.shipper_code
	AND A.reservoir_area_code=b.reservoir_area_code
	and a.id = b.max_id
group by 
	a.product_code,
	a.location_code,
	a.shipper_code
)a
left join (
	select
		goodsid,
		goodsname,
		a.bar_code,
		a.bd_id,
		a.bd_name,
		a.unit_name,
		a.catg_l_id,
		a.catg_l_name,
		a.dept_id,
		a.dept_name
	from
		dim.dim_goods_latest a)c on
	a.product_code = c.goodsid
group by 	a.product_code,
	goodsname,
	bar_code,
	bd_id,
	bd_name,
	unit_name,
	catg_l_id,
	catg_l_name,
	dept_id,
	dept_name ,
	a.location_code,
	a.shipper_code

;
-- SELECT *from temp.p_wms_01 WHERE product_code='896900';

-- 入库单
drop table if  EXISTS temp.p_wms_02;
CREATE TEMPORARY TABLE if not EXISTS temp.p_wms_02
as 

select
	receive_location_code,
	receive_location_name,
	b.product_code,
	b.product_name,
	sum(b.plan_qty)plan_qty,
	sum(b.receive_qty)receive_qty,
	sum(b.amount) amount
from
	csx_ods.wms_entry_order_header_ods a
join (
	select
		order_code,
		b.product_code,
		b.product_name,
		b.plan_qty,
		b.receive_qty,
		b.amount
	from
		csx_ods.wms_entry_order_item_ods b
	where
		sdt = '20190910')b on
	a.order_code = b.order_code
	and a.receive_status = 2
	and a.sdt = '20190910'
group by
	receive_location_code,
	receive_location_name,
	b.product_code,
	b.product_name ;
-- select * from  temp.p_wms_02;
-- 出库单
drop table if  EXISTS temp.p_wms_03;
CREATE TEMPORARY TABLE if not EXISTS temp.p_wms_03
as 

select
	shipper_code,
	shipper_name,
	location_code,
	location_name,
	b.product_code ,
	sum(case when sale_channel in (1, 2, 6) then receive_qty end) m_qty,
	sum(case when sale_channel in (1, 2, 6) then amount end) m_sale,
	sum(case when sale_channel in (7, 4, 3) then receive_qty end) b_qty,
	sum(case when sale_channel in (7, 4, 3) then amount end) b_amount,
	sum(case when sale_channel in (5) then receive_qty end) bbc_qty,
	sum(case when sale_channel in (5) then amount end) bbc_amount,
	sum(shipped_qty)shipped_qty,
	sum(receive_qty)receive_qty,
	sum(amount)amount
from
	csx_ods.wms_shipped_order_header_ods a
join 
(
select
	a.order_code,
	a.product_code ,
	sum(shipped_qty)shipped_qty,
	sum(receive_qty)receive_qty,
	sum(amount)amount,
	location_code,
	location_name
from
	csx_ods.wms_shipped_order_item_ods a
where
	sdt = '20190910'
group by
	a.order_code,
	a.product_code,
	location_code,
	location_name) b on
a.order_code = b.order_code
and a.status in (8,6)
and a.sdt = '20190910'
group by
	shipper_code,
	shipper_name,b.product_code,
	location_code,location_name
;

select
	a.* ,
	d.shop_name,
	b.* ,
	c.*
from
	temp.p_wms_01 a
left outer join temp.p_wms_02 b on
	-- a.shipper_code=b.shipper_code
 	a.location_code = b.receive_location_code
	and a.product_code = b.product_code
left outer join temp.p_wms_03 c on
	a.shipper_code = c.shipper_code
	and a.location_code = c.location_code
	and a.product_code = c.product_code
left outer join 
dim.dim_shop_latest d on a.location_code=d.shop_id
;






