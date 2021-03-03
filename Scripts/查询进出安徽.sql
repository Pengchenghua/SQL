
-- �ڳ����
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
	sum(final_qty) final_qty,
	sum(qm_amt) qm_amt,
	sum(qm_amt) /sum(final_qty) qm_price,
	is_self_product
	from (
 select
	a.product_code,
	a.location_code,
	a.shipper_code,
	sum( after_qty) qc_qty,
	sum(after_amt) qc_amt,
	sum( after_price) qc_price,
	0 final_qty,
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
		csx_ods.wms_accounting_stock_detail_ods  where sdt='20190916') a
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
		to_date(posting_time)in('2019-09-01') and sdt='20190916'
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
	sum( after_qty) final_qty,
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
		csx_ods.wms_accounting_stock_detail_ods where sdt='20190916' ) a
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
		to_date(posting_time) >='2019-09-01'and to_date(posting_time)<='2019-09-16' and sdt='20190916'
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
		a.dept_name,
		if(goods_code is null, '���ǹ�����Ʒ', '������Ʒ') as is_self_product
	from
		dim.dim_goods_latest a
	left join 
	(
  select goods_code from csx_dw.factory_bom 
  where sdt = '20190916') b on a.goodsid=b.goods_code)c on
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
	a.shipper_code,
	is_self_product

;
-- SELECT *from temp.p_wms_01 WHERE product_code='896900';

-- ��ⵥ
drop table if  EXISTS temp.p_wms_02;
CREATE TEMPORARY TABLE if not EXISTS temp.p_wms_02
as 
select
	receive_location_code,
	receive_location_name,
	a.product_code,
	a.product_name,
	sum(plan_qty)plan_qty,
	sum(receive_qty)receive_qty,
	 sum(amount)amount,
	 if(c.goods_code is null, '���ǹ�����Ʒ', '������Ʒ') as is_self_product
from 
(select
	entry_type,
	return_flag,
	supplier_code,
	supplier_name,
	send_location_code,
	send_location_name,
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
		sdt = '20190916')b on
	a.order_code = b.order_code
	and a.receive_status = 2
	and a.sdt = '20190916' and to_date(update_time)<='2019-09-16'  and to_date(update_time)>='2019-09-01'
group by
entry_type,
return_flag,
supplier_code,
	supplier_name,
	send_location_code,
	send_location_name,
	receive_location_code,
	receive_location_name,
	b.product_code,
	b.product_name) a
left outer join 
(
  select goods_code from csx_dw.factory_bom 
  where sdt = '20190916'
)c on a.product_code = c.goods_code
group by
	receive_location_code,
	receive_location_name,
	a.product_code,
	a.product_name,
if(c.goods_code is null, '���ǹ�����Ʒ', '������Ʒ');

select a.*,b.* from  temp.p_wms_01 a 
left join 
temp.p_wms_02 b 
on a.product_code=b.product_code
and a.location_code=b.receive_location_code;

select * from temp.p_wms_02 b where product_code='922744';

select * from temp.p_wms_01 b where product_code='922744'
;
-- ���ⵥ
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
	sdt = '20190916'
group by
	a.order_code,
	a.product_code,
	location_code,
	location_name) b on
a.order_code = b.order_code
and a.status in (0,8,6)
and a.sdt = '20190916'
group by
	shipper_code,
	shipper_name,b.product_code,
	location_code,location_name
;
-- ���۶��ͻ�
drop table if  EXISTS temp.p_wms_04;
create TEMPORARY table if not EXISTS temp.p_wms_04 
as 
select
  a.dc_code,
  a.dc_name,
  a.customer_no,
  a.customer_name,
  a.goods_code,
  a.goods_name,
  a.division_name,
  a.division_code,
  a.department_code,
  a.department_name,
  a.category_large_name,
  a.category_large_code,
  a.category_middle_name,
  a.category_middle_code,
  a.category_small_name,
  a.category_small_code,
  sum(sales_qty) as sales_qty,
  sum(sales_value) as sales_value,
  sum(sales_qty * coalesce(b.price, a.origin_sales_cost_price)) as sales_cost_value,
  sum(sales_qty * middle_office_price) as middle_sales_cost_value,
  sum(sales_value) - sum(sales_qty * coalesce(b.price, a.origin_sales_cost_price)) as profit,
  sum(sales_value) - sum(sales_qty * middle_office_price) as front_profit,
  if(c.goods_code is null, '���ǹ�����Ʒ', '������Ʒ') as is_self_product
from
(
  select 
    sdt,
    dc_code,
    dc_name,
    customer_no,
    customer_name,
    goods_code,
    goods_name,
    division_name,
    division_code,
    department_code,
    department_name,
    category_large_name,
    category_large_code,
    category_middle_name,
    category_middle_code,
    category_small_name,
    category_small_code,
    sales_value,
    origin_order_no,
    sales_qty,
    middle_office_price,
    origin_sales_cost_price
  from csx_dw.sale_item_m 
  where sdt >= '20190901'and sdt<='20190916' and sales_type = 'anhui'
)a left outer join 
(
  select
    source_order_no,
    product_code,
    max(price) as price
  from csx_dw.accounting_credential_item
  where move_type = '114A' and direction = '-'
  group by source_order_no, product_code
)b on a.goods_code = b.product_code and a.origin_order_no = b.source_order_no
left outer join 
(
  select goods_code from csx_dw.factory_bom 
  where sdt = '20190915'
)c on a.goods_code = c.goods_code
group by 
       dc_code,
    dc_name,
    customer_no,
    customer_name,
    a.goods_code,
    goods_name,
    division_name,
    division_code,
    department_code,
    department_name,
    category_large_name,
    category_large_code,
    category_middle_name,
    category_middle_code,
    category_small_name,
    category_small_code,
    if(c.goods_code is null, '���ǹ�����Ʒ', '������Ʒ');

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







