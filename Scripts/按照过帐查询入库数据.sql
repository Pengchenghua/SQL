

with temp_a as 
(select
	post_date,
	p.order_code,
	dist_code,
	dist_name,
	p.source_type_name,
		case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end as order_class,
	a.location_code ,
	h.shop_name,	
	product_code ,
	m.goods_id ,
	m.goods_name,
	unit ,
	m.brand_name ,
	m.department_id ,
	m.department_name ,
	category_large_code ,
	category_large_name ,
	m.classify_large_code,
	m.classify_large_name,
	m.classify_middle_code ,
	m.classify_middle_name ,
	m.classify_small_code ,
	m.classify_small_name ,
	supplier_code,
	vendor_name,
	source_location_code,
	p.source_location_name ,
	local_purchase_flag,
	business_type_name,
	sum(txn_qty) as entry_qty,
	sum(txn_amt) as entry_amt
from
	(
	select
		substr(to_date(posting_time),1,8) as post_date,
		location_code ,
		wms_order_no ,
		product_code ,
		supplier_code,
		txn_qty,
		txn_price,
		txn_amt
	from
		csx_dw.dwd_cas_r_d_accounting_stock_detail
	where
		move_type in ('101A','102A')
		and posting_time < '2020-12-01 00:00:00'
		and posting_time >= '2020-11-01 00:00:00'
		and sdt <= '20210110'
		and sdt >= '20201001'
		and in_or_out=0 ) a
join (
	select
		p.order_code ,
		p.received_order_code ,
		p.source_type_name,
		p.super_class,
		source_location_code,
		p.source_location_name,
		local_purchase_flag
	from
		csx_dw.dws_scm_r_d_header_item_price p
	where
		items_status = 4
		and super_class != '2'
	group by
		p.received_order_code ,
		p.source_type_name,
		p.super_class,
		source_location_code,
		p.source_location_name,
		order_code,
		local_purchase_flag ) as p on	a.wms_order_no = p.received_order_code
left join (
	select
		m.goods_id ,
		m.goods_name,
		m.unit_name  unit,
		m.brand_name ,
		m.department_id ,
		m.department_name ,
		category_large_code ,
		category_large_name ,
		m.classify_large_code,
		m.classify_large_name,
		m.classify_middle_code ,
		m.classify_middle_name ,
		m.classify_small_code ,
		m.classify_small_name 
	from
		csx_dw.dws_basic_w_a_csx_product_m m
	where
		sdt = 'current') m on a.product_code=m.goods_id 
left  join 
(select s.vendor_id ,s.vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m s where sdt='current') as s on s.vendor_id =a.supplier_code
left  join 
(select h.location_code,h.shop_name,dist_code,dist_name from csx_dw.csx_shop h where sdt='current') h on h.location_code =a.location_code
left  join 
(select e.order_code ,e.business_type_name from csx_dw.dws_wms_r_d_entry_detail e where sdt>='20200101' group by e.order_code ,e.business_type_name)e on a.wms_order_no=e.order_code 
group by
	p.order_code,
	post_date,
	dist_code,
	dist_name,
	p.source_type_name,
	p.super_class,
	a.location_code ,
	h.shop_name,	
	product_code ,
	m.goods_id ,
	m.goods_name,
	unit ,
	m.brand_name ,
	m.department_id ,
	m.department_name ,
	category_large_code ,
	category_large_name ,
	m.classify_large_code,
	m.classify_large_name,
	m.classify_middle_code ,
	m.classify_middle_name ,
	m.classify_small_code ,
	m.classify_small_name ,
	supplier_code,
	vendor_name,
	source_location_code,
	p.source_location_name ,
	local_purchase_flag,
	business_type_name
),
temp_b as 
(select
	post_date,
	p.order_code,
	dist_code,
	dist_name,
	p.source_type_name,
		case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end as order_class,
	a.location_code ,
	h.shop_name,	
	product_code ,
	m.goods_id ,
	m.goods_name,
	unit ,
	m.brand_name ,
	m.department_id ,
	m.department_name ,
	category_large_code ,
	category_large_name ,
	m.classify_large_code,
	m.classify_large_name,
	m.classify_middle_code ,
	m.classify_middle_name ,
	m.classify_small_code ,
	m.classify_small_name ,
	supplier_code,
	vendor_name,
	source_location_code,
	p.source_location_name ,
	local_purchase_flag,
	business_type,
	sum(txn_qty) as shipped_qty,
	sum(txn_amt) as shipped_amt
from
	(
	select
		substr(to_date(posting_time),1,8) as post_date,
		location_code ,
		wms_order_no ,
		product_code ,
		supplier_code,
		txn_qty,
		txn_price,
		txn_amt
	from
		csx_dw.dwd_cas_r_d_accounting_stock_detail
	where
		move_type in ('103A','104A')
		and posting_time < '2020-12-01 00:00:00'
		and posting_time >= '2020-11-01 00:00:00'
		and sdt <= '20210110'
		and sdt >= '20201001'
		and in_or_out=1 ) a
join (
	select
		p.order_code,
		p.shipped_order_code ,
		p.source_type_name,
		p.super_class,
		source_location_code,
		p.source_location_name ,
		local_purchase_flag
	from
		csx_dw.dws_scm_r_d_header_item_price p
	where
		items_status = 4
		and super_class in( '2','4')
	group by
		p.order_code,
		p.shipped_order_code ,
		p.source_type_name,
		p.super_class,
		source_location_code,
		p.source_location_name ,
		local_purchase_flag ) as p on	a.wms_order_no = p.shipped_order_code
 join (
	select
		m.goods_id ,
		m.goods_name,
		m.unit_name  unit,
		m.brand_name ,
		m.department_id ,
		m.department_name ,
		category_large_code ,
		category_large_name ,
		m.classify_large_code,
		m.classify_large_name,
		m.classify_middle_code ,
		m.classify_middle_name ,
		m.classify_small_code ,
		m.classify_small_name 
	from
		csx_dw.dws_basic_w_a_csx_product_m m
	where
		sdt = 'current') m on a.product_code=m.goods_id 
left  join 
(select s.vendor_id ,s.vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m s where sdt='current') as s on s.vendor_id =a.supplier_code
left join 
(select h.location_code,h.shop_name,dist_code,dist_name from csx_dw.csx_shop h where sdt='current') h on h.location_code =a.location_code
 left join 
(select e.order_no ,e.business_type from csx_dw.wms_shipped_order e where sdt>='20200101' group by e.order_no ,e.business_type) e on a.wms_order_no=e.order_no 
group by
	local_purchase_flag,
	business_type,
	p.order_code,
	post_date,
	dist_code,
	dist_name,
	p.source_type_name,
	p.super_class,
	a.location_code ,
	h.shop_name,	
	product_code ,
	m.goods_id ,
	m.goods_name,
	unit ,
	m.brand_name ,
	m.department_id ,
	m.department_name ,
	category_large_code ,
	category_large_name ,
	m.classify_large_code,
	m.classify_large_name,
	m.classify_middle_code ,
	m.classify_middle_name ,
	m.classify_small_code ,
	m.classify_small_name ,
	supplier_code,
	vendor_name,
	source_location_code,
   p.source_location_name 
)
select
	order_code,
	post_date,
	dist_code,
	dist_name,
	source_type_name,
	case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end as order_class,
	location_code ,
	shop_name,	
	product_code ,
	goods_name,
	unit ,
	brand_name ,
	department_id ,
	department_name ,
	category_large_code ,
	category_large_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code ,
	classify_middle_name ,
	classify_small_code ,
	classify_small_name ,
	supplier_code,
	vendor_name,
	source_location_code,
    source_location_name,
    local_purchase_flag,
	business_type_name,
	sum(entry_qty)as entry_qty,
	sum(entry_amt)as entry_amt,
	sum(shipped_qty) as shipped_qty,
	sum(shipped_amt)as shipped_amt
from 
(
select
    order_code,
	post_date,
	dist_code,
	dist_name,
	source_type_name,
	super_class,
	location_code ,
	shop_name,	
	product_code ,
	goods_id ,
	goods_name,
	unit ,
	brand_name ,
	department_id ,
	department_name ,
	category_large_code ,
	category_large_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code ,
	classify_middle_name ,
	classify_small_code ,
	classify_small_name ,
	supplier_code,
	vendor_name,
	source_location_code,
	source_location_name,
	local_purchase_flag,
	business_type_name,
	 entry_qty,
	 entry_amt,
	0 as shipped_qty,
	0 as shipped_amt
from temp_a 
union all 
select
    order_code,
	post_date,
	dist_code,
	dist_name,
	source_type_name,
	super_class,
	location_code ,
	shop_name,	
	product_code ,
	goods_id ,
	goods_name,
    unit ,
	brand_name ,
	department_id ,
	department_name ,
	category_large_code ,
	category_large_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code ,
	classify_middle_name ,
	classify_small_code ,
	classify_small_name ,
	supplier_code,
	vendor_name,
	source_location_code,
	source_location_name ,
	local_purchase_flag,
	business_type as business_type_name,
	0 as entry_qty,
	0 as entry_amt,
	shipped_qty,
	shipped_amt
from temp_b 
)a 
where source_type_name!='项目合伙人'
group by 
	post_date,
	dist_code,
	dist_name,
	source_type_name,
	case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end,
	location_code ,
	shop_name,	
	product_code ,
	goods_name,
	unit ,
	brand_name ,
	department_id ,
	department_name ,
	category_large_code ,
	category_large_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code ,
	classify_middle_name ,
	classify_small_code ,
	classify_small_name ,
	supplier_code,
	vendor_name,
	source_location_code,
	source_location_name,
	local_purchase_flag,
	business_type_name ,
	order_code
;


'1266339','128','1286204','852358'

;
select * from csx_dw.dws_scm_r_d_header_item_price  where order_code ='POW0G7201231001128';