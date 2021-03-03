select
	goods_code ,
	goods_name ,
	sum(amount),
	sum(shipped_qty)
from
	csx_dw.wms_shipped_order
where
	receive_location_code = 'W0A8'
	and shipped_location_code = 'W053'
	and send_sdt >= '20200101'
	and shipped_area_code='BZ02'
	and category_middle_code = '110508'
group by goods_code ,
	goods_name ;


select  
	goods_code ,
	goods_name,
	category_small_code ,
	category_small_name,
	sum(qty)qty,
	sum(amt)amt,
	sum(shipped_qty) shipped_qty ,
	sum(shipped_amt) shipped_amt,
	sum(sales_cost) sales_cost,
	sum(sales_qty) sales_qty,
	sum(sales_value) sales_value
	from (
select
	goods_code ,
	goods_name,
	category_small_code ,
	category_small_name,
	sum(receive_qty)qty,
	sum(amount)amt,
	0 shipped_qty ,
	0 shipped_amt,
	0 sales_cost,
	0 sales_qty,
	0 sales_value
from
	csx_dw.wms_entry_order
where
	sdt >= '20200901'
	and sdt<='20201031'
	and receive_location_code = 'W053'
	and category_middle_code = '110508'
group by 
goods_code ,
	goods_name,
	category_small_code ,
	category_small_name
	union all 
select
	goods_code ,
	goods_name ,
	category_small_code ,
	category_small_name,
	0 qty,
	0 amt,
	sum(shipped_qty) shipped_qty ,
	sum(amount) shipped_amt,
	0 sales_cost,
	0 sales_qty,
	0 sales_value

from
	csx_dw.wms_shipped_order a 
where
  shipped_location_code = 'W053'
	and send_sdt >= '20200901'
	and sdt<='20201031'
	-- and shipped_area_code='BZ02'
	and category_middle_code = '110508'
	and a.shipped_type like '调拨%'
group by 	goods_code ,
	goods_name ,
	category_small_code ,
	category_small_name
union all 
select
	goods_code ,
	goods_name ,
	category_small_code ,
	category_small_name,
	0 qty,
	0 amt,
	0 shipped_qty ,
	0 shipped_amt,
	sum(sales_cost) sales_cost,
	sum(sales_qty) sales_qty,
	sum(sales_value) sales_value
from
	csx_dw.dws_sale_r_d_customer_sale 
where
	dc_code in ('W0A8','W0M4','W0B6','W0K4','W0H7')
	and sdt >= '20200901'
	and sdt<='20201031'
	--and goods_code ='1122784'
	and category_middle_code = '110508'
group by 	goods_code ,
	goods_name ,
	category_small_code ,
	category_small_name
) a 
group by
	goods_code ,
	goods_name ,
	category_small_code ,
	category_small_name
;



select
	*
from
	csx_dw.wms_shipped_order
where
	 	shipped_location_code = 'W0A8'
	and  send_sdt >= '20200101'
	 and goods_code ='1122784'
	and category_middle_code = '110508'
;

select
	dc_code ,
	city_name ,
	sum(sales_value)
from
	csx_dw.dws_sale_r_d_customer_sale 
where
	   sdt >= '20200101'
	-- and goods_code ='1122784'
	and category_middle_code = '110508'
	and province_code ='15'
	
group by dc_code,city_name 
;

select
	*
from
	csx_dw.dws_mms_w_a_factory_order_bom_m
where
	sdt >= '20200101'
	and goods_type_name like '分%型'
	and factory_location_code = 'W053'
	and product_code ='5990'
	and goods_name like '鲜伊%';

SELECT
	*
FROM
	csx_dw.dws_mms_w_a_factory_bom_m
where
	sdt >= '20200101'
	and goods_type_name like '分%型'
	and factory_location_code = 'W053'
	and product_code = '5990'
	and goods_name like '鲜伊%';

-- 查询部位肉入库
select receive_location_code ,receive_location_name ,substr(sdt,1,6) mon ,goods_code ,goods_name,sum(receive_qty),category_middle_name,category_middle_code from csx_dw.wms_entry_order  
where goods_code !='5990' and sdt>='20200901' and sdt<='20201130'
and category_large_code ='1105'
--and goods_code  in ('1229845','1229845','1229831','1229839','1229837','1229853','1229851','1229830','1229835','1229847','1229846','1229834','1229842','1229838','1229840','1229843','1229844','1229833','1229841','1229852')
and receive_location_code  in ('W053')
group by receive_location_code ,receive_location_name ,substr(sdt,1,6)  ,goods_code ,goods_name,category_middle_name,category_middle_code;


select * from csx_dw.dws_mms_w_a_factory_order_bom_m where sdt>='20200101' and product_code ='5990' and factory_location_code ='W053';

select * from csx_dw.dws_mms_w_a_factory_bom_m where sdt>='20200101' and product_code ='5990' and factory_location_code ='W053';

select * from csx_dw.dws_mms_r_a_factory_order where sdt>='20200101' and goods_code ='900161' and location_code ='W053';

SELECT * from csx_ods.source_mms_r_a_factory_mr_receive_order where sdt='20201119';


select * from csx_dw.dws_mms_r_a_factory_tranfer where sdt='20201101' ;
 
with goods_01 as 
(select
	order_code,
	a.goods_code ,
	goods_name,
	category_middle_code,
	category_middle_name,
	goods_unit,
	goods_spec,
	goods_reality_receive_qty,
	product_code,
	product_name,
	product_unit,
	fact_qty,
	sdt 
from
	csx_dw.dws_mms_r_a_factory_order a
left  join (
select
	goods_id,
	category_middle_code,
	category_middle_name
from
	csx_dw.dws_basic_w_a_csx_product_m
where
			sdt = 'current'
	)b on
	a.goods_code = b.goods_id
where
	sdt >= '20201001'
	and sdt<='20201031'
	and location_code = 'W053'
	--and mrp_prop_key !='3062'
	and product_code = '5990'
),
goods_02 as 
(
select
	a.goods_code ,
	a.product_code,
	sum(goods_reality_receive_qty) as z_qty,
	sum(goods_reality_receive_qty*amount) as tr_qty,	
	sum(fact_qty)z_fact_qty
from
	csx_dw.dws_mms_r_a_factory_order a
 join (select	
	a.goods_code 	
from
	csx_dw.dws_mms_r_a_factory_order a
	where sdt >= '20201001'
	and sdt<='20201031'
	and product_code !='5990'
	group by goods_code )b on
	a.product_code = b.goods_code
join 
(select order_code ,
product_code,
	amount
from csx_dw.dws_mms_w_a_factory_order_bom_m 
where sdt='current' 
and mrp_prop_key ='3010'
) c on a.order_code =c.order_code and a.product_code =c.product_code
where
	sdt >= '20201001'
	and sdt<='20201031'
	and location_code = 'W053'
	and line_code ='H05_01'
group by a.goods_code ,
	goods_unit,
	a.product_code
)
select
	a.goods_code,
	a.goods_name,
	a.goods_unit,
	SUM(a.goods_reality_receive_qty) qty ,
	sum(a.fact_qty) fact_qty,
	a.product_code,
	b.goods_code,
	 z_qty,
	 z_fact_qty,
     tr_qty,
	b.product_code
from
	goods_01 a 
	left join 
	goods_02 b on a.goods_code=b.product_code
group by
	a.goods_code,
	a.goods_name,
	a.goods_unit,
	a.product_code,
	b.goods_code,
	 z_qty,
	 z_fact_qty,
     tr_qty,
	b.product_code
;

-- 部位肉分割查询
select 
	goods_code ,
	goods_name ,
	goods_unit ,
	product_code ,product_name ,
	 z_qty,
	tr_qty,
	z_fact_qty,
	z_fact_qty/sum(z_fact_qty)over(partition by product_code) as ration
	from (
select 
	goods_code ,
	goods_name ,
	goods_unit ,
	product_code ,product_name ,
	sum(goods_reality_receive_qty)as z_qty,
	sum(goods_reality_receive_qty*amount*1.00) as tr_qty,
	sum(fact_qty)z_fact_qty
	from (
select
	a.order_code,
	a.goods_code ,
	goods_name,
	goods_unit,
	goods_spec,
	amount,
	goods_reality_receive_qty,
	a.product_code,
	product_name,
	product_unit,
	fact_qty,
	sdt 
from
	csx_dw.dws_mms_r_a_factory_order a
 join (select	
	a.goods_code 	
from
	csx_dw.dws_mms_r_a_factory_order a
	where sdt >= '20201001'
	and sdt<='20201031'
	and product_code !='5990'
	group by goods_code )b on
	a.product_code = b.goods_code
join 
(select order_code ,
product_code,
	amount
from csx_dw.dws_mms_w_a_factory_order_bom_m 
where sdt='current' 
and mrp_prop_key ='3010'
) c on a.order_code =c.order_code and a.product_code =c.product_code
where
	sdt >= '20201001'
	and sdt<='20201031'
	and location_code = 'W053'
	and a.product_code in ('1229829','1229845','1229831','1229839','1229837','1229853','1229851','1229830','1229835','1229847','1229846','1229834','1229842','1229838','1229840','1229843','1229844','1229833','1229841','1229852')
	and line_code ='H05_01'
) a 
group by 
goods_code ,
	goods_name ,
	goods_unit ,
	product_code ,product_name 
) a;


select
	order_code,
	a.goods_code ,
	goods_name,
	goods_unit,
	goods_spec,
	goods_reality_receive_qty,
	product_code,
	product_name,
	product_unit,
	fact_qty,
	sdt 
from
	csx_dw.dws_mms_r_a_factory_order a
 
where
	sdt >= '20201001'
	and sdt<='20201131'
	and product_code ='726017'
	and location_code = 'W053'
	and line_code ='H05_01'

;

select * from csx_dw.dws_mms_w_a_factory_order_bom_m where sdt='current' and order_code ='WO201106004595';


-- 片肉分割查询
select a.goods_code,
	a.goods_name,
	a.goods_unit,
	qty ,
	fact_qty,
	fact_values,
	a.product_code
from (
select
	a.goods_code,
	a.goods_name,
	a.goods_unit,
	SUM(a.goods_reality_receive_qty) qty ,
	sum(fact_values)fact_values,
	sum(a.fact_qty) fact_qty,
	a.product_code
from (
select
	order_code,
	a.goods_code ,
	b.goods_name,
	category_middle_code,
	category_middle_name,
	goods_unit,
	goods_spec,
	goods_reality_receive_qty,
	product_code,
	product_name,
	product_unit,
	fact_qty,
	fact_values,
	sdt 
from
	csx_dw.dws_mms_r_a_factory_order a
left  join (
select
	goods_id,
	goods_name,
	category_middle_code,
	category_middle_name
from
	csx_dw.dws_basic_w_a_csx_product_m
where
	sdt = 'current'
	)b on
	a.goods_code = b.goods_id
where
	sdt >= '20201201'
	and sdt<='20201231'
	and location_code = 'W079'
	-- and mrp_prop_key !='3062'
	and product_code = '5990'
) a 
group by 
	a.goods_code,
	a.goods_name,
	a.goods_unit,
	a.product_code
) a 	
;


select * from csx_dw.csx_shop where sdt='current' and location_type_code ='2';



select 
	goods_code ,
	goods_name ,
	goods_unit ,
	product_code ,product_name ,
	 z_qty,
	tr_qty,
	z_fact_qty,
	fact_values,
	z_fact_qty/sum(z_fact_qty)over(partition by product_code) as ration
	from (
select 
	goods_code ,
	goods_name ,
	goods_unit ,
	product_code ,product_name ,
	sum(goods_reality_receive_qty)as z_qty,					--实际数量
	sum(goods_reality_receive_qty*amount*1.00) as tr_qty,   --单位转换数量kg
	sum(fact_qty)z_fact_qty,           --原料数量
	sum(fact_values) as fact_values    --原料金额
	from (
select
	a.order_code,
	a.goods_code ,
	goods_name,
	goods_unit,
	goods_spec,
	amount,						-- 物料数量
	goods_reality_receive_qty,   --实际产量
	a.product_code,
	product_name,
	product_unit,
	fact_qty,
	fact_values,
	sdt 
from
	csx_dw.dws_mms_r_a_factory_order a
 join (select	
	a.goods_code 	
from
	csx_dw.dws_mms_r_a_factory_order a
	where sdt >= '20201201'
	and sdt<='20201231'
	and product_code !='5990'
	group by goods_code )b on
	a.product_code = b.goods_code
join 
(select order_code ,
product_code,
	amount    -- 物耗数量
from  csx_dw.dws_mms_w_a_factory_order_bom_m 
where sdt='current' 
and mrp_prop_key ='3010'
) c on a.order_code =c.order_code and a.product_code =c.product_code
where
	sdt >= '20201201'
	and sdt<='20201231'
	and location_code = 'W079'
	and a.product_code in ('8811','4151','317063','2184','8785','317064','5632','8807')
	and line_code ='H05_01'
) a 
group by 
goods_code ,
	goods_name ,
	goods_unit ,
	product_code ,product_name 
) a;

select
	*
from
	csx_dw.dws_mms_w_a_factory_order_bom_m
where
	sdt = 'current'
	and factory_location_code = 'W079'
	and goods_code = '1291795'
	;
select * from csx_dw.dws_mms_r_a_factory_order where sdt>='20201001' and goods_code ='1277732' and location_code ='W079';


