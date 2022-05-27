-- 供应商入库明细【】
create TEMPORARY TABLE  csx_tmp.temp_entry_as as 
select
    substr(sdt,1,6) as mon,
    b.provicne_name,
    supplier_code,
    a.supplier_name,
    goods_code,
    m.goods_name,
	m.unit,
	m.brand_name ,
	standard,
	m.department_id ,
	m.department_name ,
	m.category_large_code ,
	m.category_large_name ,
	m.classify_large_code,
	m.classify_large_name,
	m.classify_middle_code ,
	m.classify_middle_name ,
	m.classify_small_code ,
	m.classify_small_name,
    sum(receive_qty) receive_qty,
    sum(amount) as receive_amt
from csx_dw.dws_wms_r_d_entry_batch a 
join 
(select shop_id,purpose_name,case when purchase_org='P620' or shop_id='W0G1'  THEN '全国-平台' else province_name end provicne_name 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose IN ('01','03')) b on a.receive_location_code=b.shop_id
join
(select
		m.goods_id ,
		m.goods_name,
		m.unit_name  unit,
		m.brand_name ,
		m.standard,
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
		sdt = 'current') m on a.goods_code=m.goods_id
where sdt>='20210101'
    and order_type_code like 'P%'
    and business_type !='02'
    and receive_status in ('1','2')
group by substr(sdt,1,6),
    b.provicne_name,
    supplier_code,
    a.supplier_name,
    goods_code,
    	m.goods_name,
		m.unit ,
		m.brand_name ,
		m.department_id ,
		m.department_name ,
		m.category_large_code ,
		m.category_large_name ,
		m.classify_large_code,
		m.classify_large_name,
		m.classify_middle_code ,
		m.classify_middle_name ,
		m.classify_small_code ,
		m.classify_small_name,
		standard