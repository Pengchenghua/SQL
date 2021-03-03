WITH entry_tmp as 
(select
	company_code
	, company_name
	, dist_code
	, dist_name
	, dc_code
	, shop_name
	, supplier_code
	, vendor_name
	, vat_regist_num
	, vendor_pur_lvl
	, vendor_pur_lvl_name
	, goods_code
	, goods_name
	, bar_code
	, brand_name
	, standard
	, division_code
	, division_name
	, category_large_code
	, category_large_name
	, category_middle_code
	, category_middle_name
	, category_small_code
	, category_small_name
	, classify_large_code
	, classify_large_name
	, classify_middle_code
	, classify_middle_name
	, classify_small_code
	, classify_small_name
	, purchase_goods_level
	, purchase_goods_level_name
	, department_id
	, department_name
	, tax_rate
	, valuation_category_code
	, valuation_category_name
	, unit
	, sum(purchase_qty) as purchase_qty
	, sum(purchase_amt) as purchase_amt
	, sum(return_qty) as return_qty
	, sum(return_amt) as return_amt
	, sum(purchase_amt /(1 + tax_rate / 100)) no_tax_purchase_amt
	, sum(return_amt /(1 + tax_rate / 100)) no_tax_return_amt
	, posting_date
	, sdt
from
	(
	select
		sdt
		, receive_location_code dc_code
		, order_code
		, supplier_code
		, goods_code
		, unit
		, sum(receive_qty) as purchase_qty
		, sum(amount) as purchase_amt
		, 0 return_qty
		, 0 return_amt
	from
		csx_dw.wms_entry_order a
	where
		sdt >= '20201201'
		and sdt <= '20201231'
--		and a.department_id='A02'
--		and receive_location_code ='W0A7'
 		and (entry_type_code like 'P%' or entry_type_code like 'T%' )
		and receive_status = 2
	group by
		sdt
		, receive_location_code
		, supplier_code
		, goods_code
		, unit
		, order_code
		,entry_type
		,entry_type_code
union all
	select
		send_sdt as sdt
		, shipped_location_code as dc_code
		, order_no order_code
		, supplier_code
		, goods_code
		, unit
		, 0 purchase_qty
		, 0 purchase_amt
		, sum(coalesce(shipped_qty, 0)) as return_qty
		, sum(amount) as return_amt
	from
		csx_dw.wms_shipped_order
		where
		send_sdt >= '20201201'
		and send_sdt <= '20201231'
		and status in (6, 7, 8)
		and (substr(shipped_type_code,1,2) in('P0','RP','T0','')
	group by
		send_sdt
		, shipped_type
		, shipped_type_code
		, shipped_location_code
		, supplier_code
		, goods_code
		, unit
		, order_no ) a
left join (
	select
		wms_order_no
		,product_code
		,sdt posting_date
	from
		csx_dw.dwd_cas_r_d_accounting_credential_item
	group by
		wms_order_no
		, sdt,
		product_code) j on
	a.order_code = j.wms_order_no and a.goods_code = j.product_code
join (
	select
		location_code
		, shop_name
		, dist_code
		, dist_name
		, company_code
		, company_name
	from
		csx_dw.csx_shop
	where
		sdt = 'current')b on
	a.dc_code = b.location_code
join (
	select
		vendor_id
		, vendor_name
		, vat_regist_num
		, vendor_pur_lvl
		, vendor_pur_lvl_name
	from
		csx_dw.dws_basic_w_a_csx_supplier_m
	where
		sdt = 'current' )c on
	a.supplier_code = c.vendor_id
join (
	select
		goods_id
		, goods_name
		, bar_code
		, brand_name
		, standard
		, division_code
		, division_name
		, category_large_code
		, category_large_name
		, category_middle_code
		, category_middle_name
		, category_small_code
		, category_small_name
		, classify_large_code
		, classify_large_name
		, classify_middle_code
		, classify_middle_name
		, classify_small_code
		, classify_small_name
		, purchase_goods_level
		, purchase_goods_level_name
		, department_id
		, department_name
		, tax_rate
		, valuation_category_code
		, valuation_category_name
	from
		csx_dw.dws_basic_w_a_csx_product_m
	where
		sdt = 'current') d on
	a.goods_code = d.goods_id	
group by
	company_code
	, company_name
	, dist_code
	, dist_name
	, dc_code
	, shop_name
	, supplier_code
	, vendor_name
	, vat_regist_num
	, vendor_pur_lvl
	, vendor_pur_lvl_name
	, goods_code
	, goods_name
	, bar_code
	, brand_name
	, standard
	, division_code
	, division_name
	, category_large_code
	, category_large_name
	, category_middle_code
	, category_middle_name
	, category_small_code
	, category_small_name
	, classify_large_code
	, classify_large_name
	, classify_middle_code
	, classify_middle_name
	, classify_small_code
	, classify_small_name
	, purchase_goods_level
	, purchase_goods_level_name
	, department_id
	, department_name
	, tax_rate
	, valuation_category_code
	, valuation_category_name
	, unit
	, a.sdt
	, posting_date
) select goods_code,goods_name ,sum(purchase_amt),sum(return_amt) from entry_tmp a 
where  a.dc_code='W0A7'
	and a.department_id='A02'
	group by goods_code,goods_name ;
	
refresh csx_dw.dws_crm_w_a_customer;
select * from csx_dw.dws_crm_w_customer where sdt='current' and customer_no='106773';
select * from  where sdt='20201204';

select customer_no,attribute,attribute_code,channel from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current' and customer_no ='114235';
