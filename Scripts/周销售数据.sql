with sale as (select
	weeknum
	, dc_code
	, goods_code
	, sum(sales_qty)sales_qty
	, sum(sales_value)sales_value
	, sum(profit)profit
from
	csx_dw.dws_sale_r_d_customer_sale a
join 
(select calday,concat(substr(calday,1,4),case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) else  cast (new_weeknum as string ) end) weeknum from csx_tmp.temp_date_m c )b on
	a.sdt = b.calday
where
	sdt >= '20190101'
	and sdt<'20201201'
	and  a.category_large_code  in ('1101')
group by	
 weeknum,
	dc_code
	, goods_code 
),
entry as  (
select
	weeknum
	, receive_location_code as dc_code
	, goods_code
	, supplier_code
	, sum(receive_qty)qty
from
	csx_dw.wms_entry_order a
join
(select calday,concat(substr(calday,1,4),case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) else  cast (new_weeknum as string ) end) weeknum from csx_tmp.temp_date_m c ) b on
	a.sdt = b.calday
where
	sdt >= '20190101'
	and sdt <= '20201130'
	and  a.category_large_code  in ('1101')
	and ((entry_type_code like 'P%' and business_type_code !='02') or business_type ='采购入库(old)')
group by
	weeknum
	, receive_location_code
	, goods_code
	, supplier_code
)
select a.weeknum
	,s.zone_id 
	,s.zone_name 
	,dist_code
	,dist_name
	,a.dc_code
	,shop_name
	,s.prefecture_city_name
	, a.goods_code
    , goods_name
	, bar_code
	, standard
	, unit_name
	, classify_large_code
	, classify_large_name
	, classify_middle_code
	, classify_middle_name 
	, classify_small_code 
	, classify_small_name 
	, supplier_code
	, vendor_name
	, sales_qty
	, sales_value
	, profit
	,b.qty 
from sale a 
join 
(select
	m.goods_id
	, m.goods_name
	, m.bar_code
	, m.unit_name 
	, m.standard 
	, m.classify_large_code
	, m.classify_large_name
	, m.classify_middle_code
	, m.classify_middle_name 
	,m.classify_small_code 
	,m.classify_small_name 
from
	csx_dw.dws_basic_w_a_csx_product_m m
where
	sdt = 'current')c on a.goods_code=goods_id 
join 
(select s.zone_id ,s.zone_name ,dist_code,dist_name,location_code,shop_name,s.prefecture_city_name from csx_dw.csx_shop s where sdt='current') s on s.location_code =a.dc_code
left join 
(select e.*,vendor_name from entry e 
join 
(select o.vendor_id,o.vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m o where sdt='current')o on e.supplier_code= o.vendor_id) b 
on a.weeknum=b.weeknum and a.dc_code=b.dc_code and a.goods_code=b.goods_code

;



with entry as  (
select
    supplier_code
from
	csx_dw.wms_entry_order a
where
	sdt >= '20190101'
	and sdt <= '20201130'
	and  a.category_large_code  in ('1101')
	and ((entry_type_code like 'P%' and business_type_code !='02') or business_type ='采购入库(old)')
group by
	 supplier_code
)select a.supplier_code ,vendor_name,acct_grp,vat_regist_num,s.fixed_credit_line ,s.vendor_pur_lvl,s.vendor_pur_lvl_name  from entry a 
join 
(select  vendor_id,vendor_name,acct_grp,vat_regist_num,s.fixed_credit_line ,s.vendor_pur_lvl,s.vendor_pur_lvl_name 
from csx_dw.dws_basic_w_a_csx_supplier_m s where sdt='current') s on a.supplier_code=s.vendor_id 

;

-- 
with sale as 
(select
	weeknum
	, dc_code
	, goods_code
	, sum(sales_qty)sales_qty
	, sum(sales_value)sales_value
	, sum(profit)profit
	, sum(entry_qty) entry_qty
from (select
	weeknum
	, dc_code
	, goods_code
	, sum(sales_qty)sales_qty
	, sum(sales_value)sales_value
	, sum(profit)profit
	, 0 entry_qty
from
	csx_dw.dws_sale_r_d_customer_sale a
join 
(select calday,concat(substr(calday,1,4),
case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) 
	else  cast (new_weeknum as string ) end) weeknum from csx_tmp.temp_date_m c )b on
	a.sdt = b.calday
where
	sdt >= '20200101'
	and sdt<'20201201'
	and  a.category_middle_code  in ('110132')
group by	
 weeknum,
	dc_code
	, goods_code 
union all 
select
	weeknum
	, receive_location_code as dc_code
	, goods_code
	, 0 sales_qty
	, 0 sales_value
	, 0 profit
	, sum(receive_qty)entry_qty
from
	csx_dw.wms_entry_order a
join
(select calday,concat(substr(calday,1,4),case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) else  cast (new_weeknum as string ) end) weeknum from csx_tmp.temp_date_m c ) b on
	a.sdt = b.calday
where
	sdt >= '20200101'
	and sdt <= '20201130'
	and  a.category_middle_code  in ('110132')
	and ((entry_type_code like 'P%' and business_type_code !='02') or business_type ='采购入库(old)')
group by
	weeknum
	, receive_location_code
	, goods_code
	, supplier_code
)a 
group by 
weeknum
	, dc_code
	, goods_code
)
select a.weeknum
	,s.zone_id 
	,s.zone_name 
	,dist_code
	,dist_name
	,a.dc_code
	,shop_name
	,s.prefecture_city_name
	, a.goods_code
    , goods_name
	, bar_code
	, standard
	, unit_name
	, classify_large_code
	, classify_large_name
	, classify_middle_code
	, classify_middle_name 
	, classify_small_code 
	, classify_small_name 
	, supplier_code
	, supplier_name
	, (sales_value-profit )/sales_qty as cost
	, sales_value/sales_qty price
	, sales_qty
	, sales_value
	, profit
	, profit/sales_value profit_rate
	, entry_qty 
from sale a 
join 
(select
	m.goods_id
	, m.goods_name
	, m.bar_code
	, m.unit_name 
	, m.standard 
	, m.classify_large_code
	, m.classify_large_name
	, m.classify_middle_code
	, m.classify_middle_name 
	, m.classify_small_code 
	, m.classify_small_name 
from
	csx_dw.dws_basic_w_a_csx_product_m m
where
	sdt = 'current')c on a.goods_code=goods_id 
join 
(select s.zone_id ,s.zone_name ,dist_code,dist_name,location_code,shop_name,s.prefecture_city_name from csx_dw.csx_shop s where sdt='current' 
-- and s.table_type =1 and location_type_code='1'
) s on s.location_code =a.dc_code
join 
(select o.shop_code ,o.product_code,o.supplier_code,o.supplier_name from csx_dw.dws_basic_w_a_csx_product_info o where sdt='current')o on  a.dc_code=o.shop_code and a.goods_code=o.product_code

;
;
with sale as 
(select
	weeknum
	, dc_code
	, goods_code
	, sum(sales_qty)sales_qty
	, sum(sales_value)sales_value
	, sum(profit)profit
	, sum(entry_qty) entry_qty
from (select
	weeknum
	, province_name ,
	, city_group_name 
	, channel 
	, goods_code
	, sum(sales_qty)sales_qty
	, sum(sales_value)sales_value
	, sum(profit)profit
	, 0 entry_qty
from
	csx_tmp.sale_01 a
join 
(select calday,concat(substr(calday,1,4),
case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) 
	else  cast (new_weeknum as string ) end) weeknum from csx_tmp.temp_date_m c )b on
	a.sdt = b.calday
where
	sdt >= '20200101'
	and sdt<'20201201'
	and  a.category_middle_code  in ('110132')
group by	
 weeknum,
	dc_code
	, goods_code 
union all 
select
	weeknum
	, receive_location_code as dc_code
	, goods_code
	, 0 sales_qty
	, 0 sales_value
	, 0 profit
	, sum(receive_qty)entry_qty
from
	csx_dw.wms_entry_order a
join
(select calday,concat(substr(calday,1,4),case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) else  cast (new_weeknum as string ) end) weeknum from csx_tmp.temp_date_m c ) b on
	a.sdt = b.calday
where
	sdt >= '20200101'
	and sdt <= '20201130'
	and  a.category_middle_code  in ('110132')
	and ((entry_type_code like 'P%' and business_type_code !='02') or business_type ='采购入库(old)')
group by
	weeknum
	, receive_location_code
	, goods_code
	, supplier_code
)a 
group by 
weeknum
	, dc_code
	, goods_code
)
select a.weeknum
	,s.zone_id 
	,s.zone_name 
	,dist_code
	,dist_name
	,a.dc_code
	,shop_name
	,s.prefecture_city_name
	, a.goods_code
    , goods_name
	, bar_code
	, standard
	, unit_name
	, classify_large_code
	, classify_large_name
	, classify_middle_code
	, classify_middle_name 
	, classify_small_code 
	, classify_small_name 
	, supplier_code
	, supplier_name
	, (sales_value-profit )/sales_qty as cost
	, sales_value/sales_qty price
	, sales_qty
	, sales_value
	, profit
	, profit/sales_value profit_rate
	, entry_qty 
from sale a 
join 
(select
	m.goods_id
	, m.goods_name
	, m.bar_code
	, m.unit_name 
	, m.standard 
	, m.classify_large_code
	, m.classify_large_name
	, m.classify_middle_code
	, m.classify_middle_name 
	, m.classify_small_code 
	, m.classify_small_name 
from
	csx_dw.dws_basic_w_a_csx_product_m m
where
	sdt = 'current')c on a.goods_code=goods_id 
join 
(select s.zone_id ,s.zone_name ,dist_code,dist_name,location_code,shop_name,s.prefecture_city_name from csx_dw.csx_shop s where sdt='current' 
-- and s.table_type =1 and location_type_code='1'
) s on s.location_code =a.dc_code
join 
(select o.shop_code ,o.product_code,o.supplier_code,o.supplier_name from csx_dw.dws_basic_w_a_csx_product_info o where sdt='current')o on  a.dc_code=o.shop_code and a.goods_code=o.product_code

;