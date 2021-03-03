
SELECT 
mon,
location_type,
dist_code,
dist_name,
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
department_id ,
department_name,
COUNT(goods_code )order_sku,
count(case when receive_qty !=0 then goods_code end) receive_sku,
sum(order_qty )order_qty,
sum(order_amt )order_amt ,
sum(receive_qty )as receive_qty ,
sum(receive_amt )as receive_amt ,
coalesce(count(case when receive_qty!=0 then goods_code end) /COUNT(goods_code ),0)  as sku_fill_rate,
coalesce(sum(receive_qty )/sum(order_qty ),0)  as qty_fill_rate,
coalesce(sum(receive_amt)/sum(order_amt ),0) as amt_fill_rate,
count(distinct a.order_code) as order_num
from
(
select 
substr(sdt,1,6) mon,
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
order_code,
goods_code ,
max(plan_qty )order_qty,
max(price*plan_qty )order_amt ,
sum(receive_qty )as receive_qty ,
sum(amount )as receive_amt ,
if(max(plan_qty )=sum(receive_qty ),1,0) as order_sign
from csx_dw.wms_entry_order a
where 
 sdt>='${sdate}' 
 and sdt<='${edate}'
--and super_class='1'
and receive_status =2
and entry_type_code LIKE 'P%' and entry_type_code <>'P02'
group by 
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name,
order_code ,
goods_code ,
substr(sdt,1,6)
)a 
join 
(select location_code,dist_code,dist_name,cs.location_type from csx_dw.csx_shop cs where sdt='current'and cs.location_type_code in ('1','2'))b on a.receive_location_code =b.location_code
join 
(select pm.goods_id ,pm.department_id ,pm.department_name from csx_dw.dws_basic_w_a_csx_product_m  pm where sdt='current') c on a.goods_code =c.goods_id
group by
dist_code,
dist_name,
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name,
mon,
location_type,
department_id ,department_name
order by mon,
location_type,
	dist_code,
	dist_name,
	a.receive_location_code ,
	receive_location_name ,
	department_id ,department_name,
	a.supplier_code ,
	supplier_name;
	

select
	mon,
	location_type,
	dist_code,
	dist_name,
	receive_location_code ,
	receive_location_name ,
	supplier_code ,
	supplier_name ,
	order_code,
	department_id,
	department_name,
	count(goods_code) order_sku,
	count(case when receive_qty!=0 then goods_code end)receive_sku,
	sum(order_qty) order_qty,
	sum(order_amt) order_amt ,
	sum(receive_qty) as receive_qty ,
	sum(receive_amt) as receive_amt ,
	coalesce(count(case when receive_qty!=0 then goods_code end) /COUNT(goods_code ),0)  as sku_fill_rate,
	coalesce(sum(receive_qty )/sum(order_qty ),0)  as qty_fill_rate,
	coalesce(sum(receive_amt)/sum(order_amt ),0) as amt_fill_rate,
	if(sum(receive_qty)!=sum(order_qty),'是','否') as note,	
	receive_close_date,
	plan_receive_date,
	create_date
from
	(select
	substr(sdt,1,6) mon,
	a.receive_location_code ,
	receive_location_name ,
	a.supplier_code ,
	supplier_name ,
	order_code,
	goods_code ,
	max(plan_qty) order_qty,
	max(price*plan_qty) order_amt ,
	sum(receive_qty) as receive_qty ,
	sum(amount) as receive_amt,
	to_date(close_time) receive_close_date,
	to_date(create_time)create_date,
	plan_receive_date
from
	csx_dw.wms_entry_order a
where
	 sdt>='${sdate}' 
 and sdt<='${edate}'
--and super_class='1'
and receive_status =2
and entry_type_code LIKE 'P%' and entry_type_code <>'P02'
group by
	substr(sdt,1,6) ,
	a.receive_location_code ,
	receive_location_name ,
	a.supplier_code ,
	supplier_name,
	order_code ,
	goods_code,
	to_date(close_time) ,
	to_date(create_time),
	plan_receive_date )a
join (
	select
		location_code, 
		dist_code, 
		dist_name,
		cs.location_type 
	from
		csx_dw.csx_shop cs
	where
		sdt = 'current'
		and cs.location_type_code in ('1','2') )b on a.receive_location_code = b.location_code
join 
(select pm.goods_id ,pm.department_id ,pm.department_name from csx_dw.dws_basic_w_a_csx_product_m  pm where sdt='current') c on a.goods_code =c.goods_id
group by
	order_code,
	dist_code,
	dist_name,
	receive_location_code ,
	receive_location_name ,
	supplier_code ,
	supplier_name,
	receive_close_date,
	plan_receive_date,
	create_date,
	location_type ,
	department_id,
	department_name,
	mon
order by
	mon,
	location_type,
	dist_code,
	dist_name,
	receive_location_code ,
	receive_location_name ,
	supplier_code ,
	supplier_name,
	create_date;



-- 有效期
SELECT 
--${if(len(ordertype)==0,""," a.source_type, ")}
dist_code,
dist_name,
div_id,
sum(order_sku )order_sku,
sum(receive_sku)receive_sku,
sum(order_qty )order_qty,
sum(order_amt )order_amt ,
sum(receive_qty )as receive_qty ,
sum(receive_amt )as receive_amt ,
sum(no_overdue_sku) as no_overdue_sku,
sum(no_overdue_qty) as no_overdue_qty,
sum(no_overdue_amt)as no_overdue_amt,
sum(overdue_num) as overdue_num,
--COUNT(distinct  a.supplier_code ) as supplier_num,
coalesce(sum(no_overdue_sku)/sum(order_sku ),0)  as sku_fill_rate,
coalesce(sum(no_overdue_qty)/sum(order_qty ),0)  as qty_fill_rate,
coalesce(sum(no_overdue_amt)/sum(order_amt ),0) as amt_fill_rate,
count(a.order_code) as order_num
from
(select 
--${if(len(ordertype)==0,""," a.source_type, ")}
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
order_code,
case when category_code in ('10','11') then '10' 
	when category_code in ('12','13') then '12'
	end div_id,
count(goods_code )order_sku,
count(case when receive_qty!=0 then goods_code end)receive_sku,
sum(order_qty )order_qty,
sum(order_amt )order_amt ,
sum(receive_qty )as receive_qty ,
sum(receive_amt )as receive_amt ,
COUNT(case when receive_close_date>regexp_replace(last_delivery_date,'-','')  and receive_qty!=0 then goods_code end) as no_overdue_sku,
sum(case when receive_close_date>regexp_replace(last_delivery_date,'-','') then receive_qty end) as no_overdue_qty,
sum(case when receive_close_date>regexp_replace(last_delivery_date,'-','') then receive_amt end)as no_overdue_amt,
COUNT(DISTINCT case when (receive_close_date>regexp_replace(to_date(last_delivery_date),'-','') OR receive_close_date='' ) then 1 end) as overdue_num
from csx_dw.ads_supply_order_flow a
where 
 sdt>='${sdate}' 
 and sdt<='${edate}'
and regexp_replace(to_date(last_delivery_date),'-','') <='${edate}'
and super_class='1'
 and category_code in ('12','13','11','10')
and order_status!=5
group by 
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name,
order_code,
case when category_code in ('10','11') then '10' 
	when category_code in ('12','13') then '12'
	end)a 
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop cs where sdt='current'and cs.location_type_code in ('1','2'))b on a.receive_location_code =b.location_code
group by
dist_code,
dist_name,
div_id
order by dist_code,dist_name;

select * from csx_dw.ads_supply_order_flow a
where 
 sdt>='${sdate}' 
 and sdt<='${edate}'
and regexp_replace(last_delivery_date,'-','') <='${edate}'
and super_class='1'
 and category_code in ('12','13','11','10')
and order_status!=5
;

select * from csx_dw.dws_scm_r_d_scm_order_m 


