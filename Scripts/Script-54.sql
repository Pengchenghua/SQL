-- 20201203 更改 SKU 统计按照 实收数量！=0 
SELECT 
dist_code,
dist_name,
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
COUNT(goods_code )order_sku,
count(case when receive_qty !=0  then goods_code end) receive_sku,
sum(order_qty )order_qty,
sum(order_amt )order_amt ,
sum(receive_qty )as receive_qty ,
sum(receive_amt )as receive_amt ,
coalesce(count(case when  receive_qty!=0  then goods_code end) /COUNT(goods_code ),0)  as sku_fill_rate,
coalesce(sum(receive_qty )/sum(order_qty ),0)  as qty_fill_rate,
coalesce(sum(receive_amt)/sum(order_amt ),0) as amt_fill_rate,
count(distinct a.order_code) as order_num
from
(
select 
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
order_code,
goods_code ,
max(plan_qty )order_qty,
max(price*plan_qty )order_amt ,
sum(receive_qty )as receive_qty ,
sum(price *receive_qty )as receive_amt ,
if(max(plan_qty )=sum(receive_qty ),1,0) as order_sign
from csx_dw.dws_wms_r_d_entry_detail a
where 
 sdt>='${sdate}' 
 and sdt<='${edate}'
--and super_class='1'
${if(len(ordertype)==0,"","and business_type_code in ('"+ordertype+"') ")}
${if(len(dc)==0,"","and a.receive_location_code in ('"+substitute(dc,",","','")+"')")}
${if(len(dept)==0,"","and (case when division_code ='10'then 'U01' else  department_id end) in ('"+substitute(dept,",","','")+"')")}
--${if(len(dc)==0,"","and dc_code in ('"+SUBSTITUTE(dc,",","','")+"')")}
${if(len(vendor)==0,"","and a.supplier_code in ('"+vendor+"') ")}
and receive_status =2
and order_type_code LIKE 'P%' and order_type_code <>'P02'
group by 
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name,
order_code ,
goods_code 
)a 
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop cs where sdt='current'and cs.location_type_code in ('1','2'))b on a.receive_location_code =b.location_code
group by
dist_code,
dist_name,
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name
order by dist_code,
	dist_name,
	a.receive_location_code ,
	receive_location_name ,
	a.supplier_code ,
	supplier_name;
	

select disttinct de from csx_dw.dws_basic_w_a_category_m where sdt='current';