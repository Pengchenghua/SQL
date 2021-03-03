
select 
sdt,order_code,
supplier_code vendor_id,
goods_code goodsid,receive_location_code shop_id_in,
max(case when return_flag='Y' then -1*plan_qty else plan_qty end) plan_qty,
max(case when return_flag='Y' then -1*plan_qty*price else plan_qty*price end) plan_amount,
sum(case when return_flag='Y' then -1*receive_qty else receive_qty end) receive_qty,
sum(case when return_flag='Y' then -1*receive_qty*price else receive_qty*price end) amount
--from csx_dw.wms_entry_order_m a
from csx_dw.dwd_wms_r_d_entry_order_detail a
where  sdt>='20200901'
 and sdt<='20200930' 
 AND a.receive_location_code='W0A8'
and receive_status=2 
and entry_type LIKE 'P%' and entry_type<>'P02'
group by sdt,order_code,
supplier_code,
goods_code,receive_location_code
;


SELECT 
dist_code,
dist_name,
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
COUNT(goods_code )order_sku,
count(case when receive_qty!=0 then goods_code end) receive_sku,
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
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
order_code,
goods_code ,
max(plan_qty )order_qty,
max(price*plan_qty )order_amt ,
sum(receive_qty )as receive_qty ,
sum(amount )as receive_amt 
from csx_dw.wms_entry_order a
where 
 sdt>='${sdate}' 
 and sdt<='${edate}'
--and super_class='1'
and receive_location_code ='W0A8'
and goods_code ='1261180'
and receive_status =2
and entry_type_code LIKE 'P%' and entry_type_code <>'P02'
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

select
	dist_code,
	dist_name,
	receive_location_code ,
	receive_location_name ,
	supplier_code ,
	supplier_name ,
	order_code,
	count(goods_code) order_sku,
	count(case when receive_qty != 0 then goods_code end)receive_sku,
	sum(order_qty) order_qty,
	sum(order_amt) order_amt ,
	sum(receive_qty) as receive_qty ,
	sum(receive_amt) as receive_amt ,
	if(sum(receive_qty)!=sum(order_qty),'是','否') as note,
	receive_close_date,
	plan_receive_date,
	create_date
from
	(select
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
	sdt >= '${sdate}'
	and sdt <= '${edate}'
	--and super_class='1'
	and receive_location_code = 'W0A8'
	and goods_code = '1261180'
	and receive_status = 2
	and entry_type_code LIKE 'P%'
	and entry_type_code <> 'P02'
group by
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
		location_code, dist_code, dist_name
	from
		csx_dw.csx_shop cs
	where
		sdt = 'current'
		and cs.location_type_code in ('1','2') )b on a.receive_location_code = b.location_code
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
	create_date
order by
	dist_code,
	dist_name,
	receive_location_code ,
	receive_location_name ,
	supplier_code ,
	supplier_name,
	create_date;
