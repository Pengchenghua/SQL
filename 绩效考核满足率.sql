with vendor_sku01

as
(select c.province_name,a.shop_id_in,c.shop_name,
sdt,order_code,a.vendor_id,b.vendor_name,
goodsid,plan_qty,plan_amount,receive_qty,amount
from 
(
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
where  sdt>='20200701'
 and sdt<='20200930' 
and receive_status=2 
and entry_type LIKE 'P%' and entry_type<>'P02'
group by sdt,order_code,
supplier_code,
goods_code,receive_location_code)a 
join (select shop_id,shop_name,province_name
from csx_dw.dws_basic_w_a_csx_shop_m 
where sdt='current' --and province_name='江苏省'
and sales_belong_flag in ('4_企业购','5_彩食鲜') 
and shop_id not in('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0H4','W0G1','W0K4','9978','9992','9993','9994','9995'))c on a.shop_id_in=c.shop_id
left join 
--(select vendor_id,vendor_name from dim.dim_vendor where edate='9999-12-31')b on lpad(a.vendor_id,10,'0')=lpad(b.vendor_id,10,'0')
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where frozen='0')b on lpad(a.vendor_id,10,'0')=lpad(b.vendor_id,10,'0')
)
select
province_name,
bd_id,bd_name,
sum(plsku),
sum(receive_sku)
from
(select 
province_name,
order_code,
bd_id,bd_name,
count(distinct a.goodsid) plsku,
count(distinct case when receive_qty<>0 and receive_qty is not null and coalesce(plan_qty,0)<=coalesce(receive_qty,0)
 then a.goodsid else null end) receive_sku
from  vendor_sku012 a 
join (select goods_id,
case when division_code in ('10','11') then '11'
  when division_code in ('12','13','14') then '12'
  else '20' end as bd_id, -- '事业部编码'
  case when division_code in ('10','11') then '生鲜事业部'
  when division_code in ('12','13','14') then '食百事业部'
  else '其他' end as bd_name, -- '事业部名称' 
  '' as firm_g2_id,'' as firm_g2_name,department_id as dept_id,department_name as dept_name
 from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')b on a.goodsid=b.goods_id 
group by province_name,order_code,bd_id,bd_name) a
group by province_name,
bd_id,bd_name;