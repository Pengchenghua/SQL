
create temporary table csx_tmp.vendor_sku01
as
select 
  c.province_name,
  c.city_group_name,
  a.shop_id_in,
  a.shop_name,
  sdt,order_code,
  a.vendor_id,
  b.vendor_name,
  goodsid,
  plan_qty,
  plan_amount,
  receive_qty,
  amount
from 
(
  select 
  sdt,order_code,
  supplier_code vendor_id,
  goods_code goodsid,receive_location_code shop_id_in,receive_location_name shop_name,
  max(case when return_flag='Y' then -1*plan_qty else plan_qty end) plan_qty,
  max(case when return_flag='Y' then -1*plan_qty*price else plan_qty*price end) plan_amount,
  sum(case when return_flag='Y' then -1*receive_qty else receive_qty end) receive_qty,
  sum(case when return_flag='Y' then -1*receive_qty*price else receive_qty*price end) amount
  from csx_dw.dws_wms_r_d_entry_detail a
  where --sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','') and
   sdt>='20220101' and sdt<='20220331'
  and receive_status=2  --已关业务
  and order_type_code LIKE 'P%' and order_type_code<>'P02' --剔除调拨
  group by sdt,order_code,
  supplier_code,
  goods_code,receive_location_code,receive_location_name)a 
join 
(select shop_id,province_name,city_group_name from csx_dw.dws_basic_w_a_csx_shop_m cs where sdt='current'and cs.purpose in ('01','02','03')--1仓库2工厂3门店
)c on a.shop_id_in =c.shop_id
left join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current' and frozen='0')b 
on lpad(a.vendor_id,10,'0')=lpad(b.vendor_id,10,'0');



-----月满足率 取 sku 、qty、金额满足率的 均值 
select 
province_name,city_group_name,bd_name,
sum(receive_sku)/sum(sku)
from 
(select
province_name,city_group_name,
vendor_id,
vendor_name,
sdt,order_code,
bd_id,bd_name,firm_g2_id,firm_g2_name,dept_id,dept_name,--classify_middle_code,classify_middle_name,
count(distinct a.goodsid) sku,
count(distinct case when receive_qty<>0 and receive_qty is not null  and receive_qty>=plan_qty then a.goodsid else null end) receive_sku
from   csx_tmp.vendor_sku01 a 
join (select goods_id,
case when division_code in ('10','11') then '11'
  when division_code in ('12','13','14','15') then '12'
  else '20' end as bd_id, -- '事业部编码'
  case when division_code in ('10','11') then '生鲜事业部'
  when division_code in ('12','13','14','15') then '食百事业部'
  else '其他' end as bd_name, -- '事业部名称' 
  '' as firm_g2_id,'' as firm_g2_name,department_id as dept_id,department_name as dept_name,
  classify_middle_code,classify_middle_name
 from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')b on a.goodsid=b.goods_id 
group by province_name,city_group_name,vendor_id,vendor_name,
sdt,order_code,bd_id,bd_name,firm_g2_id,firm_g2_name,dept_id,dept_name,classify_middle_code,classify_middle_name) a
group by province_name,city_group_name,bd_name;
