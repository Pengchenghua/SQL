--指定客户销售情况 法务
select   years,
        customer_no,
        customer_name,
        classify_large_name,
        min_sdt,
       sum(excluding_tax_sales)excluding_tax_sales,
       sum(excluding_tax_profit)/sum(excluding_tax_sales) as  profit_rate
    from   

(select substr(sdt,1,4) years,
        customer_no,
        customer_name,
        classify_large_name,
        min(sdt)over(partition by customer_no)min_sdt,
        (excluding_tax_sales)excluding_tax_sales,
        (excluding_tax_profit)excluding_tax_profit
from csx_dw.dws_sale_r_d_detail 
where 
(customer_name like '%江淮%' or customer_name like '%福建省司法厅%' or customer_name like '%东方剑桥%')
and sdt>='20200101'

) a 
group by   years,
        customer_no,
        customer_name,
        classify_large_name,
        min_sdt;



--城市服务商
select substr(sdt,1,4) years,
a.business_type_name,
a.region_name,
a.province_name,
a.customer_no,
a.customer_name,
c.supplier_code,
c.supplier_name, 
classify_large_name,
min_sdt,
  --sum(a.sales_value) sales_value,   --含税销售额
  --sum(a.profit) profit,   --含税毛利
  --sum(a.front_profit) front_profit,
  round(sum(b.sales_qty),2) as sales_qty1,
  round(sum(b.sales_cost),2) as sales_cost1,
  round(sum(b.sales_qty * a.no_tax_price),2) as sales_value1,       -- 销售额
  round(sum(b.sales_qty * a.no_tax_price),2) - round(sum(b.sales_cost),2) as profit1,       --毛利额
  round(sum(b.sales_qty * a.no_tax_price),2) - round(sum(b.sales_qty * a.middle_office_price),2) as front_profit1 
from 
  (
    select 
      sdt,
	  split_part(id, '&',1) as credential_no,
      region_code,
      region_name,
      province_code,
      province_name,
	  city_group_code,
	  city_group_name,
	  business_type_name,
	  dc_code, 
      customer_no,
      customer_name,
      goods_code,
      goods_name,
	  is_factory_goods_desc,
	  classify_large_name,
      sales_qty,
      sales_value,
      excluding_tax_sales/sales_qty as no_tax_price,
      excluding_tax_sales,
      excluding_tax_profit,
      excluding_tax_cost,
      excluding_tax_cost/sales_qty as no_tax_cost,
      sales_cost,
      profit,
	  front_profit,
	  purchase_price_flag,
      cost_price,
      case when purchase_price_flag=1 then purchase_price end as purchase_price,
      middle_office_price,
      sales_price
    from csx_dw.dws_sale_r_d_detail 
    where sdt>='20200101' and sdt<'20210927'
	and channel_code in ('1', '7', '9')
	and business_type_code ='4'
	and sales_type<>'fanli'
  )a 
--批次操作明细表
  left join 
  (
   select
     credential_no,
     wms_order_no,
     goods_code,
     sum(if(in_or_out = 0, -1 * qty, qty)) as sales_qty,
     sum(if(in_or_out = 0, -1 * amt_no_tax, amt_no_tax)) as sales_cost,
     link_wms_entry_move_type,
     link_wms_entry_move_name
   from csx_dw.dws_wms_r_d_batch_detail
   where sdt >= '20190101'
   group by credential_no, wms_order_no, goods_code, link_wms_entry_move_type,link_wms_entry_move_name
  )b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
  left join 
  (
   select 
     supplier_code,
     supplier_name,
     order_code,
     goods_code,
     min(to_date(receive_time))over(partition by supplier_code) as min_sdt
   from csx_dw.dws_wms_r_d_entry_detail
   where sdt >= '20190101' or sdt = '19990101'
    )c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
    where supplier_name like '阜阳%' 
group by a.business_type_name,a.region_name,a.province_name,a.customer_no,a.customer_name,c.supplier_code,c.supplier_name,
classify_large_name,
substr(sdt,1,4),
min_sdt,
coalesce(b.link_wms_entry_move_type, ''),coalesce(b.link_wms_entry_move_name, '')
;



select substr(sdt,1,4) years,
        customer_no,
        customer_name,
        classify_large_name,
        min(sdt)min_sdt,
        sum(excluding_tax_sales),
        sum(excluding_tax_profit)/sum(excluding_tax_sales) as profit_rate
from csx_dw.dws_sale_r_d_detail 
where customer_name like '%鑫林%'
and sdt>='20200101'
and business_type_code='4'
group by 
 substr(sdt,1,4) ,
        customer_no,
        customer_name,
        classify_large_name
;


select * from csx_dw.dws_basic_w_a_csx_supplier_m
where sdt='current'
and vendor_name like '%福建省司法厅%'
;


-- 供应商未税入库
select  years,
a.supplier_code,
a.supplier_name,
classify_large_name,
min(r_sdt)over(partition by supplier_code)  r_sdt,
  r_amt,
s_amt,
  aa
from 
(
select  years,
a.supplier_code,
a.supplier_name,
classify_large_name,
min(r_sdt)  r_sdt,
sum(r_amt) as r_amt,
sum( s_amt) s_amt,
sum(r_amt)-sum(s_amt) as aa
from 
(
select substr(sdt,1,4) years,
a.supplier_code,
a.supplier_name,
classify_large_name,
min(case when a.receive_status=2 then sdt else regexp_replace(to_date(a.receive_time),'-','') end  ) r_sdt,
sum((coalesce(price,0)/(1+coalesce(b.tax_rate,0)/100))*coalesce(receive_qty,0))/10000 as r_amt,
-- sum(price*receive_qty)/10000 receive_amt,
0 s_amt
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select goods_id,tax_rate,classify_large_code,classify_large_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
where supplier_name like '%益海嘉里%'
and a.receive_status in (1,2)
group by substr(sdt,1,4),
classify_large_name,
a.supplier_code,
a.supplier_name
    union all 
select substr(sdt,1,4) years,
a.supplier_code,
a.supplier_name,
b.classify_large_name,
min(sdt) r_sdt,
0 r_amt,
sum((coalesce(price,0)/(1+coalesce(b.tax_rate,0)/100))*coalesce(a.shipped_qty,0))/10000 as s_amt
from csx_dw.dws_wms_r_d_ship_detail a 
join 
(select goods_id,tax_rate,classify_large_code,classify_large_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
where supplier_name like '%益海嘉里%'
and a.status in (8)
and a.business_type_code='05'
and sdt>='20200101'
group by substr(sdt,1,4),
b.classify_large_name,
a.supplier_code,
a.supplier_name
) a 
group by  years,
a.supplier_code,
a.supplier_name,
classify_large_name
) a 
where  aa!=0


