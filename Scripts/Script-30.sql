SELECT 
${if(len(ordertype)==0,""," a.source_type, ")}
dist_code,
dist_name,
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
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
${if(len(ordertype)==0,""," a.source_type, ")}
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name ,
order_code,
count(goods_code )order_sku,
count(case when receive_qty!=0 then goods_code end)receive_sku,
sum(order_qty )order_qty,
sum(order_amt )order_amt ,
sum(receive_qty )as receive_qty ,
sum(receive_amt )as receive_amt  
--COUNT (case when receive_close_date<=regexp_replace(last_delivery_date,'-','')  and receive_qty!=0 then goods_code end) as no_overdue_sku,
--sum(case when receive_close_date<=regexp_replace(last_delivery_date,'-','') then receive_qty end) as no_overdue_qty,
--sum(case when receive_close_date<=regexp_replace(last_delivery_date,'-','') then receive_amt end)as no_overdue_amt,
--COUNT(DISTINCT case when (receive_close_date>regexp_replace(last_delivery_date,'-','') OR receive_close_date='' )then 1 end) as overdue_num
from csx_dw.ads_supply_order_flow a
where 
 sdt>='${sdate}' 
 and sdt<='${edate}'
and regexp_replace(last_delivery_date,'-','') <='${edate}'
and super_class='1'
-- and category_code in ('12','13','14','11','')
${if(len(ordertype)==0,"","and a.source_type in ("+ordertype+") ")}
${if(len(dc)==0,"","and a.receive_location_code in ('"+substitute(dc,",","','")+"')")}
${if(len(dept)==0,"","and (case when category_code ='10'then 'U01' else  purchase_group_code end) in ('"+substitute(dept,",","','")+"')")}
--${if(len(dc)==0,"","and dc_code in ('"+SUBSTITUTE(dc,",","','")+"')")}
${if(len(vendor)==0,"","and a.supplier_code in ('"+vendor+"') ")}
and order_status!=5
group by 
${if(len(ordertype)==0,""," a.source_type ,")}
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name,
order_code )a 
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop cs where sdt='current'and cs.location_type_code in ('1','2'))b on a.receive_location_code =b.location_code
group by
${if(len(ordertype)==0,""," a.source_type ,")}
dist_code,
dist_name,
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
supplier_name
order by dist_code,dist_name,a.receive_location_code ,receive_location_name ,a.supplier_code ,supplier_name;




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


 select
     *,
      substr(return_goods_dtl, 2, length(return_goods_dtl) - 2) as return_goods_dtl
    from csx_ods.source_bbc_r_d_bshop_order_return_dtl_v1
    where sdt = '20201028';
    
   select * from csx_tmp.temp_date_m where calday>='20201001';
   
   select * from csx_tmp.ads_wms_r_d_warehouse_sales A where  warehouse_sales_qty <0 and return_flag !='X' AND sdt >='20201001'and dc_code ='W0A8';
select *   from	csx_dw.dws_wms_r_d_accounting_stock_m where sdt='20201028' and goods_code='12' and dc_code='W0K7';


select sdt, sum(sales_value) from csx_dw.dws_sale_r_d_customer_sale 
where sdt like '202010%' and dc_code='W0A3'
	and goods_code ='8773'
	and division_code in ('10','11') 
group by sdt;

SELECT sdt, sum(amt_no_tax*(1+tax_rate/100 ))  from
       csx_dw.dwd_cas_r_d_accounting_stock_detail a 
       join 
       (select goods_id from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' 
       and division_code in ('11','10'))b on a.product_code =goods_id 
	where  sdt like '202010%' and move_type ='107A'
		and location_code ='W0A3'
	 and goods_id ='8773'
	 group by sdt
       ;
      
select credential_no,txn_price ,txn_amt,txn_qty,tax_rate from csx_dw.dwd_cas_r_d_accounting_stock_detail 
where credential_no ='PZ20201008027372' and sdt>='20201001' and product_code ='8773';

select credential_no ,cost_price ,sales_price,sales_qty,sales_value,tax_rate from csx_dw.dws_sale_r_d_customer_sale a 
where sdt like '202010%' and dc_code='W0A3'
	and goods_code ='8773'
	and division_code in ('10','11') 
	and sdt='20201008'
	and credential_no ='PZ20201008027372';

select a.calday,a.dow,a.calweek,
    regexp_replace(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'), 
      pmod(datediff(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'),1), '2012-01-07'), 7)+1), '-', '') as new_week_first,
    regexp_replace(date_add(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'), 
      pmod(datediff(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'),0), '2012-01-07'), 7)+1),7), '-', '') as new_week_last,
   case when   date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'), 
      pmod(datediff(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'),0), '2012-01-07'), 7)+1)='2019-12-28' then 1 
        else  weekofyear(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'), 
      pmod(datediff(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'),0), '2012-01-07'), 7)+1))+1 end as new_weeknum
from csx_dw.dws_w_a_date_m a 
where calday>='20200101' ;

select * from csx_dw.wms_shipped_order where order_no ='CY201103000154' and sdt>='20201001';
select * from csx_dw.wms_entry_order  where link_order_code ='OM20110200000059';

select * from csx_ods.source_wms_r_d_shipped_order_item where order_code ='CY201103000154';
select * from csx_dw.dwd_wms_r_d_shipped_order_header  where order_code  ='CY201103000154';
select * from csx_dw.dwd_wms_r_d_shipped_order_detail  where order_no  ='CY201103000154' and sdt>='20201001';
SELECT * from csx_dw.dws_wms_r_d_accounting_stock_m where sdt='20201104' and qty<0;



refresh csx_ods.source_master_w_a_md_product_launch_reviewed_view;
select * FROM csx_ods.source_master_w_a_md_product_launch_reviewed_view where review_status='40' and product_code='841911' and location_code='W0A6' and next_review_flow_node_id = 0;

select * from csx_tmp.dws_csms_manager_month_sale_plan_tmp;

SELECT *
FROM csx_ods.source_master_w_a_md_product_launch_reviewed_view
WHERE sdt='20201109'
  AND review_status='40'
  and location_code='W0A6'
  and product_code='841911' 
  and next_review_flow_node_id = 0
;

select * from   csx_tmp.ads_fr_profit_day_cust_top_D
where smonth='${mon}'
and region_code='${zoneid}'
order by profit_f asc;


select * from   csx_tmp.ads_fr_profit_day_goods_top_M_v1
where smonth='${mon}'
and region_code='${zoneid}'
order by profit_f asc;

select * from csx_dw.dws_basic_w_a_csx_product_m where goods_id ='1162233' and sdt='current';


select * from csx_dw.dws_basic_w_a_csx_product_info where product_code ='1162233' and sdt='current';

select * from csx_tmp.temp_date_m ;

select * from csx_dw.dwd_wms_r_m_data_ending_inventory  ;
select * from csx_tmp.ads_factory_r_d_inventory_loss_fr limit 10;