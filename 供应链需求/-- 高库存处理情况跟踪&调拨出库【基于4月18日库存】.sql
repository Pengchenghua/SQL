-- 高库存处理情况跟踪&调拨出库【基于4月18日库存】
set edate='${enddate}'  ;
set dc_uses=('寄售门店','城市服务商','合伙人物流','');
set edt=regexp_replace(${hiveconf:edate},'-','');

drop table if exists csx_tmp.temp_rece_02;
create temporary table csx_tmp.temp_rece_02 as 
SELECT location_code AS dc_code,
     product_code AS goods_id,
          coalesce(sum( amt_no_tax*(1+tax_rate/100)),0) AS receipt_amt,
          coalesce(sum( txn_qty ),0) AS receipt_qty,
          sdt max_receipt_sdt
   FROM csx_dw.dwd_cas_r_d_accounting_stock_detail a
   join 
  (SELECT location_code AS dc_code,
     product_code AS goods_id,
     max(sdt) max_receipt_sdt
   FROM csx_dw.dwd_cas_r_d_accounting_stock_detail a
   WHERE 1=1 
     and a.in_or_out='-'
     AND sdt <= regexp_replace(${hiveconf:edate},'-' ,'')   
         and move_type in ('118A','119A','202A')   -- 只计算领用与原料转码\转码
GROUP BY location_code,
         product_code
         ) b on a.location_code=b.dc_code 
         and a.product_code=b.goods_id
       AND sdt =  b.max_receipt_sdt
      where  a.move_type in ('118A','119A','202A') 
GROUP BY location_code,
         product_code,
         sdt
    
;

-- 调拨出库
create temporary table if not exists csx_tmp.p_contain_transfer_cost as
select
	a.shipped_location_code                          ,
	a.goods_code                                     ,
	coalesce(sum(a.shipped_qty) ,0)      as contain_transfer_entry_qty  ,
	coalesce(sum(price*a.shipped_qty),0) as contain_transfer_entry_value
from
	csx_dw.dws_wms_r_d_ship_batch a
    where 1=1
    and sdt> regexp_replace(date_sub(${hiveconf:edate},30),'-' ,'') and sdt<=regexp_replace(${hiveconf:edate},'-','') 
    and a.status!=9
    and a.order_type_code like'T%'
    and a.receive_area_code !='CY01'
group by
	a.shipped_location_code ,
	a.goods_code           
;


create temporary table if not exists csx_tmp.p_shipp_date as
select
	a.shipped_location_code                          ,
	a.goods_code                                     ,
	max(sdt) max_shpp_date
from
	csx_dw.dws_wms_r_d_ship_batch a
    where 1=1
    and a.status!=9
    and substr(a.order_type_code,1,1) in('T','S')
    and a.receive_area_code !='CY01'
group by
	a.shipped_location_code ,
	a.goods_code           
;


drop table  csx_tmp.p_contain_transfer_trunc;
create temporary table if not exists csx_tmp.p_contain_transfer_trunc as
select a.*,b.contain_transfer_entry_value as contain_transfer_cost_30day,	
coalesce(case when (business_division_code='11' and (cost_30day+receipt_amt+material_take_amt+coalesce(b.contain_transfer_entry_value,0)) <=0 and period_inv_amt_30day>0) then 9999 
		              when (business_division_code !='11' and cost_30day +coalesce(b.contain_transfer_entry_value,0) <=0 and period_inv_amt_30day>0) then 9999 
		              when (business_division_code='11' and (cost_30day+receipt_amt+material_take_amt+coalesce(b.contain_transfer_entry_value,0)) <=0 and period_inv_amt_30day<=0) then 0
		              when (business_division_code !='12' and cost_30day+coalesce(b.contain_transfer_entry_value,0)  <=0 and period_inv_amt_30day<=0) then 0 
					  when business_division_code ='11' then  period_inv_amt_30day/(cost_30day+receipt_amt+material_take_amt+coalesce(b.contain_transfer_entry_value,0))
		              else period_inv_amt_30day/(cost_30day+coalesce(b.contain_transfer_entry_value,0)) 
		        end,0)	as days_turnover_30_transfer,
		     max_shpp_date, 
		     coalesce(datediff(date_sub(CURRENT_DATE,1),from_unixtime(unix_timestamp( max_shpp_date,'yyyyMMdd'),'yyyy-MM-dd')),9999) as no_shipp_day
from csx_tmp.ads_wms_r_d_goods_turnover a 
left join
 csx_tmp.p_contain_transfer_cost b on a.dc_code=b.shipped_location_code and a.goods_id=b.goods_code
left join 
csx_tmp.p_shipp_date c on a.dc_code=c.shipped_location_code and a.goods_id=c.goods_code
 where sdt=regexp_replace(${hiveconf:edate},'-' ,'')  
 ;
 









 select
 performance_province_code,
 performance_province_name,
 a.dc_code,
 a.dc_name,
 a.goods_id,
 a.goods_name,
 a.unit_name,
 a.classify_large_code ,
 a.classify_large_name ,
 a.classify_middle_code,
 a.classify_middle_name ,
 a.classify_small_code,
 a.classify_small_name,
 qty,
 amt,
final_qty,
 a.final_amt ,
  days_turnover_30_transfer,
  max_shpp_date,
 a.no_shipp_day , 
 a.entry_days ,
 a.entry_qty,
 a.entry_sdt,
 a.entry_value,
 contain_transfer_entry_qty , 
 contain_transfer_entry_value ,
 contain_transfer_entry_sdt , 
 contain_transfer_entry_days 
from csx_tmp.p_contain_transfer_trunc a
join  csx_tmp.fanruan c on a.dc_code=c.dc_code and a.goods_id=c.goods_code
  join 
  (select sales_province_code,
    sales_province_name,
    purchase_org,
    purchase_org_name,
    case when performance_province_name like'平台%' then '00' else   sales_region_code end sales_region_code,
    case when performance_province_name like'平台%' then '平台' else  sales_region_name end sales_region_name,
    shop_id ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 ) b on a.dc_code=b.shop_id
;
  