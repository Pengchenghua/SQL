-- 高库存跟踪&调拨出库【全国异常库存】新规则20220811
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
create  table if not exists csx_tmp.p_contain_transfer_trunc as
select a.*,b.contain_transfer_entry_value as contain_transfer_cost_30day,	
coalesce(case when (business_division_code='11' and (cost_30day+receipt_amt+material_take_amt+coalesce(b.contain_transfer_entry_value,0)) <=0 and period_inv_amt_30day>0) then 9999 
		              when (business_division_code !='11' and cost_30day +coalesce(b.contain_transfer_entry_value,0) <=0 and period_inv_amt_30day>0) then 9999 
		              when (business_division_code='11' and (cost_30day+receipt_amt+material_take_amt+coalesce(b.contain_transfer_entry_value,0)) <=0 and period_inv_amt_30day<=0) then 0
		              when (business_division_code !='12' and cost_30day+coalesce(b.contain_transfer_entry_value,0)  <=0 and period_inv_amt_30day<=0) then 0 
					  when business_division_code ='11' then  period_inv_amt_30day/(cost_30day+receipt_amt+material_take_amt+coalesce(b.contain_transfer_entry_value,0))
		              else period_inv_amt_30day/(cost_30day+coalesce(b.contain_transfer_entry_value,0)) 
		        end,0)	as days_turnover_30_transfer ,
			datediff(to_date(date_sub(current_timestamp(),1)),from_unixtime(unix_timestamp(max_shpp_date,'yyyyMMdd'),'yyyy-MM-dd')) as no_shipped_days,
			max_shpp_date
from csx_tmp.ads_wms_r_d_goods_turnover a 
left join
 csx_tmp.p_contain_transfer_cost b on a.dc_code=b.shipped_location_code and a.goods_id=b.goods_code
 left join 
 csx_tmp.p_shipp_date c on a.dc_code=c.shipped_location_code and a.goods_id=c.goods_code
 where sdt=regexp_replace(${hiveconf:edate},'-' ,'')  
 ;
 
 
drop table  csx_tmp.temp_01 ;
create  table csx_tmp.temp_01 as 
select * from (
select 
 a.dc_uses,
 province_code,
 province_name,
 a.dc_code,
 a.dc_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
 a.goods_id,
 a.goods_name,
 a.unit_name,
qualitative_period,
product_status_name,
(final_qty) total_qty,
 a.final_amt ,
  days_turnover_30 ,
  days_turnover_30_transfer,
 no_sale_days , 
 max_sale_sdt ,
 entry_days ,
 entry_qty,
 entry_value,
 entry_sdt ,
 d.receipt_amt receipt_m_amt,
 d.receipt_qty receipt_m_qty,
 max_receipt_sdt,
 contain_transfer_entry_qty , 
 contain_transfer_entry_value ,
 contain_transfer_entry_sdt , 
 contain_transfer_entry_days,
 no_shipped_days,
 max_shpp_date
-- sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
--          when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
--           when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
--         end ) high_stock_amt,      -- 高库存金额
-- count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_id
--          when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_id
--           when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_id
--         end ) high_stock_sku,            -- 高库存SKU
-- sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end ) no_sales_stock_amt,          -- 无销售库存金额
-- count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_id   end ) no_sales_stock_sku,   -- 无销售库存SKU
-- sum(stock_amt) as validity_amt,
-- count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from csx_tmp.p_contain_transfer_trunc  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt=${hiveconf:edt}
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
left join 
(SELECT shop_code,
       product_code,
       qualitative_period,
       product_status_name
FROM csx_dw.dws_basic_w_a_csx_product_info
WHERE sdt='current') c on a.goods_id=c.product_code and a.dc_code=c.shop_code 
left join 
csx_tmp.temp_rece_02 d on a.goods_id=d.goods_id and a.dc_code=d.dc_code
where sdt=${hiveconf:edt} 
    and dc_uses not in ${hiveconf:dc_uses}
    and division_code in ('11','10','12','13','14')
   -- AND final_qty>0
    and classify_large_code in('B02','B03','B01','B04','B05','B06','B08','B07','B09')
    and ( (division_code in ('11','10') and a.days_turnover_30_transfer>15 and a.final_amt>500 and a.contain_transfer_entry_days>3 )
         or (division_code in ('13','14') and a.days_turnover_30_transfer>45 and a.final_amt>2000 and a.contain_transfer_entry_days>7 )
          or( division_code in ('12') and a.days_turnover_30_transfer>30 and a.final_amt>2000 and a.contain_transfer_entry_days>7 ))
 ) a 
  ;
  
  
 -- select * from csx_tmp.temp_02 ; 
  
  -- 不动销
   drop table csx_tmp.temp_02 ;
 create temporary table csx_tmp.temp_02 as 
select * from (
select 
 dc_uses,
 province_code,
 province_name,
 a.dc_code,
 a.dc_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
 a.goods_id,
 a.goods_name,
 a.unit_name,
qualitative_period,
product_status_name,
(final_qty) total_qty,
 a.final_amt ,
  days_turnover_30 ,
  days_turnover_30_transfer,
 no_sale_days , 
 max_sale_sdt ,
 entry_days ,
 entry_qty,
 entry_value,
 entry_sdt ,
 d.receipt_amt receipt_m_amt,
 d.receipt_qty receipt_m_qty,
 max_receipt_sdt,
 contain_transfer_entry_qty , 
 contain_transfer_entry_value ,
 contain_transfer_entry_sdt , 
 contain_transfer_entry_days ,
  no_shipped_days,
 max_shpp_date
-- sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
--          when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
--           when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
--         end ) high_stock_amt,      -- 高库存金额
-- count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_id
--          when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_id
--           when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_id
--         end ) high_stock_sku,            -- 高库存SKU
-- sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end ) no_sales_stock_amt,          -- 无销售库存金额
-- count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_id   end ) no_sales_stock_sku,   -- 无销售库存SKU
-- sum(stock_amt) as validity_amt,
-- count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from csx_tmp.p_contain_transfer_trunc   a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt=${hiveconf:edt}
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
left join 
(SELECT shop_code,
       product_code,
       qualitative_period,
       product_status_name
FROM csx_dw.dws_basic_w_a_csx_product_info
WHERE sdt='current') c on a.goods_id=c.product_code and a.dc_code=c.shop_code 
left join 
csx_tmp.temp_rece_02 d on a.goods_id=d.goods_id and a.dc_code=d.dc_code
where sdt=${hiveconf:edt} 
    and dc_uses not in ${hiveconf:dc_uses}
    and division_code in ('11','10','12','13','14')
   -- AND final_qty>0
    and classify_large_code in('B02','B03','B01','B04','B05','B06','B08','B07','B09')
    and (a.no_shipped_days>30 and a.final_qty>0.1 and a.contain_transfer_entry_days>7 )
 ) a 
  ;
  
  drop table  csx_tmp.temp_00;
  create temporary table csx_tmp.temp_00 as 
  select '高库存' note,
  a.dc_uses,
 a.province_code,
 a.province_name,
 a.dc_code,
 a.dc_name,
 a.classify_large_code ,
 a.classify_large_name ,
 a.classify_middle_code,
 a.classify_middle_name ,
 a.goods_id,
 a.goods_name,
 a.unit_name,
a.qualitative_period,
a.product_status_name,
a.total_qty,
 a.final_amt ,
  a.days_turnover_30 ,
  days_turnover_30_transfer,
  no_shipped_days,
 max_shpp_date,
 a.entry_days ,
 a.entry_qty,
 a.entry_value,
 a.entry_sdt,
  receipt_m_amt,
  receipt_m_qty,
 max_receipt_sdt,
 contain_transfer_entry_qty , 
 contain_transfer_entry_value ,
 contain_transfer_entry_sdt , 
 contain_transfer_entry_days 
 from csx_tmp.temp_01 a
  union all 
  select '未销售' note,
  a.dc_uses,
 a.province_code,
 a.province_name,
 a.dc_code,
 a.dc_name,
 a.classify_large_code ,
 a.classify_large_name ,
 a.classify_middle_code,
 a.classify_middle_name ,
 a.goods_id,
 a.goods_name,
 a.unit_name,
a.qualitative_period,
a.product_status_name,
a.total_qty,
 a.final_amt ,
  a.days_turnover_30 ,
 a.days_turnover_30_transfer,
 a.no_shipped_days,
 a.max_shpp_date,
 a.entry_days ,
 a.entry_qty,
 a.entry_value,
 a.entry_sdt ,
 a.receipt_m_amt,
 a.receipt_m_qty,
 a.max_receipt_sdt,
 a.contain_transfer_entry_qty , 
 a.contain_transfer_entry_value ,
 a.contain_transfer_entry_sdt , 
 a.contain_transfer_entry_days 
 from csx_tmp.temp_02  a 
  left join 
  csx_tmp.temp_01 b on a.dc_code=b.dc_code and a.goods_id=b.goods_id
  where b.goods_id is null and b.dc_code is null
  ;
 
  
  
  select note,
  dc_uses,
  sales_region_code,
  sales_region_name,
  performance_province_name,
  performance_city_name,
  a.dc_code,
 a.dc_name,
 a.classify_large_code ,
 a.classify_large_name ,
 a.classify_middle_code,
 a.classify_middle_name ,
 a.goods_id,
 a.goods_name,
 a.unit_name,
a.qualitative_period,
a.product_status_name,
a.total_qty,
 a.final_amt ,
  a.days_turnover_30 ,
  days_turnover_30_transfer,
 a.no_sale_days , 
 a.max_sale_sdt ,
 a.entry_days ,
 a.entry_qty,
 a.entry_value,
 a.entry_sdt,
  receipt_m_amt,
  receipt_m_qty,
 max_receipt_sdt,
 contain_transfer_entry_qty , 
 contain_transfer_entry_value ,
 contain_transfer_entry_sdt , 
 contain_transfer_entry_days 
from csx_tmp.temp_00 a 
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
  where classify_middle_name !='酒' ;
  
  
  
  
  
drop table csx_tmp.tmp_classify_kanban_fr;
create table csx_tmp.tmp_classify_kanban_fr as 
select 
 level_id,
 province_code,
 province_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
 sku,
 total_amt,
 total_turnover_day,
 high_stock_amt,      -- 高库存金额
 high_stock_sku,            -- 高库存SKU
 no_sales_stock_amt,          -- 无销售库存金额
 no_sales_stock_sku,   -- 无销售库存SKU
 validity_amt,
 validity_sku
from 
( 


select 
 '1' level_id,
 province_code,
 province_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt)/10000 total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt+coalesce(contain_transfer_entry_value,0) else cost_30day+coalesce(contain_transfer_entry_value,0) end ) total_turnover_day,
 
sum(case when division_code in ('11','10') and a.days_turnover_30_transfer>15 and a.final_amt>500 and a.entry_days>3 then final_amt
         when division_code in ('13','14') and a.days_turnover_30_transfer>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
          when division_code in ('12') and a.days_turnover_30_transfer>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
        end )/10000 high_stock_amt,      -- 高库存金额
count(distinct  case when division_code in ('11','10') and a.days_turnover_30_transfer>15 and a.final_amt>500 and a.entry_days>3 then a.goods_id
         when division_code in ('13','14') and a.days_turnover_30_transfer>45 and a.final_amt>2000 and a.entry_days>7 then goods_id
          when division_code in ('12') and a.days_turnover_30_transfer>30 and a.final_amt>2000 and a.entry_days>7 then goods_id
        end ) high_stock_sku,            -- 高库存SKU
sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end )/10000 no_sales_stock_amt,          -- 无销售库存金额
count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_id   end ) no_sales_stock_sku,   -- 无销售库存SKU
sum(stock_amt) as validity_amt,
count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from  csx_tmp.p_contain_transfer_trunc  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt='20220810'
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where 1=1
    and dc_uses not in ${hiveconf:dc_uses}
    and division_code in ('11','10','12','13','14')
group by province_name,
    province_code,
    classify_large_code ,
    classify_large_name ,
    classify_middle_code,
    classify_middle_name

union all 
select 
 '0' level_id,
 '000001'province_code
 '全国' province_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt)/10000 total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
        end )/10000 high_stock_amt,      -- 高库存金额
count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_id
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_id
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_id
        end ) high_stock_sku,            -- 高库存SKU
sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end )/10000 no_sales_stock_amt,          -- 无销售库存金额
count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_id   end ) no_sales_stock_sku,   -- 无销售库存SKU
sum(stock_amt) as validity_amt,
count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from  csx_tmp.p_contain_transfer_trunc  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt='20220811'
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where 1=1
   and dc_uses not in ${hiveconf:dc_uses}
    and division_code in ('11','10','12','13','14')
group by 
classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name 
) a 
order by total_amt desc
;


select * from  csx_tmp.p_contain_transfer_trunc;