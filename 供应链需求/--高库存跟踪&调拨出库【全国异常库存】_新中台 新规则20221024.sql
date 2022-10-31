--高库存跟踪&调拨出库【全国异常库存】_新中台 新规则20221024

drop table if exists csx_analyse_tmp.csx_analyse_tmp_raw_material_01;
create  table  csx_analyse_tmp.csx_analyse_tmp_raw_material_01 as 
SELECT location_code AS dc_code,
     a.goods_code AS goods_code,
          coalesce(sum( amt_no_tax*(1+tax_rate/100)),0) AS requisition_amt,
          coalesce(sum( txn_qty ),0) AS receipt_qty,
          sdt max_receipt_sdt
   FROM csx_dwd.csx_dwd_cas_accounting_stock_detail_di a
   join 
  (SELECT location_code AS dc_code,
     goods_code AS goods_code,
     max(sdt) max_receipt_sdt
   FROM csx_dwd.csx_dwd_cas_accounting_stock_detail_di a
   WHERE 1=1 
     and a.in_or_out='-'
     AND sdt <= '20221023'   
         and move_type_code in ('118A','119A','202A')   -- 只计算领用与原料转码\转码
GROUP BY location_code,
         goods_code
         ) b on a.location_code=b.dc_code 
         and a.goods_code=b.goods_code
       AND sdt =  b.max_receipt_sdt
    --  where  a.move_type in ('118A','119A','202A') 
GROUP BY location_code,
         a.goods_code,
         sdt
    
;


-- 调拨出库
create  table if not exists  csx_analyse_tmp.csx_analyse_tmp_raw_material_02 as
select
	a.send_dc_code                          ,
	a.goods_code                                     ,
	coalesce(sum(a.shipped_qty) ,0)      as contain_transfer_receive_qty  ,
	coalesce(sum(price*a.shipped_qty),0) as contain_transfer_receive_amt
from
 	 csx_dws.csx_dws_wms_shipped_detail_di a
    where 1=1
    and sdt>'20220924'  and sdt<='20221024'
    and a.status!=9
    and a.shipped_type like'T%'
    and a.reservoir_area_code !='CY01'
group by
	a.send_dc_code ,
	a.goods_code           
;

-- 最近出库
create  table if not exists csx_analyse_tmp.csx_analyse_tmp_raw_material_03 as
select
	a.send_dc_code                          ,
	a.goods_code                            ,
	max(sdt) max_shpp_date
from
	  csx_dws.csx_dws_wms_shipped_detail_di a
    where 1=1
    and a.status!=9
    and substr(a.shipped_type,1,1) in('T','S')
    and a.reservoir_area_code !='CY01'
group by
	a.send_dc_code ,
	a.goods_code           
;


drop table  csx_analyse_tmp.csx_analyse_tmp_raw_material_04;
create  table if not exists csx_analyse_tmp.csx_analyse_tmp_raw_material_04 as
select a.*,b.contain_transfer_receive_amt as contain_transfer_nearly30days_sale_cost,	
coalesce(case when (business_division_code='11' and (nearly30days_sale_cost+requisition_amt+material_use_amt+coalesce(b.contain_transfer_receive_amt,0)) <=0 and nearly30days_stock_amt>0) then 9999 
		              when (business_division_code !='11' and nearly30days_sale_cost +coalesce(b.contain_transfer_receive_amt,0) <=0 and nearly30days_stock_amt>0) then 9999 
		              when (business_division_code='11' and (nearly30days_sale_cost+requisition_amt+material_use_amt+coalesce(b.contain_transfer_receive_amt,0)) <=0 and nearly30days_stock_amt<=0) then 0
		              when (business_division_code !='12' and nearly30days_sale_cost+coalesce(b.contain_transfer_receive_amt,0)  <=0 and nearly30days_stock_amt<=0) then 0 
					  when business_division_code ='11' then  nearly30days_stock_amt/(nearly30days_sale_cost+requisition_amt+material_use_amt+coalesce(b.contain_transfer_receive_amt,0))
		              else nearly30days_stock_amt/(nearly30days_sale_cost+coalesce(b.contain_transfer_receive_amt,0)) 
		        end,0)	as days_turnover_30_transfer ,
			datediff(to_date(date_sub(current_timestamp(),1)),from_unixtime(unix_timestamp(max_shpp_date,'yyyyMMdd'),'yyyy-MM-dd')) as no_shipped_days,
			max_shpp_date
from
    desc csx_ads.csx_ads_wms_goods_turnover_df a 
left join
 csx_analyse_tmp.csx_analyse_tmp_raw_material_02 b on a.dc_code=b.send_dc_code and a.goods_code=b.goods_code
 left join 
csx_analyse_tmp.csx_analyse_tmp_raw_material_03  c on a.dc_code=c.send_dc_code and a.goods_code=c.goods_code
 where sdt='20221023'
 ;
 
 
drop table   csx_analyse_tmp.csx_analyse_tmp_raw_material_05 ;
create  table  csx_analyse_tmp.csx_analyse_tmp_raw_material_05 as 
select * from (
select 
 a.purpose_name,
 province_code,
 province_name,
 a.dc_code,
 a.dc_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
 a.goods_code,
 a.goods_name,
 a.unit_name,
qualitative_period,
c.goods_status_name	,
(qm_qty) total_qty,
 a.qm_amt final_amt ,
 nearly30days_turnover_days  days_turnover_30 ,
 days_turnover_30_transfer,
 no_sale_days , 
 max_sale_sdt max_sale_sdt ,
 receive_days entry_days ,
 receive_qty entry_qty,
 receive_amt entry_value,
 receive_date entry_sdt ,
 d.requisition_amt receipt_m_amt,
 d.receipt_qty receipt_m_qty,
 max_receipt_sdt,
 contain_transfer_receive_qty , 
 contain_transfer_receive_amt ,
 contain_transfer_receive_sdt , 
 contain_transfer_receive_days,
 no_shipped_days,
 max_shpp_date
-- sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
--          when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
--           when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
--         end ) high_stock_amt,      -- 高库存金额
-- count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_code
--          when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_code
--           when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_code
--         end ) high_stock_sku,            -- 高库存SKU
-- sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end ) no_sales_stock_amt,          -- 无销售库存金额
-- count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_code   end ) no_sales_stock_sku,   -- 无销售库存SKU
-- sum(stock_amt) as validity_amt,
-- count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from csx_analyse_tmp.csx_analyse_tmp_raw_material_04  a
left join 
(SELECT dc_code,
       goods_code,
       qualitative_period,
       goods_status_name	
FROM
     csx_dim.csx_dim_basic_dc_goods
WHERE sdt='current') c on a.goods_code=c.goods_code and a.dc_code=c.dc_code 
left join 
csx_analyse_tmp.csx_analyse_tmp_raw_material_01 d on a.goods_code=d.goods_code and a.dc_code=d.dc_code
where sdt='20221023'
    and purpose_name not in ('寄售门店','城市服务商','合伙人物流','')
    and division_code in ('11','10','12','13','14')
   -- AND final_qty>0
    and classify_large_code in('B02','B03','B01','B04','B05','B06','B08','B07','B09')
    and ( (division_code in ('11','10') and a.days_turnover_30_transfer>15 and a.qm_amt>500 and a.contain_transfer_receive_days>3 )
         or (division_code in ('13','14') and a.days_turnover_30_transfer>45 and a.qm_amt>2000 and a.contain_transfer_receive_days>7 )
          or( division_code in ('12') and a.days_turnover_30_transfer>30 and a.qm_amt>2000 and a.contain_transfer_receive_days>7 ))
 ) a 
  ;
  
  
 -- select * from csx_tmp.temp_02 ; 
  
  -- 不动销
   drop table csx_analyse_tmp.csx_analyse_tmp_raw_material_06 ;
 create  table csx_analyse_tmp.csx_analyse_tmp_raw_material_06 as 
select * from (
select 
 a.purpose_name,
 province_code,
 province_name,
 a.dc_code,
 a.dc_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
 a.goods_code,
 a.goods_name,
 a.unit_name,
qualitative_period,
c.goods_status_name	,
(qm_qty) total_qty,
 a.qm_amt final_amt ,
 nearly30days_turnover_days  days_turnover_30 ,
 days_turnover_30_transfer,
 no_sale_days , 
 max_sale_sdt max_sale_sdt ,
 receive_days entry_days ,
 receive_qty entry_qty,
 receive_amt entry_value,
 receive_date entry_sdt ,
 d.requisition_amt receipt_m_amt,
 d.receipt_qty receipt_m_qty,
 max_receipt_sdt,
 contain_transfer_receive_qty , 
 contain_transfer_receive_amt ,
 contain_transfer_receive_sdt , 
 contain_transfer_receive_days,
 no_shipped_days,
 max_shpp_date
-- sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
--          when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
--           when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
--         end ) high_stock_amt,      -- 高库存金额
-- count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_code
--          when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_code
--           when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_code
--         end ) high_stock_sku,            -- 高库存SKU
-- sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end ) no_sales_stock_amt,          -- 无销售库存金额
-- count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_code   end ) no_sales_stock_sku,   -- 无销售库存SKU
-- sum(stock_amt) as validity_amt,
-- count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from csx_analyse_tmp.csx_analyse_tmp_raw_material_04   a

left join 
(SELECT dc_code,
       goods_code,
       qualitative_period,
       goods_status_name	
FROM
     csx_dim.csx_dim_basic_dc_goods
WHERE sdt='current' ) c on a.goods_code=c.goods_code and a.dc_code=c.dc_code 
left join 
 csx_analyse_tmp.csx_analyse_tmp_raw_material_01 d on a.goods_code=d.goods_code and a.dc_code=d.dc_code
where sdt='20221023'
    and purpose_name not in ('寄售门店','城市服务商','合伙人物流','')
    and division_code in ('11','10','12','13','14')
   -- AND final_qty>0
    and classify_large_code in('B02','B03','B01','B04','B05','B06','B08','B07','B09')
    and (a.no_shipped_days>30 and a.qm_qty>0.1 and a.contain_transfer_receive_days>7 )
 ) a 
  ;
  
  drop table   csx_analyse_tmp.csx_analyse_tmp_raw_material_07;
  create  table  csx_analyse_tmp.csx_analyse_tmp_raw_material_07 as 
  select '高库存' note,
  a.purpose_name,
 a.province_code,
 a.province_name,
 a.dc_code,
 a.dc_name,
 a.classify_large_code ,
 a.classify_large_name ,
 a.classify_middle_code,
 a.classify_middle_name ,
 a.goods_code,
 a.goods_name,
 a.unit_name,
a.qualitative_period,
a.goods_status_name,
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
 contain_transfer_receive_qty , 
 contain_transfer_receive_amt ,
 contain_transfer_receive_sdt , 
 contain_transfer_receive_days 
 from  csx_analyse_tmp.csx_analyse_tmp_raw_material_05  a
  union all 
  select '未销售' note,
  a.purpose_name,
 a.province_code,
 a.province_name,
 a.dc_code,
 a.dc_name,
 a.classify_large_code ,
 a.classify_large_name ,
 a.classify_middle_code,
 a.classify_middle_name ,
 a.goods_code,
 a.goods_name,
 a.unit_name,
a.qualitative_period,
a.goods_status_name,
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
 a.contain_transfer_receive_qty , 
 a.contain_transfer_receive_amt ,
 a.contain_transfer_receive_sdt , 
 a.contain_transfer_receive_days 
 from  csx_analyse_tmp.csx_analyse_tmp_raw_material_06   a 
  left join 
   csx_analyse_tmp.csx_analyse_tmp_raw_material_05  b on a.dc_code=b.dc_code and a.goods_code=b.goods_code
  where b.goods_code is null and b.dc_code is null
  ;
 
  
  
  select note,
  b.purpose_name,
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
 a.goods_code,
 a.goods_name,
 a.unit_name,
a.qualitative_period,
a.goods_status_name,
a.total_qty,
 a.final_amt ,
  a.days_turnover_30 ,
  days_turnover_30_transfer,
 a.no_shipped_days no_sale_days , 
 a.max_shpp_date max_sale_sdt ,
 a.entry_days ,
 a.entry_qty,
 a.entry_value,
 a.entry_sdt,
  receipt_m_amt,
  receipt_m_qty,
 max_receipt_sdt,
 contain_transfer_receive_qty , 
 contain_transfer_receive_amt ,
 contain_transfer_receive_sdt , 
 contain_transfer_receive_days 
from  csx_analyse_tmp.csx_analyse_tmp_raw_material_07 a 
  join 
  (select  
    purchase_org,
    purchase_org_name,
    case when performance_province_name like'平台%' then '00' else    performance_region_code end sales_region_code,
    case when performance_province_name like'平台%' then '平台' else  performance_region_name end sales_region_name,
    shop_code ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name
from csx_dim.csx_dim_shop
 where sdt='current'    
     ) b on a.dc_code=b.shop_code
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
count(distinct case when final_qty!=0 then goods_code end) sku,
sum(final_amt)/10000 total_amt,
sum(nearly30days_stock_amt  )/ sum(case when division_code in ('11','10') then nearly30days_sale_cost+requisition_amt+material_use_amt+coalesce(contain_transfer_receive_amt,0) else nearly30days_sale_cost+coalesce(contain_transfer_receive_amt,0) end ) total_turnover_day,
 
sum(case when division_code in ('11','10') and a.days_turnover_30_transfer>15 and a.final_amt>500 and a.entry_days>3 then final_amt
         when division_code in ('13','14') and a.days_turnover_30_transfer>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
          when division_code in ('12') and a.days_turnover_30_transfer>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
        end )/10000 high_stock_amt,      -- 高库存金额
count(distinct  case when division_code in ('11','10') and a.days_turnover_30_transfer>15 and a.final_amt>500 and a.entry_days>3 then a.goods_code
         when division_code in ('13','14') and a.days_turnover_30_transfer>45 and a.final_amt>2000 and a.entry_days>7 then goods_code
          when division_code in ('12') and a.days_turnover_30_transfer>30 and a.final_amt>2000 and a.entry_days>7 then goods_code
        end ) high_stock_sku,            -- 高库存SKU
sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end )/10000 no_sales_stock_amt,          -- 无销售库存金额
count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_code   end ) no_sales_stock_sku,   -- 无销售库存SKU
sum(stock_amt) as validity_amt,
count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from  csx_tmp.p_contain_transfer_trunc  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt='20220810'
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_code=b.goods_code
where 1=1
    and purpose_name not in ${hiveconf:purpose_name}
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
count(distinct case when final_qty!=0 then goods_code end) sku,
sum(final_amt)/10000 total_amt,
sum(nearly30days_stock_amt  )/ sum(case when division_code in ('11','10') then nearly30days_sale_cost+requisition_amt+material_use_amt else nearly30days_sale_cost end ) total_turnover_day,
sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
        end )/10000 high_stock_amt,      -- 高库存金额
count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_code
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_code
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_code
        end ) high_stock_sku,            -- 高库存SKU
sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end )/10000 no_sales_stock_amt,          -- 无销售库存金额
count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_code   end ) no_sales_stock_sku,   -- 无销售库存SKU
sum(stock_amt) as validity_amt,
count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from  csx_tmp.p_contain_transfer_trunc  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt='20220811'
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_code=b.goods_code
where 1=1
   and purpose_name not in ${hiveconf:purpose_name}
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