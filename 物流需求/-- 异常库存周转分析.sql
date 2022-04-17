-- 异常库存周转分析【分析】
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

select * from csx_tmp.temp_00 a where a.dc_code='W053'
        AND a.goods_id='648234';

drop table  csx_tmp.temp_01 ;
create temporary table csx_tmp.temp_01 as 
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
 contain_transfer_entry_days 
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
from csx_tmp.ads_wms_r_d_goods_turnover  a
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
    and ( (division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 )
         or (division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 )
          or( division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 ))
 ) a 
  ;
  
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
 contain_transfer_entry_days 
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
from csx_tmp.ads_wms_r_d_goods_turnover  a
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
    and (a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 )
 ) a 
  ;
  
  
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
 a.no_sale_days , 
 a.max_sale_sdt ,
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