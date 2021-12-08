--供应链绩效方案数据草稿【20211206】徐力 
--入库指定DC仓【徐力提供】
-- 'WA93','W0A2','W080','W0K7','W0L4','W0AW','W0J8','W048','WB04','W0A3','WB11','W0A8','WB03','W053','W0F4','W0G9','W0K6','W0AH','W0AJ','W0J2','W0F7','W0G6','WA96','W0K1','W0AU','W0L3','W0BK','W0AL','W0S9','W0Q2','W0Q9','W0Q8','W0BS','W0BH','W0BR','W0R9','WB00','W0R8','W088','W0BZ','W0A5','W0P8','WA94','W0AS','W0AR','WA99','W0N1','W079','W0A6','W0BD','W0N0','WB01','W0P3','W0W7','W0X1','W0X2','W0Z8','W0Z9','W0AZ','W039','W0A7'




-- 供应链商品销售明细 2020年--2021年 

SELECT  mon,
       channel_code,
       channel_name,
       business_type_code,
       business_type_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       sales_value,
       sales_cost,
        profit
from 
(
SELECT  substr(sdt,1,6) as mon,
       channel_code,
       channel_name,
       case when dc_code in ('W0K4','W0Z7') then '20' ELSE  business_type_code end business_type_code,
       case when dc_code in ('W0K4','W0Z7') then '联营仓' ELSE  business_type_name end  business_type_name,
       b.classify_large_code,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       sum(a.sales_value)/10000 sales_value,
       sum(a.sales_cost)/10000 sales_cost,
       sum(profit)/10000 profit
FROM csx_dw.dws_sale_r_d_detail a 
join 
(SELECT goods_id,
        bar_code,
       goods_name,
       brand_name,
       unit_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
    -- and classify_middle_code ='B0302'
)b on a.goods_code=b.goods_id
WHERE SDT>='20211001' 
    AND SDT<='20211130'
    and channel_code ='1'
    and business_type_code!='4'
GROUP BY  substr(sdt,1,6), 
       channel_code,
       channel_name,
       case when dc_code in ('W0K4','W0Z7') then '20' ELSE  business_type_code end ,
       case when dc_code in ('W0K4','W0Z7') then '联营仓' ELSE  business_type_name end ,
       b.classify_large_code,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       b.classify_small_code,
       b.classify_small_name
    )a    ;


-- 入库
select classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       sum(a.amount)/10000 entry_amt
from csx_dw.dws_wms_r_d_entry_detail a
join 
(SELECT goods_id,
        bar_code,
       goods_name,
       brand_name,
       unit_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
    -- and classify_middle_code ='B0302'
)b on a.goods_code=b.goods_id
where sdt>='20211101' and sdt<='20211130'
and a.order_type_code like 'P%'
and a.receive_location_code in ('WA93','W0A2','W080','W0K7','W0L4','W0AW','W0J8','W048','WB04','W0A3','WB11','W0A8','WB03','W053','W0F4','W0G9','W0K6','W0AH','W0AJ','W0J2','W0F7','W0G6','WA96','W0K1','W0AU','W0L3','W0BK','W0AL','W0S9','W0Q2','W0Q9','W0Q8','W0BS','W0BH','W0BR','W0R9','WB00','W0R8','W088','W0BZ','W0A5','W0P8','WA94','W0AS','W0AR','WA99','W0N1','W079','W0A6','W0BD','W0N0','WB01','W0P3','W0W7','W0X1','W0X2','W0Z8','W0Z9','W0AZ','W039','W0A7'
)
    and a.business_type='01'
    group by classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name;
    





--供应商入库【集采标识】
select classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       a.supplier_code,
       a.supplier_name,
       joint_purchase,
       sum(a.amount)/10000 entry_amt
from csx_dw.dws_wms_r_d_entry_detail a
join 
(SELECT goods_id,
        bar_code,
       goods_name,
       brand_name,
       unit_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
    -- and classify_middle_code ='B0302'
)b on a.goods_code=b.goods_id
left join
(select vendor_id,joint_purchase from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') j on a.supplier_code=j.vendor_id
where sdt>='20211101' and sdt<='20211130'
and a.order_type_code like 'P%'
and a.business_type='01'  --供应商配送
and a.receive_location_code in ('WA93','W0A2','W080','W0K7','W0L4','W0AW','W0J8','W048','WB04','W0A3','WB11','W0A8','WB03','W053','W0F4','W0G9','W0K6','W0AH','W0AJ','W0J2','W0F7','W0G6','WA96','W0K1','W0AU','W0L3','W0BK','W0AL','W0S9','W0Q2','W0Q9','W0Q8','W0BS','W0BH','W0BR','W0R9','WB00','W0R8','W088','W0BZ','W0A5','W0P8','WA94','W0AS','W0AR','WA99','W0N1','W079','W0A6','W0BD','W0N0','WB01','W0P3','W0W7','W0X1','W0X2','W0Z8','W0Z9','W0AZ','W039','W0A7'
)
  
    group by classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       a.supplier_code,
       a.supplier_name,
       joint_purchase;