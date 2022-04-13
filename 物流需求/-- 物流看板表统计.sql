-- 物流看板表统计
-- 省区库存看板统计
drop table csx_tmp.report_wms_r_d_turnover_provinve_kanban_fr;
create table csx_tmp.report_wms_r_d_turnover_provinve_kanban_fr as 
select 
province_name,
sku,
total_amt,
total_turnover_day,
fresh_sku,
fresh_amt,
fresh_turnover_day,
food_sku,
food_amt,
food_turnover_day,
no_wine_food_sku,
no_wine_food_amt,
no_wine_turnover_day
from 
(select 
 province_name,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt)/10000 total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
count( distinct case when division_code in ('11','10') and final_qty != 0 then goods_id end ) fresh_sku,
sum(case when division_code in ('11','10') then final_amt end )/10000 fresh_amt,
sum(case when division_code in ('11','10') then period_inv_amt_30day end )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt end ) as fresh_turnover_day,
count( distinct case when division_code in ('12','13','14') and final_qty != 0 then goods_id end ) food_sku,
sum(case when division_code in ('12','13','14') then final_amt end )/10000 food_amt,
sum(case when division_code in ('12','13','14') then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') then cost_30day  end ) as food_turnover_day,
count( distinct case when division_code in ('12','13','14') and classify_middle_code !='B0401' and final_qty != 0 then goods_id end ) no_wine_food_sku,
sum(case when division_code in ('12','13','14') and classify_middle_code !='B0401' then final_amt end  )/10000 no_wine_food_amt,
sum(case when division_code in ('12','13','14') AND classify_middle_code !='B0401' then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') and classify_middle_code!='B0401' then cost_30day  end ) as no_wine_turnover_day
from csx_tmp.ads_wms_r_d_goods_turnover  a
 join 
 (select shop_id,purchase_org, case when purchase_org ='P620' then '全国-平台' else   province_name end province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.dc_code=b.shop_id
where sdt='20220412' 
    and dc_uses !=''
    and division_code in ('11','10','12','13','14')
group by province_name
union all 
select 
'全国' province_name,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt)/10000 total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
count( distinct case when division_code in ('11','10') and final_qty != 0 then goods_id end ) fresh_sku,
sum(case when division_code in ('11','10') then final_amt end )/10000 fresh_amt,
sum(case when division_code in ('11','10') then period_inv_amt_30day end )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt end ) as fresh_turnover_day,
count( distinct case when division_code in ('12','13','14') and final_qty != 0 then goods_id end ) food_sku,
sum(case when division_code in ('12','13','14') then final_amt end )/10000 food_amt,
sum(case when division_code in ('12','13','14') then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') then cost_30day  end ) as food_turnover_day,
count( distinct case when division_code in ('12','13','14') and classify_middle_code !='B0401' and final_qty != 0 then goods_id end ) no_wine_food_sku,
sum(case when division_code in ('12','13','14') and classify_middle_code !='B0401' then final_amt end  )/10000 no_wine_food_amt,
sum(case when division_code in ('12','13','14') AND classify_middle_code !='B0401' then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') and classify_middle_code!='B0401' then cost_30day  end ) as no_wine_turnover_day
from csx_tmp.ads_wms_r_d_goods_turnover  a
 join 
 (select shop_id,purchase_org, case when purchase_org ='P620' then '全国-平台' else   province_name end province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.dc_code=b.shop_id
where sdt='20220412' 
    and dc_uses !=''
     and division_code in ('11','10','12','13','14')
 
) a 
order by total_amt desc
;



-- 省区管理品类库存看板统计
-- 【指标说明】
-- 【更新记录】
-- 1、周转天数：期间库存金额/期间库存成本
-- 2、未销售： 末次销售天数>30天且库存额>0元且入库天数>7; 
-- 3、用品高库存: 周转>45天,库存额>2000元 ，入库天数>7天; 
-- 4、食品高库存: 周转大于30天，库存额>2000元，入库天数>7天；
-- 5、生鲜高库存 : 周转天数>15天且入库天数>3天且库存额>500元 ；
-- 6、SKU数:期末有库存或期间有销售商品数
-- 7、动销SKU ：期间有销售的商品
-- 8、动销率：动销SKU/SKU数
-- 9、负库存 ：库存数量<0
-- 10、30天周转天数：近30天期间库存金额/近30天累计销售成本；累计销售成本<=0且期间库存额>0 周转天数默认 9999
-- 11、销售数据剔除：一件代发、客户直送、客户配送
-- 12、入库剔除：客退入库、客户直送、货到即配、地采
drop table csx_tmp.report_wms_r_d_turnover_classify_kanban_fr;
create table csx_tmp.report_wms_r_d_turnover_classify_kanban_fr as 
select 
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
(select 
 province_name,
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
from csx_tmp.ads_wms_r_d_goods_turnover  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt='20220412'
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where sdt='20220412' 
    and dc_uses !=''
    and division_code in ('11','10','12','13','14')
group by province_name,
classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name 
union all 
select 
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
from csx_tmp.ads_wms_r_d_goods_turnover  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt='20220412'
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where sdt='20220412' 
    and dc_uses !=''
    and division_code in ('11','10','12','13','14')
group by 
classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name 
) a 
order by total_amt desc
;

-- DC 库存统计看板
drop table csx_tmp.report_wms_r_d_turnover_dc_kanban_fr;
create table csx_tmp.report_wms_r_d_turnover_dc_kanban_fr as 
select 
province_name,
dc_code,
dc_name,
dc_uses,
sku,
total_amt,
total_turnover_day,
fresh_sku,
fresh_amt,
fresh_turnover_day,
food_sku,
food_amt,
food_turnover_day,
no_wine_food_sku,
no_wine_food_amt,
no_wine_turnover_day
from 
(select 
 province_name,
 dc_code,
 dc_name,
 dc_uses,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt)/10000 total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
count( distinct case when division_code in ('11','10') and final_qty != 0 then goods_id end ) fresh_sku,
sum(case when division_code in ('11','10') then final_amt end )/10000 fresh_amt,
sum(case when division_code in ('11','10') then period_inv_amt_30day end )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt end ) as fresh_turnover_day,
count( distinct case when division_code in ('12','13','14') and final_qty != 0 then goods_id end ) food_sku,
sum(case when division_code in ('12','13','14') then final_amt end )/10000 food_amt,
sum(case when division_code in ('12','13','14') then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') then cost_30day  end ) as food_turnover_day,
count( distinct case when division_code in ('12','13','14') and classify_middle_code !='B0401' and final_qty != 0 then goods_id end ) no_wine_food_sku,
sum(case when division_code in ('12','13','14') and classify_middle_code !='B0401' then final_amt end  )/10000 no_wine_food_amt,
sum(case when division_code in ('12','13','14') AND classify_middle_code !='B0401' then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') and classify_middle_code!='B0401' then cost_30day  end ) as no_wine_turnover_day
from csx_tmp.ads_wms_r_d_goods_turnover  a
where sdt='20220412' 
    and dc_uses !=''
    and division_code in ('11','10','12','13','14')
group by province_name,
 dc_code,
 dc_name,
 dc_uses

 
) a 
order by total_amt desc
;