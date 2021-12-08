-- 供应链商品销售明细 2021年 
SELECT  
       channel_code,
       channel_name,
        region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       case when dc_code in ('W0K4','W0Z7') then '20' ELSE  business_type_code end business_type_code,
       case when dc_code in ('W0K4','W0Z7') then '联营仓' ELSE  business_type_name end  business_type_name,
       goods_code, 
       b.bar_code,
       b.goods_name,
       b.brand_name,
       b.unit_name,
       b.classify_large_code,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       b.classify_small_code,
       b.classify_small_name,
       sum(a.sales_value)/10000 sales_value,
       sum(a.sales_cost)/10000 sales_cost,
       sum(profit)/10000 profit,
       sum(profit)/sum(sales_value) as profit_rate
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
WHERE SDT>='20210101' 
    AND SDT<='20211130'
GROUP BY   
       channel_code,
       channel_name,
       channel_code,
       channel_name,
        region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       case when dc_code in ('W0K4','W0Z7') then '20' ELSE  business_type_code end ,
       case when dc_code in ('W0K4','W0Z7') then '联营仓' ELSE  business_type_name end ,
       goods_code, 
       b.bar_code,
       b.goods_name,
       b.brand_name,
       b.unit_name,
       b.classify_large_code,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       b.classify_small_code,
       b.classify_small_name
       ;