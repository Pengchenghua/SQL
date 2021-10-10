-- 供应链商品销售明细 2020年--2021年 
SELECT substr(sdt,1,4) years,
       channel_code,
       channel_name,
       business_type_code,
       business_type_name,
       goods_code, 
       b.goods_name,
       b.unit_name,
       b.brand_name,
       b.classify_large_code,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       b.classify_small_code,
       b.classify_small_name,
       sum(a.sales_value)/10000 sales_value
FROM CSX_DW.dws_sale_r_d_detail a 
join 
(SELECT goods_id,
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
WHERE SDT>='20200101' 
    AND SDT<='20210922'
GROUP BY  substr(sdt,1,4) ,
       channel_code,
       channel_name,
       business_type_code,
       business_type_name,
       goods_code, 
       b.goods_name,
       b.brand_name,
       b.unit_name,
       b.classify_large_code,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       b.classify_small_code,
       b.classify_small_name