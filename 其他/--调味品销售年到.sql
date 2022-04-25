--调味品销售年到
SELECT substr(sdt,1,6) AS mon,
       province_code,
       province_name,
       channel_name,
       business_type_name,
       goods_code,
       goods_name,
       brand_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       sum(sales_qty)qty,
       sum(sales_value)sales_value,
       sum(profit)profit
FROM csx_dw.dws_sale_r_d_detail a 

WHERE sdt>='20210101'
and classify_middle_code='B0602'
GROUP BY 
       substr(sdt,1,6) ,
       province_code,
       province_name,
       channel_name,
       business_type_name,
       goods_code,
       goods_name,
       brand_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
       ;