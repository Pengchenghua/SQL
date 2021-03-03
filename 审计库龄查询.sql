SELECT company_code,company_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       sum(final_amt) as final_amt,
       sum(final_qty) as final_qty,
       sum(case when entry_days BETWEEN 1 and 30 then final_amt end ) as final_amt_30,
       sum(case when entry_days BETWEEN 31 and 60 then final_amt end ) as final_amt_60, 
       sum(case when entry_days BETWEEN 61 and 90 then final_amt end ) as final_amt_90,
       sum(case when entry_days BETWEEN 91 and 180 then final_amt end ) as final_amt_180,
       sum(case when entry_days BETWEEN 181 and 360 then final_amt end ) as final_amt_360, 
       sum(case when entry_days BETWEEN 361 and 720 then final_amt end ) as final_amt_720,
       sum(case when entry_days >= 721  then final_amt end ) as final_amt_3y
FROM csx_tmp.ads_wms_r_d_goods_turnover a 
join 
(select shop_id,shop_name,sales_province_code,sales_province_name,company_code,company_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and table_type='1') b on a.dc_code=b.shop_id
WHERE sdt='20201231'
group by company_code,company_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name;