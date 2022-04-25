-- 高周转&高库存20211012
drop table if exists csx_tmp.tmp_hight_turn_goods ;
create temporary table  csx_tmp.tmp_hight_turn_goods as 
SELECT a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
    --   a.division_code,
    --   a.division_name,
       a.goods_id,
       b.bar_code,
       b.goods_name,
       b.unit_name,
       b.standard,
       b.classify_middle_code,
       b.classify_middle_name,
       a.dept_id,
       a.dept_name,
       coalesce(final_amt/final_qty) as cost,
       a.final_qty,
       a.final_amt,
       a.days_turnover_30,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          unit_name,
          standard,
          bar_code
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
    -- AND purpose IN ('01')
    -- and shop_id in ${hiveconf:dc}
    -- AND sales_region_code='3'
    --and sales_province_code='24'   --稽核省区编码
    ) c ON a.dc_code=c.shop_id
WHERE    
    sdt='20210930'             --更改查询日期
  AND a.final_qty>a.entry_qty
  AND ( (category_large_code='1101' and days_turnover_30>45 AND final_amt>3000)
    or (dept_id in ('H02','H03') and days_turnover_30>5 and a.final_amt>500 )
    OR (dept_id IN ('H04','H05','H06','H07','H08','H09','H10','H11') AND days_turnover_30>15 and a.final_amt>2000) 
    or (division_code ='12' and days_turnover_30>45 and final_amt>2000 )
    or (division_code in ('13','14')  and days_turnover_30>60 and final_amt>3000))
    and final_qty>0
    and a.entry_days>3
    and (a.no_sale_days>7 or no_sale_days='')
  ;
  select * from csx_tmp.tmp_hight_turn_goods;