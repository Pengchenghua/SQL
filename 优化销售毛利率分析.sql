-- TOP45客户毛利率连续下滑趋势分析
WITH weekly_sales AS (
    SELECT 
        csx_week,
        csx_week_begin_end,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        customer_code,
        customer_name,
        business_type_name,
        SUM(sale_amt) AS sale_amt,
        SUM(profit) AS profit,
        CASE WHEN SUM(sale_amt) = 0 THEN 0 ELSE SUM(profit)/SUM(sale_amt) END AS gross_margin
    FROM csx_dws.csx_dws_sale_detail_di a
    LEFT JOIN (
        SELECT code, name, extra 
        FROM csx_dim.csx_dim_basic_topic_dict_df 
        WHERE parent_code = 'direct_delivery_type'
    ) p ON CAST(a.direct_delivery_type AS STRING) = code
    LEFT JOIN (
        SELECT calday, csx_week, csx_week_begin, csx_week_end ,concat_ws('-',csx_week_begin,csx_week_end) as csx_week_begin_end
        FROM csx_dim.csx_dim_basic_date AS d 
        -- WHERE d.sdt >= '20250401' AND d.sdt <= '20250729'
    ) dd ON a.sdt = dd.calday
    WHERE sdt >= '20250401' AND sdt <= '20250729'
    AND ((business_type_code=1 AND extra='采购参与'))
    AND customer_code IN ('243348','233646','252028','252038','265077','258144','111608','112554','115080','127391','223402','225238','226207','244105','251541','257860','258358','259454','268462','131129','131187','249548','250879','255475','124524','230335','231077','255101','117262','122129','222798','258838','126387','263986','211834','254068','131501','216229','259023','258261','164512','168890','153127','259072','256667')
    GROUP BY csx_week, customer_code, customer_name, performance_region_name, 
             performance_province_code, performance_province_name, 
             performance_city_code, performance_city_name, business_type_name,
             csx_week_begin_end
),

margin_trend AS (
    SELECT 
        *,
        LAG(gross_margin, 1) OVER (PARTITION BY customer_code ORDER BY csx_week) AS prev_margin_1,
        LAG(gross_margin, 2) OVER (PARTITION BY customer_code ORDER BY csx_week) AS prev_margin_2,
        LAG(gross_margin, 3) OVER (PARTITION BY customer_code ORDER BY csx_week) AS prev_margin_3,
        ROW_NUMBER() OVER (PARTITION BY customer_code ORDER BY csx_week DESC) AS week_rank
    FROM weekly_sales
)

SELECT 
    csx_week,
    csx_week_begin_end,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    customer_code,
    customer_name,
    business_type_name,
    sale_amt,
    profit,
    gross_margin,
    prev_margin_1,
    prev_margin_2,
    prev_margin_3
FROM margin_trend
WHERE week_rank <= 4  -- 只考虑最近4周
-- AND (
--     (prev_margin_1 IS NOT NULL AND gross_margin < prev_margin_1) AND
--     (prev_margin_2 IS NOT NULL AND prev_margin_1 < prev_margin_2) AND
--     (prev_margin_3 IS NOT NULL AND prev_margin_2 < prev_margin_3)
-- )
ORDER BY customer_code, csx_week DESC;