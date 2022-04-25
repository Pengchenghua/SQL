-- 白糖销售明细【大宗】
SELECT substr(sdt,1,6) mon,
    sdt,
    channel_code,
       channel_name,
       province_code,
       province_name,
       customer_no,
       customer_name,
       goods_code,
       goods_name,
       sum(sales_qty),
       sum(sales_value),
       sum(profit)
FROM csx_dw.dws_sale_r_d_detail
WHERE channel_code IN ('6')
  AND sdt>='20210601'
  AND sdt<='20210831'
  AND goods_code='266'
  GROUP BY  channel_code,
       channel_name,
       province_code,
       province_name,
       customer_no,
       customer_name,
       goods_code,
       goods_name,
        substr(sdt,1,6)
        ,sdt;