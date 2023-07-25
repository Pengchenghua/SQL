
SET hive.execution.engine=spark;

-- insert overwrite local directory '/tmp/pengchenghua/temp01'
 -- insert overwrite local directory '/tmp/pengchenghua/temp01'
 -- 計算銷售佔比80% 商品清單

CREATE
TEMPORARY TABLE csx_tmp.temp_goods_sale_01 AS
SELECT goods_code,
       sales_value,
       sales_value/sum(sales_value)over() AS sale_ratio,
                                       sum(sales_value)over(partition BY 1
                                                            ORDER BY sales_value ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)/sum(sales_value)over() AS sum_zb
FROM
  (SELECT goods_code,
          sum(sales_value)sales_value
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>='20200101'
     AND sdt<'20201001'
     AND dc_code='W0A3'
     AND channel IN ('1',
                     '7')
     AND division_code IN ('12',
                           '13')
   GROUP BY goods_code) a
ORDER BY sales_value DESC;

-- 每月销售情况

CREATE TEMPORARY TABLE csx_tmp.temp_goods_sale_02 AS
SELECT goods_code,
       goods_name,
       division_code,
       division_name,
       department_id,
       department_name,
       category_large_code,
       category_large_name,
       category_small_code,
       category_small_name,
       unit_name,
       qualitative_period,
       mon,
       sum(sales_value) as sales_value,
       count(DISTINCT customer_no) AS sale_cust_num,
       sum(sale_sdt) sum_sdt
FROM
  (SELECT customer_no,
          a.goods_code,
          substr(sdt,1,6) AS mon,
          count(DISTINCT sdt) AS sale_sdt,
          sum(sales_value)sales_value
   FROM csx_dw.dws_sale_r_d_customer_sale a
   JOIN
     (SELECT a.goods_code,
             a.sum_zb
      FROM csx_tmp.temp_goods_sale_01 a
      WHERE sum_zb<=0.8) b ON a.goods_code=b.goods_code
   WHERE sdt>='20200101'
     AND sdt<'20201001'
     AND dc_code='W0A3'
     AND division_code IN ('12', '13')
   GROUP BY customer_no,
            a.goods_code,
            substr(sdt,1,6)) a
JOIN
  (SELECT goods_id,
          goods_name,
          division_code,
          division_name,
          department_id,
          department_name,
          category_large_code,
          category_large_name,
          category_small_code,
          category_small_name,
          unit_name,
          qualitative_period
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_code=b.goods_id
GROUP BY goods_code,
         mon,
         goods_name,
         division_code,
         division_name,
         department_id,
         department_name,
         category_large_code,
         category_large_name,
         category_small_code,
         category_small_name,
         unit_name,
         qualitative_period;

-- 统计汇总
select 
       a.goods_code,
       goods_name,
       division_code,
       division_name,
       department_id,
       department_name,
       category_large_code,
       category_large_name,
       category_small_code,
       category_small_name,
       unit_name,
       qualitative_period,
       count(mon ) as sale_mon,
       sum(sales_value)/count(mon) as sales_value,
       sum(sale_cust_num) AS sale_cust_num,
       sum(sales_value) as sales_value,
       sum(sum_sdt) sum_sdt,
       max(a.sales_value) max_sales,
       min(a.sales_value) min_sales,
       max(a.sale_cust_num) max_cust_mun,
       min(a.sale_cust_num) min_cust_num,
       sum_zb
from csx_tmp.temp_goods_sale_02 a 
join 
 (SELECT a.goods_code,
             a.sum_zb
      FROM csx_tmp.temp_goods_sale_01 a
    ) b ON a.goods_code=b.goods_code
group by 
       a.goods_code,
       goods_name,
       division_code,
       division_name,
       department_id,
       department_name,
       category_large_code,
       category_large_name,
       category_small_code,
       category_small_name,
       unit_name,
       qualitative_period,
       sum_zb
       ;
