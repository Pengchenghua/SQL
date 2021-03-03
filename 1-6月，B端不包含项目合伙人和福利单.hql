-- 1-6月，B端不包含项目合伙人和福利单，
-- 字段: 省区，部类，大中小类，
-- 商品，
-- 销售额，
-- 销售占比，
-- 销售月份数（如果每个月都有就是6），
-- 月平均销售，
-- -- 按月计算
-- 销售波动最大，
-- 销售波动最小，
-- 客户数（去重），
-- 月平均客户数，
-- 客户数波动最大，
-- 客户数波动最小，
-- 月销售频次，
-- 客户天数，

DROP TABLE csx_tmp.temp_goods_top;


CREATE
TEMPORARY TABLE csx_tmp.temp_goods_top AS
SELECT division_code,
       division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       goods_code,
       goods_name,
       unit,
       sales_value,
       row_number()over(PARTITION BY division_code
                        ORDER BY sales_value DESC) AS row_num
FROM
  (SELECT CASE
              WHEN division_code IN ('12',
                                     '13',
                                     '14') THEN '12'
              WHEN division_code IN ('10',
                                     '11') THEN '11'
              ELSE division_code
          END AS division_code,
          CASE
              WHEN division_code IN ('12',
                                     '13',
                                     '14') THEN '食百采购部'
              WHEN division_code IN ('10',
                                     '11') THEN '生鲜采购部'
              ELSE division_name
          END AS division_name,
          category_large_code,
          category_large_name,
          category_middle_code,
          category_middle_name,
          category_small_code,
          category_small_name,
          goods_code,
          goods_name,
          unit,
          sum(sales_value)AS sales_value
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>='20200101'
     AND sdt<'20200701'
     AND is_self_sale='1'
     AND channel='1'
     AND order_kind!='WELFARE'
   GROUP BY CASE
                WHEN division_code IN ('12',
                                       '13',
                                       '14') THEN '12'
                WHEN division_code IN ('10',
                                       '11') THEN '11'
                ELSE division_code
            END,
            CASE
                WHEN division_code IN ('12',
                                       '13',
                                       '14') THEN '食百采购部'
                WHEN division_code IN ('10',
                                       '11') THEN '生鲜采购部'
                ELSE division_name
            END,
            category_large_code,
            category_large_name,
            category_middle_code,
            category_middle_name,
            category_small_code,
            category_small_name,
            goods_code,
            goods_name,
            unit) a ;


DROP TABLE csx_tmp.temp_goods_top_01;


CREATE
TEMPORARY TABLE csx_tmp.temp_goods_top_01 AS
SELECT province_code,
       case when division_code in ('11','10')then '11' 
                                 when division_code in ('12','13','14')then '12'
                                 else division_code end as division_code,
       case when division_code in ('11','10')then '生鲜采购部' 
                                 when division_code in ('12','13','14')then '食百采购部'
                                 else division_name end as division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       goods_code,
       goods_name,
       unit,
       count(DISTINCT substring(sdt,1,6)) AS sale_mon,
       sum(sales_value)/6 AS avg_sale,
       sum(sales_value)AS sales_value,
       count(DISTINCT customer_no)AS sale_cust, --销售客户数
 --   avg(customer_no) as avg_cust,
 count(DISTINCT sdt) AS sale_sdt --   count(DISTINCT sdt)over(PARTITION BY goods_code,substring(sdt,1,6)) as sale_sdt,
 --   max(sum(sales_value))over(PARTITION BY a.goods_code,substring(sdt,1,6)) as max_sale
FROM csx_dw.dws_sale_r_d_customer_sale a
WHERE sdt>='20200101'
  AND sdt<'20200701'
  AND is_self_sale='1'
  AND channel='1'
  AND order_kind!='WELFARE'
  AND province_code='15'
GROUP BY case when division_code in ('11','10')then '11' 
                                 when division_code in ('12','13','14')then '12'
                                 else division_code end ,
       case when division_code in ('11','10')then '生鲜采购部' 
                                 when division_code in ('12','13','14')then '食百采购部'
                                 else division_name end,
        province_code,
         category_large_code,
         category_large_name,
         category_middle_code,
         category_middle_name,
         category_small_code,
         category_small_name,
         goods_code,
         goods_name,
         unit;


DROP TABLE csx_tmp.temp_goods_top_02;


CREATE
TEMPORARY TABLE csx_tmp.temp_goods_top_02 AS
SELECT province_code,
       goods_code,
       max(sales_value)AS max_sale,
       min(sales_value) AS min_sale,
       sum(sale_cust)/6 AS avg_sale_cust, --平均客户数
       sum(sale_cust)as sale_cust,
 max(sale_cust)AS max_sale_cust,
 min(sale_cust)AS min_sale_cust,
 sum(sale_sdt) AS sale_sdt
FROM
  (SELECT substring(sdt,1,6) AS mon,
          province_code,
          goods_code,
          sum(sales_value)AS sales_value,
          count(DISTINCT customer_no)AS sale_cust, --销售客户数
          count(DISTINCT sdt) AS sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>='20200101'
     AND sdt<'20200701'
     AND is_self_sale='1'
     AND channel='1'
     AND order_kind!='WELFARE'
     AND province_code='15'
   GROUP BY division_code,
            province_code,
            goods_code,
            substring(sdt,1,6) 
    )as  a
GROUP BY province_code,
         goods_code
;

drop table  csx_tmp.temp_peng_goods_sale;
create table csx_tmp.temp_peng_goods_sale
as 
select a.*,b.max_sale,b.min_sale,b.sale_cust as b_sale_cust,b.max_sale_cust,min_sale_cust,b.sale_sdt as b_sale_sdt,b.avg_sale_cust as b_avg_sale_cust from csx_tmp.temp_goods_top_01 as a 
left join 
(select 
       goods_code,
       max_sale,
       min_sale,
      sale_cust, --平均客户数
       max_sale_cust,
       min_sale_cust,
       sale_sdt ,avg_sale_cust
    from csx_tmp.temp_goods_top_02) as b  on a.goods_code=b.goods_code