-- 预制品商品明细
drop table csx_tmp.temp_yzp_goods;
CREATE TABLE csx_tmp.temp_yzp_goods
(bar_code STRING COMMENT '预制品条码',
cate_name STRING comment '预制品分类名称'
) COMMENT '预制品商品明细'
row format delimited fields terminated by','

;



-- 预制品销售处理
drop table  csx_tmp.temp_sale_t1;
create temporary table csx_tmp.temp_sale_t1 as 
select province_code,
        province_name,
        a.customer_no,
        a.customer_name,
        a.first_category_code,
        a.first_category_name,
        a.second_category_code,
        a.second_category_name,
        a.goods_code,
        b.cate_name,
        if(b.bar_code is null ,0,1) yzp_type,
        sum(sales_value)/10000 sales_value,
        sum(profit)/10000 profit
from csx_dw.dws_sale_r_d_detail  a 
left join
 csx_tmp.temp_yzp_goods b on a.goods_code=b.bar_code 
where sdt>='20220201' 
and sdt<'20220501'
and a.channel_code in ('1','7','9')
and a.business_type_code='1'
group by province_code,
        province_name,
        a.customer_no,
        a.customer_name,
        a.first_category_code,
        a.first_category_name,
        a.second_category_code,
        a.second_category_name,
        if(b.bar_code is null ,0,1),
         b.cate_name,
         GOODS_CODE
        ;


-- 省区数据汇总
SELECT province_code,
       province_name,
       sum(yzp_sales) yzp_sales,
       sum(yzp_profit)      yzp_profit,
       sum(yzp_profit)/sum(yzp_sales) yzp_profit_rate,
       sum(yzp_pin_sku) yzp_pin_sku,
       sum(yzp_pin_customer)/sum(pin_customer)  pin_customer_rate,
       sum(yzp_pin_customer) yzp_pin_customer,
       sum(sales) sales,
       sum(profit) profit,
       sum(profit_rate) profit_rate,
       sum(pin_sku) pin_sku,
       sum(pin_customer) pin_customer,
       sum(yzp_sales)/ sum(sales) sales_ratio
FROM
(
        
SELECT province_code,
       province_name,
       sum(sales_value) yzp_sales,
       sum(profit)      yzp_profit,
       sum(profit)/sum(sales_value) yzp_profit_rate,
       count(DISTINCT case when sales_value>0  then goods_code end ) yzp_pin_sku,
       count(DISTINCT case when sales_value>0  then customer_no end ) yzp_pin_customer,
       0 sales,
       0 profit,
       0 profit_rate,
       0 pin_sku,
       0 pin_customer
FROM csx_tmp.temp_sale_t1
WHERE yzp_type =1
GROUP BY province_code,
       province_name
union all 
SELECT province_code,
       province_name,
       0 yzp_sales,
       0 yzp_profit,
       0 yzp_profit_rate,
       0 yzp_pin_sku,
       0 yzp_pin_customer,
       sum(sales_value) sales,
       sum(profit)profit,
       sum(profit)/sum(sales_value)profit_rate,
       count(DISTINCT case when sales_value>0  then goods_code end ) pin_sku,
       count(DISTINCT case when sales_value>0  then customer_no end ) pin_customer
FROM csx_tmp.temp_sale_t1
WHERE 1=1 
GROUP BY province_code,
       province_name
       )a 
       GROUP BY province_code,
       province_name;
       

-- 分类数据汇总
SELECT cate_name,
       (yzp_sales) yzp_sales,
       (yzp_profit)      yzp_profit,
       (yzp_profit)/(yzp_sales) yzp_profit_rate,
       (yzp_pin_sku) yzp_pin_sku,
       (yzp_pin_customer) yzp_pin_customer,
       (yzp_pin_customer)/(yzp_all_pin_customer)  pin_customer_rate,
       (yzp_pin_customer) yzp_pin_customer,
       yzp_all_pin_goods,
       yzp_all_pin_customer
FROM
(
SELECT cate_name,
       sum(sales_value) yzp_sales,
       sum(profit)      yzp_profit,
       sum(profit)/sum(sales_value) yzp_profit_rate,
       count(DISTINCT case when sales_value>0  then goods_code end ) yzp_pin_sku,
       count(DISTINCT case when sales_value>0  then customer_no end ) yzp_pin_customer
FROM csx_tmp.temp_sale_t1
WHERE yzp_type =1
GROUP BY cate_name
       )a 
left join 
(
SELECT  count(DISTINCT case when sales_value>0  then goods_code end ) yzp_all_pin_goods,
       count(DISTINCT case when sales_value>0  then customer_no end ) yzp_all_pin_customer
FROM csx_tmp.temp_sale_t1
WHERE yzp_type =1
       ) b on 1=1
     
     ;
     
     
     -- 省区行业TOP3


SELECT province_code,
       province_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sales_value,
       profit,
       profit/sales_value profit_rate,
       pin_customer,
       pin_goods,
       dense_rank()over(partition by province_name order by sales_value desc) rank_aa
       
FROM
(

SELECT province_code,
       province_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sum(sales_value) sales_value,
       sum(profit) profit,
       count(DISTINCT customer_no) pin_customer,
       count(DISTINCT goods_code) pin_goods
       
FROM csx_tmp.temp_sale_t1
WHERE yzp_type=1 
GROUP BY 
    province_code,
    province_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name
union all 
SELECT '00' province_code,
       '全国'province_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sum(sales_value) sales_value,
       sum(profit) profit,
       count(DISTINCT customer_no) pin_customer,
       count(DISTINCT goods_code) pin_goods
       
FROM csx_tmp.temp_sale_t1
WHERE yzp_type=1 
GROUP BY 
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name
) a ;


-- 预制品分类行业排名

SELECT cate_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sales_value,
       profit,
       profit/sales_value profit_rate,
       pin_customer,
       pin_goods,
       dense_rank()over(partition by cate_name order by sales_value desc) rank_aa
       
FROM
(

SELECT cate_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sum(sales_value) sales_value,
       sum(profit) profit,
       count(DISTINCT customer_no) pin_customer,
       count(DISTINCT goods_code) pin_goods
       
FROM csx_tmp.temp_sale_t1
WHERE yzp_type=1 
GROUP BY 
   cate_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name
union all 
SELECT 
       '全国'cate_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sum(sales_value) sales_value,
       sum(profit) profit,
       count(DISTINCT customer_no) pin_customer,
       count(DISTINCT goods_code) pin_goods
       
FROM csx_tmp.temp_sale_t1
WHERE yzp_type=1 
GROUP BY 
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name
) a ;

-- 预制品分类客户排名
SELECT cate_name,
       customer_no,
       customer_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sales_value,
       profit,
       pin_customer,
       pin_goods,
       rank_a
      
FROM
(
SELECT cate_name,
       customer_no,
       customer_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sales_value,
       profit,
       pin_customer,
       pin_goods,
       dense_rank() OVER (PARTITION BY cate_name ORDER BY sales_value desc) rank_a
      
FROM
(

SELECT cate_name,
       customer_no,
       customer_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sum(sales_value) sales_value,
       sum(profit) profit,
       count(DISTINCT customer_no) pin_customer,
       count(DISTINCT goods_code) pin_goods
       
FROM csx_tmp.temp_sale_t1
WHERE yzp_type=1 
GROUP BY 
    cate_name,
    customer_no,
    customer_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name
) a

) a
where
rank_a<11
;



-- 省区客户排名
SELECT province_code,
        province_name,
       customer_no,
       customer_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sales_value,
       profit,
       pin_customer,
       pin_goods,
       rank_a
      
FROM
(
SELECT province_code,
        province_name,
       customer_no,
       customer_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sales_value,
       profit,
       pin_customer,
       pin_goods,
       dense_rank() OVER (PARTITION BY province_code ORDER BY sales_value desc) rank_a
      
FROM
(

SELECT province_code,
        province_name,
       customer_no,
       customer_name,
       first_category_code,
       first_category_name,
       second_category_code,
       second_category_name,
       sum(sales_value) sales_value,
       sum(profit) profit,
       count(DISTINCT customer_no) pin_customer,
       count(DISTINCT goods_code) pin_goods
       
FROM csx_tmp.temp_sale_t1
WHERE yzp_type=1 
GROUP BY 
    province_code,
    province_name,
    customer_no,
    customer_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name
) a

) a
where 1=1
-- rank_a<11
;