-- 低销售分析
CREATE
TEMPORARY TABLE csx_tmp.temp_pch_aa AS
SELECT order_no,
       region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       channel_code,
       channel_name,
       business_type_code,
       business_type_name,
       customer_no,
       customer_name,
       sum(sales_value)sales_value,
          sum(profit) profit
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20211101'
  AND logistics_mode_code='2'
  AND business_type_code='1'
  AND channel_code IN ('1')
  AND dc_code NOT IN('W0K4',
                     'W0Z7')
GROUP BY order_no,
         region_code,
         region_name,
         province_code,
         province_name,
         city_group_code,
         city_group_name,
         customer_no,
         customer_name,
         business_type_code,
         business_type_name,
         channel_code,
         channel_name;


SELECT region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       customer_no,
       customer_name,
       cn,
       all_cn,
       sales_value ,
       profit,
       profit_rate,
       sales_value/cn as per_sale,
       sales_value/all_cn as all_per_sale,
       case when sales_value<500  and sales_value>0 then '<500'
            when sales_value>=500 and sales_value<1000 then '[500,1000]'
        else '>=1000' 
        end note 
from (
SELECT region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       customer_no,
       customer_name,
       count(case when sales_value >0 then order_no end) cn,
       count(order_no) as all_cn,
       sum(sales_value) sales_value,
       sum(profit) profit,
       sum(profit)/sum(sales_value) as profit_rate
FROM csx_tmp.temp_pch_aa
GROUP BY region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       customer_no,
       customer_name
) a ;