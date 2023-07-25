--日配分级查询

SELECT region_code,
       region_name,
       a.province_code,
       province_name,
       a.city_group_code,
       city_group_name,
       a.customer_no,
       customer_name,
       b.first_category_name,
       b.second_category_name,
       b.third_category_name,
       contact_phone,
        contact_person,
        customer_address_full,
        customer_level_name,
    	customer_level_desc,
        customer_large_level, 
        customer_small_level, 
        customer_level_tag,
       collect_list( DISTINCT a.business_type_name) business_type_name ,
       sum(case when a.business_type_code='1' then a.sales_value end ) daily_sales_value,
       sum(case when a.business_type_code='1' then a.profit end ) daily_profit,
       sum(case when a.business_type_code !='1' then a.sales_value end ) qt_sales_value,
       sum(case when a.business_type_code !='1' then a.profit end ) qt_profit
FROM csx_dw.dws_sale_r_d_detail a 
LEFT JOIN 
( select customer_no,
        first_category_name,
        second_category_name,
        third_category_name,
        contact_phone,
        contact_person,
        customer_address_full,
        customer_level_name,
    	customer_level_desc
from   csx_dw.dws_crm_w_a_customer
where sdt='20220228')  b on a.customer_no=b.customer_no
LEFT JOIN 
(select province_code,
            customer_no,
            city_group_code,
            customer_large_level, 
            customer_small_level, 
            customer_level_tag
from csx_dw.report_sale_r_m_customer_level 
where month='202202') c on a.customer_no=c.customer_no and a.province_code=c.province_code
WHERE sdt>='20220215'
 -- AND business_type_code='1'
 and a.business_type_code !='4'
  AND channel_code IN ('1',
                       '7',
                       '9')
GROUP BY region_code,
       region_name,
       a.province_code,
       province_name,
       a.city_group_code,
       city_group_name,
       a.customer_no,
       customer_name,
       b.first_category_name,
       b.second_category_name,
       b.third_category_name,
       contact_phone,
        contact_person,
        customer_address_full,
        customer_level_name,
    	customer_level_desc,
        customer_large_level, 
        customer_small_level, 
        customer_level_tag
        ;



-- 非日配销售情况
        SELECT region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       a.customer_no,
       customer_name,
       first_category_name,
       second_category_name,
       third_category_name,
       business_type_name ,
        contact_phone,
        contact_person,
        customer_address_full,
        customer_level_name,
    	customer_level_desc,
       sales_value,
       profit,
       profit_rate
from (
SELECT region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       a.customer_no,
       customer_name,
       b.first_category_name,
       b.second_category_name,
       b.third_category_name,
        contact_phone,
        contact_person,
        customer_address_full,
        customer_level_name,
    	customer_level_desc,
       collect_list( DISTINCT a.business_type_name) business_type_name ,
       sum(a.sales_value  ) sales_value,
       sum(a.profit  ) profit,
       sum(a.sales_value) /sum( a.profit  )profit_rate
FROM csx_dw.dws_sale_r_d_detail a 
LEFT JOIN 
(select customer_no,
        first_category_name,
        second_category_name,
        third_category_name,
         contact_phone,
        contact_person,
        customer_address_full,
        customer_level_name,
    	customer_level_desc
from csx_dw.dws_crm_w_a_customer
where sdt='20220228')  b on a.customer_no=b.customer_no
WHERE sdt>='20220201'
    and sdt<='20220309'
  AND business_type_code not in('1','4')
  AND channel_code IN ('1',
                       '7',
                       '9')
GROUP BY region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name,
       a.customer_no,
       customer_name,
       b.first_category_name,
       b.second_category_name,
       b.third_category_name,
        contact_phone,
        contact_person,
        customer_address_full,
        customer_level_name,
    	customer_level_desc
    ) a where sales_value>=10000
    
    
    ;
    
    
SHOW create table csx_dw.report_sale_r_m_customer_level;
分级季度数据：csx_dw.report_sale_r_q_customer_level;


select * from csx_dw.dws_sale_r_d_detail where sdt>='20220215' and customer_no='106920'