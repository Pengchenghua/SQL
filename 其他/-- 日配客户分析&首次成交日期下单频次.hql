-- 日配分析&首次成交日期下单频次
with temp as 
(select substr(sdt,1,6) mon,
    sdt,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    customer_name,
    second_category_name,
    first_category_name,
    third_category_name,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(front_profit) front_profit
from csx_dw.dws_sale_r_d_detail 
where sdt>='20210801' and sdt<='20211031'
and business_type_code='1' 
and channel_code='1'
and dc_code not in ('W0Z7','W0K4')
group by sdt,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    customer_name,
    second_category_name,
    first_category_name,
    third_category_name,
    substr(sdt,1,6)
    )
select  mon,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.customer_no,
    customer_name,
    sales_name,
    first_supervisor_name,
    second_category_name,
    first_category_name,
    third_category_name,first_sign_date,sign_date,first_order_date,last_order_date,active_days,
    count(case when sales_value>0 then sdt end ) sales_days,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(profit)/ sum(sales_value)  as profit_rate,
    sum(front_profit) front_profit,
    sum(front_profit)/ sum(sales_value) as front_profit_rate
from temp a 
left join 
(select customer_no,
    sales_name,
    first_supervisor_name
    from csx_dw.dws_crm_w_a_customer where sdt='current') b on a.customer_no=b.customer_no
left join 
(select customer_no,first_sign_date,sign_date,first_order_date,last_order_date,active_days 
from  csx_dw.dws_crm_w_a_customer_business_active where sdt='20211116' and business_type_code='1')  c on a.customer_no=c.customer_no
group by mon,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.customer_no,
    customer_name,
    second_category_name,
    first_category_name,
    sales_name,
    first_supervisor_name,
    third_category_name,
    first_sign_date,sign_date,first_order_date,last_order_date,active_days