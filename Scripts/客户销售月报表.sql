


-- 销售月报表
 select
    customer_no ,
    customer_name ,
    attribute_code,
    `attribute` ,
    first_category_code,
    first_category,
    second_category_code,
    second_category,
    third_category_code,
    third_category,
    sales_province_code,
    sales_province,
    sales_name,
    sales_phone,
    supervisor_id,
    supervisor_work_no,
    supervisor_name,
    city_manager_id,
    city_manager_work_no,
    city_manager_name,
    item_province_manager_id,
    item_province_manager_work_no,
    item_province_manager_name,
    org_code,
    org_name,
    is_copemate_order,
    channel,
    channel_name,
    province_code,
    province_name,
    city_code,
    city_name,
    city_group_code,
    city_group_name,
    city_real,
    cityjob,
    province_manager_id,
    province_manager_work_no,
    province_manager_name,
    COUNT(DISTINCT goods_code ) as sales_sku,
    count(DISTINCT sdt) as sales_days,
    sum(sales_value )as sales_value,
    sum(sales_qty )as sales_qty,
    sum(profit )as profit ,
    sum(front_profit ) as front_profit ,
    sum(profit )/sum(sales_value ) as profit_rate
   case when return_flag='X'then  sum(sales_value ) as return_amt    
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt >= '20200501';
    select * from csx_dw.dws_basic_w_a_category_m where sdt='current';
    select * from csx_dw.customer_sales;
    

select * from csx_dw.ads_sale_customer_division_level_sales_months;