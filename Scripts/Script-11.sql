select 
    substr(month,1,4) as years,
    channel_name ,
    province_code ,
    province_name ,
    customer_no,
    customer_name,
    `attribute` ,
    first_category ,
    second_category ,
    third_category ,
    sales_name ,
    work_no ,
    division_code ,
    division_name ,
    department_code ,
    department_name ,
    sum(sales_value )sales_value ,
    sum(sales_cost )sales_cost ,
    sum(profit )profit,
    sum(excluding_tax_sales)as no_tax_sales,
    sum(excluding_tax_cost ) as no_tax_cost,
    sum(excluding_tax_profit ) as no_tax_profit
from
    csx_dw.ads_sale_r_m_customer_goods_sale
where
    month >= '202001' and month<='20208'
group by 
channel_name ,
    province_code ,
    province_name ,
    customer_no,
    customer_name,
    `attribute` ,
    first_category ,
    second_category ,
    third_category ,
    sales_name ,
    work_no ,
    division_code ,
    division_name ,
    department_code ,
    department_name,
    substr(month,1,4) ;