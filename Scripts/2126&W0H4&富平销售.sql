

-- 销售数据
 select 
    substr(sdt,1,4) as years,
    substr(sdt,1,6) as months,
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
    csx_dw.dws_sale_r_d_customer_sale 
where
    sdt >= '20190101'
    and 
    sdt <'20200701'
    and dc_code ='W0H4'
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
    substr(sdt,1,4),
    substr(sdt,1,6) ;

select * from csx_dw.csx_shop  where sdt='current' and company_code ='2126';
select * from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current';
