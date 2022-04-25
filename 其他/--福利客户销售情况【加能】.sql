--福利客户销售情况【加能】

select  mom,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    customer_name,
    second_category_name,
    count(order_no ) sales_no,
    count(distinct sdt) sales_days,
    sum(sales_value) sales_value,
    sum(profit) profit
from (
select substr(sdt,1,6) mom,
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
    order_no,
    sum(sales_value)sales_value,
    sum(profit) profit
     
from csx_dw.dws_sale_r_d_detail where sdt>='20210901' 
    and sdt<'20211201' 
    and business_type_code='2' and channel_code='1'
group by  substr(sdt,1,6) ,
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
    order_no
    ) a 
    group by  mom,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    customer_name,
    second_category_name;