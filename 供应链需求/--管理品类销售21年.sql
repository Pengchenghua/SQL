--管理品类销售21年
select
    substr(sdt,1,6) mon,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    business_type_name,
    classify_large_code,
    category_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(sales_qty) qty
 from csx_dw.dws_sale_r_d_detail 
 where sdt>='20210101' 
    and sdt<'20220101'
 group by 
    substr(sdt,1,6),
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    business_type_name,
    classify_large_code,
    category_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    ;