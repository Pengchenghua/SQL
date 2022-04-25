select  
    case when province_name like '福建%' then  1
        when province_name like '广东%' then  2
        when province_name like '重庆%' then  3
        when province_name like '四川%' then  4
        when province_name like '贵州%' then  5
        when province_name like '北京%' then  6
        when province_name like '河北%' then  7
        when province_name like '陕西%' then  8
        when province_name like '河南%' then  9
        when province_name like '安徽%' then  10
        when province_name like '上海%' then  11
        when province_name like '昆山%' then  12
        when province_name like '南京%' then  14
        when province_name like '浙江%' then  14
        else 15 end as aa,
    province_code,
    province_name,
    business_type_code,
    business_type_name,
    Q1_sale,
    Q2_sale,
    Q2_sale/Q1_sale -1 as sales_rate
from 
(select province_code,
    case when city_group_name like '%苏州%' then '昆山'
        when city_group_name like '%南京%' then '南京'
        else province_name end province_name,
    business_type_code,
    business_type_name,
    sum(case when sdt>='20210101' and sdt<'20210401' then sales_value end ) as Q1_sale,
    sum(case when sdt>='20210401' and sdt<'20210701' then sales_value end ) as Q2_sale
from csx_dw.dws_sale_r_d_detail where sdt>='20210101' and sdt<'20210701'
group by 
     province_code,
    province_name,
    business_type_code,
    business_type_name
    ) a 
 order by 
 case when province_name like '福建%' then  1
when province_name like '广东%' then  2
when province_name like '重庆%' then  3
when province_name like '四川%' then  4
when province_name like '贵州%' then  5
when province_name like '北京%' then  6
when province_name like '河北%' then  7
when province_name like '陕西%' then  8
when province_name like '河南%' then  9
when province_name like '安徽%' then  10
when province_name like '上海%' then  11
when province_name like '昆山%' then  12
when province_name like '南京%' then  14
when province_name like '浙江%' then  14
else 15 end 
    ;
    
    
    
    select  
    case when province_name like '福建%' then  1
        when province_name like '广东%' then  2
        when province_name like '重庆%' then  3
        when province_name like '四川%' then  4
        when province_name like '贵州%' then  5
        when province_name like '北京%' then  6
        when province_name like '河北%' then  7
        when province_name like '陕西%' then  8
        when province_name like '河南%' then  9
        when province_name like '安徽%' then  10
        when province_name like '上海%' then  11
        when province_name like '昆山%' then  12
        when province_name like '南京%' then  14
        when province_name like '浙江%' then  14
        else 15 end as aa,
    province_code,
    province_name,
    business_type_code,
    business_type_name,
    Q1_sale,
    Q2_sale,
    Q2_sale+Q1_sale
    Q2_sale/Q1_sale -1 as sales_rate
from 
(select province_code,
    case when city_group_name like '%苏州%' then '昆山'
        when city_group_name like '%南京%' then '南京'
        else province_name end province_name,
    business_type_code,
    business_type_name,
    sum(case when sdt>='20210101' and sdt<'20210401' then sales_value end ) as Q1_sale,
    sum(case when sdt>='20210401' and sdt<'20210701' then sales_value end ) as Q2_sale
from csx_dw.dws_sale_r_d_detail where sdt>='20210101' and sdt<'20210701'
group by 
     province_code,
    province_name,
    business_type_code,
    business_type_name
    ) a 
 order by 
 case when province_name like '福建%' then  1
when province_name like '广东%' then  2
when province_name like '重庆%' then  3
when province_name like '四川%' then  4
when province_name like '贵州%' then  5
when province_name like '北京%' then  6
when province_name like '河北%' then  7
when province_name like '陕西%' then  8
when province_name like '河南%' then  9
when province_name like '安徽%' then  10
when province_name like '上海%' then  11
when province_name like '昆山%' then  12
when province_name like '南京%' then  14
when province_name like '浙江%' then  14
else 15 end 
    ;