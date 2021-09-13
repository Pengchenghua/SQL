
-- t销售业务BBC、福利
select business_type_code,business_type_name,
province_code,
province_name,
city_group_code,
city_group_name,
sum(sales_value)sales_value,
sum(profit) profit
from csx_dw.dws_sale_r_d_detail where sdt>='20210901' and sdt<='20210912'
and channel_code in ('1','7') 
and business_type_code in ('2','6')
group by business_type_code,business_type_name,
province_code,
province_name,
city_group_code,
city_group_name
 ;
 