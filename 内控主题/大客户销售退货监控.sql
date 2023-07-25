-- 帆软 大销售退货监控
select 
province_code,
province_name,
city_group_code,
city_group_name,
sum(sales_order_cnt) as sales_order_cnt,
sum(sales_value) as sales_value,
sum(profit) as profit,
sum(return_order_cnt)as return_order_cnt,
sum(return_sales_value*-1)as return_sales_value,
sum(return_sales_value)/sum(sales_value)  return_rate,
sum(last_return_order_cnt)as last_return_order_cnt,
sum(last_return_sales_value*-1)as last_return_sales_value ,
sum(last_return_order_cnt)/sum(sales_order_cnt)  last_return_order_rate,
sum(last_return_sales_value)/sum(sales_value) as last_return_sale_rate,
sum(last_return_order_cnt)/sum(return_order_cnt) as last_return_cnt_rate,
sum(last_return_sales_value)/sum(return_sales_value) as last_return_rate
from
(
    -- 本月销售额
select 
province_code,
province_name,
city_group_code,
city_group_name,
count(distinct order_no) as sales_order_cnt,
sum(sales_value) as sales_value,
sum(profit) as profit,
0 return_order_cnt,
0 return_sales_value,
0  last_return_order_cnt,
0 last_return_sales_value 
from csx_dw.dws_sale_r_d_detail 
where sdt>='${sdate}' 
    and sdt<='${edate}' 
-- and business_type_code in ('1','2')
    and channel_code in ('1','7','9')
     ${if(len(sale_type)==0,""," and business_type_code in ('"+sale_type+"')" )}
 --   and sales_type!='fanli'
 ${if(len(mode)==0,"","and logistics_mode_code in ('"+mode+"')" )}
group by 
province_code,
province_name,
city_group_code,
city_group_name
union all 
-- 本月退货额
select 
province_code,
province_name,
city_group_code,
city_group_name,
0 as sales_order_cnt,
0 as sales_value,
0 as profit,
count(distinct order_no  ) as return_order_cnt,
abs(sum( sales_value  )) as return_sales_value,
0  last_return_order_cnt,
0 last_return_sales_value 
from csx_dw.dws_sale_r_d_detail 
where sdt>='${sdate}' 
    and sdt<='${edate}' 
    and channel_code in ('1','7','9')
 --   and business_type_code in ('1','2')
     ${if(len(sale_type)==0,""," and business_type_code in ('"+sale_type+"')" )}
    and return_flag='X'
     ${if(len(mode)==0,"","and logistics_mode_code in ('"+mode+"')" )}
  -- and sales_type!='fanli'
group by 
province_code,
province_name,
city_group_code,
city_group_name
union all 
-- 9月份销售10月退货
select province_code,
    province_name,
    city_group_code,
    city_group_name,
    0 as sales_order_cnt,
    0 as sales_value,
    0 as profit,
    0 as return_order_cnt,
    0 as return_sales_value,
    count(a.order_no) as last_return_order_cnt,
    sum(sales_value)    last_return_sales_value 
from 
(-- 本月退货
    select province_code,
    province_name,
    city_group_code,
    city_group_name,
    order_no,
    origin_order_no,
    abs(sum(sales_value)) return_sales_value 
from csx_dw.dws_sale_r_d_detail 
where sdt>='${sdate}' 
    and sdt<='${edate}'  
   -- and business_type_code in ('1','2')
     and channel_code in ('1','7','9')
    and return_flag='X'
     ${if(len(mode)==0,"","and logistics_mode_code in ('"+mode+"')" )}
     ${if(len(sale_type)==0,""," and business_type_code in ('"+sale_type+"')" )}
group by province_code,
    province_name,
    city_group_code,
    city_group_name,
    order_no,
    origin_order_no
)a 
join 
 
(-- 上月有销售
  select order_no,
    origin_order_no,
    abs(sum(sales_value)) sales_value 
from csx_dw.dws_sale_r_d_detail 
where sdt>='${rsdate}' 
    and sdt<='${redate}' 
--    and business_type_code in ('1','2')
  and channel_code in ('1','7','9')
      ${if(len(sale_type)==0,""," and business_type_code in ('"+sale_type+"')" )}
    and return_flag!='X'
     ${if(len(mode)==0,"","and logistics_mode_code in ('"+mode+"')" )}
    group by order_no,
    origin_order_no
)b on a.origin_order_no=b.order_no and a.return_sales_value=b.sales_value
group by province_code,
    province_name,
    city_group_code,
    city_group_name
) a 
group by province_code,
province_name,
city_group_code,
city_group_name
order by 
province_code,
province_name,
city_group_code,
city_group_name