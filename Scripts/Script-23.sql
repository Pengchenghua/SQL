select from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd') as sdt,
 b.zone_id,
 b.zone_name,
 a.dist_code,
 dist_name,
 sum(plan_sale) plan_sale,
 sum(coalesce(sales_value,0))/10000 as sales_value,
 sum(coalesce(profit,0))/10000 as profit,
 coalesce(sum(coalesce(profit,0))/sum(coalesce(sales_value,0)),0) as profit_rate,
 coalesce(round(sum(sales_value)/10000/sum(plan_sale),4),0) as sale_fill_rate
from 
(select regexp_replace(to_date(sdt),'-','') as sdt,
       a.province_code  as dist_code,
        plan_sales_value plan_sale,
        0 sales_value,
        0 profit
from  csx_tmp.dws_csms_province_day_sale_plan_tmp a  
where month='202009'
and regexp_replace(to_date(sdt),'-','')>='20200901' 
union all 
select
    sdt,
    province_code as dist_code,
    0 plan_sale,
    sum(sales_value) sales_value ,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= '20200901' 
    and sdt<='20200930'
    and order_kind != 'WELFARE'
    and channel='1'
    and attribute_code in (1,2)
group by
    sdt,
    province_code) as a
join 
(select distinct
        province_code as  dist_code,
       province_name  as dist_name,
       region_code as  zone_id,
       region_name as  zone_name
    from
        csx_dw.dim_area  as b
    where
        area_rank = 13
        and region_code ='3'
)b
 on  a.dist_code=b.dist_code
group by 
from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd'),
 b.zone_id,
 b.zone_name,
 a.dist_code,
 dist_name
    order by from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd') desc,zone_id,case when dist_code='32' then 1 when dist_code='24' then 2 when dist_code='23' then 3 end
 ;

 refresh csx_tmp.ads_sale_r_d_zone_sales_fr;
 select * from  csx_tmp.ads_sale_r_d_zone_supervisor_fr  where months='202009' and zone_id ='3';
 
 
 select * from  csx_tmp.ads_sale_r_d_zone_cust_attribute_fr 
where  months='${mon}' 
    and zone_id='${zoneid}'
order by case when attribute_code=5 then 9 else attribute_code end asc ,province_code desc;


select *,regexp_replace(to_date(update_time),'-','') up_date from  csx_tmp.ads_sale_r_d_zone_sales_fr
where months='202009' 
    and zone_id='3'
order by province_code desc,
    case when channel_code='00' then '4' else channel_code end asc, level_id ;