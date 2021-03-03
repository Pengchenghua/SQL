
with tmp_date as 
(	select  calday 
	from csx_dw.dws_basic_w_a_date
	where calday >='20210101' 
	and calday<='20210131'
),
tmp_sale as 
(
select
    sdt,
     region_code as  zone_id,
    region_name as  zone_name,
    province_code as dist_code,
    province_name as dist_name,
    city_group_code,
    city_group_name,
    0 plan_sale,
    round(sum(sales_value),4) sales_value ,
    round(sum(profit),4) profit
from
    csx_dw.dws_sale_r_d_detail a
  where
     sdt >= '20210101' 
    and sdt<='202101311'
    and business_type_code in ('1')  
    and province_code='${dist}'
group by
    sdt,
    province_code,
    city_group_code,
    city_group_name,
    region_code ,
    province_name,
       region_name  
)
select date_str,
	sdt,
  dist_code,
  dist_name,
  city_group_code,
  case when dist_code='${dist}' and city_group_code !='00' then  city_group_name else city_group_name end  as city_group_name ,
  0 plan_sale,
  round(sales_value/10000,4) sales_value ,
  round(profit/10000,4) profit,
  coalesce(coalesce(profit,0)/coalesce(sales_value,0),0) as profit_rate,
  coalesce(round(sales_value/10000/plan_sale,4),0) as sale_fill_rate
from tmp_sale  a 
left join tmp_date b  on regexp_replace(to_date(date_str),'-','')=a.sdt 
order by date_str desc ,
case when dist_code='32' then 1 when dist_code='24' then 2 when dist_code='23' then 3 else cast(dist_code as int) end ,
case when  city_group_code in ('5','12','6','8') then 10 
when city_group_code in  ('4','23','7','9') then 9 
when city_group_code  in ('1','22','20') then 8
when city_group_code  in ('2','13') then 7
when city_group_code  in ('3','21') then 6
when city_group_code  ='3' then 6
when city_group_code  ='25' then 4
when city_group_code  ='24' then 3
end desc
;





select from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd') as sdt,
 b.zone_id,
 b.zone_name,
 a.dist_code,
 dist_name,
 city_group_code ,
 city_group_name ,
 sum(plan_sale) plan_sale,
 sum(coalesce(sales_value,0))/10000 as sales_value,
 sum(coalesce(profit,0))/10000 as profit,
 coalesce(sum(coalesce(profit,0))/sum(coalesce(sales_value,0)),0) as profit_rate,
 coalesce(round(sum(sales_value)/10000/sum(plan_sale),4),0) as sale_fill_rate
from 
(select regexp_replace(to_date(sdt),'-','') as sdt,
       a.province_code  as dist_code,        
        city_group_code ,
        case when city_group_name like '攀枝%' then '攀枝花市' else city_group_name end city_group_name,
        sum(plan_sales_value) plan_sale,
        0 sales_value,
        0 profit
from  csx_tmp.dws_csms_province_day_sale_plan_tmp a  
where month='${mon}'
	and   regexp_replace(to_date(sdt),'-','') >= '${CONCATENATE(mon,"01")}'
	group by 
	sdt,
       a.province_code  ,        
        city_group_code ,
        city_group_name
union all 
select
    sdt,
    province_code as dist_code,
    city_group_code ,
    city_group_name ,
    0 plan_sale,
    sum(sales_value) sales_value ,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_detail a
where
    sdt >= '${CONCATENATE(mon,"01")}' 
    and sdt<=regexp_replace(to_date('${dateinmonth(format(concatenate(mon,"01"),"yyyy-MM-dd"),-1)}'),'-','')
    and business_type_code ='1'
group by
    sdt,
    province_code,
     city_group_code ,
    city_group_name ) as a
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
        and province_code ='${dist}'
)b
 on  a.dist_code=b.dist_code
group by 
from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd'),
 b.zone_id,
 b.zone_name,
 a.dist_code,
 dist_name,
 city_group_code ,
 city_group_name 
    order by from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd') desc,zone_id,case when dist_code='32' then 1 when dist_code='24' then 2 when dist_code='23' then 3 else cast(dist_code as int) end
 ;