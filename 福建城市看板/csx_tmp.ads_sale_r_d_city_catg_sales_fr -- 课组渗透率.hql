

 
 
set hive.execution.engine=tez;
set tez.queue.name=caishixian;
set hive.exec.parallel=true; 
set hive.exec.parallel.thread.number=100;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set hive.exec.dynamic.partition.mode=nonstrict;
-- 当前日期
SET edate= '${enddate}';
-- 月初
SET sdate=trunc(${hiveconf:edate},'MM');
-- 上月初
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
-- 上月当前日期
SET l_edate=add_months(${hiveconf:edate},-1);


-- 课组销售
drop table if exists csx_tmp.temp_city_bd_sale;
create temporary table csx_tmp.temp_city_bd_sale as 
select
    zone_id,
    c.zone_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    channel_code,
    channel_name,
    case when division_code in ('10','11') then '11'
        when division_code in ('12','13','14') then '12'
        when division_code='15'then  '15'
        else division_code
    end bd_id,
    case when division_code in ('10','11') then '生鲜采购部'
        when division_code in ('12','13','14') then '食百采购部'
        when division_code='15'then  '易耗品采购部'
        else division_name
    end bd_name,
    a.division_code,
    a.division_name,
    department_code ,
    department_name,
    a.customer_no,
    a.goods_code,
    0 as daily_plan_sale,
    0 month_plan_sale,
    0 as month_plan_profit,
    sum(coalesce(daily_sale_value,0))as daily_sale_value,
    sum(coalesce(daily_profit,0)) as daily_profit,
    sum(coalesce(month_sale,0)) month_sale,
    sum(coalesce(last_month_sale,0)) as last_month_sale,
    sum(coalesce(month_profit,0)) month_profit
from
(
select
    j.channel_code,
    j.channel_name,
    province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.division_code,
    a.division_name,
    a.customer_no,
    a.goods_code,
    case when department_code like 'U%' then 'U01'when a.division_code ='14' then 'P01' else department_code end department_code ,
    case when department_code like 'U%' then '加工课'when a.division_code ='14' then '服装课' else department_name end department_name,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
    sum(sales_value) month_sale,
    sum(profit) month_profit,
    0 as last_month_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
    join 
    csx_tmp.ads_fr_channel_code j on a.channel=j.id
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
group by 
    j.channel_code,
    j.channel_name,
    province_code,
    a.province_name ,
    a.city_group_code,
    a.city_group_name,
    a.division_code,
    a.division_name,
    a.customer_no,
    a.goods_code,
    case when department_code like 'U%' then 'U01' when a.division_code ='14' then 'P01' else department_code end  ,  
    case when department_code like 'U%' then '加工课' when a.division_code ='14' then '服装课' else department_name end 
union all 
select
   j.channel_code,
    j.channel_name,
   province_code,
   a.province_name , 
    a.city_group_code,
    a.city_group_name,
    a.division_code,
    a.division_name,
    a.customer_no,
    a.goods_code,
    case when department_code like 'U%' then 'U01' when a.division_code ='14' then 'P01' else department_code end     department_code ,
    case when department_code like 'U%' then '加工课' when a.division_code ='14' then '服装课' else department_name end  department_name,
    0 as daily_sale_value,
    0 as daily_profit,
    0 month_sale,
    0 month_profit,
    sum(sales_value)as last_month_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
        join 
    csx_tmp.ads_fr_channel_code j on a.channel=j.id
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  channel in ('1','7')
group by 
    j.channel_code,
    j.channel_name,
    province_code,
    a.province_name ,
     a.city_group_code,
    a.city_group_name,
    a.division_code,
    a.division_name,
    a.customer_no,
    a.goods_code,
    case when department_code like 'U%' then 'U01'  when a.division_code ='14' then 'P01'  else department_code end    ,
    case when department_code like 'U%' then '加工课'when a.division_code ='14' then '服装课' else department_name end 
) a 
  join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
group by zone_id,
    c.zone_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.customer_no,
    a.goods_code,
    a.division_code,
    a.division_name,
    department_code ,
    department_name,
     channel_code,
    channel_name
    
    ;
 
-- 计算各城市客户数
drop table if exists  csx_tmp.temp_city_bd_sale_02;
create temporary table csx_tmp.temp_city_bd_sale_02 as 
select
     channel_code,
    channel_name,
    zone_id,
    zone_name,
    coalesce(a.province_code,'00')province_code,
    coalesce(a.city_group_code,'00')city_group_code,
    all_sale_cust_num
from (
select
    j.channel_code,
    j.channel_name,
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code,
    count(distinct a.customer_no )as all_sale_cust_num
from
    csx_dw.dws_sale_r_d_customer_sale a
        join 
    csx_tmp.ads_fr_channel_code j on a.channel=j.id
join 
(select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    
    group by 
     j.channel_code,
    j.channel_name,
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code
    grouping sets 
    ((zone_id,
    zone_name,
    a.province_code,
    a.city_group_code,
    j.channel_code,
    j.channel_name),
    (zone_id,
    zone_name,
    a.province_code,
    j.channel_code,
    j.channel_name),
    (zone_id,
    zone_name,j.channel_code,
    j.channel_name))
)a
 ;
 
-- select * from csx_tmp.temp_city_bd_sale_02  where zone_id='3';


-- 计算各城市客户数
drop table if exists  csx_tmp.temp_city_bd_sale_03;
create temporary table csx_tmp.temp_city_bd_sale_03 as 
select 
    case when department_name is not null and a.city_group_code is not null then '1'
        when department_name is  null and division_code is not null and bd_id is not null and a.city_group_code is not null then '2'
        when division_code is  null and bd_id is not null and a.city_group_code is not null then '3'
        when bd_id is  null and a.city_group_code is not null then '4'
        when a.city_group_code is  null and a.province_code is not null  then '5'
        when a.province_code is  null  then '6'
    else '7'
    end level_id,
    a.zone_id,
    a.zone_name,
    coalesce(a.province_code,'00')province_code,
    coalesce(province_name,'-')province_name,
    coalesce(a.city_group_code,'00')city_group_code,
    coalesce(city_group_name,'-')city_group_name,
    coalesce(channel_code,'00')channel_code,
    coalesce(channel_name,'-')channel,
    coalesce(bd_id,'00')bd_id,
    coalesce(bd_name,'全品类')  bd_name,
    coalesce(division_code,'00')division_code,
    coalesce(division_name,bd_name)  division_name,
    coalesce(department_code,'00')department_code,
    coalesce(department_name,'-')   department_name,
    daily_plan_sale,
    daily_sale_value,
    coalesce(daily_sale_value/abs(daily_plan_sale),0 ) daily_sale_fill_rate,
    daily_profit,
    coalesce(daily_profit/abs(daily_sale_value),0 )daily_profit_rate,
    month_plan_sale,
    month_sale,
    coalesce(month_sale/abs(month_plan_sale),0 )  month_sale_fill_rate,
    coalesce(last_month_sale,0)last_month_sale,
    coalesce((month_sale-last_month_sale)/last_month_sale,0)mom_sale_growth_rate,
    month_plan_profit,
    month_profit,
    coalesce(month_profit/month_plan_profit,0 )  month_profit_fill_rate,
    coalesce(month_profit/abs(month_sale),0 ) month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    -- coalesce(month_sale_cust_num/all_sale_cust_num,0) as cust_penetration_rate,  -- 渗透率
    -- all_sale_cust_num,
    row_number()over(partition by a.zone_id order by month_sale desc) as row_num,
    grouping__id
from 
(
select zone_id,
    a.zone_name,
    a.province_code,
    province_name,
    a.city_group_code,
    city_group_name,
    channel_code,
    channel_name,
    bd_id,
    bd_name,
    division_code,
    division_name,
    department_code,
    department_name,
    sum(daily_plan_sale) daily_plan_sale,
    sum(daily_sale_value) daily_sale_value,
    sum(daily_profit) daily_profit,
    sum(month_plan_sale) month_plan_sale,
    sum(month_sale) month_sale,
    sum(last_month_sale) last_month_sale,
    sum(month_plan_profit) month_plan_profit,
    sum(month_profit) month_profit,
    count(distinct case when month_sale>0 then  goods_code end ) month_sales_sku,
    count(distinct case when month_sale>0 then customer_no end ) month_sale_cust_num,
    grouping__id
from csx_tmp.temp_city_bd_sale a
group by zone_id,
    a.zone_name,
    a.province_code,
    province_name,
    a.city_group_code,
    city_group_name,
    channel_code,
    channel_name,
    bd_id,
    bd_name,
    division_code,
    division_name,
    department_code,
    department_name
grouping sets (
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            a.city_group_code,
            city_group_name,
            channel_code,
            channel_name,
            bd_id,
            bd_name,
            division_code,
            division_name,
            department_code,
            department_name
        ),
        --城市课组汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            a.city_group_code,
            city_group_name,
            channel_code,
            channel_name,
            bd_id,
            bd_name,
            division_code,
            division_name
        ),
        -- 城市部类汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            a.city_group_code,
            city_group_name,
            channel_code,
            channel_name,
            bd_id,
            bd_name
        ),
        -- 事业部汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            a.city_group_code,
            city_group_name,
            channel_code,
            channel_name
        ),
        -- 城市汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel_name,
            bd_id,
            bd_name,
            division_code,
            division_name,
            department_code,
            department_name
        ),
        --省区课组汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel_name,
            bd_id,
            bd_name,
            division_code,
            division_name
        ),
        --省类部类汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel_name,
            bd_id,
            bd_name
        ),
        --省区事业部汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel_name
        ),
        -- 省区汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel_name,
            bd_id,
            bd_name,
            division_code,
            division_name,
            department_code,
            department_name
        ),
        --战区课组汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel_name,
            division_code,
            division_name
        ),
        --战区部类汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel_name,
            bd_id,
            bd_name
        ),
        --战区采购部汇总
        (zone_id,
        a.zone_name,
        channel_code,
        channel_name) --战区汇总
    )
) a 
where 1=1
; 
-- select * from csx_tmp.temp_city_bd_sale_03 where province_code='23';
 -- 插入表
insert overwrite table `csx_tmp.ads_sale_r_d_city_catg_sales_fr` partition(months)
select 
    a.level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    a.zone_id,
    a.zone_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.channel_code,
    a.channel,
    case when division_code in ('10','11') then '11'
        when division_code in ('12','13','14') then '12'
        when division_code='15'then  '15'
        else bd_id
    end bd_id,
    case when division_code in ('10','11') then '生鲜采购部'
        when division_code in ('12','13','14') then '食百采购部'
        when division_code='15'then  '易耗品采购部'
        else bd_name
    end bd_name,
    division_code,
    coalesce(division_name,'-')division_name,
    a.department_code,
    coalesce(a.department_name,'-')department_name,
    daily_plan_sale,
    daily_sale_value,
    daily_sale_fill_rate,
    daily_profit,
    daily_profit_rate,
    month_plan_sale,
    month_sale,
    month_sale_fill_rate,
    last_month_sale,
    mom_sale_growth_rate,
    month_plan_profit,
    month_profit,
    month_profit_fill_rate,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    coalesce(month_sale_cust_num/all_sale_cust_num,0) as cust_penetration_rate,  -- 渗透率
    all_sale_cust_num,
    coalesce(c.row_num,0)row_num,
    current_timestamp(),
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from 
csx_tmp.temp_city_bd_sale_03 a 
left join
csx_tmp.temp_city_bd_sale_02 b on a.zone_id=b.zone_id and a.province_code=b.province_code and a.city_group_code=b.city_group_code and a.channel_code=b.channel_code
left join 
(select level_id,
      a.zone_id,
    a.zone_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.channel_code,
    a.channel,
    department_code,
    coalesce(department_name,'-')department_name,
    dense_rank()over(partition by a.zone_id,a.province_code,a.channel_code,a.city_group_name order by month_sale desc) row_num
    from csx_tmp.temp_city_bd_sale_03 a where a.department_code!='00' ) c 
on a.zone_id=c.zone_id and a.province_code=c.province_code and a.city_group_code=c.city_group_code and a.channel_code=c.channel_code and a.department_code=c.department_code and a.level_id=c.level_id
order by level_id desc ,zone_id,province_code,city_group_code,bd_id,division_code,department_code 
;
