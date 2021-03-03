
-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
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
     '1' channel_code,
    '大客户'as channel,
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
    csx_dw.dws_sale_r_d_detail a
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    csx_dw.dws_sale_r_d_detail a 
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    department_name
    
    ;
 
-- 计算各城市客户数
drop table if exists  csx_tmp.temp_city_bd_sale_02;
create temporary table csx_tmp.temp_city_bd_sale_02 as 
select
    zone_id,
    zone_name,
    coalesce(a.province_code,'00')province_code,
    coalesce(a.city_group_code,'00')city_group_code,
    all_sale_cust_num
from (
select
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code,
    count(distinct a.customer_no )as all_sale_cust_num
from
    csx_dw.dws_sale_r_d_detail a
join 
(select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
    group by 
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code
    grouping sets 
    ((zone_id,
    zone_name,
    a.province_code,
    a.city_group_code),
    (zone_id,
    zone_name,
    a.province_code),
    (zone_id,
    zone_name))
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
    coalesce(channel,'-')channel,
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
    -- row_number()over(partition by a.zone_id order by month_sale desc) as row_num,
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
    channel,
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
    channel,
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
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 城市汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 省区汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
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
            channel,
            division_code,
            division_name
        ),
        --战区部类汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
            bd_id,
            bd_name
        ),
        --战区采购部汇总
        (zone_id,
        a.zone_name,
        channel_code,
        channel) --战区汇总
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
    coalesce(department_name,'-')department_name,
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
(select 
    level_id,
    a.province_code,
    a.city_group_code,
    a.channel_code,
    department_code,
    row_number()over(partition by a.province_code,level_id,city_group_code,channel_code order by month_sale desc) as row_num 
    from csx_tmp.temp_city_bd_sale_03 a where department_code!='00') c on a.level_id=c.level_id and a.province_code=c.province_code and a.city_group_code =c.city_group_code and a.channel_code=c.channel_code and a.department_code=c.department_code
left join
csx_tmp.temp_city_bd_sale_02 b on a.zone_id=b.zone_id and a.province_code=b.province_code and a.city_group_code=b.city_group_code
order by level_id desc ,zone_id,province_code,city_group_code,bd_id,division_code,department_code
;

    

-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
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
     '1' channel_code,
    '大客户'as channel,
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
    csx_dw.dws_sale_r_d_detail a
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    csx_dw.dws_sale_r_d_detail a 
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    department_name
    
    ;
 
-- 计算各城市客户数
drop table if exists  csx_tmp.temp_city_bd_sale_02;
create temporary table csx_tmp.temp_city_bd_sale_02 as 
select
    zone_id,
    zone_name,
    coalesce(a.province_code,'00')province_code,
    coalesce(a.city_group_code,'00')city_group_code,
    all_sale_cust_num
from (
select
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code,
    count(distinct a.customer_no )as all_sale_cust_num
from
    csx_dw.dws_sale_r_d_detail a
join 
(select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
    group by 
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code
    grouping sets 
    ((zone_id,
    zone_name,
    a.province_code,
    a.city_group_code),
    (zone_id,
    zone_name,
    a.province_code),
    (zone_id,
    zone_name))
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
    coalesce(channel,'-')channel,
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
    -- row_number()over(partition by a.zone_id order by month_sale desc) as row_num,
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
    channel,
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
    channel,
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
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 城市汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 省区汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
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
            channel,
            division_code,
            division_name
        ),
        --战区部类汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
            bd_id,
            bd_name
        ),
        --战区采购部汇总
        (zone_id,
        a.zone_name,
        channel_code,
        channel) --战区汇总
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
    coalesce(department_name,'-')department_name,
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
(select 
    level_id,
    a.province_code,
    a.city_group_code,
    a.channel_code,
    department_code,
    row_number()over(partition by a.province_code,level_id,city_group_code,channel_code order by month_sale desc) as row_num 
    from csx_tmp.temp_city_bd_sale_03 a where department_code!='00') c on a.level_id=c.level_id and a.province_code=c.province_code and a.city_group_code =c.city_group_code and a.channel_code=c.channel_code and a.department_code=c.department_code
left join
csx_tmp.temp_city_bd_sale_02 b on a.zone_id=b.zone_id and a.province_code=b.province_code and a.city_group_code=b.city_group_code
order by level_id desc ,zone_id,province_code,city_group_code,bd_id,division_code,department_code
;

    

-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
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
     '1' channel_code,
    '大客户'as channel,
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
    csx_dw.dws_sale_r_d_detail a
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    csx_dw.dws_sale_r_d_detail a 
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    department_name
    
    ;
 
-- 计算各城市客户数
drop table if exists  csx_tmp.temp_city_bd_sale_02;
create temporary table csx_tmp.temp_city_bd_sale_02 as 
select
    zone_id,
    zone_name,
    coalesce(a.province_code,'00')province_code,
    coalesce(a.city_group_code,'00')city_group_code,
    all_sale_cust_num
from (
select
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code,
    count(distinct a.customer_no )as all_sale_cust_num
from
    csx_dw.dws_sale_r_d_detail a
join 
(select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
    group by 
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code
    grouping sets 
    ((zone_id,
    zone_name,
    a.province_code,
    a.city_group_code),
    (zone_id,
    zone_name,
    a.province_code),
    (zone_id,
    zone_name))
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
    coalesce(channel,'-')channel,
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
    -- row_number()over(partition by a.zone_id order by month_sale desc) as row_num,
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
    channel,
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
    channel,
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
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 城市汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 省区汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
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
            channel,
            division_code,
            division_name
        ),
        --战区部类汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
            bd_id,
            bd_name
        ),
        --战区采购部汇总
        (zone_id,
        a.zone_name,
        channel_code,
        channel) --战区汇总
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
    coalesce(department_name,'-')department_name,
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
(select 
    level_id,
    a.province_code,
    a.city_group_code,
    a.channel_code,
    department_code,
    row_number()over(partition by a.province_code,level_id,city_group_code,channel_code order by month_sale desc) as row_num 
    from csx_tmp.temp_city_bd_sale_03 a where department_code!='00') c on a.level_id=c.level_id and a.province_code=c.province_code and a.city_group_code =c.city_group_code and a.channel_code=c.channel_code and a.department_code=c.department_code
left join
csx_tmp.temp_city_bd_sale_02 b on a.zone_id=b.zone_id and a.province_code=b.province_code and a.city_group_code=b.city_group_code
order by level_id desc ,zone_id,province_code,city_group_code,bd_id,division_code,department_code
;

    

-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
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
     '1' channel_code,
    '大客户'as channel,
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
    csx_dw.dws_sale_r_d_detail a
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    csx_dw.dws_sale_r_d_detail a 
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    department_name
    
    ;
 
-- 计算各城市客户数
drop table if exists  csx_tmp.temp_city_bd_sale_02;
create temporary table csx_tmp.temp_city_bd_sale_02 as 
select
    zone_id,
    zone_name,
    coalesce(a.province_code,'00')province_code,
    coalesce(a.city_group_code,'00')city_group_code,
    all_sale_cust_num
from (
select
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code,
    count(distinct a.customer_no )as all_sale_cust_num
from
    csx_dw.dws_sale_r_d_detail a
join 
(select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
    group by 
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code
    grouping sets 
    ((zone_id,
    zone_name,
    a.province_code,
    a.city_group_code),
    (zone_id,
    zone_name,
    a.province_code),
    (zone_id,
    zone_name))
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
    coalesce(channel,'-')channel,
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
    -- row_number()over(partition by a.zone_id order by month_sale desc) as row_num,
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
    channel,
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
    channel,
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
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 城市汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 省区汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
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
            channel,
            division_code,
            division_name
        ),
        --战区部类汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
            bd_id,
            bd_name
        ),
        --战区采购部汇总
        (zone_id,
        a.zone_name,
        channel_code,
        channel) --战区汇总
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
    coalesce(department_name,'-')department_name,
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
(select 
    level_id,
    a.province_code,
    a.city_group_code,
    a.channel_code,
    department_code,
    row_number()over(partition by a.province_code,level_id,city_group_code,channel_code order by month_sale desc) as row_num 
    from csx_tmp.temp_city_bd_sale_03 a where department_code!='00') c on a.level_id=c.level_id and a.province_code=c.province_code and a.city_group_code =c.city_group_code and a.channel_code=c.channel_code and a.department_code=c.department_code
left join
csx_tmp.temp_city_bd_sale_02 b on a.zone_id=b.zone_id and a.province_code=b.province_code and a.city_group_code=b.city_group_code
order by level_id desc ,zone_id,province_code,city_group_code,bd_id,division_code,department_code
;

    

-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
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
     '1' channel_code,
    '大客户'as channel,
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
    csx_dw.dws_sale_r_d_detail a
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    csx_dw.dws_sale_r_d_detail a 
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    department_name
    
    ;
 
-- 计算各城市客户数
drop table if exists  csx_tmp.temp_city_bd_sale_02;
create temporary table csx_tmp.temp_city_bd_sale_02 as 
select
    zone_id,
    zone_name,
    coalesce(a.province_code,'00')province_code,
    coalesce(a.city_group_code,'00')city_group_code,
    all_sale_cust_num
from (
select
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code,
    count(distinct a.customer_no )as all_sale_cust_num
from
    csx_dw.dws_sale_r_d_detail a
join 
(select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
    group by 
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code
    grouping sets 
    ((zone_id,
    zone_name,
    a.province_code,
    a.city_group_code),
    (zone_id,
    zone_name,
    a.province_code),
    (zone_id,
    zone_name))
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
    coalesce(channel,'-')channel,
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
    -- row_number()over(partition by a.zone_id order by month_sale desc) as row_num,
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
    channel,
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
    channel,
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
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 城市汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 省区汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
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
            channel,
            division_code,
            division_name
        ),
        --战区部类汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
            bd_id,
            bd_name
        ),
        --战区采购部汇总
        (zone_id,
        a.zone_name,
        channel_code,
        channel) --战区汇总
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
    coalesce(department_name,'-')department_name,
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
(select 
    level_id,
    a.province_code,
    a.city_group_code,
    a.channel_code,
    department_code,
    row_number()over(partition by a.province_code,level_id,city_group_code,channel_code order by month_sale desc) as row_num 
    from csx_tmp.temp_city_bd_sale_03 a where department_code!='00') c on a.level_id=c.level_id and a.province_code=c.province_code and a.city_group_code =c.city_group_code and a.channel_code=c.channel_code and a.department_code=c.department_code
left join
csx_tmp.temp_city_bd_sale_02 b on a.zone_id=b.zone_id and a.province_code=b.province_code and a.city_group_code=b.city_group_code
order by level_id desc ,zone_id,province_code,city_group_code,bd_id,division_code,department_code
;

    

-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
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
     '1' channel_code,
    '大客户'as channel,
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
    csx_dw.dws_sale_r_d_detail a
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    csx_dw.dws_sale_r_d_detail a 
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  a.channel_code in ('1','7')
group by 
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
    department_name
    
    ;
 
-- 计算各城市客户数
drop table if exists  csx_tmp.temp_city_bd_sale_02;
create temporary table csx_tmp.temp_city_bd_sale_02 as 
select
    zone_id,
    zone_name,
    coalesce(a.province_code,'00')province_code,
    coalesce(a.city_group_code,'00')city_group_code,
    all_sale_cust_num
from (
select
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code,
    count(distinct a.customer_no )as all_sale_cust_num
from
    csx_dw.dws_sale_r_d_detail a
join 
(select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  a.channel_code in ('1','7')
    group by 
    zone_id,
    zone_name,
    a.province_code,
    a.city_group_code
    grouping sets 
    ((zone_id,
    zone_name,
    a.province_code,
    a.city_group_code),
    (zone_id,
    zone_name,
    a.province_code),
    (zone_id,
    zone_name))
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
    coalesce(channel,'-')channel,
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
    -- row_number()over(partition by a.zone_id order by month_sale desc) as row_num,
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
    channel,
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
    channel,
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
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 城市汇总
        (
            zone_id,
            a.zone_name,
            a.province_code,
            province_name,
            channel_code,
            channel,
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
            channel,
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
            channel,
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
            channel
        ),
        -- 省区汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
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
            channel,
            division_code,
            division_name
        ),
        --战区部类汇总
        (
            zone_id,
            a.zone_name,
            channel_code,
            channel,
            bd_id,
            bd_name
        ),
        --战区采购部汇总
        (zone_id,
        a.zone_name,
        channel_code,
        channel) --战区汇总
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
    coalesce(department_name,'-')department_name,
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
(select 
    level_id,
    a.province_code,
    a.city_group_code,
    a.channel_code,
    department_code,
    row_number()over(partition by a.province_code,level_id,city_group_code,channel_code order by month_sale desc) as row_num 
    from csx_tmp.temp_city_bd_sale_03 a where department_code!='00') c on a.level_id=c.level_id and a.province_code=c.province_code and a.city_group_code =c.city_group_code and a.channel_code=c.channel_code and a.department_code=c.department_code
left join
csx_tmp.temp_city_bd_sale_02 b on a.zone_id=b.zone_id and a.province_code=b.province_code and a.city_group_code=b.city_group_code
order by level_id desc ,zone_id,province_code,city_group_code,bd_id,division_code,department_code
;


-- 城市组销售

set hive.exec.dynamic.partition.mode=nonstrict;
-- 当前日期
SET edate= '${enddate}';
-- 月初
SET sdate=trunc(${hiveconf:edate},'MM');
-- 上月初
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
-- 上月当前日期
SET l_edate=add_months(${hiveconf:edate},-1);

-- 昨日\月销售数据

DROP TABLE IF EXISTS csx_tmp.temp_war_city_sale;


CREATE TEMPORARY TABLE csx_tmp.temp_war_city_sale AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    yesterday_sales_value, 
   -- last_day_sales,
    yesterday_profit,
   -- yesterday_negative_profit,
    yesterday_new_customer_sale,
    yesterday_often_customer_sale,
    yesterday_customer_num, 
    -- 月累计
    months_sales_value, 
    months_profit,
   -- negative_profit,
    months_new_customer_sale,
    months_often_customer_sale,
    months_customer_num
FROM 
(
SELECT CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       a.province_code,
       a.province_name,
       a.city_group_code,
       a.city_group_name,
       sum(CASE  WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value  END)AS yesterday_sales_value, 
     --  sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') and profit <0 then profit end ) as yesterday_negative_profit,
    -- sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') then sales_value end ) as last_day_sales,
    sum(CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN profit  END)AS yesterday_profit,
    sum(CASE WHEN substr(sdt,1,6)=first_sale_mon  AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value     END) AS yesterday_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value END) AS yesterday_often_customer_sale,
    count(DISTINCT CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN a.customer_no END) AS yesterday_customer_num, 
    -- 月累计
    sum(sales_value)AS months_sales_value, 
    sum(profit)AS months_profit,
    -- sum(case when profit <0 then profit end ) as negative_profit,
    sum(CASE WHEN substr(sdt,1,6) =first_sale_mon THEN sales_value  END) AS months_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon THEN sales_value END) AS months_often_customer_sale,
    count(DISTINCT a.customer_no) AS months_customer_num
FROM csx_dw.dws_sale_r_d_detail a
left JOIN
  (SELECT customer_no,
          substr(first_order_date,1,6) AS first_sale_mon
   FROM csx_dw.dws_crm_r_a_customer_active_info
   WHERE sdt=regexp_replace(${hiveconf:edate},'-','')) b ON a.customer_no=b.customer_no
WHERE sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
GROUP BY province_code,
         province_name,
         a.city_group_code,
         a.city_group_name,
         CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END
)a 

;

-- 关联上周环比数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_01;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_01 AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    yesterday_sales_value, 
    last_day_sales,
    yesterday_profit,
   -- yesterday_negative_profit,
    yesterday_new_customer_sale,
    yesterday_often_customer_sale,
    yesterday_customer_num, 
    -- 月累计
    months_sales_value, 
    months_profit,
   -- negative_profit,
    months_new_customer_sale,
    months_often_customer_sale,
    months_customer_num
FROM 
csx_tmp.temp_war_city_sale  a 
left join 
(select CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       province_code,
        a.city_group_code,
        a.city_group_name,
       sum(a.sales_value)as last_day_sales
    from csx_dw.dws_sale_r_d_detail a
    where sdt=regexp_replace(date_sub(${hiveconf:edate},7),'-','')
    group by CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END ,
       a.city_group_code,
       a.city_group_name,
       province_code
     ) as c on a.province_code=c.province_code and a.channel_name=c.channel_name and a.city_group_code=c.city_group_code;

-- select regexp_replace(date_sub(${hiveconf:edate},7),'-','');
-- show create table csx_dw.ads_sale_w_d_ads_customer_sales_q;
-- 上月环比数据

DROP TABLE IF EXISTS csx_tmp.temp_ring_war_zone_sale;
CREATE TEMPORARY TABLE csx_tmp.temp_ring_war_zone_sale AS
SELECT CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END channel_name,
       province_code,
       province_name,
    a.city_group_code,
    a.city_group_name,
       sum(CASE
               WHEN sdt=regexp_replace(${hiveconf:l_edate},'-','') THEN sales_value
           END)AS last_yesterday_sales_value,
       sum(sales_value)AS last_months_sales_value
FROM csx_dw.dws_sale_r_d_detail a
WHERE sdt>=regexp_replace(${hiveconf:l_sdate},'-', '')
  AND sdt<=regexp_replace(${hiveconf:l_edate},'-','')
GROUP BY province_code,
         province_name,
         a.city_group_code,
    a.city_group_name,
         CASE
           WHEN a.channel_code IN ('1','7','9') THEN '大客户'
 		   when channel_code in ('5','6') and a.customer_no like 'S%' then '商超' 
		   when channel_code in ('5','6') and a.customer_no not like 'S%' then '大客户' 
           ELSE a.channel_name
       END ;


-- 负毛利

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_02;


CREATE TEMPORARY TABLE csx_tmp.temp_war_zone_sale_02 AS

select
    '大客户' as channel_name,
    province_code ,
    province_name  ,
    a.city_group_code,
    a.city_group_name,
    count(distinct  goods_code )as sale_sku,
    sum(sale/10000)sale,
    sum(profit/10000 )profit,
    sum(profit) /sum(sale) as profit_rate,
    sum(case when profit<0 and sdt=regexp_replace(${hiveconf:edate},'-','') then profit end ) as negative_days_profit,
    sum(case when profit<0 then profit end ) as negative_profit
 from (
select
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    sdt,
    a.customer_no,
    goods_code ,
    goods_name,
    division_code ,division_name ,
    avg(cost_price )avg_cost,
    avg(sales_price )avg_sale,
    sum(sales_qty )qty,
    sum(sales_value) sale,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_detail a 
where
    sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    and channel_code in ('1','7','9')
group by 
   province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    sdt,
    a.customer_no,
    goods_code ,
    goods_name,
    division_code ,division_name 
   ) a 
group by 
   province_code ,
   province_name ,
   a.city_group_code,
   a.city_group_name;
 
-- 计划表统计  
drop table if exists csx_tmp.temp_plan_sale;
create temporary table csx_tmp.temp_plan_sale
as 
select trim(province_code)province_code,
    channel_name,
    sum( daily_plan_sales_value)daily_plan_sales_value,
    sum( daily_plan_profit)  daily_plan_profit,
    sum(plan_sales_value)plan_sales_value ,
    sum(plan_profit)plan_profit 
   from 
   (select province_code,'大客户' as channel_name,0 daily_plan_sales_value,0 daily_plan_profit,(plan_sales_value)plan_sales_value ,(plan_profit)plan_profit 
   from csx_tmp.dws_csms_province_month_sale_plan_tmp
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
    and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
     union all 
    select province_code,'商超' as channel_name,0 daily_plan_sales_value,0 daily_plan_profit,(plan_sales_value)plan_sales_value ,(plan_profit)plan_profit 
    from csx_tmp.dws_ssms_province_month_sale_plan_tmp
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
     and sdt=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
     union all 
     select province_code,'大客户' as channel_name,coalesce(plan_sale_value,0) daily_plan_sales_value ,coalesce(plan_profit,0)daily_plan_profit,0 plan_sales_value,0 plan_profit 
      from csx_tmp.dws_daily_sales_plan
     where month= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
      and plan_sdt=${hiveconf:edate}
     and channel_code='1'
    ) d 
    group by 
province_code,
channel_name
;

-- select * from csx_tmp.dws_daily_sales_plan;
-- 本期同期负毛利汇总 
-- INSERT overwrite table csx_tmp.ads_sale_r_d_zone_sales_fr partition(months)
drop table if exists  csx_tmp.temp_plan_sale_01;
create temporary table  csx_tmp.temp_plan_sale_01 as 
SELECT '1' level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       case when a.channel_name='大客户' then '1' when a.channel_name='商超' then '2' else channel_name end  as channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       a.city_group_code,
       a.city_group_name,
       --0 as daily_plan_sale,
       sum(yesterday_sales_value/10000 )AS daily_sales_value,
       --0 as daily_sale_fill_rate,
       sum(last_day_sales/10000 ) as last_week_daily_sales,
       (coalesce(sum(yesterday_sales_value),0)-coalesce(sum(last_day_sales),0))/coalesce(sum(last_day_sales),0) as daily_sale_growth_rate,
       --0 as daily_plan_profit,
       sum(yesterday_profit/10000 )AS daily_profit,
        --0 as daily_profit_fill_rate,
       coalesce(sum(yesterday_profit)/sum(yesterday_sales_value),0) as daily_profit_rate,
       (negative_days_profit/10000) as daily_negative_profit,
       sum(yesterday_often_customer_sale/10000 )AS daily_often_cust_sale,
       sum(yesterday_new_customer_sale/10000 )AS daily_new_cust_sale,
       sum(yesterday_customer_num)AS daily_sale_cust_num,
       -- plan_sales_value as month_plan_sale,
       sum(months_sales_value/10000 )AS month_sale_value,
       -- sum(months_sales_value/10000 )/plan_sales_value as month_sale_fill_rate,
       sum(ring_months_sale/10000 )AS last_month_sale,
       (coalesce(sum(months_sales_value),0)-coalesce(sum(ring_months_sale),0))/coalesce(sum(ring_months_sale),0) as mom_sale_growth_rate,
      -- plan_profit as month_plan_profit,
       sum(months_profit/10000 )AS month_profit,
      -- sum(months_profit/10000 )/plan_profit as month_proft_fill_rate,
       sum(months_profit)/sum(months_sales_value) as month_profit_rate,
       (negative_profit/10000) as month_negative_profit, 
       sum(months_often_customer_sale/10000 )AS month_often_cust_sale,
       sum(months_new_customer_sale/10000 )AS month_new_cust_sale,
       sum(months_customer_num)AS month_sale_cust_num,
       sum(ring_date_sale/10000 ) AS last_month_daily_sale
FROM
  (SELECT channel_name,
          province_code,
          province_name,
          a.city_group_code,
          a.city_group_name,
          yesterday_sales_value,
          yesterday_profit,
          yesterday_often_customer_sale,
          yesterday_new_customer_sale,
          yesterday_customer_num,
          months_sales_value,
          months_profit,
          months_often_customer_sale,
          months_new_customer_sale,
          months_customer_num,
          last_day_sales,
          0 AS ring_date_sale,
          0 AS ring_months_sale
   FROM csx_tmp.temp_war_zone_sale_01 a
   UNION ALL SELECT channel_name,
                    province_code,
                    province_name,
                    a.city_group_code,
                    a.city_group_name,
                    0 AS yesterday_sales_value,
                    0 AS yesterday_profit,
                    0 AS yesterday_often_customer_sale,
                    0 AS yesterday_new_customer_sale,
                    0 AS yesterday_customer_num,
                    0 AS months_sales_value,
                    0 AS months_profit,
                    0 AS months_often_customer_sale,
                    0 AS months_new_customer_sale,
                    0 AS months_customer_num,
                    0 as last_day_sales,
                    last_yesterday_sales_value AS ring_date_sale,
                    last_months_sales_value AS ring_months_sale
   FROM csx_tmp.temp_ring_war_zone_sale a
   ) a  
   left join 
   csx_tmp.temp_war_zone_sale_02 c on a.province_code=c.province_code and a.channel_name=c.channel_name and a.city_group_code= c.city_group_code
   left join 
   (select DISTINCT province_code ,region_code zone_id,region_name zone_name 
    from csx_dw.dim_area where area_rank='13') b on 
    case when a.province_code in ('35','36') then '35' else a.province_code end =b.province_code
GROUP BY a.channel_name,
         a.province_code,
         a.province_name,
         a.city_group_code,
         a.city_group_name,
         zone_id,
         zone_name,
         negative_days_profit,
         negative_profit
    ;
    
    
--插入数据
INSERT overwrite table csx_tmp.ads_sale_r_d_city_sales_fr partition(months)
select 
       case when level_id ='1' then '1'
            when city_group_code is null and channel_code is not null and province_code is not null then '2'
            when  channel_code is  null and province_code is not null  then '3'
            when  province_code is  null then '4'
            end level_id,       
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_months,
       zone_id,
       zone_name,
       coalesce(channel_code,'00')channel_code,
       coalesce(a.channel_name,'全渠道') channel_name,
       coalesce(a.province_code,'00') province_code,
       coalesce(a.province_name, '-') province_name,
       coalesce(city_group_code,'00')city_group_code,
       coalesce(city_group_name,'-')  city_group_name,
       coalesce(daily_plan_sale,0)daily_plan_sale,
       daily_sales_value,
       coalesce(daily_sales_value/daily_plan_sale,0)    daily_sale_fill_rate,
       last_day_sales,
       coalesce((coalesce((daily_sales_value),0)-coalesce((last_day_sales),0))/coalesce((last_day_sales),0),0) as daily_sale_growth_rate,
       coalesce(daily_plan_profit,0)daily_plan_profit,
       daily_profit,
       coalesce(daily_profit/daily_plan_profit,0) as daily_profit_fill_rate,
       coalesce((daily_profit)/(daily_sales_value),0) as daily_profit_rate,
       daily_negative_profit,
       daily_often_cust_sale,
       daily_new_cust_sale,
       daily_sale_cust_num,
       coalesce(month_plan_sale,0)month_plan_sale,
       months_sales_value,
       (months_sales_value/month_plan_sale) as month_sale_fill_rate,
       last_month_sale,
       (coalesce((months_sales_value),0)-coalesce((last_month_sale),0))/coalesce((last_month_sale),0) as mom_sale_growth_rate,
       coalesce(month_plan_profit,0)month_plan_profit ,
       months_profit,
       (months_profit /month_plan_profit) as month_proft_fill_rate,
       (months_profit)/(months_sales_value) as month_profit_rate,
       month_negative_profit, 
       month_often_cust_sale,
       month_new_cust_sale,
       month_sale_cust_num,
       last_months_daily_sale,
       current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from 
(SELECT level_id,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       city_group_code,
       city_group_name,
       sum(coalesce(daily_plan_sales_value,0)) as daily_plan_sale,
       sum(daily_sales_value )as daily_sales_value,
       sum(daily_sales_value)/sum(coalesce(daily_plan_sales_value,0)) as daily_sale_fill_rate,
       sum(coalesce(last_week_daily_sales,0) ) as last_day_sales,
       (coalesce(sum(daily_sales_value),0)-coalesce(sum(last_week_daily_sales),0))/coalesce(sum(last_week_daily_sales),0) as daily_sale_rate,
       sum(daily_plan_profit) as daily_plan_profit,
       sum(daily_profit )AS daily_profit,
        sum(daily_profit)/sum(daily_plan_profit) as daily_profit_fill_rate,
       coalesce(sum(daily_profit)/sum(daily_sales_value),0) as daily_profit_rate,
       sum(daily_negative_profit) as daily_negative_profit,
       sum(daily_often_cust_sale )AS daily_often_cust_sale,
       sum(daily_new_cust_sale )AS daily_new_cust_sale,
       sum(daily_sale_cust_num)AS daily_sale_cust_num,
       sum(plan_sales_value)as month_plan_sale,
       sum(month_sale_value)AS months_sales_value,
       sum(month_sale_value)/sum(plan_sales_value) as month_sale_fill_rate,
       sum(last_month_sale )AS last_month_sale,
       (coalesce(sum(month_sale_value),0)-coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as months_sale_rate,
       sum(plan_profit)as month_plan_profit,
       sum(month_profit )as months_profit,
       sum(month_profit )/sum(plan_profit) as month_proft_fill_rate,
       sum(month_profit)/sum(month_sale_value) as months_profit_rate,
       sum(month_negative_profit) as month_negative_profit, 
       sum(month_often_cust_sale )AS month_often_cust_sale,
       sum(month_new_cust_sale )AS month_new_cust_sale,
       sum(month_sale_cust_num)AS month_sale_cust_num,
       sum(last_month_daily_sale ) AS last_months_daily_sale
FROM
 csx_tmp.temp_plan_sale_01 a  
left join 
   csx_tmp.temp_plan_sale d on a.province_code=d.province_code and trim(a.channel_name)=trim(d.channel_name)
   --where a.province_code='15'
 group by level_id,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       city_group_code,
       city_group_name
GROUPING SETS (
    (level_id,
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name,
       city_group_code,
       city_group_name),--一级
       (
       zone_id,
       zone_name,
       channel_code,
       a.channel_name,
       a.province_code,
       a.province_name), -- 省区渠道合计
       (
       zone_id,
       zone_name,
       a.province_code,
       a.province_name,
       city_group_code,
       city_group_name), -- 城市合计
       (
       zone_id,
       zone_name,
       a.province_code,
       a.province_name),--省区合计
       (
       zone_id,
       zone_name,
       channel_code,
       a.channel_name) , --战区渠道合计
        (
       zone_id,
       zone_name)  --战区合计
       )
) a ;


-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
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



-- 客户属性数据插入
drop table if exists csx_tmp.temp_city_attribute_01;
create temporary table csx_tmp.temp_city_attribute_01
as 
select  
       zone_id,zone_name ,
       a.province_code ,
       province_name ,
       a.city_group_code,
         a.city_group_name,
       attribute_code,
       attribute,       
       sum(daily_sales_value/10000 )as daily_sales_value,
       sum(daily_profit/10000) as daily_profit,
      --sum(daily_profit)/sum(daily_sales_value) as days_profit_rate,
       sum(month_sale/10000 )month_sale,
       sum(last_month_sale/10000 ) as last_month_sale,
      -- (sum(month_sale)- coalesce(sum(last_month_sale),0))/coalesce(sum(last_month_sale),0) as mom_sale_rate,
       sum(month_profit/10000)month_profit,
       sum(month_profit)/sum(month_sale) as profit_rate,
       sum(month_sale_cust_num )as month_sale_cust_num,
       --sum(month_sale_cust_num-last_month_sale_cust_num) as diff_sale_cust,
       sum(last_month_profit/10000) as last_month_profit,
       sum(last_month_sale_cust_num) as last_month_sale_cust_num,
       sum(0) as daily_plan_sale,
       sum(0)month_plan_sale ,
       sum(0)month_plan_profit
from (
   SELECT 
        a.region_code as zone_id,
        a.region_name as zone_name,
       province_code ,
       a.province_name,
       a.city_group_code,
        a.city_group_name,
       business_type_code as  attribute_code,
       a.business_type_name as attribute,
       sum(case when sdt= regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sales_value,
       sum(case when sdt= regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
       sum(sales_value )month_sale,
       sum(profit )month_profit,
       count(distinct a.customer_no )as month_sale_cust_num,
       0 as last_month_sale,
       0 as last_month_profit,
       0 as last_month_sale_cust_num,
       0 as daily_plan_sale,
       0 as month_plan_sale ,
       0 as month_plan_profit 
   FROM csx_dw.dws_sale_r_d_detail a 
    where sdt>=regexp_replace(${hiveconf:sdate},'-','') 
    and sdt<= regexp_replace(${hiveconf:edate},'-','') 
    and a.channel_code in('1','7','9')
   group by 
            business_type_code ,
            province_code,
            city_group_code,
            a.business_type_name,
             a.city_group_name,
             a.region_code ,
            a.region_name,
            a.province_name
 union all 
   SELECT 
        a.region_code as zone_id,
        a.region_name as zone_name,
       province_code ,
       a.province_name,
       city_group_code,
        a.city_group_name,
       business_type_code as attribute_code,
       a.business_type_name as attribute,
       0 as daily_sales_value,
       0 as daily_profit,
       0 as month_sale,
       0 as month_profit,
       0 as month_sale_cust_num,
       sum(sales_value)as last_month_sale,
       sum(profit)as last_month_profit,
       count(distinct a.customer_no)as last_month_sale_cust_num ,
        0 as daily_plan_sale,
        0 as month_plan_sale ,
        0 as month_plan_profit 
   FROM csx_dw.dws_sale_r_d_detail a     
    where sdt>= regexp_replace(${hiveconf:l_sdate},'-','') 
    and sdt<= regexp_replace(${hiveconf:l_edate},'-','') 
    and a.channel_code in('1','7','9')
   group by
       business_type_code ,
       province_code,
       city_group_code,
       a.business_type_name,
       a.city_group_name,
        a.region_code ,
        a.region_name,
       a.province_name

) a 
group by zone_id,zone_name ,
        a.province_code ,
        province_name,
        attribute,
        attribute_code,
        a.city_group_code,
        a.city_group_name
;


drop table if exists csx_tmp.temp_city_attribute_02;
create temporary table csx_tmp.temp_city_attribute_02 as 
select 
       
       zone_id,
       zone_name ,
       coalesce(a.province_code,'00') province_code ,
       coalesce(province_name,'-') province_name ,
       coalesce(city_group_code,'00' ) city_group_code,
       coalesce(a.city_group_name,'-') city_group_name,
       coalesce(attribute_code,'00') attribute_code,
       coalesce(attribute,'-') attribute, 
       daily_plan_sale,
       daily_sales_value,
       coalesce(daily_sales_value/daily_plan_sale,0) as daily_sale_fill_rate,
       daily_profit,
       coalesce(daily_profit/daily_sales_value,0) as daily_profit_rate,
       month_plan_sale,
       month_sale,
       coalesce(month_sale/month_plan_sale,0) as month_sale_fill_rate,
       coalesce(last_month_sale,0)last_month_sale,
       coalesce((month_sale-last_month_sale)/abs(last_month_sale),0)  mom_sale_growth_rate,
       month_plan_profit,
       coalesce(month_profit,0) as month_profit,
       coalesce(month_profit/month_plan_profit,0) month_profit_fill_rate,
       coalesce(month_profit/month_sale,0) as month_profit_rate,
       coalesce(month_sale_cust_num,0) as month_sale_cust_num,
       coalesce(month_sale_cust_num-last_month_sale_cust_num,0)   as mom_diff_sale_cust,
       coalesce(last_month_profit,0) as last_month_profit,
       coalesce(last_month_sale_cust_num,0) as last_month_sale_cust_num,
       grouping__id
from (
select 
       zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       city_group_code,
       a.city_group_name,
       attribute_code,
       attribute, 
       sum(daily_plan_sale) as daily_plan_sale,
       sum(daily_sales_value) as daily_sales_value,
       sum(daily_profit) as daily_profit,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale) as month_sale,
       sum(coalesce(last_month_sale,0)) last_month_sale,
       sum(month_plan_profit) month_plan_profit,
       sum(coalesce(month_profit,0)) as month_profit,
       sum(coalesce(month_sale_cust_num,0)) as month_sale_cust_num,
       sum(coalesce(last_month_profit,0)) as last_month_profit,
       sum(coalesce(last_month_sale_cust_num,0)) as last_month_sale_cust_num,
       grouping__id
from csx_tmp.temp_city_attribute_01 a 
 group by zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       city_group_code,
       a.city_group_name,
       attribute_code,
       attribute
grouping sets 
((zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       city_group_code,
       a.city_group_name,
       attribute_code,
       attribute),   --1 城市组客户属性
        (zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       city_group_code,
       a.city_group_name), --2 城市组汇总
            (zone_id,
       zone_name ,
       a.province_code ,
       province_name ,
       attribute_code,
       attribute),  -- 3 省区客户属性
       (zone_id,
       zone_name ,
       a.province_code ,
       province_name )  -- 4 省区汇总
      
       )
) a ;

 insert overwrite table  csx_tmp.ads_sale_r_d_city_cust_attribute_fr partition(months)
 
 select 
      level_id,
     substr(regexp_replace(${hiveconf:edate},'-',''),1,6)as sales_month,
       zone_id,
       zone_name ,
       province_code ,
       province_name ,
       city_group_code,
       city_group_name,
       attribute_code,
       attribute, 
       daily_plan_sale,
       daily_sales_value,
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
       month_sale_cust_num,
       mom_diff_sale_cust,
       last_month_profit,
       last_month_sale_cust_num,
        current_timestamp(),
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from(
 select 
    case when attribute !='-' and a.city_group_name !='-' then '1'
        when attribute ='-' and city_group_name !='-'  then '2'
        when city_group_name ='-' and  province_name!='-' then '3'
    else '4'
    end level_id,
       zone_id,
       zone_name ,
       province_code ,
       province_name ,
       city_group_code,
       city_group_name,
       attribute_code,
       attribute, 
       daily_plan_sale,
       daily_sales_value,
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
       month_sale_cust_num,
       mom_diff_sale_cust,
       last_month_profit,
       last_month_sale_cust_num
from csx_tmp.temp_city_attribute_02 a)a;



-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
set tez.am.speculation.enabled=true;  --是否开启推测执行，默认是false，在出现最后一个任务很慢的情况下，建议把这个参数设置为true
set tez.am.resource.memory.mb=8000;  --am分配的内存大小，默认1024
set tez.task.resource.memory.mb=8000;  --分配的内存，默认1024 ,出现内存不够时候，设置更大点
set tez.am.resource.cpu.vcores=8;  -- am分配的cpu个数，默认1
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



-- 明细
drop table if exists csx_tmp.temp_attribute_sale_01;
create temporary table csx_tmp.temp_attribute_sale_01
as 
select  province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    channel,
    channel_name,
    attribute_name,
    attribute_code,
    division_code ,
    division_name,
    department_code ,
    department_name,
    sum(coalesce(daily_sale_value,0))as daily_sale_value,
    sum(coalesce(daily_profit,0)) as daily_profit,
    sum(coalesce(month_sale,0)) month_sale,
    sum(coalesce(month_profit,0)) month_profit,
    sum(coalesce(month_sale_cust_num,0))as month_sale_cust_num,
    sum(coalesce(month_sales_sku,0))as month_sales_sku,
    sum(coalesce(last_month_sale,0)) as last_month_sale
from (
select
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
        end channel,
    case when a.channel_code in ('1','7','9') then '大客户'
        else a.channel_name
        end channel_name,
        a.business_type_name as    attribute_name,
        a.business_type_code as attribute_code,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end       department_code ,
    case when department_code like 'U%' then '加工课' else department_name end   department_name,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt =  regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
    sum(sales_value) month_sale,
    sum(profit) month_profit,
    count(distinct a.customer_no )as month_sale_cust_num,
    count(distinct goods_code )as month_sales_sku,
    0 as last_month_sale
from
    csx_dw.dws_sale_r_d_detail a
where
    sdt >=   regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=   regexp_replace(${hiveconf:edate},'-','')
  --  and  channel ='1'
  --  and  a.attribute_code in ('1','2') and a.order_kind!='WELFARE'
group by 
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end   , 
    case when department_code like 'U%' then '加工课' else department_name end ,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
        end ,
    case when a.channel_code in ('1','7','9') then '大客户'
        else a.channel_name
        end ,
     a.business_type_code,
      a.business_type_name
union all 
select
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
   case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
        end channel,
    case when a.channel_code in ('1','7','9') then '大客户'
        else a.channel_name
        end channel_name,
      a.business_type_name as attribute_name,
      a.business_type_code as  attribute_code,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end  department_code  , 
    case when department_code like 'U%' then '加工课' else department_name end  department_name,
    0 as daily_sale_value,
    0 as daily_profit,
    0 month_sale,
    0 month_profit,
    0 month_sale_cust_num,
    0 month_sales_sku,
    sum(sales_value)as last_month_sale
from
    csx_dw.dws_sale_r_d_detail a
where
    sdt >=  regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
   -- and  channel ='1'
   -- and  a.attribute_code in ('1','2') and a.order_kind!='WELFARE'
group by 
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end   , 
    case when department_code like 'U%' then '加工课' else department_name end ,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
        end ,
    case when a.channel_code in ('1','7','9') then '大客户'
        else a.channel_name
        end ,
     a.business_type_code ,
     a.business_type_name
) a 
group by 
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    channel,
    channel_name,
    attribute_name,
    attribute_code,
    division_code ,
    division_name,
    department_code ,
    department_name    ;
 
-- select sum(month_sale) from  csx_tmp.temp_attribute_sale_02 where province_code='32' and attribute_code='1' and channel='1' and department_code='104' ;
 
--- 计算课组层级
drop table  if exists csx_tmp.temp_attribute_sale_02;
create temporary table csx_tmp.temp_attribute_sale_02 as
select
   '1' as level_id,
    zone_id,
    zone_name,
    a.province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end business_division_code,
    case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end business_division_name,
    division_code ,
    division_name,
    department_code ,
    department_name,
    0 daily_plan_sale,
    daily_sale_value,
    0 daily_sale_fill_rate,
    daily_profit,
    coalesce(daily_profit/daily_sale_value,0) daily_profit_rate,
    0 as month_plan_sale,
    month_sale,
    0 as month_sale_fill_rate,
    last_month_sale,
   coalesce((month_sale-last_month_sale)/abs(last_month_sale),0) as mom_sale_growth_rate,
   coalesce(month_sale/sum(month_sale)over(partition by a.province_code,a.attribute_code),0) month_sale_ratio,
   coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
   0 month_plan_profit,
    month_profit,
    0 month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust) as penetration_rate,  -- 渗透率
    (all_sale_cust) as all_sale_cust_num,
    row_number()over(partition by a.province_code ,a.attribute_code order by month_sale desc) as row_num
from csx_tmp.temp_attribute_sale_01    a 
left join 
(
select
    province_code ,
    a.city_group_code,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
        end channel,
   a.business_type_code as  attribute_code,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_detail a
where
    sdt >=   regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    
group by 
    province_code ,
    a.city_group_code,
   case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
        end ,
    a.business_type_code
   ) b on a.province_code=b.province_code and a.attribute_code=b.attribute_code and a.channel=b.channel and a.city_group_code=b.city_group_code
   left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
;

-- 插入数据表
insert overwrite table csx_tmp.ads_sale_r_d_city_dept_fr partition(months)
select 
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    zone_id,
    zone_name,
    a.province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    department_code ,
    department_name,
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
    month_sale_ratio,
    month_avg_cust_sale,
    month_plan_profit,
    month_profit,
    month_profit_fill_rate,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    penetration_rate cust_penetration_rate,  -- 渗透率
    all_sale_cust_num,
    row_num,
    current_timestamp(),
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from  csx_tmp.temp_attribute_sale_02 a;

-- describe csx_tmp.ads_sale_r_d_zone_province_dept_fr ;
-- 插入汇总数据
insert into table csx_tmp.ads_sale_r_d_city_dept_fr partition(months)
select
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    zone_id,
    zone_name,
    province_code ,
    province_name ,
    city_group_code,
    city_group_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    department_code ,
    department_name,
    daily_plan_sale,
    daily_sale_value,
    coalesce(daily_sale_value/daily_plan_sale,0) daily_sale_fill_rate,
    daily_profit,
    coalesce(daily_profit/daily_sale_value,0) daily_profit_rate,
    month_plan_sale,
    month_sale,
    month_sale_fill_rate,
    last_month_sale,
   coalesce((month_sale-last_month_sale)/abs(last_month_sale),0) as mom_sale_growth_rate,
   coalesce(month_sale/sum(month_sale)over(partition by zone_id,a.attribute_code),0) month_sale_ratio,
   coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
    month_plan_profit,
    month_profit,
    coalesce(month_profit / month_plan_profit,0) month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust_num) as penetration_rate,  -- 渗透率
    all_sale_cust_num,
    row_number()over(partition by a.province_code ,a.attribute_code order by month_sale desc) as row_num,
    current_timestamp(),
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from(
select
    '2' as level_id,
    zone_id,
    zone_name,
    province_code ,
    province_name ,
    '00' as city_group_code,
    '-' as city_group_name,
    channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    department_code ,
    department_name,
    sum(daily_plan_sale)daily_plan_sale,
    sum(daily_sale_value)daily_sale_value,
    sum(daily_profit)daily_profit,
   --coalesce(sum(daily_profit)/sum(daily_sale_value),0) daily_profit_rate,
    sum(month_plan_sale) as month_plan_sale,
    sum(month_sale) month_sale,
    sum(month_sale_fill_rate) as month_sale_fill_rate,
    sum(last_month_sale)last_month_sale,
   --coalesce((sum(month_sale)-sum(last_month_sale))/abs(sum(last_month_sale)),0) as mom_sale_growth_rate,
   --coalesce(sum(month_sale)/sum(month_sale)over(partition by zone_id,a.attribute_code),0) month_sale_ratio,
   --coalesce(sum(month_sale)/sum(month_sale_cust_num),0) as month_avg_cust_sale,
    sum(month_plan_profit)month_plan_profit,
    sum(month_profit) month_profit,
    -- coalesce(sum(month_profit) /sum( month_plan_profit),0) month_profit_fill_rate,
   -- sum(month_profit)/sum(month_sale) as month_profit_rate,
    sum(month_sales_sku)month_sales_sku,
    sum(month_sale_cust_num)month_sale_cust_num,
    -- sum(month_sale_cust_num)/sum(all_sale_cust_num) as penetration_rate,  -- 渗透率
    sum(all_sale_cust_num) as all_sale_cust_num
from  csx_tmp.temp_attribute_sale_02 a
group by 
    zone_id,
    zone_name,
    province_code ,
    province_name ,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    division_code ,
    division_name,
    department_code ,
    department_name,
    business_division_code,
    business_division_name
) a ;



  set hive.execution.engine=spark;
-- set tez.queue.name=caishixian;
-- set hive.exec.dynamic.partition=true;
-- set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true; 
set hive.exec.parallel.thread.number=100;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=1024000000;
set mapred.min.split.size.per.rack=1024000000;
-- 当前日期
SET edate= '${enddate}';
-- 月初
SET sdate=trunc(${hiveconf:edate},'MM');
-- 上月初
SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');
-- 上月当前日期
SET l_edate=add_months(${hiveconf:edate},-1);


-- 创建临时销售表
drop table if exists csx_tmp.temp_sale_02;
create temporary table csx_tmp.temp_sale_02 as 
select
    sdt,
    zone_id,
    zone_name ,
    province_code ,
    province_name ,
     a.city_group_code,
    a.city_group_name,
    a.customer_no,
    a.goods_code,
    channel,
    channel_name,
    attribute_name,
    attribute_code,
    case when division_code in ('10','11') then '11' when division_code in ('12','13','14','15') then '12' else division_code end business_division_code,
    case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14','15') then '食百采购部' else division_name end business_division_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ,
    sales_value,
    profit,
    last_month_sale,
    last_month_profit
from (
select
    sdt,
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    a.customer_no,
    a.goods_code,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
    end channel,
    case when a.channel_code in ('1','7','9') then '大客户'
        else a.channel_name
    end channel_name,
    a.business_type_name as attribute_name,
    a.business_type_code as attribute_code,
    division_code ,
    division_name,
    m.classify_middle_code ,
    m.classify_middle_name ,
    sum( case when sdt between  regexp_replace(${hiveconf:sdate},'-','')
    and  regexp_replace(${hiveconf:edate},'-','') then  sales_value end ) sales_value,
    sum( case when sdt between  regexp_replace(${hiveconf:sdate},'-','')
    and   regexp_replace(${hiveconf:edate},'-','') then profit end) profit,
    sum( case when sdt between   regexp_replace(${hiveconf:l_sdate},'-','')
    and  regexp_replace(${hiveconf:l_edate},'-','') then  sales_value end ) last_month_sale,
    sum( case when sdt between   regexp_replace(${hiveconf:l_sdate},'-','')
    and  regexp_replace(${hiveconf:l_edate},'-','') then profit end) last_month_profit
from
    csx_dw.dws_sale_r_d_detail a
left outer join 
(SELECT category_small_code,
       classify_middle_code,
       classify_middle_name
    FROM csx_dw.dws_basic_w_a_manage_classify_m
        WHERE sdt='current') m on a.category_small_code=m.category_small_code
where
    sdt >= regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <= regexp_replace(${hiveconf:edate},'-','')
group by 
sdt,
    province_code ,
    province_name ,
    a.customer_no,
    a.goods_code,
    a.city_group_code,
    a.city_group_name,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
    end  ,
    case when a.channel_code in ('1','7','9') then '大客户'
        else a.channel_name
    end  ,
    a.business_type_code,
    a.business_type_name,
    division_code ,
    division_name,
    m.classify_middle_code ,
    m.classify_middle_name
)a 
 left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
;


-- 明细
drop table if exists csx_tmp.temp_attribute_sale_01;
create temporary table csx_tmp.temp_attribute_sale_01
as 
select 
    zone_id,
    zone_name,
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    channel,
    channel_name,
    attribute_name,
    attribute_code,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ,
    sum(coalesce(daily_sale_value,0))as daily_sale_value,
    sum(coalesce(daily_profit,0)) as daily_profit,
    sum(coalesce(month_sale,0)) month_sale,
    sum(coalesce(month_profit,0)) month_profit,
    sum(coalesce(month_sale_cust_num,0))as month_sale_cust_num,
    sum(coalesce(month_sales_sku,0))as month_sales_sku,
    sum(coalesce(last_month_sale,0)) as last_month_sale,
    sum(a.last_month_profit) as last_month_profit,
    sum(last_month_sale_cust_num) as last_month_sale_cust_num
from (
select
    zone_id,
    zone_name,
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    channel,
    channel_name,
    attribute_name,
    attribute_code,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
    sum(sales_value) month_sale,
    sum(profit) month_profit,
    count(distinct case when a.sales_value>0 then  a.customer_no end )as month_sale_cust_num,
    count(distinct case when sdt between   regexp_replace(${hiveconf:sdate},'-','')
    and  regexp_replace(${hiveconf:edate},'-','') then goods_code end )as month_sales_sku,
    sum(last_month_sale) as last_month_sale,
    sum(last_month_profit) as last_month_profit,
    count(distinct case when a.last_month_sale>0 then  a.customer_no end ) as last_month_sale_cust_num
from
    csx_tmp.temp_sale_02 a
where
   1=1
group by 
    zone_id,
    zone_name,
    province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ,
    attribute_name,
    attribute_code,
    channel,
    channel_name
) a 
group by 
    zone_id,
    zone_name,
    province_code ,
    province_name ,
     a.city_group_code,
    a.city_group_name,
    channel,
    channel_name,
    attribute_name,
    attribute_code,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ;
 
-- select sum(month_sale) from  csx_tmp.temp_attribute_sale_02 where province_code='32' and attribute_code='1' and channel='1' and department_code='104' ;
 
--- 计算课组层级
drop table  if exists csx_tmp.temp_attribute_sale_02;
create temporary table csx_tmp.temp_attribute_sale_02 as
select
   '1' as level_id,
    zone_id,
    zone_name,
    a.province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    case when division_code in ('10','11') then '11' when division_code in ('12','13','14','15') then '12' else division_code end business_division_code,
    case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14','15') then '食百采购部' else division_name end business_division_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name,
    0 daily_plan_sale,
    daily_sale_value,
    0 daily_sale_fill_rate,
    daily_profit,
    coalesce(daily_profit/daily_sale_value,0) daily_profit_rate,
    0 as month_plan_sale,
    month_sale,
    0 as month_sale_fill_rate,
    last_month_sale,
   coalesce((month_sale-last_month_sale)/abs(last_month_sale),0) as mom_sale_growth_rate,
   coalesce(month_sale/sum(month_sale)over(partition by a.province_code,a.attribute_code),0) month_sale_ratio,
   coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
   0 month_plan_profit,
    month_profit,
    0 month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust) as penetration_rate,  -- 渗透率
    (all_sale_cust) as all_sale_cust_num,
    row_number()over(partition by a.province_code ,a.attribute_code order by month_sale desc) as row_num,
    a.last_month_profit,
    a.last_month_sale_cust_num,
    last_all_sale_cust
from csx_tmp.temp_attribute_sale_01    a 
left join 
(
select
    province_code ,
    city_group_code,
    channel,
    attribute_code,
    count(distinct case when sales_value>0  then a.customer_no end  )as all_sale_cust,
    count(distinct case when a.last_month_sale>0  then a.customer_no end  )as last_all_sale_cust

from
     csx_tmp.temp_sale_02 a
where
    sdt >=   regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    
group by 
    province_code ,
     a.city_group_code,
     a.channel,
    attribute_code
   ) b on a.province_code=b.province_code and a.attribute_code=b.attribute_code and a.channel=b.channel and a.city_group_code=b.city_group_code
;

-- 插入数据表 销售环比，销售占比，毛利率环比、渗透率占比差
insert overwrite table csx_tmp.report_sale_r_d_kanban_classify_sale_fr partition(months,sdt)
-- create table csx_tmp.report_sale_r_d_kanban_classify_sale_fr as 
select 
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    zone_id,
    zone_name,
    a.province_code ,
    province_name ,
    a.city_group_code,
    a.city_group_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name,
    daily_plan_sale,
    daily_sale_value,
    daily_sale_fill_rate,
    daily_profit,
    daily_profit_rate,
    month_plan_sale,
    month_sale,
    month_sale_fill_rate,    --销售达成率
    mom_sale_growth_rate,    -- 销售环比
    month_sale_ratio,        --销售占比
    month_avg_cust_sale,     --客单价
    month_plan_profit,       -- 毛利额计划
    month_profit,            --毛利额
    month_profit_fill_rate,  --毛利额完成率
    month_profit_rate,       --毛利率
    month_sales_sku,         --销售SKU   
    month_sale_cust_num,     --成交客户数
    penetration_rate cust_penetration_rate,  -- 本期渗透率
    all_sale_cust_num,      --本期成交客户
    last_month_sale,        --上期销售额
    a.last_month_profit,    --上期毛利额
    a.last_month_profit/last_month_sale as last_profit_rate,    --上期毛利率
    last_month_sale_cust_num/last_all_sale_cust as  last_cust_penetration_rate, --上期渗透率
    a.last_month_sale_cust_num,  --上期成交客户数
    last_all_sale_cust,     --上期总成交客户数
    0 as same_period_sale,       --  '同期销售额',
    0 as same_period_profit,         -- '同期毛利额',
    0 as same_period_profit_rate,    --'同期毛利率',
    0 as same_period_cust_penetration_rate ,     --  '同期客户渗透率',
    0 as same_period_sale_cust_num ,     -- '同期成交客户数',
    0 as same_period_all_sale_cust,      --  '同期总成交客户数',
    row_num,    
    current_timestamp(),
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6),
    regexp_replace(${hiveconf:edate},'-','')
from  csx_tmp.temp_attribute_sale_02 a;

-- describe csx_tmp.ads_sale_r_d_zone_province_dept_fr ;
-- 插入汇总数据
insert into table csx_tmp.report_sale_r_d_kanban_classify_sale_fr partition(months,sdt)
select
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    a.zone_id,
    a.zone_name,
    province_code ,
    province_name ,
    city_group_code,
    city_group_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name,
    0 as daily_plan_sale,
    daily_sale_value,
    coalesce(daily_sale_value/daily_plan_sale,0) daily_sale_fill_rate,
    daily_profit,
    coalesce(daily_profit/daily_sale_value,0) daily_profit_rate,
    0 as month_plan_sale,
    month_sale,
    coalesce(month_sale/month_plan_sale,0) month_sale_fill_rate,
    coalesce((month_sale-last_month_sale)/abs(last_month_sale),0) as mom_sale_growth_rate,
    coalesce(month_sale/sum(month_sale)over(partition by a.zone_id,a.attribute_code),0) month_sale_ratio,
    coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
    0 as month_plan_profit,
    month_profit,
    coalesce(month_profit / month_plan_profit,0) month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust_num) as penetration_rate,  -- 渗透率
    all_sale_cust_num,
    last_month_sale,
    last_month_profit,
    a.last_month_profit/last_month_sale as last_profit_rate,    --上期毛利率
    last_month_sale_cust_num/last_all_sale_cust as  last_cust_penetration_rate, --上期渗透率
    a.last_month_sale_cust_num,  --上期成交客户数
    last_all_sale_cust,     --上期总成交客户数
    0 as same_period_sale,       --  '同期销售额',
    0 as same_period_profit,         -- '同期毛利额',
    0 as same_period_profit_rate,    --'同期毛利率',
    0 as same_period_cust_penetration_rate ,     --  '同期客户渗透率',
    0 as same_period_sale_cust_num ,     -- '同期成交客户数',
    0 as same_period_all_sale_cust,      --  '同期总成交客户数',
    row_number()over(partition by a.zone_id ,a.attribute_code order by month_sale desc) as row_num,
    current_timestamp(),
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6),
    regexp_replace(${hiveconf:edate},'-','')
from(
select
    '2' as level_id,
    zone_id,
    zone_name,
     province_code ,
     province_name ,
    '00' as city_group_code,
    province_name as city_group_name,
    channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name,
     0 as daily_plan_sale,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
     0 as month_plan_sale,
    sum(sales_value) month_sale,
    0 as month_plan_profit,
    sum(profit) month_profit,
    count(distinct case when a.sales_value>0 then  a.customer_no end )as month_sale_cust_num,
    count(distinct case when sdt between  regexp_replace(${hiveconf:sdate},'-','')
        and  regexp_replace(${hiveconf:edate},'-','') then goods_code end )as month_sales_sku,
    sum(last_month_sale) as last_month_sale,
    sum(last_month_profit) as last_month_profit,
    count(distinct case when a.last_month_sale>0 then  a.customer_no end ) as last_month_sale_cust_num
from  csx_tmp.temp_sale_02  a
where 1=1
group by 
    zone_id,
    zone_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name,
    business_division_code,
    business_division_name,
    province_code,
    a.province_name
union all 
select
    '3' as level_id,
    zone_id,
    zone_name,
    '00' as  province_code ,
    zone_name as province_name ,
    '00'city_group_code,
    '小计' as city_group_name,
    channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name,
     0 as daily_plan_sale,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as daily_sale_value,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as daily_profit,
     0 as month_plan_sale,
    sum(sales_value) month_sale,
    0 as month_plan_profit,
    sum(profit) month_profit,
    count(distinct case when a.sales_value>0 then  a.customer_no end )as month_sale_cust_num,
    count(distinct case when sdt between  regexp_replace(${hiveconf:sdate},'-','')
        and  regexp_replace(${hiveconf:edate},'-','') then goods_code end )as month_sales_sku,
    sum(last_month_sale) as last_month_sale,
    sum(last_month_profit) as last_month_profit,
    count(distinct case when a.last_month_sale>0 then  a.customer_no end ) as last_month_sale_cust_num
from  csx_tmp.temp_sale_02  a
where 1=1
group by 
    zone_id,
    zone_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name,
    business_division_code,
    business_division_name

) a 
left join 
(
select
    zone_id ,
    channel,
    attribute_code,
    count(distinct case when sales_value>0  then a.customer_no end  )as all_sale_cust_num,
    count(distinct case when a.last_month_sale>0  then a.customer_no end  )as last_all_sale_cust
from
     csx_tmp.temp_sale_02 a
where
    sdt >=   regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
group by 
    zone_id ,
    a.channel,
    attribute_code
   ) b on a.zone_id=b.zone_id and a.attribute_code=b.attribute_code and a.channel=b.channel
;


show create table csx_tmp.report_sale_r_d_zone_classify_sale_fr;


CREATE TABLE `csx_tmp.report_sale_r_d_kanban_classify_sale_fr`(
  `level_id` string COMMENT '层级：1管理分类 ，2部类 3、全国', 
  `sales_month` string COMMENT '销售月份', 
  `zone_id` string COMMENT '战区', 
  `zone_name` string COMMENT '战区名称', 
  `province_code` string COMMENT '省区', 
  `province_name` string COMMENT '省区名称', 
   `city_group_code` string COMMENT '城市', 
  `city_group_name` string COMMENT '城市', 
  `channel` string COMMENT '渠道', 
  `channel_name` string COMMENT '渠道名称', 
  `attribute_code` string COMMENT '客户性属', 
  `attribute_name` string COMMENT '客户性属名称', 
  `business_division_code` string COMMENT '采购部', 
  `business_division_name` string COMMENT '采购部名称', 
  `division_code` string COMMENT '部类', 
  `division_name` string COMMENT '部类名称', 
  `classify_middle_code` string COMMENT '管理二级分类', 
  `classify_middle_name` string COMMENT '管理二级分类名称', 
  `daily_plan_sale` decimal(38,6) COMMENT '计划销售额', 
  `daily_sale_value` decimal(38,6) COMMENT '昨日销售额', 
  `daily_sale_fill_rate` decimal(38,6) COMMENT '昨日销售达成率', 
  `daily_profit` decimal(38,6) COMMENT '昨日毛利额', 
  `daily_profit_rate` decimal(38,6) COMMENT '昨日毛利率', 
  `month_plan_sale` decimal(38,6) COMMENT '月计划', 
  `month_sale` decimal(38,6) COMMENT '月销售额', 
  `month_sale_fill_rate` decimal(38,6) COMMENT '月销售达成率', 
  `mom_sale_growth_rate` decimal(38,6) COMMENT '环比增长率', 
  `month_sale_ratio` decimal(38,6) COMMENT '销售占比', 
  `month_avg_cust_sale` decimal(38,6) COMMENT '客单价', 
  `month_plan_profit` decimal(38,6) COMMENT '毛利计划', 
  `month_profit` decimal(38,6) COMMENT '月毛利额', 
  `month_profit_fill_rate` decimal(38,6) COMMENT '月毛利额达成', 
  `month_profit_rate` decimal(38,6) COMMENT '月毛利率', 
  `month_sales_sku` bigint COMMENT '月销售SKU', 
  `month_sale_cust_num` bigint COMMENT '月成交客户数', 
  `cust_penetration_rate` decimal(38,6) COMMENT '客户渗透率', 
  `all_sale_cust_num` bigint COMMENT '总成交客户数', 
  `last_month_sale` decimal(38,6) COMMENT '上期销售额', 
  `last_month_profit` decimal(38,6) COMMENT '上期毛利额', 
  `last_profit_rate` decimal(38,6) COMMENT '上期毛利率', 
  `last_cust_penetration_rate` decimal(38,6) COMMENT '上期渗透率', 
  `last_month_sale_cust_num` decimal(38,6) COMMENT '上期成交客户数', 
  `last_all_sale_cust` decimal(38,6) COMMENT '上期总成交客户数', 
  `same_period_sale` decimal(38,6) COMMENT '同期销售额', 
  `same_period_profit` decimal(38,6) COMMENT '同期毛利额', 
  `same_period_profit_rate` decimal(38,6) COMMENT '同期毛利率', 
  `same_period_cust_penetration_rate` decimal(38,6) COMMENT '同期客户渗透率', 
  `same_period_sale_cust_num` bigint COMMENT '同期成交客户数', 
  `same_period_all_sale_cust` bigint COMMENT '同期总成交客户数', 
  `row_num` int COMMENT '排名', 
  `updatetime` timestamp COMMENT '更新日期')
COMMENT '看板管理二级分类分析'
PARTITIONED BY ( 
  `months` string COMMENT '月分区', 
  `sdt` string COMMENT '日期分区')
;




    
