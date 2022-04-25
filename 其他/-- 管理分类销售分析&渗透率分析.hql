--管理品类销售分析&渗透率分析 20211025
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET hive.optimize.sort.dynamic.partition=true;
--执行Map前进行小文件合并  
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;   
 


set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
--上月结束日期，当前日期不等于月末取当前日期，等于月末取上月最后一天
set last_edt=regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','');

set s_dt_30 =regexp_replace(date_sub(${hiveconf:edt},30),'-','');
-- set parquet.compression=snappy;
-- set hive.exec.dynamic.partition=true; 
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt},regexp_replace(date_sub(${hiveconf:edt},30),'-','') ;


-- 本期数据 (含城市服务商)B端 channel_code in ('1','9','7')

drop table if exists csx_tmp.tmp_dp_sale;
create temporary table csx_tmp.tmp_dp_sale
as 
select 
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.customer_no,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sum(sales_value)as sales_value,
    sum(a.profit)as profit
from csx_dw.dws_sale_r_d_detail a 
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
    --and a.business_type_code !='4'
    and a.channel_code  in ('1','7','9')
group by 
    case when channel_code in ('1','9','7') then 'B端' end,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    business_type_code ,
    a.customer_no,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.province_code,
    a.province_name,
    a.customer_no
;

-- 环期数据 
drop table if exists csx_tmp.tmp_dp_sale_01;
create temporary table csx_tmp.tmp_dp_sale_01
as 
select 
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.customer_no,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sum(sales_value)as last_sales_value,
    sum(a.profit)as last_profit
from csx_dw.dws_sale_r_d_detail a 
where sdt>=${hiveconf:last_sdt}
    and sdt<=${hiveconf:last_edt} 
    and a.channel_code   in ('1','7','9')
group by 
    case when channel_code in ('1','9','7') then 'B端' end,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.customer_no,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
;


-- 本期与环比汇总
drop table if exists csx_tmp.temp_sale_all;
create temporary table csx_tmp.temp_sale_all as 
select
    channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_no,
    business_type_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_value) as sales_value,
    sum(profit) as profit , 
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit 
from
(select channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_no,
    a.business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sales_value,
    profit, 
    0 as last_sales_value,
    0 as last_profit 
from csx_tmp.tmp_dp_sale a
union all
select channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_no,
    a.business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    0 as sales_value,
    0 as profit, 
    last_sales_value  ,
    last_profit 
from csx_tmp.tmp_dp_sale_01 a
) a
group by 
    channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_no,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name

    ; 
    


-- 本期与环比汇总层级汇总 
drop table if exists csx_tmp.temp_sale_all_01;
create temporary table csx_tmp.temp_sale_all_01 as 
select
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_value) as sales_value,
    sum(profit) as profit ,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    sum(case when business_type_code='1' then sales_value end ) as daily_sales_value,
    sum(case when business_type_code='1' then last_sales_value end ) as last_daily_sales_value,
    sum(case when business_type_code='1' then profit end ) as daily_profit,             --日配毛利额
    sum(case when business_type_code='1' then last_profit end ) as last_daily_profit,   --环期日配毛利额
    count(distinct case when sales_value>0 and business_type_code='1' then customer_no end ) as daily_cust_number, --日配成交客户数
    count(distinct case when last_sales_value>0 and business_type_code='1' then customer_no end )as last_daily_cust_number,  --环比日配冻品成交客户数
    grouping__id
from csx_tmp.temp_sale_all a
-- where business_type_code!='4'  --剔除城市服务商
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
grouping sets
((channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name),     --明细
    (channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name),   --城市中类合计
    (channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name),   --城市大类合计
    (channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name),      --城市组合计明细
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name), 
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name),
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name),--
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name),
    (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
     (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name),
     (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name),
    (channel_name,
    region_code,
    region_name),
    (channel_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
    (channel_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name),
    (channel_name,
    classify_large_code,
    classify_large_name ),
    ()
)
;

   
-- 计算日配客户数
drop table if exists  csx_tmp.temp_sale_cust;
create  temporary table csx_tmp.temp_sale_cust as 
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    count(distinct case when daily_sales_value>0 then customer_no end) as b_daily_cust_number,
    count(distinct case when last_daily_sales_value>0 then customer_no end ) as last_b_daily_cust_number,
    sum(daily_sales_value) as daily_sales_value,
    sum(last_daily_sales_value) as last_daily_sales_value,
    grouping__id
from
(
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_no,
    sum(sales_value) as daily_sales_value,
    sum(last_sales_value) as last_daily_sales_value
from csx_tmp.temp_sale_all a
where business_type_code='1'
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_no
) a
group by 
    channel_name,
    region_code,
    region_name,
    a.city_group_code,
    a.city_group_name,
    province_code,
    province_name
grouping sets
    (
    (channel_name,
     region_code,
    region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name),
    (channel_name,
     region_code,
    region_name,
    province_code,
    province_name),
    (channel_name,
    region_code,
    region_name),
    ())
;


--- 计算30日冻品销售额 
drop table if exists csx_tmp.temp_sale_30day;
create temporary table csx_tmp.temp_sale_30day as 
select 
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(a.sales_qty  ) as sales_qty_30day,
    sum(a.sales_value ) as sales_value_30day,
    sum(a.profit) as  profit_30day,
    grouping__id
from csx_dw.dws_sale_r_d_detail a 
where sdt>${hiveconf:s_dt_30} 
    and sdt<=${hiveconf:e_dt}
   -- and a.business_type_code !='4'  --剔除城市服务商
    and a.channel_code  in ('1','7','9')
   -- and classify_middle_code  in('B0304','B0305')
group by province_code,
    province_name,
    region_code,
    region_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
grouping sets
    ((
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name),     --明细
        (
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name),   --城市中类合计
    (
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name),   --城市中类合计
        (
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name),      --城市组合计明细
    (region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
    (region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name ),
    (region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name ),
     (
     region_code,
     region_name,
     province_code,
     province_name),
     (region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
     (region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name ),
     (region_code,
    region_name,
    classify_large_code,
    classify_large_name ),
     (
     region_code,
     region_name ),
    (
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
    (
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name ),
    (
    classify_large_code,
    classify_large_name ),
    ()
    );


-- 统计期末库存 01大客户物流 07 BBC物流
drop table if exists csx_tmp.temp_sale_02;
create temporary table csx_tmp.temp_sale_02 as
select zone_id as sales_region_code,
    dist_code,
    city_group_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sum(a.qty) final_qty,
    sum(a.amt) final_amt,
    grouping__id
from csx_dw.dws_wms_r_d_accounting_stock_m a
    join 
(select 
    sales_province_code as dist_code,
    sales_province_name,
    city_group_code,
    city_group_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end zone_id,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市' 
    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    ) b on a.dc_code = b.shop_id
where sdt = ${hiveconf:e_dt}
   -- and classify_middle_code  in('B0304','B0305')
    and reservoir_area_code not in ('PD01', 'PD02', 'TS01')
    and zone_id!='9'
    and shop_id not in ('W0J8','W0K4')
    and purpose in ('01', '07')
    and a.sys='new'
group by zone_id,
    dist_code,
    city_group_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code 
grouping sets (
    (
            zone_id,
            dist_code,
            city_group_code,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
        (
            zone_id,
            dist_code,
            city_group_code,
            classify_large_code,
            classify_middle_code
        ),
    (
            zone_id,
            dist_code,
            city_group_code,
            classify_large_code
        ),
    (
            zone_id,
            dist_code,
            city_group_code
        ),
        (
            zone_id,
            dist_code,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
         (
            zone_id,
            dist_code,
            classify_large_code,
            classify_middle_code
        ),
         (
            zone_id,
            dist_code,
            classify_large_code
        ),
        (zone_id, 
        dist_code),
        (
            zone_id,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
         (
            zone_id,
            classify_large_code,
            classify_middle_code
        ),
         (
            zone_id,
            classify_large_code
        ),
        (zone_id),
        (
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
          (
            classify_large_code,
            classify_middle_code
        ),
          (
            classify_large_code
        ),
    ()
    );


--写入明细层

drop table if exists csx_tmp.temp_all_sale;
create temporary table csx_tmp.temp_all_sale as 
select a.channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sales_value,                                        --本期销售额
    profit ,                                            --本期毛利额
    last_sales_value,                                   --环比销售额
    last_profit,                                        --环比毛利额
    daily_cust_number,                                  --日配成交客户数
    last_daily_cust_number,                             --环比日配成交客户数
    b.b_daily_cust_number,                                --B端本期客户数
    b.last_b_daily_cust_number,                           --B端环期客户数
    c.all_sales_value,                                  --省区销售额
    all_profit,                                         --省区毛利额
    all_profit/all_sales_value as all_profit_rate,      --省区毛利率
    a.sales_value/all_sales_value as frozen_sales_ratio, --品类销售/省区销售占比
    coalesce((sales_value-last_sales_value)/last_sales_value,0) as ring_sales_ratio , --销售环比增长率
    profit/sales_value as frozen_profit_rate,           -- 定价毛利率
    profit/sales_value-last_profit/last_sales_value as  diff_profit_rate,     --毛利率差
    daily_cust_number/b_daily_cust_number as daily_cust_penetration_rate ,   ---日配业务渗透率
    last_daily_cust_number/last_b_daily_cust_number as last_daily_cust_penetration_rate ,   ---环期日配业务渗透率
    coalesce(daily_cust_number/b_daily_cust_number-last_daily_cust_number/last_b_daily_cust_number,0) as diff_daily_cust_penetration_rate ,   --- 日配业务渗透率环比
    a.daily_sales_value/b.daily_sales_value as daily_sales_ratio,   --日配业务销售额/省区日配占比
    a.last_daily_sales_value/b.last_daily_sales_value as last_daily_sales_ratio,    --日配业务销售额/省区日配占比
    a.daily_sales_value,
    a.daily_profit,
    a.last_daily_sales_value,
    a.last_daily_profit,
    b.daily_sales_value as prov_daily_sales_value,
    b.last_daily_sales_value as last_prov_daily_sales_value,
    a.grouping__id
from csx_tmp.temp_sale_all_01 a 
left join 
(select * from csx_tmp.temp_sale_cust ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'')
    and coalesce(a.region_code,'')=coalesce(b.region_code ,'')
    and coalesce(a.city_group_code,'')=coalesce(b.city_group_code ,'')
  --  and coalesce(a.channel_name,'')=coalesce(b.channel_name ,'')
left join 
(select region_code,province_code,
    city_group_code,
    channel_name,
    sales_value as all_sales_value,
    profit as all_profit 
    from csx_tmp.temp_sale_all_01
    where grouping__id in ('0','7','31'))c on coalesce(a.province_code,'')=coalesce(c.province_code,'') 
    and  coalesce(a.region_code,'')=coalesce(c.region_code ,'')
    and coalesce(a.city_group_code,'')=coalesce(c.city_group_code ,'')
    -- and  coalesce(a.channel_name,'')=coalesce(c.channel_name ,'')
where 1=1 
-- or a.grouping__id in ('0','7','31') )
;
 
insert overwrite table csx_tmp.report_sale_r_d_classify_ratio_fr partition(months) 
select
    case when a.grouping__id = '0' then '0'
        when a.grouping__id = '2017' then '1' 
        when a.grouping__id = '7' then '2' 
        when a.grouping__id='2023' then '3' 
        when a.grouping__id='31' then '4' 
    else '5' end level_id,  --分组：0 全国，1 全国管理分类，2 大区，3大区管理分类 4省区，5省区管分类
    substr(${hiveconf:e_dt} ,1,4) as years,
    substr(${hiveconf:e_dt} ,1,6) as smonth,
    coalesce(a.channel_name,'B端')as channel_name,
    coalesce(a.region_code,'00')as region_code,
    coalesce(a.region_name,'全国')as region_name,
    coalesce(a.province_code,'00') as  province_code,
    coalesce(a.province_name,'小计') as province_name,
    coalesce(a.city_group_code,'00')city_group_code,
    coalesce(a.city_group_name,'小计')city_group_name,
    coalesce(a.classify_large_code,'00') as  classify_large_code,
    coalesce(a.classify_large_name,'小计') as  classify_large_name,
    coalesce(a.classify_middle_code,'00') as  classify_middle_code,
    coalesce(a.classify_middle_name,'小计') as  classify_middle_name,
    coalesce(a.classify_small_code,'00') as classify_small_code,
    coalesce(a.classify_small_name,'小计') as classify_small_name,
    sales_value,                                        --本期销售额
    profit ,                                            --本期毛利额
    profit/abs(sales_value) as profit_rate,           -- 定价毛利率
    a.daily_sales_value,
    a.daily_profit,
    a.daily_profit/abs(a.daily_sales_value ) as daily_profit_rate,  --日配毛利率
    last_sales_value,                                   --环比销售额
    last_profit,                                       --环比毛利额
    last_profit/abs(last_sales_value) as last_profit_rate ,  --环比毛利率
    a.last_daily_sales_value,
    a.last_daily_profit,
    a.last_daily_profit/abs(a.last_daily_sales_value) as last_daily_profit_rate,    --环比日配毛利率
    coalesce((sales_value-last_sales_value)/last_sales_value,0) as ring_B_classify_sales_rate , --B端销售额环比增长率
    coalesce((daily_sales_value-last_daily_sales_value)/last_daily_sales_value,0) as ring_daily_sales_rate , --B端销售额环比增长率
    profit/sales_value-last_profit/last_sales_value as  diff_profit_rate,     --B端销售额环比增长率
    a.daily_profit/abs(a.daily_sales_value )-a.last_daily_profit/abs(a.last_daily_sales_value) as diff_daily_profit_rate ,-- 日配销售额环比增长率
    all_sales_value,                                  --省区销售额
    all_profit,                                         --省区毛利额
    all_profit/all_sales_value as all_profit_rate,      --省区毛利率
    a.sales_value/all_sales_value as class_sales_ratio, --品类销售/省区销售占比
    prov_daily_sales_value,
    last_prov_daily_sales_value,
    b.sales_qty_30day,  --滚动30天销量
    b.sales_value_30day ,      --滚动30天销售额
    b.profit_30day,
    final_qty,      --期末库存量
    final_amt ,     --期末库存额
    daily_cust_number,                                  --日配成交客户数
    last_daily_cust_number,                             --环比日配成交客户数
    b_daily_cust_number,                                --B端本期客户数
    last_b_daily_cust_number,                           --B端环期客户数
    IF(daily_cust_number/b_daily_cust_number>=1,1,daily_cust_number/b_daily_cust_number) as daily_cust_penetration_rate ,   ---日配业务渗透率
    IF(last_daily_cust_number/last_b_daily_cust_number>=1,1,last_daily_cust_number/last_b_daily_cust_number) as last_daily_cust_penetration_rate ,   ---环期日配业务渗透率
    coalesce(IF(daily_cust_number/b_daily_cust_number>=1,1,daily_cust_number/b_daily_cust_number)- IF(last_daily_cust_number/last_b_daily_cust_number>=1,1,last_daily_cust_number/last_b_daily_cust_number),0) as diff_daily_cust_penetration_rate ,   --- 日配业务渗透率环比
    daily_sales_value/prov_daily_sales_value as daily_sales_ratio,   --日配业务销售额/省区日配占比
    last_daily_sales_value/last_prov_daily_sales_value as last_daily_sales_ratio ,   --环期日配业务销售额/省区日配占比
     daily_sales_value/prov_daily_sales_value-last_daily_sales_value/last_prov_daily_sales_value as  diff_daily_sales_ratio ,
    a.grouping__id,
    current_timestamp(),
    substr(${hiveconf:e_dt} ,1,6) 
from csx_tmp.temp_all_sale  a
left join csx_tmp.temp_sale_30day b on coalesce(a.province_code,'')= coalesce(b.province_code,'') 
        and coalesce(a.classify_small_code,'')=coalesce(b.classify_small_code,'') 
        and  coalesce(a.region_code,'')=coalesce(b.region_code ,'')
        and coalesce(a.classify_middle_code,'')=coalesce(b.classify_middle_code,'') 
        and coalesce(a.classify_large_code,'')=coalesce(b.classify_large_code,'') 
        and coalesce(a.city_group_code,'')=coalesce(b.city_group_code ,'')
left join
(select sales_region_code,
    dist_code,
    city_group_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    final_qty,
    final_amt
from csx_tmp.temp_sale_02  ) d on coalesce(a.province_code,'')=coalesce(dist_code ,'')
and coalesce(a.classify_small_code,'')=coalesce(d.classify_small_code,'') 
and  coalesce(a.region_code,'')=coalesce(d.sales_region_code ,'')
and coalesce(a.classify_middle_code,'')=coalesce(d.classify_middle_code,'') 
and coalesce(a.classify_large_code,'')=coalesce(d.classify_large_code,'') 
and coalesce(a.city_group_code,'')=coalesce(d.city_group_code ,'')
;

drop table csx_tmp.REPORT_SALE_R_D_CLASSIFY_RATIO_FR;
CREATE TABLE `csx_tmp.report_sale_r_d_classify_ratio_fr`(
  `level_id` string, 
  `years` string comment '年份', 
  `smonth` string comment '月份', 
  `channel_name` string comment'渠道', 
  `region_code` string comment'大区', 
  `region_name` string comment'大区名称', 
  `province_code` string comment '省区', 
  `province_name` string comment '省区', 
  `city_group_code` string comment '城市组', 
  `city_group_name` string comment '城市组', 
  `classify_large_code` string comment '管理一级分类', 
  `classify_large_name` string comment '管理一级分类', 
  `classify_middle_code` string comment '管理二级分类', 
  `classify_middle_name` string comment '管理二级分类', 
  `classify_small_code` string comment '管理三级分类', 
  `classify_small_name` string comment '管理三级分类', 
  `sales_value` decimal(38,6) comment '销售额', 
  `profit` decimal(38,6) comment '毛利额', 
  `profit_rate` decimal(38,6) comment '毛利率', 
  `daily_sales_value` decimal(38,6) comment '日配销售额', 
  `daily_profit` decimal(38,6) comment '日配毛利额', 
  `daily_profit_rate` decimal(38,6) comment '日配毛利率', 
  `last_sales_value` decimal(38,6) comment '环比日配销售额', 
  `last_profit` decimal(38,6) comment '环比日配毛利额', 
  `last_profit_rate` decimal(38,6) comment '环比毛利率', 
  `last_daily_sales_value` decimal(38,6) comment '环期日配销售额', 
  `last_daily_profit` decimal(38,6) comment '毛利额', 
  `last_daily_profit_rate` decimal(38,6)comment'毛利率', 
  `ring_B_sales_rate` decimal(38,6) comment 'B端销售额环比增长率', 
  `ring_daily_sales_rate` decimal(38,6) comment '日配销售额环比增长率', 
  `diff_profit_rate` decimal(38,6)comment '定价毛利率差', 
  `diff_daily_profit_rate` decimal(38,6)comment'日配毛利率差', 
  `all_sales_value` decimal(38,6) comment'B端销售额', 
  `all_profit` decimal(38,6) comment 'B端毛利额', 
  `all_profit_rate` decimal(38,6) comment 'B端毛利率', 
  `classify_sales_ratio` decimal(38,6) comment'管理类别销售占比', 
  `prov_daily_sales_value` decimal(38,6) comment '省区销售额', 
  `last_prov_daily_sales_value` decimal(38,6) comment'环期省区销售额', 
  `sales_qty_30day` decimal(30,6) comment '近30天销售量', 
  `sales_value_30day` decimal(30,6)comment'近30天销售额', 
  `profit_30day` decimal(30,6) comment '近30天毛利额', 
  `final_qty` decimal(30,6) comment '期末库存量', 
  `final_amt` decimal(30,6) comment '期末库存额', 
  `daily_cust_number` bigint comment '日配成交客户数', 
  `last_daily_cust_number` bigint comment '环期日配成交客户数', 
  `b_daily_cust_number` bigint comment 'B端日配客户', 
  `last_b_daily_cust_number` decimal(30,6) comment '环期B端日配客户', 
  `daily_cust_penetration_rate` decimal(30,6) comment '日配渗透率', 
  `last_daily_cust_penetration_rate` decimal(30,6) comment '环期日配渗透率', 
  `diff_daily_cust_penetration_rate` decimal(30,6) comment '日配渗透率差', 
  `daily_sales_ratio` decimal(38,6) comment '日配销售占比', 
  `last_daily_sales_ratio` decimal(38,6) comment '环比销售占比',
  `diff_daily_ratio_rate` decimal(30,6) comment '日配销售占比差', 
  `grouping__id` string, 
  `update_time` timestamp
)comment 'B端管理品类销售分析&日配渗透率分析'
partitioned by (months string comment '销售月分区')
STORED AS parquet 
  