set  mapreduce.job.reduces = 100;
-- set  hive.map.aggr = true;
-- set  hive.groupby.skewindata = false;
set  hive.exec.parallel = true;
set  hive.exec.dynamic.partition = true;
--启动态分区
set  hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
set  hive.exec.max.dynamic.partitions = 10000;
--在所有执行mr的节点上，最大一共可以创建多少个动态分区。
set  hive.exec.max.dynamic.partitions.pernode = 100000;
--源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错

--每个Map最大输入大小(这个值决定了合并后文件的数量)  
set mapred.max.split.size=256000000;    
--一个节点上split的至少的大小(这个值决定了多个DataNode上的文件是否需要合并)  
set mapred.min.split.size.per.node=100000000;  
--一个交换机下split的至少的大小(这个值决定了多个交换机上的文件是否需要合并)    
set mapred.min.split.size.per.rack=100000000;  
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
-- 本期数据 (不含合伙人 purpose!='06')

drop table if exists csx_tmp.tmp_dp_sale;
create temporary table csx_tmp.tmp_dp_sale
as 
select 
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.customer_no,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sum(sales_value)as sales_value,
    sum(a.profit)as profit,
    sum(case when classify_middle_code in('B0304','B0305') then a.sales_value end ) as frozen_sales,
    sum(case when classify_middle_code  in('B0304','B0305') then a.profit end ) as frozen_profit,
    sum(case when business_type_code='1' and classify_middle_code  in('B0304','B0305') then a.sales_value end ) as frozen_daily_sales,
    sum(case when business_type_code='1' and classify_middle_code  in('B0304','B0305') then a.profit end ) as frozen_daily_profit
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
    a.customer_no,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sum(sales_value)as last_sales_value,
    sum(a.profit)as last_profit,
    sum(case when classify_middle_code  in('B0304','B0305') then a.sales_value end ) as last_frozen_sales,
    sum(case when classify_middle_code  in('B0304','B0305') then a.profit end ) as last_frozen_profit,
    sum(case when a.business_type_code='1' and  classify_middle_code  in('B0304','B0305') then a.sales_value end ) as last_frozen_daily_sales,
    sum(case when a.business_type_code='1' and  classify_middle_code  in('B0304','B0305') then a.profit end ) as last_frozen_daily_profit
from csx_dw.dws_sale_r_d_detail a 
where sdt>=${hiveconf:last_sdt}
    and sdt<=${hiveconf:last_edt}
    -- and classify_middle_code='B0304'
    -- and business_type_code !='4'
    and a.channel_code   in ('1','7','9')
group by 
    case when channel_code in ('1','9','7') then 'B端' end,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
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
    sum(frozen_sales) as frozen_sales,
    sum(frozen_profit) as frozen_profit,
    sum(frozen_daily_sales) as frozen_daily_sales,
    sum(frozen_daily_profit) as frozen_daily_profit ,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    sum(last_frozen_sales) as last_frozen_sales,
    sum(last_frozen_profit) as last_frozen_profit,
    sum(last_frozen_daily_sales) as last_frozen_daily_sales,
    sum(last_frozen_daily_profit) as last_frozen_daily_profit
from
(select channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
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
    frozen_sales,
    frozen_profit,
    frozen_daily_sales,
    frozen_daily_profit,
    0 as last_sales_value,
    0 as last_profit,
    0 as last_frozen_sales,
    0 as last_frozen_profit,
    0 as last_frozen_daily_sales,
    0 as last_frozen_daily_profit
from csx_tmp.tmp_dp_sale a
union all
select channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
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
    0 as frozen_sales,
    0 as frozen_profit,
    0 as frozen_daily_sales,
    0 as frozen_daily_profit,
    last_sales_value  ,
    last_profit,
    last_frozen_sales,
    last_frozen_profit,
    last_frozen_daily_sales,
    last_frozen_daily_profit
from csx_tmp.tmp_dp_sale_01 a
) a
group by 
    channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    customer_no,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name

    ; 
    


-- 本期与环比汇总层级汇总 不含合伙人数据
drop table if exists csx_tmp.temp_sale_all_01;
create temporary table csx_tmp.temp_sale_all_01 as 
select
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_value) as sales_value,
    sum(profit) as profit ,
    sum(frozen_sales) as frozen_sales,
    sum(frozen_profit) as frozen_profit,
    sum(frozen_daily_sales) as frozen_daily_sales,
    sum(frozen_daily_profit) as frozen_daily_profit ,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    sum(last_frozen_sales) as last_frozen_sales,
    sum(last_frozen_profit) as last_frozen_profit,
    sum(last_frozen_daily_sales) as last_frozen_daily_sales,
    sum(last_frozen_daily_profit) as last_frozen_daily_profit,
    count(distinct case when frozen_daily_sales>0 then customer_no end ) as daily_cust_number, --日配成交数
    count(distinct case when last_frozen_sales>0 then customer_no end )as last_daily_cust_number,  --环比日配冻品成交数
    grouping__id
from csx_tmp.temp_sale_all a
-- where business_type_code!='4'  --剔除城市服务商
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
grouping sets
((channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),     --
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
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),
    (channel_name,
    region_code,
    region_name),
    (channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),
    ()
)
;

   
-- 计算日配数
drop table if exists  csx_tmp.temp_sale_cust;
create  temporary table csx_tmp.temp_sale_cust as 
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    count(case when daily_sales_value>0 then customer_no end) as sales_cust_number,
    count(case when last_daily_sales_value>0 then customer_no end ) as last_sales_cust_number,
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
    customer_no,
    sum(sales_value) as daily_sales_value,
    sum(last_sales_value) as last_daily_sales_value
from csx_tmp.temp_sale_all 
where business_type_code='1'
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    customer_no
) a
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name
grouping sets
    ((channel_name,
     region_code,
    region_name,
    province_code,
    province_name),
    (channel_name,
    region_code,
    region_name),
    ())
;


--- 计算30日冻品销售额 剔除合伙人
drop table if exists csx_tmp.temp_sale_30day;
create temporary table csx_tmp.temp_sale_30day as 
select 
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(a.sales_qty  ) as frozen_sales_qty_30day,
    sum(a.sales_value ) as frozen_sales_30day,
    sum(a.profit) as frozen_profit_day,
    grouping__id
from csx_dw.dws_sale_r_d_detail a 
where sdt>${hiveconf:s_dt_30} 
    and sdt<=${hiveconf:e_dt}
   -- and a.business_type_code !='4'  --剔除城市服务商
    and a.channel_code  in ('1','7','9')
    and classify_middle_code  in('B0304','B0305')
group by province_code,
    province_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
grouping sets
    (
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
     (
     region_code,
     region_name ),
    (
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name), ());


-- 统计期末库存
drop table if exists csx_tmp.temp_sale_02;
create temporary table csx_tmp.temp_sale_02 as
select zone_id as sales_region_code,
    dist_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sum(a.qty) final_qty,
    sum(a.amt) final_amt,
    grouping__id
from csx_dw.dws_wms_r_d_accounting_stock_m a
    join (
        select goods_id,
            classify_small_code,
            classify_large_code,
            classify_middle_code
        from csx_dw.dws_basic_w_a_csx_product_m
        where sdt = 'current'
            and classify_middle_code  in('B0304','B0305')
    ) c on a.goods_code = c.goods_id
    join (
        select location_code as shop_id,
            zone_id,
            dist_code
        from csx_dw.csx_shop
        where sdt = 'current'
            and purpose_code in ('01', '03', '07','06')
            and table_type = 1
    ) b on a.dc_code = b.shop_id
where sdt = ${hiveconf:e_dt}
    and reservoir_area_code not in ('PD01', 'PD02', 'TS01')
group by zone_id,
    dist_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code 
grouping sets (
        (
            zone_id,
            dist_code,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
        (zone_id, dist_code),
        (
            zone_id,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
        (zone_id),
        (
            classify_large_code,
            classify_middle_code,
            classify_small_code
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
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sales_value,
    profit ,
    frozen_sales,
    frozen_profit,
    frozen_daily_sales,
    frozen_daily_profit ,
    last_sales_value,
    last_profit,
    last_frozen_sales,
    last_frozen_profit,
    last_frozen_daily_sales,
    last_frozen_daily_profit,
    daily_cust_number,                                  --日配成交数
    last_daily_cust_number, 
    b.sales_cust_number,
    b.last_sales_cust_number,
    c.all_sales_value,
    all_profit,
    all_profit/all_sales_value as all_profit_rate,
    a.frozen_sales/all_sales_value as frozen_sales_ratio, -- 冻品销售/省区占比
    coalesce((frozen_sales-last_frozen_sales)/last_frozen_sales,0) as frozen_ring_sales_ratio , --销售环比增长率
    frozen_profit/frozen_sales as frozen_profit_rate,  --冻品定价毛利率
    frozen_profit/frozen_sales-last_profit/last_sales_value as frozen_diff_profit_rate,     --毛利率差
    daily_cust_number/sales_cust_number as daily_cust_penetration_rate ,   ---日配业务渗透率
    last_daily_cust_number/last_sales_cust_number as last_daily_cust_penetration_rate ,   ---环期日配业务渗透率
    coalesce(daily_cust_number/sales_cust_number-last_daily_cust_number/last_sales_cust_number,0) as diff_daily_cust_penetration_rate ,   --- 日配业务渗透率环比
    frozen_daily_sales/daily_sales_value as daily_sales_ratio,   --日配业务销售额/省区日配占比
    last_frozen_sales/last_daily_sales_value as last_daily_sales_ratio,    --日配业务销售额/省区日配占比
    daily_sales_value,
    last_daily_sales_value,
    a.grouping__id
from csx_tmp.temp_sale_all_01 a 
left join 
(select * from csx_tmp.temp_sale_cust ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'') and coalesce(a.region_code,'')=coalesce(b.region_code ,'')
left join 
(select region_code,province_code,channel_name,sales_value as all_sales_value,profit as all_profit 
    from csx_tmp.temp_sale_all_01
    where grouping__id in ('0','7','31'))c on coalesce(a.province_code,'')=coalesce(c.province_code,'') and  coalesce(a.region_code,'')=coalesce(c.region_code ,'')
where (classify_middle_code  in('B0304','B0305') or a.grouping__id in ('0','7','31') )
;



-- set hive.exec.dynamic.partition.mode=nonstrict;



insert overwrite table csx_tmp.ads_sale_r_d_frozen_fr partition(months)
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
    coalesce(a.classify_large_code,'00') as  classify_large_code,
    coalesce(a.classify_large_name,'小计') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'') as classify_small_code,
    coalesce(a.classify_small_name,'') as classify_small_name,
    sales_value,
    profit ,
    frozen_sales,
    frozen_profit,
    frozen_profit/frozen_sales as frozen_profit_rate,
    frozen_daily_sales,
    frozen_daily_profit ,
    frozen_daily_profit /frozen_daily_sales as frozen_daily_profit_rate,
    last_sales_value,
    last_profit,
   -- last_profit/last_sales_value as last_profit_rate,  --环期毛利率
    last_frozen_sales,
    last_frozen_profit,
    last_frozen_profit/last_frozen_sales as last_frozen_profit_rate,  --环期毛利率
    last_frozen_daily_sales,
    last_frozen_daily_profit,
    last_frozen_daily_profit / last_frozen_daily_sales as last_frozen_daily_profit_rate, --环期冻品日配业务毛利率
    daily_cust_number,                                  --日配成交数
    last_daily_cust_number, 
    sales_cust_number,                                  --省区成交数
    last_sales_cust_number,                             --环期省区成交
    all_sales_value,                                    --省区销售额/全国销售额
    all_profit,                                         --省区毛利额/全国毛利额
    all_profit/all_sales_value as all_profit_rate,      --省区毛利率/全国毛利率
    a.frozen_sales/all_sales_value as frozen_sales_ratio, -- 冻品销售/省区占比
    coalesce((frozen_sales-last_frozen_sales)/last_frozen_sales,0) as frozen_ring_sales_rate , --冻品销售环比增长率
    (frozen_profit/frozen_sales)-(last_frozen_profit/last_frozen_sales)as frozen_diff_profit_rate,     --冻品毛利率差
    daily_cust_number/sales_cust_number as daily_cust_penetration_rate ,   -- 日配业务渗透率
    last_daily_cust_number/last_sales_cust_number as last_daily_cust_penetration_rate ,   -- 环期日配业务渗透率
    coalesce(daily_cust_number/sales_cust_number-last_daily_cust_number/last_sales_cust_number,0) as diff_daily_cust_penetration_rate ,   -- 日配业务渗透率环比
    frozen_daily_sales/daily_sales_value as daily_sales_ratio,   --日配业务销售额/省区日配占比
    last_frozen_sales/last_daily_sales_value as last_daily_sales_ratio,    --环期日配业务销售额/省区日配占比
    b.frozen_sales_qty_30day,  --滚动30天销量
    b.frozen_sales_30day ,      --滚动30天销售额    
    final_qty,      --期末库存量
    final_amt ,     --期末库存额
    a.grouping__id,
    current_timestamp(),
    substr(${hiveconf:e_dt} ,1,6) 
from csx_tmp.temp_all_sale  a
left join csx_tmp.temp_sale_30day b on coalesce(a.province_code,'')= coalesce(b.province_code,'') 
and coalesce(a.classify_small_code,'')=coalesce(b.classify_small_code,'') and  coalesce(a.region_code,'')=coalesce(b.region_code ,'')
left join csx_tmp.temp_sale_02 d on coalesce(a.province_code,'')=coalesce(dist_code ,'')
and coalesce(a.classify_small_code,'')=coalesce(d.classify_small_code,'')
and  coalesce(a.region_code,'')=coalesce(d.sales_region_code,'')
;