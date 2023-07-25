set  mapreduce.job.reduces = 100;
set hive.exec.reducers.max=299;  -- reduce 个数，默认 299 不建议
-- set  hive.map.aggr = true;
-- set  hive.groupby.skewindata = false;
set hive.exec.parallel.thread.number = 16;
set  hive.exec.parallel = true;
set  hive.exec.dynamic.partition = true;
set hive.merge.size.per.task=128000000;  --合并后的文件大小 默认 128M
set hive.merge.smallfiles.avgsize=6400000; --平均最小文件大小合并 默认 64M
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
set mapred.min.split.size.per.node=64000000;  
--一个交换机下split的至少的大小(这个值决定了多个交换机上的文件是否需要合并)    
set mapred.min.split.size.per.rack=64000000;  
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
-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;
-- 本期数据 (不含合伙人 purpose!='06')

drop table if exists csx_tmp.tmp_sale_frozen_01;
create temporary table csx_tmp.tmp_sale_frozen_01
as 
select 
    channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.customer_no,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.business_type_code,
    sum(sales_qty) as  sales_qty,
    sum(sales_value ) as  sales_value,
    sum(profit)as  profit,
    sum(last_sales_qty) as last_sales_qty,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit
   from ( 
    select  
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.customer_no,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.business_type_code,
    sales_qty,
    a.sales_value,
    a.profit,
    0 last_sales_qty,
    0 last_sales_value,
    0 last_profit
    from csx_dw.dws_sale_r_d_detail a 
    where sdt<=${hiveconf:e_dt} and sdt>=${hiveconf:s_dt} 
    --  and a.business_type_code !='4'
    and a.channel_code  in ('1','7','9')
    -- and a.classify_middle_code='B0304'
    union all 
     select  
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.customer_no,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.business_type_code,
    0 as sales_qty,
    0 as sales_value,
    0 as profit,
    sales_qty as  last_sales_qty,
    sales_value as  last_sales_value,
    profit as  last_profit
    from csx_dw.dws_sale_r_d_detail a 
    where sdt<=${hiveconf:last_edt} and sdt>=${hiveconf:last_sdt} 
    and a.channel_code  in ('1','7','9')
     --  and a.business_type_code !='4'
)    a 
where business_type_code!='4'
group by 
    channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.province_code,
    a.province_name,
    first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.customer_no,
    business_type_code
;
-- select * from csx_tmp.tmp_sale_frozen_02;
-- select * from csx_tmp.tmp_sale_frozen_02 where region_code='3' and first_category_code is null   ;
-- 日配行业销售额/成交数
drop table if exists csx_tmp.tmp_sale_frozen_02;
create temporary table csx_tmp.tmp_sale_frozen_02 as 
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
    sum(sales_qty) as sales_qty,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(last_sales_qty) as last_sales_qty,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    sum(case when business_type_code='1' then  sales_value end ) as daily_sales_value,                      -- 日配销售额
    sum(case when business_type_code='1' then  profit end ) as daily_profit,                      -- 日配销售额
    sum(case when business_type_code='1' then  last_sales_value end ) as last_daily_sales_value,            -- 环期日配销售额
    sum(case when business_type_code='1' then  last_profit end ) as last_daily_profit,            -- 环期日配销售额
    count(distinct case when sales_value>0  and business_type_code='1' then customer_no end ) as sales_cust_number, --日配成交数
    count(distinct case when last_sales_value>0 and business_type_code='1' then customer_no end )as last_sales_cust_number,  --日配环比冻品成交数
    grouping__id
from 
(select
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
    a.customer_no,
    a.business_type_code,
    sum(sales_qty) as sales_qty,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(last_sales_qty) as last_sales_qty,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit
  from    csx_tmp.tmp_sale_frozen_01 a
    where classify_middle_code in('B0304','B0305')
   -- and a.business_type_code='1'
   -- and a.region_code='3'
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
    a.classify_small_name,
    customer_no,
    business_type_code
    ) a
    where  1=1
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
    a.classify_small_name),     --明细
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name),         --省区中类汇总
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name
    ),  --	511 省区汇总
    (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),  --32743 大区小类
    (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name),  --32263 大区中类汇总
    (channel_name,
    region_code,
    region_name),  --7 大区汇总
    (channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name), -- 32737 全国小类 汇总
    (channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name), -- 8161 全国中类 汇总
    ()   --0 
)
;

--select * from csx_tmp.tmp_sale_frozen_03;
   
-- 日配总数
drop table if exists  csx_tmp.tmp_sale_frozen_03;
create  temporary table csx_tmp.tmp_sale_frozen_03 as 
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    second_category_code,
    count(distinct  case when b_sales_value>0  and business_type_code='1' then customer_no end) as b_daily_cust_number,
    count( distinct case when last_b_sales_value>0 and business_type_code='1' then customer_no end ) as last_b_daily_cust_number,
    -- sum(frozen_sales_qty) as frozen_sales_qty,
    -- sum(frozen_sales) as frozen_sales,
    -- sum(frozen_profit) as frozen_profit,
    sum(b_sales_value) as  b_sales_value,
    sum(b_profit) as b_profit,
    sum(last_b_sales_value) as last_b_sales_value,
    sum(last_b_profit) as last_b_profit,
    sum(case when business_type_code='1' then b_sales_value end ) as B_daily_sale ,
    sum(case when business_type_code='1' then b_profit end ) as B_daily_profit ,
    sum(case when business_type_code='1' then last_b_sales_value end ) as last_B_daily_sale ,
    sum(case when business_type_code='1' then last_b_profit end ) as last_B_daily_profit ,
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
    second_category_code,
    business_type_code,
    -- sum(case when classify_middle_code in('B0304','B0305') then sales_qty end) as frozen_sales_qty,
    -- sum(case when classify_middle_code in('B0304','B0305') then sales_value end) as frozen_sales,
    -- sum(case when classify_middle_code in('B0304','B0305') then profit end) as frozen_profit,
    sum(sales_value) as b_sales_value,
    sum(profit) as b_profit,
    sum(last_sales_value) as last_b_sales_value,
    sum(last_profit) as last_b_profit
from csx_tmp.tmp_sale_frozen_01
where 1=1
group by 
    channel_name,
    region_code,
    second_category_code,
    region_name,
    province_code,
    province_name,
    customer_no,
    business_type_code
) a
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    second_category_code
grouping sets
    ((channel_name,
     region_code,
    region_name,
    province_code,
    province_name,
    second_category_code),
    (channel_name,
    region_code,
    region_name,
    second_category_code),
    (second_category_code),
    ())
;


-- 小类销售额
drop table if exists  csx_tmp.tmp_sale_frozen_04 ;
create temporary  table csx_tmp.tmp_sale_frozen_04 as 
select  coalesce(a.channel_name,'')as channel_name,
    coalesce(a.region_code,'')as region_code,
    coalesce(a.region_name,'')as region_name,
    coalesce(a.province_code,'') as  province_code,
    coalesce(a.province_name,'') as province_name,
    coalesce(a.classify_large_code,'') as  classify_large_code,
    coalesce(a.classify_large_name,'') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'') as classify_small_code,
    coalesce(a.classify_small_name,'') as classify_small_name,
    coalesce(sales_qty,0) as small_sales_qty,
    coalesce(a.sales_value,0) as small_sales_value,
    coalesce(profit,0) as small_profit
    from 
(select channel_name,
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
    sum(sales_qty) as sales_qty,
    sum(sales_value) as sales_value,
    sum(profit) as profit
from csx_tmp.tmp_sale_frozen_01 a
group by 
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
    classify_small_name
grouping sets 
((channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),   --省区
    (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),    --大区小类 
    (channel_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name)    --全国小
    )
    ) a 
;




-- -- 行业中类销售额
-- drop table if exists  csx_tmp.tmp_sale_frozen_05 ;
-- create temporary  table csx_tmp.tmp_sale_frozen_05 as 
-- select  coalesce(a.channel_name,'')as channel_name,
--     coalesce(a.region_code,'')as region_code,
--     coalesce(a.region_name,'')as region_name,
--     coalesce(a.province_code,'') as  province_code,
--     coalesce(a.province_name,'') as province_name,
--     coalesce(first_category_code,'')first_category_code,
--     coalesce(first_category_name,'')first_category_name,
--     coalesce(second_category_code,'')second_category_code,
--     coalesce(second_category_name,'')second_category_name,
--     coalesce(a.classify_large_code,'') as  classify_large_code,
--     coalesce(a.classify_large_name,'') as  classify_large_name,
--     coalesce(a.classify_middle_code,'') as  classify_middle_code,
--     coalesce(a.classify_middle_name,'') as  classify_middle_name,
--     coalesce(a.classify_small_code,'') as classify_small_code,
--     coalesce(a.classify_small_name,'') as classify_small_name,
--     coalesce(sales_qty,0) as small_sales_qty,
--     coalesce(a.sales_value,0) as small_sales_value,
--     coalesce(profit,0) as small_profit
--     from 
-- (select channel_name,
--     region_code,
--     region_name,
--     province_code,
--     province_name,
--     first_category_code,
--     first_category_name,
--     second_category_code,
--     second_category_name,
--     classify_large_code,
--     classify_large_name,
--     classify_middle_code,
--     classify_middle_name,
--     classify_small_code,
--     classify_small_name,
--     sum(sales_qty) as sales_qty,
--     sum(sales_value) as sales_value,
--     sum(profit) as profit
-- from csx_tmp.tmp_sale_frozen_01 a
-- group by 
--     channel_name,
--     region_code,
--     region_name,
--     province_code,
--     province_name,
--     first_category_code,
--     first_category_name,
--     second_category_code,
--     second_category_name,
--     classify_large_code,
--     classify_large_name,
--     classify_middle_code,
--     classify_middle_name,
--     classify_small_code,
--     classify_small_name
-- grouping sets 
-- ((channel_name,
--     region_code,
--     region_name,
--     province_code,
--     province_name,
--     first_category_code,
--     first_category_name,
--     second_category_code,
--     second_category_name,
--     classify_large_code,
--     classify_large_name,
--     classify_middle_code,
--     classify_middle_name),   --省区
--     (channel_name,
--     region_code,
--     region_name,
--     first_category_code,
--     first_category_name,
--     second_category_code,
--     second_category_name,
--     classify_large_code,
--     classify_large_name,
--     classify_middle_code,
--     classify_middle_name),    --大区小类 
--     (channel_name,
--     first_category_code,
--     first_category_name,
--     second_category_code,
--     second_category_name,
--     classify_large_code,
--     classify_large_name,
--     classify_middle_code,
--     classify_middle_name),    --全国小类
--     ())
--     ) a 
-- ;

insert overwrite table  csx_tmp.report_sale_r_d_frozen_industry_new_fr  partition(months)
select
    case when a.region_code is null then 1 
        when a.province_code is null then 2
        when a.classify_small_name is null then 3
        else 4
    end level_id,  --分组：1 全国，2 大区，3 省区，4 行业汇总 5、小类汇总
    case when a.grouping__id in (7,31) then 0 
        when a.grouping__id in (487,511) then 1 
        when a.grouping__id in (8167,8191) then 2 
        when a.grouping__id in (32743,32767) then 3 
        else 4 end  asc_id,
    substr(${hiveconf:e_dt} ,1,4) as years,
    substr(${hiveconf:e_dt} ,1,6) as smonth,
    coalesce(a.channel_name,'B端')as channel_name,
    coalesce(a.region_code,'00')as region_code,
    coalesce(a.region_name,'全国')as region_name,
    coalesce(a.province_code,'00') as  province_code,
    coalesce(a.province_name,'小计') as province_name,
    coalesce(a.first_category_code,'') as first_category_code,
    coalesce(a.first_category_name,'') as first_category_name,
    coalesce(a.second_category_code,'') as second_category_code,
    coalesce(a.second_category_name,'') as second_category_name,
    coalesce(a.classify_large_code,'') as  classify_large_code,
    coalesce(a.classify_large_name,'') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'00') as classify_small_code,
    coalesce(a.classify_small_name,'小计') as classify_small_name,
    coalesce(sales_qty,0) as sales_qty,
    coalesce(a.sales_value,0) as sales_value,
    coalesce(profit,0) as profit,
    coalesce(last_sales_qty,0) as last_sales_qty,
    coalesce(a.last_sales_value,0) as last_sales_value,
    coalesce(last_profit,0) as last_profit,
    coalesce(small_sales_qty,  0) as small_sales_qty,                                   --小类销量
    coalesce(small_sales_value, 0) as small_sales_value,                                      --小类销售额
    small_profit ,                                                  --小类毛利额
    small_profit/small_sales_value as small_profit_rate,
    coalesce(sales_value/small_sales_value,0) as small_sales_ratio,    -- 小类销售/小类总销售占比
    coalesce(sales_qty/small_sales_qty,0) as small_sales_qty_ratio,    -- 小类销售量/小类总销售量占比
    case when coalesce(a.last_sales_value,0)=0 and coalesce(a.sales_value)>0 then 1 
        else coalesce((sales_value -coalesce(last_sales_value,0))/last_sales_value,0) 
    end as ring_sales_rate ,                                                                 --冻品、调理销售额环比增长率
    case when coalesce(last_sales_qty,0)=0 and coalesce(a.sales_qty)>0 then 1 
        else  coalesce((sales_qty-coalesce(last_sales_qty,0))/last_sales_qty,0)
    end as ring_sales_qty_rate ,                                                             --冻品、调理销售量环比增长率
    coalesce(profit/a.sales_value,0) as profit_rate,                                          --定价毛利率
    coalesce(profit/sales_value,0)- coalesce(last_profit/last_sales_value,0) as diff_profit_rate,     --毛利率差
    coalesce(a.daily_sales_value,0) as daily_sales_value,           --日配销售额
    coalesce(a.daily_profit,0) as daily_profit,                     --日配毛利额
    coalesce(a.last_daily_sales_value,0) as last_daily_sales_value, --环期日配销售额
    coalesce(a.last_daily_profit,0) as last_daily_profit,           --环期日配毛利额
    coalesce(B_daily_sale, 0) as B_daily_sale,                                     --日配行业销售额
    coalesce(B_daily_profit, 0) as B_daily_profit,                        --日配行业销售毛利
    coalesce(last_B_daily_sale, 0) as last_B_daily_sale,                               --环期日配销售额
    coalesce(last_B_daily_profit,0) as last_B_daily_profit,                                 --环期日配行业毛利额
    coalesce(sales_cust_number,    0) as sales_cust_number,                                  --日配成交数
    coalesce(last_sales_cust_number,0) as last_sales_cust_number,                             --日配环期成交数
    coalesce(b_daily_cust_number,0) as b_daily_cust_number,                                         --日配行业成交
    coalesce(last_b_daily_cust_number, 0) as last_b_daily_cust_number,                               --环期日配行业成交
    coalesce(sales_cust_number/b_daily_cust_number,0) as daily_cust_penetration_rate ,               --日配渗透率
    coalesce(last_sales_cust_number/last_b_daily_cust_number,0) as last_daily_cust_penetration_rate ,  --环期日配渗透率
    coalesce(sales_cust_number/b_daily_cust_number,0) - coalesce(last_sales_cust_number/last_b_daily_cust_number ,0)  as diff_daily_cust_penetration_rate ,   -- 日配渗透率环比
    coalesce( daily_sales_value/B_daily_sale,0) as daily_industry_sales_ratio,                                                                                -- 日配行业销售额/行业销售额
    coalesce( last_daily_sales_value/last_B_daily_sale,0) as last_daily_industry_sales_ratio,                                                                 -- 日配行业销售额/行业销售额
    coalesce( daily_sales_value/B_daily_sale,0) - coalesce(last_daily_sales_value/last_B_daily_sale,0)  as diff_daily_industry_sale_ratio,                         -- 日配行业销售占比环比差
    a.grouping__id,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from csx_tmp.tmp_sale_frozen_02 a 
left join 
(select * from csx_tmp.tmp_sale_frozen_03 ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'') 
and coalesce(a.region_code,'')=coalesce(b.region_code ,'') 
and  coalesce(a.second_category_code,'')=coalesce(b.second_category_code ,'')
left join 
 csx_tmp.tmp_sale_frozen_04   c  on coalesce(a.region_code,'')=c.region_code 
    and coalesce(a.province_code,'') = c.province_code
    and coalesce(a.classify_small_code,'')=c.classify_small_code
  where 1=1
   and a.grouping__id  in ('31','511','8191','8167','511','487','7')
union all 
select
    case when a.region_code is null then 1 
        when a.province_code is null then 2
        when a.classify_small_name is null then 3
        else 4
    end level_id,  --分组：0 全国，1 全国管理分类，2 大区，3大区管理分类 4省区，5省区管分类
    case when a.grouping__id=0 then 0 
        when a.grouping__id=481 then 1 
        when a.grouping__id=8161 then 2 
        when a.grouping__id = 32737 then 3 
        else 4 end  asc_id,
    substr(${hiveconf:e_dt} ,1,4) as years,
    substr(${hiveconf:e_dt} ,1,6) as smonth,
    coalesce(a.channel_name,'B端')as channel_name,
    coalesce(a.region_code,'00')as region_code,
    coalesce(a.region_name,'全国')as region_name,
    coalesce(a.province_code,'00') as  province_code,
    coalesce(a.province_name,'小计') as province_name,
    coalesce(a.first_category_code,'') as first_category_code,
    coalesce(a.first_category_name,'') as first_category_name,
    coalesce(a.second_category_code,'') as second_category_code,
    coalesce(a.second_category_name,'') as second_category_name,
    coalesce(a.classify_large_code,'') as  classify_large_code,
    coalesce(a.classify_large_name,'') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'00') as classify_small_code,
    coalesce(a.classify_small_name,'小计') as classify_small_name,
    coalesce(sales_qty,0) as sales_qty,
    coalesce(a.sales_value,0) as sales_value,
    coalesce(profit,0) as profit,
    coalesce(last_sales_qty,0) as last_sales_qty,
    coalesce(a.last_sales_value,0) as last_sales_value,
    coalesce(last_profit,0) as last_profit,
    coalesce(small_sales_qty,  0) as small_sales_qty,                                   --小类销量
    coalesce(small_sales_value, 0) as small_sales_value,                                      --小类销售额
    small_profit ,                                                  --小类毛利额
    small_profit/small_sales_value as small_profit_rate,
    coalesce(sales_value/small_sales_value,0) as small_sales_ratio,    -- 小类销售/小类总销售占比
    coalesce(sales_qty/small_sales_qty,0) as small_sales_qty_ratio,    -- 小类销售量/小类总销售量占比
    case when coalesce(a.last_sales_value,0)=0 and coalesce(a.sales_value)>0 then 1 
        else coalesce((sales_value -coalesce(last_sales_value,0))/last_sales_value,0) 
    end as ring_sales_rate ,                                                                 --冻品、调理销售额环比增长率
    case when coalesce(last_sales_qty,0)=0 and coalesce(a.sales_qty)>0 then 1 
        else  coalesce((sales_qty-coalesce(last_sales_qty,0))/last_sales_qty,0)
    end as ring_sales_qty_rate ,                                                             --冻品、调理销售量环比增长率
    coalesce(profit/a.sales_value,0) as profit_rate,                                          --定价毛利率
    coalesce(profit/sales_value,0)- coalesce(last_profit/last_sales_value,0) as diff_profit_rate,     --毛利率差
    coalesce(a.daily_sales_value,0) as daily_sales_value,           --日配销售额
    coalesce(a.daily_profit,0) as daily_profit,                     --日配毛利额
    coalesce(a.last_daily_sales_value,0) as last_daily_sales_value, --环期日配销售额
    coalesce(a.last_daily_profit,0) as last_daily_profit,           --环期日配毛利额
    coalesce(B_daily_sale, 0) as B_daily_sale,                                     --日配行业销售额
    coalesce(B_daily_profit, 0) as B_daily_profit,                        --日配行业销售毛利
    coalesce(last_B_daily_sale, 0) as last_B_daily_sale,                               --环期日配销售额
    coalesce(last_B_daily_profit,0) as last_B_daily_profit,                                 --环期日配行业毛利额
    coalesce(sales_cust_number, 0) as sales_cust_number,                                  --日配成交数
    coalesce(last_sales_cust_number,0) as last_sales_cust_number,                             --日配环期成交数
    coalesce(b_daily_cust_number,0) as b_daily_cust_number,                                         --日配行业成交
    coalesce(last_b_daily_cust_number, 0) as last_b_daily_cust_number,                               --环期日配行业成交
    coalesce(sales_cust_number/b_daily_cust_number,0) as daily_cust_penetration_rate ,               --日配渗透率
    coalesce(last_sales_cust_number/last_b_daily_cust_number,0) as last_daily_cust_penetration_rate ,  --环期日配渗透率
    coalesce(sales_cust_number/b_daily_cust_number,0) - coalesce(last_sales_cust_number/last_b_daily_cust_number ,0)  as diff_daily_cust_penetration_rate ,   -- 日配渗透率环比
    coalesce( daily_sales_value/B_daily_sale,0) as daily_industry_sales_ratio,                                                                                -- 日配行业销售额/行业销售额
    coalesce( last_daily_sales_value/last_B_daily_sale,0) as last_daily_industry_sales_ratio,                                                                 -- 日配行业销售额/行业销售额
    coalesce( daily_sales_value/B_daily_sale,0) - coalesce(last_daily_sales_value/last_B_daily_sale,0)  as diff_daily_industry_sale_ratio,                         -- 日配行业销售占比环比差
    a.grouping__id,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from csx_tmp.tmp_sale_frozen_02 a 
left join 
(select * from csx_tmp.tmp_sale_frozen_03 ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'') 
and coalesce(a.region_code,'')=coalesce(b.region_code ,'') 
and  coalesce(a.second_category_code,'')=coalesce(b.second_category_code ,'')
left join 
 csx_tmp.tmp_sale_frozen_04   c  on coalesce(a.region_code,'')=c.region_code 
    and coalesce(a.province_code,'') = c.province_code
    and coalesce(a.classify_small_code,'')=c.classify_small_code
    where 1=1
   and a.grouping__id not in ('31','511','8191','8167','511','487','7')
 

;


