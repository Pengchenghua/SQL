
set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
set last_edt=regexp_replace(add_months(${hiveconf:edt} ,-1),'-','');
set s_dt_30 =regexp_replace(date_sub(${hiveconf:edt},30),'-','');
-- set parquet.compression=snappy;
-- set hive.exec.dynamic.partition=true; 
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;
-- 本期数据 (不含合伙人 purpose!='06')

drop table if exists csx_tmp.tmp_dp_sale;
create temporary table csx_tmp.tmp_dp_sale
as 
select 
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
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
    sum(case when classify_middle_code='B0304' then a.sales_value end ) as frozen_sales,
    sum(case when classify_middle_code='B0304' then a.profit end ) as frozen_profit,
    sum(case when business_type_code='1' and classify_middle_code='B0304' then a.sales_value end ) as frozen_daily_sales,
    sum(case when business_type_code='1' and classify_middle_code='B0304' then a.profit end ) as frozen_daily_profit
from csx_dw.dws_sale_r_d_detail a 
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
    and a.business_type_code !='4'
    and a.channel_code  in ('1','7','9')
group by 
    case when channel_code in ('1','9','7') then 'B端' end,
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

-- 环期数据 (不含合伙人 purpose!='06')
drop table if exists csx_tmp.tmp_dp_sale_01;
create temporary table csx_tmp.tmp_dp_sale_01
as 
select 
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
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
    sum(case when classify_middle_code='B0304' then a.sales_value end ) as last_frozen_sales,
    sum(case when classify_middle_code='B0304' then a.profit end ) as last_frozen_profit,
    sum(case when a.business_type_code='1' and  classify_middle_code='B0304' then a.sales_value end ) as last_frozen_daily_sales,
    sum(case when a.business_type_code='1' and  classify_middle_code='B0304' then a.profit end ) as last_frozen_daily_profit
from csx_dw.dws_sale_r_d_detail a 
where sdt>=${hiveconf:last_sdt}
    and sdt<=${hiveconf:last_edt}
-- and classify_middle_code='B0304'
and business_type_code !='4'
and a.channel_code   in ('1','7','9')
group by 
    case when channel_code in ('1','9','7') then 'B端' end,
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

-- 本期与环比汇总层级汇总
drop table if exists csx_tmp.temp_sale_all_01;
create temporary table csx_tmp.temp_sale_all_01 as 
select
    channel_name,
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
group by 
    channel_name,
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
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),
    (channel_name,
    province_code,
    province_name),
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

   
-- 总数
drop table if exists  csx_tmp.temp_sale_cust;
create  temporary table csx_tmp.temp_sale_cust as 
select 
    channel_name,
    province_code,
    province_name,
    count(distinct case when sales_value>0 then customer_no end) as sales_cust_number,
    count(distinct case when last_sales_value>0 then customer_no end ) as last_sales_cust_number,
    sum(sales_value) as daily_sales_value,
    sum(last_sales_value) as last_daily_sales_value,
    grouping__id
from csx_tmp.temp_sale_all 
where business_type_code='1'
group by 
    channel_name,
    province_code,
    province_name
grouping sets
    (( channel_name,
    province_code,
    province_name),
    ())
;


--- 计算30日冻品销售额
drop table if exists csx_tmp.temp_sale_30day;
create temporary table csx_tmp.temp_sale_30day as 
select 
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
where sdt>=${hiveconf:s_dt_30} 
    and sdt<=${hiveconf:e_dt}
    and a.business_type_code !='4'
    and a.channel_code  in ('1','7','9')
    and classify_middle_code='B0304'
group by province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
grouping sets
    (
    (province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
     (province_code,
    province_name),
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
select 
dist_code,
classify_large_code,
classify_middle_code,
classify_small_code,
sum(final_qty) final_qty,
sum(final_amt)final_amt ,
grouping__id
from csx_tmp.ads_wms_r_d_goods_turnover a 
join 
(select goods_id,classify_small_code,classify_large_code,classify_middle_code from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and classify_middle_code='B0304') c on a.goods_id=c.goods_id
join 
(select shop_id from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose !='06')b on a.dc_code=b.shop_id
where sdt=${hiveconf:e_dt}
group by dist_code,classify_large_code,
classify_middle_code,
classify_small_code
grouping sets
((dist_code,
classify_large_code,
classify_middle_code,
classify_small_code),
(dist_code),
(classify_large_code,
classify_middle_code,
classify_small_code),());


--写入明细层

drop table if exists csx_tmp.temp_all_sale;
create temporary table csx_tmp.temp_all_sale as 
select a.channel_name,
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
(select * from csx_tmp.temp_sale_cust ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'')
left join 
(select province_code,channel_name,sales_value as all_sales_value,profit as all_profit from csx_tmp.temp_sale_all_01 where grouping__id in ('7','0'))c on coalesce(a.province_code,'')=coalesce(c.province_code,'')
where (classify_middle_code='B0304' or a.grouping__id in ('0','7') )
;






set  mapreduce.job.reduces = 80;
set  hive.map.aggr = true;
set  hive.groupby.skewindata = true;
set  hive.exec.parallel = true;
set  hive.exec.dynamic.partition = true;
--启动态分区
set  hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
set  hive.exec.max.dynamic.partitions = 10000;
--在所有执行mr的节点上，最大一共可以创建多少个动态分区。
set  hive.exec.max.dynamic.partitions.pernode = 100000;
--源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错

insert overwrite table csx_tmp.ads_sale_r_d_frozen_fr partition(sdt)
select
    case when a.grouping__id = '0' then '0' when a.grouping__id = '7' then '3' when a.grouping__id = '505' then '2' else '4' end level_id,  --分组：0 全国，1 省区 2省区管理分类
    substr(${hiveconf:e_dt} ,1,4) as years,
    substr(${hiveconf:e_dt} ,1,6) as smonth,
    coalesce(a.channel_name,'B端')as channel_name,
    coalesce(a.province_code,'00') as  province_code,
    coalesce(a.province_name,'全国') as province_name,
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
    coalesce((frozen_sales-last_frozen_sales)/last_frozen_sales,0) as frozen_ring_sales_ratio , --冻品销售环比增长率
    frozen_profit/frozen_sales-last_profit/last_sales_value as frozen_diff_profit_rate,     --冻品毛利率差
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
    ${hiveconf:e_dt}
from csx_tmp.temp_all_sale  a
left join csx_tmp.temp_sale_30day b on coalesce(a.province_code,'')= coalesce(b.province_code,'') and coalesce(a.classify_small_code,'')=coalesce(b.classify_small_code,'')
left join csx_tmp.temp_sale_02 d on coalesce(a.province_code,'')=coalesce(dist_code ,'')and coalesce(a.classify_small_code,'')=coalesce(d.classify_small_code,'')
;


----创建表
drop table  csx_tmp.ads_sale_r_d_frozen_fr;
CREATE  TABLE csx_tmp.ads_sale_r_d_frozen_fr(
  level_id string comment '分组：0 全国，1 省区 2省区管理分类',
  years string comment '销售年份',
  smonth string comment '销售月份',
  channel_name string comment  '渠道', 
  province_code string comment '省区编码', 
  province_name string comment '省区名称', 
  classify_large_code string comment '管理一级分类', 
  classify_large_name string comment '管理一级分类', 
  classify_middle_code string comment '管理二级分类', 
  classify_middle_name string comment '管理二级分类', 
  classify_small_code string comment '管理三级分类', 
  classify_small_name string comment '管理三级分类', 
  sales_value decimal(38,6) comment '销售额', 
  profit decimal(38,6) comment '毛利额', 
  frozen_sales decimal(38,6) comment '冻品销售额', 
  frozen_profit decimal(38,6) comment '冻品毛利额', 
  frozen_profit_rate decimal(38,6) comment '冻品毛利率', 
  frozen_daily_sales decimal(38,6)comment '冻品日配业务销售额', 
  frozen_daily_profit decimal(38,6)comment '冻品日配业务毛利额', 
  frozen_daily_profit_rate decimal(38,6)comment '冻品日配业务毛利率', 
  last_sales_value decimal(38,6) comment '环期销售额', 
  last_profit decimal(38,6) comment '环期毛利额', 
  last_frozen_sales decimal(38,6) comment '环期冻品销售额', 
  last_frozen_profit decimal(38,6) comment '环期冻品毛利额', 
  last_frozen_profit_rate decimal(38,6) comment '环期冻品毛利率',
  last_frozen_daily_sales decimal(38,6) comment '环期日配业务销售额', 
  last_frozen_daily_profit decimal(38,6) comment '环期日配业务毛利额',
  last_frozen_daily_profit_rate decimal(38,6) comment '环期日配业务毛利率', 
  daily_cust_number bigint comment '日配业务成交数（总）', 
  last_daily_cust_number bigint comment '环期日配业务成交(总)', 
  sales_cust_number bigint comment '成交数(B端)', 
  last_sales_cust_number bigint comment '环期成交数(B端)', 
  all_sales_value decimal(38,6) comment '总销售额(B端)省区/全国', 
  all_profit decimal(38,6) comment '总毛利额(B端)省区/全国', 
  all_profit_rate decimal(38,6) comment '总毛利率(B端)省区毛利率/全国毛利率', 
  frozen_sales_ratio decimal(38,6) comment '冻品销售/省区销售额占比(B端)', 
  frozen_ring_sales_rate decimal(38,6) comment '冻品环期增长率', 
  frozen_diff_profit_rate decimal(38,6) comment '冻品环期毛利率差', 
  daily_cust_penetration_rate decimal(38,6) comment '日配业务渗透率', 
  last_daily_cust_penetration_rate decimal(38,6) comment '环期日配业务渗透率', 
  diff_daily_cust_penetration_rate decimal(38,6) comment '日配业务渗透率差', 
  daily_sales_ratio decimal(38,6) comment '日配销售额/日配省区销售额占比', 
  last_daily_sales_ratio decimal(38,6)comment '环期日配销售额/日配省区销售额占比', 
  frozen_sales_qty_30day decimal(30,3)comment '冻品滚动30天销量', 
  frozen_sales_30day decimal(30,6) comment '冻品滚动30天销售额', 
  final_qty decimal(38,6) comment '期末库存量', 
  final_amt decimal(38,6) comment '期末库存额', 
  grouping_id string comment '分组ID',
   update_time TIMESTAMP COMMENT '插入时间'
  ) comment '冻品B端管理分类销售汇总'
  partitioned by(sdt string comment '日期分区')
  STORED AS PARQUET 

;


csx_tmp_ads_sale_r_d_frozen_fr