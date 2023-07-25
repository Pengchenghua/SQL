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
    a.customer_no,
    a.goods_code,
    channel,
    channel_name,
    attribute_name,
    attribute_code,
    case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end business_division_code,
    case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end business_division_name,
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
    a.customer_no,
    a.goods_code,
    case when a.channel in ('1','7','9') then '1'
        else a.channel
    end channel,
    case when a.channel in ('1','7','9') then '大'
        else a.channel_name
    end channel_name,
    case when a.channel ='7' then 'BBC'
        when ( a.channel in ('1','9') and b.attribute_code=3) then '贸易'
        when ( a.channel in ('1','9') and b.attribute_code=5) then '合伙人'
        when ( a.channel in ('1','9') and order_kind='WELFARE') then '福利单'
    else '日配单'
    --    else  a.channel_name
    end attribute_name,
    case when channel='7' then '7'
            when ( a.channel in ('1','9') and b.attribute_code=3)  then '3'
            when ( a.channel in ('1','9') and b.attribute_code=5)  then '5'
            when ( a.channel in ('1','9') and order_kind='WELFARE') then '2'
            else '1'
    end attribute_code,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ,
    sum( case when sdt between  regexp_replace(${hiveconf:sdate},'-','')
    and  regexp_replace(${hiveconf:edate},'-','') then  sales_value end ) sales_value,
    sum( case when sdt between  regexp_replace(${hiveconf:sdate},'-','')
    and   regexp_replace(${hiveconf:edate},'-','') then profit end) profit,
    sum( case when sdt between   regexp_replace(${hiveconf:l_sdate},'-','')
    and  regexp_replace(${hiveconf:l_edate},'-','') then  sales_value end ) last_month_sale,
    sum( case when sdt between   regexp_replace(${hiveconf:l_sdate},'-','')
    and  regexp_replace(${hiveconf:l_edate},'-','') then profit end) last_month_profit
from
    csx_dw.dws_sale_r_d_customer_sale a
join 
(select customer_no,attribute_code
		from csx_dw.dws_crm_w_a_customer_20200924
		where sdt='current' ) as b on a.customer_no=b.customer_no
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
    case when a.channel in ('1','7','9') then '1'
        else a.channel
    end  ,
    case when a.channel in ('1','7','9') then '大'
        else a.channel_name
    end  ,
    case when a.channel ='7' then 'BBC'
        when ( a.channel in ('1','9') and b.attribute_code=3) then '贸易'
        when ( a.channel in ('1','9') and b.attribute_code=5) then '合伙人'
        when ( a.channel in ('1','9') and order_kind='WELFARE') then '福利单'
    else '日配单'
    --    else  a.channel_name
    end  ,
    case when channel='7' then '7'
            when ( a.channel in ('1','9') and b.attribute_code=3)  then '3'
            when ( a.channel in ('1','9') and b.attribute_code=5)  then '5'
            when ( a.channel in ('1','9') and order_kind='WELFARE') then '2'
            else '1'
    end  ,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name
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
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end business_division_code,
    case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end business_division_name,
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
     a.channel,
    attribute_code
   ) b on a.province_code=b.province_code and a.attribute_code=b.attribute_code and a.channel=b.channel
;

-- 插入数据表 销售环比，销售占比，毛利率环比、渗透率占比差
insert overwrite table csx_tmp.report_sale_r_d_zone_classify_sale_fr partition(months,sdt)
--create table csx_tmp.ads_sale_r_d_zone_classify_sale_fr as 
select 
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    zone_id,
    zone_name,
    a.province_code ,
    province_name ,
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
    month_sale_cust_num,     --成交数
    penetration_rate cust_penetration_rate,  -- 本期渗透率
    all_sale_cust_num,      --本期成交
    last_month_sale,        --上期销售额
    a.last_month_profit,    --上期毛利额
    a.last_month_profit/last_month_sale as last_profit_rate,    --上期毛利率
    last_month_sale_cust_num/last_all_sale_cust as  last_cust_penetration_rate, --上期渗透率
    a.last_month_sale_cust_num,  --上期成交数
    last_all_sale_cust,     --上期总成交数
    0 as same_period_sale,       --  '同期销售额',
    0 as same_period_profit,         -- '同期毛利额',
    0 as same_period_profit_rate,    --'同期毛利率',
    0 as same_period_cust_penetration_rate ,     --  '同期渗透率',
    0 as same_period_sale_cust_num ,     -- '同期成交数',
    0 as same_period_all_sale_cust,      --  '同期总成交数',
    row_num,    
    current_timestamp(),
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6),
    regexp_replace(${hiveconf:edate},'-','')
from  csx_tmp.temp_attribute_sale_02 a;

-- describe csx_tmp.ads_sale_r_d_zone_province_dept_fr ;
-- 插入汇总数据
insert into table csx_tmp.report_sale_r_d_zone_classify_sale_fr partition(months,sdt)
select
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    a.zone_id,
    a.zone_name,
    province_code ,
    province_name ,
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
    a.last_month_sale_cust_num,  --上期成交数
    last_all_sale_cust,     --上期总成交数
    0 as same_period_sale,       --  '同期销售额',
    0 as same_period_profit,         -- '同期毛利额',
    0 as same_period_profit_rate,    --'同期毛利率',
    0 as same_period_cust_penetration_rate ,     --  '同期渗透率',
    0 as same_period_sale_cust_num ,     -- '同期成交数',
    0 as same_period_all_sale_cust,      --  '同期总成交数',
    row_number()over(partition by a.zone_id ,a.attribute_code order by month_sale desc) as row_num,
    current_timestamp(),
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6),
    regexp_replace(${hiveconf:edate},'-','')
from(
select
    '2' as level_id,
    zone_id,
    zone_name,
    '00' as province_code ,
    zone_name as province_name ,
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

 
 

create table  csx_tmp.report_sale_r_d_zone_classify_sale_fr
(
level_id string comment '层级：1管理分类 ，2部类 3、全国',
sales_month string comment '销售月份',
zone_id string comment '战区',
zone_name string comment '战区名称',
province_code string comment '省区',
province_name string comment '省区名称',
channel string comment '渠道',
channel_name string comment '渠道名称',
attribute_code string comment '性属',
attribute_name string comment '性属名称',
business_division_code string comment '采购部',
business_division_name string comment '采购部名称',
division_code string comment '部类',
division_name string comment '部类名称',
classify_middle_code string comment '管理二级分类',
classify_middle_name string comment '管理二级分类名称',
daily_plan_sale decimal(38,6) comment '计划销售额',
daily_sale_value decimal(38,6) comment '昨日销售额',
daily_sale_fill_rate decimal(38,6) comment '昨日销售达成率',
daily_profit decimal(38,6) comment '昨日毛利额',
daily_profit_rate decimal(38,6) comment '昨日毛利率',
month_plan_sale decimal(38,6) comment '月计划',
month_sale decimal(38,6) comment '月销售额',
month_sale_fill_rate decimal(38,6) comment '月销售达成率',
mom_sale_growth_rate decimal(38,6) comment '环比增长率',
month_sale_ratio decimal(38,6) comment '销售占比',
month_avg_cust_sale decimal(38,6) comment '客单价',
month_plan_profit decimal(38,6) comment '毛利计划',
month_profit decimal(38,6) comment '月毛利额',
month_profit_fill_rate decimal(38,6) comment '月毛利额达成',
month_profit_rate decimal(38,6) comment '月毛利率',
month_sales_sku bigint comment '月销售SKU',
month_sale_cust_num bigint comment '月成交数',
cust_penetration_rate decimal(38,6) comment '渗透率',
all_sale_cust_num bigint comment '总成交数',
last_month_sale decimal(38,6) comment '上期销售额',
last_month_profit decimal(38,6) comment '上期毛利额',
last_profit_rate decimal(38,6) comment '上期毛利率',
last_cust_penetration_rate decimal(38,6) comment '上期渗透率',
last_month_sale_cust_num decimal(38,6) comment '上期成交数',
last_all_sale_cust decimal(38,6) comment '上期总成交数',
same_period_sale decimal(38,6) comment '同期销售额',
same_period_profit decimal(38,6) comment '同期毛利额',
same_period_profit_rate decimal(38,6) comment '同期毛利率',
same_period_cust_penetration_rate decimal(38,6) comment '同期渗透率',
same_period_sale_cust_num bigint comment '同期成交数',
same_period_all_sale_cust bigint comment '同期总成交数',
row_num int comment '排名',
updatetime timestamp comment '更新日期'
)comment '看板管理二级分类分析'
partitioned　by ( months string comment '月分区',sdt string comment '日期分区')
;
