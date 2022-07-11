 -- 大区品类销售跟踪 report_sale_r_d_zone_classify_sale_fr
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

SET l_edate=  if(${hiveconf:edate}=last_day(${hiveconf:edate}),last_day(add_months(${hiveconf:edate},-1)),add_months(${hiveconf:edate},-1)) ;


--select * from  csx_tmp.temp_sale_02 where classify_middle_code='B0802'  AND PROVINCE_CODE='15' AND division_code='12';

-- 创建临时销售表
drop table if exists csx_tmp.temp_sale_02;
create temporary table csx_tmp.temp_sale_02 as 
select
    sdt,
    zone_id,
    zone_name ,
    a.province_code ,
    a.province_name ,
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
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
    end channel,
    case when a.channel_code in ('1','7','9') then '大客户'
        else a.channel_name
    end channel_name,
    business_type_name as attribute_name,
    business_type_code as  attribute_code,
    b.division_code ,
    b.division_name,
    b.classify_middle_code ,
    b.classify_middle_name ,
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
left join 
(SELECT * FROM csx_dw.dws_basic_w_a_normal_default_reject_warehouse) c on a.dc_code=c.dc_code  	-- 剔除日配剔除联营仓
 join
 (SELECT goods_id,
       classify_middle_code,
       classify_middle_name,
       division_code,
       division_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') b on a.goods_code=b.goods_id
where
    sdt >= regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <= regexp_replace(${hiveconf:edate},'-','')
	and c.dc_code is null 
group by 
sdt,
    province_code ,
    province_name ,
    a.customer_no,
    a.goods_code,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
    end  ,
    case when a.channel_code in ('1','7','9') then '大客户'
        else a.channel_name
    end  ,
    business_type_name,
    business_type_code,
    b.division_code ,
    b.division_name,
    b.classify_middle_code ,
    b.classify_middle_name
)a 
 left join 
   (select distinct province_code,province_name,region_code as zone_id,region_name as zone_name from csx_dw.dws_sale_w_a_area_belong ) c on a.province_code=c.province_code 
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
    case when division_code in ('10','11') then '生鮮' when division_code in ('12','13','14') then '食百' else division_name end business_division_name,
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
   case when coalesce(last_month_sale,0)=0 then 1 else coalesce((month_sale-last_month_sale)/last_month_sale,0) end as mom_sale_growth_rate,  --销售增长率
   coalesce(month_sale/sum(month_sale)over(partition by a.province_code,a.attribute_code,a.channel),0) month_sale_ratio,
   coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
   0 month_plan_profit,
    month_profit,
    0 month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust) as penetration_rate,  -- 渗透率
    (all_sale_cust) as all_sale_cust_num,
    row_number()over(partition by a.province_code ,a.attribute_code,a.channel order by month_sale desc) as row_num,
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

drop table csx_tmp.temp_sales_class_01;
create temporary table csx_tmp.temp_sales_class_01 as 
 select 
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    a.business_division_code,
    business_division_name,
    division_code ,
    division_name,
    a.classify_middle_code ,
    a.classify_middle_name,
    daily_plan_sale,
    daily_sale_value,
    daily_sale_fill_rate,
    daily_profit,
    daily_profit_rate,
    plan_sales_value as  month_plan_sale,
    month_sale,
    month_sale_fill_rate,    --销售达成率
    mom_sale_growth_rate,    -- 销售环比
    month_sale_ratio,        --销售占比
    month_avg_cust_sale,     --客单价
    plan_profit as month_plan_profit,       -- 毛利额计划
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
    coalesce(last_month_sale/sum(last_month_sale)over(partition by a.province_code,a.attribute_code,a.channel),0) last_month_sale_ratio,
    row_num
from  csx_tmp.temp_attribute_sale_02 a
left join 
(select province_code,
    province_name,
    case when substr(a.classify_middle_code,1,3) in ('B01','B02','B03') THEN '11'
        when classify_middle_code='B0902' then '15'
        else '12'  end business_division_code,
    classify_middle_code,
    classify_middle_name,
    business_type_name,
    sum(plan_sales_value) plan_sales_value,
    sum(plan_profit) plan_profit
from csx_tmp.source_r_m_province_month_category_target a 
    where months=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
    group by  province_code,
    province_name,
    classify_middle_code,
    classify_middle_name,
    business_type_name,
    case when substr(a.classify_middle_code,1,3) in ('B01','B02','B03') THEN '11'
        when classify_middle_code='B0902' then '15'
        else '12'  end 
) b on a.province_code=b.province_code and a.classify_middle_code=b.classify_middle_code and a.attribute_name=b.business_type_name and a.business_division_code=b.business_division_code
;

drop table csx_tmp.temp_sales_class_02;
create temporary table csx_tmp.temp_sales_class_02 as 
select
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    a.zone_id,
    a.zone_name,
    a.province_code ,
    a.province_name ,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    a.business_division_code,
    business_division_name,
    division_code ,
    division_name,
    a.classify_middle_code ,
    a.classify_middle_name,
    0 as daily_plan_sale,
    daily_sale_value,
    coalesce(daily_sale_value/daily_plan_sale,0) daily_sale_fill_rate,
    daily_profit,
    coalesce(daily_profit/daily_sale_value,0) daily_profit_rate,
    coalesce(plan_sales_value,0) month_plan_sale,
    month_sale,
    coalesce(month_sale/coalesce(plan_sales_value,0),0) month_sale_fill_rate,
    case when coalesce(last_month_sale,0)=0 then 1 else coalesce((month_sale-last_month_sale)/last_month_sale,0) end as mom_sale_growth_rate,  --销售增长率
    coalesce(month_sale/sum(month_sale)over(partition by a.zone_id,a.attribute_code,a.channel),0) month_sale_ratio,
    coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
    coalesce(plan_profit,0) month_plan_profit,
    month_profit,
    coalesce(month_profit / coalesce(plan_profit,0),0) month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust_num) as cust_penetration_rate,  -- 渗透率
    all_sale_cust_num,
    last_month_sale,
    last_month_profit,
    a.last_month_profit/last_month_sale as last_profit_rate,    --上期毛利率
    last_month_sale_cust_num/last_all_sale_cust as  last_cust_penetration_rate, --上期渗透率
    a.last_month_sale_cust_num,  --上期成交客户数
    last_all_sale_cust,     --上期总成交客户数
    coalesce(last_month_sale/sum(last_month_sale)over(partition by a.zone_id,a.attribute_code,a.channel),0) last_month_sale_ratio,
    row_number()over(partition by a.zone_id ,a.attribute_code,a.channel order by month_sale desc) as row_num
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
    a.business_division_code,
    business_division_name,
    division_code ,
    division_name,
    a.classify_middle_code ,
    a.classify_middle_name,
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

group by 
    zone_id,
    zone_name,
    a.channel,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    division_code ,
    division_name,
    a.classify_middle_code ,
    a.classify_middle_name,
    a.business_division_code,
    business_division_name
) a 
left join 
(select zone_id,
    zone_name,
    '00' as province_code ,
    zone_name as province_name ,
    case when substr(a.classify_middle_code,1,3) in ('B01','B02','B03') THEN '11'
        when classify_middle_code='B0902' then '15'
        else '12'  end business_division_code,
    classify_middle_code,
    classify_middle_name,
    business_type_name,
    sum(plan_sales_value) plan_sales_value,
    sum(plan_profit) plan_profit
from csx_tmp.source_r_m_province_month_category_target a 
 left join 
   (select distinct province_code,province_name,region_code as zone_id,region_name as zone_name from csx_dw.dws_sale_w_a_area_belong ) c on a.province_code=c.province_code 

    where months=substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
    group by  zone_id,
    zone_name,
    classify_middle_code,
    classify_middle_name,
    business_type_name,
    case when substr(a.classify_middle_code,1,3) in ('B01','B02','B03') THEN '11'
        when classify_middle_code='B0902' then '15'
        else '12'  end 
) b on a.zone_id=b.zone_id and a.classify_middle_code=b.classify_middle_code and a.attribute_name=b.business_type_name and a.business_division_code=b.business_division_code
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
   ) c on a.zone_id=c.zone_id and a.attribute_code=c.attribute_code and a.channel=c.channel
-- where province_code='15' 

;
-- 插入数据表 销售环比，销售占比，毛利率环比、渗透率占比差
--insert overwrite table csx_tmp.report_sale_r_d_zone_classify_sale_fr partition(months,sdt)
--create table csx_tmp.ads_sale_r_d_zone_classify_sale_fr as 
insert overwrite table csx_tmp.report_sale_r_d_zone_classify_sale_fr partition(months,sdt)
select 
    level_id,
    sales_month,
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
    sum(daily_plan_sale) daily_plan_sale,
    sum(daily_sale_value) daily_sale_value,
    sum(daily_sale_fill_rate) daily_sale_fill_rate,
    sum(daily_profit) daily_profit,
    sum(daily_profit_rate) daily_profit_rate,
    sum(month_plan_sale) month_plan_sale,
    sum(month_sale) month_sale,
    sum(month_sale_fill_rate   ) month_sale_fill_rate,    --销售达成率
    sum(mom_sale_growth_rate   ) mom_sale_growth_rate,    -- 销售环比
    sum(month_sale_ratio       ) month_sale_ratio,        --销售占比
    sum(month_avg_cust_sale    ) month_avg_cust_sale,     --客单价
    sum(month_plan_profit      ) month_plan_profit,       -- 毛利额计划
    sum(month_profit           ) month_profit,            --毛利额
    sum(month_profit_fill_rate ) month_profit_fill_rate,  --毛利额完成率
    sum(month_profit_rate      ) month_profit_rate,       --毛利率
    sum(month_sales_sku        ) month_sales_sku,         --销售SKU   
    sum(month_sale_cust_num    ) month_sale_cust_num,     --成交客户数
    sum(cust_penetration_rate) cust_penetration_rate,  -- 本期渗透率
    sum(all_sale_cust_num  ) all_sale_cust_num,      --本期成交客户
    sum(last_month_sale    ) last_month_sale,        --上期销售额
    sum(a.last_month_profit) last_month_profit,    --上期毛利额
    sum(last_profit_rate          ) last_profit_rate,                        --上期毛利率
    sum(last_cust_penetration_rate) last_cust_penetration_rate,             --上期渗透率
    sum(last_month_sale_cust_num  ) last_month_sale_cust_num,               --上期成交客户数
    sum(last_all_sale_cust        ) last_all_sale_cust,                 --上期总成交客户数
    sum(last_month_sale_ratio) last_month_sale_ratio,
    0 as same_period_sale,       --  '同期销售额',
    0 as same_period_profit,         -- '同期毛利额',
    0 as same_period_profit_rate,    --'同期毛利率',
    0 as same_period_cust_penetration_rate ,     --  '同期客户渗透率',
    0 as same_period_sale_cust_num ,     -- '同期成交客户数',
    0 as same_period_all_sale_cust,      --  '同期总成交客户数',
    0 as same_sale_ratio,
    row_num,   
    current_timestamp() ,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6),
    regexp_replace(${hiveconf:edate},'-','')
from(
select * from csx_tmp.temp_sales_class_02 
union all 
select * from csx_tmp.temp_sales_class_01 
) a  
group by 
level_id,
    sales_month,
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
    row_num
;
