-- 大区品类销售跟踪

-- 创建临时销售表
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_02;
create  table csx_analyse_tmp.csx_analyse_tmp_sale_02 as 
select
    sdt,
    performance_region_code,
    performance_region_name ,
    a.performance_province_code ,
    a.performance_province_name ,
    a.customer_code,
    a.goods_code,
    channel_code,
    channel_name,
    business_type_name ,
    business_type_code ,
    case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end business_division_code,
    case when division_code in ('10','11') then '生鲜' when division_code in ('12','13','14') then '食百' else division_name end business_division_name,
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
    performance_region_code,
    performance_region_name ,
    a.performance_province_code ,
    a.performance_province_name ,
    a.customer_code,
    a.goods_code,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
    end channel_code,
    case when a.channel_code in ('1','7','9') then '大'
        else a.channel_name
    end channel_name,
    business_type_name ,
    business_type_code ,
    b.division_code ,
    b.division_name,
    b.classify_middle_code ,
    b.classify_middle_name ,
    sum( case when sdt between  regexp_replace(trunc('${edate}','MM'),'-','')
    and  regexp_replace('${edate}','-','') then  sale_amt end ) sales_value,
    sum( case when sdt between  regexp_replace(trunc('${edate}','MM'),'-','')
    and   regexp_replace('${edate}','-','') then profit end) profit,
    sum( case when sdt between   regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','') 
    and  regexp_replace(if('${edate}'=last_day('${edate}'),last_day(add_months('${edate}',-1)),add_months('${edate}',-1)),'-','')  then  sale_amt end ) last_month_sale,
    sum( case when sdt between   regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','') 
    and  regexp_replace(if('${edate}'=last_day('${edate}'),last_day(add_months('${edate}',-1)),add_months('${edate}',-1)),'-','')  then profit end) last_month_profit
from
    csx_dws.csx_dws_sale_detail_di a
left join 
(SELECT shop_code,shop_low_profit_flag FROM csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code  	-- 剔除日配剔除联营仓
 join
 (SELECT goods_code,
       classify_middle_code,
       classify_middle_name,
       division_code,
       division_name
FROM csx_dim.csx_dim_basic_goods
WHERE sdt='current') b on a.goods_code=b.goods_code
where
    sdt >= regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','') 
    and sdt <= regexp_replace('${edate}','-','')
	and shop_low_profit_flag=0
group by 
    sdt,
    performance_region_code,
    performance_region_name ,
    a.performance_province_code ,
    a.performance_province_name ,
    a.customer_code,
    a.goods_code,
    case when a.channel_code in ('1','7','9') then '1'
        else a.channel_code
    end  ,
    case when a.channel_code in ('1','7','9') then '大'
        else a.channel_name
    end  ,
    business_type_name,
    business_type_code,
    b.division_code ,
    b.division_name,
    b.classify_middle_code ,
    b.classify_middle_name
)a 
;


-- 明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_attribute_sale_01;
create  table csx_analyse_tmp.csx_analyse_tmp_attribute_sale_01
as 
select 
    performance_region_code,
    performance_region_name,
    performance_province_code ,
    performance_province_name ,
    channel_code,
    channel_name,
    business_division_code,
    business_division_name,
    business_division_code,
    business_division_name,
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
    performance_region_code,
    performance_region_name,
    performance_province_code ,
    performance_province_name ,
    channel_code,
    channel_name,
    attribute_name,
    attribute_code,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ,
    sum(case when sdt = regexp_replace('${edate}','-','') then sale_amt end )as daily_sale_value,
    sum(case when sdt = regexp_replace('${edate}','-','') then profit end) as daily_profit,
    sum(sales_value) month_sale,
    sum(profit) month_profit,
    count(distinct case when a.sales_value>0 then  a.customer_code end )as month_sale_cust_num,
    count(distinct case when sdt between   regexp_replace(trunc('${edate}','MM'),'-','')
    and  regexp_replace('${edate}','-','') then goods_code end )as month_sales_sku,
    sum(last_month_sale) as last_month_sale,
    sum(last_month_profit) as last_month_profit,
    count(distinct case when a.last_month_sale>0 then  a.customer_code end ) as last_month_sale_cust_num
from
    csx_analyse_tmp.csx_analyse_tmp_sale_02 a
where
   1=1
group by 
    performance_region_code,
    performance_region_name,
    performance_province_code ,
    performance_province_name ,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ,
    attribute_name,
    attribute_code,
    channel_code,
    business_division_code,
    business_division_name,
    channel_name
) a 
group by 
    performance_region_code,
    performance_region_name,
    performance_province_code ,
    performance_province_name ,
    channel_code,
    channel_name,
    attribute_name,
    attribute_code,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    classify_middle_code ,
    classify_middle_name ;
 
-- select sum(month_sale) from  csx_analyse_tmp.csx_analyse_tmp_attribute_sale_02 where performance_province_code='32' and attribute_code='1' and channel_code='1' and department_code='104' ;
 
-- 计算课组层级
drop table  if exists csx_analyse_tmp.csx_analyse_tmp_attribute_sale_02;
create  table csx_analyse_tmp.csx_analyse_tmp_attribute_sale_02 as
select
   '1' as level_id,
    performance_region_code,
    performance_region_name,
    a.performance_province_code ,
    performance_province_name ,
    a.channel_code,
    a.channel_name,
    a.attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
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
   coalesce(month_sale/sum(month_sale)over(partition by a.performance_province_code,a.attribute_code,a.channel_code),0) month_sale_ratio,
   coalesce(month_sale/month_sale_cust_num,0) as month_avg_cust_sale,
   0 month_plan_profit,
    month_profit,
    0 month_profit_fill_rate,
    month_profit/month_sale as month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    (month_sale_cust_num)/(all_sale_cust) as penetration_rate,  -- 渗透率
    (all_sale_cust) as all_sale_cust_num,
    row_number()over(partition by a.performance_province_code ,a.attribute_code,a.channel_code order by month_sale desc) as row_num,
    a.last_month_profit,
    a.last_month_sale_cust_num,
    last_all_sale_cust
from csx_analyse_tmp.csx_analyse_tmp_attribute_sale_01    a 
left join 
(
select
    performance_province_code ,
    channel_code,
    attribute_code,
    count(distinct case when sales_value>0  then a.customer_code end  )as all_sale_cust,
    count(distinct case when a.last_month_sale>0  then a.customer_code end  )as last_all_sale_cust

from
     csx_analyse_tmp.csx_analyse_tmp_sale_02 a
where
    sdt >=   regexp_replace(${l_sdate},'-','')
    and sdt <=  regexp_replace('${edate}','-','')
    
group by 
    performance_province_code ,
     a.channel_code,
    attribute_code
   ) b on a.performance_province_code=b.performance_province_code and a.attribute_code=b.attribute_code and a.channel_code=b.channel_code
;

drop table csx_analyse_tmp.csx_analyse_tmp_sales_class_01;
create  table csx_analyse_tmp.csx_analyse_tmp_sales_class_01 as 
 select 
    level_id,
    substr(regexp_replace('${edate}','-',''),1,6) as sales_month,
    performance_region_code,
    performance_region_name,
    a.performance_province_code ,
    a.performance_province_name ,
    a.channel_code,
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
    month_sale_cust_num,     --成交数
    penetration_rate cust_penetration_rate,  -- 本期渗透率
    all_sale_cust_num,      --本期成交
    last_month_sale,        --上期销售额
    a.last_month_profit,    --上期毛利额
    a.last_month_profit/last_month_sale as last_profit_rate,    --上期毛利率
    last_month_sale_cust_num/last_all_sale_cust as  last_cust_penetration_rate, --上期渗透率
    a.last_month_sale_cust_num,  --上期成交数
    last_all_sale_cust,     --上期总成交数
    coalesce(last_month_sale/sum(last_month_sale)over(partition by a.performance_province_code,a.attribute_code,a.channel_code),0) last_month_sale_ratio,
    row_num
from  csx_analyse_tmp.csx_analyse_tmp_attribute_sale_02 a
left join 
(select performance_province_code,
    performance_province_name,
    case when substr(a.classify_middle_code,1,3) in ('B01','B02','B03') THEN '11'
        when classify_middle_code='B0902' then '15'
        else '12'  end business_division_code,
    classify_middle_code,
    classify_middle_name,
    business_type_name,
    sum(plan_sales_value) plan_sales_value,
    sum(plan_profit) plan_profit
from csx_analyse_tmp.source_r_m_province_month_category_target a 
    where months=substr(regexp_replace('${edate}','-',''),1,6)
    group by  performance_province_code,
    performance_province_name,
    classify_middle_code,
    classify_middle_name,
    business_type_name,
    case when substr(a.classify_middle_code,1,3) in ('B01','B02','B03') THEN '11'
        when classify_middle_code='B0902' then '15'
        else '12'  end 
) b on a.performance_province_code=b.performance_province_code and a.classify_middle_code=b.classify_middle_code and a.attribute_name=b.business_type_name and a.business_division_code=b.business_division_code
;

drop table csx_analyse_tmp.csx_analyse_tmp_sales_class_02;
create  table csx_analyse_tmp.csx_analyse_tmp_sales_class_02 as 
select
    level_id,
    substr(regexp_replace('${edate}','-',''),1,6) as sales_month,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code ,
    a.performance_province_name ,
    a.channel_code,
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
    coalesce(month_sale/sum(month_sale)over(partition by a.performance_region_code,a.attribute_code,a.channel_code),0) month_sale_ratio,
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
    a.last_month_sale_cust_num,  --上期成交数
    last_all_sale_cust,     --上期总成交数
    coalesce(last_month_sale/sum(last_month_sale)over(partition by a.performance_region_code,a.attribute_code,a.channel_code),0) last_month_sale_ratio,
    row_number()over(partition by a.performance_region_code ,a.attribute_code,a.channel_code order by month_sale desc) as row_num
from(
select
    '2' as level_id,
    performance_region_code,
    performance_region_name,
    '00' as performance_province_code ,
    performance_region_name as performance_province_name ,
    channel_code,
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
    sum(case when sdt = regexp_replace('${edate}','-','') then sales_value end )as daily_sale_value,
    sum(case when sdt = regexp_replace('${edate}','-','') then profit end) as daily_profit,
    0 as month_plan_sale,
    sum(sales_value) month_sale,
    0 as month_plan_profit,
    sum(profit) month_profit,
    count(distinct case when a.sales_value>0 then  a.customer_code end )as month_sale_cust_num,
    count(distinct case when sdt between  regexp_replace(trunc('${edate}','MM'),'-','')
        and  regexp_replace('${edate}','-','') then goods_code end )as month_sales_sku,
    sum(last_month_sale) as last_month_sale,
    sum(last_month_profit) as last_month_profit,
    count(distinct case when a.last_month_sale>0 then  a.customer_code end ) as last_month_sale_cust_num
from  csx_analyse_tmp.csx_analyse_tmp_sale_02  a
group by 
    performance_region_code,
    performance_region_name,
    a.channel_code,
    a.channel_name,
    business_division_code,
    business_division_name,
    division_code ,
    division_name,
    a.classify_middle_code ,
    a.classify_middle_name,
    a.business_division_code,
    business_division_name
) a 
left join 
(select performance_region_code,
    performance_region_name,
    '00' as performance_province_code ,
    performance_region_name as performance_province_name ,
    case when substr(a.classify_middle_code,1,3) in ('B01','B02','B03') THEN '11'
        when classify_middle_code='B0902' then '15'
        else '12'  end business_division_code,
    classify_middle_code,
    classify_middle_name,
    business_type_name,
    sum(plan_sales_value ) plan_sales_value,
    sum(plan_profit) plan_profit
from csx_ods.csx_ods_data_analysis_prd_source_r_m_province_month_category_target_df a 
 join 
    (select distinct performance_region_code,performance_region_name,performance_province_code,performance_province_name from csx_dim.csx_dim_sales_area_belong_mapping) b on a.province_code=b.performance_province_code
 where months=substr(regexp_replace('${edate}','-',''),1,6)
    group by  performance_region_code,
    performance_region_name,
    classify_middle_code,
    classify_middle_name,
    business_type_name,
    case when substr(a.classify_middle_code,1,3) in ('B01','B02','B03') THEN '11'
        when classify_middle_code='B0902' then '15'
        else '12'  end 
) b on a.performance_region_code=b.performance_region_code 
        and a.classify_middle_code=b.classify_middle_code 
        and a.business_type_name=b.business_type_name 
        and a.business_division_code=b.business_division_code
left join 
(
select
    performance_region_code ,
    channel_code,
    business_type_code,
    count(distinct case when sales_value>0  then a.customer_code end  )as all_sale_cust_num,
    count(distinct case when a.last_month_sale>0  then a.customer_code end  )as last_all_sale_cust
from
     csx_analyse_tmp.csx_analyse_tmp_sale_02 a
where
    sdt >=   regexp_replace(${l_sdate},'-','')
    and sdt <=  regexp_replace('${edate}','-','')
group by 
    performance_region_code ,
    a.channel_code,
    business_type_code
   ) c on a.performance_region_code=c.performance_region_code and a.attribute_code=c.attribute_code and a.channel_code=c.channel_code
-- where performance_province_code='15' 

;
-- 插入数据表 销售环比，销售占比，毛利率环比、渗透率占比差
--insert overwrite table csx_analyse_tmp.report_sale_r_d_zone_classify_sale_fr partition(months,sdt)
--create table csx_analyse_tmp.ads_sale_r_d_zone_classify_sale_fr as 
insert overwrite table csx_analyse_tmp.report_sale_r_d_zone_classify_sale_fr partition(months,sdt)
select 
    level_id,
    sales_month,
    performance_region_code,
    performance_region_name,
    a.performance_province_code ,
    performance_province_name ,
    a.channel_code,
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
    sum(month_plan_sale*10000) month_plan_sale,
    sum(month_sale) month_sale,
    sum(month_sale   )/sum(month_plan_sale*10000) month_sale_fill_rate,    --销售达成率
    sum(mom_sale_growth_rate   ) mom_sale_growth_rate,    -- 销售环比
    sum(month_sale_ratio       ) month_sale_ratio,        --销售占比
    sum(month_avg_cust_sale    ) month_avg_cust_sale,     --客单价
    sum(month_plan_profit *10000     ) month_plan_profit,       -- 毛利额计划
    sum(month_profit) month_profit,            --毛利额
    sum(month_profit )/sum(month_plan_profit *10000 ) month_profit_fill_rate,  --毛利额完成率
    sum(month_profit_rate      ) month_profit_rate,       --毛利率
    sum(month_sales_sku        ) month_sales_sku,         --销售SKU   
    sum(month_sale_cust_num    ) month_sale_cust_num,     --成交数
    sum(cust_penetration_rate) cust_penetration_rate,  -- 本期渗透率
    sum(all_sale_cust_num  ) all_sale_cust_num,      --本期成交
    sum(last_month_sale    ) last_month_sale,        --上期销售额
    sum(a.last_month_profit) last_month_profit,    --上期毛利额
    sum(last_profit_rate          ) last_profit_rate,                        --上期毛利率
    sum(last_cust_penetration_rate) last_cust_penetration_rate,             --上期渗透率
    sum(last_month_sale_cust_num  ) last_month_sale_cust_num,               --上期成交数
    sum(last_all_sale_cust        ) last_all_sale_cust,                 --上期总成交数
    sum(last_month_sale_ratio) last_month_sale_ratio,
    0 as same_period_sale,       --  '同期销售额',
    0 as same_period_profit,         -- '同期毛利额',
    0 as same_period_profit_rate,    --'同期毛利率',
    0 as same_period_cust_penetration_rate ,     --  '同期渗透率',
    0 as same_period_sale_cust_num ,     -- '同期成交数',
    0 as same_period_all_sale_cust,      --  '同期总成交数',
    0 as same_sale_ratio,
    row_num,   
    current_timestamp() ,
    substr(regexp_replace('${edate}','-',''),1,6),
    regexp_replace('${edate}','-','')
from(
select * from csx_analyse_tmp.csx_analyse_tmp_sales_class_02 
union all 
select * from csx_analyse_tmp.csx_analyse_tmp_sales_class_01 
) a  
group by 
level_id,
    sales_month,
    performance_region_code,
    performance_region_name,
    a.performance_province_code ,
    performance_province_name ,
    a.channel_code,
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
