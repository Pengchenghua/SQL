--供应链管理分类销售分析【20211118】
--迭代说明 ：1、日配业务剔除dc_code not in ('W0Z7','W0K4'),2、自营大渠道 business_type_code not in('4','9')
set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');

--上月结束日期，当前日期不等于月末取当前日期，等于月末取上月最后一天
set last_edt=regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','');
set parquet.compression=snappy;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;
-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;

-- 本期数据
drop table if exists csx_tmp.tmp_dp_sale;
create temporary table csx_tmp.tmp_dp_sale
as 
select 
    case when channel_code in ('1','9','7') then 'B端'
        when   channel_code ='2' then 'M端'
     end channel_name,
    case when dc_code in ('W0Z7','W0K4') then '20' else a.business_type_code end business_type_code,
    case when dc_code in ('W0Z7','W0K4') then '联营仓' else a.business_type_name end business_type_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    '' sales_plan,
    sum(sales_value)as sales_value,
    sum(a.profit)as profit
from csx_dw.dws_sale_r_d_detail a 
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
 -- and a.business_type_code!='4'
 -- and classify_middle_code in ('B0304','B0305')
and a.channel_code not  in ('5','6','4')
group by 
    case when channel_code in ('1','9','7') then 'B端'
        when   channel_code ='2' then 'M端' end,
    classify_large_code,
    case when dc_code in ('W0Z7','W0K4') then '20' else a.business_type_code end ,
    case when dc_code in ('W0Z7','W0K4') then '联营仓' else a.business_type_name end,
    classify_large_name,
   a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
;

-- 环期数据
drop table if exists csx_tmp.tmp_dp_sale_01;
create temporary table csx_tmp.tmp_dp_sale_01
as 
select 
    case when channel_code in ('1','9','7') then 'B端'
        when   channel_code ='2' then 'M端' end channel_name,
    classify_large_code,
    case when dc_code in ('W0Z7','W0K4') then '20' else a.business_type_code end business_type_code,
    case when dc_code in ('W0Z7','W0K4') then '联营仓' else a.business_type_name end business_type_name,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    '' as sales_plan,
    sum(sales_value)as last_sales_value,
    sum(a.profit)as last_profit
from csx_dw.dws_sale_r_d_detail a 

where sdt>=${hiveconf:last_sdt}
    and sdt<=${hiveconf:last_edt}
-- and classify_middle_code in('B0304','B0305')
-- and a.business_type_code!='4'
and a.channel_code not  in ('5','6','4')
group by 
    case when channel_code in ('1','9','7') then 'B端'
        when   channel_code ='2' then 'M端'
    end,
    case when dc_code in ('W0Z7','W0K4') then '20' else a.business_type_code end  ,
    case when dc_code in ('W0Z7','W0K4') then '联营仓' else a.business_type_name end  ,
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
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    0 as sales_plan,
    sum(sales_value) as sales_value      ,
    sum(profit) as profit      ,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    grouping__id as group_id
from
(select channel_name,
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    '' sales_plan,
    sales_value,
    profit,
    0 as last_sales_value,
    0 as last_profit
from csx_tmp.tmp_dp_sale a
union all
select channel_name,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    '' as sales_plan,
    0 as sales_value,
    0 as profit,
    last_sales_value  ,
    last_profit
from csx_tmp.tmp_dp_sale_01 a
) a
group by 
    channel_name,
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
grouping sets
((channel_name,
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),
    (
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),
    (channel_name,
    business_type_code,
    business_type_name),
    (channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),
     (channel_name,
     business_type_code,
     business_type_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name),
    (channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name),
     (
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name),
    (channel_name),
    ()
    )
    ;
    

insert overwrite table csx_tmp.report_sale_r_d_manage_sum  partition(months)
select level_id,
    substr(${hiveconf:e_dt},1,4) as years,
    channel_name,
   coalesce(a.business_type_code,'00' ) as business_type_code,
   business_type_name ,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sales_plan,
    sales_value      ,
    profit      ,
    profit_rate,
    last_sales_value,
    last_profit,
    last_profit_rate,
    ring_sale_rate,
    group_id,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from (
select 
    case when group_id =0 then 0 
        when group_id =60 then 1 
        when a.group_id =252 then 2
        when a.group_id=1 then 3
        when a.group_id=253 then 4
        when group_id=3 then 5 
        when a.group_id =255 then 6
        else a.group_id end level_id,
    substr(${hiveconf:e_dt},1,4) as years,
    case when a.group_id in (0,252,60)  then '全国' 
        else channel_name 
    end channel_name,
    coalesce(a.business_type_code,'00')as business_type_code,
    coalesce(business_type_name,concat(channel_name,'_小计')) as business_type_name,
    coalesce(classify_large_code, '00')as classify_large_code,
    coalesce(classify_large_name, '小计')as classify_large_name,
    coalesce(a.classify_middle_code, '')as classify_middle_code,
    coalesce(a.classify_middle_name, '')as classify_middle_name,
    coalesce(a.classify_small_code, '00')as classify_small_code,
    coalesce(a.classify_small_name, '小计')as classify_small_name,
    sales_plan,
    sales_value ,
    profit ,
    profit/sales_value as profit_rate,
    last_sales_value,
    last_profit,
    last_profit/last_sales_value as last_profit_rate,
    (sales_value-last_sales_value)/last_sales_value as ring_sale_rate,
    group_id,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from csx_tmp.temp_sale_all a 
where 1=1 
and (a.channel_name !='M端' or a.business_type_code is null )  
--classify_large_code is not null
union all 
--计算B自营数据，剔除M端、城市服务商
select 
   case when group_id =0 then 0 
        when group_id =60 then 1 
        when a.group_id =252 then 2
        when a.group_id=1 then 3
        when a.group_id=253 then 4
        when group_id=3 then 5 
        when a.group_id =255 then 6
        else a.group_id end level_id,
    substr(${hiveconf:e_dt},1,4) as years,
    case when a.group_id in (0,252,60) then '全国' 
        else channel_name 
    end channel_name,
    '99' as business_type_code,
    'B端(自营)' as business_type_name,
    coalesce(classify_large_code, '00')as classify_large_code,
    coalesce(classify_large_name, '小计')as classify_large_name,
    coalesce(a.classify_middle_code, '')as classify_middle_code,
    coalesce(a.classify_middle_name, '')as classify_middle_name,
    coalesce(a.classify_small_code, '00')as classify_small_code,
    coalesce(a.classify_small_name, '小计')as classify_small_name,
    sum(sales_plan) as sales_plan,
    sum(sales_value)as sales_value      ,
    sum(profit)as profit      ,
    sum(profit)/sum(sales_value)as profit_rate,
    sum(last_sales_value)as last_sales_value,
    sum(last_profit)as last_profit,
    sum(last_profit)/sum(last_sales_value) as last_profit_rate,
    sum(sales_value-last_sales_value)/sum(last_sales_value) as ring_sale_rate,
    100 as group_id,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from csx_tmp.temp_sale_all a 
where a.business_type_code not in('4','9')  --自营剔除城市服务商&业务代理
group by 
     case when group_id =0 then 0 
        when group_id =60 then 1 
        when a.group_id =252 then 2
        when a.group_id=1 then 3
        when a.group_id=253 then 4
        when group_id=3 then 5 
        when a.group_id =255 then 6
        else a.group_id end ,
    substr(${hiveconf:e_dt},1,4) ,
    case when a.group_id in (0,252,60) then '全国' 
        else channel_name 
    end,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
    ) a 

;