set hive.execution.engine=tez;
set tez.queue.name=caishixian;
set hive.exec.dynamic.partition=true;
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
    case when a.channel in ('1','7','9') then '1'
        else a.channel
        end channel,
    case when a.channel in ('1','7','9') then '大'
        else a.channel_name
        end channel_name,
       case when a.channel='7' then 'BBC'
            when ( a.channel='1' and  attribute_code=3) then '贸易'
            when ( a.channel='1' and order_kind='WELFARE') then '福利单'
            when ( a.channel='1' and attribute_code=5) then '合伙人'
            when  a.channel in ('1') then '日配单'
           -- when a.channel not in ('1','7') then a.channel_name
            else  a.channel_name
            end attribute_name,
        case when a.channel='7' then '7'
            when ( a.channel='1' and  attribute_code=3) then '3'
            when ( a.channel='1' and order_kind='WELFARE') then '2'
            when ( a.channel='1' and attribute_code=5 ) then '5'
            when  a.channel='1' then '1'
          --  when  a.channel not in ('1','7') then concat('1',channel)
            else concat('1',channel)
            end attribute_code,
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
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >=   regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=   regexp_replace(${hiveconf:edate},'-','')
  --  and  channel ='1'
  --  and  a.attribute_code in ('1','2') and a.order_kind!='WELFARE'
group by 
    province_code ,
    province_name ,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end   , 
    case when department_code like 'U%' then '加工课' else department_name end ,
    case when a.channel in ('1','7','9') then '1'
        else a.channel
        end ,
    case when a.channel in ('1','7','9') then '大'
        else a.channel_name
        end ,
     case when a.channel='7' then 'BBC'
            when ( a.channel='1' and  attribute_code=3) then '贸易'
            when ( a.channel='1' and order_kind='WELFARE') then '福利单'
            when ( a.channel='1' and attribute_code=5) then '合伙人'
            when  a.channel in ('1') then '日配单'
           -- when a.channel not in ('1','7') then a.channel_name
            else  a.channel_name
            end ,
        case when a.channel='7' then '7'
            when ( a.channel='1' and  attribute_code=3) then '3'
            when ( a.channel='1' and order_kind='WELFARE') then '2'
            when ( a.channel='1' and attribute_code=5 ) then '5'
            when  a.channel='1' then '1'
          --  when  a.channel not in ('1','7') then concat('1',channel)
            else concat('1',channel)
            end 
union all 
select
    province_code ,
    province_name ,
   case when a.channel in ('1','7','9') then '1'
        else a.channel
        end channel,
    case when a.channel in ('1','7','9') then '大'
        else a.channel_name
        end channel_name,
     case when a.channel='7' then 'BBC'
            when ( a.channel='1' and  attribute_code=3) then '贸易'
            when ( a.channel='1' and order_kind='WELFARE') then '福利单'
            when ( a.channel='1' and attribute_code=5) then '合伙人'
            when  a.channel in ('1') then '日配单'
           -- when a.channel not in ('1','7') then a.channel_name
            else  a.channel_name
            end attribute_name,
        case when a.channel='7' then '7'
            when ( a.channel='1' and  attribute_code=3) then '3'
            when ( a.channel='1' and order_kind='WELFARE') then '2'
            when ( a.channel='1' and attribute_code=5 ) then '5'
            when  a.channel='1' then '1'
          --  when  a.channel not in ('1','7') then concat('1',channel)
            else concat('1',channel)
            end    attribute_code,
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
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >=  regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
   -- and  channel ='1'
   -- and  a.attribute_code in ('1','2') and a.order_kind!='WELFARE'
group by 
    province_code ,
    province_name ,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end   , 
    case when department_code like 'U%' then '加工课' else department_name end ,
    case when a.channel in ('1','7','9') then '1'
        else a.channel
        end ,
    case when a.channel in ('1','7','9') then '大'
        else a.channel_name
        end ,
     case when a.channel='7' then 'BBC'
            when ( a.channel='1' and  attribute_code=3) then '贸易'
            when ( a.channel='1' and order_kind='WELFARE') then '福利单'
            when ( a.channel='1' and attribute_code=5) then '合伙人'
            when  a.channel in ('1') then '日配单'
           -- when a.channel not in ('1','7') then a.channel_name
            else  a.channel_name
            end ,
        case when a.channel='7' then '7'
            when ( a.channel='1' and  attribute_code=3) then '3'
            when ( a.channel='1' and order_kind='WELFARE') then '2'
            when ( a.channel='1' and attribute_code=5 ) then '5'
            when  a.channel='1' then '1'
          --  when  a.channel not in ('1','7') then concat('1',channel)
            else concat('1',channel)
            end 
) a 
group by 
    province_code ,
    province_name ,
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
   coalesce(month_sale/sum(month_sale)over(partition by zone_id,a.attribute_code,a.department_code),0) month_sale_ratio,
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
    case when a.channel in ('1','7') then '1'
        else a.channel
        end channel,
    case when channel='7' then '7'
            when attribute_code=3 then '3'
            when order_kind='WELFARE' then '2'
            when attribute_code=5 then '5'
            when  a.channel not in ('1','7') then concat('1',channel)
            else '1'
            end attribute_code,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >=   regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    
group by 
    province_code ,
   case when a.channel in ('1','7') then '1'
        else a.channel
        end ,
    case when channel='7' then '7'
            when attribute_code=3 then '3'
            when order_kind='WELFARE' then '2'
            when attribute_code=5 then '5'
            when  a.channel not in ('1','7') then concat('1',channel)
            else '1'
            end
   ) b on a.province_code=b.province_code and a.attribute_code=b.attribute_code and a.channel=b.channel
   left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
;

-- 插入数据表
insert overwrite table csx_tmp.ads_sale_r_d_zone_province_dept_fr partition(months)
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
insert into table csx_tmp.ads_sale_r_d_zone_province_dept_fr partition(months)
select
    level_id,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sales_month,
    zone_id,
    zone_name,
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
    '00' as province_code ,
    zone_name as province_name ,
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
