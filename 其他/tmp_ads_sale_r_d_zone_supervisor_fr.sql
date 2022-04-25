SET edate= '${enddate}';
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.parallel=true;

drop table csx_tmp.temp_manger_sale;
create temporary  TABLE csx_tmp.temp_manger_sale AS
select channel,
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    manager_name,
   coalesce(sum(old_cust_count),0) as  old_cust_count,
   coalesce(sum(old_daily_sale),0) as  old_daily_sale,
   coalesce(sum(old_month_sale),0) as  old_month_sale,
   coalesce(sum(old_month_profit),0) as  old_month_profit,
   coalesce(sum(old_last_month_sale),0) as  old_last_month_sale,
   coalesce(sum(new_cust_count),0) as  new_cust_count,
   coalesce(sum(new_daily_sale),0) as  new_daily_sale,
   coalesce(sum(new_month_sale),0) as  new_month_sale,
   coalesce(sum(new_month_profit),0) as  new_month_profit,
   coalesce(sum(new_last_month_sale),0) as  new_last_month_sale,
   coalesce(sum(all_daily_sale),0) as  all_daily_sale,
   coalesce(sum(all_month_sale),0) as  all_month_sale,
   coalesce(sum(all_month_profit),0) as  all_month_profit,
   coalesce(sum(all_last_month_sale),0) as  all_last_month_sale,
   coalesce(sum(old_plan_sale),0)as old_plan_sale,
   coalesce(sum(new_plan_sale),0)as new_plan_sale,
   coalesce(sum(new_plan_sale_cust_num),0)as new_plan_sale_cust_num,
   coalesce(sum(all_plan_sale),0)as all_plan_sale,
   sum(all_plan_profit)all_plan_profit
from 
(select channel_name_code as channel,
    channel_name_1 as channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    first_supervisor_name as manager_name,
    coalesce(count(distinct case when smonth='本月' and is_new_sale='否' then customer_no end),0)as old_cust_count,  --老客-累计客户数
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then Md_sales_value end)/10000,0) as old_daily_sale, --老客-昨日销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then sales_value end)/10000,0) as old_month_sale,  --老客-累计销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then profit end)/10000,0) as old_month_profit,  --老客-累计毛利额
    coalesce(sum(case when smonth='环比月' and is_new_sale='否' then sales_value end)/10000,0) as old_last_month_sale,  --老客-环比累计销售额
    coalesce(count(distinct case when smonth='本月' and is_new_sale='是' then customer_no end),0)as new_cust_count,  --新客-累计客户数
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then Md_sales_value end)/10000,0) as new_daily_sale, --新客-昨日销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then sales_value end)/10000,0) as new_month_sale,  --新客-累计销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then profit end)/10000,0) as new_month_profit,  --新客-累计毛利额
    coalesce(sum(case when smonth='环比月' and is_new_sale='是' then sales_value end)/10000,0) as new_last_month_sale,  --新客-环比累计销售额
    coalesce(sum(case when smonth='本月' then Md_sales_value end)/10000,0) as all_daily_sale, --汇总-昨日销售额
    coalesce(sum(case when smonth='本月' then sales_value end)/10000,0) as all_month_sale,  --汇总-累计销售额
    coalesce(sum(case when smonth='本月' then profit end)/10000,0) as all_month_profit,  --汇总-累计毛利额
    coalesce(sum(case when smonth='环比月' then sales_value end)/10000,0) as all_last_month_sale,  --汇总-环比累计销售额
    0 as old_plan_sale,
    0 as new_plan_sale,
    0 as new_plan_sale_cust_num,
    0 as all_plan_sale,
    0 as all_plan_profit
from (SELECT channel,
             channel_name,
             region_code,
             region_name,
             province_code,
             province_name,
             city_group_code,
             city_group_name,
             third_supervisor_name,
             coalesce(first_supervisor_name,'')as first_supervisor_name,
             customer_no,
             customer_name,
             province_manager_id,
             province_manager_name,
             city_group_manager_id,
             city_group_manager_name,
             order_kind,
             sales_belong_flag,
             is_partner,
             attribute_0,
             ascription_type_name,
             sale_group,
             is_new_sale,
             coalesce(Md_sales_value,0)as Md_sales_value, --昨日销售额
             coalesce(sales_value,0)as sales_value,
             coalesce(profit,0) as profit,
             smonth,
             substr(sdt,1,6)as months,
             case when channel_name='商超' then '商超'
				when channel_name='大客户' or channel_name like '企业购%' then '大客户'
				else channel_name end channel_name_1,
			case when channel_name='商超' then '2'
				when channel_name='大客户' or channel_name like '企业购%' then '1'
				else channel end channel_name_code
        FROM csx_tmp.tmp_supervisor_day_detail
    )a
        where 1=1
         --   and channel_name_1='B端'
        group by region_code,
                 region_name,
                 province_code,
                 province_name,
                 city_group_name,
                 first_supervisor_name,
                 channel_name_code,
                 channel_name_1
    union all 
     SELECT a.channel,
            a.channel_name,
            b.region_code,
            b.region_name,
            b.province_code,
            b.province_name as province_name,
            manager_name,
            0 as old_cust_count,
            0 as  old_daily_sale,
            0 as  old_month_sale,
            0 as  old_month_profit,
            0 as  old_last_month_sale,
            0 as  new_cust_count,
            0 as  new_daily_sale,
            0 as  new_month_sale,
            0 as  new_month_profit,
            0 as  new_last_month_sale,
            0 as  all_daily_sale,
            0 as  all_month_sale,
            0 as  all_month_profit,
            0 as  all_last_month_sale,
            sum(case when customer_age_code ='1' then plan_sales_value end ) as old_plan_sale,
            sum(case when customer_age_code ='2' then plan_sales_value end ) as new_plan_sale,
            sum(case when customer_age_code ='2' then a.customer_count end ) as new_plan_sale_cust_num,
            coalesce(sum(plan_sales_value),0)all_plan_sale,
            sum(plan_profit)all_plan_profit
      FROM csx_tmp.dws_csms_manager_month_sale_plan_tmp a
      join 
      (select region_code,region_name,province_code,province_name from csx_dw.dim_area where area_rank=13) b on a.province_code=b.province_code
      WHERE MONTH= regexp_replace(${hivecof:edate},'-','')
       --  AND channel_name='大客户'
      GROUP BY b.region_code,
            b.region_name,
            b.province_code,
            b.province_name,
            manager_name,
            a.channel,
            a.channel_name
)a 
GROUP BY  
            region_code,
            region_name,
            province_code,
            province_name,
            manager_name,
            channel,
            channel_name
grouping sets((region_code,region_name),
             (region_code,region_name,channel_name,channel),
             (region_code,region_name,province_code,province_name),
             (region_code,region_name,province_code,province_name,channel_name,channel),
             (region_code,region_name,province_code,province_name,channel_name,channel,manager_name))
;

-- alter table csx_tmp.ads_sale_r_d_zone_supervisor_fr drop partition(months!=0);

insert overwrite table csx_tmp.ads_sale_r_d_zone_supervisor_fr partition(months)
SELECT  
        case  when channel is null then '0'
            when province_code is null then '1'
            when manager_name is null then '2'
            else '3'
        end level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_month,
        region_code,
        region_name,
        case when  channel is null then '0' else channel end channel,
        case when  channel is null then '全渠道' else channel_name end channel_name,
        case when province_code is null then '00' else province_code end province_code,
        case when province_name is null then '合计' else  province_name end province_name,
        '' manager_no,
        case when manager_name is null then '小计' else manager_name end manager_name,
        (new_cust_count+old_cust_count) as all_cust_count,
        all_daily_sale,
        all_plan_sale,
         all_month_sale,
        coalesce( all_month_sale/all_plan_sale,0) as all_sales_fill_rate,
        all_last_month_sale,
        coalesce((all_month_sale-all_last_month_sale)/abs(all_last_month_sale),0) as all_mom_sale_growth_rate,
        all_plan_profit,
        all_month_profit,
        coalesce(all_month_profit/all_plan_profit,0) as all_month_profit_fill_rate,
        coalesce(all_month_profit/all_month_sale,0) as all_month_profit_rate,
        old_cust_count,
        old_daily_sale,
        old_plan_sale,
        old_month_sale,
        coalesce(old_month_sale/old_plan_sale,0) as  old_sales_fill_rate,
        old_last_month_sale,
        coalesce((old_month_sale-old_last_month_sale)/abs(old_last_month_sale),0) as old_mom_sale_growth_rate,
        old_month_profit,
        coalesce(old_month_profit/old_month_sale,0) as old_month_profit_rate,
        new_plan_sale_cust_num,
        new_cust_count,
        coalesce(new_cust_count-new_plan_sale_cust_num,0) new_cust_count_fill,
        new_daily_sale,
        new_plan_sale,
        new_month_sale,
        coalesce(new_month_sale/new_plan_sale,0) new_month_sale_fill_rate,
        new_last_month_sale,
        coalesce((new_month_sale-new_last_month_sale)/abs(new_last_month_sale),0) as new_mom_sale_growth_rate,
        new_month_profit,
        coalesce(new_month_profit/new_month_sale,0) new_month_profit_rate,
        current_timestamp(),
        substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
FROM csx_tmp.temp_manger_sale
;




--- 表结构
create table csx_tmp.ads_sale_r_d_zone_supervisor_fr(
  level_id STRING comment '层级：0 大区汇总、1 渠道汇总、2省区汇总、3、明细'
  sales_months string comment '销售月份',
  zone_id string comment '大区编码',
  zone_name string comment '大区',
  channel string comment '渠道编码',
  channel_name string comment '渠道',
  province_code string comment '省区编码',
  province_name string comment '销售省区',  
  manager_no string comment '主管工号',
  manager_name string comment '主管姓名',
  all_cust_count bigint comment '汇总累计客户数',
  all_daily_sale decimal(19, 6) comment '汇总昨日销售额',
  all_plan_sale decimal(19, 6) comment '汇总目标销售额',
  all_month_sale decimal(19, 6) comment '汇总累计销售额',
  all_sales_fill_rate decimal(19, 6) comment '汇总销售达成率',
  all_last_month_sale decimal(19, 6) comment '汇总环比累计销售额',
  all_mom_sale_growth_rate decimal(19, 6) comment '汇总销售环比增长率率',
  all_plan_profit decimal(19, 6) comment '汇总累计毛利额计划',
  all_month_profit decimal(19, 6) comment '汇总累计毛利额',
  all_month_profit_fill_rate decimal(19, 6) comment '汇总累计毛利额达成率',
  all_month_profit_rate decimal(19, 6) comment '汇总累计毛利率',
  old_cust_count bigint comment '老客累计客户数',
  old_daily_sale decimal(19, 6) comment '老客昨日销售额',
  old_plan_sale decimal(19, 6) comment '老客目标销售额',
  old_month_sale decimal(19, 6) comment '老客累计销售额',
  old_sales_fill_rate decimal(19, 6) comment '老客户销售达成率',
  old_last_month_sale decimal(19, 6) comment '老客环比累计销售额',
  old_mom_sale_growth_rate decimal(19, 6) comment '老客销售环比增长率率',
  old_month_profit decimal(19, 6) comment '老客累计毛利额',
  old_month_profit_rate decimal(19, 6) comment '老客累计毛利率',
  new_plan_sale_cust_num bigint comment '新客户数计划',
  new_cust_count bigint comment '新客累计客户数',
  new_cust_count_fill bigint comment '新客累计客户数达成情况',
  new_daily_sale decimal(19, 6) comment '新客昨日销售额',
  new_plan_sale decimal(19, 6) comment '新客目标销售额',
  new_month_sale decimal(19, 6) comment '新客累计销售额',
  new_month_sale_fill_rate decimal(19, 6) comment '新客累计销售额达成率',
  new_last_month_sale decimal(19, 6) comment '新客环比累计销售额',
  new_mom_sale_growth_rate decimal(19, 6) comment '新客销售环比增长率率',
  new_month_profit decimal(19, 6) comment '新客累计毛利额',
  new_month_profit_rate decimal(19, 6) comment '新客毛利率',
  update_time timestamp comment '更新日期'
) comment '大区销售主管经营业绩' 
partitioned by (months string comment '月分区') 
stored as parquet;
