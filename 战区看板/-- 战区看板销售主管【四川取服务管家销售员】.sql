-- 战区看板销售主管【四川取服务管家销售员】
SET edt= '${enddate}';
-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;

--set tez.am.speculation.enabled=true;  --是否开启推测执行，默认是false，在出现最后一个任务很慢的情况下，建议把这个参数设置为true
--set tez.am.resource.memory.mb=8000;  --am分配的内存大小，默认1024
--set tez.task.resource.memory.mb=8000;  --分配的内存，默认1024 ,出现内存不够时候，设置更大点
--set tez.am.resource.cpu.vcores=8;  -- am分配的cpu个数，默认1
set hive.exec.parallel=true; 
set hive.exec.parallel.thread.number=100;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set hive.exec.dynamic.partition.mode=nonstrict;

set edate= regexp_replace(${hiveconf:edt},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');

SET l_edate=  regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','') ;

set l_sdate=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');

--select ${hiveconf:l_edate},${hiveconf:l_sdate};
set table_sale =csx_dw.dws_sale_r_d_detail;
set table_customer =csx_dw.dws_crm_w_a_customer;

drop    table csx_tmp.temp_supervisor_sale_01;
create temporary  table csx_tmp.temp_supervisor_sale_01
as 
select 
  c.region_code
  ,c.region_name
  ,a.province_code
  ,a.province_name
  ,a.city_group_code
  ,a.city_group_name
  ,a.channel
  ,a.channel_name
  ,b.work_no
  ,b.sales_name
  ,b.third_supervisor_name
  ,b.first_supervisor_name
  ,b.third_supervisor_work_no
  ,b.first_supervisor_work_no
  ,a.customer_no
  ,b.customer_name
  ,x.province_manager_id
  ,x.province_manager_name
  ,x.city_group_manager_id
  ,x.city_group_manager_name
  ,a.order_kind
  ,d.sales_belong_flag
  ,if( a.business_type_name='城市服务商','城市服务商',null) is_partner
  ,b.attribute_0
  ,g.ascription_type_name,
  --b.attribute,
  --case when a.channel in ('1','7') and a.order_kind='WELFARE' then '福利' else b.attribute end attribute,
    business_type_name as sale_group, 
  if(substr(b.sign_time,1,6)=substr(a.sdt,1,6),'是', '否') is_new_sign, --首次签约日期=本月
  if(substr(e.first_order_date,1,6)=substr(a.sdt,1,6),'是', '否') is_new_sale, --首次成交日期=本月
  sign_time,
  a.sales_value,
  a.profit,
  a.front_profit,
  a.smonth,
  a.sdt as  sale_date,
  ${hiveconf:edate} sdt
from
  (select province_code,
          province_name,
          city_group_code,
          city_group_name,
          channel_code as channel,
          a.business_type_code,
          a.business_type_name,
          channel_name,
          customer_no,
          sdt,
          case when (sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate}) then '本月' else '环比月' end smonth,
          a.business_type_code as  order_kind,
        sum(sales_value)sales_value,
        sum(sales_cost)sales_cost,
        sum(profit)profit,
        sum(front_profit)front_profit
  --from csx_dw.dws_sale_r_d_customer_sale 
  from csx_dw.dws_sale_r_d_detail a 
  -- ${hiveconf:table_sale}
  where (  ( sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate} ) --本月
      or (sdt>=${hiveconf:l_sdate} and sdt<=${hiveconf:l_edate}))--环比月
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
            'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
  group by province_code,province_name,city_group_code,city_group_name,a.channel_code,channel_name,customer_no,sdt,
  case when (sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate}) then '本月' else '环比月' end,
  business_type_code,a.business_type_name
  )a
left join 
  (select customer_no,
      customer_name,
      work_no,
      sales_name,
      third_supervisor_work_no,
      first_supervisor_work_no,
      third_supervisor_name,
      first_supervisor_name,
      attribute as attribute_0,
    regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_time,
    if(channel_code in('1','7','9'),case when attribute is null then '日配'  else attribute end,'') as  attribute
  -- from csx_dw.dws_crm_w_a_customer_m_v1
  from ${hiveconf:table_customer}
  where sdt ='current'
  --and customer_no='116756'
    --  and city_group_code='2'
  )b on b.customer_no=a.customer_no
left join (select distinct province_code,province_name,region_code  ,region_name from csx_dw.dws_sale_w_a_area_belong )c on c.province_code=a.province_code
left join (select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' ) d on a.customer_no=concat('S',d.shop_id)
left join (select customer_no,  first_order_date from csx_dw.dws_crm_w_a_customer_active where sdt=regexp_replace(${hiveconf:edt},'-','')) as  e on e.customer_no=a.customer_no
left join (SELECT DISTINCT province_code,
                province_name,
                region_code AS zone_id,
                region_name AS zone_name,
                province_manager_id,
                province_manager_name,
                city_group_code,
                city_group_name,
                city_group_manager_id,
                city_group_manager_name
FROM csx_dw.dws_sale_w_a_area_belong) x on x.province_code=a.province_code and x.city_group_code=a.city_group_code
left join (select shop_id from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current')g on concat('S',g.rt_shop_code)=a.customer_no
-- where a.city_group_code='2'
;


drop table csx_tmp.temp_manger_sale;
create temporary  TABLE csx_tmp.temp_manger_sale AS
select channel,
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
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
   coalesce(sum(real_month_sale),0)  as real_month_sale,
   coalesce(sum(all_month_profit),0) as  all_month_profit,
   coalesce(sum(all_last_month_sale),0) as  all_last_month_sale,
   coalesce(sum(old_plan_sale),0)as old_plan_sale,
   coalesce(sum(new_plan_sale),0)as new_plan_sale,
   coalesce(sum(new_plan_sale_cust_num),0)as new_plan_sale_cust_num,
   coalesce(sum(all_plan_sale),0)as all_plan_sale,
   sum(all_plan_profit)all_plan_profit,
   sum(sign_cust_num) as new_sign_cust_num,     --新签约数
   sum(sign_amount) as new_sign_amount,  --新签约金额
    sum(daily_sign_cust_num) as daily_sign_cust_num,
    sum(daily_sign_amount) as daily_sign_amount,
   GROUPING__ID
from 
(select channel_name_code as channel,
    channel_name_1 as channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    first_supervisor_name as manager_name,
    coalesce(count(distinct case when smonth='本月' and is_new_sale='否' then customer_no end),0)as old_cust_count,  --老客-累计数
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then Md_sales_value end)/10000,0) as old_daily_sale, --老客-昨日销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then sales_value end)/10000,0) as old_month_sale,  --老客-累计销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then profit end)/10000,0) as old_month_profit,  --老客-累计毛利额
    coalesce(sum(case when smonth='环比月' and is_new_sale='否' then sales_value end)/10000,0) as old_last_month_sale,  --老客-环比累计销售额
    coalesce(count(distinct case when smonth='本月' and is_new_sale='是' then customer_no end),0)as new_cust_count,  --新客-累计数
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then Md_sales_value end)/10000,0) as new_daily_sale, --新客-昨日销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then sales_value end)/10000,0) as new_month_sale,  --新客-累计销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then profit end)/10000,0) as new_month_profit,  --新客-累计毛利额
    coalesce(sum(case when smonth='环比月' and is_new_sale='是' then sales_value end)/10000,0) as new_last_month_sale,  --新客-环比累计销售额
    coalesce(sum(case when smonth='本月' then Md_sales_value end)/10000,0) as all_daily_sale, --汇总-昨日销售额
    coalesce(sum(case when smonth='本月' then sales_value end)/10000,0) as all_month_sale,  --汇总-累计销售额
    coalesce(sum(case when smonth='本月' and sale_group !='批发内购' then sales_value end)/10000,0) as real_month_sale,  --汇总-累计销售额(剔除批发内购销售)
    coalesce(sum(case when smonth='本月' then profit end)/10000,0) as all_month_profit,  --汇总-累计毛利额
    coalesce(sum(case when smonth='环比月' then sales_value end)/10000,0) as all_last_month_sale,  --汇总-环比累计销售额
    0 as old_plan_sale,
    0 as new_plan_sale,
    0 as new_plan_sale_cust_num,
    0 as all_plan_sale,
    0 as all_plan_profit,
    0 as daily_sign_cust_num,
    0 as daily_sign_amount,
    0 as sign_cust_num,
    0 as sign_amount
from (SELECT channel,
             channel_name,
             region_code,
             region_name,
             a.province_code,
             province_name,
             city_group_code,
             city_group_name,
             third_supervisor_name,
             coalesce(case when a.province_code='24' then sales_name_new else  first_supervisor_name end,'')as first_supervisor_name,
             a.customer_no,
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
             is_new_sign,
             coalesce(case when sale_date=${hiveconf:edate} and smonth='本月' then sales_value end,0)as Md_sales_value, --昨日销售额
             coalesce(sales_value,0)as sales_value,
             coalesce(profit,0) as profit,
             smonth,
             substr(sdt,1,6)as months,
             case when channel_name='商超' then '商超'
        when channel_name='大' or channel_name like '企业购%' then '大'
        else channel_name end channel_name_1,
      case when channel_name='商超' then '2'
        when channel_name='大' or channel_name like '企业购%' then '1'
        else channel end channel_name_code
        FROM csx_tmp.temp_supervisor_sale_01 a 
        left join 
        (select customer_no,province_code,user_position_new,sales_name_new
        from csx_tmp.report_crm_w_a_customer_service_manager_info_business
        where month=substr(${hiveconf:edate},1,6)
            and user_position_new='销售员') b on a.customer_no=b.customer_no and a.province_code=b.province_code
  
    )a
        where 1=1
         --   and channel_name_1='B端'
        group by region_code,
                 region_name,
                 province_code,
                 province_name,
                 city_group_name,
                 first_supervisor_name ,
                 channel_name_code,
                 channel_name_1,
                 city_group_code,
                 city_group_name
    union all 
     SELECT a.channel,
            a.channel_name,
            b.region_code,
            b.region_name,
            b.province_code,
            b.province_name as province_name,
            city_group_code,
            case when city_group_name like '攀枝%' then '攀枝花市' else city_group_name end city_group_name,
            coalesce(a.manager_name,'')manager_name,
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
            0 as real_month_sale,   --不含批发内购销售
            0 as  all_month_profit,
            0 as  all_last_month_sale,
            sum(case when customer_age_code ='1' then plan_sales_value end ) as old_plan_sale,
            sum(case when customer_age_code ='2' then plan_sales_value end ) as new_plan_sale,
            sum(case when customer_age_code ='2' then a.customer_count end ) as new_plan_sale_cust_num,
            coalesce(sum(plan_sales_value),0)all_plan_sale,
            sum(plan_profit)all_plan_profit,
            0 as daily_sign_cust_num,
            0 as daily_sign_amount,
            0 as sign_cust_num,
            0 as sign_amount
      FROM csx_tmp.dws_csms_manager_month_sale_plan_tmp a
      join 
      (select distinct region_code,region_name,province_code,province_name from csx_dw.dws_sale_w_a_area_belong) b on a.province_code=b.province_code
      WHERE MONTH= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
      and sdt =substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
      and customer_attribute_name!='批发内购'
       --  AND channel_name='大'
      GROUP BY b.region_code,
            b.region_name,
            b.province_code,
            b.province_name,
            manager_name,
            a.channel,
            a.channel_name,
            city_group_code,
            city_group_name
    union all 
     SELECT a.channel_code as channel,
            a.channel_name,
            b.region_code,
            b.region_name,
            b.province_code,
            b.province_name,
            a.city_group_code,
            a.city_group_name,
            case when b.province_code='24' then sales_name_new else a.first_supervisor_name end as manager_name ,
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
            0 as real_month_sale,   --不含批发内购销售
            0 as  all_month_profit,
            0 as  all_last_month_sale,
            0 as old_plan_sale,
            0 as new_plan_sale,
            0 as new_plan_sale_cust_num,
            0 as all_plan_sale,
            0 as all_plan_profit,
            count(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace(${hiveconf:edate},'-','') then a.customer_no end ) as daily_sign_cust_num,
            coalesce(sum(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace(${hiveconf:edate},'-','') then estimate_contract_amount end ),0) as daily_sign_amount,
            count(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) then a.customer_no end ) as sign_cust_num,
            coalesce(sum(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr(regexp_replace(${hiveconf:edate},'-',''),1,6) then estimate_contract_amount end ),0) as sign_amount
        from csx_dw.dws_crm_w_a_customer a 
        left join 
        (select customer_no,province_code,user_position_new,sales_name_new
        from csx_tmp.report_crm_w_a_customer_service_manager_info_business
        where month=substr(${hiveconf:edate},1,6)
            and user_position_new='销售员') c on a.customer_no=c.customer_no and a.sales_province_code=c.province_code
        join 
        (select distinct region_code,
            region_name,
            city_group_code,
            city_group_name,
            province_code,
            province_name 
        from csx_dw.dws_sale_w_a_area_belong ) b on a.city_group_code=b.city_group_code and a.sales_province_code=b.province_code
        where sdt='current' 
         and a.first_supervisor_code!=''
        group by  a.channel_code,
            a.channel_name,
            b.region_code,
            b.region_name,
            b.province_code,
            b.province_name,
            a.city_group_code,
            a.city_group_name,
            case when b.province_code='24' then sales_name_new else a.first_supervisor_name end
            
    
)a 
GROUP BY  
            region_code,
            region_name,
            province_code,
            province_name,
            manager_name,
            channel,
            channel_name,
            city_group_code,
            city_group_name
grouping sets((region_code,region_name),
             (region_code,region_name,channel_name,channel),
             (region_code,region_name,province_code,province_name),
             (region_code,region_name,province_code,province_name,channel_name,channel),
             (region_code,region_name,province_code,province_name,channel_name,channel,city_group_code,city_group_name),
             (region_code,region_name,province_code,province_name,channel_name,channel,manager_name),
             (region_code,region_name,province_code,province_name,channel_name,channel,manager_name,city_group_code,city_group_name))
;

insert overwrite table csx_tmp.ads_sale_r_d_zone_supervisor_fr partition(sdt) 
SELECT  
        case  when channel is null   then '0'
            when province_code is null then '1'
            when city_group_code is null and manager_name is null  then '2'
            when city_group_code is null    then '3'
            when manager_name is null then '4'
            else '5'
        end level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_month,
        a.region_code as zone_id,
        a.region_name as zone_name,
        case when  channel is null then '0' else channel end channel,
        case when  channel is null then concat(region_name,'_全渠道') else channel_name end channel_name,
        case when province_code is null then '00' else province_code end province_code,
        case when province_name is null then concat(region_name,'_合计') else  province_name end province_name,
        city_group_code,
        city_group_name,
        first_supervisor_work_no as  manager_no,
        case when manager_name is null then '小计' else manager_name end manager_name,
        (new_cust_count+old_cust_count) as all_cust_count,
        all_daily_sale,
        all_plan_sale,
        all_month_sale,
        real_month_sale,
        coalesce( real_month_sale/all_plan_sale,0) as real_sales_fill_rate,
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
        new_sign_cust_num,     --新签约数
        new_sign_amount,  --新签约金额
        daily_sign_cust_num,
        daily_sign_amount,
        current_timestamp(),
        regexp_replace(${hiveconf:edate},'-','')
FROM csx_tmp.temp_manger_sale a 
left join 
    (select distinct first_supervisor_name,'' as first_supervisor_work_no,sales_province_code 
    from csx_dw.dws_crm_w_a_customer
    where sdt='current') as b 
    on a.province_code=b.sales_province_code and trim(a.manager_name)=trim(b.first_supervisor_name)
  --  where a.city_group_code='13' 
    -- and channel='1'
 order by 
    zone_id,case when   province_code='00' then '9999' else   province_code end desc,level_id asc ,
    case when manager_name like '虚%' then 9999 else 1 end asc;

