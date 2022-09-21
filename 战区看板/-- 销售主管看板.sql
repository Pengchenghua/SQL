-- 销售主管看板
-- ******************************************************************** 
-- @功能描述：
-- @创建者： 彭承华 
-- @创建者日期：2022-08-23 17:11:13 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

drop table csx_analyse_tmp.csx_analyse_tmp_user;
create  table csx_analyse_tmp.csx_analyse_tmp_user as 
select
  *
from
(
  select
    user_id as sales_id,
    user_name as sales_user_name,
    user_number as sales_user_number,
    user_position as position,
    -- 服务管家
    first_value(case when leader_position_type = 'CUSTOMER_SERVICE_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as sales_service_manager_id,
    first_value(case when leader_position_type = 'CUSTOMER_SERVICE_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as        sales_service_manager_name,
    first_value(case when leader_position_type = 'CUSTOMER_SERVICE_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as sales_service_manager_work_no,
    -- 主管
    first_value(case when leader_position_type = 'SALES_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as sales_supervisor_user_id,
    first_value(case when leader_position_type = 'SALES_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as sales_supervisor_name,
    first_value(case when leader_position_type = 'SALES_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as sales_supervisor_work_no,
    -- 销售经理
    first_value(case when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as sales_manager_id,
    first_value(case when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as        sales_manager_name,
    first_value(case when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as sales_manager_work_no,
    -- 城市经理
    first_value(case when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as sales_city_manager_id,
    first_value(case when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as sales_city_manager_name,
    first_value(case when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as sales_city_manager_work_no,
    -- 省区总
    first_value(case when leader_position_type = 'AREA_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as district_manager_id,
    first_value(case when leader_position_type = 'AREA_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as district_manager_name,
    first_value(case when leader_position_type = 'AREA_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as district_manager_work_no,
    province_id,
    status,
    row_number() over(partition by user_id order by distance desc) as rank
  from csx_dim.csx_dim_uc_user_extend
  where sdt = 'current'
    and user_source_business=1
   -- and status=1
) tmp where tmp.rank = 1
and position is not null and position!=''

;
drop table  csx_analyse_tmp.csx_analyse_tmp_supervisor_sale_01;

create   table csx_analyse_tmp.csx_analyse_tmp_supervisor_sale_01
as 
select 
   a.performance_region_code
  ,a.performance_region_name
  ,a.performance_province_code
  ,a.performance_province_name
  ,a.performance_city_code
  ,a.performance_city_name
  ,a.channel_code
  ,a.channel_name
  ,a.sales_user_number
  ,a.sales_user_name
  ,a.supervisor_user_name
  ,a.supervisor_user_number
  ,sales_manager_user_number
  ,sales_manager_user_name
  ,a.customer_code
  ,a.customer_name
  ,a.order_kind
  ,business_type_name as sale_group, 
  if(substr(a.sign_time,1,6)=substr(a.sdt,1,6),'是', '否') is_new_sign,         -- 首次签约日期=本月
  if(substr(e.first_sale_date,1,6)=substr(a.sdt,1,6),'是', '否') is_new_sale,  -- 首次成交日期=本月
  sign_time,
  a.sales_value,
  a.profit,
  a.smonth,
  a.sdt as  sale_date,
  a.business_type_code,
  a.business_type_name
from
  (select performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name,
        channel_code ,
        a.business_type_code,
        a.business_type_name,
        channel_name,
        a.customer_code,
        sdt,
        case when a.performance_province_code='24' then b.sales_id_new else  m.sales_user_id end sales_user_id,
        case when a.performance_province_code='24' then b.sales_name_new else  m.sales_user_name end sales_user_name,
        case when a.performance_province_code='24' then b.work_no_new else  m.sales_user_number end sales_user_number,
        case when a.performance_province_code='24' then b.sales_name_new else m.supervisor_user_name end supervisor_user_name,
        case when a.performance_province_code='24' then b.work_no_new else m.supervisor_user_number end supervisor_user_number,
        case when a.performance_province_code='24' then coalesce(m.supervisor_user_id,m.sales_manager_user_id) else m.sales_manager_user_id end sales_manager_user_id,
        case when a.performance_province_code='24' then coalesce(m.supervisor_user_number,m.sales_manager_user_number) else m.sales_manager_user_number end  sales_manager_user_number,
        case when a.performance_province_code='24' then coalesce(m.supervisor_user_name,m.sales_manager_user_name) else m.sales_manager_user_name end sales_manager_user_name,
        customer_name,
        regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_time,
        case when (sdt>= regexp_replace(trunc('${edate}','MM'),'-','') and sdt<=regexp_replace('${edate}','-','' )  ) then '本月' else '环比月' end smonth,
        a.business_type_code as  order_kind,
        sum(sale_amt)sales_value,
        sum(sale_cost)sales_cost,
        sum(profit)profit
  from csx_dws.csx_dws_sale_detail_di a 
   left join 
  (select
  customer_code,
  sales_user_id,
  sales_user_number,
  sales_user_name,
  supervisor_user_id,
  supervisor_user_number,
  supervisor_user_name,
  sales_manager_user_id,
  sales_manager_user_number,
  sales_manager_user_name
from
  csx_dim.csx_dim_crm_customer_info
where sdt='current'
  ) m on a.customer_code=m.customer_code
   left join 
    (select customer_no,province_code,user_position_new,sales_name_new,a.work_no_new,sales_id_new
        from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a 
          where sdt=regexp_replace('${edate}','-','' )
            and user_position_new='销售员'
        )b  on a.customer_code=b.customer_no
  where (( sdt>= regexp_replace(trunc('${edate}','MM'),'-','') and sdt<=regexp_replace('${edate}','-','' ) )     -- 本月
        or (sdt>=regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','') 
            and sdt<= regexp_replace(if('${edate}'=last_day('${edate}'),last_day(add_months('${edate}',-1)),add_months('${edate}',-1)),'-','') ) )      -- 环比月
  and (order_code not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
            'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_code is null)
    and channel_code not in ('2','4','5','6')
  group by 
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        channel_code ,
        a.business_type_code,
        a.business_type_name,
        channel_name,
        a.customer_code,
        sdt,
        case when a.performance_province_code='24' then b.sales_id_new else  m.sales_user_id end  ,
        case when a.performance_province_code='24' then b.sales_name_new else  m.sales_user_name end  ,
        case when a.performance_province_code='24' then b.work_no_new else  m.sales_user_number end  ,
        case when a.performance_province_code='24' then b.sales_name_new else m.supervisor_user_name end   ,
        case when a.performance_province_code='24' then b.work_no_new else m.supervisor_user_number end  ,
        customer_name,
        regexp_replace(split(sign_time, ' ')[0], '-', '') ,
        case when (sdt>= regexp_replace(trunc('${edate}','MM'),'-','') and sdt<=regexp_replace('${edate}','-','' ) ) then '本月' else '环比月' end ,
        a.business_type_code,
        case when a.performance_province_code='24' then coalesce(m.supervisor_user_id,m.sales_manager_user_id) else m.sales_manager_user_id end  ,
        case when a.performance_province_code='24' then coalesce(m.supervisor_user_number,m.sales_manager_user_number) else m.sales_manager_user_number end   ,
        case when a.performance_province_code='24' then coalesce(m.supervisor_user_name,m.sales_manager_user_name) else m.sales_manager_user_name end
  )a
left join 
(select customer_code,  first_sale_date 
  from csx_dws.csx_dws_crm_customer_active_di
    where sdt=regexp_replace('${edate}','-','' )) as  e on e.customer_code=a.customer_code
-- where performance_province_code='24'
;


drop table csx_analyse_tmp.csx_analyse_tmp_manger_sale_01;
create  table csx_analyse_tmp.csx_analyse_tmp_manger_sale_01 as 
select
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    supervisor_user_number  manager_no ,
    supervisor_user_name as manager_name,
    sales_manager_user_number,
    sales_manager_user_name,
    coalesce(count(distinct case when smonth='本月' and is_new_sale='否' then customer_code end),0)as old_cust_count,    -- 老客-累计客户数
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then Md_sales_value end)/10000,0) as old_daily_sale,       -- 老客-昨日销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then sales_value end)/10000,0) as old_month_sale,          -- 老客-累计销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='否' then profit end)/10000,0) as old_month_profit,             -- 老客-累计毛利额
    coalesce(sum(case when smonth='环比月' and is_new_sale='否' then sales_value end)/10000,0) as old_last_month_sale,   -- 老客-环比累计销售额
    coalesce(count(distinct case when smonth='本月' and is_new_sale='是' then customer_code end),0)as new_cust_count,    -- 新客-累计客户数
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then Md_sales_value end)/10000,0) as new_daily_sale,       -- 新客-昨日销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then sales_value end)/10000,0) as new_month_sale,          -- 新客-累计销售额
    coalesce(sum(case when smonth='本月' and is_new_sale='是' then profit end)/10000,0) as new_month_profit,             -- 新客-累计毛利额
    coalesce(sum(case when smonth='环比月' and is_new_sale='是' then sales_value end)/10000,0) as new_last_month_sale,   -- 新客-环比累计销售额
    coalesce(sum(case when smonth='本月' then Md_sales_value end)/10000,0) as all_daily_sale,                            -- 汇总-昨日销售额
    coalesce(sum(case when smonth='本月' then sales_value end)/10000,0) as all_month_sale,                               -- 汇总-累计销售额
    coalesce(sum(case when smonth='本月' and sale_group !='批发内购' then sales_value end)/10000,0) as real_month_sale,  -- 汇总-累计销售额(剔除批发内购销售)
    coalesce(sum(case when smonth='本月' then profit end)/10000,0) as all_month_profit,                                     -- 汇总-累计毛利额
    coalesce(sum(case when smonth='环比月' then sales_value end)/10000,0) as all_last_month_sale,                           -- 汇总-环比累计销售额
    coalesce(sum(case when smonth='环比月' then profit end)/10000,0) as all_last_month_profit,                              -- 汇总-环比累计毛利额,
    0 as old_plan_sale,
    0 as new_plan_sale,
    0 as new_plan_sale_cust_num,
    0 as all_plan_sale,
    0 as all_plan_profit,
    0 as daily_sign_cust_num,
    0 as daily_sign_amount,
    0 as sign_cust_num,
    0 as sign_amount
from (SELECT
            channel_code,
            channel_name,
             performance_region_code,
             performance_region_name,
             performance_province_code,
             performance_province_name,
             performance_city_code,
             performance_city_name,
             a.supervisor_user_name,
             a.supervisor_user_number,
             sales_manager_user_number,
             sales_manager_user_name,
             a.customer_code,
             customer_name,
             order_kind,
            -- is_partner,
            -- ascription_type_name,
             sale_group,
             is_new_sale,
             is_new_sign,
             coalesce(case when sale_date= regexp_replace('${edate}','-','' ) and smonth='本月' then sales_value end,0)as Md_sales_value, --昨日销售额
             coalesce(sales_value,0)as sales_value,
             coalesce(profit,0) as profit,
             smonth
        FROM csx_analyse_tmp.csx_analyse_tmp_supervisor_sale_01 a 

    )a
        where 1=1
        group by performance_region_code,
                 performance_region_name,
                 performance_province_code,
                 performance_province_name,
                 performance_city_code,
                 performance_city_name,
                 supervisor_user_name,
                 supervisor_user_number,
                 sales_manager_user_number,           -- 销售经理工号
                 sales_manager_user_name       -- 销售经理
    ;
   
  -- 销售主管计划处理 
drop table csx_analyse_tmp.csx_analyse_tmp_plan_01;
 CREATE  table csx_analyse_tmp.csx_analyse_tmp_plan_01 as 
   SELECT 
            b.performance_region_code,
            b.performance_region_name,
            b.performance_province_code province_code,
            b.performance_province_name as province_name,
            city_group_code,
            city_group_name,
            coalesce(city_manager_job_no,d.sales_manager_work_no,'') sales_manager_no,
            coalesce(city_manager_name,d.sales_manager_name,'') sales_manager_name,
            coalesce(sales_service_manager_work_no,sales_user_number,'') manager_no,
            coalesce(a.manager_name,'')manager_name,
            coalesce(c.position,d.position) as position,
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
            0 as real_month_sale,   -- 不含批发内购销售
            0 as  all_month_profit,
            0 as  all_last_month_sale,
            0 all_last_month_profit,
            sum(case when customer_age_code ='1' then plan_sales_value end ) as old_plan_sale,
            sum(case when customer_age_code ='2' then plan_sales_value end ) as new_plan_sale,
            sum(case when customer_age_code ='2' then a.customer_count end ) as new_plan_sale_cust_num,
            coalesce(sum(plan_sales_value),0)all_plan_sale,
            sum(plan_profit)all_plan_profit,
            0 as daily_sign_cust_num,
            0 as daily_sign_amount,
            0 as sign_cust_num,
            0 as sign_amount
      FROM csx_ods.csx_ods_data_analysis_prd_dws_csms_manager_month_sale_plan_df a
      left join 
        (select distinct  
                sales_manager_name,
                sales_manager_work_no,
                sales_service_manager_name,
                sales_service_manager_work_no,
                province_id,
                position
            from  csx_analyse_tmp.csx_analyse_tmp_user
            where position in( 'CUSTOMER_SERVICE_MANAGER')
        ) c on regexp_replace( c.sales_service_manager_name ,' ','') = regexp_replace(coalesce(manager_name,'0'),' ','') and c.province_id=a.province_code
            left join 
        (select distinct  
                sales_manager_name,
                sales_manager_work_no,
                sales_user_name,
                sales_user_number,
                province_id,
                position
            from  csx_analyse_tmp.csx_analyse_tmp_user
            where 1=1
        ) d on regexp_replace( d.sales_user_name ,' ','') = regexp_replace(coalesce(manager_name,'0'),' ','') and d.province_id=a.province_code
      join 
      (select distinct performance_region_code,performance_region_name,performance_province_code,performance_province_name 
      from csx_dim.csx_dim_sales_area_belong_mapping
        where sdt='current') b on a.province_code=b.performance_province_code
      WHERE MONTH= substr(regexp_replace('${edate}','-','' ) ,1,6) 
      and sdt =substr(regexp_replace('${edate}','-','' ) ,1,6) 
        and customer_attribute_name!='批发内购'
       --  AND channel_name='大客户'
     --  and province_code='24'
       group by b.performance_region_code,
            b.performance_region_name,
            b.performance_province_code,
            b.performance_province_name ,
            city_group_code,
            city_group_name,
           coalesce(city_manager_job_no,d.sales_manager_work_no,'') ,
            coalesce(city_manager_name,d.sales_manager_name,'') ,
            coalesce(sales_service_manager_work_no,sales_user_number,'') ,
            coalesce(a.manager_name,''),
            coalesce(c.position,d.position) 
            ;
    
    
drop table csx_analyse_tmp.csx_analyse_tmp_manger_sale;
create   TABLE csx_analyse_tmp.csx_analyse_tmp_manger_sale AS
select 
   '1' channel_code,
   performance_region_code,
   performance_region_name,
   performance_province_code,
   performance_province_name,
   performance_city_code,
   performance_city_name,
   if(coalesce(sales_manager_name,'')='' ,'无经理',sales_manager_name) as sales_manager_name, 
   if(coalesce(sales_manager_no ,'')='' ,'88888888',sales_manager_no)  as sales_manager_work_no,   
   case when manager_name='' or manager_name is null then '88888888'else manager_no end  manager_no,  
   case when manager_name='' or manager_name is null then  '无主管'else manager_name end  manager_name, 
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
   coalesce(sum(all_last_month_profit),0) as all_last_month_profit,
   coalesce(sum(old_plan_sale),0)as old_plan_sale,
   coalesce(sum(new_plan_sale),0)as new_plan_sale,
   coalesce(sum(new_plan_sale_cust_num),0)as new_plan_sale_cust_num,
   coalesce(sum(all_plan_sale),0)as all_plan_sale,
   sum(all_plan_profit)all_plan_profit,
   sum(sign_cust_num) as new_sign_cust_num,     --新签约客户数
   sum(sign_amount) as new_sign_amount,  --新签约金额
   sum(daily_sign_cust_num) as daily_sign_cust_num,
   sum(daily_sign_amount) as daily_sign_amount
from 
(select 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    sales_manager_user_number sales_manager_no,  
    sales_manager_user_name   sales_manager_name,
    manager_no ,
    manager_name,
    old_cust_count,  --老客-累计客户数
    old_daily_sale, --老客-昨日销售额
    old_month_sale,  --老客-累计销售额
    old_month_profit,  --老客-累计毛利额
    old_last_month_sale,  --老客-环比累计销售额
    new_cust_count,  --新客-累计客户数
    new_daily_sale, --新客-昨日销售额
    new_month_sale,  --新客-累计销售额
    new_month_profit,  --新客-累计毛利额
    new_last_month_sale,  --新客-环比累计销售额
    all_daily_sale, --汇总-昨日销售额
    all_month_sale,  --汇总-累计销售额
    real_month_sale,  --汇总-累计销售额(剔除批发内购销售)
    all_month_profit,  --汇总-累计毛利额
    all_last_month_sale,  --汇总-环比累计销售额
    all_last_month_profit,
    0 as old_plan_sale,
    0 as new_plan_sale,
    0 as new_plan_sale_cust_num,
    0 as all_plan_sale,
    0 as all_plan_profit,
    0 as daily_sign_cust_num,
    0 as daily_sign_amount,
    0 as sign_cust_num,
    0 as sign_amount
from   csx_analyse_tmp.csx_analyse_tmp_manger_sale_01
    union all 
    SELECT 
        performance_region_code,
        performance_region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        sales_manager_no,
        sales_manager_name,
        case when position='CUSTOMER_SERVICE_MANAGER'then '' else manager_no end manager_no ,
        case when position='CUSTOMER_SERVICE_MANAGER'then '' else manager_name end  manager_name,
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
        0 all_last_month_profit,
        sum(old_plan_sale) as old_plan_sale,
        sum(new_plan_sale) as new_plan_sale,
        sum(new_plan_sale_cust_num) as new_plan_sale_cust_num,
        coalesce(sum(all_plan_sale),0)all_plan_sale,
        sum(all_plan_profit)all_plan_profit,
        0 as daily_sign_cust_num,
        0 as daily_sign_amount,
        0 as sign_cust_num,
        0 as sign_amount
      FROM csx_analyse_tmp.csx_analyse_tmp_plan_01
      group by performance_region_code,
        performance_region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        sales_manager_no,
        sales_manager_name,
        case when position='CUSTOMER_SERVICE_MANAGER'then '' else manager_no end  ,
        case when position='CUSTOMER_SERVICE_MANAGER'then '' else manager_name end  
    union all 
     SELECT
            b.performance_region_code,
            b.performance_region_name,
            b.performance_province_code,
            b.performance_province_name,
            a.performance_city_code,
            a.performance_city_name,
            a.sales_manager_user_number  as  sales_manager_no, 
            a.sales_manager_user_name    as sales_manager_name,
            case when b.performance_province_code='24' then work_no_new else a.supervisor_user_number end manager_no,
            case when b.performance_province_code='24' then sales_name_new else a.supervisor_user_name end as manager_name ,
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
            0 as all_last_month_profit,
            0 as old_plan_sale,
            0 as new_plan_sale,
            0 as new_plan_sale_cust_num,
            0 as all_plan_sale,
            0 as all_plan_profit,
            count(case when regexp_replace(to_date(sign_time),'-','') =regexp_replace('${edate}','-','' )  then a.customer_code end ) as daily_sign_cust_num,
            coalesce(sum(case when regexp_replace(to_date(sign_time),'-','') = regexp_replace('${edate}','-','' )  then estimate_contract_amount end ),0) as daily_sign_amount,
            count(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)=substr( regexp_replace('${edate}','-','' ) ,1,6) then a.customer_code end ) as sign_cust_num,
            coalesce(sum(case when substr(regexp_replace(to_date(sign_time),'-',''),1,6)= substr(regexp_replace('${edate}','-','' ) ,1,6) then estimate_contract_amount end ),0) as sign_amount
        from csx_dim.csx_dim_crm_customer_info a 
        left join 
        (select customer_no,province_code,user_position_new,sales_name_new,a.work_no_new
        from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a 
         where sdt='${edate}'
            and user_position_new='销售员'
        ) c on a.customer_code=c.customer_no and a.performance_province_code=c.province_code
        join 
        (select distinct performance_region_code,
            performance_region_name,
            performance_city_name,
            performance_city_code,
            performance_province_name,
            performance_province_code 
        from csx_dim.csx_dim_sales_area_belong_mapping ) b on a.performance_city_code=b.performance_city_code and a.performance_province_code=b.performance_province_code
        where sdt='current' 
         and a.supervisor_user_name !=''
        group by  
            b.performance_region_code,
            b.performance_region_name,
            b.performance_province_code,
            b.performance_province_name,
            a.performance_city_code,
            a.performance_city_name,
            a.sales_manager_user_name,
            a.sales_manager_user_number,
            case when b.performance_province_code='24' then work_no_new else a.supervisor_user_number end,
            case when b.performance_province_code='24' then sales_name_new else a.supervisor_user_name end           
    
)a 
   --  where performance_region_code='3'
    GROUP BY  
            performance_region_code,
            performance_region_name,
            performance_province_code,
            performance_province_name,
            performance_city_name,
            performance_city_code,
            if(coalesce(sales_manager_name,'')='' ,'无经理',sales_manager_name), 
            if(coalesce(sales_manager_no ,'')='' ,'88888888',sales_manager_no)  ,   
            case when manager_name='' or manager_name is null then '88888888'else manager_no end  ,  
            case when manager_name='' or manager_name is null then  '无主管'else manager_name end   
;




drop table csx_analyse_tmp.csx_analyse_tmp_manger_sale_02;
create   TABLE csx_analyse_tmp.csx_analyse_tmp_manger_sale_02 AS
select 
    '1' channel_code,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
   sales_manager_name,
   sales_manager_work_no,
   manager_no,
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
   coalesce(sum(all_last_month_profit),0) as all_last_month_profit,
   coalesce(sum(old_plan_sale),0)as old_plan_sale,
   coalesce(sum(new_plan_sale),0)as new_plan_sale,
   coalesce(sum(new_plan_sale_cust_num),0)as new_plan_sale_cust_num,
   coalesce(sum(all_plan_sale),0)as all_plan_sale,
   sum(all_plan_profit)all_plan_profit,
   sum(new_sign_cust_num) as new_sign_cust_num,     --新签约客户数
   sum(new_sign_amount) as new_sign_amount,  --新签约金额
    sum(daily_sign_cust_num) as daily_sign_cust_num,
    sum(daily_sign_amount) as daily_sign_amount,
   GROUPING__ID
from csx_analyse_tmp.csx_analyse_tmp_manger_sale
group by
performance_region_code,performance_region_name,performance_province_code,performance_province_name,manager_name, manager_no,performance_city_code,performance_city_name,sales_manager_name,sales_manager_work_no
grouping sets((performance_region_code,performance_region_name),                                    -- 大区汇总
             (performance_region_code,performance_region_name,performance_province_code,performance_province_name),         -- 省区汇总
             (performance_region_code,performance_region_name,performance_province_code,performance_province_name,sales_manager_name,sales_manager_work_no),            -- 销售经理汇总 
             (performance_region_code,performance_region_name,performance_province_code,performance_province_name,manager_name, manager_no,sales_manager_name,sales_manager_work_no), -- 省区销售主管
             (performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,sales_manager_name,sales_manager_work_no), -- 城市销售经理
             (performance_region_code,performance_region_name,performance_province_code,performance_province_name,manager_name, manager_no,performance_city_code,performance_city_name,sales_manager_name,sales_manager_work_no),
             (performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name) -- 城市汇总
)  
;


insert overwrite table csx_analyse.csx_analyse_fr_supervisor_sale_di partition(month) 
SELECT  
        case  
            when performance_province_code is null then 0
            when performance_city_code is null and sales_manager_name is null and manager_name is null    then 1      -- 省区汇总
            when performance_city_code is null and sales_manager_name is not null  and   manager_name is  null  then 2      -- 省区经理汇总
            when performance_city_code is null and   manager_name is not null  then 3       -- 省区主管
            when performance_city_code is not null and   manager_name is null AND sales_manager_work_no IS NULL   then 4
            when performance_city_code is not null  AND manager_name IS NULL  then 5
            when manager_name is null then 6
            else 7
        end level_id,
       substr(regexp_replace('${edate}','-','' ) ,1,6) as  sales_month,
        a.performance_region_code ,
        a.performance_region_name ,
       '1'  channel_code,
       '1' channel_name,
        case when performance_province_code is null then '00' else performance_province_code end performance_province_code,
        case when performance_province_name is null then concat(performance_region_name,'_合计') else  performance_province_name end performance_province_name,
        performance_city_code,
        performance_city_name,
        coalesce(sales_manager_work_no,'-') as sales_manager_number,
        case when sales_manager_name is null and performance_province_name is not null then '小计' else sales_manager_name end as sales_manager_name,
        coalesce(manager_no,'-')    supervisor_user_number,
        case when manager_name is null and a.sales_manager_name is not null  then '小计' else manager_name end supervisor_user_name,
        (new_cust_count+old_cust_count) as all_cust_count,
        all_daily_sale as all_yesterday_sale,
        all_plan_sale,
        all_month_sale,
        real_month_sale,
        coalesce( real_month_sale/all_plan_sale,0) as real_sales_fill_rate,
        coalesce( all_month_sale/all_plan_sale,0) as all_sales_fill_rate,
        all_last_month_sale,
        all_last_month_profit,
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
        new_sign_cust_num,     --新签约客户数
        new_sign_amount,  --新签约金额
        daily_sign_cust_num as yesterday_sign_cust_num,
        daily_sign_amount as yesterday_sign_amount,
        current_timestamp(),
        substr(regexp_replace('${edate}','-','' ) ,1,6) 
FROM csx_analyse_tmp.csx_analyse_tmp_manger_sale_02 a 
 where 1=1 
    -- and a.city_group_code='13' 
    -- and channel='1'
 order by 
    performance_region_code,case when   performance_province_code='00' then '9999' else   performance_province_code end desc,level_id asc ,
    case when manager_name like '虚%' then 9999 else 1 end asc;
