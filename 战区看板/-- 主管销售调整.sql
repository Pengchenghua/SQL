-- 主管销售调整
SET edt= '${enddate}';
set hive.exec.parallel=true; 
set hive.exec.parallel.thread.number=100;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set hive.exec.dynamic.partition.mode=nonstrict;
-- set hive.exec.compress.intermediate=true --启用中间数据压缩
-- SET hive.exec.compress.output=true; -- 启用最终数据输出压缩
-- set mapreduce.output.fileoutputformat.compress=true; --启用reduce输出压缩
-- set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec --设置reduce输出压缩格式
-- set mapreduce.map.output.compress=true; --启用map输入压缩
-- set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec；-- 设置map输出压缩格式
-- set parquet.compression=snappy;

set edate= regexp_replace(${hiveconf:edt},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
SET l_edate=  regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','') ;
set l_sdate=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');

--select ${hiveconf:l_edate},${hiveconf:l_sdate};

set table_sale =csx_dw.dws_sale_r_d_detail;
set table_customer =csx_dw.dws_crm_w_a_customer;
drop table csx_tmp.temp_user;
create temporary table csx_tmp.temp_user as 
select
  *
from
(
  select
    id as sales_id,
    name as sales_name,
    user_number as work_no,
    user_position as position,
    -- 服务管家
    first_value(case when leader_user_position = 'CUSTOMER_SERVICE_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_service_manager_id,
    first_value(case when leader_user_position = 'CUSTOMER_SERVICE_MANAGER' then leader_name end, true) over(partition by id order by distance) as        sales_service_manager_name,
    first_value(case when leader_user_position = 'CUSTOMER_SERVICE_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_service_manager_work_no,
    -- 主管
    first_value(case when leader_user_position = 'SALES_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_supervisor_id,
    first_value(case when leader_user_position = 'SALES_MANAGER' then leader_name end, true) over(partition by id order by distance) as sales_supervisor_name,
    first_value(case when leader_user_position = 'SALES_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_supervisor_work_no,
    -- 销售经理
    first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_manager_id,
    first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_name end, true) over(partition by id order by distance) as        sales_manager_name,
    first_value(case when leader_user_position = 'SALES_CITY_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_manager_work_no,
    -- 城市经理
    first_value(case when leader_user_position = 'SALES_PROV_MANAGER' then leader_id end, true) over(partition by id order by distance) as sales_city_manager_id,
    first_value(case when leader_user_position = 'SALES_PROV_MANAGER' then leader_name end, true) over(partition by id order by distance) as sales_city_manager_name,
    first_value(case when leader_user_position = 'SALES_PROV_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as sales_city_manager_work_no,
    -- 省区总
    first_value(case when leader_user_position = 'AREA_MANAGER' then leader_id end, true) over(partition by id order by distance) as district_manager_id,
    first_value(case when leader_user_position = 'AREA_MANAGER' then leader_name end, true) over(partition by id order by distance) as district_manager_name,
    first_value(case when leader_user_position = 'AREA_MANAGER' then leader_user_number end, true) over(partition by id order by distance) as district_manager_work_no,
    prov_code,
    status,
    row_number() over(partition by id order by distance desc) as rank
  from csx_dw.dwd_uc_w_a_user_adjust
  where sdt = 'current'
    and user_source_busi=1
   -- and status=1
) tmp where tmp.rank = 1
and position is not null and position!=''

;


drop    table csx_tmp.temp_supervisor_sale_01;
create temporary  table csx_tmp.temp_supervisor_sale_01
as 
select 
  region_code
  ,region_name
  ,a.province_code
  ,a.province_name
  ,a.city_group_code
  ,a.city_group_name
  ,a.channel_code
  ,a.channel_name
  ,a.work_no
  ,a.sales_name
  ,a.supervisor_name
  ,a.supervisor_work_no
  ,sales_manager_no  
  ,sales_manager_name
  ,a.customer_no
  ,a.customer_name
  ,a.order_kind
  ,business_type_name as sale_group, 
  if(substr(a.sign_time,1,6)=substr(a.sdt,1,6),'是', '否') is_new_sign,         -- 首次签约日期=本月
  if(substr(e.first_order_date,1,6)=substr(a.sdt,1,6),'是', '否') is_new_sale,  -- 首次成交日期=本月
  sign_time,
  a.sales_value,
  a.profit,
  a.front_profit,
  a.smonth,
  a.sdt as  sale_date,
  a.business_type_code,
  a.business_type_name
from
  (select region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        channel_code ,
        a.business_type_code,
        a.business_type_name,
        channel_name,
        a.customer_no,
        sdt,
        sales_name,
        work_no,
        supervisor_name,
        supervisor_work_no,
        sales_manager_no,           -- 销售经理工号
        sales_manager_name,         -- 销售经理
        customer_name,
        regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_time,
        case when (sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate}) then '本月' else '环比月' end smonth,
        a.business_type_code as  order_kind,
        sum(sales_value)sales_value,
        sum(sales_cost)sales_cost,
        sum(profit)profit,
        sum(front_profit)front_profit
  from csx_dw.dws_sale_r_d_detail a 
    left join (select customer_no,second_supervisor_work_no as sales_manager_no,second_supervisor_name as sales_manager_name,
                     first_supervisor_work_no,
                     first_supervisor_name 
                from csx_dw.dws_crm_w_a_customer where sdt='current') b on a.customer_no=b.customer_no
      where (  ( sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate} ) --本月
        or (sdt>=${hiveconf:l_sdate} and sdt<=${hiveconf:l_edate}))--环比月
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
            'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
  group by province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.channel_code,
    channel_name,
    a.customer_no,
    sdt,
    sales_manager_no,  
    sales_manager_name,
    sales_name,
    work_no,
    a.supervisor_name,
    supervisor_work_no,
    customer_name,
    case when (sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate}) then '本月' else '环比月' end,
    business_type_code,
    a.business_type_name,
    regexp_replace(split(sign_time, ' ')[0], '-', ''),
    region_code,
    region_name
  )a
left join (select customer_no,  first_order_date from csx_dw.dws_crm_w_a_customer_active where sdt=regexp_replace(${hiveconf:edt},'-','')) as  e on e.customer_no=a.customer_no

;

drop table csx_tmp.temp_manger_sale_01;
create temporary table csx_tmp.temp_manger_sale_01 as 
select 
    
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    supervisor_work_no manager_no ,
    supervisor_name as manager_name,
    sales_manager_no,  
    sales_manager_name,
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
    coalesce(sum(case when smonth='本月' and sale_group !='批发内购' then sales_value end)/10000,0) as real_month_sale,  --汇总-累计销售额(剔除批发内购销售)
    coalesce(sum(case when smonth='本月' then profit end)/10000,0) as all_month_profit,  --汇总-累计毛利额
    coalesce(sum(case when smonth='环比月' then sales_value end)/10000,0) as all_last_month_sale,  --汇总-环比累计销售额
    coalesce(sum(case when smonth='环比月' then profit end)/10000,0) as all_last_month_profit,  --汇总-环比累计毛利额,
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
             region_code,
             region_name,
             a.province_code,
             province_name,
             city_group_code,
             city_group_name,
             coalesce(case when a.province_code='24' then work_no_new else  a.supervisor_work_no end,'') supervisor_work_no,
             coalesce(case when a.province_code='24' then sales_name_new else  supervisor_name end,'')as supervisor_name,
             sales_manager_no,  
            sales_manager_name,
             a.customer_no,
             customer_name,
             order_kind,
            -- is_partner,
            -- ascription_type_name,
             sale_group,
             is_new_sale,
             is_new_sign,
             coalesce(case when sale_date=${hiveconf:edate} and smonth='本月' then sales_value end,0)as Md_sales_value, --昨日销售额
             coalesce(sales_value,0)as sales_value,
             coalesce(profit,0) as profit,
             smonth
        FROM csx_tmp.temp_supervisor_sale_01 a 
        left join 
        (select customer_no,province_code,user_position_new,sales_name_new,a.work_no_new
        from csx_tmp.report_crm_w_a_customer_service_manager_info_business a 
          where month=substr(${hiveconf:edate},1,6)
            and user_position_new='销售员'
        ) b on a.customer_no=b.customer_no and a.province_code=b.province_code
             where   a.channel_code in('1','7','9')

    )a
        where 1=1
        group by region_code,
                 region_name,
                 province_code,
                 province_name,
                 city_group_name,
                 sales_manager_no,  
                sales_manager_name,
                 supervisor_name,
                 supervisor_work_no ,
                 city_group_code,
                 city_group_name 
    ;
   
  -- 销售主管计划处理 
  drop table csx_tmp.temp_plan_01;
 CREATE temporary table csx_tmp.temp_plan_01 as 
   SELECT 
            b.region_code,
            b.region_name,
            b.province_code,
            b.province_name as province_name,
            city_group_code,
            city_group_name,
            coalesce(c.sales_manager_work_no,d.sales_manager_work_no) sales_manager_no,
            coalesce(c.sales_manager_name,d.sales_manager_name) sales_manager_name,
            coalesce(sales_service_manager_work_no,work_no) manager_no,
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
      FROM csx_tmp.dws_csms_manager_month_sale_plan_tmp a
      left join 
        (select distinct  
                sales_manager_name,
                sales_manager_work_no,
                sales_service_manager_name,
                sales_service_manager_work_no,
                prov_code
            from  csx_tmp.temp_user
            where position in( 'CUSTOMER_SERVICE_MANAGER')
        ) c on regexp_replace( c.sales_service_manager_name ,' ','') = regexp_replace(coalesce(manager_name,'0'),' ','') and c.prov_code=a.province_code
            left join 
        (select distinct  
                sales_manager_name,
                sales_manager_work_no,
                sales_name,
                work_no,
                prov_code
            from  csx_tmp.temp_user
            where position in( 'SALES')
        ) d on regexp_replace( d.sales_name ,' ','') = regexp_replace(coalesce(manager_name,'0'),' ','') and d.prov_code=a.province_code
      join 
      (select distinct region_code,region_name,province_code,province_name from csx_dw.dws_sale_w_a_area_belong) b on a.province_code=b.province_code
    
      WHERE MONTH= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
      and sdt =substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
        and customer_attribute_name!='批发内购'
       --  AND channel_name='大客户'
      GROUP BY b.region_code,
            b.region_name,
            b.province_code,
            b.province_name ,
            city_group_code,
            city_group_name,
            coalesce(c.sales_manager_work_no,d.sales_manager_work_no) ,
            coalesce(c.sales_manager_name,d.sales_manager_name) ,
            coalesce(sales_service_manager_work_no,work_no) ,
            coalesce(a.manager_name,'')
            ;
    
    
drop table csx_tmp.temp_manger_sale;
create temporary  TABLE csx_tmp.temp_manger_sale AS
select 
   '1' channel_code,
   region_code,
   region_name,
   province_code,
   province_name,
   city_group_code,
   city_group_name,
   if(sales_manager_name is null ,'无经理',sales_manager_name) as sales_manager_name, 
   if(sales_manager_no is null ,'88888888',sales_manager_no)  as sales_manager_work_no,   
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
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    sales_manager_no,  
    sales_manager_name,
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
from   csx_tmp.temp_manger_sale_01
    union all 
    SELECT 
            b.region_code,
            b.region_name,
            b.province_code,
            b.province_name as province_name,
            city_group_code,
            case when city_group_name like '攀枝%' then '攀枝花市' else city_group_name end city_group_name,
            sales_manager_work_no sales_manager_no,
            sales_manager_name,
            sales_supervisor_work_no manager_no,
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
      FROM csx_tmp.dws_csms_manager_month_sale_plan_tmp a
      join 
      (select distinct region_code,region_name,province_code,province_name from csx_dw.dws_sale_w_a_area_belong) b on a.province_code=b.province_code
      left join 
        (select distinct case when prov_code='24' then sales_name else sales_supervisor_name end sales_supervisor_name,
                case when prov_code='24' then work_no else sales_supervisor_work_no end sales_supervisor_work_no,
                sales_manager_name,
                sales_manager_work_no,
                prov_code
            from  csx_tmp.temp_user
            --  where prov_code='24'
        ) c on regexp_replace( c.sales_supervisor_name ,' ','') = regexp_replace(coalesce(manager_name,'0'),' ','') and c.prov_code=a.province_code
      WHERE MONTH= substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
      and sdt =substr(regexp_replace(${hiveconf:edate},'-',''),1,6) 
        and customer_attribute_name!='批发内购'
       --  AND channel_name='大客户'
      GROUP BY b.region_code,
            b.region_name,
            b.province_code,
            b.province_name,
            sales_supervisor_work_no,
            manager_name,
            sales_manager_name,
            sales_manager_work_no,
            city_group_code,
            city_group_name
    union all 
     SELECT
            b.region_code,
            b.region_name,
            b.province_code,
            b.province_name,
            a.city_group_code,
            a.city_group_name,
            a.second_supervisor_work_no  as  sales_manager_no, 
            a.second_supervisor_name    as sales_manager_name,
            case when b.province_code='24' then work_no_new else a.first_supervisor_work_no end manager_no,
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
            0 as all_last_month_profit,
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
        (select customer_no,province_code,user_position_new,sales_name_new,a.work_no_new
        from csx_tmp.report_crm_w_a_customer_service_manager_info_business a 
         where month=substr(${hiveconf:edate},1,6)
            and user_position_new='销售员'
        ) c on a.customer_no=c.customer_no and a.sales_province_code=c.province_code
        join 
        (select distinct region_code,
            region_name,
            city_group_code,
            city_group_name,
            province_code,
            province_name 
        from csx_dw.dws_sale_w_a_area_belong ) b on a.city_group_code=b.city_group_code and a.sales_province_code=b.province_code
        where sdt='current' 
         and a.first_supervisor_code !=''
        group by  
            b.region_code,
            b.region_name,
            b.province_code,
            b.province_name,
            a.city_group_code,
            a.city_group_name,
            a.second_supervisor_work_no,
            a.second_supervisor_name,
            case when b.province_code='24' then work_no_new else a.first_supervisor_work_no end,
            case when b.province_code='24' then sales_name_new else a.first_supervisor_name end           
    
)a 
   --  where region_code='3'
    GROUP BY  
            region_code,
            region_name,
            province_code,
            province_name,
            manager_name,
            manager_no,
            city_group_code,
            city_group_name,
            sales_manager_name,
            sales_manager_no 
;


drop table csx_tmp.temp_manger_sale_02;
create temporary  TABLE csx_tmp.temp_manger_sale_02 AS
select 
    '1' channel_code,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
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
from csx_tmp.temp_manger_sale
group by
region_code,region_name,province_code,province_name,manager_name, manager_no,city_group_code,city_group_name,sales_manager_name,sales_manager_work_no
grouping sets((region_code,region_name),                                    --大区汇总
             (region_code,region_name,province_code,province_name),         --省区汇总
             (region_code,region_name,province_code,province_name,sales_manager_name,sales_manager_work_no),            --销售经理汇总 
             (region_code,region_name,province_code,province_name,manager_name, manager_no,sales_manager_name,sales_manager_work_no), --省区销售主管
             (region_code,region_name,province_code,province_name,city_group_code,city_group_name,sales_manager_name,sales_manager_work_no), --城市销售经理
             (region_code,region_name,province_code,province_name,manager_name, manager_no,city_group_code,city_group_name,sales_manager_name,sales_manager_work_no))  
;


insert overwrite table csx_tmp.ads_sale_r_d_zone_supervisor_fr_back partition(sdt) 
SELECT  
        case  
            when province_code is null then '0'
            when city_group_code is null and sales_manager_name is null and manager_name is null and grouping__id='15'  then '1'      --省区汇总
            when city_group_code is null and   manager_name is null    then '2'
            when city_group_code is null    then '3'
            when manager_name is null then '4'
            else '5'
        end level_id,
       substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as  sales_month,
        a.region_code as zone_id,
        a.region_name as zone_name,
       '1'  channel,
       '1' channel_name,
        case when province_code is null then '00' else province_code end province_code,
        case when province_name is null then concat(region_name,'_合计') else  province_name end province_name,
        city_group_code,
        city_group_name,
        coalesce(sales_manager_work_no,'-') as sales_manager_no,
        case when sales_manager_name is null and province_name is not null then '小计' else sales_manager_name end as sales_manager_name,
        coalesce(manager_no,'-')manager_no,
        case when manager_name is null and a.sales_manager_name is not null  then '小计' else manager_name end manager_name,
        (new_cust_count+old_cust_count) as all_cust_count,
        all_daily_sale,
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
        daily_sign_cust_num,
        daily_sign_amount,
        current_timestamp(),
        regexp_replace(${hiveconf:edate},'-','')
FROM csx_tmp.temp_manger_sale_02 a 
 where 1=1 
    -- and a.city_group_code='13' 
    -- and channel='1'
 order by 
    zone_id,case when   province_code='00' then '9999' else   province_code end desc,level_id asc ,
    case when manager_name like '虚%' then 9999 else 1 end asc;
