-- ******************************************************************** 
-- @功能描述：大区经营看板-商超业态分析
-- @创建者： 彭承华 
-- @创建者日期：2022-08-25 14:56:48 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
set hive.exec.dynamic.partition.mode=nonstrict;


-- 商超查询数据 20200730
drop table if exists csx_analyse_tmp.csx_analyse_tmp_supper_sale_00;
create  table csx_analyse_tmp.csx_analyse_tmp_supper_sale_00
as 
select
    a.performance_province_code ,
    case when  performance_province_code not in ('32','24') and shop_name like '%代加工%' then '2' else '1' end  process_type_code,
    case when  performance_province_code not in ('32','24') and shop_name like '%代加工%' then '代加工' else '非代加工' end process_type,
    coalesce(case
        when customer_code in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活'
        when (a.customer_code IN ('S9961','S99A0','S9996','SW098','S99A7') or sales_belong_flag='1_云超') then '1_云超'
        else sales_belong_flag
    end,'其他') as format_type,
    sum(daily_sale_value )as daily_sale_value,
    sum(daily_profit) as daily_profit,
    sum(daily_profit)/sum(daily_sale_value ) as daily_profit_rate,
    sum(month_sale) month_sale,
    sum(last_month_sale)  as last_month_sale,
    sum(month_profit )month_profit ,
    sum(month_profit )/sum(month_sale )as month_profit_rate,
    sum(last_month_profit)  as last_month_profit,
    sum(daily_plan_sale) daily_plan_sale
from
(
select
    performance_province_code ,
    inventory_dc_code ,
    customer_code ,
    sum(case when sdt=regexp_replace('${edate}','-','')  then sale_amt end )as daily_sale_value,
    sum(case when sdt=regexp_replace('${edate}','-','')  then profit end )as daily_profit,
    sum(sale_amt) month_sale,
    sum(profit )month_profit ,
    0 as last_month_sale,
    0 as last_month_profit,
    0 as daily_plan_sale,
    0 as month_plan_sale,
    0 as month_plan_profit
from
    csx_dws.csx_dws_sale_detail_di as a
where
    sdt <= regexp_replace('${edate}','-','') 
    and  sdt >= regexp_replace(trunc('${edate}','MM'),'-','') 
    and a.channel_code = '2'
   -- and performance_province_code in ('32','23','24')
  group by 
    performance_province_code ,
    a.inventory_dc_code ,
   -- case when a.dc_code in('W0M6','W0S8','W0T7') then '代加工' else '非代加工' end,
    customer_code 
union all 
select 
    performance_province_code ,
     a.inventory_dc_code  ,
    customer_code ,
    0 as daily_sale_value,
    0 as daily_profit,
    0 as month_sale,
    0 as month_profit ,
    sum(sale_amt) last_month_sale,
    sum(profit ) last_month_profit ,
    0 daily_plan_sale,
    0 as month_plan_sale,
    0 as month_plan_profit
from
    csx_dws.csx_dws_sale_detail_di as a

where
    sdt >= regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','') 
    and  sdt <= regexp_replace(if('${edate}'=last_day('${edate}'),last_day(add_months('${edate}',-1)),add_months('${edate}',-1)),'-','')  
    and a.channel_code = '2'
   -- and performance_province_code in ('32','23','24')
group by 
    performance_province_code ,
     a.inventory_dc_code,
     customer_code 
)a 
left join 
(
    select
        concat('S', shop_code)shop_code, 
        sales_belong_flag
    from
        csx_dim.csx_dim_shop a
    where
        sdt = 'current') b on a.customer_code = b.shop_code
left join 
(select shop_code,shop_name from csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code
group by   a.performance_province_code ,
    case when  performance_province_code not in ('32','24') and  shop_name like '%代加工%' then '2' else '1' end  ,
    case when  performance_province_code not in ('32','24') and  shop_name like '%代加工%' then '代加工' else '非代加工' end ,
    coalesce(case
        when customer_code in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活'
        when (a.customer_code IN ('S9961','S99A0','S9996','SW098','S99A7') or sales_belong_flag='1_云超') then '1_云超'
        else sales_belong_flag
    end,'其他') 
;
-- 销售关联计划表
drop table if exists csx_analyse_tmp.csx_analyse_tmp_supper_sale;
create  table csx_analyse_tmp.csx_analyse_tmp_supper_sale
as 
select
    a.performance_province_code ,
    process_type_code,
    process_type,
    format_type,
    sum(daily_sale_value )as daily_sale_value,
    sum(daily_profit) as daily_profit,
    sum(daily_profit)/sum(daily_sale_value ) as daily_profit_rate,
    sum(month_sale) month_sale,
    sum(last_month_sale)  as last_month_sale,
    sum(month_profit )month_profit ,
    sum(month_profit )/sum(month_sale )as month_profit_rate,
    sum(last_month_profit)  as last_month_profit,
    sum(daily_plan_sale) daily_plan_sale,
    sum(month_plan_sale) as month_plan_sale,
    sum(month_plan_profit) as month_plan_profit
from (
select performance_province_code,
     process_type_code, 
     process_type,
     format_type,
     daily_sale_value,
     daily_profit,
     month_sale,
     month_profit ,
     last_month_sale,
     last_month_profit ,
     daily_plan_sale,
     0 month_plan_sale,
     0 month_plan_profit
from
csx_analyse_tmp.csx_analyse_tmp_supper_sale_00
union all 
SELECT province_code,
          case when process_type='代加工' then '2' else '1' end    as process_type_code, 
          process_type,
          format_name as format_type,
            0 as daily_sale_value,
            0 as daily_profit,
            0 as month_sale,
            0 as month_profit ,
            0 as last_month_sale,
            0 as last_month_profit ,
            0 as daily_plan_sale,
          sum(plan_sales_value)month_plan_sale,
          sum(plan_profit)month_plan_profit
   FROM csx_analyse_tmp.dws_ssms_province_month_sale_plan_tmp
   WHERE MONTH=     substr(regexp_replace('${edate}','-',''),1,6) 
		and	sdt=    substr(regexp_replace('${edate}','-',''),1,6) 
   	and (weeknum is null or weeknum='')
   GROUP BY performance_province_code,
            format_name,
            process_type,
            case when process_type='代加工' then '2' else '1' end
)a 
group by a.performance_province_code ,
    process_type_code,
    process_type,
    format_type;
    
-- 明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_super_type_fr   ;
create  table csx_analyse_tmp.csx_analyse_tmp_super_type_fr as 
SELECT '1' as level_id,
        substr(regexp_replace('${edate}','-',''),1,6) as  sales_months,
        performance_region_code,
        performance_region_name,
       process_type_code,
       a.process_type,
       a.performance_province_code,
       performance_province_name,
       case when a.format_type='1_云超' then '1'
            when a.format_type='2_云创永辉生活' then '2'
            when a.format_type='3_云创超级物种' then '3'
            when a.format_type='8_云超MINI' then '8'
            when a.format_type='红旗/中百' then '7'
            when a.format_type='其他' then '-1'
            else format_type 
            end as format_type_code,
       format_type,
       daily_plan_sale,
       daily_sale_value,
       coalesce(daily_sale_value/10000/daily_plan_sale,0 ) as daily_sale_fill_rate,
       daily_profit,
       daily_profit_rate,
       month_plan_sale,
       month_sale,
       coalesce(month_sale/10000/month_plan_sale,0) as month_sale_fill_rate,
       last_month_sale,
       (month_sale-last_month_sale) /abs(last_month_sale) mom_sale_growth_rate,
       month_plan_profit,
       month_profit,
       coalesce(month_profit/10000/month_plan_profit,0) as month_profit_fill_rate,
       coalesce(month_profit/month_sale,0) as  month_profit_rate,
       last_month_profit,
       current_timestamp(),
       substr(regexp_replace('${edate}','-',''),1,6)
FROM csx_analyse_tmp.csx_analyse_tmp_supper_sale a
LEFT JOIN
(select performance_province_code,
    performance_province_name,
    performance_region_code,
    performance_region_name
 from csx_dim.csx_dim_sales_area_belong_mapping 
    where sdt='current' ) c on a.performance_province_code=c.performance_province_code ;


INSERT OVERWRITE table csx_analyse.csx_analyse_fr_sale_kanban_super_type_di partition(months)
SELECT *
FROM csx_analyse_tmp.csx_analyse_tmp_super_type_fr  a
 ;


-- 2 level_id 按照加工类型汇总
INSERT into table csx_analyse.csx_analyse_fr_sale_kanban_super_type_di partition(months)
SELECT '2' as level_id,
        substr(regexp_replace('${edate}','-',''),1,6) as  sales_months,
         performance_region_code,
        performance_region_name,
       process_type_code,
        process_type,
       performance_province_code,
       performance_province_name,
       a.process_type_code as format_type_code,
       concat(process_type,'_小计') as format_type,
       sum(daily_plan_sale)   daily_plan_sale,
       sum(daily_sale_value) yesterday_sale_value,
       coalesce(sum(daily_sale_value)/10000/sum(daily_plan_sale),0 ) as yesterday_sale_fill_rate,
       sum(daily_profit)    yesterday_profit,
       coalesce(sum(daily_profit)/sum(daily_sale_value),0) as yesterday_profit_rate,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale)month_sale,
       coalesce(sum(month_sale)/10000/sum(month_plan_sale),0)  as month_sale_fill_rate,
       sum(last_month_sale)last_month_sale,
       coalesce((sum(month_sale)-sum(last_month_sale)) /abs(sum(last_month_sale)),0) mom_sale_growth_rate,
       sum(month_plan_profit) as month_plan_profit,
       sum(month_profit)month_profit,
       coalesce( sum(month_profit)/10000/sum(month_plan_profit),0) as month_profit_fill_rate,
       coalesce(sum(month_profit)/sum(month_sale),0) as  month_profit_rate,
       sum(last_month_profit)last_month_profit,
       current_timestamp(),
       substr(regexp_replace('${edate}','-',''),1,6)
FROM 
csx_analyse_tmp.csx_analyse_tmp_super_type_fr  a
where level_id='1'
GROUP BY
         performance_region_code,
        performance_region_name,
       process_type_code,
        process_type,
       performance_province_code,
       performance_province_name,
       concat(process_type,'_小计') ;



-- 3 level_id 按照省区汇总
INSERT into table csx_analyse.csx_analyse_fr_sale_kanban_super_type_di partition(months)
SELECT '3' as level_id,
        substr(regexp_replace('${edate}','-',''),1,6) as  sales_months,
        performance_region_code,
        performance_region_name,
       performance_province_code as  process_type_code,
       concat(performance_province_name,'_小计') process_type,
       performance_province_code,
       performance_province_name,
       performance_province_code as format_type_code,
       concat(performance_province_name,'_小计') as format_type,
       sum(daily_plan_sale) daily_plan_sale,
       sum(daily_sale_value) yesterday_sale_value,
       coalesce(sum(daily_sale_value)/10000/sum(daily_plan_sale),0 ) as yesterday_sale_fill_rate,
       sum(daily_profit)    yesterday_profit,
       coalesce(sum(daily_profit)/sum(daily_sale_value),0) as yesterday_profit_rate,
       sum(month_plan_sale) as month_plan_sale,
       sum(month_sale)month_sale,
       coalesce(sum(month_sale)/10000/sum(month_plan_sale),0)  as month_sale_fill_rate,
       sum(last_month_sale)last_month_sale,
       coalesce((sum(month_sale)-sum(last_month_sale)) /abs(sum(last_month_sale)),0) mom_sale_growth_rate,
       sum(month_plan_profit) as month_plan_profit,
       sum(month_profit)month_profit,
       coalesce( sum(month_profit)/10000/sum(month_plan_profit),0) as month_profit_fill_rate,
       coalesce(sum(month_profit)/sum(month_sale),0) as  month_profit_rate,
       sum(last_month_profit)last_month_profit,
       current_timestamp(),
       substr(regexp_replace('${edate}','-',''),1,6)
FROM 
csx_analyse_tmp.csx_analyse_tmp_super_type_fr  a
where level_id='1'
GROUP BY
         performance_region_code,
        performance_region_name,
        concat(performance_province_name,'_小计') ,
       performance_province_code,
       performance_province_name ;

       
	CREATE TABLE `csx_analyse.csx_analyse_fr_sale_kanban_super_type_di`(
	  `level_id` string COMMENT '汇总层级:1明细、2 加工类型小计、3 省区汇总、4 大区汇总', 
	  `sales_month` string COMMENT '销售月份', 
	  `performance_region_code` string COMMENT '大区编码', 
	  `performance_region_name` string COMMENT '大区名称', 
	  `process_type_code` string COMMENT '加工类型：1 非代加工、2 代加工', 
	  `process_type` string COMMENT '加工类型名称', 
	  `performance_province_code` string COMMENT '省区编码', 
	  `performance_province_name` string COMMENT '省区名称', 
	  `format_type_code` string COMMENT '业态编码', 
	  `format_type` string COMMENT '业态名称', 
	  `daily_plan_sale` decimal(26,6) COMMENT '昨日计划销售额', 
	  `yesterday_sale_value` decimal(26,6) COMMENT '昨日销售额', 
	  `yesterday_sale_fill_rate` decimal(26,6) COMMENT '昨日销售达成率', 
	  `yesterday_profit` decimal(26,6) COMMENT '昨日毛利额', 
	  `yesterday_profit_rate` decimal(26,6) COMMENT '昨日毛利率', 
	  `month_plan_sale` decimal(26,6) COMMENT '月至今销售预算', 
	  `month_sale` decimal(26,6) COMMENT '月至今销售额', 
	  `month_sale_fill_rate` decimal(26,6) COMMENT '月至今销售达成率', 
	  `last_month_sale` decimal(26,6) COMMENT '月环比销售额', 
	  `mom_sale_growth_rate` decimal(26,6) COMMENT '月环比增长率', 
	  `month_plan_profit` decimal(26,6) COMMENT '月度毛利计划', 
	  `month_profit` decimal(26,6) COMMENT '月至今毛利额', 
	  `month_profit_fill_rate` decimal(26,6) COMMENT '月度毛利完成率', 
	  `month_profit_rate` decimal(26,6) COMMENT '月至今毛利率', 
	  `last_month_profit` decimal(26,6) COMMENT '环比毛利额', 
	  `update_time` timestamp COMMENT '更新时间')
	COMMENT '大区经营看板商超业态销售'
	PARTITIONED BY ( 
	  `months` string COMMENT '按月分区')
		STORED AS parquet 
	