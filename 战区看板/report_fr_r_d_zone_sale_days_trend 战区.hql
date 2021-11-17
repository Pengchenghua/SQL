-- report_fr_r_d_zone_sale_days_trend 战区看板 日配销售日趋势
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;  --设置为非严格模式
set enddate='${enddate}';
set edt=regexp_replace(${hiveconf:edt},'-','');;
set sdt='20210101';


insert overwrite table csx_tmp.report_fr_r_d_zone_sale_days_trend partition(mon)
select from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd') as sdt,
 b.region_code,
 b.region_name,
 a.dist_code,
 province_name,
 city_group_code,
 city_group_name,
 sum(plan_sale) plan_sale,
 0 ,
 sum(coalesce(sales_value,0))/10000 as sales_value,
 sum(coalesce(profit,0))/10000 as profit,
 coalesce(sum(coalesce(profit,0))/sum(coalesce(sales_value,0)),0) as profit_rate,
 coalesce(round(sum(sales_value)/10000/sum(plan_sale),4),0) as sale_fill_rate,
 0,
 current_timestamp(),
 substr(sdt,1,6)
from 
(select regexp_replace(to_date(sdt),'-','') as sdt,
       a.province_code  as dist_code,
        city_group_code ,
        city_group_name,
        plan_sales_value plan_sale,
        0 sales_value,
        0 profit
from  csx_tmp.dws_csms_province_day_sale_plan_tmp a  
where month=substr(${hiveconf:edt},1,6)
	and   regexp_replace(to_date(sdt),'-','') >= ${hiveconf:sdt}
union all 
select
    sdt,
    province_code as dist_code,
    a.city_group_code,
    a.city_group_name,
    0 plan_sale,
    sum(sales_value) sales_value ,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_detail a
where
    sdt >= ${hiveconf:sdt}
    and sdt<=${hiveconf:edt}
    and business_type_code ='1'
    and dc_code not in ('W0K4','W0Z7')
group by
    sdt,
    province_code,
    a.city_group_code,
    a.city_group_name) as a
join 
(select distinct
        province_code  ,
       province_name   ,
       region_code ,
       region_name 
    from
        csx_dw.dws_sale_w_a_area_belong  as b
    where
        1=1
)b
 on  a.dist_code=b.province_code
group by 
from_unixtime(unix_timestamp(a.sdt,'yyyyMMdd'),'yyyy-MM-dd'),
 b.region_code,
 b.region_name,
 a.dist_code,
 province_name,
 substr(sdt,1,6),
 city_group_code,
 city_group_name
    order by sdt desc,
    region_code,
    case when dist_code='32' then 1 
        when dist_code='24' then 2 
        when dist_code='23' then 3  
        when dist_code='19' then 50 
    else cast(dist_code as int) end
 ;
 
 show create table drop table  csx_tmp.report_fr_r_d_zone_sale_days;
 
 drop table csx_tmp.report_fr_r_d_zone_sale_days_trend ;
   create table csx_tmp.report_fr_r_d_zone_sale_days_trend(
  `sdt` string comment '销售日期', 
  `region_code` string comment'大区编码', 
  `region_name` string comment'大区名称', 
  `province_code` string comment'省区编码', 
  `province_name` string comment '省区名称', 
   city_group_code string comment '城市组编码',
    city_group_name string comment '城市组名称',
  `plan_sales` decimal(29,6)comment '销售计划', 
  `plan_profit `decimal(29,6)comment '毛利计划', 
  `sales_value` decimal(38,6)comment '实际销售额', 
  `profit` decimal(38,6)comment '定价毛利额', 
  `profit_rate` decimal(38,6)comment '定价毛利率', 
  `sales_fill_rate` decimal(24,6) comment '销售达成率',
  `profit_fill_rate` decimal(24,6) comment '毛利达成率',
  update_time timestamp comment '更新时间'
  )comment '日配销售日趋势'
  partitioned by (mon string comment '月分区')
STORED AS parquet ;


show create table csx_tmp.dws_csms_province_day_sale_plan_tmp;