set
mapreduce.job.queuename = caishixian;
set
mapreduce.job.reduces = 80;
set
hive.map.aggr = true;
--set hive.groupby.skewindata                 =false;
set
hive.exec.parallel = true;
set
hive.exec.dynamic.partition = true;
--开启动态分区
set
hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
set
hive.exec.max.dynamic.partitions = 10000;
--在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set
hive.exec.max.dynamic.partitions.pernode = 100000;
SET i_edate = '${START_DATE}';


SET s_date=regexp_replace(to_date(trunc(${hiveconf:i_edate},'YY')),'-','');

--源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
insert overwrite table csx_dw.ads_supply_daily_sales_trends partition(sdt)
-- 每日销售趋势
select date_m,sale_sdt,bd_id,bd_name,sum(sale) sale ,sum(profit) profit,sum(profit)/sum(sale)*1.00 as profit_rate,regexp_replace(date_sub(current_date(),1),'-','') 
from (
select '本月' date_m,
from_unixtime(unix_timestamp(sdt ,'yyyyMMdd'),'MM-dd')as sale_sdt,
case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end bd_id,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end bd_name,
    sum(sales_value)sale,
    sum(profit )profit ,
    sum(profit )/sum(sales_value) as profit_rate
from
    csx_dw.dws_sale_r_d_customer_sale 
where
    sdt >=${hiveconf:s_date} and sdt<=regexp_replace(${hiveconf:i_edate},'-','') 
    group by case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end ,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end ,
sdt
union all 
select '本年' date_m,
from_unixtime(unix_timestamp(sdt ,'yyyyMMdd'),'yyyy-MM')as sale_sdt,
case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end bd_id,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end bd_name,
    sum(sales_value)sale,
    sum(profit )profit ,
    sum(profit )/sum(sales_value) as profit_rate
from
    csx_dw.dws_sale_r_d_customer_sale 
where
    sdt >=${hiveconf:s_date} and sdt<=regexp_replace(${hiveconf:i_edate},'-','') 
    group by case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end ,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end ,
from_unixtime(unix_timestamp(sdt ,'yyyyMMdd'),'yyyy-MM')
)a where 1=1 
group by date_m,sale_sdt,bd_id,bd_name
order by sale_sdt
;