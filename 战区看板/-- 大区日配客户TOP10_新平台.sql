-- ******************************************************************** 
-- @功能描述大区经营看板日配客户TOP10
-- @创建者： 彭承华 
-- @创建者日期：2022-08-25 10:48:31 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
SET hive.exec.parallel = true;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 2000;
SET hive.optimize.sort.dynamic.partition = true;
--执行Map前进行小文件合并  
set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
drop table csx_analyse_tmp.csx_analyse_tmp_top_10;
create table csx_analyse_tmp.csx_analyse_tmp_top_10 as
select performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    customer_code,
    customer_name,
    ring_sale as ring_sale,
    ring_profit as ring_profit,
    ROW_NUMBER () over (
        partition by performance_region_code
        order by ring_sale desc
    ) as rank_desc
from (
        select performance_region_code,
            performance_region_name,
            performance_province_code,
            performance_province_name,
            performance_city_code,
            performance_city_name,
            customer_code,
            customer_name,
            sum(sale_amt) ring_sale,
            SUM(profit) ring_profit
        from csx_dws.csx_dws_sale_detail_di
        where sdt >= regexp_replace(
                to_date(trunc(add_months('${edate}', -1), 'MM')),
                '-',
                ''
            )
            and sdt <= regexp_replace(
                IF(
                    '${edate}' = last_day('${edate}'),
                    last_day(add_months('${edate}', -1)),
                    add_months('${edate}', -1)
                ),
                '-',
                ''
            )
            and channel_code in ('1', '7', '9')
            and business_type_code = '1'
        group by performance_region_code,
            performance_region_name,
            performance_province_code,
            performance_province_name,
            performance_city_code,
            performance_city_name,
            customer_code,
            customer_name
    ) a;
drop table csx_analyse_tmp.csx_analyse_tmp_top_10_01;
create table csx_analyse_tmp.csx_analyse_tmp_top_10_01 as
select mon,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.customer_code,
    a.customer_name,
    sale,
    profit,
    ROW_NUMBER () over (
        partition by performance_region_code
        order by sale desc
    ) as rank_desc
from (
        select substr(sdt, 1, 6) mon,
            performance_region_code,
            performance_region_name,
            performance_province_code,
            performance_province_name,
            performance_city_code,
            performance_city_name,
            customer_code,
            customer_name,
            sum(sale_amt) sale,
            SUM(profit) profit
        from csx_dws.csx_dws_sale_detail_di
        where sdt >= regexp_replace(trunc('${edate}', 'MM'), '-', '')
            and sdt <= regexp_replace(to_date('${edate}'), '-', '')
            and channel_code in ('1', '7', '9')
            and business_type_code = '1'
        group by performance_region_code,
            performance_region_name,
            performance_province_code,
            performance_province_name,
            performance_city_code,
            performance_city_name,
            customer_code,
            customer_name,
            substr(sdt, 1, 6)
    ) a;
    
insert overwrite table csx_analyse.csx_analyse_fr_sale_customer_top10_kanban  partition(months)
select a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.customer_code,
    a.customer_name,
    ring_sale / 10000 as ring_sale_value,
    ring_profit / 10000 as ring_profit,
    ring_profit / ring_sale as ring_profit_rate,
    c.sale / 10000 as sale_value,
    c.profit / 10000 as profit,
    c.profit / c.sale as profit_rate,
    c.profit / c.sale - ring_profit / ring_sale as diff_profit_rate,
    a.rank_desc,
    c.rank_desc,
    current_timestamp(),
    mon
from csx_analyse_tmp.csx_analyse_tmp_top_10 a
    left join csx_analyse_tmp.csx_analyse_tmp_top_10_01 c on a.customer_code = c.customer_code
    and a.performance_province_code = c.performance_province_code
where a.rank_desc < 11
order by a.rank_desc;



CREATE TABLE `csx_analyse.csx_analyse_fr_sale_customer_top10_kanban`(
	  `performance_region_code` string COMMENT '大区', 
	  `performance_region_name` string COMMENT '大区', 
	  `performance_province_code` string COMMENT '省区', 
	  `performance_province_name` string COMMENT '省区', 
	  `performance_city_code` string COMMENT '城市组', 
	  `performance_city_name` string COMMENT '城市组', 
	  `customer_code` string COMMENT '客户编码', 
	  `customer_name` string COMMENT '客户名称', 
	  `ring_sale_value` decimal(36,12) COMMENT '环期销售额整月', 
	  `ring_profit` decimal(36,12) COMMENT '环期毛利额', 
	  `ring_profit_rate` decimal(38,22) COMMENT '环期毛利率', 
	  `sale_value` decimal(36,12) COMMENT '本期销售额', 
	  `profit` decimal(36,12) COMMENT '本期毛利额', 
	  `profit_rate` decimal(38,22) COMMENT '本期毛利率', 
	  `diff_profit_rate` decimal(38,22) COMMENT '毛利率差', 
	  `ring_rank_desc` int COMMENT '上期排名', 
	  `rank_desc` int COMMENT '本期排名', 
	  `update_time` timestamp COMMENT '更新时间')
	COMMENT '大区看板客户TOP10'
	PARTITIONED BY (   `months` string COMMENT '月分区销售月')
    STORED AS parquet 

