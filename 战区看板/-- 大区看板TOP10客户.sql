-- 大区看板TOP10客户

CREATE TABLE `csx_tmp.report_fr_r_d_zone_sale_customer_top10`(
  `region_code` string COMMENT '大区', 
  `region_name` string COMMENT '大区', 
  `province_code` string COMMENT'省区', 
  `province_name` string COMMENT'省区', 
  `city_group_code` string COMMENT '城市组', 
  `city_group_name` string COMMENT '城市组', 
  `customer_no` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `ring_sale_value` decimal(36,12) COMMENT '环期销售额整月', 
  `ring_profit` decimal(36,12) COMMENT '环期毛利额', 
  `ring_profit_rate` decimal(38,22) COMMENT '环期毛利率', 
  `sale_value` decimal(36,12) COMMENT '本期销售额', 
  `profit` decimal(36,12) COMMENT '本期毛利额', 
  `profit_rate` decimal(38,22) COMMENT '本期毛利率', 
  `diff_profit_rate` decimal(38,22) COMMENT '毛利率差', 
  `ring_rank_desc` int COMMENT '上期排名'
   rank_desc int comment '本期排名',
   update_time timestamp comment '更新时间'
   ) comment '大区看板客户TOP10'
STORED AS parquet
; 
--

set enddate='${enddate}';
set l_edt= regexp_replace(to_date(last_day(add_months(${hiveconf:enddate},-1))),'-','');
set l_sdt= regexp_replace(to_date(trunc(add_months(${hiveconf:enddate},-1),'MM')),'-','');
set edt= regexp_replace(${hiveconf:enddate},'-','');
set sdt =regexp_replace(to_date(trunc(${hiveconf:enddate},'MM')),'-','');


--select ${hiveconf:l_edt},${hiveconf:l_sdt},${hiveconf:edt},${hiveconf:sdt};
--上月全月销售TOP客户
drop table csx_tmp.temp_top_10;
create temporary table csx_tmp.temp_top_10 as
select region_code ,
     region_name,
     province_code ,
     province_name ,
     city_group_code ,
     city_group_name ,
     customer_no ,
     customer_name,
     sale as ring_sale ,
     profit as ring_profit,
     ROW_NUMBER ()over (partition by region_code ,region_name  order by sale desc ) as rank_desc 
     from      
     (select region_code ,region_name,province_code ,province_name ,city_group_code ,
     city_group_name ,customer_no ,customer_name ,sum(sales_value) sale,SUM(profit) profit
     from csx_dw.dws_sale_r_d_detail  
     where sdt>= ${hiveconf:l_sdt}
     	and sdt<= ${hiveconf:l_edt}
     	and channel_code in ('1')
     	and business_type_code ='1'
    --	and  region_code ='${zoneid}'
     group by region_code ,region_name,province_code ,province_name ,city_group_code ,
     city_group_name ,customer_no ,customer_name 
     )a  
     
;

-- 本月客户销售
drop table csx_tmp.temp_top_10_01;
create temporary table csx_tmp.temp_top_10_01 as
select mon,
     region_code ,
     region_name,
     province_code ,
     province_name ,
     city_group_code ,
     city_group_name ,
     customer_no ,
     customer_name,
     sale ,
     profit,
     ROW_NUMBER ()over (partition by region_code ,region_name  order by sale desc ) as rank_desc 
     from      
     (
select  substr(sdt,1,6) mon,
        region_code ,
        region_name,
        province_code ,
        province_name ,
        city_group_code ,
        city_group_name ,
        customer_no ,
        customer_name ,
        sum(sales_value)  sale,
        SUM(profit) profit
     from csx_dw.dws_sale_r_d_detail 
      where sdt>= ${hiveconf:sdt} and sdt<= ${hiveconf:edt}
     	and channel_code in ('1')
     	and business_type_code ='1'
     group by region_code ,region_name,province_code ,province_name ,city_group_code ,
     city_group_name ,customer_no ,customer_name ,
        substr(sdt,1,6)
    )a ;
  
  -- 插入TOP
--  create table csx_tmp.report_fr_r_d_zone_sale_customer_top10 as
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.report_fr_r_d_zone_sale_customer_top10  partition(months)
      select a.region_code,
      	a.region_name,
      	a.province_code,
      	a.province_name,
      	a.city_group_code,
      	a.city_group_name,
      	a.customer_no,
      	a.customer_name,
      	ring_sale/10000 as ring_sale_value,
      	ring_profit/10000 as ring_profit,
      	ring_profit/ring_sale as ring_profit_rate,
      	c.sale/10000 as sale_value,
      	c.profit/10000 as profit,
      	c.profit/c.sale as profit_rate,
      	c.profit/c.sale-ring_profit/ring_sale as diff_profit_rate,
      	a.rank_desc,
      	c.rank_desc,
      	current_timestamp(),
      	mon
      from csx_tmp.temp_top_10  a 
      left join csx_tmp.temp_top_10_01 c on a.customer_no=c.customer_no and a.province_code=c.province_code
      where a.rank_desc<11
      order by a.rank_desc ;








--日配客户TOP 大区看板 IMPALA
 with tmp_sale as 
      (select region_code ,region_name,province_code ,province_name ,city_group_code ,
     city_group_name ,customer_no ,customer_name,sale as ring_sale ,profit as ring_profit,
     ROW_NUMBER ()over (partition by region_code ,region_name  order by sale desc ) as aa 
     from      
     (select region_code ,region_name,province_code ,province_name ,city_group_code ,
     city_group_name ,customer_no ,customer_name ,sum(sales_value) sale,SUM(profit) profit
     from csx_dw.dws_sale_r_d_detail  
     where sdt>=regexp_replace(to_date(trunc(months_sub('${edt}',1),'MM')),'-','')
     	and sdt<= regexp_replace(to_date(last_day(months_sub('${edt}',1))),'-','')
     	and channel_code in ('1','7','9')
     	and business_type_code ='1'
    	and  region_code ='${zoneid}'
     group by region_code ,region_name,province_code ,province_name ,city_group_code ,
     city_group_name ,customer_no ,customer_name 
     )a  
     )
	,
      tmp_sale_02 as 
	(select  region_code ,region_name,province_code ,province_name ,city_group_code ,
     city_group_name ,customer_no ,customer_name ,sum(sales_value)  sale,SUM(profit) profit
     from csx_dw.dws_sale_r_d_detail 
      where sdt>=concat('${mon}','01') and sdt<= regexp_replace('${edt}','-','')
     	and channel_code in ('1','7','9')
     	and business_type_code ='1'
    	and  region_code ='${zoneid}'
     group by region_code ,region_name,province_code ,province_name ,city_group_code ,
     city_group_name ,customer_no ,customer_name 
      )
      
      select a.region_code,
      	a.region_name,
      	a.province_code,
      	a.province_name,
      	a.city_group_code,
      	a.city_group_name,
      	a.customer_no,
      	a.customer_name,
      	ring_sale/10000 as ring_sale_value,
      	ring_profit/10000 as ring_profit,
      	ring_profit/ring_sale as ring_profit_rate,
      	c.sale/10000 as sale_value,
      	c.profit/10000 as profit,
      	c.profit/c.sale as profit_rate,
      	c.profit/c.sale-ring_profit/ring_sale as diff_profit_rate
      from tmp_sale  a 
      left join tmp_sale_02 c on a.customer_no=c.customer_no and a.province_code=c.province_code
      where a.aa<11
       order by aa ;