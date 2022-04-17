-- 物流看板同步myslq
CREATE TABLE `csx_tmp.report_wms_r_d_turnover_province_kanban_fr`(
  level_id BIGINT COMMENT '0 全国 省区1 dc2 层级',
   province_code string comment '省区编码，全国-平台100000，全国000001',
  `province_name` string COMMENT'省区名称',
   dc_code string comment 'DC编码',
   dc_name string comment 'dc名称',
   dc_uses string comment 'DC用途', 
  `sku` bigint COMMENT '库存SKU', 
  `total_amt` decimal(38,8) COMMENT '库存金额', 
  `total_turnover_day` decimal(38,18) COMMENT '周转天数', 
  `fresh_sku` bigint COMMENT '生鲜SKU', 
  `fresh_amt` decimal(38,8) COMMENT '生鲜库存金额', 
  `fresh_turnover_day` decimal(38,18) COMMENT '生鲜周转天数', 
  `food_sku` bigint   COMMENT '食百SKU', 
  `food_amt` decimal(38,8) COMMENT '食百库存金额', 
  `food_turnover_day` decimal(38,18) COMMENT '食百周转天数', 
  `no_wine_food_sku` bigint COMMENT '食百未含酒类SKU', 
  `no_wine_food_amt` decimal(38,8) COMMENT '食百未含酒类库存金额', 
  `no_wine_turnover_day` decimal(38,18) COMMENT '食百未含酒类周转天数',
   update_time timestamp comment '更新日期'
  
  )COMMENT'物流库存概览看板-省区部类统计'
  partitoned by (sdt string COMMENT'日期分区')

;

-- 插入MYSQL
CREATE TABLE `csx_tmp.report_wms_r_d_turnover_province_kanban_fr`(
    id bigint not null auto_increment,
  level_id BIGINT COMMENT '0 全国 省区1 dc2 层级',
   province_code varchar(64) comment '省区编码，全国-平台100000，全国000001',
  `province_name` varchar(64) COMMENT'省区名称',
   dc_code varchar(64) comment 'DC编码',
   dc_name varchar(64) comment 'dc名称',
   dc_uses varchar(64) comment 'DC用途', 
  `sku` bigint COMMENT '库存SKU', 
  `total_amt` decimal(38,8) COMMENT '库存金额', 
  `total_turnover_day` decimal(38,18) COMMENT '周转天数', 
  `fresh_sku` bigint COMMENT '生鲜SKU', 
  `fresh_amt` decimal(38,8) COMMENT '生鲜库存金额', 
  `fresh_turnover_day` decimal(38,18) COMMENT '生鲜周转天数', 
  `food_sku` bigint   COMMENT '食百SKU', 
  `food_amt` decimal(38,8) COMMENT '食百库存金额', 
  `food_turnover_day` decimal(38,18) COMMENT '食百周转天数', 
  `no_wine_food_sku` bigint COMMENT '食百未含酒类SKU', 
  `no_wine_food_amt` decimal(38,8) COMMENT '食百未含酒类库存金额', 
  `no_wine_turnover_day` decimal(38,18) COMMENT '食百未含酒类周转天数',
   update_time timestamp comment '更新日期',
   sdt varchar(64) comment '日期分区',
    primary key (id),
    key index_sdt(sdt,level_id,province_name,province_code,dc_code)
  )ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='物流库存概览看板-省区部类统计'
 
  ;

columns='level_id,province_code,province_name,dc_code,dc_name,dc_uses,sku,total_amt,total_turnover_day,fresh_sku,fresh_amt,fresh_turnover_day,food_sku,food_amt,food_turnover_day,no_wine_food_sku,no_wine_food_amt,no_wine_turnover_day,update_time,sdt'
day=2022-04-13
yesterday=`date -d ${day} +%Y%m%d`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_wms_r_d_turnover_province_kanban_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_wms_r_d_turnover_province_kanban_fr \
--hive-partition-key sdt \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"

set hive.exec.dynamic.partition.mode=nonstrict;  
set edate='${enddate}'  ;
set edt=regexp_replace(${hiveconf:edate},'-','');
-- drop table csx_tmp.report_wms_r_d_turnover_province_kanban_fr;

drop table csx_tmp.turnover_talbe_01;
create temporary table csx_tmp.turnover_talbe_01 as 
select
level_id,
province_code,
province_name,
'' dc_code,
'' dc_name,
'' dc_uses,
sku,
total_amt,
total_turnover_day,
fresh_sku,
fresh_amt,
fresh_turnover_day,
food_sku,
food_amt,
food_turnover_day,
no_wine_food_sku,
no_wine_food_amt,
no_wine_turnover_day
from 
(select
  '1' level_id,
 a.province_code,
 province_name,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt)/10000 total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
count( distinct case when division_code in ('11','10') and final_qty != 0 then goods_id end ) fresh_sku,
sum(case when division_code in ('11','10') then final_amt end )/10000 fresh_amt,
sum(case when division_code in ('11','10') then period_inv_amt_30day end )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt end ) as fresh_turnover_day,
count( distinct case when division_code in ('12','13','14') and final_qty != 0 then goods_id end ) food_sku,
sum(case when division_code in ('12','13','14') then final_amt end )/10000 food_amt,
sum(case when division_code in ('12','13','14') then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') then cost_30day  end ) as food_turnover_day,
count( distinct case when division_code in ('12','13','14') and classify_middle_code !='B0401' and final_qty != 0 then goods_id end ) no_wine_food_sku,
sum(case when division_code in ('12','13','14') and classify_middle_code !='B0401' then final_amt end  )/10000 no_wine_food_amt,
sum(case when division_code in ('12','13','14') AND classify_middle_code !='B0401' then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') and classify_middle_code!='B0401' then cost_30day  end ) as no_wine_turnover_day
from csx_tmp.ads_wms_r_d_goods_turnover  a
where sdt= ${hiveconf:edt}
    and dc_uses !=''
    and division_code in ('11','10','12','13','14')
group by province_name,a.province_code
union all 
select 
'0' level_id,
'000001' province_code,
'全国' province_name,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt)/10000 total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
count( distinct case when division_code in ('11','10') and final_qty != 0 then goods_id end ) fresh_sku,
sum(case when division_code in ('11','10') then final_amt end )/10000 fresh_amt,
sum(case when division_code in ('11','10') then period_inv_amt_30day end )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt end ) as fresh_turnover_day,
count( distinct case when division_code in ('12','13','14') and final_qty != 0 then goods_id end ) food_sku,
sum(case when division_code in ('12','13','14') then final_amt end )/10000 food_amt,
sum(case when division_code in ('12','13','14') then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') then cost_30day  end ) as food_turnover_day,
count( distinct case when division_code in ('12','13','14') and classify_middle_code !='B0401' and final_qty != 0 then goods_id end ) no_wine_food_sku,
sum(case when division_code in ('12','13','14') and classify_middle_code !='B0401' then final_amt end  )/10000 no_wine_food_amt,
sum(case when division_code in ('12','13','14') AND classify_middle_code !='B0401' then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') and classify_middle_code!='B0401' then cost_30day  end ) as no_wine_turnover_day
from csx_tmp.ads_wms_r_d_goods_turnover  a
where sdt= ${hiveconf:edt}
    and dc_uses !=''
     and division_code in ('11','10','12','13','14')
 
) a 

;

insert overwrite table csx_tmp.report_wms_r_d_turnover_province_kanban_fr partition(sdt) 
select 
province_code,
province_name,
dc_code,
dc_name,
dc_uses,
sku,
total_amt,
total_turnover_day,
fresh_sku,
fresh_amt,
fresh_turnover_day,
food_sku,
food_amt,
food_turnover_day,
no_wine_food_sku,
no_wine_food_amt,
no_wine_turnover_day,
current_timestamp(),
 ${hiveconf:edt}
from
(
select 
level_id,
province_code,
province_name,
dc_code,
dc_name,
dc_uses,
sku,
total_amt,
total_turnover_day,
fresh_sku,
fresh_amt,
fresh_turnover_day,
food_sku,
food_amt,
food_turnover_day,
no_wine_food_sku,
no_wine_food_amt,
no_wine_turnover_day
from  csx_tmp.turnover_talbe_01
union all 
select
 '3' level_id,
 a.province_code,
 province_name,
 dc_code,
 dc_name,
 dc_uses,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt)/10000 total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
count( distinct case when division_code in ('11','10') and final_qty != 0 then goods_id end ) fresh_sku,
sum(case when division_code in ('11','10') then final_amt end )/10000 fresh_amt,
sum(case when division_code in ('11','10') then period_inv_amt_30day end )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt end ) as fresh_turnover_day,
count( distinct case when division_code in ('12','13','14') and final_qty != 0 then goods_id end ) food_sku,
sum(case when division_code in ('12','13','14') then final_amt end )/10000 food_amt,
sum(case when division_code in ('12','13','14') then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') then cost_30day  end ) as food_turnover_day,
count( distinct case when division_code in ('12','13','14') and classify_middle_code !='B0401' and final_qty != 0 then goods_id end ) no_wine_food_sku,
sum(case when division_code in ('12','13','14') and classify_middle_code !='B0401' then final_amt end  )/10000 no_wine_food_amt,
sum(case when division_code in ('12','13','14') AND classify_middle_code !='B0401' then period_inv_amt_30day end )/ sum(case when division_code in ('12','13','14') and classify_middle_code!='B0401' then cost_30day  end ) as no_wine_turnover_day
from csx_tmp.ads_wms_r_d_goods_turnover  a
where sdt= ${hiveconf:edt} 
    and dc_uses !=''
    and division_code in ('11','10','12','13','14')
group by a.province_code,
 province_name,
 dc_code,
 dc_name,
 dc_uses
) a 
 

;

CREATE TABLE `csx_tmp.report_wms_r_d_tunover_trend_kanban_fr`(
       province_code string comment '省区编码',
	  `province_name` string comment '省区名称', 
       dc_code string comment 'DC编码',
       dc_name string comment 'DC名称',
       dc_uses string comment 'DC用途',
	  `months` string comment '月份', 
	  `stock_amt` decimal(38,8) COMMENT '期末库存金额',
       period_inv_amt_30day decimal(38,8) comment'30天期间库存额', 
       cost_30day decimal(38,8) comment'近30天成本，生鲜包含原料领用+消耗',
	  `stock_turnover_day` decimal(21,0) comment '近30天周转天数'
    )comment '物流库存概览看板-物流月度趋势'
	partitioned by (months string comment '月份')
	STORED AS parquet
;



-- mysql 同步
CREATE TABLE `csx_tmp.report_wms_r_d_tunover_trend_kanban_fr`(
       province_code string comment '省区编码',
	  `province_name` string comment '省区名称', 
       dc_code string comment 'DC编码',
       dc_name string comment 'DC名称',
       dc_uses string comment 'DC用途',
	  `stock_amt` decimal(38,8) COMMENT '期末库存金额',
       period_inv_amt_30day decimal(38,8) comment'30天期间库存额', 
       cost_30day decimal(38,8) comment'近30天成本，生鲜包含原料领用+消耗',
	  `stock_turnover_day` decimal(21,0) comment '近30天周转天数',
        update_time timestamp comment '更新日期'
    )comment '物流库存概览看板-物流月度趋势'
	partitioned by (months string comment '月份')
	STORED AS parquet
;



set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.report_wms_r_d_tunover_trend_kanban_fr partition(months) 
select a.province_code,
        a.province_name,
        dc_code,
        dc_name,
        dc_uses,
       sum(final_amt)/10000 total_amt,
       sum( period_inv_amt_30day  )period_inv_amt_30day,
       sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) as total_cost_30day,
        round(sum( period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) ,0)total_turnover_day,
        current_timestamp(),
        substr(a.sdt,1,6)  
from csx_tmp.ads_wms_r_d_goods_turnover a
join 
(select distinct month,
    if(regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd'))),'-','')>regexp_replace(date_sub(current_date(),0),'-',''),  regexp_replace(date_sub(current_date(),1),'-',''),regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd'))),'-','')) sdt  
from csx_dw.dws_basic_w_a_date 
    where calday >regexp_replace(to_date(add_months(current_date(),-12)),'-','')
    and calday  < regexp_replace(current_date(),'-','')
) b on a.sdt=b.sdt
where 1=1
     and division_code in ('11','10','12','13','14')
     and a.dc_uses !=''
group by substr(a.sdt,1,6),a.province_code,
        a.province_name,
        dc_code,
        dc_name,
        dc_uses
;

columns='province_code,province_name,dc_code,dc_name,dc_uses,stock_amt,period_inv_amt_30day,cost_30day,stock_turnover_day,update_time,months'
day=2022-04-13
yesterday=`date -d ${day} +%Y%m`
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table report_wms_r_d_tunover_trend_kanban_fr \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table report_wms_r_d_tunover_trend_kanban_fr \
--hive-partition-key months \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"



;

-- 物流看板--管理品类异常统计表
  drop table csx_tmp.report_wms_r_d_turnover_classify_kanban_fr;
CREATE TABLE `csx_tmp.report_wms_r_d_turnover_classify_kanban_fr`(
  level_id string COMMENT'层级：0 全国 1省区 2 DC',
  province_code string comment '省区编码,全国-平台100000，000001 全国',
  `province_name` string COMMENT '省区名称', 
  dc_code string comment 'DC编码',
  dc_name string COMMENT 'DC名称',
  dc_uses string comment 'DC用途',
  `classify_large_code` string comment '管理一级品类编码', 
  `classify_large_name` string COMMENT '管理一级品类名称', 
  `classify_middle_code` string comment '管理二级品类编码', 
  `classify_middle_name` string comment '管理二级品类名称', 
  `sku` bigint COMMENT '有库存SKU数', 
  `total_amt` decimal(38,8) COMMENT '库存总金额', 
  `total_turnover_day` decimal(38,18) COMMENT '总库存周转天数', 
  `high_stock_amt` decimal(38,8) COMMENT '高库存金额', 
  `high_stock_sku` bigint COMMENT '高库存SKU', 
  `no_sales_stock_amt` decimal(38,8) COMMENT '未销售库存金额', 
  `no_sales_stock_sku` bigint COMMENT '未销售SKU', 
  `validity_amt` decimal(38,6) COMMENT  '临期库存金额(临期、过期)', 
  `validity_sku` bigint COMMENT '临期商品SKU',
  update_time TIMESTAMP comment '更新时间'
  )comment '物流库存概览看板-管理品类异常统计'
  partitioned by (sdt string comment '日期分区')
  stored as parquet
  ;

  CREATE TABLE `report_wms_r_d_turnover_classify_kanban_fr`(
    id bigint not null auto_increment,
    level_id varchar(64) COMMENT'层级：0 全国 1省区 2 DC',
    province_code varchar(64) comment '省区编码,全国-平台100000，000001 全国',
  `province_name` varchar(64) COMMENT '省区名称', 
   dc_code varchar(64) comment 'DC编码',
  dc_name varchar(64) COMMENT 'DC名称',
  dc_uses varchar(64) comment 'DC用途',
  `classify_large_code` varchar(64) comment '管理一级品类编码', 
  `classify_large_name` varchar(64) COMMENT '管理一级品类名称', 
  `classify_middle_code` varchar(64) comment '管理二级品类编码', 
  `classify_middle_name` varchar(64) comment '管理二级品类名称', 
  `sku` bigint COMMENT '有库存SKU数', 
  `total_amt` decimal(38,8) COMMENT '库存总金额', 
  `total_turnover_day` decimal(38,18) COMMENT '总库存周转天数', 
  `high_stock_amt` decimal(38,8) COMMENT '高库存金额', 
  `high_stock_sku` bigint COMMENT '高库存SKU', 
  `no_sales_stock_amt` decimal(38,8) COMMENT '未销售库存金额', 
  `no_sales_stock_sku` bigint COMMENT '未销售SKU', 
  `validity_amt` decimal(38,6) COMMENT  '临期库存金额(临期、过期)', 
  `validity_sku` bigint COMMENT '临期商品SKU',
  update_time TIMESTAMP comment '更新时间',
  sdt varchar(64) comment '日期',
  primary key (id),
  key index_sdt(sdt,level_id,province_name,province_code,dc_code,dc_name,dc_uses)
  )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment= '物流库存概览看板-管理品类异常统计'



-- 库存看板--管理品类异常统计
set edate='${enddate}'  ;
set dc_uses=('寄售门店','城市服务商','合伙人物流','');
set edt=regexp_replace(${hiveconf:edate},'-','');

drop table csx_tmp.temp_wms_kanban_class;
create temporary table csx_tmp.temp_wms_kanban_class as 
select 
 level_id,
 province_code,
 province_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
 sku,
 total_amt,
 total_turnover_day,
 high_stock_amt,      -- 高库存金额
 high_stock_sku,            -- 高库存SKU
 no_sales_stock_amt,          -- 无销售库存金额
 no_sales_stock_sku,   -- 无销售库存SKU
 validity_amt,
 validity_sku,
 current_timestamp(),
 ${hiveconf:edt}
from 
(select 
 '1' level_id,
 province_code,
 province_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt) total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
        end ) high_stock_amt,      -- 高库存金额
count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_id
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_id
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_id
        end ) high_stock_sku,            -- 高库存SKU
sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end ) no_sales_stock_amt,          -- 无销售库存金额
count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_id   end ) no_sales_stock_sku,   -- 无销售库存SKU
sum(stock_amt) as validity_amt,
count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from csx_tmp.ads_wms_r_d_goods_turnover  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt=${hiveconf:edt}
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where sdt=${hiveconf:edt} 
    and dc_uses not in ${hiveconf:dc_uses}
    and division_code in ('11','10','12','13','14')
group by province_name,a.province_code,
classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name 
union all 
select 
 '0' level_id,
 '000001'province_code,
 '全国' province_name,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt) total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
        end ) high_stock_amt,      -- 高库存金额
count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_id
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_id
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_id
        end ) high_stock_sku,            -- 高库存SKU
sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end ) no_sales_stock_amt,          -- 无销售库存金额
count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_id   end ) no_sales_stock_sku,   -- 无销售库存SKU
sum(stock_amt) as validity_amt,
count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from csx_tmp.ads_wms_r_d_goods_turnover  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt=${hiveconf:edt}
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where sdt=${hiveconf:edt} 
    and dc_uses not in ${hiveconf:dc_uses}
    and division_code in ('11','10','12','13','14')
group by 
classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name
 G 
) a 
;


insert overwrite table csx_tmp.report_wms_r_d_turnover_classify_kanban_fr partition(sdt)
select  
 level_id,
 province_code,
 province_name,
 dc_code,
 dc_name,
 dc_uses,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
 sku,
 total_amt/10000 total_amt,
 total_turnover_day,
 high_stock_amt/10000 high_stock_amt,      -- 高库存金额
 high_stock_sku,            -- 高库存SKU
 no_sales_stock_amt/10000 no_sales_stock_amt,          -- 无销售库存金额
 no_sales_stock_sku,   -- 无销售库存SKU
 validity_amt/10000 validity_amt,
 validity_sku,
 current_timestamp(),
 ${hiveconf:edt}
 from (
select  
 level_id,
 province_code,
 province_name,
 '' as dc_code,
 '' as dc_name,
 '' as dc_uses,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
 sku,
 total_amt,
 total_turnover_day,
 high_stock_amt,      -- 高库存金额
 high_stock_sku,            -- 高库存SKU
 no_sales_stock_amt,          -- 无销售库存金额
 no_sales_stock_sku,   -- 无销售库存SKU
 validity_amt,
 validity_sku
from  csx_tmp.temp_wms_kanban_class
union all 
select 
 '2' level_id,
 province_code,
 province_name,
 a.dc_code,
 a.dc_name,
 dc_uses,
 classify_large_code ,
 classify_large_name ,
 classify_middle_code,
 classify_middle_name ,
count(distinct case when final_qty!=0 then goods_id end) sku,
sum(final_amt) total_amt,
sum(period_inv_amt_30day  )/ sum(case when division_code in ('11','10') then cost_30day+receipt_amt+material_take_amt else cost_30day end ) total_turnover_day,
sum(case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then final_amt
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then final_amt
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then final_amt
        end ) high_stock_amt,      -- 高库存金额
count(distinct  case when division_code in ('11','10') and a.days_turnover_30>15 and a.final_amt>500 and a.entry_days>3 then a.goods_id
         when division_code in ('13','14') and a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 then goods_id
          when division_code in ('12') and a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 then goods_id
        end ) high_stock_sku,            -- 高库存SKU
sum(case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then final_amt  end ) no_sales_stock_amt,          -- 无销售库存金额
count( distinct case when a.no_sale_days>30 and a.final_qty>0.1 and a.entry_days>7 then a.goods_id   end ) no_sales_stock_sku,   -- 无销售库存SKU
sum(stock_amt) as validity_amt,
count(distinct case when b.goods_code is not null then b.goods_code end ) validity_sku
from csx_tmp.ads_wms_r_d_goods_turnover  a
left join 
(select dc_code,goods_code,sum(stock_qty) stock_qty,sum(stock_amt) stock_amt from csx_dw.report_wms_r_a_validity_goods
    where sdt=${hiveconf:edt}
        and validity_type in ('过期','临期')
        group by  dc_code,goods_code
        )  b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where sdt=${hiveconf:edt} 
    and dc_uses not in ${hiveconf:dc_uses}
    and division_code in ('11','10','12','13','14')
group by province_name,a.province_code,
a.dc_code,
dc_name,
dc_uses,
classify_large_code ,
classify_large_name ,
classify_middle_code,
 classify_middle_name 
 ) a 
 ;