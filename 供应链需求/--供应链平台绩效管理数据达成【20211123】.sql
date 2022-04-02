--供应链平台绩效管理数据达成【20211123】
-- 更新日期20220216
--迭代说明 ：1、日配业务剔除dc_code not in ('W0Z7','W0K4','WB26'),2、自营大客户渠道 business_type_code not in('4','9')set parquet.compression=snappy;

--更新 蔬菜按照区域占比 孙鲁 北京、安徽、湖北、陕西、河南，江韩国 福建、广东，董春发 四川 、成都、贵州
--

set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
set shopid=('WA93','W0A2','W080','W0K7','W0L4','W0AW','W0J8','W048','WB04','W0A3','WB11','W0A8','WB03','W053','W0F4','W0G9',
'W0K6','W0AH','W0AJ','W0J2','W0F7','W0G6','WA96','W0K1','W0AU','W0L3','W0BK','W0AL','W0S9','W0Q2','W0Q9','W0Q8',
'W0BS','W0BH','W0BR','W0R9','WB00','W0R8','W088','W0BZ','W0A5','W0P8','WA94','W0AS','W0AR','WA99','W0N1','W079',
'W0A6','W0BD','W0N0','WB01','W0P3','W0W7','W0X1','W0X2','W0Z8','W0Z9','W0AZ','W039','W0A7','W0BT');
-- 蔬菜基地
SET sc_shop=('W080','WA93','WB04','W048','WB03','W0G9','W0BK','W0S9','W0Q9','W0BH','W0BR','W0BT','W0BZ',
'W088','WB00','W0P8','WA99','W0AR','W0AS','W079','W0N0','W0W7','W039','W0AZ');
--上月结束日期，当前日期不等于月末取当前日期，等于月末取上月最后一天
set last_edt=regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','');
set parquet.compression=snappy;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;
-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;

-- 大客户销售销售业务包含（日配业务、福利业务、省区大宗、批发内购）
drop table if exists csx_tmp.temp_sale_01 ;
create temporary table csx_tmp.temp_sale_01 as 
select substr(sdt,1,6) as sales_months,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sum(sales_value)sales_value,
    sum(profit) profit,
    sum(case when business_type_code='1' and  dc_code not in ('W0Z7','W0K4','WB26','WB38') then sales_value end ) as daliy_sales_value,
    sum(case when business_type_code='1' AND dc_code not in ('W0Z7','W0K4','WB26','WB38') then profit end ) as  daliy_profit
from csx_dw.dws_sale_r_d_detail
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
    and channel_code in ('1')
    and business_type_code!='4'
--    AND dc_code not in ('W0Z7','W0K4','WB26')
group by  classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    business_type_code,
    business_type_name,
     province_name,
    substr(sdt,1,6) ;
    
--商品入库全国采购占比=全国类型供应商（全国基地/产地、自有品牌、全国集采）品类入库金额/品类总入库金额
-- 指标说明：按品类采购负责的管理二级/三级分类 是否集采：是
drop table  csx_tmp.temp_pch_sale_01 ;
create temporary table csx_tmp.temp_pch_sale_01 as 
select substr(sdt,1,6) sales_months,
    a.province_name,
    supplier_code,
    joint_purchase,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sum(amount) as amt,
    sum(case when receive_location_code in ${hiveconf:sc_shop} and classify_middle_code ='B0202' and b.joint_purchase=1 then  amount end) sc_amt
from csx_dw.dws_wms_r_d_entry_batch as a
left join 
(select vendor_id,joint_purchase from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
    and a.order_type_code like 'P%'
    and a.business_type ='01'
    and a.receive_location_code in ${hiveconf:shopid}
    and supplier_code !='C05013'
    and receive_status='2'
group by  supplier_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    joint_purchase,
    province_name,
    substr(sdt,1,6) ;



--计算集采占比
drop table if exists  csx_tmp.temp_sale_02 ;
create temporary table csx_tmp.temp_sale_02 as 

select sales_months,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    classify_large_management,
    classify_middle_management,
    sum(amt) entry_amt,
    sum(case when  a.classify_middle_code ='B0202' then sc_amt when  joint_purchase=1 and a.classify_middle_code !='B0202'  then amt end ) join_entry_amt
from csx_tmp.temp_pch_sale_01 as a
left join 
(SELECT distinct classify_middle_code,
        classify_large_code,
        classify_large_management,
        classify_middle_management,
        province_name
 FROM `csx_tmp`.`report_scm_r_d_classify_performance_person` 
   -- WHERE sdt=substr(${hiveconf:e_dt},1,6) 
    ) b on a.classify_large_code=b.classify_large_code 
        and a.classify_middle_code=b.classify_middle_code  
        and if(a.classify_middle_code !='B0202','',a.province_name)=b.province_name
group by 
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
     classify_large_management,
    classify_middle_management,
    sales_months
;

-- select * from csx_tmp.temp_sale_02;

insert overwrite table csx_tmp.report_scm_r_d_classify_performance_fr partition(months)
select 
    sales_months,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    classify_large_management,
    classify_middle_management,
    sum(sales_target)sales_target,
    sum(sales_value)sales_value,
    0 sales_completion_rate,
    sum(profit)profit,
    coalesce(sum(profit)/sum(sales_value),0) profit_rate,
    sum(daliy_sales_target) daliy_sales_target,
    sum(daliy_sales_value)daliy_sales_value,
    0 daliy_sales_completion_rate,
    sum(daliy_profit)daliy_profit,
    coalesce(sum(daliy_profit)/sum(daliy_sales_value),0) daliy_profit_rate,
    sum(join_entry_target)join_entry_target,
    sum(entry_amt)entry_amt,
    sum(join_entry_amt)join_entry_amt,
    sum(join_entry_amt)/sum(entry_amt) join_entry_ratio,
    0 join_entry_completion_rate,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from (
select sales_months,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    classify_large_management,
    classify_middle_management,
    sales_target,
    sales_value,
    profit,
    daliy_sales_target,
    daliy_sales_value,
    daliy_profit,
    0 join_entry_target,
    0 entry_amt,
    0 join_entry_amt
from  csx_tmp.temp_sale_01 a 
left join 
(SELECT  distinct classify_middle_code,
        classify_large_code,
        classify_large_management,
        classify_middle_management,
        daliy_sales_target,
        join_entry_target,
        sales_target,
        province_name
FROM `csx_tmp`.`report_scm_r_d_classify_performance_person` 
   -- WHERE sdt=substr(${hiveconf:e_dt},1,6) 
    ) b on a.classify_large_code=b.classify_large_code and a.classify_middle_code=b.classify_middle_code  
    and if(a.classify_middle_code !='B0202','',a.province_name)=b.province_name
union all 
select sales_months,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_large_management,
    classify_middle_management,
    0 sales_target,
    0 sales_value,
    0 profit,
    0 daliy_sales_target,
    0 daliy_sales_value,
    0 daliy_profit,
    0 join_entry_target,
    entry_amt,
    join_entry_amt
from  csx_tmp.temp_sale_02 a 
) a 
group by a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    sales_months,
    classify_large_management,
    classify_middle_management
    ;
    
    show create table csx_tmp.report_scm_r_d_classify_performance_fr;

-- select * from csx_tmp.temp_sale_02;

insert overwrite table csx_tmp.report_scm_r_d_classify_performance_fr partition(months)
select 
    sales_months,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    classify_large_management,
    classify_middle_management,
    sum(sales_target)sales_target,
    sum(sales_value)sales_value,
    0 sales_completion_rate,
    sum(profit)profit,
    coalesce(sum(profit)/sum(sales_value),0) profit_rate,
    sum(daliy_sales_target) daliy_sales_target,
    sum(daliy_sales_value)daliy_sales_value,
    0 daliy_sales_completion_rate,
    sum(daliy_profit)daliy_profit,
    coalesce(sum(daliy_profit)/sum(daliy_sales_value),0) daliy_profit_rate,
    sum(join_entry_target)join_entry_target,
    sum(entry_amt)entry_amt,
    sum(join_entry_amt)join_entry_amt,
    sum(join_entry_amt)/sum(entry_amt) join_entry_ratio,
    0 join_entry_completion_rate,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from (
select sales_months,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    classify_large_management,
    classify_middle_management,
    sales_target,
    sales_value,
    profit,
    daliy_sales_target,
    daliy_sales_value,
    daliy_profit,
    0 join_entry_target,
    0 entry_amt,
    0 join_entry_amt
from  csx_tmp.temp_sale_01 a 
left join 
(SELECT  distinct classify_middle_code,
        classify_large_code,
        classify_large_management,
        classify_middle_management,
        daliy_sales_target,
        join_entry_target,
        sales_target
FROM `csx_tmp`.`report_scm_r_d_classify_performance_person` 
   -- WHERE sdt=substr(${hiveconf:e_dt},1,6) 
    ) b on a.classify_large_code=b.classify_large_code and a.classify_middle_code=b.classify_middle_code  

union all 
select sales_months,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_large_management,
    classify_middle_management,
    0 sales_target,
    0 sales_value,
    0 profit,
    0 daliy_sales_target,
    0 daliy_sales_value,
    0 daliy_profit,
    0 join_entry_target,
    entry_amt,
    join_entry_amt
from  csx_tmp.temp_sale_02 a 
) a 
group by a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    sales_months,
    classify_large_management,
    classify_middle_management
    ;
    
    show create table csx_tmp.report_scm_r_d_classify_performance_fr;

    ---以下上一版本绩效方案数据
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;

set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
set shopid=('W080','WA93','WB04','W048','WB03','W0G9','W0BK','W0S9','W0Q9','W0BH','W0BR','W0BT',
'W0BZ','W088','WB00','W0P8','WA99','W0AR','W0AS','W079','W0N0','W0W7','W039','W0AZ');

--上月结束日期，当前日期不等于月末取当前日期，等于月末取上月最后一天
set last_edt=regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','');

-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;

-- 大客户销售销售业务包含（日配业务、福利业务、省区大宗、批发内购）
drop table if exists csx_tmp.temp_sale_01 ;
create temporary table csx_tmp.temp_sale_01 as 
select substr(sdt,1,6) as sales_months,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_value)sales_value,
    sum(profit) profit,
    sum(case when business_type_code='1' and  dc_code not in ('W0Z7','W0K4','WB26') then sales_value end ) as daliy_sales_value,
    sum(case when business_type_code='1' AND dc_code not in ('W0Z7','W0K4','WB26') then profit end ) as  daliy_profit
from csx_dw.dws_sale_r_d_detail
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
    and channel_code in ('1')
    and business_type_code!='4'
--    AND dc_code not in ('W0Z7','W0K4','WB26')
group by  classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    business_type_code,
    business_type_name,
    substr(sdt,1,6) ;
    
--商品入库全国采购占比=全国类型供应商（全国基地/产地、自有品牌、全国集采）品类入库金额/品类总入库金额
-- 指标说明：按品类采购负责的管理二级/三级分类 是否集采：是

drop table  csx_tmp.temp_pch_sale_01 ;
create temporary table csx_tmp.temp_pch_sale_01 as 
select substr(sdt,1,6) sales_months,
    supplier_code,
    joint_purchase,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(price*receive_qty) as amt
from csx_dw.dws_wms_r_d_entry_batch as a
left join 
(select vendor_id,joint_purchase from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
    and a.order_type_code like 'P%'
    and a.business_type ='01'
    and a.receive_location_code in ${hiveconf:shopid}
    and supplier_code !='C05013'
    and receive_status='2'
group by  supplier_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    joint_purchase,
    substr(sdt,1,6) ;



--计算集采占比

drop table if exists  csx_tmp.temp_sale_02 ;

create temporary table csx_tmp.temp_sale_02 as 

select sales_months,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(amt) entry_amt,
    sum(case when joint_purchase=1 then amt end ) join_entry_amt
from csx_tmp.temp_pch_sale_01 as a
group by 
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sales_months


;




insert overwrite table csx_tmp.report_scm_r_d_classify_performance_fr partition(sdt)
select 
    sales_months,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    classify_large_management,
    classify_middle_management,
    sum(b.sales_target)sales_target,
    sum(sales_value)sales_value,
    0 sales_completion_rate,
    sum(profit)profit,
    coalesce(sum(profit)/sum(sales_value),0) profit_rate,
    sum(b.daliy_sales_target) daliy_sales_target,
    sum(daliy_sales_value)daliy_sales_value,
    0 daliy_sales_completion_rate,
    sum(daliy_profit)daliy_profit,
    coalesce(sum(daliy_profit)/sum(daliy_sales_value),0) daliy_profit_rate,
    sum(b.join_entry_target)join_entry_target,
    sum(entry_amt)entry_amt,
    sum(join_entry_amt)join_entry_amt,
    sum(join_entry_amt)/sum(entry_amt) join_entry_ratio,
    0 join_entry_completion_rate,
    current_timestamp(),
    ${hiveconf:e_dt}
from (
select sales_months,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sales_value,
    profit,
    daliy_sales_value,
    daliy_profit,
    0 entry_amt,
    0 join_entry_amt
from  csx_tmp.temp_sale_01 a 
union all 
select sales_months,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    0 sales_value,
    0 profit,
    0 daliy_sales_value,
    0 daliy_profit,
    entry_amt,
    join_entry_amt
from  csx_tmp.temp_sale_02 a 
) a 
left join 
(SELECT classify_middle_code,
        classify_large_code,
        classify_large_management,
        classify_middle_management,
        daliy_sales_target,
        join_entry_target,
        sales_target
FROM csx_tmp.report_scm_r_d_classify_performance_person
   -- WHERE sdt=substr(${hiveconf:e_dt},1,6) 
    ) b on a.classify_large_code=b.classify_large_code and a.classify_middle_code=b.classify_middle_code  
group by a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    sales_months,
    classify_large_management,
    classify_middle_management
    ;
    


 SELECT * from  csx_tmp.report_scm_r_d_classify_performance_fr where sdt='20211231';
    
    
-- 供应链绩效 Performance report
	DROP table csx_tmp.report_scm_r_d_classify_performance_fr;
CREATE  TABLE `csx_tmp.report_scm_r_d_classify_performance_fr`(
    sales_months string COMMENT '销售月份',
  `classify_large_code` string comment '管理一级', 
  `classify_large_name` string comment '管理一级', 
  `classify_middle_code` string comment '管理二级', 
  `classify_middle_name` string comment '管理二级', 
   classify_large_management string comment '管理一级负责人',
   classify_middle_management string comment '管理二级负责人',
   sales_target decimal(38,6) comment '销售目标额',
  `sales_value` decimal(38,6) comment '销售额',
   sales_completion_rate DECIMAL(26,6) comment '销售额完成率', 
  `profit` decimal(38,6) comment '毛利额',
  `profit_rate` decimal(38,6) comment '毛利率', 
   daliy_sales_target decimal(38,6) comment '日配销售目标额',
  `daliy_sales_value` decimal(38,6) comment '日配销售额', 
    daliy_sales_completion_rate DECIMAL(38,6) comment '日配销售额完成率', 
  `daliy_profit` decimal(38,6) COMMENT '日配毛利额',
  `daliy_profit_rate` decimal(38,6) comment '日配毛利率', 
   join_entry_target decimal(38,6) comment'集采入库额目标',
  `entry_amt` decimal(38,6) comment '入库额', 
  `join_entry_amt` decimal(38,6) comment '集采入库额',
  join_entry_completion_ratio DECIMAL(38,6) comment '集采入库占比' ,
   join_entry_completion_rate DECIMAL(38,6) comment '集采入库达成率' ,
   update_time TIMESTAMP comment '数据更新时间'
  )comment'供应链管理品类绩效报表'
  partitioned by(sdt string comment'日期分区')
  STORED AS PARQUET
  ;

-- 供应链管理品类绩效对应负责人
  
	DROP table csx_tmp.report_scm_r_d_classify_performance_person;
CREATE  TABLE `csx_tmp.report_scm_r_d_classify_performance_person`(
   sales_months string COMMENT '销售月份',
  `classify_large_code` string comment '管理一级', 
  `classify_large_name` string comment '管理一级', 
  `classify_middle_code` string comment '管理二级', 
  `classify_middle_name` string comment '管理二级', 
   classify_large_management string comment '管理一级负责人',
   classify_middle_management string comment '管理二级负责人',
   sales_target decimal(38,6) comment '销售目标额',
   daliy_sales_target decimal(38,6) comment '日配销售目标额',
   join_entry_target decimal(38,6) comment'集采入库额目标',
   update_time TIMESTAMP comment '数据更新时间'
  )comment'供应链管理品类绩效对应负责人'
  partitioned by(sdt string comment'日期分区')
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  
  ;

-- 增加省区维度
  	DROP table csx_tmp.report_scm_r_d_classify_performance_person;
CREATE  TABLE `csx_tmp.report_scm_r_d_classify_performance_person`(
   sales_months string COMMENT '销售月份',
  `classify_large_code` string comment '管理一级', 
  `classify_large_name` string comment '管理一级', 
  `classify_middle_code` string comment '管理二级', 
  `classify_middle_name` string comment '管理二级', 
   classify_large_management string comment '管理一级负责人',
   classify_middle_management string comment '管理二级负责人',
   province_name string comment '省区',
   sales_target decimal(38,6) comment '销售目标额',
   daliy_sales_target decimal(38,6) comment '日配销售目标额',
   join_entry_target decimal(38,6) comment'集采入库额目标',
   update_time TIMESTAMP comment '数据更新时间'
  )comment'供应链管理品类绩效对应负责人'
  partitioned by(sdt string comment'日期分区')
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  
  ;
  -- INVALIDATE METADATA csx_tmp.report_scm_r_d_classify_performance_fr;
