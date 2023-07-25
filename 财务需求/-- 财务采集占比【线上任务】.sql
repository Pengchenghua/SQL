
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式

set hive.support.quoted.identifiers=none;

set purpose = ('01','02','03','05','07','08');
set edate = regexp_replace('${enddate}','-','');
set sdate = regexp_replace(trunc('${enddate}','MM'),'-','');
set month = substr(regexp_replace('${enddate}','-',''),1,6);


-- select ${hiveconf:edate},${hiveconf:sdate},${hiveconf:month};
-- select * from  csx_tmp.temp_dc_new ;

-- 大区处理
drop table csx_tmp.temp_dc_new ;
create TEMPORARY TABLE csx_tmp.temp_dc_new as 
select case when region_code!='10' then '大区'else '平台' end dept_name,
    region_code,
    region_name,
    sales_province_code,
    sales_province_name,
    purchase_org,
    purchase_org_name,
    case when performance_province_name like'平台%' then '00' else   sales_region_code end sales_region_code,
    case when performance_province_name like'平台%' then '平台' else  sales_region_name end sales_region_name,
    shop_id ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
    case when c.dc_code is not null then '1' else '0' end as is_purchase_dc 
from csx_dw.dws_basic_w_a_csx_shop_m a 
left join 
(select a.code as province_code,a.name as province_name,b.code region_code,b.name region_name 
from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql a 
 left join 
(select code,name,parent_code from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql where level=1)  b on a.parent_code=b.code
 where level=2) b on a.performance_province_code=b.province_code
 left join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date from csx_ods.source_basic_w_a_conf_supplychain_location where sdt='20220718') c on a.shop_id=c.dc_code
 where sdt='current'    
      and table_type=1 
    ;
    
    
drop table  csx_tmp.temp_purchase_01 ;
create temporary table csx_tmp.temp_purchase_01 as 
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       sum(net_entry_amount) as net_entry_amount,
       sum(coalesce(B02_entry_amount,0 )  ) B02_entry_amount,  -- 蔬果采购
       sum(coalesce(base_entry_amount ,0 )  ) base_entry_amount,  -- 基地采购   
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       sdt
FROM
(
SELECT dept_name,
       d.region_code,
       d.region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       a.order_business_type,
       a.supplier_classify_code,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       case when a.supplier_name like '%永辉%' then '云超配送'
            when business_type_name like '云超配送%' then '云超配送'
       else '供应商配送' end business_type_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)) AS net_entry_amount,
       coalesce(sum(case when d.is_purchase_dc='1' and classify_large_code ='B02' then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0) as B02_entry_amount,
       coalesce(sum(case when order_business_type=1 and classify_large_code ='B02' and d.is_purchase_dc='1' then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) base_entry_amount,
       d.purpose,
       sdt
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
join csx_tmp.temp_dc_new  d on a.dc_code=d.shop_id 
WHERE months <= ${hiveconf:month}
    and months>='202201'
  AND d.purpose in ${hiveconf:purpose}
  and source_type_name not in  ('城市服务商','联营直送','项目合伙人')
  GROUP BY dept_name,
       d.region_code,
       d.region_name,
       performance_city_code,
       performance_city_name,
       performance_province_code,
       performance_province_name,
       a.order_business_type,
       a.supplier_classify_code,
       classify_middle_name,
       a.classify_middle_code,
       case when a.supplier_name like '%永辉%' then '云超配送'
       when business_type_name like '云超配送%' then '云超配送'
              ELSE '供应商配送' end ,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end,
       classify_large_code,
       classify_large_name,
       sdt,
       d.purpose
) a 
GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       sdt
;



-- show create table csx_tmp.report_r_m_purchase_entry_analysis;

 insert overwrite table csx_tmp.report_r_m_purchase_entry_analysis partition(months)
select 
substr(sdt,1,4) year
,concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) quarter
,dept_name
,region_code
,region_name
,province_code
,province_name
,city_code
,city_name
,bd_id
,bd_name
,classify_large_code
,classify_large_name
,classify_middle_code
,classify_middle_name
-- ,if(coalesce(b02_entry_amount,0)!=0,'1','0') group_purchase_tag
,sum(net_entry_amount) as net_entry_amount
,sum(b02_entry_amount) as b02_entry_amount
,sum(base_entry_amount) as base_entry_amount
,sum(cash_entry_amount) as cash_entry_amount
,sum(yh_entry_amount) as yh_entry_amount
,current_timestamp,
substr(sdt,1,6) as months
from csx_tmp.temp_purchase_01
group by 
substr(sdt,1,4)  
,concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1)  
,dept_name
,region_code
,region_name
,province_code
,province_name
,city_code
,city_name
,bd_id
,bd_name
,classify_large_code
,classify_large_name
,classify_middle_code
,classify_middle_name
,substr(sdt,1,6)
;

-- 插入周分析表
 insert overwrite table csx_tmp.report_r_w_purchase_entry_analysis partition(week)
select 
substr(sdt,1,4) year
,dept_name
,region_code
,region_name
,province_code
,province_name
,city_code
,city_name
,bd_id
,bd_name
,classify_large_code
,classify_large_name
,classify_middle_code
,classify_middle_name
-- ,if(coalesce(b02_entry_amount,0)!=0,'1','0') group_purchase_tag
,sum(net_entry_amount) as net_entry_amount
,sum(b02_entry_amount) as b02_entry_amount
,sum(base_entry_amount) as base_entry_amount
,sum(cash_entry_amount) as cash_entry_amount
,sum(yh_entry_amount) as yh_entry_amount
,date_interval
,current_timestamp
,week_of_year
from csx_tmp.temp_purchase_01 a 
left join 
(select calday,week_of_year,concat( week_begin,'-',week_end) date_interval from csx_dw.dws_basic_w_a_date where calday>='20210101' and calday<= ${hiveconf:edate}) b on a.sdt=b.calday
group by 
substr(sdt,1,4)  
,concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1)  
,dept_name
,region_code
,region_name
,province_code
,province_name
,city_code
,city_name
,bd_id
,bd_name
,classify_large_code
,classify_large_name
,classify_middle_code
,classify_middle_name
,week_of_year
,date_interval
;

-- 集采入库 剔除蔬果B0202
drop table   csx_tmp.temp_join_entry ;
create table csx_tmp.temp_join_entry as 
SELECT dept_name,
       region_code,
       region_name,
       coalesce(province_code,'')province_code,
       coalesce(province_name,'')province_name,
       coalesce(city_code,'')city_code,
       coalesce(city_name,'')city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_tag,      -- 集采标签
       sum(group_purchase_amount) group_purchase_amount,
       sum(net_entry_amount) net_entry_amount,       
       sdt
FROM 
(
SELECT d.dept_name,
       d.region_code,
       d.region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       b.short_name,
       a.classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when  b.classify_small_code IS NOT NULL and short_name is not NULL then '1' end group_purchase_tag,
       coalesce(sum(case when joint_purchase_flag=1 and b.classify_small_code IS NOT NULL and a.months>= substr(regexp_replace(start_date,'-',''),1,6) and is_flag='0' then receive_amt-shipped_amt end ),0) as group_purchase_amount,
       sum(receive_amt-shipped_amt) AS net_entry_amount,
       sdt
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
left join  csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
 join 
  csx_tmp.temp_dc_new d on a.dc_code=d.shop_id 
WHERE months <= ${hiveconf:month}
    and months>='202201'
   and source_type_name not in ('城市服务商','联营直送','项目合伙人')
   and super_class_name in ('供应商订单','供应商退货订单')
   -- AND d.purpose IN ('01','03')
   and d.is_purchase_dc='1'
  and a.classify_middle_code !='B0202'
  GROUP BY d.sales_region_code,
      d.sales_region_name,
      performance_city_code,
      performance_city_name,
      performance_province_code,
       performance_province_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end ,
       b.short_name,
       d.dept_name,
       d.region_code,
       d.region_name,
       sdt,
       a.classify_large_code,
       classify_large_name,
        case when  b.classify_small_code IS NOT NULL and short_name is not NULL then '1' end 
    ) a
GROUP BY dept_name,
        region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       short_name,
       classify_middle_code,
       classify_middle_name,
       sdt,
       group_purchase_tag,
       a.classify_large_code,
       classify_large_name

       ;



-- 集采销售 剔除蔬菜 B0202
drop table   csx_tmp.temp_join_sale ;
create table csx_tmp.temp_join_sale as 
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       group_purchase_tag,
       sales_value,
       profit,
       group_purchase_sales_value,
       group_purchase_profit,
       sdt 
FROM
(
SELECT dept_name,
        d.region_code,
       d.region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       case when   a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when   a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       b.short_name,
       a.classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when  b.classify_small_code IS NOT NULL and short_name is not NULL then '1' end group_purchase_tag,
       sum(a.sales_value) AS sales_value,
       sum(a.profit) profit,
       sum( case when  b.classify_small_code IS NOT NULL and a.sdt>= regexp_replace(start_date,'-','') and is_flag='0' then sales_value end ) group_purchase_sales_value,
       sum( case when  b.classify_small_code IS NOT NULL and a.sdt>= regexp_replace(start_date,'-','') and is_flag='0' then profit end ) group_purchase_profit,
       sdt 
FROM csx_dw.dws_sale_r_d_detail a 
left join  csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
join  csx_tmp.temp_dc_new d on a.dc_code=d.shop_id 
WHERE a.sdt >= '20220101'
    and sdt<= ${hiveconf:edate}
    and a.channel_code in ('1','7','9')
    and a.business_type_code='1'
    and a.classify_middle_code !='B0202'
    and d.is_purchase_dc='1'
GROUP BY dept_name,
        d.region_code,
       d.region_name,
       performance_city_code,
       performance_city_name,
       performance_province_code,
       performance_province_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end,
       b.short_name,
       sdt,
       case when  b.classify_small_code IS NOT NULL and short_name is not NULL then '1' end,
       a.classify_large_code,
       classify_large_name

) a 

;
 
 drop table  csx_tmp.temp_group_purchase_analysis_report;
 CREATE  temporary  table csx_tmp.temp_group_purchase_analysis_report as 
 SELECT dept_name,
       region_code,
       region_name,
       coalesce(province_code,'')province_code,
       coalesce(province_name,'')province_name,
       coalesce(city_code,'')city_code,
       coalesce(city_name,'')city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_tag  ,      -- 集采标签
       sum(group_purchase_amount)  group_purchase_amount,
       sum(net_entry_amount)  net_entry_amount,  
       sum(sales_value) sales_value,
       sum(profit) profit,
       sum(profit)/sum(sales_value) as profit_rate,
       sum(group_purchase_sales_value) group_purchase_sales_value,
       sum(group_purchase_profit) group_purchase_profit,
       sum(group_purchase_profit)/sum(group_purchase_sales_value) as group_purchase_profit_rate,
       sdt
FROM 
 (SELECT dept_name,
       region_code,
       region_name,
       coalesce(province_code,'')province_code,
       coalesce(province_name,'')province_name,
       coalesce(city_code,'')city_code,
       coalesce(city_name,'')city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_tag,      -- 集采标签
       group_purchase_amount,
       net_entry_amount,  
       0 sales_value,
       0 profit,
       0 group_purchase_sales_value,
       0 group_purchase_profit,
       sdt
FROM csx_tmp.temp_join_entry a
union all 
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       group_purchase_tag,
       0 group_purchase_amount,
       0 net_entry_amount, 
       sales_value,
       profit,
       group_purchase_sales_value,
       group_purchase_profit,
       sdt
FROM csx_tmp.temp_join_sale a
 ) a 
 group by dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       short_name,
       group_purchase_tag,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       sdt
     ;  
     
 insert overwrite table csx_tmp.report_r_m_group_purchase_analysis partition(months)
select 
        substr(sdt,1,4) year,
       concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) quarter  ,
       dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_tag  ,      -- 集采标签
       sum(group_purchase_amount)   group_purchase_amount,
       sum(net_entry_amount )   net_entry_amount,  
       sum(sales_value) sales_value,
       sum(profit)  profit,
       sum(profit)/sum(sales_value) profit_rate,
       sum(group_purchase_sales_value)  group_purchase_sales_value,
       sum(group_purchase_profit)   group_purchase_profit,
       sum(group_purchase_profit)/sum(group_purchase_sales_value)  group_purchase_profit_rate,
       current_timestamp,
       substr(sdt,1,6) as months
    from csx_tmp.temp_group_purchase_analysis_report a
    group by substr(sdt,1,4) ,
       concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1)   ,
       dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_tag ,
       substr(sdt,1,6)

;

 insert overwrite table csx_tmp.report_r_w_group_purchase_analysis partition(week)
select 
        substr(sdt,1,4) year,
       dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_tag  ,      -- 集采标签
       sum(group_purchase_amount)   group_purchase_amount,
       sum(net_entry_amount )   net_entry_amount,  
       sum(sales_value) sales_value,
       sum(profit)  profit,
       sum(profit)/sum(sales_value) profit_rate,
       sum(group_purchase_sales_value)  group_purchase_sales_value,
       sum(group_purchase_profit)   group_purchase_profit,
       sum(group_purchase_profit)/sum(group_purchase_sales_value)  group_purchase_profit_rate,
       date_interval,
       current_timestamp,
       week_of_year
    from csx_tmp.temp_group_purchase_analysis_report a
    left join 
(select calday,week_of_year,concat( week_begin,'-',week_end) date_interval from csx_dw.dws_basic_w_a_date where calday>='20210101' and calday<= ${hiveconf:edate}) b on a.sdt=b.calday
    group by substr(sdt,1,4) ,
       dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_tag ,
       date_interval,
       week_of_year

;



drop table `csx_tmp.report_r_w_purchase_entry_analysis`
;
	CREATE TABLE `csx_tmp.report_r_w_purchase_entry_analysis`(
	  `year` string COMMENT '年', 
	  `dept_name` string COMMENT '营运部门:平台、大区', 
	  `region_code` string COMMENT '大区编码', 
	  `region_name` string COMMENT '大区名称', 
	  `province_code` string COMMENT '省区编码', 
	  `province_name` string COMMENT '省区名称', 
	  `city_code` string COMMENT '城市编码', 
	  `city_name` string COMMENT '城市名称', 
	  `bd_id` string COMMENT '采购部门编码', 
	  `bd_name` string COMMENT '采购部门名称', 
	  `classify_large_code` string COMMENT '管理大类', 
	  `classify_large_name` string COMMENT '管理大类', 
	  `classify_middle_code` string COMMENT '管理中类', 
	  `classify_middle_name` string COMMENT '管理中类', 
	  `net_entry_amount` decimal(38,6) COMMENT '类别净入库额', 
	  `b02_entry_amount` decimal(38,6) COMMENT '蔬果净入库额(大+工厂)', 
	  `base_entry_amount` decimal(38,6) COMMENT '基地净入库额(大+工厂)', 
	  `cash_entry_amount` decimal(38,6) COMMENT '现金采购', 
	  `yh_entry_amount` decimal(38,6) COMMENT '云超采购', 
       date_interval string comment '日期区间',
	  `update_time` timestamp COMMENT '数据插入时间')
	COMMENT '采购周分析-整体分析&基地分析'
	PARTITIONED BY ( 
	  `week` string COMMENT '周分区')
	STORED as parquet
;



CREATE TABLE `csx_tmp.report_r_w_group_purchase_analysis`(
  `year` string COMMENT '年',  
  `dept_name` string COMMENT '营运部门 平台、大区', 
  `region_code` string COMMENT '大区编码', 
  `region_name` string COMMENT '大区名称', 
  `province_code` string COMMENT '省区编码', 
  `province_name` string COMMENT '省区名称', 
  `city_code` string COMMENT '城市编码', 
  `city_name` string COMMENT '城市名称', 
  `bd_id` string COMMENT '采购部门编码', 
  `bd_name` string COMMENT '采购部门名称', 
  `short_name` string COMMENT '集采分级简称', 
  `classify_large_code` string COMMENT '管理大类', 
  `classify_large_name` string COMMENT '管理大类', 
  `classify_middle_code` string COMMENT '管理中类', 
  `classify_middle_name` string COMMENT '管理中类', 
  `group_purchase_tag` string COMMENT '集采标签 1', 
  `group_purchase_amount` decimal(38,6) COMMENT '集采净入库额', 
  `net_entry_amount` decimal(38,6) COMMENT '类别净入库额', 
  `sales_value` decimal(38,6) COMMENT '类别销售额', 
  `profit` decimal(38,6) COMMENT '类别毛利额', 
  `profit_rate` decimal(38,6) COMMENT '类别毛利率', 
  `group_purchase_sales_value` decimal(38,6) COMMENT '集采销售额', 
  `group_purchase_profit` decimal(38,6) COMMENT '集采毛利额', 
  `group_purchase_profit_rate` decimal(38,6) COMMENT '集采毛利率', 
   date_interval string comment '日期区间',
  `update_time` timestamp COMMENT '数据插入时间')
COMMENT '采购周分析-集采分析'
PARTITIONED BY ( 
  `week` string COMMENT '周分区')
STORED AS parquet 
;



drop table `csx_tmp.report_r_w_purchase_entry_analysis`
;
	CREATE TABLE `csx_tmp.report_r_w_purchase_entry_analysis`(
	  `year` string COMMENT '年', 
	  `dept_name` string COMMENT '营运部门:平台、大区', 
	  `region_code` string COMMENT '大区编码', 
	  `region_name` string COMMENT '大区名称', 
	  `province_code` string COMMENT '省区编码', 
	  `province_name` string COMMENT '省区名称', 
	  `city_code` string COMMENT '城市编码', 
	  `city_name` string COMMENT '城市名称', 
	  `bd_id` string COMMENT '采购部门编码', 
	  `bd_name` string COMMENT '采购部门名称', 
	  `classify_large_code` string COMMENT '管理大类', 
	  `classify_large_name` string COMMENT '管理大类', 
	  `classify_middle_code` string COMMENT '管理中类', 
	  `classify_middle_name` string COMMENT '管理中类', 
	  `net_entry_amount` decimal(38,6) COMMENT '类别净入库额', 
	  `b02_entry_amount` decimal(38,6) COMMENT '蔬果净入库额(大+工厂)', 
	  `base_entry_amount` decimal(38,6) COMMENT '基地净入库额(大+工厂)', 
	  `cash_entry_amount` decimal(38,6) COMMENT '现金采购', 
	  `yh_entry_amount` decimal(38,6) COMMENT '云超采购', 
       date_interval string comment '日期区间',
	  `update_time` timestamp COMMENT '数据插入时间')
	COMMENT '采购周分析-整体分析&基地分析'
	PARTITIONED BY ( 
	  `week` string COMMENT '周分区')
	STORED as parquet
;
 