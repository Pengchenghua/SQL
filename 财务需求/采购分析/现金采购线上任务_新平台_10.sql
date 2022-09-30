-- 财务采购分析【现金采买分析】
-- 采购现金分析,
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式

set hive.support.quoted.identifiers=none;

set purpose = ('01','02','03','05','07','08');
set edate = regexp_replace('${enddate}','-','');
set sdate = regexp_replace(trunc('${enddate}','MM'),'-','');
set month = substr(regexp_replace('${enddate}','-',''),1,6);
set year  = concat(substr('${enddate}',1,4),'0101');


-- select ${hiveconf:edate},${hiveconf:sdate},${hiveconf:month};
-- select * from  csx_analyse_tmp.csx_analyse_tmp_dc_new ;

-- 大区处理
drop table csx_analyse_tmp.csx_analyse_tmp_dc_new ;
create  TABLE csx_analyse_tmp.csx_analyse_tmp_dc_new as 
select case when performance_region_code!='10' then '大区'else '平台' end dept_name,
    purchase_org,
    purchase_org_name,
    performance_region_code as region_code,
    performance_region_name as region_name,
    case when performance_province_name like'平台%' then '00'    else performance_region_code end  region_code,
    case when performance_province_name like'平台%' then '平台'  else  performance_region_name end  region_name,
    shop_code ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    basic_performance_city_code as performance_city_code,
    basic_performance_city_name as performance_city_name,
    basic_performance_province_code as performance_province_code,
    basic_performance_province_name as performance_province_name,
    case when c.dc_code is not null then '1' else '0' end as is_purchase_dc ,
    enable_date
from csx_dim.csx_dim_shop a 
left join 
(select a.code as province_code,
       a.name as province_name,
       b.code region_code,
       b.name region_name 
from csx_dim.csx_dim_basic_performance_region_province_city_tomysql a 
 left join 
(select code,name,parent_code 
from csx_dim.csx_dim_basic_performance_region_province_city_tomysql
 where level=1)  b on a.parent_code=b.code
 where level=2) b on a.basic_performance_province_code=b.province_code
 left join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.shop_code=c.dc_code
 where sdt='current'
    and purpose in  ('01','02','03','05','07','08')    
    ;
    
    

drop table  csx_analyse_tmp.csx_analyse_tmp_purchase_01 ;
create  table csx_analyse_tmp.csx_analyse_tmp_purchase_01 as 
SELECT dept_name,
       d.region_code,
       d.region_name,
       d.performance_province_code as province_code,
       d.performance_province_name as province_name,
       d.performance_city_code as city_code,
       d.performance_city_name as city_name,
       b.source_type_name,
       channel,
       source_channel,
       a.order_business_type,
       a.supplier_classify_code,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       supplier_code,
       d.is_purchase_dc,
       case when a.supplier_name like '%永辉%' then '云超配送'
            when business_type_name like '云超配送%' then '云超配送'
       else '供应商配送' end business_type_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)) AS net_entry_amount,
       coalesce(sum(case when d.is_purchase_dc='1' and classify_large_code ='B02' then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0) as B02_entry_amount,
       coalesce(sum(case when order_business_type=1 and classify_large_code ='B02' and d.is_purchase_dc='1' then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) base_entry_amount,
       d.purpose,
       sdt,
       substr(sdt,1,6) months,
       concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) quarter  ,
       substr(sdt,1,4) year,
       t.week_of_year,
       concat(t.week_begin,'-',week_end) as week_date,
       enable_date
FROM  csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
left join 
(select order_code, 
    source_type,
    config_value as source_type_name,
    channel,
    concat(config_value,channel) source_channel
 from csx_dws.csx_dws_scm_order_detail_di a
 left join
(select config_type,
        config_key,
        config_value,
        config_old_value,
        description
from  csx_ods.csx_ods_csx_b2b_scm_scm_configuration_df  a
 where a.config_type = 'PURCHASE_ORDER_SOURCE_TYPE' 
 and sdt=regexp_replace(date_sub(current_date(),1),'-','') 
 ) b on a.source_type=b.config_key
group by  order_code,source_type,channel,config_value,concat(config_value,channel)
) b on a.purchase_order_code = b.order_code
join csx_dim.csx_dim_basic_date t on a.sdt=t.calday
join csx_analyse_tmp.csx_analyse_tmp_dc_new  d on a.dc_code=d.shop_code 
WHERE
    sdt <= '${edate}'
    and sdt>=  '${sdate}'
   and b.source_type_name not in  ('城市服务商','联营直送','项目合伙人')
  GROUP BY dept_name,
       d.region_code,
       d.region_name,
       d.performance_city_code,
       d.performance_city_name,
       d.performance_province_code,
       d.performance_province_name,
       a.order_business_type,
       a.supplier_classify_code,
       classify_middle_name,
       a.classify_middle_code,
       supplier_code,
       case when a.supplier_name like '%永辉%' then '云超配送'
       when business_type_name like '云超配送%' then '云超配送'
              ELSE '供应商配送' end ,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end,
       classify_large_code,
       classify_large_name,
       sdt,
       d.is_purchase_dc,
       d.purpose,
       enable_date,
        substr(sdt,1,6) ,
       concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1)   ,
       substr(sdt,1,4),
       t.week_of_year,
       concat(t.week_begin,'-',week_end),
       b.source_type_name,
       source_channel

;

insert overwrite table  csx_analyse.csx_analyse_fr_scm_purchase_cash_analysis_di partition(months) 
SELECT months as sales_month,
        dept_name,
       channel as source_terminal,
       source_type as source_type_code,
       source_type_name,
       source_channel,
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
       sum( net_entry_amount) net_entry_amount,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       current_timestamp(),
       months
 from  csx_analyse_tmp.csx_analyse_tmp_purchase_01   a
 group by dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       months,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       source_channel,
       channel ,
       source_type ,
       source_type_name,
       source_channel;


show CREATE TABLE  csx_analyse_tmp.report_fr_r_m_financial_purchase_detail
;



  
 drop table csx_analyse_tmp.report_r_d_purchase_cash_analysis;
CREATE TABLE `csx_analyse.csx_analyse_fr_scm_purchase_cash_analysis_di`(
    sales_month string comment '销售月份',
  `dept_name` string comment '运营部门平台、大区', 
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_code` string comment '城市编码', 
  `city_name` string comment '城市名称', 
   source_terminal string comment '来源终端',
   source_type_code string comment '来源类型编码',
   source_type_name string comment '来源类型名称',
   source_channel string comment '来源类型订单',
  `bd_id` string comment '部类编码', 
  `bd_name` string comment '部类名称', 
  `classify_large_code` string comment '管理大类', 
  `classify_large_name` string comment '管理大类', 
  `classify_middle_code` string comment '管理中类', 
  `classify_middle_name` string comment '管理中类', 
  `net_entry_amount` decimal(38,6) comment '净入库额', 
  `cash_entry_amount` decimal(38,6) COMMENT '现金采买金额', 
  update_time TIMESTAMP COMMENT '数据更新日期',
  primary key (id),
  key (sales_month, region_code,region_name,province_code,province_name)using btree
  ) ENGINE=InnoDB CHARSET=utf8mb4  comment='现金采买报表明细'

  ;


  CREATE TABLE `data_analyse_prd.report_fr_scm_purchase_cash_analysis_di`(
    id int not NULL auto_increment,
    sales_month varchar(64) comment '销售月份',
  `dept_name` varchar(64) comment '运营部门平台、大区', 
  `region_code` varchar(64) comment '大区编码', 
  `region_name` varchar(64) comment '大区名称', 
  `province_code` varchar(64) comment '省区编码', 
  `province_name` varchar(64) comment '省区名称', 
  `city_code` varchar(64) comment '城市编码', 
  `city_name` varchar(64) comment '城市名称', 
   source_terminal varchar(64) comment '来源终端',
   source_type_code varchar(64) comment '来源类型编码',
   source_type_name varchar(64) comment '来源类型名称',
   source_channel varchar(64) comment '来源类型订单',
  `bd_id` varchar(64) comment '部类编码', 
  `bd_name` varchar(64) comment '部类名称', 
  `classify_large_code` varchar(64) comment '管理大类', 
  `classify_large_name` varchar(64) comment '管理大类', 
  `classify_middle_code` varchar(64) comment '管理中类', 
  `classify_middle_name` varchar(64) comment '管理中类', 
  `net_entry_amount` decimal(38,6) comment '净入库额', 
  `cash_entry_amount` decimal(38,6) COMMENT '现金采买金额', 
  update_time TIMESTAMP COMMENT '数据更新日期',
  primary key (id),
  key (sales_month, region_code,region_name,province_code,province_name)using btree
  ) ENGINE=InnoDB CHARSET=utf8mb4  comment='现金采买报表明细'

  