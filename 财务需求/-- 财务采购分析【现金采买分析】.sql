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
    case when c.dc_code is not null then '1' else '0' end as is_purchase_dc ,
    enable_date
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
       d.region_code,
       d.region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       b.source_type_name,
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
FROM  csx_tmp.report_fr_r_m_financial_purchase_detail a 
left join 
(select order_code, source_type,config_value as source_type_name,channel,concat(config_value,channel) source_channel from csx_dw.dws_scm_r_d_order_detail a
 left join
(select *
from  csx_ods.source_scm_w_d_configuration  a where a.config_type = 'PURCHASE_ORDER_SOURCE_TYPE' and sdt=regexp_replace(date_sub(current_date(),1),'-','') ) b on a.source_type=b.config_key
group by  order_code,source_type,channel,config_value,concat(config_value,channel)
) b on a.purchase_order_code = b.order_code
join csx_dw.dws_basic_w_a_date t on a.sdt=t.calday
join csx_tmp.temp_dc_new  d on a.dc_code=d.shop_id 
WHERE months <= ${hiveconf:month}
    and months>= substr(${hiveconf:year},1,6)
  AND d.purpose in ${hiveconf:purpose}
  and b.source_type_name not in  ('城市服务商','联营直送','项目合伙人')
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

create table csx_tmp.temp_cash_01 as 
SELECT dept_name,
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
       months
 from  csx_tmp.temp_purchase_01   a
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
       source_channel;


show CREATE TABLE  csx_tmp.report_fr_r_m_financial_purchase_detail
;



  
 drop table csx_tmp.report_r_d_purchase_cash_analysis;
CREATE TABLE `csx_tmp.report_r_d_purchase_cash_analysis`(
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
  update_time TIMESTAMP COMMENT '数据更新日期'
  )comment'现金采买报表明细'
 partitioned by (months string comment '日期分区')
STORED AS parquet 
  
  