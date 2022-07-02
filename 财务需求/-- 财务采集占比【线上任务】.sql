-- 财务采集占比【线上任务】

set hive.execution.engine = tez;
set tez.queue.name = caishixian;
set purpose = ('01','02','03','05','07','08');
set edate = regexp_replace('${enddate}','-','');
set sdate = regexp_replace(trunc(${enddate},'MM'),'-','');
set month = substr(regexp_replace(${enddate}',-',''),1,6);

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
    performance_province_name
from csx_dw.dws_basic_w_a_csx_shop_m a 
left join 
(select a.code as province_code,a.name as province_name,b.code region_code,b.name region_name 
from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql a 
 left join 
(select code,name,parent_code from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql where level=1)  b on a.parent_code=b.code
 where level=2) b on a.performance_province_code=b.province_code
 where sdt='current'    
      and table_type=1 
    ;
drop table  csx_tmp.temp_purchase_01 ;
create table csx_tmp.temp_purchase_01 as 
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
       sum(coalesce(case when classify_large_code ='B02' and purpose in('01','03') then net_entry_amount end ,0 )  ) B02_entry_amount,  -- 蔬果采购
       sum(coalesce(case when order_business_type=1 and classify_large_code ='B02' and purpose in('01','03') then net_entry_amount end ,0 )  ) base_entry_amount,  -- 基地采购
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       months
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
       sum(receive_amt-shipped_amt) AS net_entry_amount,
       d.purpose,
       months
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
join csx_tmp.temp_dc_new  d on a.dc_code=d.shop_id 
WHERE months = ${hiveconf:month}
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
       months,
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
       months
;



--  基地采购
drop   table csx_tmp.temp_jd_purchase ;
create table csx_tmp.temp_jd_purchase as 
SELECT 
       dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       sum(net_amt) AS net_entry_amount,
       sum(coalesce(case when order_business_type=1 then net_amt end,0 )  ) base_entry_amount,  -- 基地采购
       months
FROM
(
SELECT d.dept_name,
       d.region_code,
       d.region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       a.order_business_type,
       a.supplier_classify_code,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       sum(receive_amt-shipped_amt) AS net_amt,
       months
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
 join csx_tmp.temp_dc_new  d on a.dc_code=d.shop_id 
WHERE months= ${hiveconf:month}
  AND d.purpose IN ('01','03')
  and a.classify_large_code='B02'
  and source_type_name not in  ('城市服务商','联营直送','项目合伙人')
  GROUP BY  d.dept_name,
    d.region_code,
    d.region_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
    a.order_business_type,
    a.supplier_classify_code,
    classify_middle_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    case when a.division_code in ('10','11') then '11' else '12' end ,
    case when a.division_code in ('10','11') then '生鲜' else '食百' end,
    months
) a 
GROUP BY  dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       months
;


-- 集采入库 剔除蔬果B02
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
       months
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
       case when  a.classify_small_code IS NOT NULL and short_name is not NULL then '1' end group_purchase_tag,
       coalesce(sum(case when joint_purchase_flag=1 and a.classify_small_code IS NOT NULL then receive_amt-shipped_amt end ),0) as group_purchase_amount,
       sum(receive_amt-shipped_amt) AS net_entry_amount,
       months
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
left join  csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
 join 
  csx_tmp.temp_dc_new d on a.dc_code=d.shop_id 
WHERE months = ${hiveconf:month}
   and source_type_name not in ('城市服务商','联营直送','项目合伙人')
   and super_class_name in ('供应商订单','供应商退货订单')
  AND d.purpose IN ('01','03')
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
       months,
       a.classify_large_code,
       classify_large_name,
       case when  a.classify_small_code IS NOT NULL then '1' end
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
       months,
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
       months 
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
       case when  a.classify_small_code IS NOT NULL and short_name is not NULL then '1' end group_purchase_tag,
       sum(a.sales_value) AS sales_value,
       sum(a.profit) profit,
       sum( case when  a.classify_small_code IS NOT NULL then sales_value end ) group_purchase_sales_value,
       sum( case when  a.classify_small_code IS NOT NULL then profit end ) group_purchase_profit,
       substr(sdt,1,6) months
FROM csx_dw.dws_sale_r_d_detail a 
left join  csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
join  csx_tmp.temp_dc_new d on a.dc_code=d.shop_id 
WHERE a.sdt >= ${hiveconf:sdate}
    and sdt<= ${hiveconf:edate}
    and a.channel_code in ('1','7','9')
    and a.business_type_code='1'
    and a.classify_middle_code !='B0202'
    AND d.purpose IN ('01','03')
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
       substr(sdt,1,6),
       case when  a.classify_small_code IS NOT NULL then '1' end,
       a.classify_large_code,
       classify_large_name

) a 

;
 
 
 CREATE table csx_tmp.temp_group_purchase_analysis_report as 
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
       months
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
       months
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
       
       months 
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
       months
     ;  
     
CREATE TABLE `csx_tmp.report_r_m_group_purchase_analysis`(
  year string comment '年',
  quarter string comment '季度',
  `dept_name` string comment '营运部门 平台、大区', 
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_code` string ccomment '城市编码', 
  `city_name` string ccomment '城市名称', 
  `bd_id` string comment '采购部门编码', 
  `bd_name` string comment '采购部门名称', 
  `short_name` string comment '集采分级简称', 
  `classify_large_code` string comment '管理大类', 
  `classify_large_name` string comment '管理大类', 
  `classify_middle_code` string comment '管理中类', 
  `classify_middle_name` string comment '管理中类', 
  `group_purchase_tag` string comment '集采标签 1', 
  `group_purchase_amount` decimal(38,6) comment '集采净入库额', 
  `net_entry_amount` decimal(38,6) comment '类别净入库额', 
  `sales_value` decimal(38,6) comment '类别销售额', 
  `profit` decimal(38,6) comment '类别毛利额', 
  `profit_rate` decimal(38,6) comment '类别毛利率', 
  `group_purchase_sales_value` decimal(38,6) comment '集采销售额', 
  `group_purchase_profit` decimal(38,6) comment '集采毛利额', 
  `group_purchase_profit_rate` decimal(38,6) comment '集采毛利率', 
   update_time timestamp comment '数据插入时间'
   )comment '采购分析-集采分析'
partitioned by(months string c  '月分区')
STORED AS parquet 
 ;

 
CREATE TABLE `csx_tmp.report_r_m_purchase_analysis`(
  year string comment '年',
  quarter string comment '季度',
  `dept_name` string comment '营运部门 平台、大区', 
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_code` string ccomment '城市编码', 
  `city_name` string ccomment '城市名称', 
  `bd_id` string comment '采购部门编码', 
  `bd_name` string comment '采购部门名称', 
  `short_name` string comment '集采分级简称', 
  `classify_large_code` string comment '管理大类', 
  `classify_large_name` string comment '管理大类', 
  `classify_middle_code` string comment '管理中类', 
  `classify_middle_name` string comment '管理中类', 
  `group_purchase_tag` string comment '集采标签 1', 
  `net_entry_amount` decimal(38,6) comment '类别净入库额', 
  `B02_entry_amount` decimal(38,6) comment '蔬果净入库额(大客户+工厂)', 
  `base_entry_amount` decimal(38,6) comment '基地净入库额(大客户+工厂)', 
  `cash_entry_amount` decimal(38,6) comment '现金采购', 
  `yh_entry_amount` decimal(38,6) comment '云超采购'
   update_time timestamp comment '数据插入时间'
   )comment '采购分析-整体分析-基地分析'
partitioned by(months string c  '月分区')
STORED AS parquet 
;