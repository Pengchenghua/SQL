-- 财务采购分析【线上任务】
-- 采购整体分析&供应商数',
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
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
join csx_dw.dws_basic_w_a_date t on a.sdt=t.calday
join csx_tmp.temp_dc_new  d on a.dc_code=d.shop_id 
WHERE months <= ${hiveconf:month}
    and months>= substr(${hiveconf:year},1,6)
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
       concat(t.week_begin,'-',week_end) 
;


-- 周整体入库
drop table csx_tmp.temp_purchase_w;
create temporary table csx_tmp.temp_purchase_w as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       sum( net_entry_amount) net_entry_amount,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       count( distinct supplier_code ) as all_num,
       count( distinct case when supplier_classify_code=2 then supplier_code end   )    as cash_entry_num,
       count( distinct case when business_type_name='云超配送' then supplier_code end ) as yh_entry_num,
       week_of_year
FROM
    csx_tmp.temp_purchase_01  a 
   
GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       week_of_year
GROUPING SETS
    ( (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       week_of_year),
       (dept_name,
       week_of_year),
       (dept_name,
       bd_id,
       bd_name,
       week_of_year),
        (
       bd_id,
       bd_name,
      week_of_year),
       (week_of_year))
       ;


-- 月度整体入库
drop table csx_tmp.temp_purchase_m;
create temporary table csx_tmp.temp_purchase_m as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       sum( net_entry_amount) net_entry_amount,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       count( distinct supplier_code ) as all_num,
       count( distinct case when supplier_classify_code=2 then supplier_code end   )    as cash_entry_num,
       count( distinct case when business_type_name='云超配送' then supplier_code end ) as yh_entry_num,
       months
FROM
    csx_tmp.temp_purchase_01
GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       months
GROUPING SETS
    ( (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       months),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       months),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       months),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       months),
       (dept_name,
       region_code,
       region_name,
       months),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       months),
       (dept_name,
       months),
       (dept_name,
       bd_id,
       bd_name,
       months),
        (
       bd_id,
       bd_name,
      months),
       (months))
       ;



-- 季度整体入库
drop table csx_tmp.temp_purchase_q;
create temporary table csx_tmp.temp_purchase_q as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       sum( net_entry_amount) net_entry_amount,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       count( distinct supplier_code ) as all_num,
       count( distinct case when supplier_classify_code=2 then supplier_code end   )    as cash_entry_num,
       count( distinct case when business_type_name='云超配送' then supplier_code end ) as yh_entry_num,
       quarter
FROM
    csx_tmp.temp_purchase_01
GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       quarter
GROUPING SETS
    ( (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       quarter),
       (dept_name,
       quarter),
       (dept_name,
       bd_id,
       bd_name,
       quarter),
        (
       bd_id,
       bd_name,
      quarter),
       (quarter))
       ;
       
--  年度汇总   
drop table csx_tmp.temp_purchase_y;
create temporary table csx_tmp.temp_purchase_y as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       sum( net_entry_amount) net_entry_amount,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       count( distinct supplier_code ) as all_num,
       count( distinct case when supplier_classify_code=2 then supplier_code end   )    as cash_entry_num,
       count( distinct case when business_type_name='云超配送' then supplier_code end ) as yh_entry_num,
       year
FROM
    csx_tmp.temp_purchase_01
GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       year
GROUPING SETS
    ( (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       year),
       (dept_name,
       region_code,
       region_name,
       year),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       year),
       (dept_name,
       year),
       (dept_name,
       bd_id,
       bd_name,
       year),
        (
       bd_id,
       bd_name,
      year),
       (year))
       ; 
       
 
 -- 聚合周、月、季、年   
drop table csx_tmp.temp_purchase_m1;
create temporary table csx_tmp.temp_purchase_m1 as       
select date_m,
       case when dept_name ='合计' then '1'
            when region_name='' then '2'
            when province_name='' then '3'
            when city_name=''   then '4'
            else '5' end level_id,
       coalesce(dept_name,'合计')dept_name,
       coalesce(region_code,'00')region_code,
       case when dept_name ='大区' and region_name='' then '大区'
            when dept_name ='平台' and region_name='' then '平台'
            else region_name end   region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       net_entry_amount,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       net_entry_amount/sum(net_entry_amount)over(partition by dept_name,region_name,province_name,city_name,bd_name,sdt,date_m)*2 as net_entry_amount_ratio,
       yh_entry_amount/sum(yh_entry_amount)over(partition by dept_name,region_name,province_name,city_name,bd_name,sdt,date_m)*2 as yh_entry_amount_ratio,
       cash_entry_amount/sum(cash_entry_amount)over(partition by dept_name,region_name,province_name,city_name,bd_name,sdt,date_m)*2 as cash_entry_amount_ratio,
       all_num/sum(all_num)over(partition by dept_name,region_name,province_name,city_name,sdt,bd_name,date_m)*2 as                 all_num_ratio,
       cash_entry_num/sum(cash_entry_num)over(partition by dept_name,region_name,province_name,city_name,bd_name,sdt,date_m)*2 as   cash_entry_num_ratio,
       yh_entry_num/sum(yh_entry_num)over(partition by dept_name,region_name,province_name,city_name,bd_name,sdt,date_m)*2 as       yh_entry_num_ratio,
        sdt 
FROM (   
select 
       'week' date_m,
       coalesce(dept_name,'合计')dept_name,
       coalesce(region_code,'00')region_code,
       coalesce(region_name,'')region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       net_entry_amount,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       week_of_year sdt
FROM csx_tmp.temp_purchase_w
union all 
select 
       'months' date_m,
       coalesce(dept_name,'合计')dept_name,
       coalesce(region_code,'00')region_code,
       coalesce(region_name,'')region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       net_entry_amount,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       months sdt
FROM csx_tmp.temp_purchase_m
union all 
select 
       'quarter' date_m,
       coalesce(dept_name,'合计')dept_name,
       coalesce(region_code,'00')region_code,
       coalesce(region_name,'')region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       net_entry_amount,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       quarter sdt
FROM csx_tmp.temp_purchase_q 
union all 
select 
       'year' date_m,
       coalesce(dept_name,'合计')dept_name,
       coalesce(region_code,'00')region_code,
       coalesce(region_name,'')region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       net_entry_amount,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       year sdt
FROM csx_tmp.temp_purchase_y
) a ;


-- 周基地采购
drop table csx_tmp.temp_bash_w;
create temporary table csx_tmp.temp_bash_w as
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
       a.classify_middle_code,
       classify_middle_name,
       count( distinct  supplier_code  ) as b02_entry_num,
       count( distinct case when order_business_type=1 then supplier_code end ) as base_entry_num,
       sum(B02_entry_amount) as B02_entry_amount,
       sum(if(order_business_type=1,base_entry_amount,0)) as base_entry_amount,
       week_of_year
FROM
    csx_tmp.temp_purchase_01 a
    where is_purchase_dc=1
    and classify_large_code ='B02'
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
       week_of_year
GROUPING SETS
    ( (dept_name,
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
      week_of_year),
       (dept_name,
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
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       week_of_year),
       (dept_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       week_of_year),
       (dept_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       week_of_year),
        (
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       week_of_year),
       (
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       week_of_year)
       )
       ;
       
-- 月度
drop table csx_tmp.temp_bash_m;
create temporary table csx_tmp.temp_bash_m as
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
       a.classify_middle_code,
       classify_middle_name,
       count( distinct  supplier_code  ) as b02_entry_num,
       count( distinct case when order_business_type=1 then supplier_code end ) as base_entry_num,
       sum(B02_entry_amount) as B02_entry_amount,
       sum(if(order_business_type=1,base_entry_amount,0)) as base_entry_amount,
       months
FROM
    csx_tmp.temp_purchase_01 a
    where is_purchase_dc=1
    and classify_large_code ='B02'
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
GROUPING SETS
    ( (dept_name,
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
      months),
       (dept_name,
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
       months),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       months),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       months),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       months),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       months),
       (dept_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       months),
       (dept_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       months),
        (
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       months),
       (
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       months)
       
       )
       ;

-- 季度基地数据

drop table csx_tmp.temp_bash_q;
create temporary table csx_tmp.temp_bash_q as
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
       a.classify_middle_code,
       classify_middle_name,
       count( distinct  supplier_code  ) as b02_entry_num,
       count( distinct case when order_business_type=1 then supplier_code end ) as base_entry_num,
       sum(B02_entry_amount) as B02_entry_amount,
       sum(if(order_business_type=1,base_entry_amount,0)) as base_entry_amount,
       quarter
FROM
    csx_tmp.temp_purchase_01 a
    where is_purchase_dc=1
    and classify_large_code ='B02'
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
       quarter
GROUPING SETS
    ( (dept_name,
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
      quarter),
       (dept_name,
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
       quarter),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       quarter),
       (dept_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       quarter),
       (dept_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       quarter),
        (
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       quarter),
       (
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       quarter)
       
       )
       ;


-- 年度基地数据

drop table csx_tmp.temp_bash_y;
create temporary table csx_tmp.temp_bash_y as
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
       a.classify_middle_code,
       classify_middle_name,
       count( distinct  supplier_code  ) as b02_entry_num,
       count( distinct case when order_business_type=1 then supplier_code end ) as base_entry_num,
       sum(B02_entry_amount) as B02_entry_amount,
       sum(if(order_business_type=1,base_entry_amount,0)) as base_entry_amount,
       year
FROM
    csx_tmp.temp_purchase_01 a
    where is_purchase_dc=1
    and classify_large_code ='B02'
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
       year
GROUPING SETS
    ( (dept_name,
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
      year),
       (dept_name,
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
       year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       year),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       year),
       (dept_name,
       region_code,
       region_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       year),
       (dept_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       year),
       (dept_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       year),
        (
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       year),
       (
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       year)
       
       )
       ;


drop table csx_tmp.temp_bash_m1;
create temporary table csx_tmp.temp_bash_m1 as
SELECT  date_m,
    case when dept_name ='合计' then '1'
            when region_name='' then '2'
            when province_name='' then '3'
            when city_name=''   then '4'
            else '5' end level_id,
       coalesce(dept_name,'合计')dept_name,
       coalesce(region_code,'00')region_code,
       case when dept_name ='大区' and region_name='' then '大区'
            when dept_name ='平台' and region_name='' then '平台'
            else region_name end   region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       coalesce(classify_large_code,'00') as classify_large_code,
       coalesce(classify_large_name,'') as classify_large_name,
       coalesce(a.classify_middle_code,'00') as classify_middle_code,
       coalesce(classify_middle_name,'') as classify_middle_name,
       b02_entry_num,
       base_entry_num,
       B02_entry_amount,
       base_entry_amount,
       sdt
FROM
(
SELECT  'week' date_m,
       coalesce(dept_name,'合计')  dept_name,
       coalesce(region_code,'00')   region_code,
       coalesce(region_name,'') region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       coalesce(classify_large_code,'00') as classify_large_code,
       coalesce(classify_large_name,'') as classify_large_name,
       coalesce(a.classify_middle_code,'00') as classify_middle_code,
       coalesce(classify_middle_name,'') as classify_middle_name,
       b02_entry_num,
       base_entry_num,
       B02_entry_amount,
       base_entry_amount,
       week_of_year sdt
      
FROM  csx_tmp.temp_bash_w as a
union all 

SELECT  'months' date_m,
       coalesce(dept_name,'合计')  dept_name,
       coalesce(region_code,'00')   region_code,
       coalesce(region_name,'') region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       coalesce(classify_large_code,'00') as classify_large_code,
       coalesce(classify_large_name,'') as classify_large_name,
       coalesce(a.classify_middle_code,'00') as classify_middle_code,
       coalesce(classify_middle_name,'') as classify_middle_name,
       b02_entry_num,
       base_entry_num,
       B02_entry_amount,
       base_entry_amount,
       
        months sdt 
FROM  csx_tmp.temp_bash_m as a
union all 

SELECT  'quarter' date_m,
       coalesce(dept_name,'合计')  dept_name,
       coalesce(region_code,'00')   region_code,
       coalesce(region_name,'') region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       coalesce(classify_large_code,'00') as classify_large_code,
       coalesce(classify_large_name,'') as classify_large_name,
       coalesce(a.classify_middle_code,'00') as classify_middle_code,
       coalesce(classify_middle_name,'') as classify_middle_name,
       b02_entry_num,
       base_entry_num,
       B02_entry_amount,
       base_entry_amount,
 
        quarter sdt  
FROM  csx_tmp.temp_bash_q as a
union all
SELECT  'year' date_m,
       coalesce(dept_name,'合计')  dept_name,
       coalesce(region_code,'00')   region_code,
       coalesce(region_name,'') region_name,
       coalesce(province_code,'00') province_code,
       coalesce(province_name,'')   province_name,
       coalesce(city_code,'00')  as city_code,
       coalesce(city_name,'')  as city_name,
       coalesce(bd_id,'00')  as bd_id,
       coalesce(bd_name,'')  as bd_name,
       coalesce(classify_large_code,'00') as classify_large_code,
       coalesce(classify_large_name,'') as classify_large_name,
       coalesce(a.classify_middle_code,'00') as classify_middle_code,
       coalesce(classify_middle_name,'') as classify_middle_name,
       b02_entry_num,
       base_entry_num,
       B02_entry_amount,
       base_entry_amount,
 
        year sdt
FROM  csx_tmp.temp_bash_y as a



)a
;



DROP TABLE csx_tmp.temp_purchase_05 ;
create   table csx_tmp.temp_purchase_05 as
select  id rank_id,
        date_m,
        level_id,
        dept_name,
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
       net_entry_amount,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       net_entry_amount_ratio,
       yh_entry_amount_ratio,
       cash_entry_amount_ratio,
       all_num_ratio,
       cash_entry_num_ratio,
       yh_entry_num_ratio,
       b02_entry_num,
       base_entry_num,
       B02_entry_amount,
       base_entry_amount,
       base_entry_num_ratio,
       base_entry_amount_ratio,
       sdt
from (
select '1' id,
        date_m,
        level_id,
        dept_name,
        region_code,
        region_name,
        province_code,
        province_name,
        city_code,
        city_name,
        bd_id,
        bd_name,
       '' classify_large_code,
       '' classify_large_name,
       '' classify_middle_code,
       '' classify_middle_name,
       net_entry_amount,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       net_entry_amount_ratio,
       yh_entry_amount_ratio,
       cash_entry_amount_ratio,
       all_num_ratio,
       cash_entry_num_ratio,
       yh_entry_num_ratio,
       0 b02_entry_num,
       0 base_entry_num,
       0 B02_entry_amount,
       0 base_entry_amount,
       0 base_entry_num_ratio,
       0 base_entry_amount_ratio,
       sdt
from  csx_tmp.temp_purchase_m1 
union all 
select '2' id,
        date_m,
        level_id,
        dept_name,
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
      0  net_entry_amount,
      0 cash_entry_amount,  -- 现金采购
      0 yh_entry_amount,    -- 云超采购
      0 all_num,
      0 cash_entry_num,
      0 yh_entry_num,
      0 net_entry_amount_ratio,
      0 yh_entry_amount_ratio,
      0 cash_entry_amount_ratio,
      0 all_num_ratio,
      0 cash_entry_num_ratio,
      0 yh_entry_num_ratio,
       b02_entry_num,
       base_entry_num,
       B02_entry_amount,
       base_entry_amount,
       coalesce(base_entry_num/b02_entry_num,0)base_entry_num_ratio,
       coalesce(base_entry_amount/B02_entry_amount,0)base_entry_amount_ratio,
       sdt
from  csx_tmp.temp_bash_m1
) a
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



show create table  csx_tmp.temp_purchase_05;

CREATE TABLE `csx_tmp.report_r_d_purchase_entry_analysis`(
  `rank_id` string comment '层级1整体分析，2基地采购', 
  `level_id` string comment '层级 1 合计，2 平台、大区，3大区，4省区5城市',  
  `dept_name` string comment '运营部门平台、大区', 
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_code` string comment '城市编码', 
  `city_name` string comment '城市名称', 
  `bd_id` string comment '部类编码', 
  `bd_name` string comment '部类名称', 
  `classify_large_code` string comment '管理大类', 
  `classify_large_name` string comment '管理大类', 
  `classify_middle_code` string comment '管理中类', 
  `classify_middle_name` string comment '管理中类', 
  `net_entry_amount` decimal(38,6) comment '净入库额', 
  `cash_entry_amount` decimal(38,6) COMMENT '现金采买金额', 
  `yh_entry_amount` decimal(38,6) COMMENT '云超采购金额', 
  `all_supplier_num` bigint COMMENT '入库供应商数', 
  `cash_supplier_num` bigint COMMENT '现金采购供应商数', 
  `yh_supplier_num` bigint COMMENT '云超供应商数', 
  `net_entry_amount_ratio` decimal(38,6) COMMENT '净入库额占比', 
  `yh_entry_amount_ratio` decimal(38,6)  COMMENT '云超入库额占比', 
  `cash_entry_amount_ratio` decimal(38,6) COMMENT '现金采购占比', 
  `cash_supplier_ratio` decimal(38,6) COMMENT '现金采购供应商占比', 
  `yh_supplier_ratio` decimal(38,6) COMMENT '云超供应商占比', 
   all_supplier_ratio DECIMAL(38,6) COMMENT '供应商占比',
  `b02_supplier_num` bigint COMMENT '基地仓供应商数', 
  `base_supplier_num` bigint comment '基地订单供应商数', 
  `b02_entry_amount` decimal(38,6) comment '基地仓入库金额', 
  `base_entry_amount` decimal(38,6) comment '基地采购额', 
  `base_supplier_num_ratio` DECIMAL(38,6) comment '基地供应商占比', 
  `base_entry_amount_ratio` decimal(38,6) COMMENT '基地入库占比', 
  update_time TIMESTAMP COMMENT '数据更新日期'
  )comment'采购整体分析'
 partitioned by (sdt string comment '日期分区',date_m string comment '日期维度:周、月、季、年')
STORED AS parquet 
  
  
  
  ;