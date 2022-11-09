-- ******************************************************************** 
-- @功能描述：采购入库类型分析
-- @创建者： 彭承华 
-- @创建者日期：2022-10-31 20:13:10 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=none;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode =20000;

 -- 大区处理

drop table  csx_analyse_tmp.csx_analyse_tmp_group_basic_dc_new ;
create temporary TABLE   csx_analyse_tmp.csx_analyse_tmp_group_basic_dc_new as 
select case when belong_region_code!='10' then '大区'else '平台' end dept_name,
    purchase_org,
    purchase_org_name,
    belong_region_code  region_code,
    belong_region_name  region_name,
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
    enable_date,
    shop_low_profit_flag
from csx_dim.csx_dim_shop a 
 left join 
 (select distinct belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name
  from csx_dim.csx_dim_basic_performance_attribution) b on a.basic_performance_city_code= b.performance_city_code
 left join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.shop_code=c.dc_code
 where sdt='current'    
    ;
    

    
    

drop table  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01 ;
create temporary table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01 as 
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
       is_central_tag,
       case when classify_large_code='B02' then 'B02' else 'B01' end join_classify_code,
       case when classify_large_code='B02' then '蔬果' else '非蔬果' end join_classify_name,
       case when a.supplier_name like '%永辉%' then '云超配送'
            when business_type_name like '云超配送%' then '云超配送'
       else '供应商配送' end business_type_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt,       
       sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)) AS net_entry_amount,
       sum( no_tax_receive_amt-no_tax_shipped_amt) AS no_tax_net_entry_amt,
       coalesce(sum(case when d.is_purchase_dc='1' and classify_large_code ='B02' then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0) as B02_entry_amount,
       coalesce(sum(case when order_business_type=1 and classify_large_code ='B02' and d.is_purchase_dc='1' then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) base_entry_amount,
       receive_sdt sdt,
       substr(receive_sdt,1,6) months,
       concat(substr(receive_sdt,1,4),'Q',floor(substr(receive_sdt,5,2)/3.1)+1) quarter  ,
       substr(receive_sdt,1,4) year,
       t.week_of_year,
       concat(t.week_begin,'-',week_end) as week_date,
       enable_date
FROM   csx_analyse.csx_analyse_scm_purchase_order_flow_di  a 
join csx_dim.csx_dim_basic_date t on a.receive_sdt=t.calday
join csx_analyse_tmp.csx_analyse_tmp_group_basic_dc_new d on a.dc_code=d.shop_code
WHERE  receive_sdt <= '${edate}'
   and receive_sdt >= '${sdate}'
   and sdt >='${s_year}'
   and source_type_code not in ('4','15','18') -- 剔除 4项目合伙人、15联营直送、18城市服务商
   and super_class_code in (1,2)    -- 1供应商订单、2供应商退货单
  -- and d.purpose in ('01','02','03','05','07','08')
  -- and a.classify_large_code !='B02'
  -- and is_purchase_dc=1
  GROUP BY dept_name,
       is_central_tag,
       d.region_code,
       d.region_name,
       d.performance_province_code ,
       d.performance_province_name ,
       d.performance_city_code ,
       d.performance_city_name ,
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
       else '供应商配送' end ,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end ,
       receive_sdt  ,
       d.is_purchase_dc,
       d.purpose,
       enable_date,
        substr(receive_sdt,1,6) ,
       concat(substr(receive_sdt,1,4),'Q',floor(substr(receive_sdt,5,2)/3.1)+1)   ,
       substr(receive_sdt,1,4),
       t.week_of_year,
       concat(t.week_begin,'-',week_end) ,
        case when classify_large_code='B02' then 'B02' else 'B01' end ,
       case when classify_large_code='B02' then '蔬果' else '非蔬果' end 
;


-- 周整体入库
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_w;
create temporary table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_w as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt, 
       sum( net_entry_amount) net_entry_amount,
       sum(no_tax_net_entry_amt) as  no_tax_net_entry_amt,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       sum(coalesce(case when supplier_classify_code=2 then no_tax_net_entry_amt end  ,0  ) ) cash_entry_amount_no_tax,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then no_tax_net_entry_amt end,0) ) yh_entry_amount_no_tax,    -- 云超采购
       count( distinct supplier_code ) as all_num,
       count( distinct case when supplier_classify_code=2 then supplier_code end   )    as cash_entry_num,
       count( distinct case when business_type_name='云超配送' then supplier_code end ) as yh_entry_num,
       week_of_year
FROM
   csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01  a    
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
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_m;
create temporary table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_m as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt, 
       sum(net_entry_amount) net_entry_amount,
       sum(no_tax_net_entry_amt) as  no_tax_net_entry_amt,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       sum(coalesce(case when supplier_classify_code=2 then no_tax_net_entry_amt end  ,0  ) ) cash_entry_amount_no_tax,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then no_tax_net_entry_amt end,0) ) yh_entry_amount_no_tax,    -- 云超采购
       count( distinct supplier_code ) as all_num,
       count( distinct case when supplier_classify_code=2 then supplier_code end   )    as cash_entry_num,
       count( distinct case when business_type_name='云超配送' then supplier_code end ) as yh_entry_num,
       months
FROM
   csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01
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
drop table  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_q ;
create temporary table  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_q  as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt, 
       sum(net_entry_amount) net_entry_amount,
       sum(no_tax_net_entry_amt) as  no_tax_net_entry_amt,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       sum(coalesce(case when supplier_classify_code=2 then no_tax_net_entry_amt end  ,0  ) ) cash_entry_amount_no_tax,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then no_tax_net_entry_amt end,0) ) yh_entry_amount_no_tax,    -- 云超采购
       count( distinct supplier_code ) as all_num,
       count( distinct case when supplier_classify_code=2 then supplier_code end   )    as cash_entry_num,
       count( distinct case when business_type_name='云超配送' then supplier_code end ) as yh_entry_num,
       quarter
FROM
    csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01
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
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_y;
create temporary table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_y as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt, 
       sum(net_entry_amount) net_entry_amount,
       sum(no_tax_net_entry_amt) as  no_tax_net_entry_amt,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then net_entry_amount end,0) ) yh_entry_amount,    -- 云超采购
       sum(coalesce(case when supplier_classify_code=2 then no_tax_net_entry_amt end  ,0  ) ) cash_entry_amount_no_tax,  -- 现金采购
       sum(coalesce(case when business_type_name='云超配送' then no_tax_net_entry_amt end,0) ) yh_entry_amount_no_tax,    -- 云超采购
       count( distinct supplier_code ) as all_num,
       count( distinct case when supplier_classify_code=2 then supplier_code end   )    as cash_entry_num,
       count( distinct case when business_type_name='云超配送' then supplier_code end ) as yh_entry_num,
       year
FROM
    csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01
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
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_all;
create temporary  table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_all as       
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
        receive_amt,
       no_tax_receive_amt,
       shipped_amt,
       no_tax_shipped_amt, 
       net_entry_amount,
       no_tax_net_entry_amt,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       cash_entry_amount_no_tax,  -- 现金采购
       yh_entry_amount_no_tax,    -- 云超采购
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
       receive_amt,
       no_tax_receive_amt,
       shipped_amt,
       no_tax_shipped_amt, 
       net_entry_amount,
       no_tax_net_entry_amt,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       cash_entry_amount_no_tax,  -- 现金采购
       yh_entry_amount_no_tax,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       week_of_year sdt
FROM csx_analyse_tmp.csx_analyse_tmp_temp_purchase_w
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
        receive_amt,
       no_tax_receive_amt,
       shipped_amt,
       no_tax_shipped_amt, 
       net_entry_amount,
       no_tax_net_entry_amt,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       cash_entry_amount_no_tax,  -- 现金采购
       yh_entry_amount_no_tax,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       months sdt
FROM csx_analyse_tmp.csx_analyse_tmp_temp_purchase_m
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
        receive_amt,
       no_tax_receive_amt,
       shipped_amt,
       no_tax_shipped_amt, 
       net_entry_amount,
       no_tax_net_entry_amt,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       cash_entry_amount_no_tax,  -- 现金采购
       yh_entry_amount_no_tax,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       quarter sdt
FROM csx_analyse_tmp.csx_analyse_tmp_temp_purchase_q
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
        receive_amt,
       no_tax_receive_amt,
       shipped_amt,
       no_tax_shipped_amt, 
       net_entry_amount,
       no_tax_net_entry_amt,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       cash_entry_amount_no_tax,  -- 现金采购
       yh_entry_amount_no_tax,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       year sdt
FROM csx_analyse_tmp.csx_analyse_tmp_temp_purchase_y
) a ;


-- 周基地采购
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_w;
create temporary  table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_w as
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
       sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)) as B02_no_tax_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(receive_amt,0) end ),0 ) base_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(shipped_amt,0) end ),0 ) base_shipped_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_receive_amt,0) end ),0 ) base_no_tax_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_shipped_amt,0) end ),0 ) base_no_tax_shipped_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0) end ),0 ) base_no_tax_net_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) base_net_entry_amount,
       week_of_year
FROM
     csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01  a
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
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_m;
create  temporary table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_m as
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
       sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)) as B02_no_tax_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(receive_amt,0) end ),0 ) base_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(shipped_amt,0) end ),0 ) base_shipped_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_receive_amt,0) end ),0 ) base_no_tax_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_shipped_amt,0) end ),0 ) base_no_tax_shipped_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0) end ),0 ) base_no_tax_net_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) base_net_entry_amount,
       months
FROM
    csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01 a
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
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_q;
create temporary  table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_q as
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
       sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)) as B02_no_tax_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(receive_amt,0) end ),0 ) base_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(shipped_amt,0) end ),0 ) base_shipped_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_receive_amt,0) end ),0 ) base_no_tax_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_shipped_amt,0) end ),0 ) base_no_tax_shipped_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0) end ),0 ) base_no_tax_net_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) base_net_entry_amount,
       quarter
FROM
     csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01  a
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
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_y;
create temporary  table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_y as
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
       sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)) as B02_no_tax_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(receive_amt,0) end ),0 ) base_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(shipped_amt,0) end ),0 ) base_shipped_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_receive_amt,0) end ),0 ) base_no_tax_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_shipped_amt,0) end ),0 ) base_no_tax_shipped_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0) end ),0 ) base_no_tax_net_entry_amount,
       coalesce(sum(case when order_business_type=1  then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) base_net_entry_amount,
       year
FROM
     csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01  a
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

drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_all;
create temporary table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_all  as
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
       B02_no_tax_entry_amount,
       base_entry_amount,
       base_shipped_amount,
       base_no_tax_entry_amount,
       base_no_tax_shipped_amount,
       base_no_tax_net_entry_amount,
       base_net_entry_amount,
       
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
       B02_no_tax_entry_amount,
       base_entry_amount,
       base_shipped_amount,
       base_no_tax_entry_amount,
       base_no_tax_shipped_amount,
       base_no_tax_net_entry_amount,
       base_net_entry_amount,
       week_of_year sdt
      
FROM  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_w as a
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
       B02_no_tax_entry_amount,
       base_entry_amount,
       base_shipped_amount,
       base_no_tax_entry_amount,
       base_no_tax_shipped_amount,
       base_no_tax_net_entry_amount,
       base_net_entry_amount,
       
        months sdt 
FROM  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_m as a
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
       B02_no_tax_entry_amount,
       base_entry_amount,
       base_shipped_amount,
       base_no_tax_entry_amount,
       base_no_tax_shipped_amount,
       base_no_tax_net_entry_amount,
       base_net_entry_amount,
       quarter sdt  
FROM  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_q as a
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
       B02_no_tax_entry_amount,
       base_entry_amount,
       base_shipped_amount,
       base_no_tax_entry_amount,
       base_no_tax_shipped_amount,
       base_no_tax_net_entry_amount,
       base_net_entry_amount,
 
        year sdt
FROM  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_y as a



)a
;


-- 周度集采数据


-- 周度集采数据
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_w;
create temporary   table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_w as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       join_classify_code bd_id,
       join_classify_name bd_name,
       bd_id classify_large_code,
       bd_name classify_large_name,
       count( distinct  supplier_code  ) as join_dc_supplier_num,   --  总供应商数
       count( distinct case when is_central_tag=1 then supplier_code end ) as central_entry_num,    -- 集采供应商数
       coalesce(sum(coalesce(receive_amt,0)),0 )        join_dc_entry_amount,
       coalesce(sum(coalesce(shipped_amt,0)),0 )        join_dc_shipped_amount,
       coalesce(sum(coalesce(no_tax_receive_amt,0)),0 ) join_dc_no_tax_entry_amount,
       coalesce(sum(coalesce(no_tax_shipped_amt,0)),0 ) join_dc_no_tax_shipped_amt,
       coalesce(sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)),0 )        join_dc_net_entry_amount,
       coalesce(sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)),0 ) join_dc_no_tax_net_entry_amount,
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_receive_amt,0)  end ),0 ) central_no_tax_entry_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(receive_amt,0)  end ),0 ) central_entry_amount,                        -- 集采含税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_shipped_amt,0)  end ),0 ) central_no_tax_shipped_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(shipped_amt,0)  end ),0 ) central_shipped_amount,                        -- 集采含税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0) end ),0 ) central_no_tax_net_entry_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) central_net_entry_amount,                        -- 集采含税净入库额
       week_of_year 
FROM
     csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01  a
    where is_purchase_dc=1
  GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       join_classify_code  ,
       join_classify_name  ,
       bd_id,
       bd_name,
       week_of_year
 GROUPING SETS
    (
      (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
      join_classify_code  ,
      join_classify_name  ,
      week_of_year),
      (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
      bd_id  ,
      bd_name  ,
      week_of_year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name ,
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
       join_classify_code  ,
       join_classify_name ,
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
       bd_id,
       bd_name,
       week_of_year),
       (dept_name,
       region_code,
       region_name,
       join_classify_code  ,
       join_classify_name ,
       week_of_year),
       (dept_name,
       region_code,
       region_name, 
       week_of_year),
       (dept_name,
       bd_id,
       bd_name,
       week_of_year),
       (dept_name,
       join_classify_code ,
       join_classify_name ,
       week_of_year),
       (bd_id,
       bd_name,
       week_of_year),
       (join_classify_code  ,
       join_classify_name , 
       week_of_year),
       (dept_name,
       week_of_year),
       (week_of_year)
       )
       ;


-- 月度集采数据
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_m;
create  temporary table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_m as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       join_classify_code bd_id,
       join_classify_name bd_name,
       bd_id classify_large_code,
       bd_name classify_large_name,
       count( distinct  supplier_code  ) as join_dc_supplier_num,   --  总供应商数
       count( distinct case when is_central_tag=1 then supplier_code end ) as central_entry_num,    -- 集采供应商数
       coalesce(sum(coalesce(receive_amt,0)),0 )        join_dc_entry_amount,
       coalesce(sum(coalesce(shipped_amt,0)),0 )        join_dc_shipped_amount,
       coalesce(sum(coalesce(no_tax_receive_amt,0)),0 ) join_dc_no_tax_entry_amount,
       coalesce(sum(coalesce(no_tax_shipped_amt,0)),0 ) join_dc_no_tax_shipped_amt,
       coalesce(sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)),0 )        join_dc_net_entry_amount,
       coalesce(sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)),0 ) join_dc_no_tax_net_entry_amount,
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_receive_amt,0)  end ),0 ) central_no_tax_entry_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(receive_amt,0)  end ),0 ) central_entry_amount,                        -- 集采含税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_shipped_amt,0)  end ),0 ) central_no_tax_shipped_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(shipped_amt,0)  end ),0 ) central_shipped_amount,                        -- 集采含税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0) end ),0 ) central_no_tax_net_entry_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) central_net_entry_amount,                        -- 集采含税净入库额
       months 
FROM
     csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01  a
    where is_purchase_dc=1
 GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       join_classify_code  ,
       join_classify_name  ,
       months,
       bd_id,
       bd_name 
  GROUPING SETS
    (
      (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
      join_classify_code  ,
      join_classify_name  ,
      months),
      (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
      bd_id  ,
      bd_name  ,
      months),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name ,
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
       join_classify_code  ,
       join_classify_name ,
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
       bd_id,
       bd_name,
       months),
       (dept_name,
       region_code,
       region_name,
       join_classify_code  ,
       join_classify_name ,
       months),
       (dept_name,
       region_code,
       region_name, 
       months),
       (dept_name,
       bd_id,
       bd_name,
       months),
       (dept_name,
       join_classify_code ,
       join_classify_name ,
       months),
       (bd_id,
       bd_name,
       months),
       (join_classify_code  ,
       join_classify_name , 
       months),
       (dept_name,
       months),
       (months)
       )
       ;




-- 季度集采数据
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_q;
create temporary  table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_q as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       join_classify_code bd_id,
       join_classify_name bd_name,
       bd_id classify_large_code,
       bd_name classify_large_name,
       count( distinct  supplier_code  ) as join_dc_supplier_num,   --  总供应商数
       count( distinct case when is_central_tag=1 then supplier_code end ) as central_entry_num,    -- 集采供应商数
       coalesce(sum(coalesce(receive_amt,0)),0 )        join_dc_entry_amount,
       coalesce(sum(coalesce(shipped_amt,0)),0 )        join_dc_shipped_amount,
       coalesce(sum(coalesce(no_tax_receive_amt,0)),0 ) join_dc_no_tax_entry_amount,
       coalesce(sum(coalesce(no_tax_shipped_amt,0)),0 ) join_dc_no_tax_shipped_amt,
       coalesce(sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)),0 )        join_dc_net_entry_amount,
       coalesce(sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)),0 ) join_dc_no_tax_net_entry_amount,
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_receive_amt,0)  end ),0 ) central_no_tax_entry_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(receive_amt,0)  end ),0 ) central_entry_amount,                        -- 集采含税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_shipped_amt,0)  end ),0 ) central_no_tax_shipped_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(shipped_amt,0)  end ),0 ) central_shipped_amount,                        -- 集采含税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0) end ),0 ) central_no_tax_net_entry_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) central_net_entry_amount,                        -- 集采含税净入库额
       quarter 
FROM
     csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01  a
    where is_purchase_dc=1
  GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       join_classify_code  ,
       join_classify_name  ,
       bd_id,
       bd_name,
       quarter
 GROUPING SETS
    (
      (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
      join_classify_code  ,
      join_classify_name  ,
      quarter),
      (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
      bd_id  ,
      bd_name  ,
      quarter),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name ,
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
       join_classify_code  ,
       join_classify_name ,
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
       bd_id,
       bd_name,
       quarter),
       (dept_name,
       region_code,
       region_name,
       join_classify_code  ,
       join_classify_name ,
       quarter),
       (dept_name,
       region_code,
       region_name, 
       quarter),
       (dept_name,
       bd_id,
       bd_name,
       quarter),
       (dept_name,
       join_classify_code ,
       join_classify_name ,
       quarter),
       (bd_id,
       bd_name,
       quarter),
       (join_classify_code  ,
       join_classify_name , 
       quarter),
       (dept_name,
       quarter),
       (quarter)
       )
       ;

-- 年度集采数据
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_y;
create temporary  table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_y as
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       join_classify_code bd_id,
       join_classify_name bd_name,
       bd_id classify_large_code,
       bd_name classify_large_name,
       count( distinct  supplier_code  ) as join_dc_supplier_num,   --  总供应商数
       count( distinct case when is_central_tag=1 then supplier_code end ) as central_entry_num,    -- 集采供应商数
       coalesce(sum(coalesce(receive_amt,0)),0 )        join_dc_entry_amount,
       coalesce(sum(coalesce(shipped_amt,0)),0 )        join_dc_shipped_amount,
       coalesce(sum(coalesce(no_tax_receive_amt,0)),0 ) join_dc_no_tax_entry_amount,
       coalesce(sum(coalesce(no_tax_shipped_amt,0)),0 ) join_dc_no_tax_shipped_amt,
       coalesce(sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)),0 )        join_dc_net_entry_amount,
       coalesce(sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)),0 ) join_dc_no_tax_net_entry_amount,
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_receive_amt,0)  end ),0 ) central_no_tax_entry_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(receive_amt,0)  end ),0 ) central_entry_amount,                        -- 集采含税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_shipped_amt,0)  end ),0 ) central_no_tax_shipped_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(shipped_amt,0)  end ),0 ) central_shipped_amount,                        -- 集采含税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0) end ),0 ) central_no_tax_net_entry_amount,   -- 集采未税净入库额
       coalesce(sum(case when is_central_tag=1  then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) central_net_entry_amount,                        -- 集采含税净入库额
       year 
FROM
     csx_analyse_tmp.csx_analyse_tmp_temp_purchase_01  a
    where is_purchase_dc=1
 GROUP BY dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       join_classify_code  ,
       join_classify_name  ,
       bd_id,
       bd_name,
       year
 GROUPING SETS
    (
      (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
      join_classify_code  ,
      join_classify_name  ,
      year),
      (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
      bd_id  ,
      bd_name  ,
      year),
       (dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name ,
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
       join_classify_code  ,
       join_classify_name ,
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
       bd_id,
       bd_name,
       year),
       (dept_name,
       region_code,
       region_name,
       join_classify_code  ,
       join_classify_name ,
       year),
       (dept_name,
       region_code,
       region_name, 
       year),
       (dept_name,
       bd_id,
       bd_name,
       year),
       (dept_name,
       join_classify_code ,
       join_classify_name ,
       year),
       (bd_id,
       bd_name,
       year),
       (join_classify_code  ,
       join_classify_name , 
       year),
       (dept_name,
       year),
       (year)
       )
       ;

-- 集采汇总数据
drop table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_all;
create temporary  table csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_all  as
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
       coalesce(classify_large_code,'00')  as classify_large_code,
       coalesce(classify_large_name,'')  as classify_large_name,
        join_dc_supplier_num,   --  总供应商数
       central_entry_num,    -- 集采供应商数
       join_dc_entry_amount,
       join_dc_shipped_amount,
       join_dc_no_tax_entry_amount,
       join_dc_no_tax_shipped_amt,
       join_dc_net_entry_amount,
       join_dc_no_tax_net_entry_amount,
       central_no_tax_entry_amount,   -- 集采未税净入库额
       central_entry_amount,                        -- 集采含税净入库额
       central_no_tax_shipped_amount,   -- 集采未税净入库额
       central_shipped_amount,                        -- 集采含税净入库额
       central_no_tax_net_entry_amount,   -- 集采未税净入库额
       central_net_entry_amount,    
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
        coalesce(classify_large_code,'00')  as classify_large_code,
       coalesce(classify_large_name,'')  as classify_large_name,
       join_dc_supplier_num,   --  总供应商数
       central_entry_num,    -- 集采供应商数
       join_dc_entry_amount,
       join_dc_shipped_amount,
       join_dc_no_tax_entry_amount,
       join_dc_no_tax_shipped_amt,
       join_dc_net_entry_amount,
       join_dc_no_tax_net_entry_amount,
       central_no_tax_entry_amount,   -- 集采未税净入库额
       central_entry_amount,                        -- 集采含税净入库额
       central_no_tax_shipped_amount,   -- 集采未税净入库额
       central_shipped_amount,                        -- 集采含税净入库额
       central_no_tax_net_entry_amount,   -- 集采未税净入库额
       central_net_entry_amount,                        -- 集采含税净入库额
       week_of_year sdt
      
FROM  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_w as a
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
        coalesce(classify_large_code,'00')  as classify_large_code,
       coalesce(classify_large_name,'')  as classify_large_name,
       join_dc_supplier_num,   --  总供应商数
       central_entry_num,    -- 集采供应商数
       join_dc_entry_amount,
       join_dc_shipped_amount,
       join_dc_no_tax_entry_amount,
       join_dc_no_tax_shipped_amt,
       join_dc_net_entry_amount,
       join_dc_no_tax_net_entry_amount,
       central_no_tax_entry_amount,   -- 集采未税净入库额
       central_entry_amount,                        -- 集采含税净入库额
       central_no_tax_shipped_amount,   -- 集采未税净入库额
       central_shipped_amount,                        -- 集采含税净入库额
       central_no_tax_net_entry_amount,   -- 集采未税净入库额
       central_net_entry_amount,                        -- 集采含税净入库额
        
        months sdt 
FROM  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_m as a
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
        coalesce(classify_large_code,'00')  as classify_large_code,
       coalesce(classify_large_name,'')  as classify_large_name,
       join_dc_supplier_num,   --  总供应商数
       central_entry_num,    -- 集采供应商数
       join_dc_entry_amount,
       join_dc_shipped_amount,
       join_dc_no_tax_entry_amount,
       join_dc_no_tax_shipped_amt,
       join_dc_net_entry_amount,
       join_dc_no_tax_net_entry_amount,
       central_no_tax_entry_amount,   -- 集采未税净入库额
       central_entry_amount,                        -- 集采含税净入库额
       central_no_tax_shipped_amount,   -- 集采未税净入库额
       central_shipped_amount,                        -- 集采含税净入库额
       central_no_tax_net_entry_amount,   -- 集采未税净入库额
       central_net_entry_amount,                        -- 集采含税净入库额
       quarter sdt  
FROM  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_q as a
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
        coalesce(classify_large_code,'00')  as classify_large_code,
       coalesce(classify_large_name,'')  as classify_large_name,
       join_dc_supplier_num,   --  总供应商数
       central_entry_num,    -- 集采供应商数
       join_dc_entry_amount,
       join_dc_shipped_amount,
       join_dc_no_tax_entry_amount,
       join_dc_no_tax_shipped_amt,
       join_dc_net_entry_amount,
       join_dc_no_tax_net_entry_amount,
       central_no_tax_entry_amount,   -- 集采未税净入库额
       central_entry_amount,                        -- 集采含税净入库额
       central_no_tax_shipped_amount,   -- 集采未税净入库额
       central_shipped_amount,                        -- 集采含税净入库额
       central_no_tax_net_entry_amount,   -- 集采未税净入库额
       central_net_entry_amount,                        -- 集采含税净入库额
 
        year sdt
FROM  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_y as a



)a
;

-- DROP TABLE csx_tmp.temp_purchase_05 ;
 -- create   table csx_analyse_tmp.csx_analyse_tmp_temp_scm_purchase_analysis_di  as
 INSERT OVERWRITE table csx_analyse.csx_analyse_scm_purchase_type_analysis_di partition(year)
select  id rank_id,
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
        receive_amt,
       no_tax_receive_amt,
       shipped_amt,
       no_tax_shipped_amt, 
       net_entry_amount,
       no_tax_net_entry_amt,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       cash_entry_amount_no_tax,  -- 现金采购
       yh_entry_amount_no_tax,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
       central_entry_num,    -- 集采供应商数
       central_no_tax_entry_amount,   -- 集采未税净入库额
       central_entry_amount,                        -- 集采含税净入库额
       central_no_tax_shipped_amount,   -- 集采未税净入库额
       central_shipped_amount,                        -- 集采含税净入库额
       central_no_tax_net_entry_amount,   -- 集采未税净入库额
       central_net_entry_amount,       
       base_entry_num,
       base_entry_amount,
       base_shipped_amount,
       base_no_tax_entry_amount,
       base_no_tax_shipped_amount,
       base_no_tax_net_entry_amount,
       base_net_entry_amount,
        base_entry_num_ratio,
        base_entry_amount_ratio,
       current_timestamp(),
       
       date_m,
       sdt,
       substr(sdt,1,4) 
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
       receive_amt,
       no_tax_receive_amt,
       shipped_amt,
       no_tax_shipped_amt, 
       net_entry_amount,
       no_tax_net_entry_amt,
       cash_entry_amount,  -- 现金采购
       yh_entry_amount,    -- 云超采购
       cash_entry_amount_no_tax,  -- 现金采购
       yh_entry_amount_no_tax,    -- 云超采购
       all_num,
       cash_entry_num,
       yh_entry_num,
      0 central_entry_num,    -- 集采供应商数
      0 central_no_tax_entry_amount,   -- 集采未税净入库额
      0 central_entry_amount,                        -- 集采含税净入库额
      0 central_no_tax_shipped_amount,   -- 集采未税净入库额
      0 central_shipped_amount,                        -- 集采含税净入库额
      0 central_no_tax_net_entry_amount,   -- 集采未税净入库额
      0 central_net_entry_amount, 
      0 base_entry_num,
      0 base_entry_amount,
      0 base_shipped_amount,
      0 base_no_tax_entry_amount,
      0 base_no_tax_shipped_amount,
      0 base_no_tax_net_entry_amount,
      0 base_net_entry_amount,
      0 base_entry_num_ratio,
      0 base_entry_amount_ratio,
       sdt
from  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_all
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
       '' classify_middle_code,
       '' classify_middle_name,
       join_dc_entry_amount     as receive_amt,
       join_dc_no_tax_entry_amount as no_tax_receive_amt,
       join_dc_shipped_amount as shipped_amt,
       join_dc_no_tax_shipped_amt as no_tax_shipped_amt, 
       join_dc_net_entry_amount as net_entry_amount,
       join_dc_no_tax_net_entry_amount as  no_tax_net_entry_amt,
       0 cash_entry_amount,  -- 现金采购
       0 yh_entry_amount,    -- 云超采购
       0 cash_entry_amount_no_tax,  -- 现金采购
       0 yh_entry_amount_no_tax,    -- 云超采购
       join_dc_supplier_num as  all_num,     --  总供应商数
       0 cash_entry_num,
       0 yh_entry_num,
       central_entry_num,    -- 集采供应商数
       central_no_tax_entry_amount,   -- 集采未税净入库额
       central_entry_amount,                        -- 集采含税净入库额
       central_no_tax_shipped_amount,   -- 集采未税净入库额
       central_shipped_amount,                        -- 集采含税净入库额
       central_no_tax_net_entry_amount,   -- 集采未税净入库额
       central_net_entry_amount, 
      0 base_entry_num,
      0 base_entry_amount,
      0 base_shipped_amount,
      0 base_no_tax_entry_amount,
      0 base_no_tax_shipped_amount,
      0 base_no_tax_net_entry_amount,
      0 base_net_entry_amount,
      0 base_entry_num_ratio,
      0 base_entry_amount_ratio,
      sdt
from  csx_analyse_tmp.csx_analyse_tmp_temp_purchase_join_all 
union all 
select '3' id,
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
       0 receive_amt,
       0 no_tax_receive_amt,
       0 shipped_amt,
       0 no_tax_shipped_amt, 
       B02_entry_amount net_entry_amount,
       B02_no_tax_entry_amount no_tax_net_entry_amt,
       0 cash_entry_amount,  -- 现金采购
       0 yh_entry_amount,    -- 云超采购
       0 cash_entry_amount_no_tax,  -- 现金采购
       0 yh_entry_amount_no_tax,    -- 云超采购
       b02_entry_num all_num,
       0 cash_entry_num,
       0 yh_entry_num,
      0 central_entry_num,    -- 集采供应商数
      0 central_no_tax_entry_amount,   -- 集采未税净入库额
      0 central_entry_amount,                        -- 集采含税净入库额
      0 central_no_tax_shipped_amount,   -- 集采未税净入库额
      0 central_shipped_amount,                        -- 集采含税净入库额
      0 central_no_tax_net_entry_amount,   -- 集采未税净入库额
      0 central_net_entry_amount, 
       base_entry_num,
       base_entry_amount,
       base_shipped_amount,
       base_no_tax_entry_amount,
       base_no_tax_shipped_amount,
       base_no_tax_net_entry_amount,
       base_net_entry_amount,
       coalesce(base_entry_num/b02_entry_num,0)     base_entry_num_ratio,
       coalesce(base_no_tax_net_entry_amount/B02_no_tax_entry_amount,0)   base_no_tax_net_entry_amount_ratio,
       sdt
from csx_analyse_tmp.csx_analyse_tmp_temp_purchase_bash_all
) a
;
