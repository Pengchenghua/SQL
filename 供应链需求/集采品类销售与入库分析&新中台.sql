-- ******************************************************************** 
-- @功能描述： 集采品类销售与入库分析
-- @创建者： 彭承华 
-- @创建者日期：2022-10-17 17:08:25 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
-- 大区处理 增加低毛利DC 标识,关联供应链仓信息

drop table      csx_analyse_tmp.csx_analyse_tmp_group_basic_dc_new ;
create  temporary TABLE   csx_analyse_tmp.csx_analyse_tmp_group_basic_dc_new as 
select case when performance_region_code!='10' then '大区'else '平台' end dept_name,
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
    

drop table  csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_01;
create temporary table csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_01 as 
SELECT dept_name,
       region_code,
       region_name,
       coalesce(province_code,'')province_code,
       coalesce(province_name,'')province_name,
       coalesce(city_code,'')city_code,
       coalesce(city_name,'')city_name,
       bd_id,
       bd_name,
       central_purchase_short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       is_central_tag,      -- 集采标签
       sum(group_purchase_amt) group_purchase_amt,
       sum(no_tax_group_purchase_amt) as no_tax_group_purchase_amt,
       sum(net_entry_amt) net_entry_amt,  
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt,
       sum(no_tax_net_entry_amt) no_tax_net_entry_amt,
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
       a.central_purchase_short_name,
       a.classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       is_central_tag,
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt,
       coalesce(sum(case when is_central_tag=1 then no_tax_receive_amt-no_tax_shipped_amt end ),0) as no_tax_group_purchase_amt,
       coalesce(sum(case when is_central_tag=1 then receive_amt-shipped_amt end ),0) as group_purchase_amt,
       sum(receive_amt-shipped_amt) AS net_entry_amt,
       sum( no_tax_receive_amt-no_tax_shipped_amt) AS no_tax_net_entry_amt,
       receive_sdt sdt
FROM   csx_analyse.csx_analyse_scm_purchase_order_flow_di  a 
join csx_analyse_tmp.csx_analyse_tmp_group_basic_dc_new d on a.dc_code=d.shop_code
WHERE receive_sdt <= '${edate}'
   and receive_sdt >= '${sdate}'
   and sdt >='${s_year}'
   and source_type_code not in ('4','15','18') -- 剔除 4项目合伙人、15联营直送、18城市服务商
   and super_class_code in (1,2)    -- 1供应商订单、2供应商退货单
  -- and d.purpose in ('01','02','03','05','07','08')
  -- and a.classify_large_code !='B02'
   and is_purchase_dc=1
  GROUP BY d.region_code,
      d.region_name,
      d.performance_city_code,
      d.performance_city_name,
      d.performance_province_code,
       d.performance_province_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end ,
       a.central_purchase_short_name,
       d.dept_name,
       d.region_code,
       d.region_name,
       receive_sdt,
       a.classify_large_code,
       classify_large_name,
       is_central_tag
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
       central_purchase_short_name,
       classify_middle_code,
       classify_middle_name,
       sdt,
       is_central_tag,
       a.classify_large_code,
       classify_large_name
       ;




drop table csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_02 ;
create temporary table  csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_02 as 
SELECT dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       central_purchase_short_name,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       is_central_tag,
       sale_amt,
       profit,
       group_purchase_sale_amt,
       group_purchase_profit,
       sale_amt_no_tax,
       profit_no_tax,
       group_purchase_sale_amt_no_tax,
       group_purchase_profit_no_tax,
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
       b.central_purchase_short_name,
       a.classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when   b.is_flag='0' and a.sdt>= start_date and sdt>= enable_date and is_purchase_dc=1 then '1' else '0'  end is_central_tag,
       sum(a.sale_amt) AS sale_amt,
       sum(a.profit) profit,
       sum(sale_amt_no_tax) AS sale_amt_no_tax,
       sum(profit_no_tax)profit_no_tax,
       sum( case when  b.is_flag='0' and a.sdt>= start_date and sdt>= enable_date then sale_amt end ) group_purchase_sale_amt,
       sum( case when  b.is_flag='0' and a.sdt>= start_date and sdt>= enable_date then profit end ) group_purchase_profit,
       sum( case when  b.is_flag='0' and a.sdt>= start_date and sdt>= enable_date then sale_amt_no_tax end ) group_purchase_sale_amt_no_tax,
       sum( case when  b.is_flag='0' and a.sdt>= start_date and sdt>= enable_date then profit_no_tax end ) group_purchase_profit_no_tax,
       sdt 
FROM csx_dws.csx_dws_sale_detail_di a 
left join 
(select short_name as central_purchase_short_name,
        classify_small_code,
        start_date,
        end_date,
        is_flag
     from csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
    )  b on a.classify_small_code=b.classify_small_code
join 
  csx_analyse_tmp.csx_analyse_tmp_group_basic_dc_new d on a.inventory_dc_code=d.shop_code
 WHERE  sdt<= '${edate}'
   and sdt >= '${sdate}'
    and a.channel_code in ('1','7','9')
    and a.business_type_code='1'
    and is_purchase_dc=1
  --  and a.classify_large_code !='B02'
    and shop_low_profit_flag=0
GROUP BY dept_name,
       d.region_code,
       d.region_name,
       d.performance_city_code,
       d.performance_city_name,
       d.performance_province_code,
       d.performance_province_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end,
       b.central_purchase_short_name,
       sdt,
       case when   b.is_flag='0' and a.sdt>= start_date and sdt>= enable_date and is_purchase_dc=1 then '1' else '0'    end,
       a.classify_large_code,
       classify_large_name
) a 

;
 
  drop table  csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_03;
 CREATE temporary   table  csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_03 as 
  SELECT dept_name,
       region_code,
       region_name,
       coalesce(province_code,'')province_code,
       coalesce(province_name,'')province_name,
       coalesce(city_code,'')city_code,
       coalesce(city_name,'')city_name,
       bd_id,
       bd_name,
       central_purchase_short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       is_central_tag  ,      -- 集采标签
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt,
       sum(group_purchase_amt) as group_purchase_amt,
       sum(net_entry_amt) as net_entry_amt,
       sum(no_tax_group_purchase_amt) as no_tax_group_purchase_amt,
       sum(no_tax_net_entry_amt ) as no_tax_net_entry_amt,  
       sum(sale_amt) as sale_amt,
       sum(profit) as profit,
       sum(sale_amt_no_tax) as sale_amt_no_tax,
       sum(profit_no_tax) as profit_no_tax,
       sum(group_purchase_sale_amt) as group_purchase_sale_amt,
       sum(group_purchase_profit) as group_purchase_profit,
       sum(group_purchase_sale_amt_no_tax) as group_purchase_sale_amt_no_tax,
       sum(group_purchase_profit_no_tax) as group_purchase_profit_no_tax,
       sum(profit)/sum(sale_amt) as profit_rate,
       sum(group_purchase_profit)/sum(group_purchase_sale_amt) as group_purchase_profit_rate,
       sum(profit_no_tax)/sum(sale_amt_no_tax) as no_tax_profit_rate,
       sum(group_purchase_profit_no_tax)/sum(group_purchase_sale_amt_no_tax) as no_tax_group_purchase_profit_rate,
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
       central_purchase_short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       is_central_tag,      -- 集采标签
       receive_amt,
       no_tax_receive_amt,
       shipped_amt,
       no_tax_shipped_amt,
       group_purchase_amt,
       net_entry_amt,
       no_tax_group_purchase_amt,
       no_tax_net_entry_amt,  
       0 sale_amt,
       0 profit,
       0 sale_amt_no_tax,
       0 profit_no_tax,
       0 group_purchase_sale_amt,
       0 group_purchase_profit,
       0 group_purchase_sale_amt_no_tax,
       0 group_purchase_profit_no_tax,
       sdt
 FROM csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_01 a
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
       central_purchase_short_name,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       is_central_tag,
       0 as receive_amt,
       0 as no_tax_receive_amt,
       0 as shipped_amt,
       0 as no_tax_shipped_amt,
       0 as group_purchase_amt,
       0 as net_entry_amt,
       0 as no_tax_group_purchase_amt,
       0 as no_tax_net_entry_amt, 
       sale_amt,
       profit,
       sale_amt_no_tax,
       profit_no_tax,
       group_purchase_sale_amt,
       group_purchase_profit,
       group_purchase_sale_amt_no_tax,
       group_purchase_profit_no_tax,
       sdt
FROM csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_02 a
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
       central_purchase_short_name,
       is_central_tag,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       sdt
     ;  




insert overwrite table csx_analyse.csx_analyse_fr_fina_group_purchase_analysis_m_di partition(year)
select 
       substr(sdt,1,4) year,
       concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) quarter  ,
       substr(sdt,1,6) months,
       dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       coalesce(central_purchase_short_name,'') central_purchase_short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       is_central_tag  ,      -- 集采标签
       sum(receive_amt) as receive_amt,
       sum(no_tax_receive_amt) as no_tax_receive_amt,
       sum(shipped_amt) as shipped_amt,
       sum(no_tax_shipped_amt) as no_tax_shipped_amt,
       sum(net_entry_amt) as net_entry_amt,
       sum(no_tax_net_entry_amt ) as no_tax_net_entry_amt,  
       sum(group_purchase_amt) as group_purchase_amt,
       sum(no_tax_group_purchase_amt) as no_tax_group_purchase_amt,
       sum(sale_amt) as sale_amt,
       sum(sale_amt_no_tax) as sale_amt_no_tax,
       sum(profit) as profit,
       sum(profit_no_tax) as profit_no_tax,
       sum(profit)/sum(sale_amt) as profit_rate,
       sum(profit_no_tax)/sum(sale_amt_no_tax) as no_tax_profit_rate,
       sum(group_purchase_sale_amt) as group_purchase_sale_amt,
       sum(group_purchase_sale_amt_no_tax) as group_purchase_sale_amt_no_tax,
       sum(group_purchase_profit) as group_purchase_profit,
       sum(group_purchase_profit_no_tax) as group_purchase_profit_no_tax,
       sum(group_purchase_profit)/sum(group_purchase_sale_amt) as group_purchase_profit_rate,
       sum(group_purchase_profit_no_tax)/sum(group_purchase_sale_amt_no_tax) as no_tax_group_purchase_profit_rate,
       current_timestamp,
       substr(sdt,1,4) 
    from csx_analyse_tmp.csx_analyse_tmp_scm_join_entry_03 a
      --  where region_code='1'
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
       central_purchase_short_name,
       a.classify_large_code,
       a.classify_large_name,
       classify_middle_code,
       classify_middle_name,
       is_central_tag ,
       substr(sdt,1,6)

;