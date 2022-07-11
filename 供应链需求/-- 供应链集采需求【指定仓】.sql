-- 供应链集采需求【指定仓】
set shop=('W0A3','W0Q9','W0P8','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0R9','W0A5','W0N0','W0AS','W0A8','W0F4','W0L3','W0K1','WB11','W0G9','WA96','W0AU','W0K6','W0F7','W0BK','W0A2','W0BR','W0BH','W048','W0Q8','W039','W0X1','W0Z8','W079','W0S9','W0R8','W088','W0P3','W0AR','W053','W080','W0BT','WB04','W0AZ','WB00','W0BZ','WB01','WB03','W0AT','W0T7','WA93');
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
       a.classify_small_code,
       a.classify_small_name,
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
       a.classify_small_code,
       a.classify_small_name,
       case when  a.classify_small_code IS NOT NULL and short_name is not NULL then '1' end group_purchase_tag,
       coalesce(sum(case when joint_purchase_flag=1 and a.classify_small_code IS NOT NULL then receive_amt-shipped_amt end ),0) as group_purchase_amount,
       sum(receive_amt-shipped_amt) AS net_entry_amount,
       months
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
left join  csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
 join 
  csx_tmp.temp_dc_new d on a.dc_code=d.shop_id 
WHERE months >= '202201'
   and a.dc_code in ${hiveconf:shop}
   and source_type_name not in ('城市服务商','联营直送','项目合伙人')
   and super_class_name in ('供应商订单','供应商退货订单')
 -- AND d.purpose IN ('01','03')
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
       a.classify_small_code,
       a.classify_small_name,
        case when  a.classify_small_code IS NOT NULL and short_name is not NULL then '1' end 
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
       a.classify_small_code,
       a.classify_small_name,
       classify_large_name

       ;