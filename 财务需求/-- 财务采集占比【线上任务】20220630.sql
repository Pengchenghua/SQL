-- 财务采集占比【线上任务】20220630
set hive.execution.engine = tez;
set tez.queue.name = caishixian;
set purpose= ('01','02','03','05','07','08');
drop table  csx_tmp.temp_purchase_01 ;
create   table csx_tmp.temp_purchase_01 as 

SELECT sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       classify_middle_code,
       classify_middle_name,
       sum(net_amt) AS net_amt,
       sum(coalesce(case when order_business_type=1 then net_amt end     ,0 )  ) jd_amt,
       sum(coalesce(case when supplier_classify_code=2 then net_amt end  ,0  ) ) xj_amt,
       sum(coalesce(case when business_type_name='云超配送' then net_amt end,0)) yc_amt
FROM
(
SELECT d.sales_region_code,
       d.sales_region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       a.order_business_type,
       a.supplier_classify_code,
       a.classify_middle_code,
       classify_middle_name,
       case when a.supplier_name like '%永辉%' then '云超配送'
        when business_type_name like '云超配送%' then '云超配送'
           ELSE '供应商配送' end business_type_name,
       case when   a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when   a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       sum(receive_amt-shipped_amt) AS net_amt
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
 join 
(select sales_province_code,
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
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 ) d on a.dc_code=d.shop_id 
WHERE months='202205'
--  AND joint_purchase_flag=1    -- 集采供应商
  AND d.purpose IN ${hiveconf:purpose}
--   AND classify_middle_name IN ('蛋','米','水产','家禽','猪肉','食用油类','常温乳品饮料','调味品类','香烟饮料')
--  and a.classify_small_code !='B040207'
  and source_type_name not in  ('城市服务商','联营直送','项目合伙人')
  GROUP BY d.sales_region_code,
    d.sales_region_name,
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
    case when a.division_code in ('10','11') then '生鲜' else '食百' end

) a 
GROUP BY sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       a.classify_middle_code,
       classify_middle_name
;


select case when b.region_code!='10' then '大区'else '平台' end dept_name,
    b.region_code,b.region_name,
    b.province_code,
    b.province_name,
    case when b.province_name in ('福建','重庆','浙江') then a.city_code else '' end  city_code  ,
    case when b.province_name in ('福建','重庆','浙江') then a.city_name else '' end  city_name ,
    a.bd_id,
    a.bd_name,
    classify_middle_code,
    classify_middle_name,
    sum(a.net_amt)net_amt,
    sum(a.jd_amt)jd_amt,
    sum(a.xj_amt)xj_amt,
    sum(a.yc_amt)yc_amt
from  csx_tmp.temp_purchase_01 a 
left join 
(select a.code as province_code,a.name as province_name,b.code region_code,b.name region_name from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql a 
 left join 
(select code,name,parent_code from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql where level=1)  b on a.parent_code=b.code
 where level=2) b on a.province_code=b.province_code
 group by case when b.region_code!='10' then '大区'else '平台' end  ,
    b.region_code,
    b.region_name,
    b.province_code,
    b.province_name,
    case when b.province_name in ('福建','重庆','浙江') then a.city_code else '' end    ,
    case when b.province_name in ('福建','重庆','浙江') then a.city_name else '' end    ,
    a.bd_id,
    a.bd_name,
    classify_middle_code,
    classify_middle_name;