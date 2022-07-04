-- 财务供应链需求 
    
-- 整体采购数据分析
-- 集采仓
set  shop_id =('WA93','W0A2','W080','W048','WB04','W0A3','WB11','W0A8','WB03','W053','W0F4','W0K6','W0AH','W0F7',
'WA96','W0K1','W0AU','W0L3','W0BK','W0S9','W0Q2','W0Q9','W0Q8','W0AT','W0BH','W0BR','W0BT','W0R9','WB00','W0R8',
'W088','W0BZ','W0A5','W0P8','W0AS','W0AR','W079','W0A6','W0N0','WB01','W0P3','W0X1','W0X2','W0Z8','W0Z9','W0AZ',
'W039','W0A7','W0T7');

-- 剔除城市服务商、合伙人仓
set purpose= ('01','02','03','05','07','08');
SELECT sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
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
       case when a.supplier_name like '%永辉%' then '云超配送'
        when business_type_name like '云超配送%' then '云超配送'
           ELSE '供应商配送' end  business_type_name,
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
--  AND classify_middle_name IN ('蛋','米','水产','家禽','猪肉','食用油类','常温乳品饮料','调味品类','香烟饮料')
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
    case when a.supplier_name like '%永辉%' then '云超配送'
        when business_type_name like '云超配送%' then '云超配送'
           ELSE '供应商配送' end  ,
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
       bd_name
;


-- 基地采购 管理中类
SELECT sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       classify_middle_name,
       sum(net_amt) AS net_amt,
       sum(coalesce(case when order_business_type=1 then net_amt end,0 )  ) jd_amt
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
       classify_middle_name
;


-- 剔除城市服务商、合伙人仓 集采品类销售
set purpose= ('01','02','03','05','07','08');
SELECT sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
        a.classify_middle_code,
       a.classify_middle_name,
        sales_value,
      profit
FROM
(
SELECT d.sales_region_code,
       d.sales_region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       case when   a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when   a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       a.classify_middle_code,
       a.classify_middle_name,
       sum(a.sales_value) AS sales_value,
       sum(a.profit) profit
FROM csx_dw.dws_sale_r_d_detail a 
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
WHERE a.sdt >='20220501' and sdt<='20220531'
    and a.channel_code in ('1','7','9')
    and a.business_type_code='1'
--  AND joint_purchase_flag=1    -- 集采供应商
  and dc_code in ${hiveconf:shop_id}
 AND classify_middle_name IN ('蛋','米','水产','家禽','猪肉','食用油类','常温乳品饮料','调味品类','香烟饮料')
  and a.classify_small_code !='B040207'
  GROUP BY d.sales_region_code,
    d.sales_region_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
   a.classify_middle_code,
       a.classify_middle_name,
    case when a.division_code in ('10','11') then '11' else '12' end ,
    case when a.division_code in ('10','11') then '生鲜' else '食百' end

) a 

;


-- 日配销售额
SELECT sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
        a.classify_middle_code,
       a.classify_middle_name,
        sales_value,
      profit
FROM
(
SELECT d.sales_region_code,
       d.sales_region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       case when   a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when   a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       a.classify_middle_code,
       a.classify_middle_name,
       sum(a.sales_value) AS sales_value,
       sum(a.profit) profit
FROM csx_dw.dws_sale_r_d_detail a 
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
WHERE a.sdt >='20220501' and sdt<='20220531'
    and a.channel_code in ('1','7','9')
    and a.business_type_code='1'
--  AND joint_purchase_flag=1    -- 集采供应商
  AND d.purpose IN ('01','02','03','05','07','08')
-- AND classify_middle_name IN ('蛋','米','水产','家禽','猪肉','食用油类','常温乳品饮料','调味品类','香烟饮料')
  and a.classify_small_code !='B040207'
  GROUP BY d.sales_region_code,
    d.sales_region_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
   a.classify_middle_code,
       a.classify_middle_name,
    case when a.division_code in ('10','11') then '11' else '12' end ,
    case when a.division_code in ('10','11') then '生鲜' else '食百' end

) a 
;


-- 集采品类入库占比明细
SELECT coalesce(sales_region_code,'')sales_region_code,
       coalesce(sales_region_name,'')sales_region_name,
       coalesce(province_code,'')province_code,
       coalesce(province_name,'')province_name,
       coalesce(city_code,'')city_code,
       coalesce(city_name,'')city_name,
       division_code,
       division_name,
       classify_middle_code,
       classify_middle_name,
       sum(joint_amt)joint_amt,
       sum(net_amt)net_amt
    --   net_amt/sum(net_amt)over(PARTITION BY city_code,division_code) as sales_ratio
       
FROM 
(
SELECT d.sales_region_code,
       d.sales_region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       a.division_code,
       a.division_name,
       classify_middle_code,
       classify_middle_name,
       sum(if(joint_purchase_flag=1,receive_amt-shipped_amt,0)) as joint_amt,
       sum(receive_amt-shipped_amt) AS net_amt
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
 join 
(  select sales_province_code,
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
--  AND joint_purchase_flag=1 -- 集采供应商
   and source_type_name not in ('城市服务商','联营直送','项目合伙人')
   and super_class_name in ('供应商订单','供应商退货订单')
  AND d.purpose IN ('01','03')
   and dc_code in ${hiveconf:shop_id}
  AND ((classify_middle_name IN ('蛋','米','家禽','猪肉','食用油类','常温乳品饮料','调味品类','香烟饮料')
  and a.classify_small_code !='B040207')
    or a.classify_small_code in ( 'B030408','B030409','B030411','B030410','B030407') )
  GROUP BY d.sales_region_code,
      d.sales_region_name,
      performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
       classify_middle_code,
       classify_middle_name,
        a.division_code,
       a.division_name) a
GROUP BY sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       division_code,
       division_name,
       classify_middle_code,
       classify_middle_name

       ;
       
       select distinct classify_small_code , 
            classify_small_name, 
            classify_middle_name   
       from csx_dw.dws_basic_w_a_csx_product_m 
            where sdt='current' and classify_small_name='冷冻水产';
            
            

set purpose= ('01','02','03','05','07','08');

SELECT sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
        a.classify_middle_code,
       a.classify_middle_name,
        sales_value,
      profit
FROM
(
SELECT d.sales_region_code,
       d.sales_region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       case when   a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when   a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       a.classify_middle_code,
       a.classify_middle_name,
       sum(a.sales_value) AS sales_value,
       sum(a.profit) profit
FROM csx_dw.dws_sale_r_d_detail a 
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
WHERE a.sdt >='20220501' and sdt<='20220531'
    and a.channel_code in ('1','7','9')
    and a.business_type_code='1'
--  AND joint_purchase_flag=1    -- 集采供应商
--  AND d.purpose IN ('01','02','03','05','07','08')
  AND ((classify_middle_name IN ('蛋','米','家禽','猪肉','食用油类','常温乳品饮料','调味品类','香烟饮料')
  and a.classify_small_code !='B040207')
    or a.classify_small_code in ( 'B030408','B030409','B030411','B030410','B030407') )
  GROUP BY d.sales_region_code,
    d.sales_region_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
   a.classify_middle_code,
       a.classify_middle_name,
    case when a.division_code in ('10','11') then '11' else '12' end ,
    case when a.division_code in ('10','11') then '生鲜' else '食百' end

) a 

;

SELECT coalesce(sales_region_code,'')sales_region_code,
       coalesce(sales_region_name,'')sales_region_name,
       coalesce(province_code,'')province_code,
       coalesce(province_name,'')province_name,
       coalesce(city_code,'')city_code,
       coalesce(city_name,'')city_name,
       division_code,
       division_name,
       classify_middle_code,
       classify_middle_name,
        (net_amt)net_amt,
       net_amt/sum(net_amt)over(PARTITION BY city_code,division_code,province_code,sales_region_code) as sales_ratio
       
FROM 
(
SELECT coalesce(sales_region_code,'')sales_region_code,
       coalesce(sales_region_name,'')sales_region_name,
       coalesce(province_code,'')province_code,
       coalesce(province_name,'')province_name,
       coalesce(city_code,'')city_code,
       coalesce(city_name,'')city_name,
       division_code,
       division_name,
       classify_middle_code,
       classify_middle_name,
       sum(net_amt)net_amt
    --   net_amt/sum(net_amt)over(PARTITION BY city_code,division_code) as sales_ratio
       
FROM 
(
SELECT d.sales_region_code,
       d.sales_region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       a.division_code,
       a.division_name,
       classify_middle_code,
       classify_middle_name,
       sum(receive_amt-shipped_amt) AS net_amt
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
 join 
(  select sales_province_code,
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
   case 
        when performance_province_name in ('福建','浙江','重庆')   then performance_city_code else ''  end performance_city_code,
   case  when performance_province_name in ('福建','浙江','重庆')  then performance_city_name else '' end  performance_city_name,
    performance_province_code,
    performance_province_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 ) d on a.dc_code=d.shop_id 
WHERE months='202205'
  AND joint_purchase_flag=1 -- 集采供应商
   and source_type_name not in  ('城市服务商','联营直送','项目合伙人')
  AND d.purpose IN ('01','02','03','05','07','08')
  AND classify_middle_name IN ('蛋','米','水产','家禽','猪肉','食用油类','常温乳品饮料','调味品类','香烟饮料')
  and a.classify_small_code !='B040207'
  GROUP BY d.sales_region_code,
      d.sales_region_name,
      performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
       classify_middle_code,
       classify_middle_name,
        a.division_code,
       a.division_name
) a
GROUP BY sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       division_code,
       division_name,
       classify_middle_code,
       classify_middle_name
GROUPING SETS
((sales_region_code,
       sales_region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       division_code,
       division_name,
       classify_middle_code,
       classify_middle_name),
       (sales_region_code,、
       sales_region_name,
       province_code,
       province_name,
       division_code,
       division_name,
       classify_middle_code,
       classify_middle_name),
       (sales_region_code,
       sales_region_name,
       division_code,
       division_name,
       classify_middle_code,
       classify_middle_name)
       ,()
       )
       )a ;


-- 指定仓整体入库情况
set  shop_id =('WA93','W0A2','W080','W048','WB04','W0A3','WB11','W0A8','WB03','W053','W0F4','W0K6','W0AH','W0F7','WA96','W0K1','W0AU','W0L3','W0BK','W0S9','W0Q2','W0Q9','W0Q8','W0AT','W0BH','W0BR','W0BT','W0R9','WB00','W0R8','W088','W0BZ','W0A5','W0P8','W0AS','W0AR','W079','W0A6','W0N0','WB01','W0P3','W0X1','W0X2','W0Z8','W0Z9','W0AZ','W039','W0A7','W0T7');

-- 剔除城市服务商、合伙人仓
set purpose= ('01','02','03','05','07','08');
SELECT sales_region_code,
       sales_region_name,
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
       classify_small_code ,
       classify_small_name ,
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
       case when a.supplier_name like '%永辉%' then '云超配送'
        when business_type_name like '云超配送%' then '云超配送'
           ELSE '供应商配送' end  business_type_name,
       case when   a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when   a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code ,
       classify_small_name ,
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
--  AND d.purpose IN ${hiveconf:purpose}
--  AND classify_middle_name IN ('蛋','米','水产','家禽','猪肉','食用油类','常温乳品饮料','调味品类','香烟饮料')
--  and a.classify_small_code !='B040207'
   and source_type_name not in  ('城市服务商','联营直送','项目合伙人')
   and super_class_name in ('供应商订单','供应商退货订单')
   and dc_code in ${hiveconf:shop_id}
  GROUP BY d.sales_region_code,
    d.sales_region_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
    a.order_business_type,
    a.supplier_classify_code,
    case when a.supplier_name like '%永辉%' then '云超配送'
        when business_type_name like '云超配送%' then '云超配送'
           ELSE '供应商配送' end  ,
    case when a.division_code in ('10','11') then '11' else '12' end ,
    case when a.division_code in ('10','11') then '生鲜' else '食百' end,
    classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code ,
       classify_small_name 

) a 
GROUP BY sales_region_code,
       sales_region_name,
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
       classify_small_code ,
       classify_small_name 
;