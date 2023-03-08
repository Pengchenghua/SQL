-- 供应商集中度 谢艳艳
drop table csx_analyse_tmp.csx_analyse_tmp_dc_new ;
create  TABLE   csx_analyse_tmp.csx_analyse_tmp_dc_new as 
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
 where sdt='20230101'
    and purpose in   ('01','02','03','05','07','08')    
    ;
    
    
   -- select distinct purpose,purpose_name from csx_analyse_tmp.csx_analyse_tmp_dc_new 

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
        business_type_name as order_business_type_name,
       case when a.supplier_name like '%永辉%' then '云超配送'
            when business_type_name like '云超配送%' then '云超配送'
       else '供应商配送' end business_type_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)) AS net_entry_amount,
       sum(coalesce(no_tax_receive_amt,0)-coalesce(no_tax_shipped_amt,0)) AS no_tax_net_entry_amount,
       sum(coalesce(no_tax_receive_amt,0)) no_tax_receive_amt,
       sum(coalesce(no_tax_shipped_amt,0)) no_tax_shipped_amt,
       d.purpose,
       sdt,
       substr(sdt,1,6) months,
      
       substr(sdt,1,4) year,
       enable_date
FROM    csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
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
 join csx_analyse_tmp.csx_analyse_tmp_dc_new  d on a.dc_code=d.shop_code 
WHERE
    sdt <= '20221231'
    and sdt>=  '20210101'
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
       business_type_name,
       sdt,
       d.is_purchase_dc,
       d.purpose,
       enable_date,
        substr(sdt,1,6) ,
              channel,
       substr(sdt,1,4),
       b.source_type_name,
       source_channel 

;

 


-- 计算各维度的金额与分组
drop table csx_analyse_tmp.csx_analyse_tmp_purchase_03;
create table csx_analyse_tmp.csx_analyse_tmp_purchase_03 as 
select year,
    bd_name,
    dept_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_code,
    no_tax_net_entry_amount,
    case when no_tax_net_entry_amount < 10 then '0~10万'
         when no_tax_net_entry_amount >= 10 and  no_tax_net_entry_amount < 100 then '10~100万'
         when no_tax_net_entry_amount >= 100 then   '100万以上'
         end note
from (
select year,bd_name,
    dept_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_code,
    round(sum(no_tax_net_entry_amount)/10000,2) no_tax_net_entry_amount
from csx_analyse_tmp.csx_analyse_tmp_purchase_01
where business_type_name='供应商配送'
group by year,
    bd_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_code,
    dept_name
  grouping sets (
    (year,
    bd_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_code,
    dept_name),
    (year,
    bd_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_code,
    dept_name) ,     -- 城市总计
    (year,
    bd_name,
    region_code,
    region_name,
    province_code,
    province_name,
    supplier_code,
    dept_name),       -- 省区
    (year,
     region_code,
    region_name,
    province_code,
    province_name,
    supplier_code,
    dept_name),       -- 省区总计
    (year,
    bd_name,
    region_code,
    region_name, 
    supplier_code,
    dept_name),       -- 大区
    (year,
    region_code,
    region_name, 
    supplier_code,
    dept_name),       -- 大区总计
    (year,
    bd_name,
    supplier_code,
    dept_name),       -- 平台部类
    (year,
     supplier_code,
    dept_name),       -- 平台部类总计
    (year,
    bd_name,
    supplier_code ),
    (year,
     supplier_code )      -- 全国总计


  )
    )a 
;

-- 结果
  select 
    year,
    dept_name,
    bd_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    note,
    no_tax_net_entry_amount,
    cn
from 
(select
   year,
  coalesce(dept_name,'全国') dept_name,
  coalesce(bd_name, '合计') bd_name,
  coalesce(region_code, '00') region_code,
  coalesce(region_name, '')region_name,
  coalesce(province_code,'00') province_code,
  coalesce(province_name,'') province_name,
  coalesce(city_code,'00') city_code,
  coalesce(city_name,'') city_name,
  coalesce(note,'')note,
  sum(no_tax_net_entry_amount) no_tax_net_entry_amount,
  count(distinct supplier_code) cn
from
  csx_analyse_tmp.csx_analyse_tmp_purchase_03
where 1=1
    and year = '2022'
  --  and province_name='四川'
group by
  year,
  dept_name,
  bd_name,
  region_code,
  region_name,
  province_code,
  province_name,
  city_code,
  city_name,
  note 
  union all 
  select
   year,
  coalesce(dept_name,'全国') dept_name,
  coalesce(bd_name, '合计') bd_name,
  coalesce(region_code, '00') region_code,
  coalesce(region_name, '')region_name,
  coalesce(province_code,'00') province_code,
  coalesce(province_name,'') province_name,
  coalesce(city_code,'00') city_code,
  coalesce(city_name,'') city_name,
  '' note,
  sum(no_tax_net_entry_amount) no_tax_net_entry_amount,
  count(distinct supplier_code) cn
from
  csx_analyse_tmp.csx_analyse_tmp_purchase_03
where 1=1
    and year = '2022'
  --  and province_name='四川'
group by
  year,
  dept_name,
  bd_name,
  region_code,
  region_name,
  province_code,
  province_name,
  city_code,
  city_name
  )a
 order by 
   case when note='0~10万' then 1 
    when note ='0~100万' then 2 
    when note='100万以上' then 3 
    else '4' end  asc,
case
        when a.region_code = '1' then  '7'
        when a.region_code = '10' then  '98'
        else a.region_code
    end,
case
        when a.province_name in('重庆', '上海宝山', '上海松江', '安徽', '北京') then '1'
        when a.province_name in('四川', '江苏苏州', '河南', '河北') then '2'
        when a.province_name in('江苏南京') then '3'
        else '4'
    end,
 case when a.city_name in ('福州','重庆区','杭州','上海宝山') then '1'  
when a.city_name in ('厦门','黔江区','宁波','上海松江') then '2'  
when a.city_name in ('泉州','舟山','江苏苏州') then '3' 
when a.city_name in ('莆田','台州','江苏南京') then '4' 
when a.city_name in ('南平') then '5' 
when a.city_name in ('三明') then '6' 
when a.city_name in ('宁德') then '7' 
when a.city_name in ('龙岩') then '8' 
else '9' end,
    a.bd_name asc 

    ;
  
 
select
  year,
  coalesce(dept_name,'全国') dept_name,
  coalesce(bd_name, '合计') bd_name,
  coalesce(region_code, '00') region_code,
  coalesce(region_name, '')region_name,
  coalesce(province_code,'00') province_code,
  coalesce(province_name,'') province_name,
  coalesce(city_code,'00') city_code,
  coalesce(city_name,'') city_name,
  coalesce(note,'')note,
  no_tax_net_entry_amount,
  cn
from
  csx_analyse_tmp.csx_analyse_tmp_purchase_04 a
 where year='2022'
order by 
 
   case when note='0~10万' then 1 
    when note ='0~100万' then 2 
    when note='100万以上' then 3 
    else '4' end  asc,
case
        when a.region_code = '1' then  '7'
        when a.region_code = '10' then  '98'
        else a.region_code
    end,
case
        when a.province_name in('重庆', '上海宝山', '上海松江', '安徽', '北京') then '1'
        when a.province_name in('四川', '江苏苏州', '河南', '河北') then '2'
        when a.province_name in('江苏南京') then '3'
        else '4'
    end,
 case when a.city_name in ('福州','重庆区','杭州','上海宝山') then '1'  
when a.city_name in ('厦门','黔江区','宁波','上海松江') then '2'  
when a.city_name in ('泉州','舟山','江苏苏州') then '3' 
when a.city_name in ('莆田','台州','江苏南京') then '4' 
when a.city_name in ('南平') then '5' 
when a.city_name in ('三明') then '6' 
when a.city_name in ('宁德') then '7' 
when a.city_name in ('龙岩') then '8' 
else '9' end,
    a.bd_name asc 

    
-- 月度

drop table csx_analyse_tmp.csx_analyse_tmp_purchase_m;
create table csx_analyse_tmp.csx_analyse_tmp_purchase_m as 
select year,
    dept_name,
    months,
    bd_name,
    supplier_code,
    no_tax_net_entry_amount,
    case when no_tax_net_entry_amount < 10 then '0~10万'
         when no_tax_net_entry_amount >= 10 and  no_tax_net_entry_amount < 100 then '10~100万'
         when no_tax_net_entry_amount >= 100 then   '100万以上'
         end note
from (select year,
    dept_name,
    months,
    bd_name,
    supplier_code,
    round(sum(no_tax_net_entry_amount)/10000,2) no_tax_net_entry_amount
from csx_analyse_tmp.csx_analyse_tmp_purchase_01
where business_type_name='供应商配送'
group by  year,
    dept_name,
    months,
    bd_name,
    supplier_code
grouping sets (
  (year,
  dept_name,
    months,
    bd_name,
    supplier_code),
    (year,
    dept_name,
    months,
    supplier_code)
) )a 
;





select year,
    months,
    coalesce(bd_name,'合计')bd_name,
    coalesce(note,'总计')note,
    no_tax_net_entry_amount,
    cn
from 
(select year,
    months,
    coalesce(bd_name,'合计')bd_name,
    coalesce(note,'总计')note,
    sum(no_tax_net_entry_amount) no_tax_net_entry_amount,
    count(distinct supplier_code) cn
from csx_analyse_tmp.csx_analyse_tmp_purchase_m
    where 1=1
    -- year='2022'
    and dept_name='大区'
group by year,
    months,
    bd_name,
    note
    union all 
select year,
    months,
    coalesce(bd_name,'合计')bd_name,
    '' note,
    sum(no_tax_net_entry_amount) no_tax_net_entry_amount,
    count(distinct supplier_code) cn
from csx_analyse_tmp.csx_analyse_tmp_purchase_m
    where 1=1
    -- year='2022'
    and dept_name='大区'
group by year,
    months,
    bd_name 
    
)a
order by 
case when note='0~10万' then 1 
    when note ='0~100万' then 2 
    when note='100万以上' then 3 
    else '4' end  asc,
    year,months,
    bd_name
    ;
    
    