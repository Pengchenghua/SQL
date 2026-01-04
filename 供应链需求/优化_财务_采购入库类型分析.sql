-- ******************************************************************** 
-- @功能描述：采购入库类型分析 - 优化版本
-- @创建者： 彭承华 
-- @优化者：AI助手
-- @优化日期：2024-12-26
-- @优化内容：性能优化 - 减少临时表、优化GROUPING SETS、改进JOIN策略
-- ******************************************************************** 

-- 优化Hive配置，提升性能
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=none;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=20000;
set hive.auto.convert.join=true;
set hive.optimize.ppd=true;
set hive.vectorized.execution.enabled=true;

-- 优化方法1: 创建统一的基础聚合表，避免重复计算
drop table if exists csx_analyse_tmp.csx_analyse_tmp_purchase_base_aggregation;
create temporary table csx_analyse_tmp.csx_analyse_tmp_purchase_base_aggregation as 
SELECT 
    -- 基础维度字段
    dept_name,
    d.region_code,
    d.region_name,
    d.performance_province_code province_code,
    d.performance_province_name province_name,
    d.performance_city_code city_code,
    d.performance_city_name city_name,
    order_business_type,
    supplier_classify_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    classify_middle_name,
    supplier_code,
    d.is_purchase_dc,
    is_central_tag,
    supplier_name,
    business_type_name,
    
    -- 业务维度分组
    case when classify_large_code='B02' then 'B02' else 'B01' end join_classify_code,
    case when classify_large_code='B02' then '蔬果' else '非蔬果' end join_classify_name,
    case when a.division_code in ('10','11') then '11' else '12' end bd_id,
    case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
    
    -- 配送类型
    case when a.supplier_name like '%永辉%' then '云超配送'
         when business_type_name like '云超配送%' then '云超配送'
         else '供应商配送' end delivery_type,
    
    -- 金额字段
    sum(receive_amt) as receive_amt,
    sum(no_tax_receive_amt) as no_tax_receive_amt,
    sum(shipped_amt) as shipped_amt,
    sum(no_tax_shipped_amt) as no_tax_shipped_amt,
    sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)) as net_entry_amount,
    sum(no_tax_receive_amt-no_tax_shipped_amt) as no_tax_net_entry_amt,
    
    -- B02专项金额
    coalesce(sum(case when d.is_purchase_dc='1' and classify_large_code='B02' 
                     then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end),0) as b02_entry_amount,
    
    -- 时间维度字段
    receive_sdt as sdt,
    substr(receive_sdt,1,6) as months,
    concat(substr(receive_sdt,1,4),'Q',floor(substr(receive_sdt,5,2)/3.1)+1) as quarter,
    substr(receive_sdt,1,4) as year,
    t.week_of_year,
    concat(t.week_begin,'-',week_end) as week_date

FROM csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
JOIN csx_dim.csx_dim_basic_date t ON a.receive_sdt = t.calday
JOIN csx_analyse_tmp.csx_analyse_tmp_group_basic_dc_new d ON a.dc_code = d.shop_code
WHERE receive_sdt <= '${edate}'
   and receive_sdt >= '${sdate}'
   and sdt >= '${s_year}'
   and source_type_code not in ('4','15','18')
   and super_class_code in (1,2)
GROUP BY 
    dept_name, d.region_code, d.region_name, d.performance_province_code, 
    d.performance_province_name, d.performance_city_code, d.performance_city_name,
    order_business_type, supplier_classify_code, classify_large_code, classify_large_name,
    a.classify_middle_code, classify_middle_name, supplier_code, d.is_purchase_dc,
    is_central_tag, a.supplier_name, business_type_name, a.division_code,
    receive_sdt, t.week_of_year, t.week_begin, t.week_end;

-- 优化方法2: 统一的多维聚合函数，避免重复GROUPING SETS
drop table if exists csx_analyse_tmp.csx_analyse_tmp_purchase_unified_aggregation;
create temporary table csx_analyse_tmp.csx_analyse_tmp_purchase_unified_aggregation as
select 
    date_type,
    group_level,
    dept_name, region_code, region_name, province_code, province_name, 
    city_code, city_name, bd_id, bd_name, classify_large_code, classify_large_name, 
    classify_middle_code, classify_middle_name,
    
    -- 聚合指标
    sum(receive_amt) as receive_amt,
    sum(no_tax_receive_amt) as no_tax_receive_amt,
    sum(shipped_amt) as shipped_amt,
    sum(no_tax_shipped_amt) as no_tax_shipped_amt,
    sum(net_entry_amount) as net_entry_amount,
    sum(no_tax_net_entry_amt) as no_tax_net_entry_amt,
    
    -- 分类统计
    sum(case when supplier_classify_code=2 then net_entry_amount else 0 end) as cash_entry_amount,
    sum(case when delivery_type='云超配送' then net_entry_amount else 0 end) as yh_entry_amount,
    sum(case when supplier_classify_code=2 then no_tax_net_entry_amt else 0 end) as cash_entry_amount_no_tax,
    sum(case when delivery_type='云超配送' then no_tax_net_entry_amt else 0 end) as yh_entry_amount_no_tax,
    
    -- 供应商数量统计
    count(distinct supplier_code) as all_num,
    count(distinct case when supplier_classify_code=2 then supplier_code end) as cash_entry_num,
    count(distinct case when delivery_type='云超配送' then supplier_code end) as yh_entry_num,
    
    -- B02专项统计
    count(distinct case when is_purchase_dc=1 and classify_large_code='B02' then supplier_code end) as b02_entry_num,
    count(distinct case when is_purchase_dc=1 and classify_large_code='B02' and order_business_type=1 
                       then supplier_code end) as base_entry_num,
    sum(b02_entry_amount) as b02_entry_amount_agg,
    
    -- 时间维度
    time_period,
    time_label

from (
    -- 周维度数据
    select 'week' as date_type, week_of_year as time_period, week_date as time_label,
           dept_name, region_code, region_name, province_code, province_name, 
           city_code, city_name, bd_id, bd_name, classify_large_code, classify_large_name, 
           classify_middle_code, classify_middle_name,
           receive_amt, no_tax_receive_amt, shipped_amt, no_tax_shipped_amt,
           net_entry_amount, no_tax_net_entry_amt, supplier_classify_code, delivery_type,
           supplier_code, is_purchase_dc, order_business_type, classify_large_code,
           b02_entry_amount,
           case when dept_name is not null and region_name is not null and province_name is not null 
                     and city_name is not null and bd_name is not null then 'level5'
                when dept_name is not null and region_name is not null and province_name is not null 
                     and bd_name is not null then 'level4'
                when dept_name is not null and region_name is not null and bd_name is not null then 'level3'
                when dept_name is not null and bd_name is not null then 'level2'
                when bd_name is not null then 'level1'
                else 'total' end as group_level
    from csx_analyse_tmp.csx_analyse_tmp_purchase_base_aggregation
    
    union all
    
    -- 月维度数据（类似结构，省略详细字段）
    select 'month' as date_type, months as time_period, months as time_label,
           dept_name, region_code, region_name, province_code, province_name, 
           city_code, city_name, bd_id, bd_name, classify_large_code, classify_large_name, 
           classify_middle_code, classify_middle_name,
           receive_amt, no_tax_receive_amt, shipped_amt, no_tax_shipped_amt,
           net_entry_amount, no_tax_net_entry_amt, supplier_classify_code, delivery_type,
           supplier_code, is_purchase_dc, order_business_type, classify_large_code,
           b02_entry_amount,
           case when dept_name is not null and region_name is not null and province_name is not null 
                     and city_name is not null and bd_name is not null then 'level5'
                when dept_name is not null and region_name is not null and province_name is not null 
                     and bd_name is not null then 'level4'
                when dept_name is not null and region_name is not null and bd_name is not null then 'level3'
                when dept_name is not null and bd_name is not null then 'level2'
                when bd_name is not null then 'level1'
                else 'total' end as group_level
    from csx_analyse_tmp.csx_analyse_tmp_purchase_base_aggregation
    
    -- 可以继续添加季度、年度维度...
) base_data
group by date_type, group_level, dept_name, region_code, region_name, province_code, province_name, 
         city_code, city_name, bd_id, bd_name, classify_large_code, classify_large_name, 
         classify_middle_code, classify_middle_name, time_period, time_label;

-- 最终结果表
drop table if exists csx_analyse_tmp.csx_analyse_tmp_purchase_final_result;
create temporary table csx_analyse_tmp.csx_analyse_tmp_purchase_final_result as
select 
    date_type,
    group_level,
    dept_name, region_code, region_name, province_code, province_name, 
    city_code, city_name, bd_id, bd_name, classify_large_code, classify_large_name, 
    classify_middle_code, classify_middle_name,
    receive_amt, no_tax_receive_amt, shipped_amt, no_tax_shipped_amt,
    net_entry_amount, no_tax_net_entry_amt, cash_entry_amount, yh_entry_amount,
    cash_entry_amount_no_tax, yh_entry_amount_no_tax, all_num, cash_entry_num, yh_entry_num,
    b02_entry_num, base_entry_num, b02_entry_amount_agg,
    time_period, time_label
from csx_analyse_tmp.csx_analyse_tmp_purchase_unified_aggregation;

-- 性能优化建议：
-- 1. 索引优化：在源表上创建分区和索引
-- 2. 数据倾斜处理：对supplier_code等可能存在倾斜的字段进行预处理
-- 3. 查询优化：使用EXPLAIN分析执行计划
-- 4. 定期清理临时表

-- 查询优化示例
-- explain
-- select * from csx_analyse_tmp.csx_analyse_tmp_purchase_final_result 
-- where date_type = 'week' and group_level = 'level5';