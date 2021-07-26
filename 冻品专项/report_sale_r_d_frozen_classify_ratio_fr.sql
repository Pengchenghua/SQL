set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
--上月结束日期，当前日期不等于月末取当前日期，等于月末取上月最后一天
set last_edt=regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','');

set s_dt_30 =regexp_replace(date_sub(${hiveconf:edt},30),'-','');
-- set parquet.compression=snappy;
-- set hive.exec.dynamic.partition=true; 
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt},regexp_replace(date_sub(${hiveconf:edt},30),'-','') ;


-- 本期数据 (不含合伙人 purpose!='06')

drop table if exists csx_tmp.tmp_dp_sale;
create temporary table csx_tmp.tmp_dp_sale
as 
select 
    note,
    channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.customer_no,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.business_type_code,
    sum(sales_qty) as  sales_qty,
    sum(sales_value ) as  sales_value,
    sum(profit)as  profit,
    sum(last_sales_qty) as last_sales_qty,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit
   from ( 
    select  
    '1' as note,  --本期数据
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.customer_no,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.business_type_code,
    sales_qty,
    a.sales_value,
    a.profit,
    0 last_sales_qty,
    0 last_sales_value,
    0 last_profit
    from csx_dw.dws_sale_r_d_detail a 
    where sdt<=${hiveconf:e_dt} and sdt>=${hiveconf:s_dt} 
    and a.channel_code  in ('1','7','9')
    union all 
     select 
    '2' as note,  --环期数据 
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.customer_no,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.business_type_code,
    0 as sales_qty,
    0 as sales_value,
    0 as profit,
    sales_qty as  last_sales_qty,
    sales_value as  last_sales_value,
    profit as  last_profit
    from csx_dw.dws_sale_r_d_detail a 
    where sdt<=${hiveconf:last_edt} and sdt>=${hiveconf:last_sdt} 
    and a.channel_code  in ('1','7','9')
)    a 
where business_type_code !='4'
group by 
    note,
    channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.province_code,
    a.province_name,
    a.customer_no,
    business_type_code
;
    
-- 求classify_middle_code in('B0304','B0305') 聚合
drop table if exists csx_tmp.tmp_dp_sale_01;
create temporary table csx_tmp.tmp_dp_sale_01 as 
select
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_value) as  frozen_sales,
    sum(profit) as  frozen_profit,
    sum(case when business_type_code='1' then  sales_value end) frozen_daily_sales,
    sum(case when business_type_code='1' then  profit end) frozen_daily_profit,
    sum(  last_sales_value  ) as last_frozen_sales,
    sum(  last_profit  ) as last_frozen_profit,
    sum(case when business_type_code='1'   then last_sales_value end)  last_frozen_daily_sales,
    sum(case when business_type_code='1'   then last_profit end ) last_frozen_daily_profit,
    count(distinct case when note='1' and  business_type_code='1' then customer_no end ) as daily_cust_number, --日配成交客户数
    count(distinct case when note='2' and  business_type_code='1' then customer_no end )as  last_daily_cust_number,  --环比日配冻品成交客户数
    grouping__id 
from csx_tmp.tmp_dp_sale a
where 1=1
    and classify_middle_code in('B0304','B0305')
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
grouping sets
    ((channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name),     --明细
        (channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name),   --城市中类合计
        (channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name),      --城市组合计明细
        (channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name),     --省区明细
        (channel_name,
        region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name),   --省区中类汇总
        (channel_name,
        region_code,
        region_name,
        province_code,
        province_name),   -- 31 省区汇总
        (channel_name,
        region_code,
        region_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name),  --32743 大区小类
        (channel_name,
        region_code,
        region_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name),  --32263 大区中类汇总
        (channel_name,
        region_code,
        region_name),  --7 大区汇总
        (channel_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name), -- 32737 全国小类 汇总
        (channel_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name), -- 8161 全国中类 汇总
        ()   --0 
    )
;
  
-- 日配总客户数
drop table if exists  csx_tmp.tmp_dp_sale_02;
create  temporary table csx_tmp.tmp_dp_sale_02 as 
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    count(distinct case when  note='1'  and business_type_code='1' then customer_no end) as b_daily_cust_number,    --B端日配客户数
    count(distinct case when  note='2'  and business_type_code='1' then customer_no end ) as last_b_daily_cust_number,
    -- sum(frozen_sales_qty) as frozen_sales_qty,
    -- sum(frozen_sales) as frozen_sales,
    -- sum(frozen_profit) as frozen_profit,
    sum(b_sales_value) as  b_sales_value,
    sum(b_profit) as b_profit,
    sum(last_b_sales_value) as last_b_sales_value,
    sum(last_b_profit) as last_b_profit,
    sum(case when business_type_code='1' then b_sales_value end ) as B_daily_sale ,
    sum(case when business_type_code='1' then b_profit end ) as B_daily_profit ,
    sum(case when business_type_code='1' then last_b_sales_value end ) as last_B_daily_sale ,
    sum(case when business_type_code='1' then last_b_profit end ) as last_B_daily_profit ,
    grouping__id
from
(
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    business_type_code,
    note,
    -- sum(case when classify_middle_code in('B0304','B0305') then sales_qty end) as frozen_sales_qty,
    -- sum(case when classify_middle_code in('B0304','B0305') then sales_value end) as frozen_sales,
    -- sum(case when classify_middle_code in('B0304','B0305') then profit end) as frozen_profit,
    sum(sales_value) as b_sales_value,
    sum(profit) as b_profit,
    sum(last_sales_value) as last_b_sales_value,
    sum(last_profit) as last_b_profit
from csx_tmp.tmp_dp_sale
where 1=1
 --   and classify_middle_code in('B0304','B0305')
group by 
    note,
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    business_type_code
) a
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name
grouping sets
    ((channel_name,
     region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name),
    (channel_name,
     region_code,
    region_name,
    province_code,
    province_name),
    (channel_name,
    region_code,
    region_name),
    ())
;

drop table if exists csx_tmp.tmp_dp_sale_03;
create temporary table csx_tmp.tmp_dp_sale_03 as 
select  
    a.channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    frozen_sales,
    frozen_profit,
    frozen_daily_sales,
    frozen_daily_profit,
    last_frozen_sales,
    last_frozen_profit,
    last_frozen_daily_sales,
    last_frozen_daily_profit,
    daily_cust_number, --日配成交客户数
    last_daily_cust_number,  --环比日配冻品成交客户数
    b_daily_cust_number,
    last_b_daily_cust_number,
    b_sales_value,
    b_profit,
    B_daily_sale,
    last_B_daily_sale,
    a.grouping__id 
from csx_tmp.tmp_dp_sale_01 a
left join csx_tmp.tmp_dp_sale_02  b on coalesce(a.city_group_code,'')=coalesce(b.city_group_code,'') and coalesce(a.province_code,'')=coalesce(b.province_code,'')
    and coalesce(a.region_code,'')=coalesce(b.region_code,'')


;

select * from  csx_tmp.temp_sale_30day;
-- 城市省区销售额  计算B端销售占比 30天销售额
drop table if exists csx_tmp.temp_sale_30day;
create temporary table csx_tmp.temp_sale_30day as 
select 
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(a.sales_qty ) as frozen_sales_qty_30day,
    sum(a.sales_value) as frozen_sales_30day,
    sum(a.profit) as frozen_profit_day,
    grouping__id
from csx_dw.dws_sale_r_d_detail a 
where sdt>${hiveconf:s_dt_30} 
    and sdt<=${hiveconf:e_dt}
   -- and a.business_type_code !='4'  --剔除城市服务商
    and a.channel_code  in ('1','7','9')
    and classify_middle_code  in('B0304','B0305')
group by province_code,
    province_name,
    region_code,
    region_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
grouping sets
    ((
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name),     --明细
        (
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name),   --城市中类合计
        (
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name),      --城市组合计明细
        (
        region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name),     --省区明细
        (
        region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name),   --省区中类汇总
        (
        region_code,
        region_name,
        province_code,
        province_name),   -- 31 省区汇总
        (
        region_code,
        region_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name),  --32743 大区小类
        (
        region_code,
        region_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name),  --32263 大区中类汇总
        (
        region_code,
        region_name),  --7 大区汇总
        (
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name), -- 32737 全国小类 汇总
        (
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name), -- 8161 全国中类 汇总
        ()   --0 
    )
;



-- select * from  csx_tmp.temp_sale_02;

-- 统计期末库存
drop table if exists csx_tmp.temp_sale_02;
create temporary table csx_tmp.temp_sale_02 as
select sales_region_code as sales_region_code,
    sales_province_code,
    city_group_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sum(a.qty) final_qty,
    sum(a.amt) final_amt,
    grouping__id
from csx_dw.dws_wms_r_d_accounting_stock_m a
    join (
        select  shop_id,
            case when purchase_org='P620' then '62' else sales_region_code end sales_region_code,
            case when purchase_org='P620' then '62' else sales_province_code end sales_province_code,
            case when purchase_org='P620' then '' else city_group_code end city_group_code,
            city_group_name
        from csx_dw.dws_basic_w_a_csx_shop_m
        where sdt = 'current'
            and purpose in ('01', '03', '07','06')
            and table_type = 1
            and purchase_org !='P620'
    ) b on a.dc_code = b.shop_id
where sdt = ${hiveconf:e_dt}
    and classify_middle_code  in('B0304','B0305')
    and reservoir_area_code not in ('PD01', 'PD02', 'TS01')
group by sales_region_code,
    sales_province_code,
     city_group_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code 
grouping sets (
        (
            sales_region_code,
            sales_province_code,
            city_group_code,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
        (
            sales_region_code,
            sales_province_code,
            city_group_code,
            classify_large_code,
            classify_middle_code
        ),
        (
            sales_region_code,
            sales_province_code,
            city_group_code),
        (
            sales_region_code,
            sales_province_code,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
        (
            sales_region_code,
            sales_province_code,
            classify_large_code,
            classify_middle_code
        ),
         (
            sales_region_code,
            sales_province_code),
        (
            sales_region_code,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
        (   sales_region_code,
            classify_large_code,
            classify_middle_code),
         (   sales_region_code),
          (
            
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
         (
            
            classify_large_code,
            classify_middle_code
        ),
        ()
    );


drop table if exists  csx_tmp.temp_db_sale_00 ;
create temporary table csx_tmp.temp_db_sale_00 as 
select  
     a.channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    frozen_sales,           --冻品销售额
    frozen_profit,          --冻品毛利额
    frozen_profit/frozen_sales as frozen_profit_rate ,  --冻品毛利率
    frozen_daily_sales,     --冻品日配销售额
    frozen_daily_profit,    --冻品日配毛利额
    frozen_daily_profit/frozen_daily_sales as frozen_daily_profit_rate ,  --冻品日配毛利率
    last_frozen_sales,      --环期冻品销售额
    last_frozen_profit,     --环期冻品毛利额
    last_frozen_profit/last_frozen_sales as last_frozen_profit_rate ,  --冻品日配毛利率
    last_frozen_daily_sales,    --环期冻品日配销售额
    last_frozen_daily_profit,   --环期冻品日配毛利额
    last_frozen_daily_profit/last_frozen_daily_sales as last_frozen_daily_profit_rate ,  --环期冻品日配毛利率    
    (frozen_sales-last_frozen_sales)/last_frozen_sales as ring_sales_rate,   --冻品销售环比
    b_sales_value,              --B端销售额
    b_profit,                   --B端毛利额
    b_profit/b_sales_value as B_profit_rate ,  --B端毛利率
    frozen_sales/b_sales_value as frozen_sales_ratio , --冻品销售占比=冻品销售额/B端销售额
    B_daily_sale,               --B端日配销售额
    last_B_daily_sale,          --B端日配环期销售额
    frozen_sales_qty_30day,     --30天销量
    frozen_sales_30day,         --30天销售额
    frozen_profit_day,          --30天毛利额
    b.final_qty,                --期末库存量
    b.final_amt,                --期末库存额
    daily_cust_number as daily_sales_cust_number,          --日配成交客户数
    last_daily_cust_number as last_daily_sales_cust_number,     --环比日配冻品成交客户数
    b_daily_cust_number as b_daily_cust_number,        --B端日配客户成交数
    last_b_daily_cust_number as last_b_daily_cust_number,   --B端环期日配客户成交数
    daily_cust_number/b_daily_cust_number as daily_cust_penetration_rate,   --日配成交客户渗透率：日配客户
    last_daily_cust_number/last_b_daily_cust_number as last_daily_cust_penetration_rate,   --日配成交客户渗透率：日配客户
    (daily_cust_number/b_daily_cust_number)-(last_daily_cust_number/last_b_daily_cust_number) as diff_daily_cust_penetration_rate,
    frozen_daily_sales/B_daily_sale as frozen_daily_sales_ratio, --日配占比
    last_frozen_daily_sales/last_B_daily_sale as last_frozen_daily_sales_ratio, --环期日配占比
    (frozen_daily_sales/B_daily_sale)-( last_frozen_daily_sales/last_B_daily_sale ) diff_daily_sale_ratio,
    a.grouping__id  
from csx_tmp.tmp_dp_sale_03 a 
left join 
csx_tmp.temp_sale_02 b 
on coalesce(a.region_code,'')=coalesce(b.sales_region_code ,'')
    and coalesce(a.province_code,'')=coalesce(b.sales_province_code ,'')
    and coalesce(a.city_group_code,'')=coalesce(b.city_group_code ,'')
    and coalesce(a.classify_small_code,'')=coalesce(b.classify_small_code,'')
    and coalesce(a.classify_middle_code,'')=coalesce(b.classify_middle_code,'')
left join csx_tmp.temp_sale_30day c 
on coalesce(a.region_code,'')=coalesce(c.region_code ,'')
    and coalesce(a.province_code,'')=coalesce(c.province_code ,'')
    and coalesce(a.city_group_code,'')=coalesce(c.city_group_code ,'')
    and coalesce(a.classify_small_code,'')=coalesce(c.classify_small_code,'')
    and coalesce(a.classify_middle_code,'')=coalesce(c.classify_middle_code,'')
;

 set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.report_sale_r_d_frozen_classify_ratio_fr partition(months)
select  
       case when region_code is null then 1 
        when province_code is null then 2
        when city_group_code is null then 3
        when classify_small_name is null then 4
        else 5
    end level_id,  --分组：1 全国，2 大区，3 省区，4 城市组 5、小类汇总
    case  when grouping__id in (7,0,31,127) then 0 
        when grouping__id in (1921,1927,1951,2047) then 1
        when grouping__id in (8051,8071,8095,8191) then 2
        else 3 end  asc_id,
    substr(${hiveconf:e_dt} ,1,4) as years,
    substr(${hiveconf:e_dt} ,1,6) as smonth,
    coalesce(channel_name,'B端')as channel_name,
    coalesce(region_code,'00')as region_code,
    coalesce(region_name,'全国')as region_name,
    coalesce(province_code,'00') as  province_code,
    coalesce(province_name,'小计') as province_name,
    coalesce(city_group_code,'00')city_group_code,
    coalesce(city_group_name,'小计')city_group_name,
    coalesce(classify_large_code,'') as  classify_large_code,
    coalesce(classify_large_name,'') as  classify_large_name,
    coalesce(classify_middle_code,'') as  classify_middle_code,
    coalesce(classify_middle_name,'') as  classify_middle_name,
    coalesce(classify_small_code,'00') as classify_small_code,
    coalesce(classify_small_name,'小计') as classify_small_name,
    frozen_sales,           --冻品销售额
    frozen_profit,          --冻品毛利额
    frozen_profit/frozen_sales as frozen_profit_rate ,  --冻品毛利率
    frozen_daily_sales,     --冻品日配销售额
    frozen_daily_profit,    --冻品日配毛利额
    frozen_daily_profit/frozen_daily_sales as frozen_daily_profit_rate ,  --冻品日配毛利率
    last_frozen_sales,      --环期冻品销售额
    last_frozen_profit,     --环期冻品毛利额
    last_frozen_profit/last_frozen_sales as last_frozen_profit_rate ,  --冻品日配毛利率
    last_frozen_daily_sales,    --环期冻品日配销售额
    last_frozen_daily_profit,   --环期冻品日配毛利额
    last_frozen_daily_profit/last_frozen_daily_sales as last_frozen_daily_profit_rate ,  --环期冻品日配毛利率    
    (frozen_sales-last_frozen_sales)/last_frozen_sales as ring_sales_rate,   --冻品销售环比
    b_sales_value,              --B端销售额
    b_profit,                   --B端毛利额
    b_profit/b_sales_value as B_profit_rate ,  --B端毛利率
    frozen_sales/b_sales_value as frozen_sales_ratio , --冻品销售占比=冻品销售额/B端销售额
    B_daily_sale,               --B端日配销售额
    last_B_daily_sale,          --B端日配环期销售额
    frozen_sales_qty_30day,     --30天销量
    frozen_sales_30day,         --30天销售额
    frozen_profit_day,          --30天毛利额
    final_qty,                --期末库存量
    final_amt,                --期末库存额
    daily_sales_cust_number,          --日配成交客户数
    last_daily_sales_cust_number,     --环比日配冻品成交客户数
    b_daily_cust_number,        --B端日配客户成交数
    last_b_daily_cust_number,   --B端环期日配客户成交数
    daily_sales_cust_number/b_daily_cust_number as daily_cust_penetration_rate,   --日配成交客户渗透率：日配客户
    last_daily_sales_cust_number/last_b_daily_cust_number as last_daily_cust_penetration_rate,   --日配成交客户渗透率：日配客户
    (daily_sales_cust_number/b_daily_cust_number)-(last_daily_sales_cust_number/last_b_daily_cust_number) as diff_daily_cust_penetration_rate,
    frozen_daily_sales/B_daily_sale as frozen_daily_sales_ratio, --日配占比
    last_frozen_daily_sales/last_B_daily_sale as last_frozen_daily_sales_ratio, --环期日配占比
    (frozen_daily_sales/B_daily_sale)-( last_frozen_daily_sales/last_B_daily_sale ) diff_daily_sale_ratio,
    grouping__id ,
    current_timestamp(),
    substr(${hiveconf:e_dt} ,1,6)
from csx_tmp.temp_db_sale_00 

;

select * from  csx_tmp.temp_sale_all_01 where region_code='3';

show create table csx_tmp.report_sale_r_d_frozen_industry_new_fr;




CREATE TABLE `csx_tmp.report_sale_r_d_frozen_classify_ratio_fr`(
  `level_id` int COMMENT '分组', 
  `asc_id` int COMMENT '排序', 
  `years` string COMMENT '年', 
  `smonth` string COMMENT '销售月', 
  `channel_name` string COMMENT '销售渠道', 
  `region_code` string COMMENT '大区编码', 
  `region_name` string COMMENT '大区名称', 
  `province_code` string COMMENT '省区编码', 
  `province_name` string COMMENT '省区名称', 
  `city_group_code` string COMMENT '城市组', 
  `city_group_name` string COMMENT '城市组名称', 
  `classify_large_code` string COMMENT '一级管理分类', 
  `classify_large_name` string COMMENT '一级管理分类', 
  `classify_middle_code` string COMMENT '二级管理分类', 
  `classify_middle_name` string COMMENT '二级管理分类', 
  `classify_small_code` string COMMENT '三级管理分类', 
  `classify_small_name` string COMMENT '三级管理分类', 
  `frozen_sales` decimal(38,6) COMMENT '冻品销售额', 
  `frozen_profit` decimal(38,6) COMMENT '冻品毛利额', 
  `frozen_profit_rate` decimal(38,6) COMMENT '冻品毛利率', 
  `frozen_daily_sales` decimal(38,6) COMMENT '冻品日配销售额', 
  `frozen_daily_profit` decimal(38,6) COMMENT '冻品日配毛利', 
  `frozen_daily_profit_rate` decimal(38,6) COMMENT '冻品日配毛利率', 
  `last_frozen_sales` decimal(38,6) COMMENT '环期冻品销售额', 
  `last_frozen_profit` decimal(38,6) COMMENT '环期冻品毛利额', 
  `last_frozen_profit_rate` decimal(38,6) COMMENT '环期冻品毛利率', 
  `last_frozen_daily_sales` decimal(38,6) COMMENT '环期冻品日配销售额', 
  `last_frozen_daily_profit` decimal(38,6) COMMENT '环期冻品日配毛利额', 
  `last_frozen_daily_profit_rate` decimal(38,6) COMMENT '环期日配冻品毛利率',   
  `ring_sales_rate` decimal(38,18) COMMENT '环比销售增长率',
  `b_sales_value` decimal(38,6) COMMENT 'B城市组\省区\大区毛利汇总',
  `B_profit` decimal(38,6) COMMENT 'B城市组\省区\大区毛利汇总', 
  `B_profit_rate` decimal(38,18) COMMENT 'B城市组\省区\大区毛利汇总', 
  `frozen_sales_ratio` decimal(38,18) COMMENT '冻品销售占比=冻品销售额/B端销售额', 
  `B_daily_sale` decimal(38,6) COMMENT 'B日配销售额', 
  `last_B_daily_sale` decimal(38,6) COMMENT 'B日配销售额', 
  `frozen_sales_qty_30day` decimal(38,18) COMMENT '30天销量', 
  `frozen_sales_30day` decimal(38,18) COMMENT '30天销售额', 
  `frozen_profit_day` decimal(38,6) COMMENT '30天毛利额', 
  `final_qty` decimal(38,6) COMMENT '期末库存量', 
  `final_amt` decimal(38,6) COMMENT '期末库存额', 
  `daily_sales_cust_number` bigint COMMENT '日配成交客户数', 
  `last_daily_sales_cust_number` bigint COMMENT '日配环期成交客户数', 
  `b_daily_cust_number` bigint COMMENT 'B端日配成交客户', 
  `last_b_daily_cust_number` bigint COMMENT 'B端日配环期成交客户', 
  `daily_cust_penetration_rate` decimal(38,18) COMMENT '日配成交客户渗透率：日配客户/B端日配客户', 
  `last_daily_cust_penetration_rate` decimal(38,18) COMMENT '环期日配成交客户渗透率：日配客户/B端日配客户', 
  `diff_daily_cust_penetration_rate` decimal(38,18) COMMENT '日配成交客户渗透率差：当期渗透率-环期渗透率', 
  `daily_sales_ratio` decimal(38,18) COMMENT '占比=小类行业销售额/B端行业销售额', 
  `last_daily_sales_ratio` decimal(38,18) COMMENT '占比=小类行业销售额/B端行业销售额', 
  `diff_daily_sale_ratio` decimal(38,18) COMMENT '占比差=当前占比-环期占比', 
  `grouping__id` string, 
  `update_time` timestamp)
COMMENT '冻品管理分类销售报表占比-新'
PARTITIONED BY ( 
  `months` string COMMENT '月度分区')
 
STORED AS parquet 
;