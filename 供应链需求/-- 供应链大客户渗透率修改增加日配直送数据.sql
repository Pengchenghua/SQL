-- 供应链大客户渗透率修改增加日配直送数据
-- ******************************************************************** 
-- @功能描述：
-- @创建者： 彭承华 
-- @创建者日期：2022-08-01 18:27:04 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET hive.optimize.sort.dynamic.partition=true;
--执行Map前进行小文件合并  
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;   
 




-- 本期数据 (含城市服务商)B端 channel_code not in ('2','4','5','6')

drop table if exists csx_analyse_tmp.csx_analyse_tmp_dp_sale;
create  table csx_analyse_tmp.csx_analyse_tmp_dp_sale
as 
select 
    case when a.channel_code not in ('2','4','5','6') then 'B端' end channel_name,
    a.performance_region_code region_code,
    a.performance_region_name region_name,
    a.performance_province_code province_code,
    a.performance_province_name province_name,
    a.performance_city_code city_group_code,
    a.performance_city_name city_group_name,
    a.customer_code,
    business_type_code,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name,
    sum(sale_amt)as sales_value,
    sum(a.profit)as profit,
    sum( case when c.shop_low_profit_flag=0  and business_type_code=1 then sale_amt end ) as daily_sales_value,
    sum( case when c.shop_low_profit_flag=0  and business_type_code=1 then profit end ) as daily_profit,
    sum( case when business_type_code=1 then sale_amt end ) as daily_direct_sales_value,
    sum( case when business_type_code=1 then profit end ) as daily_direct_profit
from  csx_dws.csx_dws_sale_detail_di a 
join
(select goods_code,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
    from csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code
left join 
(SELECT shop_code,shop_low_profit_flag FROM csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code  	-- 剔除日配剔除联营仓
where sdt>=regexp_replace(trunc('${edt}','MM'),'-','')
    and sdt<=regexp_replace('${edt}','-','')
    -- and a.business_type_code !='4'
    and a.channel_code not in ('2','4','5','6')
group by 
    case when a.channel_code not in ('2','4','5','6') then 'B端' end,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    business_type_code ,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name,
    a.customer_code
;

-- 环期数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_dp_sale_01;
create temporary table csx_analyse_tmp.csx_analyse_tmp_dp_sale_01
as 
select 
    case when channel_code not in ('2','4','5','6') then 'B端' end channel_name,
    a.performance_region_code region_code,
    a.performance_region_name region_name,
    a.performance_province_code province_code,
    a.performance_province_name province_name,
    a.performance_city_code city_group_code,
    a.performance_city_name city_group_name,
    a.customer_code,
    business_type_code,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name,
    sum(sale_amt)as last_sales_value,
    sum(a.profit)as last_profit,
    sum( case when c.shop_low_profit_flag=0  and business_type_code=1 then sale_amt end ) as last_daily_sales_value,
    sum( case when c.shop_low_profit_flag=0  and business_type_code=1 then profit end ) as last_daily_profit,
    sum( case when business_type_code=1 then sale_amt end ) as last_daily_direct_sales_value,
    sum( case when business_type_code=1 then profit end ) as last_daily_direct_profit
from  csx_dws.csx_dws_sale_detail_di a 
join
(select goods_code,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
    from csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code
left join 
(SELECT shop_code,shop_low_profit_flag FROM csx_dim.csx_dim_shop where sdt='current') c on a.inventory_dc_code=c.shop_code  	-- 剔除日配剔除联营仓
where sdt>= regexp_replace(add_months(trunc('${edt}','MM'),-1),'-','')
    and sdt<= regexp_replace(if('${edt}'=last_day('${edt}'),last_day(add_months('${edt}',-1)),add_months('${edt}',-1)),'-','')
    -- and a.business_type_code !='4'
    and a.channel_code not in ('2','4','5','6')
group by 
    case when channel_code not in ('2','4','5','6') then 'B端' end,
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    business_type_code  ,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name,
    a.customer_code
;



-- 本期与环比汇总
drop table if exists csx_analyse_tmp.csx_analyse_tmp_scm_sale_all;
create  temporary table csx_analyse_tmp.csx_analyse_tmp_scm_sale_all as 
select
    channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_code,
    business_type_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_value) as sales_value,
    sum(profit) as profit , 
    sum(daily_sales_value) as daily_sales_value,
    sum(daily_profit) as daily_profit,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit ,
    sum(last_daily_sales_value) as last_daily_sales_value,
    sum(last_daily_profit) as last_daily_profit,
    sum(daily_direct_sales_value ) as daily_direct_sales_value,
    sum(daily_direct_profit) as daily_direct_profit
from
(select channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_code,
    a.business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sales_value,
    profit, 
    daily_sales_value,
    daily_profit,
    daily_direct_sales_value,
    daily_direct_profit,
    0 as last_sales_value,
    0 as last_profit ,
    0 as last_daily_sales_value,
    0 as last_daily_profit,
    0 as last_daily_direct_sales_value,
    0 as last_daily_direct_profit,
from csx_analyse_tmp.csx_analyse_tmp_dp_sale a
union all
select channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_code,
    a.business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    0 as sales_value,
    0 as profit, 
    0 as daily_sales_value,
    0 as daily_profit,
    last_sales_value  ,
    last_profit ,
    last_daily_sales_value,
    last_daily_profit,
    last_daily_direct_sales_value,
    last_daily_direct_profit,
from csx_analyse_tmp.csx_analyse_tmp_dp_sale_01 a
) a
where region_code !=''
group by 
    channel_name,
    a.region_code,
    a.region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_code,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name

    ; 
    




-- 本期与环比汇总层级汇总 
drop table if exists csx_analyse_tmp.csx_analyse_tmp_scm_sale_all_01;
create temporary table csx_analyse_tmp.csx_analyse_tmp_scm_sale_all_01 as 
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
    sum(sales_value) as sales_value,
    sum(profit) as profit ,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    sum(daily_sales_value) as daily_sales_value,
    sum(last_daily_sales_value) as last_daily_sales_value,
    sum(daily_profit ) as daily_profit,             --日配毛利额
    sum(last_daily_profit ) as last_daily_profit,   --环期日配毛利额
    count(distinct case when daily_sales_value>0 then customer_code end ) as daily_cust_number, --日配成交客户数
    count(distinct case when last_daily_sales_value>0  then customer_code end )as last_daily_cust_number,  --环比日配冻品成交客户数
    sum(daily_direct_sales_value) as daily_direct_sales_value,              --日配含直送销售额
    sum(last_daily_direct_sales_value) as last_daily_direct_sales_value,    --环期含直送日配销售
    sum(daily_direct_profit ) as daily_direct_profit,             --日配含直送毛利额
    sum(last_daily_direct_profit ) as last_daily_direct_profit,   --环期含直送日配毛利额
    count(distinct case when daily_direct_sales_value>0 then customer_code end ) as daily_direct_cust_number, --日配含直送成交客户数
    count(distinct case when last_daily_direct_sales_value>0  then customer_code end )as last_daily_direct_cust_number  --环比日配含直送成交客户数
    grouping__id
from csx_analyse_tmp.csx_analyse_tmp_scm_sale_all a
-- where business_type_code!='4'  --剔除城市服务商
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
        city_group_name,
        classify_large_code,
        classify_large_name),   --城市大类合计
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
    classify_small_name), 
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name),
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name),--
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name),
    (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
     (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name),
     (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name),
    (channel_name,
    region_code,
    region_name),
    (channel_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
    (channel_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name),
    (channel_name,
    classify_large_code,
    classify_large_name ),
    ()
)
;

--   select * from  csx_analyse_tmp.temp_sale_cust;
   
-- 计算日配客户数
drop table if exists  csx_analyse_tmp.csx_analyse_tmp_sale_cust;
create  temporary table csx_analyse_tmp.csx_analyse_tmp_sale_cust as 
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    count(distinct case when daily_sales_value>0 then customer_code end) as b_daily_cust_number,
    count(distinct case when last_daily_sales_value>0 then customer_code end ) as last_b_daily_cust_number,
    sum(daily_sales_value) as daily_sales_value,
    sum(last_daily_sales_value) as last_daily_sales_value,
    grouping__id
from
(

select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_code,
    sum(daily_sales_value) as daily_sales_value,
    sum(last_daily_sales_value) as last_daily_sales_value
from csx_analyse_tmp.csx_analyse_tmp_scm_sale_all a
where 1=1
    -- business_type_code='1'
	-- and channel_code not in ('2','4','5','6')
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name,
    customer_code

) a
group by 
    channel_name,
    region_code,
    region_name,
    a.city_group_code,
    a.city_group_name,
    province_code,
    province_name
grouping sets
    (
    (channel_name,
     region_code,
    region_name,
    province_code,
    province_name,
    a.city_group_code,
    a.city_group_name),
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


--- 计算近30日均销售额 
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_30day;
create temporary table csx_analyse_tmp.csx_analyse_tmp_sale_30day as 
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
    sum(a.sales_qty_30day  ) as sales_qty_30day,
    sum(a.sales_value_30day ) as sales_value_30day,
    sum(a.profit_30day) as  profit_30day,
    grouping__id
from
(
select 
    a.performance_region_code region_code,
    a.performance_region_name region_name,
    a.performance_province_code province_code,
    a.performance_province_name province_name,
    a.performance_city_code city_group_code,
    a.performance_city_name city_group_name,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name,
   (a.sale_qty  ) as sales_qty_30day,
   (a.sale_amt ) as sales_value_30day,
   (a.profit) as  profit_30day
from csx_dws.csx_dws_sale_detail_di a  
join
(select goods_code,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
    from csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code
where sdt> regexp_replace(date_sub('${edt}',30),'-','')
    and sdt<= regexp_replace('${edt}','-','')
   -- and a.business_type_code !='4'  --剔除城市服务商
    and a.channel_code  in ('1','7','9')
    and performance_region_code!=''
  )a 
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
        city_group_name,
        classify_large_code,
        classify_large_name),   --城市中类合计
        (
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name),      --城市组合计明细
    (region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
    (region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name ),
    (region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name ),
     (
     region_code,
     region_name,
     province_code,
     province_name),
     (region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
     (region_code,
    region_name,
     classify_large_code,
     classify_large_name,
     classify_middle_code,
     classify_middle_name ),
     (region_code,
    region_name,
    classify_large_code,
    classify_large_name ),
     (
     region_code,
     region_name ),
    (
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
    (
     classify_large_code,
     classify_large_name,
     classify_middle_code,
     classify_middle_name ),
    (
     classify_large_code,
     classify_large_name ),
    ()
    );


-- 统计期末库存 01大客户物流 07 BBC物流
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_02;

create temporary table csx_analyse_tmp.csx_analyse_tmp_sale_02 as
select zone_id sales_region_code,
    dist_code,
    city_group_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    sum(a.final_qty) final_qty,
    sum(a.final_amt) final_amt,
    grouping__id
from(
select zone_id ,
    dist_code,
    city_group_code,
    c.classify_large_code,
    c.classify_middle_code,
    c.classify_small_code,
     (a.qty) final_qty,
     (a.amt) final_amt
     
from csx_dws.csx_dws_cas_accounting_stock_m_df a
join
(select goods_code,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
    from csx_dim.csx_dim_basic_goods 
    where sdt='current') c on a.goods_code=c.goods_code
    join 
(select 
    performance_province_code as dist_code,
    performance_province_name sales_province_name,
    performance_city_code city_group_code,
    performance_city_name city_group_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_code ='W0J8' then '9' else  performance_region_code end zone_id,
    case when (purchase_org ='P620' and purpose!='07') or shop_code ='W0J8' then '平台' else  performance_region_name end sales_region_name,
    shop_code,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_name end  city_name,
    province_code,
    province_name,
    purpose,
    purpose_name
from csx_dim.csx_dim_shop a 
join 
(select dc_code,enable_time from csx_dim.csx_dim_csx_data_market_conf_supplychain_location where sdt='current') b on a.shop_code=b.dc_code
 where sdt='current'    
    ) b on a.dc_code = b.shop_code
where sdt = regexp_replace('${edt}','-','')
   -- and classify_middle_code  in('B0304','B0305')
    and reservoir_area_code not in ('PD01', 'PD02', 'TS01','CY01')
)a 
group by zone_id,
    dist_code,
    city_group_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code 
grouping sets (
    (
            zone_id,
            dist_code,
            city_group_code,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
        (
            zone_id,
            dist_code,
            city_group_code,
            classify_large_code,
            classify_middle_code
        ),
    (
            zone_id,
            dist_code,
            city_group_code,
            classify_large_code
        ),
    (
            zone_id,
            dist_code,
            city_group_code
        ),
        (
            zone_id,
            dist_code,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
         (
            zone_id,
            dist_code,
            classify_large_code,
            classify_middle_code
        ),
         (
            zone_id,
            dist_code,
            classify_large_code
        ),
        (zone_id, 
        dist_code),
        (
            zone_id,
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
         (
            zone_id,
            classify_large_code,
            classify_middle_code
        ),
         (
            zone_id,
            classify_large_code
        ),
        (zone_id),
        (
            classify_large_code,
            classify_middle_code,
            classify_small_code
        ),
          (
            classify_large_code,
            classify_middle_code
        ),
          (
            classify_large_code
        ),
    ()
    );



-- 写入明细层  select * from csx_analyse_tmp.temp_all_sale;

drop table if exists csx_analyse_tmp.csx_analyse_tmp_all_sale;
create temporary table csx_analyse_tmp.csx_analyse_tmp_all_sale as 
select a.channel_name,
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
    sales_value,                                        -- 本期销售额
    profit ,                                            -- 本期毛利额
    last_sales_value,                                   -- 环比销售额
    last_profit,                                        -- 环比毛利额
    daily_cust_number,                                  -- 日配成交客户数
    last_daily_cust_number,                             -- 环比日配成交客户数
    b.b_daily_cust_number,                                -- B端本期客户数
    b.last_b_daily_cust_number,                           -- B端环期客户数
    c.all_sales_value,                                  -- 省区销售额
    all_profit,                                         -- 省区毛利额
    all_profit/all_sales_value as all_profit_rate,      -- 省区毛利率
    a.sales_value/all_sales_value as frozen_sales_ratio, -- 品类销售/省区销售占比
    coalesce((sales_value-last_sales_value)/last_sales_value,0) as ring_sales_ratio , -- 销售环比增长率
    profit/sales_value as frozen_profit_rate,           -- 定价毛利率
    profit/sales_value-last_profit/last_sales_value as  diff_profit_rate,     -- 毛利率差
    daily_cust_number/b_daily_cust_number as daily_cust_penetration_rate ,    -- 日配业务渗透率
    last_daily_cust_number/last_b_daily_cust_number as last_daily_cust_penetration_rate ,   -- 环期日配业务渗透率
    coalesce(daily_cust_number/b_daily_cust_number-last_daily_cust_number/last_b_daily_cust_number,0) as diff_daily_cust_penetration_rate ,   -- 日配业务渗透率环比
    a.daily_sales_value/b.daily_sales_value as daily_sales_ratio,   -- 日配业务销售额/省区日配占比
    a.last_daily_sales_value/b.last_daily_sales_value as last_daily_sales_ratio,    -- 日配业务销售额/省区日配占比
    a.daily_sales_value,
    a.daily_profit,
    a.last_daily_sales_value,
    a.last_daily_profit,
    b.daily_sales_value as prov_daily_sales_value,
    b.last_daily_sales_value as last_prov_daily_sales_value,
    a.grouping__id
from csx_analyse_tmp.csx_analyse_tmp_scm_sale_all_01 a 
left join 
(select * from csx_analyse_tmp.csx_analyse_tmp_sale_cust ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'')
    and coalesce(a.region_code,'')=coalesce(b.region_code ,'')
    and coalesce(a.city_group_code,'')=coalesce(b.city_group_code ,'')
  --  and coalesce(a.channel_name,'')=coalesce(b.channel_name ,'')
left join 
(select region_code,province_code,
    city_group_code,
    channel_name,
    sales_value as all_sales_value,
    profit as all_profit 
    from csx_analyse_tmp.csx_analyse_tmp_scm_sale_all_01
     
    where (classify_large_name is null
        and classify_middle_name is null
        and classify_small_name is null 
        )
    -- grouping__id in ('0','7','31','127')
    )c on coalesce(a.province_code,'')=coalesce(c.province_code,'') 
    and  coalesce(a.region_code,'')=coalesce(c.region_code ,'')
    and coalesce(a.city_group_code,'')=coalesce(c.city_group_code ,'')
    -- and  coalesce(a.channel_name,'')=coalesce(c.channel_name ,'')
where 1=1 
 --   and a.classify_small_code !=''
-- or a.grouping__id in ('0','7','31') )
;
 
insert overwrite table csx_analyse.csx_analyse_scm_classify_ratio_fr_di partition(months) 

select
    case when a.grouping__id = '0' then '0'
        when a.grouping__id = '2017' then '1' 
        when a.grouping__id = '7' then '2' 
        when a.grouping__id='2023' then '3' 
        when a.grouping__id='31' then '4' 
    else '5' end level_id,                                   -- 分组：0 全国，1 全国管理分类，2 大区，3大区管理分类 4省区，5省区管分类
    substr(regexp_replace('${edt}','-','') ,1,4) as years,
    substr(regexp_replace('${edt}','-','') ,1,6) as smonth,
    coalesce(a.channel_name,'B端')as channel_name,
    coalesce(a.region_code,'00')as region_code,
    coalesce(a.region_name,'全国')as region_name,
    coalesce(a.province_code,'00') as  province_code,
    coalesce(a.province_name,'小计') as province_name,
    coalesce(a.city_group_code,'00')city_group_code,
    coalesce(a.city_group_name,'小计')city_group_name,
    coalesce(a.classify_large_code,'00') as  classify_large_code,
    coalesce(a.classify_large_name,'小计') as  classify_large_name,
    coalesce(a.classify_middle_code,'00') as  classify_middle_code,
    coalesce(a.classify_middle_name,'小计') as  classify_middle_name,
    coalesce(a.classify_small_code,'00') as classify_small_code,
    coalesce(a.classify_small_name,'小计') as classify_small_name,
    sales_value,                                        -- 本期销售额
    profit ,                                            -- 本期毛利额
    profit/abs(sales_value) as profit_rate,           -- 定价毛利率
    a.daily_sales_value,
    a.daily_profit,
    a.daily_profit/abs(a.daily_sales_value ) as daily_profit_rate,  -- 日配毛利率
    last_sales_value,                                   -- 环比销售额
    last_profit,                                       -- 环比毛利额
    last_profit/abs(last_sales_value) as last_profit_rate ,  -- 环比毛利率
    a.last_daily_sales_value,
    a.last_daily_profit,
    a.last_daily_profit/abs(a.last_daily_sales_value) as last_daily_profit_rate,    -- 环比日配毛利率
    coalesce((sales_value-last_sales_value)/last_sales_value,0) as ring_B_classify_sales_rate , -- B端销售额环比增长率
    coalesce((daily_sales_value-last_daily_sales_value)/last_daily_sales_value,0) as ring_daily_sales_rate , -- B端销售额环比增长率
    profit/sales_value-last_profit/last_sales_value as  diff_profit_rate,     --B端销售额环比增长率
    a.daily_profit/abs(a.daily_sales_value )-a.last_daily_profit/abs(a.last_daily_sales_value) as diff_daily_profit_rate ,-- 日配销售额环比增长率
    all_sales_value,                                    -- 省区销售额
    all_profit,                                         -- 省区毛利额
    all_profit/all_sales_value as all_profit_rate,      -- 省区毛利率
    a.sales_value/all_sales_value as class_sales_ratio, -- 品类销售/省区销售占比
    prov_daily_sales_value,
    last_prov_daily_sales_value,
    b.sales_qty_30day,           -- 滚动30天销量
    b.sales_value_30day ,        -- 滚动30天销售额
    b.profit_30day,
    final_qty,      -- 期末库存量
    final_amt ,     -- 期末库存额
    daily_cust_number,                                  -- 日配成交客户数
    last_daily_cust_number,                             -- 环比日配成交客户数
    b_daily_cust_number,                                -- B端本期客户数
    last_b_daily_cust_number,                           -- B端环期客户数
    IF(daily_cust_number/b_daily_cust_number>=1,1,daily_cust_number/b_daily_cust_number) as daily_cust_penetration_rate ,   -- 日配业务渗透率
    IF(last_daily_cust_number/last_b_daily_cust_number>=1,1,last_daily_cust_number/last_b_daily_cust_number) as last_daily_cust_penetration_rate ,   -- 环期日配业务渗透率
    coalesce(IF(daily_cust_number/b_daily_cust_number>=1,1,daily_cust_number/b_daily_cust_number)- IF(last_daily_cust_number/last_b_daily_cust_number>=1,1,last_daily_cust_number/last_b_daily_cust_number),0) as diff_daily_cust_penetration_rate ,   -- 日配业务渗透率环比
    daily_sales_value/prov_daily_sales_value as daily_sales_ratio,                                      -- 日配业务销售额/省区日配占比
    last_daily_sales_value/last_prov_daily_sales_value as last_daily_sales_ratio ,                      -- 环期日配业务销售额/省区日配占比
     daily_sales_value/prov_daily_sales_value-last_daily_sales_value/last_prov_daily_sales_value as  diff_daily_sales_ratio ,
    a.grouping__id,
    current_timestamp(),
    substr(regexp_replace('${edt}','-','') ,1,6) 
from csx_analyse_tmp.csx_analyse_tmp_all_sale  a
left join csx_analyse_tmp.csx_analyse_tmp_sale_30day b on coalesce(a.province_code,'00')= coalesce(b.province_code,'00') 
        and coalesce(a.classify_small_code,'00')=coalesce(b.classify_small_code,'00') 
        and coalesce(a.region_code,'00')=coalesce(b.region_code ,'00')
        and coalesce(a.classify_middle_code,'00')=coalesce(b.classify_middle_code,'00') 
        and coalesce(a.classify_large_code,'00')=coalesce(b.classify_large_code,'00') 
        and coalesce(a.city_group_code,'00')=coalesce(b.city_group_code ,'00')
left join
(select sales_region_code,
    dist_code,
    city_group_code,
    classify_large_code,
    classify_middle_code,
    classify_small_code,
    final_qty,
    final_amt
from csx_analyse_tmp.csx_analyse_tmp_sale_02  ) d on coalesce(a.province_code,'00')=coalesce(dist_code ,'00')
and coalesce(a.classify_small_code,'00')=coalesce(d.classify_small_code,'00') 
and  coalesce(a.region_code,'00')=coalesce(d.sales_region_code ,'00')
and coalesce(a.classify_middle_code,'00')=coalesce(d.classify_middle_code,'00') 
and coalesce(a.classify_large_code,'00')=coalesce(d.classify_large_code,'00') 
and coalesce(a.city_group_code,'00')=coalesce(d.city_group_code ,'00')
;