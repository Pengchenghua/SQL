set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
set last_edt=regexp_replace(add_months(${hiveconf:edt} ,-1),'-','');

-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;
-- 本期数据 (不含合伙人 purpose!='06')
drop table if exists csx_tmp.tmp_dp_sale;
create temporary table csx_tmp.tmp_dp_sale
as 
select 

    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    sum(sales_value)as all_sales_value,
    sum(a.profit)as all_profit,
    sum(case when channel_code in ('1','9') then sales_value end ) as b_sales_value,
    sum(case when channel_code in ('1','9') then profit end ) as b_profit,
    sum(case when channel_code in ('7') then sales_value end ) as bbc_sales_value,
    sum(case when channel_code in ('7') then profit end ) as bbc_profit,
    sum(case when channel_code in ('2') then sales_value end ) as m_sales_value,
    sum(case when channel_code in ('2') then profit end ) as m_profit
from csx_dw.dws_sale_r_d_detail a 
join 
(select shop_id,purpose_name 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose!='06' ) b on a.dc_code=b.shop_id
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
and classify_middle_code='B0304'
group by 
    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name
;

-- 环期数据 (不含合伙人 purpose!='06')
drop table if exists csx_tmp.tmp_dp_sale_01;
create temporary table csx_tmp.tmp_dp_sale_01
as 
select 
    
    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    sum(sales_value)as last_all_sales_value,
    sum(a.profit)as last_all_profit,
    sum(case when channel_code in ('1','9') then sales_value end ) as last_b_sales_value,
    sum(case when channel_code in ('1','9') then profit end ) as last_b_profit,
    sum(case when channel_code in ('7') then sales_value end ) as last_bbc_sales_value,
    sum(case when channel_code in ('7') then profit end ) as last_bbc_profit,
    sum(case when channel_code in ('2') then sales_value end ) as last_m_sales_value,
    sum(case when channel_code in ('2') then profit end ) as last_m_profit
from csx_dw.dws_sale_r_d_detail a 
join 
(select shop_id,purpose_name 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose!='06' ) b on a.dc_code=b.shop_id
where sdt>=${hiveconf:last_sdt}
    and sdt<=${hiveconf:last_edt}
and classify_middle_code='B0304'
group by 

    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name
;


-- 本期与环比汇总
drop table if exists csx_tmp.temp_sale_all;
create temporary table csx_tmp.temp_sale_all as 
select
    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    sum(all_sales_value) as all_sales_value,
    sum(all_profit)as all_profit,
    sum(b_sales_value ) as b_sales_value,
    sum(b_profit ) as b_profit,
    sum(bbc_sales_value ) as bbc_sales_value,
    sum(bbc_profit ) as bbc_profit,
    sum(m_sales_value ) as m_sales_value,
    sum(m_profit) as m_profit,
    sum(all_sales_value) as sales_value      ,
    sum(m_profit) as profit      ,
    sum(last_all_sales_value)as last_all_sales_value,
    sum(last_all_profit)as last_all_profit,
    sum(last_b_sales_value) as last_b_sales_value,
    sum(last_b_profit) as last_b_profit,
    sum(last_bbc_sales_value) as last_bbc_sales_value,
    sum(last_bbc_profit ) as last_bbc_profit,
    sum(last_m_sales_value) as last_m_sales_value,
    sum(last_m_profit) as last_m_profit
from
(select 
    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    all_sales_value,
    all_profit,
    b_sales_value,
    b_profit,
    bbc_sales_value,
    bbc_profit,
    m_sales_value,
    m_profit,
    0 as last_all_sales_value,
    0 as last_all_profit,
    0 as last_b_sales_value,
    0 as last_b_profit,
    0 as last_bbc_sales_value,
    0 as last_bbc_profit,
    0 as last_m_sales_value,
    0 as last_m_profit
from csx_tmp.tmp_dp_sale a
union all
select 
    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    0 as all_sales_value,
    0 as all_profit,
    0 as b_sales_value,
    0 as b_profit,
    0 as bbc_sales_value,
    0 as bbc_profit,
    0 as m_sales_value,
    0 as m_profit,
    last_all_sales_value,
    last_all_profit,
    last_b_sales_value,
    last_b_profit,
    last_bbc_sales_value,
    last_bbc_profit,
    last_m_sales_value,
    last_m_profit
from csx_tmp.tmp_dp_sale_01 a
) a
group by 
   
    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name
    ;
   
drop table if exists csx_tmp.ads_sale_r_d_frozen_fr;
create  table csx_tmp.ads_sale_r_d_frozen_fr as 
select '1' level_id,
    case when channel_name is null and classify_large_code is not  null  then '小计' 
        else channel_name 
    end channel_name,
    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    sales_plan,
    sales_value      ,
    profit      ,
    profit/sales_value as profit_rate,
    last_sales_value,
    last_profit,
    last_profit/last_sales_value as last_profit_rate,
    (sales_value-last_sales_value)/last_sales_value as ring_sale_rate,
    current_timestamp()
from csx_tmp.temp_sale_all
where classify_large_code is not null


union all 

select
    '2' as level_id,
    channel_name,
    classify_large_code,
    classify_large_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    sales_plan,
    sales_value      ,
    profit      ,
    profit/sales_value as profit_rate,
    last_sales_value,
    last_profit,
    last_profit/last_sales_value as last_profit_rate,
    (sales_value-last_sales_value)/last_sales_value as ring_sale_rate,
    current_timestamp()
from csx_tmp.temp_sale_all
where channel_name is null and classify_large_code is null 

;


---- 第二版
set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
set last_edt=regexp_replace(add_months(${hiveconf:edt} ,-1),'-','');

-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;
-- 本期数据 (不含合伙人 purpose!='06')
drop table if exists csx_tmp.tmp_dp_sale;
create temporary table csx_tmp.tmp_dp_sale
as 
select 
    case when channel_code in ('1','9') then 'B端'
        when   channel_code ='2' then 'M端'
        when a.channel_code ='7' then 'BBC' end channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    '' sales_plan,
    sum(sales_value)as sales_value,
    sum(a.profit)as profit
from csx_dw.dws_sale_r_d_detail a 
join 
(select shop_id,purpose_name 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose!='06' ) b on a.dc_code=b.shop_id
where sdt>=${hiveconf:s_dt}
    and sdt<=${hiveconf:e_dt}
and classify_middle_code='B0304'
and a.channel_code not  in ('5','6','4')
group by 
    case when channel_code in ('1','9') then 'B端'
        when   channel_code ='2' then 'M端'
        when a.channel_code ='7' then 'BBC' end,
    classify_large_code,
    classify_large_name,
   a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
;

-- 环期数据 (不含合伙人 purpose!='06')
drop table if exists csx_tmp.tmp_dp_sale_01;
create temporary table csx_tmp.tmp_dp_sale_01
as 
select 
    case when channel_code in ('1','9') then 'B端'
        when   channel_code ='2' then 'M端'
        when a.channel_code ='7' then 'BBC' end channel_name,
    classify_large_code,
    classify_large_name,
   a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    '' as sales_plan,
    sum(sales_value)as last_sales_value,
    sum(a.profit)as last_profit
from csx_dw.dws_sale_r_d_detail a 
join 
(select shop_id,purpose_name 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose!='06' ) b on a.dc_code=b.shop_id
where sdt>=${hiveconf:last_sdt}
    and sdt<=${hiveconf:last_edt}
and classify_middle_code='B0304'
and a.channel_code not  in ('5','6','4')
group by 
    case when channel_code in ('1','9') then 'B端'
        when   channel_code ='2' then 'M端'
        when a.channel_code ='7' then 'BBC' end,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
;


-- 本期与环比汇总
drop table if exists csx_tmp.temp_sale_all;
create temporary table csx_tmp.temp_sale_all as 
select
    channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    0 as sales_plan,
    sum(sales_value) as sales_value      ,
    sum(profit) as profit      ,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit
from
(select channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    '' sales_plan,
    sales_value,
    profit,
    0 as last_sales_value,
    0 as last_profit
from csx_tmp.tmp_dp_sale a
union all
select channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    '' as sales_plan,
    0 as sales_value,
    0 as profit,
    last_sales_value  ,
    last_profit
from csx_tmp.tmp_dp_sale_01 a
) a
group by 
    channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
grouping sets
((channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),
    (
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name)
    )
    ;
    
    
drop table if exists csx_tmp.ads_sale_r_d_frozen_fr;
create  table csx_tmp.ads_sale_r_d_frozen_fr as 
select '1' level_id,
    case when channel_name is null and classify_large_code is not  null  then '小计' 
        else channel_name 
    end channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sales_plan,
    sales_value      ,
    profit      ,
    profit/sales_value as profit_rate,
    last_sales_value,
    last_profit,
    last_profit/last_sales_value as last_profit_rate,
    (sales_value-last_sales_value)/last_sales_value as ring_sale_rate,
    current_timestamp()
from csx_tmp.temp_sale_all a 
where classify_large_code is not null



;

report_sale_r_d_frozen_fr
-- 冻品管理分类销售汇总
CREATE TABLE `csx_tmp.report_sale_r_d_frozen_fr`(
  `level_id` string comment '层级', 
  `years` string COMMENT '销售年',
  `channel_name` string comment '渠道', 
  `classify_large_code` string comment '管理一级分类', 
  `classify_large_name` string comment '管理一级分类名称', 
  `classify_middle_code` string comment '管理二级分类', 
  `classify_middle_name` string comment '管理二级分类名称', 
  `classify_small_code` string comment '管理三级分类编码' ,
  `classify_small_name` string comment '管理三级分类名称', 
  `sales_plan` decimal(38,6) comment '销售预算', 
  `sales_value` decimal(38,6) comment '销售额', 
  `profit` decimal(38,6) comment '毛利额', 
  `profit_rate` decimal(38,6) comment '毛利率', 
  `last_sales_value` decimal(38,6) comment '环比期销售额', 
  `last_profit` decimal(38,6) comment  '环期毛利额', 
  `last_profit_rate` decimal(38,6) comment '环期毛利率', 
  `ring_sale_rate` decimal(38,6) comment '环期增长率', 
  `update_time` timestamp comment '更新日期'
)comment '冻品管理分类销售汇总'
partitioned by (months string comment '日期分区')
stored as parquet tblproperties('parquet.compression'='SNAPPY')
;

