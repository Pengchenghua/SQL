
-- 工厂分类数据
-- 商超代加工仓 'W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4'

set edate = '${enddate}';
set edt =regexp_replace(${hiveconf:edate},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');

--select ${hiveconf:edt},${hiveconf:sdate};
-- 1.获取工厂商品销售明细
drop table if exists csx_tmp.temp_fac_sale ;
create temporary table if not exists  csx_tmp.temp_fac_sale as 
select split(id,'&')[0] as id ,
    case when channel_code in ('1','7','9') then '1' else channel_code end channel_code,
    case when channel_code in ('1','7','9') then '大' else channel_name end channel_name,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '11'
     when channel_code='2' then '12' else business_type_code end business_type_code,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
     when channel_code='2' then '非代加工' else business_type_name end business_type_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    goods_code,
    sum(sales_qty) as sales_qty,
    sum(sales_cost)as sales_cost,
    sum(profit) as profit,
    sum(excluding_tax_sales) as no_tax_sales,
    sum(excluding_tax_cost) as no_tax_cost,
    sum(excluding_tax_profit) as no_tax_profit
from csx_dw.dws_sale_r_d_detail
where sdt >=${hiveconf:sdate}
    and sdt<= ${hiveconf:edt}
    and is_factory_goods='1' 
    -- and business_type_name not like '%城市%'
    -- and dc_code='W0A8' 
    and classify_middle_code in ('B0304','B0305')
group by split(id,'&')[0] ,
    case when channel_code in ('1','7','9') then '1' else channel_code end ,
    case when channel_code in ('1','7','9') then '大' else channel_name end,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '11'
     when channel_code='2' then '12' else business_type_code end ,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
     when channel_code='2' then '非代加工' else business_type_name end ,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    goods_code
    ;


-- 2.根据销售凭证号查找工单单号 WO210521000796  source_order_no来源单号(工单号)
-- ZH wms_batch_no WMS原料单号
drop table if exists csx_tmp.temp_fac_sale_01;
create temporary table if not exists csx_tmp.temp_fac_sale_01 as 
select channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    wms_batch_no
from csx_dw.dws_wms_r_d_batch_detail a 
join 
csx_tmp.temp_fac_sale b on a.credential_no=b.id 
 where a.move_type in ('107A','108A')
group by 
    wms_batch_no,
    channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name
; 

-- select distinct business_type_name from  csx_tmp.temp_fac_sale_01;
-- create temporary table if not exists csx_tmp.temp_fac_sale_01 as 

-- 3.计算原料领用金额
insert overwrite  table csx_tmp.ads_fr_r_d_frozen_account_factory_category_cost partition(months) 
select   substr(${hiveconf:edt},1,6) as sales_months,
    case when channel_code is null then '00'
             else channel_code
        end channel_code,
        case when channel_code is null then '合计'
             else channel_name 
        end channel_name,
        case when business_type_code is null then '00' 
             else  business_type_code 
        end business_type_code,
        case when business_type_name is null and channel_code is null then '合计'  
             when business_type_name is null then channel_name 
             else business_type_name 
        end business_type_name,
        case when classify_large_code is null and business_type_name is null then '00' 
             when classify_large_code is null then '00'
             else classify_large_code 
        end classify_large_code,
        case when classify_large_name is null and business_type_name is null then '00' 
             when classify_large_name is null then '合计'
             else classify_large_name 
        end classify_large_name,
        case when classify_middle_code is null and classify_large_code is null then '00' 
             when classify_middle_code is null then '00'
             else classify_middle_code 
        end classify_middle_code,
        case when classify_middle_name is null and classify_large_code is null then '合计' 
             when classify_middle_name is null then '合计'
             else classify_middle_name
             end classify_middle_name,
        case when classify_small_code is null and classify_middle_name is null then '00' 
             when classify_small_code is null then '00'
             else classify_small_code 
        end classify_small_code,
        case when classify_small_code is null and classify_middle_code is null then '合计'
             when classify_small_code is null then classify_middle_name 
             else classify_small_name 
        end classify_small_name,
        raw_no_tax_amt,
        raw_amt,
        finished_no_tax_amt,
        finished_amt,
    current_timestamp(),
    substr(${hiveconf:edt},1,6)
from 
(select  
    channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(coalesce(raw_no_tax_amt,0)) as raw_no_tax_amt,
    sum(coalesce(raw_amt,0)) as raw_amt,
    sum(coalesce(finished_no_tax_amt,0)) as finished_no_tax_amt,
    sum(coalesce(finished_amt,0)) as finished_amt
from 
(select   channel_code,
    channel_name,
    c.business_type_code,
    business_type_name,
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
    link_wms_batch_no,
    goods_code,
    sum(case when a.in_or_out=1 and a.move_type in ('119A','119B') then amt_no_tax end ) as raw_no_tax_amt,   --原料领用成本未税
    sum(case when a.in_or_out=1 and a.move_type in ('119A','119B') then amt end ) as raw_amt,                 -- 原料领用成本含税
    sum(case when a.in_or_out=0 and a.move_type in ('120A','120B') then amt_no_tax end ) as finished_no_tax_amt,   --成品未税
    sum(case when a.in_or_out=0 and a.move_type in ('120A','120B') then amt end ) as finished_amt                    --成品含税
from csx_dw.dws_wms_r_d_batch_detail a 
join 
    (SELECT goods_id,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
    FROM csx_dw.dws_basic_w_a_csx_product_m
    WHERE sdt='current' and classify_middle_code in ('B0304','B0305') ) b on a.goods_code=b.goods_id
join 
 csx_tmp.temp_fac_sale_01 c on a.link_wms_batch_no=c.wms_batch_no
where a.move_type in ('119A','119B','120A','120B')
group by
    channel_code,
    channel_name,
    c.business_type_code,
    business_type_name,
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
    link_wms_batch_no,
    goods_code
) a 
   group by 
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
  grouping sets (
    ( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),
    (channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  -- 业务中类合计
        ( channel_code, 
    channel_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),  -- 渠道三级分类
        ( channel_code, 
    channel_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  --渠道+二级分类合计
        ( 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),   --三级分类汇总
        (
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  --二级分类汇总
        ( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name),  -- 一级分类汇总
         (channel_code,
        channel_name , classify_large_code, 
    classify_large_name),())
 ) a    ;


drop table csx_tmp.ads_fr_r_d_frozen_account_factory_category_cost;
CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_account_factory_category_cost`(
            sales_months string COMMENT '销售月',
          `channel_code` string COMMENT '渠道', 
          `channel_name` string COMMENT '渠道', 
          `business_type_code` string COMMENT '销售业务', 
          `business_type_name` string COMMENT '销售业务', 
          `classify_large_code` string COMMENT '管理一级分类', 
          `classify_large_name` string COMMENT '管理一级分类', 
          `classify_middle_code` string COMMENT '管理二级分类', 
          `classify_middle_name` string COMMENT '管理二级分类', 
          `classify_small_code` string COMMENT '管理三级分类', 
          `classify_small_name` string COMMENT '管理三级分类', 
          `raw_no_tax_amt` decimal(38,6) COMMENT '原料未税成本', 
          `raw_amt` decimal(38,6) COMMENT '原料含税成本', 
          `finished_no_tax_amt` decimal(38,6) COMMENT '成品未税成本', 
          `finished_amt` decimal(38,6) comment '成品含税成本',
           update_time TIMESTAMP COMMENT '更新时间'
    ) COMMENT '冻品工厂财务品类成本'
    partitioned by (months string comment'月分区')
    STORED AS parquet 
    ;