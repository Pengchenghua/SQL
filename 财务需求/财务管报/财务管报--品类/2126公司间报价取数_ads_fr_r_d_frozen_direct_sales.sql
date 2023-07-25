

--取入库结算地点是W0AQ的入库单
drop table csx_tmp.W0AQ_wms_order_tmp00;
create  table csx_tmp.W0AQ_wms_order_tmp00
as
select
  b.credential_no,
  sum(if (in_or_out=1,b.qty,-1*qty)) qty,
  sum(if (in_or_out=1,b.amt,-1*amt)) amt,--含税金额
  sum(if (in_or_out=1,b.amt_no_tax,-1*amt_no_tax)) amt_no_tax,--未税金额
  b.goods_code
from
(
  select 
    credential_no,
    in_or_out,
    qty,
    amt,
    amt_no_tax,
    wms_order_no,
    goods_code
  from csx_dw.dws_wms_r_d_batch_detail
)b
join 
(
select distinct
  order_code,settlement_dc
from csx_dw.dwd_wms_r_d_entry_order_header
where settlement_dc='W0AQ'--取结算DC是W0AQ的
)c
on b.wms_order_no=c.order_code
group by   b.credential_no,b.goods_code;



insert overwrite table csx_dw.report_sale_r_m_company_pricing partition (month)
select
  concat_ws('&',credential_no,goods_code,month) as biz_id,
  business_type_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,
  city_code,
  city_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  a.credential_no,
  a.goods_code,
  a.goods_name,
  cast(sum(sales_qty) as decimal(30,6)) sales_qty,
  cast(sum(sales_value) as decimal(30,6)) sales_value,
  cast(sum(excluding_tax_sales) as decimal(30,6)) excluding_tax_sales,
  cast(sum(sales_cost) as decimal(30,6)) sales_cost,
  cast(sum(excluding_tax_cost) as decimal(30,6)) excluding_tax_cost,
  cast(sum(profit) as decimal(30,6)) profit,
  cast(sum(excluding_tax_profit) as decimal(30,6)) excluding_tax_profit,
  cast(sum(e.qty) as decimal(30,6)) entry_qty,
  cast(sum(e.amt) as decimal(30,6)) entry_amt,
  cast(sum(e.amt_no_tax) as decimal(30,6)) entry_amt_no_tax,
  cast(sum(e.amt)/sum(e.qty) as decimal(30,6)) as entry_price,
  cast(sum(e.amt_no_tax)/sum(e.qty) as decimal(30,6)) as entry_price_no_tax,
  month
from
(
  select 
    substr(sdt,1,6) as month,
    case when business_type_name='商超' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
         when business_type_name='商超' and dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '非代加工'
         else business_type_name end business_type_name,
    split(id, '&')[0] as credential_no,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    city_code,
    city_name,
    goods_code,
    goods_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sales_qty,--数量
    excluding_tax_sales,--未税交易收入
    sales_value,--含税销售金额
    excluding_tax_cost,--未税定价成本
    sales_cost,--含税销售成本
    excluding_tax_profit,--未税定价毛利
    profit--含税定价毛利
  from csx_dw.dws_sale_r_d_detail 
  where sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','') and sdt<regexp_replace(current_date,'-','')
  and classify_middle_code in ('B0304','B0305')
)a
join
csx_tmp.W0AQ_wms_order_tmp00 e
on a.credential_no=e.credential_no and e.goods_code=a.goods_code
group by   month,
  business_type_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,
  city_code,
  city_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  a.credential_no,
  a.goods_code,
  a.goods_name
  ;


set edate = '${enddate}';
set edt =regexp_replace(${hiveconf:edate},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');

insert overwrite table csx_tmp.ads_fr_r_d_frozen_direct_sales partition(months) 
select months,
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
       no_tax_sales_value,
       no_tax_profit,
       no_tax_profit/no_tax_sales_value as no_tax_profit_rate,
       sales_value,
       profit,
       profit/sales_value as profit_rate,
       current_timestamp(),
       months
from (
SELECT month as months,
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
       sum(no_tax_sales_value ) as no_tax_sales_value,
       sum(no_tax_profit) as no_tax_profit,
       sum(no_tax_profit)/sum(no_tax_sales_value) as no_tax_profit_rate,
       sum(sales_value ) as sales_value,
       sum(profit) as profit,
       sum(profit)/sum(sales_value) as profit_rate,
       grouping__id
       from 
(SELECT  month,
    case when channel_code in ('1','7','9') then '1' else channel_code end channel_code,
    case when channel_code in ('1','7','9') then '大' else channel_name end channel_name,
        business_type_code,
       business_type_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       sum(excluding_tax_sales ) as no_tax_sales_value,
       sum(excluding_tax_profit) as no_tax_profit,
       sum(excluding_tax_profit)/sum(excluding_tax_sales) as no_tax_profit_rate,
       sum(sales_value ) as sales_value,
       sum(profit) as profit,
       sum(profit)/sum(sales_value) as profit_rate
FROM csx_dw.report_sale_r_m_company_pricing
where month=substr(${hiveconf:edt},1,6)
GROUP BY 
 case when channel_code in ('1','7','9') then '1' else channel_code end ,
    case when channel_code in ('1','7','9') then '大' else channel_name end ,
        business_type_code,
       business_type_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       month
       )a 
group by  month,
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
grouping sets (( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),
        ( channel_code, 
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
) a ;;



  CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_direct_sales`(
	sales_months string comment '销售月份',
  `channel_code` string comment '渠道', 
  `channel_name` string comment '渠道', 
  `business_type_code` string comment '销售业务类型', 
  `business_type_name` string comment '销售业务类型', 
  `classify_large_code` string comment '管理一级分类', 
  `classify_large_name` string comment '管理一级分类', 
  `classify_middle_code` string comment '管理二级', 
  `classify_middle_name` string comment '管理二级', 
  `classify_small_code` string comment '管理三级', 
  `classify_small_name` string comment '管理三级', 
  `no_tax_sales_value` decimal(38,6) comment '未税销售额', 
  `no_tax_profit` decimal(38,6) comment '未税毛利额', 
  `no_tax_profit_rate` decimal(38,18) comment '未税毛利率',
   sales_value decimal(38,6) comment '未税销售额', 
   profit decimal(38,6) comment '毛利额', 
   profit_rate decimal(38,18) comment '毛利率',
   update_time timestamp comment '插入时间'
  ) comment '冻品财务-公司间交易销售'
  partitioned by (months string comment '月分区')
STORED AS parquet
; 

