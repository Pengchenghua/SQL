
-- 成品成本 source_order_type_code like 'KN%' 包含商品转码、原料转成品
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
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    wms_batch_no,
    batch_no,
    a.goods_code,
    a.credential_no,
    qty,
    price,
    price_no_tax,
    source_order_type_code,
    link_wms_batch_no,
    link_wms_order_no,
    move_type,
    source_order_no,
    order_no
from  csx_tmp.temp_fina_sale_00 a 
 join 
(
  select
    case when substr(link_wms_order_no,1,2) in ('03','04','06') then substr(link_wms_order_no,1,10) 
	  else link_wms_order_no end as link_wms_order_no,--107A 108A 对应销售单号
    move_type,
    goods_code,
    if(a.move_type='108A',qty*-1,qty)qty,
    price,
    a.amt_no_tax,
    a.price_no_tax,
    source_order_no,
	wms_batch_no,
	a.batch_no,
	a.link_wms_batch_no,
	credential_no,
	a.source_order_type_code
  from csx_dw.dws_wms_r_d_batch_detail a
  where move_type in ('107A','108A') --107A 销售出库 108A 退货入库
  --   and source_order_no like 'WO%'
   and source_order_type_code in ('KN')
) b 
 on a.credential_no=b.credential_no 
 and a.goods_code=b.goods_code

; 


--工单推采购原料 

drop table csx_tmp.tmp_sales_batch_detail_02;
create temporary table csx_tmp.tmp_sales_batch_detail_02
as
select
  channel_code,
  channel_name,
  business_type_code,
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
  order_no,
  a.goods_code,
  a.qty,
  a.price,
  order_code,
  product_code,
  goods_reality_receive_qty,
  p_total,
  no_tax_p_total,
  no_tax_fact_values,
  fact_values,
  qty*fact_values/goods_reality_receive_qty as batch_fact_values,--物料批次原料成本含税
  qty*no_tax_fact_values/goods_reality_receive_qty as no_tax_batch_fact_values,--物料批次原料成本未税
  qty*p_total/goods_reality_receive_qty as batch_p_total, --物料批次总成本
  qty*no_tax_p_total/goods_reality_receive_qty as no_tax_batch_p_total --物料批次总成本 未税
from
(
  select 
    channel_code,
    channel_name,
    business_type_code,
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
    wms_batch_no,
    batch_no,
    order_no,
    goods_code,
    qty,
    price,
    source_order_no
  from csx_tmp.temp_fac_sale_01  --销售订单商品
) a
left join
(
  select  
	goods_code,
	order_code,
	product_code,
	sum(goods_reality_receive_qty) over(partition by goods_code,order_code) as goods_reality_receive_qty,
	p_total,
	no_tax_p_total,
	no_tax_fact_values,
	fact_values
  from 
  (
    select 
      goods_code,--成品
      order_code,--工单号
  	  product_code,
  	  sum(goods_reality_receive_qty) as goods_reality_receive_qty,  --商品实际产量
      sum(p_total) as no_tax_p_total,  --计划成本小计(工单计划成本)
      sum(p_total*(1+tax_rate/100)) as p_total,
      sum(no_tax_fact_values) as no_tax_fact_values,  --不含税原料金额成本(原料成本即入库成本)
      sum(fact_values) as fact_values
    from   csx_dw.dws_mms_r_a_factory_order a 
    left join 
    (select goods_id,tax_rate from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')  b on a.goods_code=b.goods_id
    group by goods_code,order_code,product_code
  ) a 
) b on a.source_order_no = b.order_code and a.goods_code = b.goods_code
;




-- 原料、转码、成品数据汇总


drop table if exists csx_tmp.temp_fac_sale_03;
create temporary table if not exists csx_tmp.temp_fac_sale_03 as 

select  channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(raw_no_tax_amt)   as raw_no_tax_amt,
    sum(raw_amt) as raw_amt,
    sum(finished_no_tax_amt) as finished_no_tax_amt,
    sum(finished_amt) as finished_amt
from (
--商品转码金额
select  
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
    sum( case when source_order_type_code='KN' then  coalesce(qty*price_no_tax,0) end )   as raw_no_tax_amt,
    sum( case when source_order_type_code='KN' then  coalesce(qty*price,0) end ) as raw_amt,
    sum( case when source_order_type_code='KN' then  coalesce(qty*price_no_tax,0) end ) as finished_no_tax_amt,
    sum( case when source_order_type_code='KN' then  coalesce(qty*price,0) end ) as finished_amt
from 
  csx_tmp.temp_fac_sale_01  a 
  where a.source_order_type_code='KN' 
    and source_order_no not like 'WO%'
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
union all 
select 
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
    sum(no_tax_batch_fact_values)   as raw_no_tax_amt,
    sum(batch_fact_values) as raw_amt,
    sum(no_tax_batch_p_total) as finished_no_tax_amt,
    sum(batch_p_total) as finished_amt
from csx_tmp.tmp_sales_batch_detail_02
group by  channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    ) a 
group by  channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
;




-- select sum(finished_no_tax_amt) ,sum(raw_no_tax_amt) from  csx_tmp.temp_fac_sale_03
-- ;

-- 2.1计算原料领用金额
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
    sum(raw_no_tax_amt)   as raw_no_tax_amt,
    sum(raw_amt) as raw_amt,
    sum(finished_no_tax_amt) as finished_no_tax_amt,
    sum(finished_amt) as finished_amt
from 
  csx_tmp.temp_fac_sale_03  a 
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
 ) a    
 ;


