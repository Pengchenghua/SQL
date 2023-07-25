

--冻品联采管报20210910
-- -- 销售收入表
-- REFRESH csx_tmp.ads_fr_r_d_frozen_financial_classify_sales_join;  
-- -- 中台报价商品成本
-- REFRESH csx_tmp.ads_fr_r_d_frozen_financial_middle_join ; 
-- --公司间的销售
-- REFRESH csx_tmp.ads_fr_r_d_frozen_direct_sales_join ; 
-- -- 返利支出
-- -- REFRESH csx_tmp.ads_fr_r_d_frozen_fanli_out_jion ; 
-- --销售成本
-- REFRESH csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost_join ; 
-- --工厂成本
-- REFRESH csx_tmp.ads_fr_r_d_frozen_account_factory_category_cost_join;  
-- --调整分摊金额
--  REFRESH csx_tmp.ads_fr_r_d_frozen_adjust_apportion_join;

-- select * from  csx_tmp.ads_fr_r_d_frozen_financial_classify_sales_join where months ='202108';  
-- select * from  csx_tmp.ads_fr_r_d_frozen_financial_middle_join where months='202108';
-- select * from csx_tmp.ads_fr_r_d_frozen_adjust_apportion_join where months='202108';管报


set edate = '${enddate}';
set edt =regexp_replace(${hiveconf:edate},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists csx_tmp.temp_fina_sale_00 ;
create temporary table if not exists csx_tmp.temp_fina_sale_00 as 
    select
        split(id,'&')[0] as credential_no ,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        b.classify_large_code,
        b.classify_large_name,
        b.classify_middle_code,
        b.classify_middle_name,
        b.classify_small_code,
        b.classify_small_name,
        origin_order_no, 
        case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end as order_no, 
        dc_code, 
        goods_code, 
        tax_rate,
        case when channel_code in ('1','7','9') then '1' when channel_code in ('5','6') then '4' else  channel_code end channel_code,
        case when channel_code in ('1','7','9') then '大'  when channel_code in ('5','6') then '大宗'  else  channel_name end channel_name ,
        case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '21'
            when channel_code='2' then '22' else business_type_code  end business_type_code,
        case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
            when channel_code='2' then '非代加工' else business_type_name end business_type_name,
        order_category_name, 
        shipped_time, 
        regexp_replace(substr(shipped_time, 1, 10), '-', '') as shipped_date,
        purchase_price_flag,
        cost_price,
        sales_qty,
        sales_value,
        sales_cost,
        profit,
        excluding_tax_sales,
        excluding_tax_cost,
        excluding_tax_profit,
        purchase_price,
        middle_office_price,
        if(purchase_price=0 ,(a.cost_price*sales_qty), (a.purchase_price*a.sales_qty)) as  purchase_price_cost,
        middle_office_cost,
        if(purchase_price=0 ,(a.cost_price/(1+a.tax_rate/100))*a.sales_qty,(a.purchase_price/(1+a.tax_rate/100))*a.sales_qty) as no_tax_purchase_price_cost,
        (a.middle_office_price/(1+a.tax_rate/100))*sales_qty as no_tax_middle_office_cost,
        joint_purchase_flag,
        sales_type,
        is_factory_goods
    from csx_dw.dws_sale_r_d_detail a
    join
    (select shop_code,
        product_code,
        joint_purchase_flag,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_info a 
    left  join 
    (select 
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_small_code
    from csx_dw.dws_basic_w_a_manage_classify_m 
        where sdt='current' 
         and classify_middle_code in ('B0304','B0305')
    ) b on a.small_category_code=b.category_small_code
    where sdt='current'  
        
    )  b on a.goods_code=b.product_code and a.dc_code=b.shop_code
    where sdt >=${hiveconf:sdate}
      and sdt<= ${hiveconf:edt}
      and joint_purchase_flag='1'

    
    ;


--中台调节费用  增加调节项
insert overwrite table  csx_tmp.ads_fr_r_d_frozen_financial_middle_join partition(months)
select  
    substr(${hiveconf:edt},1,6),
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
    sales_qty,
    sales_cost,
    profit,
    middle_office_cost,
    purchase_price_cost,
    sales_value,
    warehouse_fee_amt,
    deliver_fee_amt,
    credit_fee_amt,
    run_fee_amt,
    joint_venture_fee_amt,
    adjust_amt,
    no_tax_cost,
    no_tax_profit,
    no_tax_sales,
    no_tax_middle_office_cost,
    no_tax_purchase_price_cost,
    no_tax_warehouse_fee_amt,
    no_tax_deliver_fee_amt,
    no_tax_credit_fee_amt,
    no_tax_run_fee_amt,
    no_tax_joint_venture_fee_amt,
    no_tax_adjust_amt,
    current_timestamp(),
    substr(${hiveconf:edt},1,6)
from 
    (select  
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
        sum(sales_qty)sales_qty,  
        sum(sales_cost)sales_cost, 
        sum(profit)profit,
        sum(middle_office_cost) as middle_office_cost,
        sum(case when  channel_code in  ('1','7','9')  and   purchase_price_flag=1 then (a.purchase_price*a.sales_qty)
                 when  channel_code in  ('1','7','9')  and   purchase_price_flag=0 then  ( a.cost_price *sales_qty) 
            end ) as  purchase_price_cost,            --采购成本
        sum(a.sales_value) as sales_value,
        sum(warehouse_rate*a.sales_value) as warehouse_fee_amt,
        sum(delivery_rate*a.sales_value) as deliver_fee_amt,
        sum(credit_rate*a.sales_value) as credit_fee_amt,
        sum(run_rate*a.sales_value) as run_fee_amt,
        sum(joint_venture_rate*a.sales_value) as joint_venture_fee_amt, 
        sum(adjust_rate*a.sales_value) as adjust_amt  ,
        sum(excluding_tax_cost)as no_tax_cost,   
        sum(excluding_tax_profit)as no_tax_profit, 
        sum(no_tax_middle_office_cost) as no_tax_middle_office_cost,
        sum(a.excluding_tax_sales) as no_tax_sales,
        sum(case when  channel_code in  ('1','7','9') and   purchase_price_flag=1 then (a.purchase_price/(1+a.tax_rate/100))*a.sales_qty 
                 when  channel_code in  ('1','7','9') and   purchase_price_flag=0 then  ( a.cost_price /(1+a.tax_rate/100))*sales_qty 
            end ) as no_tax_purchase_price_cost,   --采购报价成本未税
        sum(warehouse_rate*a.excluding_tax_sales) as no_tax_warehouse_fee_amt,
        sum(delivery_rate*a.excluding_tax_sales) as no_tax_deliver_fee_amt,
        sum(credit_rate*a.excluding_tax_sales) as no_tax_credit_fee_amt,
        sum(run_rate*a.excluding_tax_sales) as no_tax_run_fee_amt,
        sum(joint_venture_rate*a.excluding_tax_sales) as no_tax_joint_venture_fee_amt,
        sum(adjust_rate*a.excluding_tax_sales) as no_tax_adjust_amt     --调节项金额
    from
    csx_tmp.temp_fina_sale_00 a 
left join
    (
    select
         warehouse_code,
         goods_code,
         warehouse_rate,         --仓储率
         delivery_rate,          --配送率
         credit_rate,            --信控率       
         run_rate,               --运营率
         joint_venture_rate,     --联营率
         adjust_rate,            --调节项
         price_begin_time,
         price_end_time,
         channel,
         type,
         sdt
     from csx_dw.dws_price_r_d_goods_prices_m
        where 1=1
        and sdt >= regexp_replace(date_add(trunc(${hiveconf:edate},'MM'),-60),'-','')
        and sdt<= regexp_replace(${hiveconf:edate},'-','')
    ) b on a.dc_code = b.warehouse_code and a.goods_code = b.goods_code 
        and a.channel_code = cast(b.channel as string) and a.order_category_name = b.type
        and a.shipped_date = b.sdt
    -- where a.shipped_time >= b.price_begin_time 
    --     and a.shipped_time <= b.price_end_time
    group by a.province_code,
        a.province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
)a
;



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
  where move_type in ('107A','108A') 
  --   107A 销售出库 108A 退货入库
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


-- 计算原料领用金额
insert overwrite  table csx_tmp.ads_fr_r_d_frozen_account_factory_category_cost_join  partition(months) 
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


    -- 3.0 销售成本调整 csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost

    --冻品管报调整成本+盘点&报损

     

-- 3.0 销售成本调整 csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost

 --冻品管报调整成本+盘点&报损
-- 盘点—+报损
drop table csx_tmp.tmp_loss_01;
create temporary table csx_tmp.tmp_loss_01 
as 
    SELECT location_code  as dc_code,
               location_name,
               company_code,
               company_name,
               goods_code ,
               goods_name ,
               unit,
               price_no_tax,
               credential_no,
               posting_time,
               purchase_group_code,
               purchase_group_name,
               move_type,
               reservoir_area_code,
               wms_biz_type_code,
               wms_order_no,
               wms_biz_type_name,
               cost_center_code,
               cost_center_name,
               coalesce( case when move_type IN ('117B') then -1*qty when move_type IN ('117A') then  qty end,0) loss_qty,
               coalesce( case when move_type IN ('117B') then -1*amt_no_tax when move_type IN ('117A') then  amt_no_tax end,0) no_tax_loss_amt,
               coalesce( case when move_type IN ('117B') then -1*amt when move_type IN ('117A') then amt end,0)  loss_amt,
               coalesce( case when move_type IN ('115B') then -1*qty when move_type IN ('115A') then  qty end,0) pd_profit_qty,   --盘盈数量
               coalesce( case when move_type IN ('115B') then -1*amt_no_tax when move_type IN ('115A') then  amt_no_tax end,0)  no_tax_pd_profit_amt, --未税盘盈金额
               coalesce( case when move_type IN ('115B') then -1*amt when move_type IN ('115A') then amt end,0) pd_profit_amt,       --含税盘盈金额
               coalesce( case when move_type IN ('116B') then -1*qty when move_type IN ('116A') then  qty end,0) pd_loss_qty,        --盘亏数量
               coalesce( case when move_type IN ('116B') then -1*amt_no_tax when move_type IN ('116A') then  amt_no_tax end,0)  no_tax_pd_loss_amt,  --盘亏未税金额
               coalesce( case when move_type IN ('116B') then -1*amt when move_type IN ('116A') then amt end,0) pd_loss_amt  --盘亏含税金额
        FROM csx_dw.dws_cas_r_d_account_credential_detail a
        WHERE sdt>=${hiveconf:sdate}
          AND sdt<=${hiveconf:edt}
          and move_type in ('117A','117B','115A','115B','116A','116B')
          --  and wms_biz_type_code in (35, 36, 37, 38, 39, 40, 41, 64, 66, 76, 77, 78)
;


  
drop  table if exists csx_tmp.temp_cbgb_tz_v11;
create temporary table if not exists csx_tmp.temp_cbgb_tz_v11 
as 
select 
    dc_code,
    case when dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
        when dc_code in ('W0J8','W0G1') then '大宗一部'
        when dc_code in ('W0H4') then '大宗二部'
        else ''
        end dc_type,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(adj_ddfkc_no) adj_ddfkc_no,
    sum(a.adj_ddfkc) adj_ddfkc,
    sum(a.adj_cgth_no) adj_cgth_no,
    sum(adj_cgth) adj_cgth,
    sum(a.adj_gc_xs_no) adj_gc_xs_no,
    sum(adj_gc_xs) adj_gc_xs,
    sum(a.adj_gc_db_no) adj_gc_db_no,
    sum(adj_gc_db) adj_gc_db,
    sum(a.adj_gc_qt_no) adj_gc_qt_no,
    sum(adj_gc_qt) adj_gc_qt,
    sum(a.adj_sg_no) adj_sg_no,
    sum(adj_sg) adj_sg,
    sum(a.adj_bj_xs_no) adj_bj_xs_no,
    sum(adj_bj_xs) adj_bj_xs,
    sum(a.adj_bj_db_no) adj_bj_db_no,
    sum(adj_bj_db) adj_bj_db,
    sum(a.adj_bj_qt_no) adj_bj_qt_no,
    sum(adj_bj_qt) adj_bj_qt,
    sum(loss_qty) loss_qty,
    sum(no_tax_loss_amt) no_tax_loss_amt,
    sum(loss_amt) loss_amt,
    sum(pd_profit_qty) pd_profit_qty,   --盘盈数量
    sum(no_tax_pd_profit_amt) no_tax_pd_profit_amt, --未税盘盈金额
    sum(pd_profit_amt) pd_profit_amt,       --含税盘盈金额
    sum(pd_loss_qty) pd_loss_qty,        --盘亏数量
    sum(no_tax_pd_loss_amt) no_tax_pd_loss_amt,  --盘亏未税金额
    sum(pd_loss_amt) pd_loss_amt,
    sum(adj_ddfkc_no+adj_cgth_no+adj_gc_xs_no+adj_gc_db_no+adj_gc_qt_no+adj_sg_no+adj_bj_xs_no+adj_bj_db_no+adj_bj_qt_no+(pd_profit_amt-no_tax_loss_amt-no_tax_pd_loss_amt)) as total_no_tax_amt,
    sum(adj_ddfkc+adj_cgth+adj_gc_xs+adj_gc_db+adj_gc_qt+adj_sg+adj_bj_xs+adj_bj_db+adj_bj_qt+(pd_profit_amt-loss_amt-pd_loss_amt)) as total_amt

from
(select a.dc_code,
    a.goods_code,
    sum(coalesce(adj_ddfkc_no_tax,0)) adj_ddfkc_no,
    sum(coalesce(a.adj_ddfkc,0)) adj_ddfkc,
    sum(coalesce(a.adj_cgth_no_tax,0)) adj_cgth_no,
    sum(coalesce(adj_cgth,0)) adj_cgth,
    sum(coalesce(a.adj_gc_xs_no_tax,0)) adj_gc_xs_no,
    sum(coalesce(adj_gc_xs,0)) adj_gc_xs,
    sum(coalesce(a.adj_gc_db_no_tax,0)) adj_gc_db_no,
    sum(coalesce(adj_gc_db,0)) adj_gc_db,
    sum(coalesce(a.adj_gc_qt_no_tax,0)) adj_gc_qt_no,
    sum(coalesce(adj_gc_qt,0)) adj_gc_qt,
    sum(coalesce(a.adj_sg_no_tax,0)) adj_sg_no,
    sum(coalesce(adj_sg,0)) adj_sg,
    sum(coalesce(a.adj_bj_xs_no_tax,0)) adj_bj_xs_no,
    sum(coalesce(adj_bj_xs,0)) adj_bj_xs,
    sum(coalesce(a.adj_bj_db_no_tax,0)) adj_bj_db_no,
    sum(coalesce(adj_bj_db,0)) adj_bj_db,
    sum(coalesce(a.adj_bj_qt_no_tax,0)) adj_bj_qt_no,
    sum(coalesce(adj_bj_qt,0)) adj_bj_qt,
    0 loss_qty,
    0 no_tax_loss_amt,
    0 loss_amt,
    0 pd_profit_qty,   --盘盈数量
    0 no_tax_pd_profit_amt, --未税盘盈金额
    0 pd_profit_amt,       --含税盘盈金额
    0 pd_loss_qty,        --盘亏数量
    0 no_tax_pd_loss_amt,  --盘亏未税金额
    0 pd_loss_amt  
from 
csx_dw.dws_sync_r_d_data_relation_cas_sale_adjustment  a 
where  sdt >= ${hiveconf:sdate}
 and sdt<=${hiveconf:edt}
group by  a.dc_code,
    a.goods_code
union all 
select  a.dc_code,
    a.goods_code,
    0 adj_ddfkc_no,
    0 adj_ddfkc,
    0 adj_cgth_no,
    0 adj_cgth,
    0 adj_gc_xs_no,
    0 adj_gc_xs,
    0 adj_gc_db_no,
    0 adj_gc_db,
    0 adj_gc_qt_no,
    0 adj_gc_qt,
    0 adj_sg_no,
    0 adj_sg,
    0 adj_bj_xs_no,
    0 adj_bj_xs,
    0 adj_bj_db_no,
    0 adj_bj_db,
    0 adj_bj_qt_no,
    0 adj_bj_qt,
    loss_qty,
    no_tax_loss_amt,
    loss_amt,
    pd_profit_qty,   --盘盈数量
    no_tax_pd_profit_amt, --未税盘盈金额
    pd_profit_amt,       --含税盘盈金额
    pd_loss_qty,        --盘亏数量
    no_tax_pd_loss_amt,  --盘亏未税金额
    pd_loss_amt  
from csx_tmp.tmp_loss_01  a
) a 
join 
(select goods_id,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_m 
    where sdt='current' 
        and classify_middle_code in ('B0304','B0305')
    ) b on a.goods_code=b.goods_id
    left join 
(select shop_code,
    product_code,
    joint_purchase_flag
    from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current' 
    ) c on a.goods_code=c.product_code and a.dc_code=c.shop_code 
left join 

-- W0G1\W0J8 大宗一  W0H4 大宗二
(SELECT shop_id,
       sales_province_code ,
       sales_province_name ,
       province_code,
       province_name,
       city_group_code ,
       city_group_name ,
       city_code,
       city_name,
       purpose,
       purpose_name
    FROM csx_dw.dws_basic_w_a_csx_shop_m
        WHERE sdt = 'current'
            and table_type=1 
        --    and purchase_org ='P620'
    ) d on d.shop_id=a.dc_code
where  c.joint_purchase_flag=1
group by   dc_code,
    case when dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
        when dc_code in ('W0J8','W0G1') then '大宗一部'
        when dc_code in ('W0H4') then '大宗二部'
        else ''
        end,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    ;


-- 3.3计算渠道占比
drop table  csx_tmp.temp_tz_01;
create temporary table csx_tmp.temp_tz_01 as 
 select 
    dc_type,
    channel_code,
    channel_name,
    a.business_type_code,
    a.business_type_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    no_tax_sale_amt,
    sum(no_tax_sale_amt)over(partition by dc_type ) as sum_sales_amt,
    no_tax_sale_amt/(sum(no_tax_sale_amt)over(partition by business_type_name,dc_type )) as classify_ratio,
    no_tax_sale_amt/(sum(no_tax_sale_amt)over(partition by dc_type )) as dc_type_ratio,
    no_tax_sale_amt/(sum(no_tax_sale_amt)over()) as ratio
from 

(select 
    case when a.business_type_code in ('1','2','3','4','5','6','22') then '-'
        else a.business_type_name end dc_type,
    channel_code,
    channel_name,
    a.business_type_code,
    a.business_type_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sum(excluding_tax_sales) as no_tax_sale_amt
 from csx_tmp.temp_fina_sale_00 a 
   where business_type_code !='4'
   group by 
    case when a.business_type_code in ('1','2','3','4','5','6','22') then '-'
        else a.business_type_name end ,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.business_type_code,
    a.business_type_name,
    channel_code,
    channel_name
)a

 ;
  

drop table  csx_tmp.temp_tz_02;
create table csx_tmp.temp_tz_02 as 
select a.*,b.total_no_tax_amt,a.dc_type_ratio*b.total_no_tax_amt  as no_tax_apportion_value,
a.dc_type_ratio*b.total_value  as apportion_value
from csx_tmp.temp_tz_01 a 
left join 
( select case when   dc_type='' then '-' else dc_type end dc_type ,
   sum(total_no_tax_amt) total_value,
   sum(total_amt) total_no_tax_amt
from  csx_tmp.temp_cbgb_tz_v11  
 group by  case when   dc_type='' then '-' else dc_type end
 )  b on a.dc_type=b.dc_type 
 ;
 
insert overwrite table `csx_tmp.ads_fr_r_d_frozen_adjust_apportion_join` partition(months)
select substr(${hiveconf:edt},1,6)months,
dc_type,
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
        case when classify_large_name is null and business_type_name is null then '合计' 
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
    no_tax_sale_amt,
    no_tax_apportion_amt,
    no_tax_sale_amt/sum(no_tax_sale_amt)over(partition by channel_name)*3.00 as sales_ratio,
        apportion_amt,
    current_timestamp(),
   substr(${hiveconf:edt},1,6)
from 
(
select dc_type,channel_code,
channel_name,
business_type_code,
business_type_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name,
sum(no_tax_sale_amt) no_tax_sale_amt,
sum(no_tax_apportion_value) no_tax_apportion_amt,
sum(apportion_value) apportion_amt

 from  csx_tmp.temp_tz_02
 group by dc_type,
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
grouping sets 
(( channel_code, 
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
) a ;



--销售汇总
 drop table if exists csx_tmp.temp_classify_sales;
 create temporary table if not exists csx_tmp.temp_classify_sales as  
-- insert overwrite table csx_tmp.ads_fr_r_d_frozen_financial_classify_sales partition(months)
   select substr(${hiveconf:edt},1,6), 
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
        case when classify_large_name is null and business_type_name is null then '合计' 
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
        sales_cost,
        sales_value,
        profit,
        no_tax_sales_cost,
        no_tax_sales,
        no_tax_profit,
        current_timestamp(),
        substr(${hiveconf:edt},1,6)
    from (
    select channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        sum(sales_cost) as sales_cost,
        sum(sales_value) as sales_value,
        sum(profit) as profit,
        sum(a.excluding_tax_cost) as no_tax_sales_cost,
        sum(a.excluding_tax_sales) as no_tax_sales,
        sum(a.excluding_tax_profit) as no_tax_profit,
        grouping__id
    from 
     csx_tmp.temp_fina_sale_00 a 
     where 1=1 
     and joint_purchase_flag =1
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
    )a

  ;

-- 后台收入W0AQ DC
 DROP TABLE IF EXISTS csx_tmp.temp_classify_sales_01;
  create temporary table csx_tmp.temp_classify_sales_01 as   
 select
 channel_code,
 business_type_code,
 classify_large_code,
 '00'classify_middle_code,
 '00' classify_small_code,
 sum(no_tax_back_amt) no_tax_back_amt,
 sum(back_total_amt) back_total_amt
 from (
 select '1' channel_code,
    '1' business_type_code,
    'B03' classify_large_code,
    '00' classify_middle_code,
    substr(SDT,1,6) months,
    sum(net_value) no_tax_back_amt,
    sum(value_tax_total) as back_total_amt
from  csx_dw.dwd_gss_r_d_settle_bill
where settle_place_code ='W0AQ' 
	AND sdt >= ${hiveconf:sdate}
	and sdt<=${hiveconf:edt}
	group by  substr(SDT,1,6) 
) a 
group by  channel_code,
 business_type_code,
 classify_large_code
 grouping sets 
 (( channel_code,
 business_type_code,
 classify_large_code),
 (business_type_code,
 classify_large_code),
 ( channel_code,
 classify_large_code),
 ());
 
 
 --销售表关联后台收入
  DROP TABLE IF EXISTS csx_tmp.temp_classify_sales_02;
  create temporary table csx_tmp.temp_classify_sales_02 as  
select a.*,coalesce(b.no_tax_back_amt,0) as no_tax_back_amt,
coalesce(b.back_total_amt,0) as back_total_amt
from  csx_tmp.temp_classify_sales  a 
left join  
csx_tmp.temp_classify_sales_01 b on coalesce(b.channel_code,'00')=a.channel_code
and  a.business_type_code=coalesce(b.business_type_code,'00')
and a.classify_large_code=coalesce(b.classify_large_code,'00')
and a.classify_middle_code=coalesce(b.classify_middle_code,'00')
and a.classify_small_code= coalesce(b.classify_small_code,'00')
;

-- 采购入库额 剔除KN  where  source_order_type_code not like 'KN%'   and move_type in ('107A','108A')
drop table if exists  csx_tmp.temp_frozen_purch_amt;
create temporary table if not exists csx_tmp.temp_frozen_purch_amt as 
 select substr(${hiveconf:edt},1,6), 
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
        case when classify_large_name is null and business_type_name is null then '合计' 
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
        purchase_qty,
        purchase_amt,
        no_tax_purchase_amt,
        current_timestamp(),
        substr(${hiveconf:edt},1,6)
    from (
select channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name,
    sum(coalesce(purchase_qty,0 ))purchase_qty,
    sum(coalesce(purchase_amt,0))purchase_amt,
    sum(coalesce(no_tax_purchase_amt,0 ))no_tax_purchase_amt
from(
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
    sum(coalesce(purchase_qty,0 ))purchase_qty,
    sum(coalesce(purchase_amt,0))purchase_amt,
    sum(coalesce(no_tax_purchase_amt,0 ))no_tax_purchase_amt
from 
 (select * from
    csx_tmp.temp_fina_sale_00
) a 
   join 
(
  select
    goods_code,
    credential_no,
    sum(if( move_type='107A',qty ,-1*qty) ) as purchase_qty,
    sum(if( move_type='107A',price_no_tax*qty,price_no_tax*qty*-1)) as no_tax_purchase_amt,
    sum(if( move_type='107A',price*qty,price *qty*-1)  ) as purchase_amt
  from csx_dw.dws_wms_r_d_batch_detail
     where  source_order_type_code not like 'KN%'
        and move_type in ('107A','108A')
  group by goods_code, credential_no
) b on a.credential_no = b.credential_no and a.goods_code = b.goods_code
group by channel_code, 
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
group by channel_code, 
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
    
)a
  ;

-- 5.0 销售收入 csx_tmp.ads_fr_r_d_frozen_financial_classify_sales
 insert overwrite table csx_tmp.ads_fr_r_d_frozen_financial_classify_sales_join partition(months)
  select  substr(${hiveconf:edt},1,6),
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
        sum(sales_cost) as sales_cost,
        sum(sales_value) as sales_value,
        sum(profit) as profit,
        sum(profit)/sum(sales_value) as profit_rate,
        sum(no_tax_sales_cost) as no_tax_sales_cost,
        sum(no_tax_sales) as no_tax_sales,
        sum(no_tax_profit) as no_tax_profit,
        sum(no_tax_profit) /sum(no_tax_sales)  as no_tax_profit_rate,
        sum(no_tax_apportion_amt) as no_tax_apportion_amt,
        sum(apportion_amt) as no_tax_apportion_amt,
        sum(no_tax_rebate_out_value) as no_tax_rebate_out_value,    --未税返利支出金额
        sum(rebate_out_value ) as rebate_out_value ,             --含税返利支出金额
        sum(no_tax_rebate_in_value) as no_tax_rebate_in_value,       --未税后台W0AQ收入金额
        sum(rebate_in_value) as rebate_in_value,               --含税后台W0AQ收入金额
        sum(no_tax_profit)-sum(no_tax_apportion_amt)+(sum(no_tax_rebate_in_value)-sum(no_tax_rebate_out_value)) as no_tax_net_profit,            --综合毛利=定价毛利+调整成本+（后台收入-后台支出）
        sum(profit)-sum(apportion_amt)+(sum(rebate_in_value)-sum(rebate_out_value)) as net_profit ,          --综合毛利=定价毛利+调整成本+（后台收入-后台支出）
        (sum(no_tax_profit)-sum(no_tax_apportion_amt)+(sum(no_tax_rebate_in_value)-sum(no_tax_rebate_out_value)))/sum(no_tax_sales) as no_tax_net_profit_rate,
        (sum(profit)-sum(apportion_amt)+(sum(rebate_in_value)-sum(rebate_out_value)))/sum(no_tax_sales) as net_profit_rate,
        sum(purchase_qty) purchase_qty,
        sum(purchase_amt ) purchase_amt,
        sum (no_tax_purchase_amt)no_tax_purchase_amt,
        current_timestamp(),
        substr(${hiveconf:edt},1,6)
  from 
  ( select  channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        sales_cost,
        sales_value,
        profit,
        no_tax_sales_cost as  no_tax_sales_cost,
        no_tax_sales as  no_tax_sales,
        no_tax_profit as no_tax_profit,
        0 no_tax_apportion_amt,
        0 apportion_amt,
        0 no_tax_rebate_out_value,    --未税返利支出金额
        0 rebate_out_value ,             --含税返利支出金额
        no_tax_back_amt as no_tax_rebate_in_value,       --未税W0AQ后台收入
        back_total_amt as  rebate_in_value ,              --含税W0AQ后台收入
        0 purchase_qty,
        0 purchase_amt,
        0 no_tax_purchase_amt
  from  csx_tmp.temp_classify_sales_02

 union all 
 --关联销售调整数据
 select channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        0 sales_cost,
        0 sales_value,
        0 profit,
        0 no_tax_sales_cost,
        0 no_tax_sales,
        0 no_tax_profit,
        coalesce(no_tax_apportion_amt,0) as no_tax_apportion_amt,
        coalesce(apportion_amt,0) apportion_amt,
        0 no_tax_rebate_out_value,    --未税返利支出金额
        0 rebate_out_value ,             --含税返利支出金额
        0 no_tax_rebate_in_value,       --未税返利收入金额
        0 rebate_in_value  ,
        0 purchase_qty,
        0 purchase_amt,
        0 no_tax_purchase_amt
    from  csx_tmp.ads_fr_r_d_frozen_adjust_apportion_join 
    where months=substr( ${hiveconf:edt},1,6)
    union all 
    select channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        0 sales_cost,
        0 sales_value,
        0 profit,
        0 no_tax_sales_cost,
        0 no_tax_sales,
        0 no_tax_profit,
        0 as no_tax_apportion_amt,
        0 apportion_amt,
        0 no_tax_rebate_out_value,    --未税返利支出金额
        0 rebate_out_value ,             --含税返利支出金额
        0 no_tax_rebate_in_value,       --未税返利收入金额
        0 rebate_in_value  ,
        purchase_qty,
        purchase_amt,
        no_tax_purchase_amt
    from csx_tmp.temp_frozen_purch_amt 
    ) a 
    group by channel_code,
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
           
-- show create table  csx_tmp.ads_fr_r_d_frozen_direct_sales_join;
        
   -- 公司间销售  csx_tmp.ads_fr_r_d_frozen_direct_sales 2116公司调拨
insert overwrite table csx_tmp.ads_fr_r_d_frozen_direct_sales_join partition(months) 
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
        case when classify_small_name is null and classify_middle_code is null then '合计'
             when classify_small_name is null then classify_middle_name 
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
FROM csx_dw.report_sale_r_m_company_pricing a 
join 
(select shop_code,product_code from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' and joint_purchase_flag=1 and shop_code='W0AQ') b on a.goods_code=b.product_code
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
) a ;





-- 采购销售入库成本(废弃) 与销售表共用

insert overwrite table csx_tmp.ads_fr_r_d_frozen_purch_join partition(months) 
select 
    a.credential_no,
    channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    a.goods_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    origin_order_no, 
    order_no, 
    dc_code,
    tax_rate,
    sales_cost,
    sales_qty,
    sales_value,
    profit,
    excluding_tax_cost,
    excluding_tax_profit,
    excluding_tax_sales,
    coalesce(purchase_qty,0 )purchase_qty,
    coalesce(purchase_amt,0)purchase_amt,
    coalesce(no_tax_purchase_amt,0 )no_tax_purchase_amt,
    coalesce(return_qty,0)return_qty,
    coalesce(return_amt,0)return_amt,
    coalesce(no_tax_return_amt,0)no_tax_return_amt,
    current_timestamp(),
    substr( ${hiveconf:edt},1,6)
from 
 (select * from
    csx_tmp.temp_fina_sale_00
   -- where   channel_code in ('1','7','9')
) a 
  left join 
(
  select
    goods_code,
    credential_no,
    sum(case when move_type='107A' then qty end ) as purchase_qty,
    sum(case when move_type='107A' then price_no_tax*qty end ) as no_tax_purchase_amt,
    sum(case when move_type='107A' then price *qty end ) as purchase_amt,
    sum(case when move_type='108A' then qty end ) as return_qty,
    sum(case when move_type='108A' then price_no_tax *qty end ) as no_tax_return_amt,
    sum(case when move_type='108A' then price *qty end ) as return_amt
  from csx_dw.dws_wms_r_d_batch_detail
  where move_type in ('107A','108A')
  group by goods_code, credential_no
) b on a.credential_no = b.credential_no and a.goods_code = b.goods_code

;

select   classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(no_tax_purchase_amt-no_tax_return_amt) purchase_amt
from  csx_tmp.temp_frozen_purch_amt
group by classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
union all 
select   classify_large_code,
    classify_large_name,
    '00'classify_middle_code,
    '00'classify_middle_name,
    '00'classify_small_code,
    '00'classify_small_name,
    sum(no_tax_purchase_amt-no_tax_return_amt)  as purchase_amt
from  csx_tmp.temp_frozen_purch_amt
group by classify_large_code,
    classify_large_name
    union all 
select   classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    '00'classify_small_code,
    '00'classify_small_name,
    sum(no_tax_purchase_amt-no_tax_return_amt) purchase_amt
from  csx_tmp.temp_frozen_purch_amt
group by classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name;
	
 
 CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_financial_middle_join`(
  `sales_months` string COMMENT '销售月份', 
  `province_code` string COMMENT '省区', 
  `province_name` string COMMENT '省区', 
  `city_group_code` string COMMENT '城市组', 
  `city_group_name` string COMMENT '城市组', 
  `classify_large_code` string COMMENT '管理一级', 
  `classify_large_name` string COMMENT '管理一级', 
  `classify_middle_code` string COMMENT '管理二级', 
  `classify_middle_name` string COMMENT '管理二级', 
  `classify_small_code` string COMMENT '管理三级', 
  `classify_small_name` string COMMENT '管理三级', 
  `sales_qty` decimal(30,6) COMMENT '销售量', 
  `sales_cost` decimal(30,6) COMMENT '销售成本', 
  `profit` decimal(30,6) COMMENT '毛利额', 
  `middle_office_cost` decimal(30,6) COMMENT '中台报价成本', 
  `purchase_price_cost` decimal(30,6) COMMENT '采购中台报价成本', 
  `sales_value` decimal(30,6) COMMENT '销售额', 
  `warehouse_fee_amt` decimal(38,10) COMMENT '含税仓配成本', 
  `deliver_fee_amt` decimal(38,10) COMMENT '含税配送金额', 
  `credit_fee_amt` decimal(38,10) COMMENT '含税信控金额', 
  `run_fee_amt` decimal(38,10) COMMENT '含税运营成本', 
  `joint_venture_fee_amt` decimal(38,10) COMMENT '含税联营成本', 
  `adjust_amt` decimal(38,10) COMMENT '调节项费用含税', 
  `no_tax_cost` decimal(30,6) COMMENT '未税销售成本', 
  `no_tax_profit` decimal(30,6) COMMENT '未税毛利额', 
  `no_tax_sales` decimal(30,6) COMMENT '未税销售额', 
  `no_tax_middle_office_cost` decimal(38,22) COMMENT '未税中台报价成本', 
  `no_tax_purchase_price_cost` decimal(30,6) COMMENT '未税采购中台报价成本', 
  `no_tax_warehouse_fee_amt` decimal(38,10) COMMENT '未税仓配成本', 
  `no_tax_deliver_fee_amt` decimal(38,10) COMMENT '未税配送金额', 
  `no_tax_credit_fee_amt` decimal(38,10) COMMENT '未税信控金额', 
  `no_tax_run_fee_amt` decimal(38,10) COMMENT '未税运营成本', 
  `no_tax_joint_venture_fee_amt` decimal(38,10) COMMENT '未税联营成本', 
  `no_tax_adjust_amt` decimal(38,10) COMMENT '调节项费用', 
  `update_time` timestamp COMMENT '插入时间')
COMMENT '联采-中台报价运营成本'
PARTITIONED BY ( 
  `months` string COMMENT '月分区')
 STORED AS parquet 
;


CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_account_factory_category_cost_join`(
  `sales_months` string COMMENT '销售月', 
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
  `finished_amt` decimal(38,6) COMMENT '成品含税成本', 
  `update_time` timestamp COMMENT '更新时间')
COMMENT '联采-冻品工厂财务品类成本'
PARTITIONED BY ( 
  `months` string COMMENT '月分区')
 
STORED AS parquet 
  ;
  



CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost_join`(
  `level_id` string COMMENT '层级', 
  `sales_months` string COMMENT '销售月份', 
  `channel_code` string COMMENT '渠道', 
  `channel_name` string COMMENT '渠道', 
  `business_type_code` string COMMENT '销售业务类型', 
  `business_type_name` string COMMENT '销售业务类型', 
  `classify_large_code` string COMMENT '管理一级分类', 
  `classify_large_name` string COMMENT '管理一级分类', 
  `classify_middle_code` string COMMENT '管理二级分类', 
  `classify_middle_name` string COMMENT '管理二级分类', 
  `classify_small_code` string COMMENT '管理二级分类', 
  `classify_small_name` string COMMENT '管理二级分类', 
  `adj_ddfkc_no` decimal(26,6) COMMENT '对抵负库存_未税', 
  `adj_ddfkc` decimal(26,6) COMMENT '对抵负库存_含税', 
  `adj_cgth_no` decimal(26,6) COMMENT '采购退货_未税', 
  `adj_cgth` decimal(26,6) COMMENT '采购退货_含税', 
  `adj_gc_xs_no` decimal(26,6) COMMENT '工厂月末分摊-调整销售_未税', 
  `adj_gc_xs` decimal(26,6) COMMENT '工厂月末分摊-调整销售_含税', 
  `adj_gc_db_no` decimal(26,6) COMMENT '工厂月末分摊-调整跨公司调拨_未税', 
  `adj_gc_db` decimal(26,6) COMMENT '工厂月末分摊-调整跨公司调拨_含税', 
  `adj_gc_qt_no` decimal(26,6) COMMENT '工厂月末分摊-调整其他_未税', 
  `adj_gc_qt` decimal(26,6) COMMENT '工厂月末分摊-调整其他_含税', 
  `adj_sg_no` decimal(26,6) COMMENT '手工调整销售成本_未税', 
  `adj_sg` decimal(26,6) COMMENT '手工调整销售成本_含税', 
  `adj_bj_xs_no` decimal(26,6) COMMENT '采购入库价格补救-调整销售_未税', 
  `adj_bj_xs` decimal(26,6) COMMENT '采购入库价格补救-调整销售_含税', 
  `adj_bj_db_no` decimal(26,6) COMMENT '采购入库价格补救-调整跨公司调拨_未税', 
  `adj_bj_db` decimal(26,6) COMMENT '采购入库价格补救-调整跨公司调拨_含税', 
  `adj_bj_qt_no` decimal(26,6) COMMENT '采购入库价格补救-调整其他_未税', 
  `adj_bj_qt` decimal(26,6) COMMENT '采购入库价格补救-调整其他_含税', 
  `adj_no_tax_sum_value` decimal(26,6) COMMENT '总销售成本金额未税', 
  `adj_sum_value` decimal(26,6) COMMENT '总销售成本金额含税', 
  `no_tax_sales_value` decimal(26,6) COMMENT '未税销售额', 
  `update_time` timestamp COMMENT '插入时间')
COMMENT '联采-财报管报冻品调整成本-销售调整'
PARTITIONED BY ( 
  `months` string COMMENT '月分区')

STORED AS parquet 
;
SHOW CREATE TABLE csx_tmp.ads_fr_r_d_frozen_direct_sales;

drop table csx_tmp.ads_fr_r_d_frozen_financial_classify_sales_join;
CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_financial_classify_sales_join`(
  `sales_monts` string COMMENT '销售日期', 
  `channel_code` string COMMENT '渠道', 
  `channel_name` string COMMENT '渠道', 
  `business_type_code` string COMMENT '销售业务', 
  `business_type_name` string COMMENT '销售业务', 
  `classify_large_code` string COMMENT '管理一级分类', 
  `classify_large_name` string COMMENT '管理一级分类', 
  `classify_middle_code` string COMMENT '管理二级分类', 
  `classify_middle_name` string COMMENT '管理二级分类', 
  `classify_small_code` string COMMENT '管理三级', 
  `classify_small_name` string COMMENT '管理三级', 
  `sales_cost` decimal(38,6) COMMENT '销售成本含税', 
  `sales_value` decimal(38,6) COMMENT '销售额含税', 
  `profit` decimal(38,6) COMMENT '毛利额含税', 
  `profit_rate` decimal(38,6) COMMENT '毛利率', 
  `no_tax_sales_cost` decimal(38,6) COMMENT '未税成本', 
  `no_tax_sales` decimal(38,6) COMMENT '未税销售', 
  `no_tax_profit` decimal(38,6) COMMENT '未税毛利额', 
  `no_tax_profit_rate` decimal(38,6) COMMENT '未税毛利率', 
  `adj_no_tax_sum_value` decimal(38,6) COMMENT '未税调整成本', 
  `adj_sum_value` decimal(38,6) COMMENT '含税调整成本', 
  `no_tax_rebate_out_value` decimal(38,6) COMMENT '返利支出未税额', 
  `rebate_out_value` decimal(38,6) COMMENT '返利支出含税额', 
  `no_tax_rebate_in_value` decimal(38,6) COMMENT '返利收入未税额', 
  `rebate_in_value` decimal(38,6) COMMENT '返利收入含税额', 
  `no_tax_net_profit` decimal(38,6) COMMENT '净毛利额未税', 
  `net_profit` decimal(38,6) COMMENT '净毛利额含税', 
  `no_tax_net_profit_rate` decimal(38,6) COMMENT '未税净毛利率', 
  `net_profit_rate` decimal(38,6) COMMENT '净毛利率', 
   `purchase_qty` decimal(38,6)COMMENT '采购数量', 
  `purchase_amt` decimal(38,6) COMMENT'采购金额含税', 
  `no_tax_purchase_amt` decimal(38,6) COMMENT '采购金额未税', 
  `update_time` timestamp COMMENT '更新时间')
COMMENT '联采-冻品财报-品类销售收入'
PARTITIONED BY ( `months` string COMMENT '月分区')
STORED AS parquet 
;


CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_direct_sales_join`(
  `sales_months` string COMMENT '销售月份', 
  `channel_code` string COMMENT '渠道', 
  `channel_name` string COMMENT '渠道', 
  `business_type_code` string COMMENT '销售业务类型', 
  `business_type_name` string COMMENT '销售业务类型', 
  `classify_large_code` string COMMENT '管理一级分类', 
  `classify_large_name` string COMMENT '管理一级分类', 
  `classify_middle_code` string COMMENT '管理二级', 
  `classify_middle_name` string COMMENT '管理二级', 
  `classify_small_code` string COMMENT '管理三级', 
  `classify_small_name` string COMMENT '管理三级', 
  `no_tax_sales_value` decimal(38,6) COMMENT '未税销售额', 
  `no_tax_profit` decimal(38,6) COMMENT '未税毛利额', 
  `no_tax_profit_rate` decimal(38,18) COMMENT '未税毛利率', 
  `sales_value` decimal(38,6) COMMENT '未税销售额', 
  `profit` decimal(38,6) COMMENT '毛利额', 
  `profit_rate` decimal(38,18) COMMENT '毛利率', 
  `update_time` timestamp COMMENT '插入时间')
COMMENT '联采-冻品财务-公司间交易销售'
PARTITIONED BY ( 
  `months` string COMMENT '月分区')

STORED AS parquet 
;




show create table csx_tmp.temp_frozen_purch_amt ;

drop table csx_tmp.ads_fr_r_d_frozen_purch_join;
CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_purch_join`(
  `credential_no` string comment '凭证号', 
  `channel_code` string comment '渠道', 
  `channel_name` string comment '渠道', 
  `business_type_code` string comment '业务类型', 
  `business_type_name` string comment '业务类型', 
  `goods_code` string comment '商品编码', 
  `classify_large_code` string comment '管理一级分类', 
  `classify_large_name` string comment '管理一级分类', 
  `classify_middle_code` string comment '管理二级分类', 
  `classify_middle_name` string comment '管理二级分类', 
  `classify_small_code` string comment '管理三级分类', 
  `classify_small_name` string comment '管理三级分类', 
  `origin_order_no` string comment '来源单号', 
  `order_no` string comment '单号', 
  `dc_code` string comment 'DC编码',  
  `tax_rate` decimal(38,6) comment '税率', 
  `sales_cost` decimal(38,6) comment '销售成本', 
  `sales_qty` decimal(38,6) comment '销售数量', 
  `sales_value` decimal(38,6) comment '销售金额', 
  `profit` decimal(38,6) comment '销售毛利额', 
  `excluding_tax_cost` decimal(38,6) comment '未税销售成本', 
  `excluding_tax_profit` decimal(38,6) comment '未税定价毛利额', 
  `excluding_tax_sales` decimal(38,6) COMMENT '未税销售额', 
  `purchase_qty` decimal(38,6)comment '采购数量', 
  `purchase_amt` decimal(38,6)comment '采购金额-含税', 
  `no_tax_purchase_amt` decimal(38,6)comment '未税采购额', 
  `return_qty` decimal(30,6)comment '退货数量', 
  `return_amt` decimal(38,6)comment '退货金额', 
  `no_tax_return_amt` decimal(38,6)comment '未税退货金额',
  `update_time` timestamp COMMENT '插入时间'
  )comment '联采-财务管报-冻品-采购入库成本'
PARTITIONED BY ( 
  `months` string COMMENT '月分区')
STORED AS parquet 
;



