
-- -- 销售收入表
-- REFRESH csx_tmp.ads_fr_r_d_frozen_financial_classify_sales;  
-- -- 中台报价商品成本
-- REFRESH csx_tmp.ads_fr_r_d_frozen_financial_middle ; 
-- --公司间的销售
-- REFRESH csx_tmp.ads_fr_r_d_frozen_direct_sales ; 
-- -- 返利支出
-- REFRESH csx_tmp.ads_fr_r_d_frozen_fanli_out ; 
-- --销售成本
-- REFRESH csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost ; 
-- --工厂成本
-- REFRESH csx_tmp.ads_fr_r_d_frozen_account_factory_category_cost;  

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
        order_no, 
        dc_code, 
        goods_code, 
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
        sales_qty,
        sales_value,
        sales_cost,
        profit,
        excluding_tax_sales,
        excluding_tax_cost,
        excluding_tax_profit,
        purchase_price,
        middle_office_price,
        if(purchase_price=0 ,(sales_cost*sales_qty), (a.purchase_price*a.sales_qty)) as  purchase_price_cost,
        middle_office_cost,
        if(purchase_price=0 ,(excluding_tax_cost*sales_qty),(a.purchase_price/(1+a.tax_rate/100))*a.sales_qty) as no_tax_purchase_price_cost,
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
    ) b 
    where sdt='current'  
        and a.small_category_code=b.category_small_code
    )  b on a.goods_code=b.product_code and a.dc_code=b.shop_code
    where sdt >=${hiveconf:sdate}
      and sdt<= ${hiveconf:edt}

    
    ;



  
-- 1.0 插入中台报价表   csx_tmp.ads_fr_r_d_frozen_financial_middle
drop table if exists csx_tmp.temp_sale_01 ;
create temporary table if not exists csx_tmp.temp_sale_01 as 
  select
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
    a.origin_order_no, 
    a.order_no, 
    a.dc_code, 
    a.goods_code, 
    a.channel_code,
    a.channel_name,
    a.order_category_name, 
    coalesce(c.delivery_date, a.shipped_date) as shipped_date,
    coalesce(c.delivery_time, a.shipped_time) as shipped_time,
    purchase_price_flag,            --采购报价标识
    sales_qty,                      --销量
    sales_value,                    --销售额
    sales_cost,                     --销售成本
    profit,                         --毛利额
    excluding_tax_sales,            --未税销售额    
    excluding_tax_cost,             --未税成本
    excluding_tax_profit,           --未税毛利额
    purchase_price,                 --采购报价
    middle_office_price             --中台报价
    no_tax_purchase_price_cost,     --采购报价成本未税
    purchase_price_cost,            --采购成本
    no_tax_middle_office_cost,       -- 中台报价成本未税
    middle_office_cost                  --中台成本
  from
  (select * from csx_tmp.temp_fina_sale_00
    where sales_type in ('qyg','bbc') 
    ) a
   left join
     (
    SELECT distinct order_no, 
        delivery_time, 
        regexp_replace(substr(delivery_time, 1, 10), '-', '') as delivery_date
    FROM csx_dw.dws_csms_r_d_yszx_order_m_new
    WHERE sdt >= regexp_replace(date_add(trunc(${hiveconf:edate},'MM'),-30),'-','')
        AND return_flag = ''
    ) c on a.origin_order_no = c.order_no
 ;
 
 
 
 
 
 --中台调节费用  增加调节项
 --中台调节费用  增加调节项
insert overwrite table  csx_tmp.ads_fr_r_d_frozen_financial_middle partition(months)
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


-- 2.0 工厂数据 csx_tmp.ads_fr_r_d_frozen_account_factory_category_cost 

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
csx_tmp.temp_fina_sale_00 b on a.credential_no=b.credential_no 
 where a.move_type in ('107A','108A')
 -- and b.joint_purchase_flag=1
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
    sum(case when a.in_or_out=1 and a.move_type in ('119A','119B') then coalesce(amt_no_tax,0) end ) as raw_no_tax_amt,   --原料领用成本未税
    sum(case when a.in_or_out=1 and a.move_type in ('119A','119B') then coalesce(amt,0) end ) as raw_amt,                 -- 原料领用成本含税
    sum(case when a.in_or_out=0 and a.move_type in ('120A','120B') then coalesce(amt_no_tax,0) end ) as finished_no_tax_amt,   --成品未税
    sum(case when a.in_or_out=0 and a.move_type in ('120A','120B') then coalesce(amt,0) end ) as finished_amt                    --成品含税
from csx_dw.dws_wms_r_d_batch_detail a 
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
      join 
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
    ) b 
    where sdt='current'  
        and a.small_category_code=b.category_small_code
    )  b on a.goods_code=b.product_code and a.dc_code=b.shop_code
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
 ) a    
 ;






-- 3.0 销售成本调整 csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost

--1 2 3 4 5 6  成本调整 adjustment_amt_no_tax,adjustment_amt
drop  table if exists csx_tmp.temp_cbgb_tz_v11;
create temporary table if not exists csx_tmp.temp_cbgb_tz_v11 
as 
select 
    coalesce(c.channel_code,'1') as channel_code,
    coalesce(c.channel_name,'大') as channel_name,
    coalesce(c.business_type_code,'6')as business_type_code,
    coalesce(c.business_type_name,'BBC') as business_type_name,
    coalesce(c.province_code,b.province_code) as province_code,
    coalesce(c.province_name,b.province_name) as province_name,
	coalesce(c.city_group_code,b.city_code) as city_code,
	coalesce(c.city_group_name,b.city_name) as city_name,
	a.classify_large_code,
	a.classify_large_name,
	a.classify_middle_code,
	a.classify_middle_name,
	a.classify_small_code,
	a.classify_small_name,
	sum(adj_ddfkc_no) adj_ddfkc_no,
	sum(adj_ddfkc) adj_ddfkc,
	sum(adj_cgth_no) adj_cgth_no,
	sum(adj_cgth) adj_cgth,
	sum(adj_gc_xs_no) adj_gc_xs_no,
	sum(adj_gc_xs) adj_gc_xs,
	sum(adj_gc_db_no) adj_gc_db_no,
	sum(adj_gc_db) adj_gc_db,
	sum(adj_gc_qt_no) adj_gc_qt_no,
	sum(adj_gc_qt) adj_gc_qt,
	sum(adj_sg_no) adj_sg_no,
	sum(adj_sg) adj_sg,
	sum(adj_bj_xs_no) adj_bj_xs_no,
	sum(adj_bj_xs) adj_bj_xs,
	sum(adj_bj_db_no) adj_bj_db_no,
	sum(adj_bj_db) adj_bj_db,
	sum(adj_bj_qt_no) adj_bj_qt_no,
	sum(adj_bj_qt) adj_bj_qt,
	sum(c.sales_value) sales_value,
	sum(c.excluding_tax_sales) as no_tax_sales_value
from
	(select item_source_order_no,
	    product_code,
	    location_code,
	    location_name,
	    classify_large_code,
	    classify_large_name,
	    classify_middle_code,
	    classify_middle_name,
	    classify_small_code,
	    classify_small_name,
		--对抵负库存的成本调整
		case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_ddfkc_no,
		case when adjustment_reason='in_remark' then adjustment_amt end adj_ddfkc,
		--采购退货金额差异的成本调整
		case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cgth_no,
		case when adjustment_reason='out_remark' then adjustment_amt end adj_cgth,
		--工厂月末分摊-调整销售订单
		case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
				then adjustment_amt_no_tax end adj_gc_xs_no,
		case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
				then adjustment_amt end adj_gc_xs,		
		--工厂月末分摊-调整跨公司调拨订单
		case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and item_wms_biz_type in('06','07','08','09','12','15','17') )
				then adjustment_amt_no_tax end adj_gc_db_no,
		case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and item_wms_biz_type in('06','07','08','09','12','15','17') )
				then adjustment_amt end adj_gc_db,		
		--工厂月末分摊-调整其他
		case when adjustment_reason in('fac_remark_sale','fac_remark_span')		
				and adjustment_type='sale'
				and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_gc_qt_no,
		case when adjustment_reason in('fac_remark_sale','fac_remark_span')  
				and adjustment_type='sale'
				and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt end adj_gc_qt,		
		--手工调整销售成本
		case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt_no_tax,adjustment_amt_no_tax) end adj_sg_no,
		case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt,adjustment_amt) end adj_sg,
		--采购入库价格补救-调整销售
		case when adjustment_reason = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and item_wms_biz_type in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
				then adjustment_amt_no_tax end adj_bj_xs_no,
		case when adjustment_reason = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and item_wms_biz_type in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
				then adjustment_amt end adj_bj_xs,				
		--采购入库价格补救-调整跨公司调拨	
		case when adjustment_reason = 'pur_remark_remedy'
				and adjustment_type='sale'
				and item_wms_biz_type in ('06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_bj_db_no,
		case when adjustment_reason = 'pur_remark_remedy'
				and adjustment_type='sale'
				and item_wms_biz_type in ('06','07','08','09','12','15','17')
				then adjustment_amt end adj_bj_db,				
		--采购入库价格补救-调整其他
		case when adjustment_reason = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and item_wms_biz_type not in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_bj_qt_no,	
		case when adjustment_reason = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and item_wms_biz_type not in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt end adj_bj_qt
	from 
		(select a.* ,
		    joint_purchase_flag,
            classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name,
            classify_small_code,
            classify_small_name
		from csx_dw.dwd_sync_r_d_data_relation_cas_sale_adjustment a 
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
    ) b 
    where sdt='current'  
        and a.small_category_code=b.category_small_code
    )  b on a.product_code=b.product_code and a.location_code=b.shop_code
	    	where  sdt >= ${hiveconf:sdate}
	            and sdt<=${hiveconf:edt}
	           -- and joint_purchase_flag=1
	    )a
)a
left join csx_tmp.temp_fina_sale_00 c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select shop_id,sales_province_code province_code,sales_province_name as province_name,city_group_code as city_code,city_group_name as city_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by   coalesce(c.channel_code,'1') ,
    coalesce(c.channel_name,'大') ,
    coalesce(c.business_type_code,'6'),
    coalesce(c.business_type_name,'BBC'),
    coalesce(c.province_code,b.province_code) ,
    coalesce(c.province_name,b.province_name) ,
	coalesce(c.city_group_code,b.city_code) ,
	coalesce(c.city_group_name,b.city_name) ,
	a.classify_large_code,
	a.classify_large_name,
	a.classify_middle_code,
	a.classify_middle_name,
	a.classify_small_code,
	a.classify_small_name;

--插入销售成本表
-- set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost partition(months)
select 
    case when channel_code='00' then 1 
        when business_type_code='00' then 2
        when classify_large_code='00' then 3
        when classify_middle_code='00' then 4
        when classify_small_code='00' then 5
        else 6
    end level_id,
    substr( ${hiveconf:edt},1,6),
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
    coalesce(adj_ddfkc_no, 0)as adj_ddfkc_no, 
    coalesce(adj_ddfkc, 0)as adj_ddfkc, 
    coalesce(adj_cgth_no,0 )as adj_cgth_no, 
    coalesce(adj_cgth, 0)as adj_cgth, 
    coalesce(adj_gc_xs_no,0 )as adj_gc_xs_no, 
    coalesce(adj_gc_xs,0 )as adj_gc_xs, 
    coalesce(adj_gc_db_no,0 )as adj_gc_db_no, 
    coalesce(adj_gc_db,0 )as adj_gc_db, 
    coalesce(adj_gc_qt_no,0 )as adj_gc_qt_no, 
    coalesce(adj_gc_qt,0 )as adj_gc_qt, 
    coalesce(adj_sg_no,0 )as adj_sg_no, 
    coalesce(adj_sg,0 )as adj_sg, 
    coalesce(adj_bj_xs_no,0 )as adj_bj_xs_no, 
    coalesce(adj_bj_xs,0 )as adj_bj_xs, 
    coalesce(adj_bj_db_no,0 )as adj_bj_db_no, 
    coalesce(adj_bj_db,0 )as adj_bj_db, 
    coalesce(adj_bj_qt_no,0 )as adj_bj_qt_no, 
    coalesce(adj_bj_qt,0 )as adj_bj_qt,    
    coalesce(adj_ddfkc_no, 0)+coalesce(adj_cgth_no,0 )+coalesce(adj_gc_xs_no,0 )+coalesce(adj_gc_db_no,0 )+coalesce(adj_gc_qt_no,0 )+coalesce(adj_sg_no,0 )+coalesce(adj_bj_xs_no,0 )+coalesce(adj_bj_db_no,0)+coalesce(adj_bj_qt_no,0 ) as adj_no_tax_sum_value,
    coalesce(adj_ddfkc, 0)+coalesce(adj_cgth,0 )+coalesce(adj_gc_xs,0 )+coalesce(adj_gc_db,0 )+coalesce(adj_gc_qt,0 )+coalesce(adj_sg,0 )+coalesce(adj_bj_xs,0 )+coalesce(adj_bj_db,0)+coalesce(adj_bj_qt,0 ) as adj_sum_value,
    coalesce(no_tax_sales_value,0)as no_tax_sales_value,
    current_timestamp(),
   substr( ${hiveconf:edt},1,6)
from (
select 
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
         coalesce(adj_ddfkc_no, 0)as adj_ddfkc_no, 
        coalesce(adj_ddfkc, 0)as adj_ddfkc, 
        coalesce(adj_cgth_no,0 )as adj_cgth_no, 
        coalesce(adj_cgth, 0)as adj_cgth, 
        coalesce(adj_gc_xs_no,0 )as adj_gc_xs_no, 
        coalesce(adj_gc_xs,0 )as adj_gc_xs, 
        coalesce(adj_gc_db_no,0 )as adj_gc_db_no, 
        coalesce(adj_gc_db,0 )as adj_gc_db, 
        coalesce(adj_gc_qt_no,0 )as adj_gc_qt_no, 
        coalesce(adj_gc_qt,0 )as adj_gc_qt, 
        coalesce(adj_sg_no,0 )as adj_sg_no, 
        coalesce(adj_sg,0 )as adj_sg, 
        coalesce(adj_bj_xs_no,0 )as adj_bj_xs_no, 
        coalesce(adj_bj_xs,0 )as adj_bj_xs, 
        coalesce(adj_bj_db_no,0 )as adj_bj_db_no, 
        coalesce(adj_bj_db,0 )as adj_bj_db, 
        coalesce(adj_bj_qt_no,0 )as adj_bj_qt_no, 
        coalesce(adj_bj_qt,0 )as adj_bj_qt, 
        coalesce(no_tax_sales_value,0)as no_tax_sales_value,
        grouping__id
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
    sum(adj_ddfkc_no) as adj_ddfkc_no, 
    sum(adj_ddfkc) as adj_ddfkc, 
    sum(adj_cgth_no) as adj_cgth_no, 
    sum(adj_cgth) as adj_cgth, 
    sum(adj_gc_xs_no) as adj_gc_xs_no, 
    sum(adj_gc_xs) as adj_gc_xs, 
    sum(adj_gc_db_no) as adj_gc_db_no, 
    sum(adj_gc_db) as adj_gc_db, 
    sum(adj_gc_qt_no) as adj_gc_qt_no, 
    sum(adj_gc_qt) as adj_gc_qt, 
    sum(adj_sg_no) as adj_sg_no, 
    sum(adj_sg) as adj_sg, 
    sum(adj_bj_xs_no) as adj_bj_xs_no, 
    sum(adj_bj_xs) as adj_bj_xs, 
    sum(adj_bj_db_no) as adj_bj_db_no, 
    sum(adj_bj_db) as adj_bj_db, 
    sum(adj_bj_qt_no) as adj_bj_qt_no, 
    sum(adj_bj_qt) as adj_bj_qt, 
    sum(sales_value ) as sales_value , 
    sum(no_tax_sales_value) as no_tax_sales_value,
    grouping__id
from csx_tmp.temp_cbgb_tz_v11 
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
) a 
) a;	


 -- 4.0 返利支出  csx_tmp.ads_fr_r_d_frozen_fanli_out 
-- create  table csx_tmp.ads_fr_r_d_frozen_fanli_out  as 
-- insert overwrite table csx_tmp.ads_fr_r_d_frozen_fanli_out  partition(months)
-- select   substr(${hiveconf:edt},1,6), 
--         case when channel_code is null then '00'
--              else channel_code
--         end channel_code,
--         case when channel_code is null then '合计'
--              else channel_name 
--         end channel_name,
--         case when business_type_code is null then '00' 
--              else  business_type_code 
--         end business_type_code,
--         case when business_type_name is null and channel_code is null then '合计'  
--              when business_type_name is null then channel_name 
--              else business_type_name 
--         end business_type_name,
--         case when classify_large_code is null and business_type_name is null then '00' 
--              when classify_large_code is null then '00'
--              else classify_large_code 
--         end classify_large_code,
--         case when classify_large_name is null and business_type_name is null then '00' 
--              when classify_large_name is null then '合计'
--              else classify_large_name 
--         end classify_large_name,
--         case when classify_middle_code is null and classify_large_code is null then '00' 
--              when classify_middle_code is null then '00'
--              else classify_middle_code 
--         end classify_middle_code,
--         case when classify_middle_name is null and classify_large_code is null then '合计' 
--              when classify_middle_name is null then '合计'
--              else classify_middle_name
--              end classify_middle_name,
--         case when classify_small_code is null and classify_middle_name is null then '00' 
--              when classify_small_code is null then '00'
--              else classify_small_code 
--         end classify_small_code,
--         case when classify_small_code is null and classify_middle_code is null then '合计'
--              when classify_small_code is null then classify_middle_name 
--              else classify_small_name 
--         end classify_small_name, 
--     no_tax_sale_value,
--     sales_value,
--     current_timestamp(),
--      substr(${hiveconf:edt},1,6) 
-- from (
-- select channel_code, 
--     channel_name, 
--     business_type_code, 
--     business_type_name, 
--     classify_large_code, 
--     classify_large_name, 
--     classify_middle_code, 
--     classify_middle_name, 
--     classify_small_code, 
--     classify_small_name, 
--     sum(excluding_tax_sales) no_tax_sale_value,
--     sum(sales_value) sales_value
-- from  csx_tmp.temp_fina_sale_00
-- where 1=1 
--   -- and joint_purchase_flag =1
-- group by channel_code, 
--     channel_name, 
--     business_type_code, 
--     business_type_name, 
--     classify_large_code, 
--     classify_large_name, 
--     classify_middle_code, 
--     classify_middle_name, 
--     classify_small_code, 
--     classify_small_name
-- grouping sets (( channel_code, 
--     channel_name, 
--     business_type_code, 
--     business_type_name, 
--     classify_large_code, 
--     classify_large_name, 
--     classify_middle_code, 
--     classify_middle_name, 
--     classify_small_code, 
--     classify_small_name),
--         ( channel_code, 
--     channel_name, 
--     business_type_code, 
--     business_type_name, 
--     classify_large_code, 
--     classify_large_name, 
--     classify_middle_code, 
--     classify_middle_name),  -- 业务中类合计
--         ( channel_code, 
--     channel_name, 
--     classify_large_code, 
--     classify_large_name, 
--     classify_middle_code, 
--     classify_middle_name, 
--     classify_small_code, 
--     classify_small_name),  -- 渠道三级分类
--         ( channel_code, 
--     channel_name, 
--     classify_large_code, 
--     classify_large_name, 
--     classify_middle_code, 
--     classify_middle_name),  --渠道+二级分类合计
--         ( 
--     classify_large_code, 
--     classify_large_name, 
--     classify_middle_code, 
--     classify_middle_name, 
--     classify_small_code, 
--     classify_small_name),   --三级分类汇总
--         (
--     classify_large_code, 
--     classify_large_name, 
--     classify_middle_code, 
--     classify_middle_name),  --二级分类汇总
--         ( channel_code, 
--     channel_name, 
--     business_type_code, 
--     business_type_name, 
--     classify_large_code, 
--     classify_large_name),  -- 一级分类汇总
--          (channel_code,
--         channel_name , classify_large_code, 
--     classify_large_name),())
-- ) a ;




--销售汇总
 drop table if exists temp_classify_sales;
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
     where  joint_purchase_flag =1
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

-- 5.0 销售收入 csx_tmp.ads_fr_r_d_frozen_financial_classify_sales
 insert overwrite table csx_tmp.ads_fr_r_d_frozen_financial_classify_sales partition(months)
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
        sum(adj_no_tax_sum_value) as adj_no_tax_sum_value,
        sum(adj_sum_value) as adj_sum_value,
        sum(no_tax_rebate_out_value) as no_tax_rebate_out_value,    --未税返利支出金额
        sum(rebate_out_value ) as rebate_out_value ,             --含税返利支出金额
        sum(no_tax_rebate_in_value) as no_tax_rebate_in_value,       --未税后台W0AQ收入金额
        sum(rebate_in_value) as rebate_in_value,               --含税后台W0AQ收入金额
        sum(no_tax_profit)-sum(adj_no_tax_sum_value)+(sum(no_tax_rebate_in_value)-sum(no_tax_rebate_out_value)) as no_tax_net_profit,            --综合毛利=定价毛利+调整成本+（后台收入-后台支出）
        sum(profit)-sum(adj_sum_value)+(sum(rebate_in_value)-sum(rebate_out_value)) as net_profit ,          --综合毛利=定价毛利+调整成本+（后台收入-后台支出）
        (sum(no_tax_profit)-sum(adj_no_tax_sum_value)+(sum(no_tax_rebate_in_value)-sum(no_tax_rebate_out_value)))/sum(no_tax_sales) as no_tax_net_profit_rate,
        (sum(profit)-sum(adj_sum_value)+(sum(rebate_in_value)-sum(rebate_out_value)))/sum(no_tax_sales) as net_profit_rate,
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
        0 adj_no_tax_sum_value,
        0 adj_sum_value,
        0 no_tax_rebate_out_value,    --未税返利支出金额
        0 rebate_out_value ,             --含税返利支出金额
        no_tax_back_amt as no_tax_rebate_in_value,       --未税W0AQ后台收入
        back_total_amt as  rebate_in_value               --含税W0AQ后台收入
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
        coalesce(adj_no_tax_sum_value,0) as adj_no_tax_sum_value,
        coalesce(adj_sum_value,0) adj_sum_value,
        0 no_tax_rebate_out_value,    --未税返利支出金额
        0 rebate_out_value ,             --含税返利支出金额
        0 no_tax_rebate_in_value,       --未税返利收入金额
        0 rebate_in_value     
    from  csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost 
    where months=substr( ${hiveconf:edt},1,6)
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
        
        
        
   -- 公司间销售  csx_tmp.ads_fr_r_d_frozen_direct_sales 2116公司调拨
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
FROM csx_dw.report_sale_r_m_company_pricing a 
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


-- 采购销售入库成本
drop table  csx_tmp.temp_frozen_purch_amt;
create table csx_tmp.temp_frozen_purch_amt as 
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
    coalesce(no_tax_return_amt,0)no_tax_return_amt
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

drop table  csx_tmp.temp_frozen_purch_amt;
create table csx_tmp.temp_frozen_purch_amt as 
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
    coalesce(no_tax_return_amt,0)no_tax_return_amt
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