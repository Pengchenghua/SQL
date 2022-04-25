
-- 本月第一天，上月第一天，上上月第一天
-- set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
-- set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
-- set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);
set edate = '${enddate}';
set edt =regexp_replace(${hiveconf:edate},'-','');  --作为日期分区
set sdate=trunc(${hiveconf:edate},'MM');

-- 本月第一天，上月第一天，上上月第一天
-- set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
-- set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
-- set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:sdate},${hiveconf:edt},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

-- select substr(sdt,1,6),sum(excluding_tax_sales) from csx_dw.dws_sale_r_d_detail where sdt>='20210401' and classify_middle_code in ('B0304','B0305') group by substr(sdt,1,6);
--销售订单所在省区渠道
drop table if exists csx_tmp.temp_cbgb_tz_v10;
create temporary table if not exists csx_tmp.temp_cbgb_tz_v10 
as 
SELECT split(id,'&')[0] as id,
    case when channel_code in ('1','7','9') then '1' else channel_code end channel_code,
    case when channel_code in ('1','7','9') then '大客户' else channel_name end channel_name,
       case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '21'
     when channel_code='2' then '22' else business_type_code end business_type_code,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
     when channel_code='2' then '非代加工' else business_type_name end business_type_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    origin_order_no,
    order_no,
    goods_code,
    sum(sales_value)sales_value,
    sum(excluding_tax_sales)excluding_tax_sales,
    sum(sales_cost) as sales_cost
FROM csx_dw.dws_sale_r_d_detail a 
 join 
   (select goods_id,
	 classify_large_code,
	 classify_large_name,
	 classify_middle_code,
	 classify_middle_name,
	 classify_small_code,
	 classify_small_name
   from csx_dw.dws_basic_w_a_csx_product_m 
      where sdt = 'current'
      and  classify_middle_code in ('B0304','B0305') 
    ) d on a.goods_code=d.goods_id
WHERE sdt>=regexp_replace(add_months(trunc(${hiveconf:edate},'MM'),-6),'-','')
and a.sdt<=regexp_replace(${hiveconf:edate},'-','')
--and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
--and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
GROUP BY case when channel_code in ('1','7','9') then '1' else channel_code end ,
    case when channel_code in ('1','7','9') then '大客户' else channel_name end ,
       case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '21'
     when channel_code='2' then '22' else business_type_code end ,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
     when channel_code='2' then '非代加工' else business_type_name end ,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    origin_order_no,
    order_no,
    goods_code,
    split(id,'&')[0];





--1 2 3 4 5 6  成本调整 adjustment_amt_no_tax,adjustment_amt
drop  table if exists csx_tmp.temp_cbgb_tz_v11;
create temporary table if not exists csx_tmp.temp_cbgb_tz_v11 
as 
select 
    coalesce(c.channel_code,'1') as channel_code,
    coalesce(c.channel_name,'大客户') as channel_name,
    coalesce(c.business_type_code,'6')as business_type_code,
    coalesce(c.business_type_name,'BBC') as business_type_name,
    coalesce(c.province_code,b.province_code) as province_code,
    coalesce(c.province_name,b.province_name) as province_name,
	coalesce(c.city_group_code,b.city_code) as city_code,
	coalesce(c.city_group_name,b.city_name) as city_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
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
		(select * 
		    from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment a
		 join 
	    (select goods_id,
	        classify_large_code,
	        classify_large_name,
	        classify_middle_code,
	        classify_middle_name,
	        classify_small_code,
	        classify_small_name
	    from csx_dw.dws_basic_w_a_csx_product_m 
	      where sdt = 'current'
	        and  classify_middle_code in ('B0304','B0305') ) d on a.product_code=d.goods_id
	    	where sdt = '19990101'
	    	and to_date(posting_time) >= ${hiveconf:sdate}
	    	and to_date(posting_time) <= ${hiveconf:edate}
	    )a
)a
left join csx_tmp.temp_cbgb_tz_v10 c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
(select shop_id,sales_province_code province_code,sales_province_name as province_name,city_group_code as city_code,city_group_name as city_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
group by   coalesce(c.channel_code,'1') ,
    coalesce(c.channel_name,'大客户') ,
    coalesce(c.business_type_code,'6'),
    coalesce(c.business_type_name,'BBC'),
    coalesce(c.province_code,b.province_code) ,
    coalesce(c.province_name,b.province_name) ,
	coalesce(c.city_group_code,b.city_code) ,
	coalesce(c.city_group_name,b.city_name) ,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name;


set hive.exec.dynamic.partition.mode=nonstrict;
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
    coalesce(no_tax_sales_value,0)as no_tax_sales_value,
    coalesce(adj_ddfkc_no, 0)+coalesce(adj_cgth_no,0 )+coalesce(adj_gc_xs_no,0 )+coalesce(adj_gc_db_no,0 )+coalesce(adj_gc_qt_no,0 )+coalesce(adj_sg_no,0 )+coalesce(adj_bj_xs_no,0 )+coalesce(adj_bj_db_no,0)+coalesce(adj_bj_qt_no,0 ) as adj_no_tax_sum_value,
    coalesce(adj_ddfkc, 0)+coalesce(adj_cgth,0 )+coalesce(adj_gc_xs,0 )+coalesce(adj_gc_db,0 )+coalesce(adj_gc_qt,0 )+coalesce(adj_sg,0 )+coalesce(adj_bj_xs,0 )+coalesce(adj_bj_db,0)+coalesce(adj_bj_qt,0 ) as adj_sum_value,
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


	
drop table if exists csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost;
create table csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost(
    level_id string comment '层级',
    sales_months string comment '销售月份',
--   `province_code` string COMMENT  '省区编号',
--   `province_name` string COMMENT  '省区',
--   `city_group_code` string COMMENT '城市编号',  
--   `city_group_name` string COMMENT '城市',
  `channel_code` string COMMENT '渠道',
  `channel_name` string COMMENT '渠道',
  `business_type_code` string COMMENT '销售业务类型', 
   business_type_name string comment '销售业务类型',
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
  `adj_sg` decimal(26,6)  COMMENT '手工调整销售成本_含税',
  `adj_bj_xs_no` decimal(26,6) COMMENT '采购入库价格补救-调整销售_未税',
  `adj_bj_xs` decimal(26,6) COMMENT '采购入库价格补救-调整销售_含税',
  `adj_bj_db_no` decimal(26,6) COMMENT '采购入库价格补救-调整跨公司调拨_未税',
  `adj_bj_db` decimal(26,6) COMMENT '采购入库价格补救-调整跨公司调拨_含税',
  `adj_bj_qt_no` decimal(26,6) COMMENT '采购入库价格补救-调整其他_未税',
  `adj_bj_qt` decimal(26,6)  COMMENT '采购入库价格补救-调整其他_含税' ,
  adj_no_tax_sum_value decimal(26,6) comment '总销售成本金额未税',
  adj_sum_value decimal(26,6) comment '总销售成本金额含税',
  no_tax_sales_value decimal(26,6)  COMMENT '未税销售额' ,
  update_time timestamp comment '插入时间'
) COMMENT '财报管报冻品调整成本-销售调整'
PARTITIONED BY (months string COMMENT '月分区')
STORED AS parquet;