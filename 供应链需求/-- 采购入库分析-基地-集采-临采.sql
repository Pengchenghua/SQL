-- 采购入库分析-基地-集采-临采
select   belong_region_code  ,
  belong_region_name  ,
  basic_performance_province_code ,
  basic_performance_province_name ,
  a.goods_code , 
  bar_code   ,
  goods_name , 
  unit_name , 
  brand_name , 
  a.classify_large_code , 
  a.classify_large_name , 
  a.classify_middle_code , 
  a.classify_middle_name ,
  supplier_code, 
  supplier_name, 
  sum(receive_qty ) receive_qty , 
  sum(receive_amt) receive_amt ,
  sum(receive_amt)/sum(receive_qty) avg_cost,
  sum(case when a.order_business_type_name='是' then receive_qty end ) as jd_qty,
  sum(case when a.order_business_type_name='是' then receive_amt end ) jd_amt,
  sum(case when a.is_central_tag='1'  then receive_qty end ) as jc_qty,
  sum(case when a.is_central_tag='1'  then receive_amt end ) jc_amt,
  sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) then receive_qty end ) as lc_qty,
  sum(case when a. source_type_name  in('临时地采','临时加单','客户直送','紧急采购' ) then receive_amt end ) lc_amt,
  sum(case when a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name !='是' then receive_qty end ) as qt_qty,
  sum(case when a. source_type_name not in('临时地采','临时加单','客户直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name!='是' then receive_amt end ) qt_amt,
  if(b.goods_code is null ,'否','是') as is_jd,
  if(c.goods_code is null ,'否','是') as is_jc,
  if(d.goods_code is null ,'否','是') as is_lc
  --  rank_aa
 from   csx_analyse_tmp.csx_analyse_tmp_entry_goods  a 
left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where order_business_type_name='是' ) b on a.goods_code=b.goods_code  -- 基地标识
left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where is_central_tag='1' ) c on a.goods_code=c.goods_code  -- 集采标识 
left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where source_type_name  in('临时地采','临时加单','客户直送','紧急采购' )) d on a.goods_code=d.goods_code  -- 临采标识 

-- join csx_analyse_tmp.csx_analyse_tmp_goods_top_20 b on a.goods_code=b.goods_code
where source_type_name not in ('城市服务商','联营直送','项目合伙人')
    and is_supplier_dc='是'
group by 
-- rank_aa,
  belong_region_code  ,
  belong_region_name  ,
  basic_performance_province_code ,
  basic_performance_province_name ,
  a.goods_code , 
  bar_code   ,
  goods_name , 
  unit_name , 
  brand_name , 
  a.classify_large_code , 
  a.classify_large_name , 
  a.classify_middle_code , 
  a.classify_middle_name,
  supplier_code, 
  supplier_name, 
  if(b.goods_code is null ,'否','是'),
if(c.goods_code is null ,'否','是'),
if(d.goods_code is null ,'否','是');

drop table csx_analyse_tmp.csx_analyse_tmp_goods_top_20 ;
create table csx_analyse_tmp.csx_analyse_tmp_goods_top_20 as 
select
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  goods_code,
  sale_amt,
  profit,
   rank_aa
  from (select
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  goods_code,
  sale_amt,
  profit,
  dense_rank()over(partition by classify_middle_name order by sale_amt desc  ) rank_aa
  from (
select
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  goods_code,
  sum(sale_amt) sale_amt,
  sum(profit) profit
from
    csx_dws.csx_dws_sale_detail_di a 
    join 
    (select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop where sdt='current') b on a.inventory_dc_code=b.shop_code
where
  sdt >= '20230401'
  and sdt <= '20230430'
  and business_type_code=1
  and shop_low_profit_flag=0
  group by classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  goods_code
  ) a 
  ) a 
  -- where rank_aa<21
  ;


create table csx_analyse_tmp.csx_analyse_tmp_entry_goods as 
select 
  receive_sdt  , 
  purchase_org_code , 
  purchase_org_name  , 
  purchase_order_code  ,
  order_code ,
  batch_code  ,
  belong_region_code  ,
  belong_region_name  ,
  province_code  , 
  province_name  , 
  city_code ,
  city_name ,
  basic_performance_province_code ,
  basic_performance_province_name ,
  basic_performance_city_code ,
  basic_performance_city_name ,
  source_type_code , 
  source_type_name , 
  super_class_code ,
  super_class_name , 
  a.dc_code ,  
  dc_name , 
  goods_code , 
  bar_code   ,
  goods_name , 
  unit_name , 
  brand_name , 
  division_code , 
  division_name , 
  classify_large_code , 
  classify_large_name , 
  classify_middle_code , 
  classify_middle_name , 
  classify_small_code , 
  classify_small_name , 
  supplier_code, 
  supplier_name, 
  local_purchase_flag , 
  business_type_name , 
  receive_qty , 
  receive_amt , 
  receive_sdt , 
  order_create_date ,
  if(order_business_type='1','是','否') as order_business_type_name, 
  case when order_type='0' then '普通供应商订单'
    when order_type='1' then '国货订单'
    when order_type='2' then '日采订单'
    when order_type='3' then '计划订单'
    else '其他' end as order_type_name,
  case when supplier_classify_code='0' then '基础供应商'
    when supplier_classify_code='1' then '农户供应商'
    when supplier_classify_code='2' then '现金采买' else '其他' end supplier_classify_name,
  purpose  , 
  purpose_name  ,
  if(b.dc_code is not null,'是','否') is_supplier_dc,
  central_purchase_short_name ,
  is_central_tag ,
  link_order_code ,
  remedy_order_code	 		,
  original_purchase_order_code		
  positive_purchase_order_code	,
  reverse_purchase_order_code	,
   remedy_flag
  from csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
  LEFT JOIN 
  (select dc_code,enable_time from csx_dim.csx_dim_csx_data_market_conf_supplychain_location where sdt='current')b on a.dc_code=b.dc_code
   left join 
  (select shop_code,basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name ,belong_region_code,belong_region_name
    from csx_dim.csx_dim_shop  a 
    left join 
 (select distinct belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name
  from csx_dim.csx_dim_basic_performance_attribution) b on a.basic_performance_city_code= b.performance_city_code
    where sdt='current') c on a.dc_code=c.shop_code
  where 
   sdt>= '20230401' 
   and sdt<='20230430'
   and remedy_flag!='1'

 
