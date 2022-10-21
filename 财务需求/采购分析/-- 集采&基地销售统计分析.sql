-- 集采&基地销售统计分析

create table csx_analyse.csx_analyse_fr_fina_central_base_analysis_di (
region_code	string	comment		'大区编码'	,
region_name	string	comment		'大区名称'	,
province_code	string	comment		'省区编码'	,
province_name	string	comment		'省区名称'	,
city_group_code	string	comment		'城市编码'	,
city_group_name	string	comment		'城市名称'	,
 
business_division_code	string	comment		'部类分类编码'	,
business_division_name	string	comment		'部类分类名称'	,
division_code	string	comment		'部类编码'	,
division_name	string	comment		'部类名称'	,
classify_large_code	string	comment		'管理大类编码'	,
classify_large_name	string	comment		'管理大类名称'	,
classify_middle_code	string	comment		'管理中类编码'	,
classify_middle_name	string	comment		'管理中类名称'	,
classify_small_code	string	comment		'管理小类编码'	,
classify_small_name	string	comment		'管理小类名称'	,
short_name	string	comment		'集采分类简称'	,
central_pursh_tag	string	comment		'是否集采 1'	,
base_pursh_tag	string	comment		'是否基地采购1'	,
sale_amt_no_tax	decimal(30,6)	comment		'未税销售额'	,
sale_amt	decimal(30,6)	comment		'含税销售额'	,
profit	decimal(30,6)	comment		'含税毛利额'	,
profit_no_tax	decimal(30,6)	comment		'未税毛利额'	,
central_pursh_cost_amt	decimal(30,6)	comment		'集采销售成本'	,
central_pursh_cost_amt_no_tax	decimal(30,6)	comment		'未税集采销售成本'	,
central_pursh_profit	decimal(30,6)	comment		'集采毛利额'	,
central_pursh_profit_no_tax	decimal(30,6)	comment		'未税集采毛利额'	,
central_pursh_sale_amt	decimal(30,6)	comment		'集采销售额'	,
central_pursh_sale_amt_no_tax	decimal(30,6)	comment		'未税集采销售额'	,
base_pursh_cost_amt	decimal(30,6)	comment		'基地销售成本'	,
base_pursh_cost_amt_no_tax	decimal(30,6)	comment		'未税基地销售成本'	,
base_pursh_profit	decimal(30,6)	comment		'基地毛利额'	,
base_pursh_profit_no_tax	decimal(30,6)	comment		'未税基地毛利额'	,
base_pursh_sale_amt	decimal(30,6)	comment		'基地销售额'	,
base_pursh_sale_amt_no_tax	decimal(30,6)	comment		'未税基地销售额'	,
update_time timestamp comment '更新时间',
sale_month string comment '统计月份',
sale_year string comment '年统计',
sale_sdt string comment '日'
) comment '财务采购集采&基地统计分析'
partitioned by (sdt string comment'日分区')
;

-- 商品销售追溯采购入库
-- 大区处理 增加低毛利DC 标识,关联供应链仓信息
drop table csx_analyse_tmp.csx_analyse_tmp_dc_new_01 ;
create  TABLE csx_analyse_tmp.csx_analyse_tmp_dc_new_01 as 
select case when performance_region_code!='10' then '大区'else '平台' end dept_name,
    purchase_org,
    purchase_org_name,
    belong_region_code  region_code,
    belong_region_name  region_name,
    shop_code ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    basic_performance_city_code as performance_city_code,
    basic_performance_city_name as performance_city_name,
    basic_performance_province_code as performance_province_code,
    basic_performance_province_name as performance_province_name,
    case when c.dc_code is not null then '1' else '0' end as is_purchase_dc ,
    enable_date,
    shop_low_profit_flag
from csx_dim.csx_dim_shop a 
 left join 
 (select belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name
  from csx_dim.csx_dim_basic_performance_attribution) b on a.basic_performance_city_code= b.performance_city_code
 left join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.shop_code=c.dc_code
 where sdt='current'    
    ;

-- 销售指定供应链仓
drop table   csx_analyse_tmp.csx_analyse_tmp_sale_group_detail_01 ;
create table csx_analyse_tmp.csx_analyse_tmp_sale_group_detail_01 as
select
  sdt,
  split(id, '&') [ 0 ] as credential_no,
  order_code,
  region_code,
  region_name,
  b.performance_province_code province_code,
  b.performance_province_name province_name,
  b.performance_city_code city_group_code,
  b.performance_city_name city_group_name,
  business_type_name,
  inventory_dc_code as dc_code,
  shop_name as dc_name,
  customer_code,
  customer_name,
  a.goods_code,
  goods_name,
  sale_qty,
  sale_amt,
  sale_amt_no_tax,
  sale_cost_no_tax,
  profit_no_tax,
  sale_cost,
  profit,
  cost_price,
  sale_price,
  c.business_division_code,
  c.business_division_name,
  c.division_code,
  c.division_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_small_code,
  c.classify_small_name,
  c.short_name,
  c.start_date,
  c.end_date,
  case when enable_date<=sdt  and is_purchase_dc=1 then 1 else 0 end is_purchase_dc,
  case
    when c.classify_small_code is not null
    and start_date <= sdt
    and end_date >= sdt 
    then 1    
    else 0
  end as central_pursh_class_tag
from
  csx_dws.csx_dws_sale_detail_di a
  left join csx_analyse_tmp.csx_analyse_tmp_dc_new_01 b on a.inventory_dc_code = b.shop_code
  left join
  (SELECT goods_code,
          tax_rate/100 product_tax_rate,
          business_division_code,
          business_division_name,
          division_code,
          division_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          a.classify_small_code,
          classify_small_name,
          short_name,
          start_date,
          end_date
   FROM  csx_dim.csx_dim_basic_goods a 
   left join 
   (
    select
      short_name,
      classify_small_code,
      start_date,
      end_date
    from
      csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
  ) b on a.classify_small_code=b.classify_small_code 
   WHERE sdt='current') c on a.goods_code = c.goods_code
where
  sdt >= '20220901'
  and sdt <= '20220930' 
  -- and is_purchase_dc=1
  and channel_code not in ('2', '4', '6', '5')
  and business_type_code = '1' -- 日配业务
  and b.shop_low_profit_flag = 0
  -- and refund_order_flag = 0 
  -- and order_channel_code <>'4'  -- 不含返利
  -- and division_code in('11', '10', '12', '13')
  ;

drop table csx_analyse_tmp.csx_analyse_tmp_sale_group_detail_02;
create table csx_analyse_tmp.csx_analyse_tmp_sale_group_detail_02 as  
select
    sdt,
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  order_code,
  batch_no,
  meta_batch_no,
  product_code,
  c.business_division_code,
  c.business_division_name,
  c.division_code,
  c.division_name,
  c.classify_large_code,
  c.classify_large_name,
  c.classify_middle_code,
  c.classify_middle_name,
  c.classify_small_code,
  c.classify_small_name,
  c.short_name,
  c.start_date,
  c.end_date,
  purchase_order_type,  
  goods_shipped_type,   
  case when enable_date<=sdt  and is_purchase_dc=1 then 1 else 0 end is_purchase_dc,
  case
    when c.classify_small_code is not null
    and start_date <= sdt
    and end_date >= sdt then 1
    else 0
  end as central_pursh_class_tag,
  product_cost_amt,       
  product_cost_amt_no_tax,
  product_profit     ,    
  product_profit_no_tax  ,
  product_sale_amt       ,
  product_sale_amt_no_tax
 from 
  (
select
  sdt,
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  order_code,
  batch_no,
  meta_batch_no,
  product_code,
  purchase_order_type,              -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
  goods_shipped_type,               -- 商品出库类型1 A进A出 2工厂加工 3其他
  max(product_cost_amt)product_cost_amt,
  max(product_cost_amt_no_tax) product_cost_amt_no_tax,
  max(product_profit) product_profit,
  max(product_profit_no_tax) product_profit_no_tax,
  max(product_sale_amt ) product_sale_amt,
  max(product_sale_amt_no_tax) product_sale_amt_no_tax
from
   csx_analyse_tmp.csx_analyse_fr_fina_goods_sale_trace_po_di a 
   where sdt>='20220901' and sdt <='20220930'
    group by 
      receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  order_code,
  batch_no,
  meta_batch_no,
  product_code  ,
  purchase_order_type,
  goods_shipped_type,
  sdt
  ) a
 left join csx_analyse_tmp.csx_analyse_tmp_dc_new_01 b on a.receive_dc_code = b.shop_code
  left join
  (SELECT goods_code,
          tax_rate/100 product_tax_rate,
          business_division_code,
          business_division_name,
          division_code,
          division_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          a.classify_small_code,
          classify_small_name,
          short_name,
          start_date,
          end_date
   FROM  csx_dim.csx_dim_basic_goods a 
   left join 
   (
    select
      short_name,
      classify_small_code,
      start_date,
      end_date
    from
      csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
  ) b on a.classify_small_code=b.classify_small_code 
   WHERE sdt='current') c on a.product_code = c.goods_code
 

;
 
drop table csx_analyse_tmp.csx_analyse_tmp_sale_group_detail_03;
create table csx_analyse_tmp.csx_analyse_tmp_sale_group_detail_03 as   
select
    sdt,
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  business_division_code,
  business_division_name,
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  coalesce(short_name,'')short_name,
  if(purchase_order_type=1,1,'') as central_pursh_tag,
  if(purchase_order_type=2,1,'') as base_pursh_tag,
  coalesce(sum(case when purchase_order_type=1 then product_cost_amt        end ),0)  central_pursh_cost_amt,
  coalesce(sum(case when purchase_order_type=1 then product_cost_amt_no_tax end ),0)  central_pursh_cost_amt_no_tax,
  coalesce(sum(case when purchase_order_type=1 then product_profit          end ),0)  central_pursh_profit,
  coalesce(sum(case when purchase_order_type=1 then product_profit_no_tax   end ),0)  central_pursh_profit_no_tax,
  coalesce(sum(case when purchase_order_type=1 then product_sale_amt        end ),0)  central_pursh_sale_amt,
  coalesce(sum(case when purchase_order_type=1 then product_sale_amt_no_tax end ),0)  central_pursh_sale_amt_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_cost_amt        end ),0)  base_pursh_cost_amt,
  coalesce(sum(case when purchase_order_type=2 then product_cost_amt_no_tax end ),0)  base_pursh_cost_amt_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_profit          end ),0)  base_pursh_profit,
  coalesce(sum(case when purchase_order_type=2 then product_profit_no_tax   end ),0)  base_pursh_profit_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_sale_amt        end ),0)  base_pursh_sale_amt,
  coalesce(sum(case when purchase_order_type=2 then product_sale_amt_no_tax end ),0)  base_pursh_sale_amt_no_tax
from  csx_analyse_tmp.csx_analyse_tmp_sale_group_detail_02  a
group by  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  business_division_code,
  business_division_name,
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  short_name,
  if(purchase_order_type=1,1,''),
  if(purchase_order_type=2,1,''),
  sdt


;

create table csx_analyse_tmp.csx_analyse_tmp_fr_fina_central_base_analysis as 
select
   a.sdt,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  a.business_division_code,
  a.business_division_name,
  a.division_code,
  a.division_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  coalesce(a.short_name,'') short_name, 
  coalesce(central_pursh_tag,'')central_pursh_tag,
  coalesce(base_pursh_tag,'')base_pursh_tag,
  (sale_amt_no_tax) sale_amt_no_tax,
  (sale_amt) sale_amt,
  (profit) profit,
  (profit_no_tax) profit_no_tax,
  central_pursh_cost_amt,
  central_pursh_cost_amt_no_tax,
  central_pursh_profit,
  central_pursh_profit_no_tax,
  central_pursh_sale_amt,
  central_pursh_sale_amt_no_tax,
  base_pursh_cost_amt,
  base_pursh_cost_amt_no_tax,
  base_pursh_profit,
  base_pursh_profit_no_tax,
  base_pursh_sale_amt,
  base_pursh_sale_amt_no_tax,
  current_timestamp() update_time,
  substr(sdt,1,6) sale_month,
  substr(sdt,1,4) sale_year
from
(select
    sdt,
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,
  a.business_division_code,
  a.business_division_name,
  a.division_code,
  a.division_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  a.short_name,
  sum(sale_amt_no_tax) sale_amt_no_tax,
  sum(sale_amt) sale_amt,
  sum(profit) profit,
  sum(profit_no_tax) profit_no_tax
from
  csx_analyse_tmp.csx_analyse_tmp_sale_group_detal_01 a 
  group by 
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,
  a.business_division_code,
  a.business_division_name,
  a.division_code,
  a.division_name,
  a.classify_large_code,
  a.classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.classify_small_code,
  a.classify_small_name,
  a.short_name,
  sdt
  ) a 
  left join  csx_analyse_tmp.csx_analyse_tmp_sale_group_detail_03  b on a.classify_small_code=b.classify_small_code 
    and a.city_group_code=b.city_group_code
    and a.sdt=b.sdt
    and a.province_code=b.province_code
    and a.region_code=b.region_code
  ;



select * from  csx_analyse_tmp.csx_analyse_tmp_fr_fina_central_base_analysis;