CREATE TABLE `csx_analyse.csx_analyse_data_analysis_prd_bj_purchase_customer_tag_w_df` (
  `id` int  ,
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区名称',
  `customer_code` string COMMENT '客户编码',
  `customer_name` string COMMENT '客户名称',
  customer_tag int  comment '客户标签 1 新客、2 项目供应商转自营',
  `create_time` timestamp  COMMENT '创建时间',
  `create_by` string  COMMENT '创建者',
  `update_time` timestamp  COMMENT '更新时间',
  `update_by` string  COMMENT '更新者'
) COMMENT'北京供应链_客户标签维护';




-- data_analysis_prd.data_analysis_prd_purchase_classify_info_w definition

CREATE TABLE `csx_analyse_data_analysis_prd_purchase_classify_info_w_df` (
  `id` string NOT NULL,
  `province_code` string COMMENT '省区编码',
  `province_name` string '省区名称',
  `city_code` string COMMENT '城市编码',
  `city_name` string COMMENT '城市名称',
  `classify_large_code` string COMMENT '管理一级编码',
  `classify_large_name` string COMMENT '管理一级名称',
  `classify_middle_code` string '管理二级编码',
  `classify_middle_name` string '管理二级名称',
  `purchase_name` string '采购人员姓名',
  `purchase_work_no` string '采购人员工号',
  `create_time` timestamp  COMMENT '创建时间',
  `create_by` varchar(64)  COMMENT '创建者',
  `update_time` timestamp  ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_by` varchar(64)  COMMENT '更新者'
) COMMENT'供应链_采购人员信息维护'
stored as arov;




-- 最新版本
 -- drop table csx_analyse_tmp.csx_analyse_tmp_bj_suppliy_sale_sum;
create table csx_analyse_tmp.csx_analyse_tmp_bj_suppliy_sale_sum as 
select performance_province_code,
  performance_province_name,
  performance_city_code,
  purchase_name,
  business_code,
  business_name,
  classify_large_code,
  classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  (yesterday_sale)   yesterday_sale,
  (yesterday_profit)   yesterday_profit,
  (yesterday_adjust_amt) yesterday_adjust_amt,
  (yesterday_new_cust_sale_amt)    yesterday_new_cust_sale_amt,    -- 新客销售额
  (yesterday_new_cust_profit ) yesterday_new_cust_profit,    -- 新客毛利额
  (yesterday_xm_cust_sale_amt) yesterday_xm_cust_sale_amt,    -- 项目供应商转自营销售额
  (yesterday_xm_cust_profit)   yesterday_xm_cust_profit ,   -- 项目供应商转自
  (coalesce(yesterday_sale,0)-coalesce(yesterday_new_cust_sale_amt,0)-coalesce(yesterday_xm_cust_sale_amt,0)) as yesterday_old_sale_amt,
  (coalesce(yesterday_profit,0)-coalesce(yesterday_new_cust_profit,0)-coalesce(yesterday_xm_cust_profit,0)) as yesterday_old_profit,
  (new_cust_sale_amt)    new_cust_sale_amt,    -- 新客销售额
  (new_cust_profit ) new_cust_profit,    -- 新客毛利额
  (xm_cust_sale_amt) xm_cust_sale_amt,    -- 项目供应商转自营销售额
  (xm_cust_profit)   xm_cust_profit,    -- 项目供应商转自
  coalesce(coalesce(sale_amt,0)- coalesce(new_cust_sale_amt,0)-coalesce(xm_cust_sale_amt,0),0) as old_sale_amt,
  coalesce(coalesce(profit,0)- coalesce(new_cust_profit,0)-coalesce(xm_cust_profit,0) ,0) as old_profit,  
  (direct_sale_amt)  direct_sale_amt,
  (direct_profit)    direct_profit,
  (adjust_amt)   adjust_amt,
  (sale_amt) sale_amt,
  (profit)   profit,
  (last_sale_amt)    last_sale_amt,
  (last_profit)  last_profit,
  (adjust_profit)    adjust_profit,
  (yesterday_adjust_profit)  yesterday_adjust_profit
-- sale_amt/sum(sale_amt)over(partition by  performance_province_code  ) as sale_ratio
--  去除调账金额月至今毛利率	月至今直送金额	去除直送月至今毛利率	毛利率差值
 from 

 (select
  a.performance_province_code,
  a.performance_province_name,
  performance_city_code,
  case when classify_large_code in ('B01','B02','B03') then '11' else '12' end      business_code,
  case when classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end business_name,
  a.classify_large_code,
  classify_large_name,
  a.classify_middle_code,
  a.classify_middle_name,
  sum(case when sdt='20230829' then sale_amt end )  as  yesterday_sale,
  sum(case when sdt='20230829' then profit end )  as  yesterday_profit,
  sum(case when sdt='20230829' and order_channel_code=6 then sale_amt end ) as yesterday_adjust_amt,
  sum(case when sdt='20230829' and order_channel_code=6 then profit end ) as yesterday_adjust_profit,
  sum(case when customer_tag=1 and sdt='20230829' then sale_amt end ) yesterday_new_cust_sale_amt,    -- 新客销售额
  sum(case when customer_tag=1 and sdt='20230829' then profit end ) yesterday_new_cust_profit,    -- 新客毛利额
  sum(case when customer_tag=2 and sdt='20230829' then sale_amt end ) yesterday_xm_cust_sale_amt,    -- 项目供应商转自营销售额
  sum(case when customer_tag=2 and sdt='20230829' then profit end ) yesterday_xm_cust_profit,    -- 项目供应商转自
  sum(case when customer_tag=1 and sdt>='20230801' and sdt<='20230829' then sale_amt end ) new_cust_sale_amt,    -- 新客销售额
  sum(case when customer_tag=1 and sdt>='20230801' and sdt<='20230829' then profit end ) new_cust_profit,    -- 新客毛利额
  sum(case when customer_tag=2 and sdt>='20230801' and sdt<='20230829' then sale_amt end ) xm_cust_sale_amt,    -- 项目供应商转自营销售额
  sum(case when customer_tag=2 and sdt>='20230801' and sdt<='20230829' then profit end ) xm_cust_profit,    -- 项目供应商转自
  sum(case when sdt>='20230801' and sdt<='20230829' and  delivery_type_code=2 then sale_amt end ) as direct_sale_amt,
  sum(case when sdt>='20230801' and sdt<='20230829' and  delivery_type_code=2 then profit end ) as direct_profit,
  sum(case when sdt>='20230801' and sdt<='20230829' and  order_channel_code=6 then sale_amt end ) as adjust_amt,
  sum(case when sdt>='20230801' and sdt<='20230829' and  order_channel_code=6 then profit end ) as adjust_profit,
  sum(case when sdt>='20230801' and sdt<='20230829'  then sale_amt end ) sale_amt,
  sum(case when sdt>='20230801' and sdt<='20230829'  then profit end ) profit,
  sum(case when sdt>='20230701' and sdt<='20230729'  then sale_amt end ) last_sale_amt,
  sum(case when sdt>='20230701' and sdt<='20230729'  then profit end ) last_profit
from
       csx_dws.csx_dws_sale_detail_di a 
join 
(select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0 ) b on a.inventory_dc_code=b.shop_code
left join 
(select province_code,customer_code,customer_tag from csx_analyse.csx_analyse_data_analysis_prd_bj_purchase_customer_tag_w_df ) d on a.customer_code=d.customer_code and a.performance_province_code=d.province_code 
where
  sdt >= '20230701' and sdt<='20230829'
  and business_type_code = 1
  and a.performance_province_code = '1'
  group by a.performance_province_code,
  a.performance_province_name,
  a.classify_middle_code,
  classify_middle_name ,
  performance_city_code,
  classify_large_code,
  classify_large_name,
  case when classify_large_code in ('B01','B02','B03') then '11' else '12' end       ,
  case when classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end
  )a 
  left join 
(select province_code,city_code,classify_middle_code,purchase_name from csx_analyse.csx_analyse_data_analysis_prd_purchase_classify_info_w_df)c  
on a.performance_city_code=c.city_code and a.performance_province_code=c.province_code and a.classify_middle_code=c.classify_middle_code



select
  a.performance_province_code,
  a.performance_province_name,
  performance_city_code,
  sum(sale_amt )  as  yesterday_sale,
  sum(case when  order_channel_code=6 then sale_amt end ) as yesterday_adjust_amt,
  sum(case when customer_tag=1  then sale_amt end ) yesterday_new_cust_sale_amt,    -- 新客销售额
  sum(case when customer_tag=2  then sale_amt end ) yesterday_xm_cust_sale_amt,    -- 项目供应商转自营销售额
  from
       csx_dws.csx_dws_sale_detail_di a 
join 
(select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0 ) b on a.inventory_dc_code=b.shop_code
where
  sdt = '20230829' 
  and business_type_code = 1
  and a.performance_province_code = '1'
  group by a.performance_province_code,
  a.performance_province_name,
  performance_city_code
  
  ;

-- 表头文字描述 
  create table csx_analyse_tmp.csx_analyse_tmp_fr_bj_sale_customer_rank as 
select a.performance_province_code,
  a.performance_province_name,
  performance_city_code,
  concat('(总客户数',cast(count(case when old_sale_amt>0 then customer_code end ) as string),'家,实际履约配送',cast(count(case when yesterday_old_sale_amt>0 then customer_code end ) as string),'家)') old_cunt_rank,
  concat('(总客户数',cast(count(case when customer_tag=1  then customer_code end ) as string),'家,实际履约配送',cast(count(case when yesterday_new_cust_sale_amt>0 then customer_code end ) as string),'家)') as new_cunt_rank,
 concat('(总客户数',cast(count(case when customer_tag=2  then customer_code end ) as string),'家,实际履约配送',cast(count(case when yesterday_xm_cust_sale_amt>0 then customer_code end ) as string),'家)') as xm_cunt_rank
--   count(case when customer_tag=1  then customer_code end ) new_cunt,
--   count(case when customer_tag=2 then customer_code end ) xm_cunt ,
--   count(case when yesterday_old_sale_amt>0 then customer_code end ) yesterday_old_cunt,
--   count(case when yesterday_new_cust_sale_amt>0 then customer_code end ) yesterday_new_cunt,
--   count(case when yesterday_xm_cust_sale_amt>0 then customer_code end ) yesterday_xm_cunt
  from (
select
  a.performance_province_code,
  a.performance_province_name,
  performance_city_code,
  a.customer_code,
  coalesce(customer_tag,0) customer_tag,
  sum(case when sdt='20230829' then sale_amt end )  as  yesterday_sale,
  coalesce(sum(case when customer_tag is null and sdt='20230829' then sale_amt end ),0) yesterday_old_sale_amt,
  coalesce(sum(case when customer_tag=1 and sdt='20230829' then sale_amt end),0) yesterday_new_cust_sale_amt,    -- 新客销售额
  coalesce(sum(case when customer_tag=2 and sdt='20230829' then sale_amt end),0) yesterday_xm_cust_sale_amt,   -- 项目供应商转自营销售额
   sum( sale_amt  )  as  sale_amt,
  coalesce(sum(case when customer_tag is null then sale_amt end ),0) old_sale_amt,
  coalesce(sum(case when customer_tag=1  then sale_amt end),0) new_cust_sale_amt,    -- 新客销售额
  coalesce(sum(case when customer_tag=2  then sale_amt end),0)  xm_cust_sale_amt   -- 项目供应商转自营销售额
  from
       csx_dws.csx_dws_sale_detail_di a 
join 
(select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0 ) b on a.inventory_dc_code=b.shop_code
left join 
(select province_code,customer_code,customer_tag from csx_analyse.csx_analyse_data_analysis_prd_bj_purchase_customer_tag_w_df ) d on a.customer_code=d.customer_code and a.performance_province_code=d.province_code 

where
  sdt >= '20230701'    
  and business_type_code = 1
  and a.performance_province_code = '1'
  group by a.performance_province_code,
  a.performance_province_name,
  performance_city_code,
  a.customer_code,
   coalesce(customer_tag,0)
   ) a 
   group by  a.performance_province_code,
  a.performance_province_name,
  performance_city_code
  ;

  --表结构
create table data_analysis_prd.report_csx_analysis_fr_bj_sale_days_sum (
  id int not null auto_increment primary key ,
  sale_month varchar(64) not null comment '销售月份',
  performance_province_code	varchar(64) comment '省区编码',	
  performance_province_name	varchar(64) comment '省区名称',	
  performance_city_code	varchar(64) comment '城市编码',	
  purchase_name	varchar(64 ) comment	'采购负责人',
  business_code	varchar(64 ) comment	'业务部类编码',
  business_name	varchar(64 ) comment	'业务部类名称',
  classify_large_code	varchar(64) comment '管理大类编码',	
  classify_large_name	varchar(64) comment '管理大类名称',	
  classify_middle_code	varchar(64) comment '管理中类编码',
  classify_middle_name	varchar(64) comment '管理中类名称',	
  yesterday_sale	decimal(30,6)	comment '昨日销售额',
  yesterday_profit	decimal(30,6)	comment '昨日毛利额',
  yesterday_adjust_amt	decimal(30,6)	comment '昨日调价金额 ',
  yesterday_new_cust_sale_amt	decimal(30,6)	comment '昨日新客销售额',
  yesterday_new_cust_profit	decimal(30,6)	 comment '昨日新客毛利额',
  yesterday_xm_cust_sale_amt	decimal(30,6)	comment '昨日项目销售额',
  yesterday_xm_cust_profit	decimal(30,6)	comment '昨日项目毛利',
  yesterday_old_sale_amt	decimal(32,6)	comment '昨日老客销售',
  yesterday_old_profit	decimal(32,6)	comment '昨日老客毛利额',
  new_cust_sale_amt	decimal(30,6)	comment '月至今新客销售额',
  new_cust_profit	decimal(30,6)	comment '月至今新客毛利额',
  xm_cust_sale_amt	decimal(30,6)	comment '月至今项目销售额',
  xm_cust_profit	decimal(30,6)	comment '月至今项目毛利额',
  old_sale_amt	decimal(32,6)	comment '月至今老客销售额',
  old_profit	decimal(32,6)	comment '月至今老客毛利额',
  direct_sale_amt	decimal(30,6)	comment '月至今直送销售额',
  direct_profit	decimal(30,6)	comment '月至今直送毛利额',
  adjust_amt	decimal(30,6)	comment '月至今调价金额',
  sale_amt	decimal(30,6)	comment'月至今销售额',
  profit	decimal(30,6)	 comment '月至今毛利额',
  last_sale_amt	decimal(30,6)	comment '上月环期销售额',
  last_profit	decimal(30,6)	comment '上月环期毛利额',
  adjust_profit	decimal(30,6)	comment '月至今调价毛利额',
  yesterday_adjust_profit	decimal(30,6)	 comment '昨日调价毛利额',
  update_time timestamp comment '数据更新时间',
  key index_prov(performance_province_name),
  key index_month(sale_month)
)COMMENT='北京供应链销售日报'

;



create table data_analysis_prd.csx_analyse_tmp_bj_suppliy_sale_sum_customer_rank (
  id int not null auto_increment primary key ,
  sale_month varchar(64) not null comment '销售月份',
  performance_province_code	varchar(64) comment '省区编码',	
  performance_province_name	varchar(64) comment '省区名称',	
  performance_city_code	varchar(64) comment '城市编码',	
  old_cunt_rank	varchar(128 ) comment	'老客表头',
  new_cunt_rank	varchar(128 ) comment	'新客表头',
  xm_cunt_rank	varchar(128 ) comment	'项目表头',
  update_time timestamp comment '数据更新时间',
  key index_prov(performance_province_name),
  key index_month(sale_month)
)COMMENT='北京供应链销售日报表头说明'


;

-- 负毛利结构 

with aa as (
select
  a.performance_province_code,
  a.performance_province_name,
  performance_city_code,
  TYPE_NAME,
  goods_code,
  goods_name,
  unit_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  sale_qty,
  sale_amt,
  profit,
  no_direct_sale,
  no_direct_profit ,
  rank()over(partition by TYPE_NAME order by profit asc ) rank
  from
  (select
  a.performance_province_code,
  a.performance_province_name,
  performance_city_code,
  goods_code,
  goods_name,
  unit_name,
  case when classify_middle_code in ('B0101','B0102','B0103') then '干货(米、蛋、干)' when classify_small_code in('B030302','B030303') then '活鲜'
    when classify_small_code not  in('B030302','B030303') and classify_middle_code='B0303' THEN '水产冰鲜冷冻'
    ELSE  classify_middle_name END TYPE_NAME,

  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  sum(sale_qty) sale_qty,
  sum(sale_amt )  as  sale_amt,
  sum(profit) profit,
  sum(case when  order_channel_code=6 and delivery_type_code=2 and refund_order_flag=1 then sale_amt end ) as no_direct_sale,
  sum(case when  order_channel_code=6 and delivery_type_code=2 and refund_order_flag=1 then profit end ) as no_direct_profit 
  from
         csx_dws.csx_dws_sale_detail_di a 
join 
(select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0 ) b on a.inventory_dc_code=b.shop_code
where
  sdt = '20230829' 
  and business_type_code = 1
  and a.performance_province_code = '1'
  group by a.performance_province_code,
  a.performance_province_name,
  a.classify_middle_code,
  classify_middle_name ,
  performance_city_code,
  classify_large_code,
  classify_large_name,
  goods_code,
  goods_name,  unit_name,
    case when classify_middle_code in ('B0101','B0102','B0103') then '干货(米、蛋、干)' when classify_small_code in('B030302','B030303') then '活鲜'
    when classify_small_code not  in('B030302','B030303') and classify_middle_code='B0303' THEN '水产冰鲜冷冻'
    ELSE  classify_middle_name END
  ) a where profit <0
 
  )
  select * from aa 
  where 1=1 
   and ((type_name in ('熟食加工','水果','蔬菜','家禽','猪肉','活鲜贝类','水产冰鲜冷冻','调理预制品','牛羊','酒','调味品类') and rank<21 
        and  (type_name='干货(米、蛋、干)' and rank<31)
        and rank<16
        )
  
  