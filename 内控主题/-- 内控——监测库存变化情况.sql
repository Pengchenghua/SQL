-- 内控——监测库存变化情况
-- 取库存数据
create table csx_analyse_tmp.csx_analyse_tmp_dc_stock_01 as 
select
  basic_performance_province_code province_code,
  basic_performance_province_name province_name,
  basic_performance_city_code city_code,
  basic_performance_city_name city_name,
  dc_code,
  dc_name,
  goods_code,
  goods_name,
  purchase_group_code,
  purchase_group_name,
  company_code,
  company_name,
  sum(qty) qty,
  sum(amt) amt,
  sum(amt_no_tax) amt_no_tax
from
  csx_dws.csx_dws_cas_accounting_stock_m_df a 
  join 
  (select shop_code,
         shop_name,
         basic_performance_province_code,
         basic_performance_province_name,
         basic_performance_city_code,
         basic_performance_city_name,
         purpose,
         purpose_name
  from  csx_dim.csx_dim_shop 
    where sdt='current'
        and purpose in('01')
    ) b on a.dc_code=b.shop_code
where
  sdt = '20221020'
  and division_code in ('10','12','11','13','14')
  and substr(reservoir_area_code,1,2) in ('BZ','TH')
 group by 
  basic_performance_province_code ,
  basic_performance_province_name ,
  basic_performance_city_code ,
  basic_performance_city_name,dc_code,
  dc_name,
  goods_code,
  goods_name,
  purchase_group_code,
  purchase_group_name,
  company_code,
  company_name
  ;
  
  
 drop table  csx_analyse_tmp.csx_analyse_tmp_dc_stock_02;
 create table   csx_analyse_tmp.csx_analyse_tmp_dc_stock_02 as 
  select inventory_dc_code,
    goods_code ,
    sum(sale_amt)/90 all_sale_amt,
    sum(sale_qty)/90 all_sale_qty,
    sum(case when sdt>='20220920' then  sale_amt end )/30 sale_amt_30d,
    sum(case when sdt>='20220920' then  sale_qty end )/30 sale_qty_30d
  from csx_dws.csx_dws_sale_detail_di
  where sdt<='20221020'
    and sdt>='20220720'
  group by inventory_dc_code,
    goods_code
  ;
  
create table csx_analyse_tmp.csx_analyse_fr_internal_abnormal_stock_di as 

 select a.*,
    coalesce(b.all_sale_amt,0) all_sale_amt,
    coalesce(all_sale_qty  ,0) all_sale_qty , 
    coalesce(sale_amt_30d  ,0) sale_amt_30d , 
    coalesce(sale_qty_30d  ,0) sale_qty_30d 
 from csx_analyse_tmp.csx_analyse_tmp_dc_stock_01 a 
 left join csx_analyse_tmp.csx_analyse_tmp_dc_stock_02 b on a.dc_code=b.inventory_dc_code and a.goods_code=b.goods_code
 where a.qty>0 



 ;

 CREATE TABLE `csx_analyse`.`csx_analyse_fr_internal_abnormal_stock_mi`(
     sale_month string comment '月份',
	`province_code` string COMMENT '省区编码',
	`province_name` string COMMENT '省区名称',
	`city_code` string COMMENT '城市编码',
	`city_name` string COMMENT '城市名称',
	`dc_code` string COMMENT 'DC编码',
	`dc_name` string COMMENT 'DC名称',
     purpose string COMMENT 'DC用途编码',
	 purpose_name string COMMENT 'DC用途名称',
	`goods_code` string COMMENT '商品编码',
	`goods_name` string COMMENT '商品名称',
	`purchase_group_code` string COMMENT '课组编码',
	`purchase_group_name` string COMMENT '课组名称',
	`company_code` string COMMENT '公司编码',
	`company_name` string COMMENT '公司名称',
	`qty` decimal(24,3) COMMENT '库存量',
	`amt` decimal(26,4) COMMENT '含税库存额',
	`amt_no_tax` decimal(26,4) COMMENT '未税库存额',
	`all_sale_amt` decimal(33,9) COMMENT '近90天销售额',
	`all_sale_qty` decimal(33,9) comment '近90天销售量',
	`sale_amt_30d` decimal(33,9) comment '近30天销售额',
	`sale_qty_30d` decimal(33,9) comment '近30天销售量',
     receive_qty_90d decimal(30,6) comment '近90天领用量',
     receive_amt_90d decimal(30,6) comment '近90天领用金额',
     receive_qty_30d decimal(30,6) comment '近30天领用量',
     receive_amt_30d decimal(30,6) comment '近30天领用金额',
     material_qty_90d decimal(30,6) comment '近90天原料量',
     material_amt_90d decimal(30,6) comment '近90天原料金额',
     material_qty_30d decimal(30,6) comment '近30天原料量',
     material_amt_30d decimal(30,6) comment '近30天原料金额',
     update_time timestamp comment  '更新时间'
) comment '内控-大客户物流仓库存变化异常分析'
partitioned by (month string comment '月分区，每月底执行')

	STORED AS parquet

