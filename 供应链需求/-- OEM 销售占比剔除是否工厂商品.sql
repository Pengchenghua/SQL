-- OEM 销售占比剔除是否工厂商品
-- ******************************************************************** 
-- @功能描述：OEM 销售占比剔除是否工厂商品
-- @创建者： 彭承华 
-- @创建者日期：2022-08-31 19:59:33 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

-- OEM管理中类销售
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式

set hive.support.quoted.identifiers=none;

 
drop table if exists csx_analyse_tmp.csx_analyse_tmp_temp_goods_info;
create  table csx_analyse_tmp.csx_analyse_tmp_temp_goods_info as 
SELECT goods_code,
       goods_bar_code,
       goods_name,
       unit_name,
       brand_name,
       standard,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       division_code,
       division_name,
       purchase_group_code,
       purchase_group_name,
       category_small_code,
	   csx_purchase_level_code,
       csx_purchase_level_name,
       is_factory_goods_flag
FROM csx_dim.csx_dim_basic_goods
WHERE sdt='current'

;


drop table if exists csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale;
create  table csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale as 
select   sdt,
        `performance_region_code` ,
		`performance_region_name` ,
		`performance_province_code` ,
		`performance_province_name` ,
		`performance_city_code`  ,
		`performance_city_name`  ,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        a.goods_code,
        b.goods_bar_code,
        b.goods_name,
        b.unit_name,
        b.brand_name,
        b.standard,
        b.classify_large_code,
        b.classify_large_name,
        b.classify_middle_code,
        b.classify_middle_name,
        b.classify_small_code,
        b.classify_small_name,
        b.division_code,
        b.division_name,
        b.purchase_group_code,
        b.purchase_group_name,
        b.category_small_code,
        csx_purchase_level_code,
        csx_purchase_level_name,
        b.is_factory_goods_flag,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(sale_amt_no_tax) sale_amt_no_tax,
        sum(sale_cost_no_tax) sale_cost_no_tax,
        sum(profit_no_tax) profit_no_tax
from csx_dws.csx_dws_sale_detail_di a 
join
csx_analyse_tmp.csx_analyse_tmp_temp_goods_info  b on a.goods_code=b.goods_code
where sdt>= regexp_replace(add_months(trunc('${enddate}','MM'),-1),'-','')
    and sdt <= regexp_replace('${enddate}','-','')
group by sdt,
        `performance_region_code` ,
		`performance_region_name` ,
		`performance_province_code` ,
		`performance_province_name` ,
		`performance_city_code`  ,
		`performance_city_name`  ,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        a.goods_code,
        b.goods_bar_code,
        b.goods_name,
        b.unit_name,
        b.brand_name,
        b.standard,
        b.classify_large_code,
        b.classify_large_name,
        b.classify_middle_code,
        b.classify_middle_name,
        b.classify_small_code,
        b.classify_small_name,
        b.division_code,
        b.division_name,
        b.purchase_group_code,
        b.purchase_group_name,
        b.category_small_code,
        csx_purchase_level_code,
        csx_purchase_level_name,
        b.is_factory_goods_flag
       ;
       
	  -- 鸡蛋红壳	\鲜鸡蛋粉壳	 福建、上海更改为一般商品 
       drop table if exists  csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale_01;
       create  table csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale_01 as 
       select sdt,
        `performance_region_code` ,
		`performance_region_name` ,
		`performance_province_code` ,
		`performance_province_name` ,
		`performance_city_code`  ,
		`performance_city_name`  ,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        a.goods_code,
        a.goods_bar_code,
        a.goods_name,
        a.unit_name,
        a.brand_name,
        a.standard,
        a.classify_large_code,
        a.classify_large_name,
        a.classify_middle_code,
        a.classify_middle_name,
        a.classify_small_code,
        a.classify_small_name,
        a.division_code,
        a.division_name,
        a.purchase_group_code,
        a.purchase_group_name,
        a.category_small_code,
        a.is_factory_goods_flag,
        (sale_qty) sale_qty,
        (sale_cost) sale_cost,
        (sale_amt) sale_amt,
        (profit) profit,
        (sale_amt_no_tax) sale_amt_no_tax,
        (sale_cost_no_tax) sale_cost_no_tax,
        (profit_no_tax) profit_no_tax,
        case when performance_province_code not in ('15','2') and goods_code in ('1456612' ,'1456631') then '02' 
                else 
                    csx_purchase_level_code
                end csx_purchase_level_code,
        case when performance_province_code not in ('15','2') and goods_code in ('1456612' ,'1456631') then '一般商品' 
                else 
                    csx_purchase_level_name 
         end csx_purchase_level_name
    from csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale a
;

   drop table if exists csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale_02;
  create  table  csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale_02 as 
    select  sdt,
        `performance_region_code` ,
		`performance_region_name` ,
		`performance_province_code` ,
		`performance_province_name` ,
		`performance_city_code`  ,
		`performance_city_name`  ,
        channel_code,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sale_amt) sale_amt,
        sum(sale_cost) sale_cost,
        sum(sale_qty)sale_qty,
        sum(profit) profit,
        sum(`sale_amt_no_tax`) sale_amt_no_tax, 
        sum(`sale_cost_no_tax`) sale_cost_no_tax, 
        sum(`profit_no_tax`) profit_no_tax ,
        sum(case when csx_purchase_level_code='03' and is_factory_goods_flag!=1 then sale_amt end ) oem_sale_amt,
        sum(case when csx_purchase_level_code='03' and is_factory_goods_flag!=1 then sale_cost end ) oem_sale_cost,
        sum(case when csx_purchase_level_code='03' and is_factory_goods_flag!=1 then sale_qty end) oem_sale_qty,
        sum(case when csx_purchase_level_code='03' and is_factory_goods_flag!=1 then profit end ) oem_profit,
        sum(case when csx_purchase_level_code='03' and is_factory_goods_flag!=1 then sale_amt_no_tax end )  oem_sale_amt_no_tax,
        sum(case when csx_purchase_level_code='03' and is_factory_goods_flag!=1 then sale_cost_no_tax end )   oem_sale_cost_no_tax,
        sum(case when csx_purchase_level_code='03' and is_factory_goods_flag!=1 then profit_no_tax end ) oem_profit_no_tax
    from  csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale_01
    group by
        sdt,
        `performance_region_code` ,
		`performance_region_name` ,
		`performance_province_code` ,
		`performance_province_name` ,
		`performance_city_code`  ,
		`performance_city_name`  ,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
;

drop table if exists csx_analyse_tmp.csx_analyse_tmp_temp_oem_sale_01;
create  table csx_analyse_tmp.csx_analyse_tmp_temp_oem_sale_01 as 
    select 
        sdt,
        substr(sdt,1,6) months,
        `performance_region_code` ,
		`performance_region_name` ,
		`performance_province_code` ,
		`performance_province_name` ,
		
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sale_qty) sale_qty,
        sum(sale_cost) sale_cost,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        sum(coalesce(oem_sale_qty,0)) oem_sale_qty,
        sum(coalesce(oem_sale_amt,0)) oem_sale_amt,
        sum(coalesce(oem_sale_cost,0)) oem_sale_cost,
        sum(coalesce(oem_profit,0)) oem_profit
    from csx_analyse_tmp.csx_analyse_tmp_temp_goods_sale_02 
        group by 
        `performance_region_code` ,
		`performance_region_name` ,
		`performance_province_code` ,
		`performance_province_name` ,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sdt,
        substr(sdt,1,6)
   
    ;
    
    
insert overwrite table csx_analyse.csx_analyse_fr_oem_classify_sale_di partition(sdt)

select  months,
        `performance_region_code` ,
		`performance_region_name` ,
		`performance_province_code` ,
		`performance_province_name` ,
		 '' as `performance_city_code`  ,
		 '' as `performance_city_name`  ,   
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sale_qty,
        sale_cost,
        sale_amt,
        profit,
        oem_sale_qty,
        oem_sale_amt,
        oem_sale_cost,
        oem_profit,
        coalesce(coalesce(oem_sale_amt,0)/sale_amt,0) oem_sale_ratio,
        CURRENT_TIMESTAMP(),
        sdt as sale_sdt,
        sdt
from csx_analyse_tmp.csx_analyse_tmp_temp_oem_sale_01;