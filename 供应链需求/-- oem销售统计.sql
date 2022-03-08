show create table csx_dw.ads_sale_r_m_customer_business_goods;
-- 商品采购级别	01	全国商品
-- 商品采购级别	02	一般商品
-- 商品采购级别	03	OEM商品

-- 
SHOW CREATE TABLE csx_ods.source_master_w_a_md_product_info ;

-- OEM管理中类销售
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式

set hive.support.quoted.identifiers=none;

set edate='${enddate}';
set edt=regexp_replace(${hiveconf:edate},'-','');
set sdt=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');
set l_edt=regexp_replace(if(${hiveconf:edate}=last_day(${hiveconf:edate}),last_day(add_months(${hiveconf:edate},-1)),add_months(${hiveconf:edate},-1)),'-','');
set l_sdt=regexp_replace(add_months(trunc(${hiveconf:edate},'MM'),-1),'-','');

drop table if exists csx_tmp.temp_goods_info;
create temporary table csx_tmp.temp_goods_info as 
select classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       division_code,
       division_name,
       department_id,
       department_name,
       category_small_code,
       goods_id,
       bar_code,
       goods_name,
       unit_name,
       brand_name,
       standard,
       product_purchase_level,
       product_purchase_level_name
from
(SELECT goods_id,
       bar_code,
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
       department_id,
       department_name,
       category_small_code
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current')a 
left join 
(select temp_sync_product_code,
        product_purchase_level,
    case when product_purchase_level='01' then '全国商品'
        when product_purchase_level='02' then '一般商品'
        when product_purchase_level='03' then 'OEM商品'
        else '其他'
        end product_purchase_level_name
from csx_ods.source_master_w_a_md_product_info a 
where sdt=${hiveconf:edt}
and temp_sync_product_code is not null 
and temp_sync_product_code !=''
) b on a.goods_id=b.temp_sync_product_code 
;


drop table if exists csx_tmp.temp_goods_sale;
create temporary table csx_tmp.temp_goods_sale as 
select sdt,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        goods_code,
        b.bar_code,
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
        b.department_id,
        b.department_name,
        b.category_small_code,
        product_purchase_level,
        product_purchase_level_name,
        sum(sales_qty) sales_qty,
        sum(sales_cost) sales_cost,
        sum(sales_value) sales_value,
        sum(profit) profit,
        sum(excluding_tax_sales)excluding_tax_sales,
        sum(excluding_tax_cost)excluding_tax_cost,
        sum(excluding_tax_profit)excluding_tax_profit
from csx_dw.dws_sale_r_d_detail a 
join
csx_tmp.temp_goods_info  b on a.goods_code=b.goods_id
where sdt>=${hiveconf:l_sdt} 
    and sdt<=${hiveconf:edt}
group by sdt,
        region_code,region_name,province_code,province_name,
        city_group_code,
        city_group_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        goods_code,
        b.bar_code,
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
        b.department_id,
        b.department_name,
        b.category_small_code,
        product_purchase_level,
        product_purchase_level_name
       ;
       
       drop table if exists  csx_tmp.temp_goods_sale_01;
       create temporary table csx_tmp.temp_goods_sale_01 as 
       select `(product_purchase_level_name|product_purchase_level)?+.+`,
       case when province_code not in ('15','2') and goods_code in ('1456612' ,'1456631') then '02' else 
       product_purchase_level end purchase_level,
        case when province_code not in ('15','2') and goods_code in ('1456612' ,'1456631') then '一般商品' else 
        product_purchase_level_name end purchase_level_name
    from csx_tmp.temp_goods_sale 
;
   drop table if exists csx_tmp.temp_goods_sale_02;
  create temporary table  csx_tmp.temp_goods_sale_02 as 
    select sdt,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sales_value) sales_value,
        sum(sales_cost) sales_cost,
        sum(sales_qty)sales_qty,
        sum(profit) profit,
        sum(`excluding_tax_sales`) excluding_tax_sales, 
        sum(`excluding_tax_cost`) excluding_tax_cost, 
        sum(`excluding_tax_profit`) excluding_tax_profit ,
        sum(case when purchase_level='03' then sales_value end ) oem_sales_value,
        sum(case when purchase_level='03' then sales_cost end ) oem_sales_cost,
        sum(case when purchase_level='03' then sales_qty end) oem_sales_qty,
        sum(case when purchase_level='03' then profit end ) oem_profit,
        sum(case when purchase_level='03' then excluding_tax_sales end )  oem_excluding_tax_sales,
        sum(case when purchase_level='03' then excluding_tax_cost end )   oem_excluding_tax_cost,
        sum(case when purchase_level='03' then excluding_tax_profit end ) oem_excluding_tax_profit
    from  csx_tmp.temp_goods_sale_01
    group by
        sdt,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
;

drop table if exists csx_tmp.temp_oem_sale_01;
create temporary table csx_tmp.temp_oem_sale_01 as 
    select 
        sdt,
        substr(sdt,1,6) months,
        region_code,
        region_name,
        province_code,
        province_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sales_qty) sales_qty,
        sum(sales_cost) sales_cost,
        sum(sales_value) sales_value,
        sum(profit) profit,
        sum(coalesce(oem_sales_qty,0)) oem_sales_qty,
        sum(coalesce(oem_sales_value,0)) oem_sales_value,
        sum(coalesce(oem_sales_cost,0)) oem_sales_cost,
        sum(coalesce(oem_profit,0)) oem_profit
    from csx_tmp.temp_goods_sale_02 
        group by 
        region_code,
        region_name,
        province_code,
        province_name,
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
insert overwrite table csx_tmp.report_fr_r_d_oem_classify_sale partition(sdt)

select  months,
        region_code,
        region_name,
        province_code,
        province_name,
        ''`city_group_code`,
        ''`city_group_name`,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sales_qty,
        sales_cost,
        sales_value,
        profit,
        oem_sales_qty,
        oem_sales_value,
        oem_sales_cost,
        oem_profit,
        coalesce(coalesce(oem_sales_value,0)/sales_value,0) oem_sale_ratio,
        sdt 
from csx_tmp.temp_oem_sale_01;

show create table csx_tmp.temp_oem_sale_01; 



CREATE  TABLE `csx_tmp.report_fr_r_d_oem_classify_sale`(
  `sales_months` string comment '销售月份', 
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_group_code` string comment '城市组编码', 
  `city_group_name` string comment '城市组名称', 
  `channel_code` string comment '渠道编码', 
  `channel_name` string comment '渠道名称', 
  `business_type_code` string comment '销售业务类型', 
  `business_type_name` string comment '销售业务类型名称', 
  `classify_large_code` string comment '管理一级编码', 
  `classify_large_name` string comment '管理一级名称', 
  `classify_middle_code` string comment '管理二级编码', 
  `classify_middle_name` string comment '管理二级名称', 
  `sales_qty` decimal(38,6) comment '销量', 
  `sales_cost` decimal(38,6) comment '销售成本', 
  `sales_value` decimal(38,6) comment '销售额', 
  `profit` decimal(38,6) comment '毛利额', 
  `oem_sales_qty` decimal(38,6) comment 'OEM销售量', 
  `oem_sales_value` decimal(38,6) comment 'OEM销售额', 
  `oem_sales_cost` decimal(38,6) comment 'OEM销售成本', 
  `oem_profit` decimal(38,6) comment 'OEM毛利额',
  `oem_sales_ratio` decimal(38,6) comment 'OEM销售占比'
  )comment 'OEM管理二级销售报表'
partitioned by (sdt string comment '销售日期分区') 
STORED AS PARQUET
;

csx_tmp_report_fr_r_d_oem_classify_sale


select  
        region_code,
        region_name,
        province_code,
        province_name,
        case when classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end div_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sales_value)/10000 sales_value,
        sum(oem_sales_qty)oem_sales_qty,
        sum(oem_sales_value)/10000 oem_sales_value,
        sum(oem_profit)/10000 oem_profit,
        sum(oem_profit)/sum(oem_sales_value) as oem_profit_rate,
        coalesce(sum(oem_sales_value)/sum(sales_value),0) oem_sale_ratio,
        sum(last_oem_sales_value)/10000 last_oem_sales_value,
        round((sum(oem_sales_value)-sum(last_oem_sales_value))/sum(last_oem_sales_value),4) as sale_rate
from
(
select  
        region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sales_value)sales_value,
        sum(oem_sales_qty)oem_sales_qty,
        sum(oem_sales_value)oem_sales_value,
        sum(oem_profit)oem_profit,
        coalesce(sum(oem_sales_value)/sum(sales_value),0) oem_sale_ratio,
        0 last_oem_sales_value
from csx_tmp.report_fr_r_d_oem_classify_sale 
    where  sdt>='${sdt}'
        and sdt<='${edt}'
group by region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
union all 
select  
        '00' as region_code,
        '全国' as region_name,
        ''as province_code,
        ''as province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sales_value)sales_value,
        sum(oem_sales_qty)oem_sales_qty,
        sum(oem_sales_value)oem_sales_value,
        sum(oem_profit)oem_profit,
        coalesce(sum(oem_sales_value)/sum(sales_value),0) oem_sale_ratio,
        0 last_oem_sales_value
from csx_tmp.report_fr_r_d_oem_classify_sale 
   where  sdt>='${sdt}'
        and sdt<='${edt}'
group by 
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
-- 环期
union all
select  
        region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        0 sales_value,
        0 oem_sales_qty,
        0 oem_sales_value,
        0 oem_profit,
        0 oem_sale_ratio,
        sum(oem_sales_value) last_oem_sales_value
from csx_tmp.report_fr_r_d_oem_classify_sale 
    where  sdt>='${l_sdt}'
        and sdt<='${l_edt}'
group by region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
union all 
select  
        '00' as region_code,
        '全国' as region_name,
        ''as province_code,
        ''as province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        0 sales_value,
        0 oem_sales_qty,
        0 oem_sales_value,
        0 oem_profit,
        0 oem_sale_ratio,
        sum(oem_sales_value) last_oem_sales_value
from csx_tmp.report_fr_r_d_oem_classify_sale 
   where sdt>='${l_sdt}'
        and sdt<='${l_edt}'
group by 
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
)a 
where region_code in('2','1','4')
group by region_code,
        region_name,
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
  
order by case when region_code='00' then '0' else region_code end,
province_code,classify_middle_code
;



-- 年至今刷新

set hive.support.quoted.identifiers=none;

set edate='${enddate}';
set edt=regexp_replace(${hiveconf:edate},'-','');
set sdt=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');
set l_edt=regexp_replace(if(${hiveconf:edate}=last_day(${hiveconf:edate}),last_day(add_months(${hiveconf:edate},-1)),add_months(${hiveconf:edate},-1)),'-','');
set l_sdt=regexp_replace(add_months(trunc(${hiveconf:edate},'MM'),-3),'-','');

drop table if exists csx_tmp.temp_goods_info;
create temporary table csx_tmp.temp_goods_info as 
select classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       division_code,
       division_name,
       department_id,
       department_name,
       category_small_code,
       goods_id,
       bar_code,
       goods_name,
       unit_name,
       brand_name,
       standard,
       product_purchase_level,
       product_purchase_level_name
from
(SELECT goods_id,
       bar_code,
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
       department_id,
       department_name,
       category_small_code
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current')a 
left join 
(select temp_sync_product_code,
        product_purchase_level,
    case when product_purchase_level='01' then '全国商品'
        when product_purchase_level='02' then '一般商品'
        when product_purchase_level='03' then 'OEM商品'
        else '其他'
        end product_purchase_level_name
from csx_ods.source_master_w_a_md_product_info a 
where sdt=${hiveconf:edt}
and temp_sync_product_code is not null 
and temp_sync_product_code !=''
) b on a.goods_id=b.temp_sync_product_code 
;


drop table if exists csx_tmp.temp_goods_sale;
create temporary table csx_tmp.temp_goods_sale as 
select sdt,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        goods_code,
        b.bar_code,
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
        b.department_id,
        b.department_name,
        b.category_small_code,
        product_purchase_level,
        product_purchase_level_name,
        sum(sales_qty) sales_qty,
        sum(sales_cost) sales_cost,
        sum(sales_value) sales_value,
        sum(profit) profit,
        sum(excluding_tax_sales)excluding_tax_sales,
        sum(excluding_tax_cost)excluding_tax_cost,
        sum(excluding_tax_profit)excluding_tax_profit
from csx_dw.dws_sale_r_d_detail a 
join
csx_tmp.temp_goods_info  b on a.goods_code=b.goods_id
where sdt>=${hiveconf:l_sdt} 
    and sdt<=${hiveconf:edt}
group by sdt,
        region_code,region_name,province_code,province_name,
        city_group_code,
        city_group_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        goods_code,
        b.bar_code,
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
        b.department_id,
        b.department_name,
        b.category_small_code,
        product_purchase_level,
        product_purchase_level_name
       ;
       
       drop table if exists  csx_tmp.temp_goods_sale_01;
       create temporary table csx_tmp.temp_goods_sale_01 as 
       select `(product_purchase_level_name|product_purchase_level)?+.+`,
       case when province_code not in ('15','2') and goods_code in ('1456612' ,'1456631') then '02' else 
       product_purchase_level end purchase_level,
        case when province_code not in ('15','2') and goods_code in ('1456612' ,'1456631') then '一般商品' else 
        product_purchase_level_name end purchase_level_name
    from csx_tmp.temp_goods_sale 
;


 drop table if exists csx_tmp.temp_goods_sale_02;
  create temporary table  csx_tmp.temp_goods_sale_02 as 
    select sdt,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sales_value) sales_value,
        sum(sales_cost) sales_cost,
        sum(sales_qty)sales_qty,
        sum(profit) profit,
        sum(`excluding_tax_sales`) excluding_tax_sales, 
        sum(`excluding_tax_cost`) excluding_tax_cost, 
        sum(`excluding_tax_profit`) excluding_tax_profit ,
        sum(case when purchase_level='03' then sales_value end ) oem_sales_value,
        sum(case when purchase_level='03' then sales_cost end ) oem_sales_cost,
        sum(case when purchase_level='03' then sales_qty end) oem_sales_qty,
        sum(case when purchase_level='03' then profit end ) oem_profit,
        sum(case when purchase_level='03' then excluding_tax_sales end )  oem_excluding_tax_sales,
        sum(case when purchase_level='03' then excluding_tax_cost end )   oem_excluding_tax_cost,
        sum(case when purchase_level='03' then excluding_tax_profit end ) oem_excluding_tax_profit
    from  csx_tmp.temp_goods_sale_01
    group by
        sdt,
        region_code,
        region_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name
;

drop table if exists csx_tmp.temp_oem_sale_01;
create temporary table csx_tmp.temp_oem_sale_01 as 
    select 
        sdt,
        substr(sdt,1,6) months,
        region_code,
        region_name,
        province_code,
        province_name,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sum(sales_qty) sales_qty,
        sum(sales_cost) sales_cost,
        sum(sales_value) sales_value,
        sum(profit) profit,
        sum(coalesce(oem_sales_qty,0)) oem_sales_qty,
        sum(coalesce(oem_sales_value,0)) oem_sales_value,
        sum(coalesce(oem_sales_cost,0)) oem_sales_cost,
        sum(coalesce(oem_profit,0)) oem_profit
    from csx_tmp.temp_goods_sale_02 
        group by 
        region_code,
        region_name,
        province_code,
        province_name,
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
insert overwrite table csx_tmp.report_fr_r_d_oem_classify_sale partition(sdt)

select  months,
        region_code,
        region_name,
        province_code,
        province_name,
        ''`city_group_code`,
        ''`city_group_name`,
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        sales_qty,
        sales_cost,
        sales_value,
        profit,
        oem_sales_qty,
        oem_sales_value,
        oem_sales_cost,
        oem_profit,
        coalesce(coalesce(oem_sales_value,0)/sales_value,0) oem_sale_ratio,
        sdt 
from csx_tmp.temp_oem_sale_01;