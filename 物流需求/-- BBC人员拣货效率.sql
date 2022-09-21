-- BBC人员拣货效率
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式

set i_date='${s_date}';
set sdate =regexp_replace(add_months(trunc(${hiveconf:i_date},'MM'),-1),'-','') ;
set edate =regexp_replace(${hiveconf:i_date},'-','') ;
-- set plan_sdt =regexp_replace(add_months(trunc(${hiveconf:i_date},'MM'),-6),'-','') ; -- 计划发货日期
-- set shop=('W0P9', 'W0G8', 'W0N9', 'W0N8', 'W0S4', 'W0M9', 'W0K8', 'W0R2', 'W0B6', 'W0Q6', 'W0S6', 'W0H2');
-- set shop=(select shop_id from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose='07' );


-- 库内拣货与打包

drop table if exists csx_analyse_tmp.csx_analyse_tmp_pick_stat;
create  table csx_analyse_tmp.csx_analyse_tmp_pick_stat
as 
select dc_code ,
       create_by,
       in_out_code,
       regexp_replace(to_date(finish_time),'-','') sdt,
       coalesce(count(case when task_type='04' then goods_code end),0) as pick_sku, -- 拣货SKU
       coalesce(sum(case when task_type='04' then adjustment_qty end),0) as pick_qty, -- 拣货数量
       coalesce(count(case when task_type='08' then goods_code end),0) as pack_sku, -- 打包SKU
       coalesce(sum(case when task_type='08' then adjustment_qty end),0) as pack_qty -- 打包数
from csx_dwd.csx_dwd_wms_product_stock_log_di  a 
 join 
 (select shop_code from csx_dim.csx_dim_shop where 
    sdt='current' 
    and purpose='07' ) b on a.dc_code=b.shop_code
 where sdt=regexp_replace('${edate}','-','') 
    and regexp_replace(to_date(finish_time ),'-','')>= regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','')  
    and regexp_replace(to_date(finish_time ),'-','')<= regexp_replace('${edate}','-','') 
    and task_type in ('04','02','08')
group by 
    dc_code ,
    create_by,
    in_out_code,
    regexp_replace(to_date(finish_time),'-','')
;

--select create_by,sum(pick_qty),sum(pick_sku),sum(pack_qty),sum(pack_sku) from csx_analyse.csx_analyse_tmp_pick_stat where dc_code='W0B6' and sdt>='20200901' group by create_by;

-- 寻找 '21'同城配,'20' 自提,'22' 快递配 单据号

drop table if exists csx_analyse_tmp.csx_analyse_tmp_pick_stat_01;
create  table csx_analyse_tmp.csx_analyse_tmp_pick_stat_01
as 
select
    a.sdt,
    dc_code ,
    create_by,
    -- a.in_out_code,
    -- order_code,
    sum(pick_sku)as pick_sku, 
    sum(pick_qty)as pick_qty, 
    sum(pack_sku)as pack_sku, 
    sum(pack_qty)as pack_qty
from csx_analyse_tmp.csx_analyse_tmp_pick_stat a 
 join 
(select order_code from csx_dwd.csx_dwd_wms_shipped_order_header_di 
where sdt between regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','')  
    and regexp_replace('${edate}','-','')    
    and business_type_code in ('21','20','22')
group by order_code) b on a.in_out_code=b.order_code
-- where order_code is  null 
-- and a.dc_code='W0B6' 
-- and a.sdt>='20200901'
group by a.sdt,
    dc_code ,
    create_by
    -- a.in_out_code,
    -- order_code
;

-- 包裹打包数
drop table if exists csx_analyse_tmp.csx_analyse_tmp_pick_stat_02;

create  table csx_analyse_tmp.csx_analyse_tmp_pick_stat_02
as
select sdt, 
    dc_code,
    create_by,
    count(distinct order_code) as pack_order,
    count(goods_code) pack_sku,
    sum(pack_qty) pack_qty
from(
select sdt,
    dc_code,
    order_code,
    goods_code,
    create_by,
    sum(qty) pack_qty
from csx_dws.csx_dws_wms_package_product_detail_di
where status in(3,1)
    and sdt between regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','')  
    and regexp_replace('${edate}','-','') 
group by  dc_code,
    order_code,
    goods_code,
    create_by,
    sdt
)a 
group by dc_code,
    create_by,
    sdt
;


 
-- 汇总打包数
drop table if exists csx_analyse.csx_analyse_tmp_pick_stat_03;
create temporary table csx_analyse.csx_analyse_tmp_pick_stat_03
as
select  
    sdt,
    dc_code ,
    create_by,
    sum(pick_sku)pick_sku, 
    sum(pick_qty)pick_qty, 
    sum(pack_sku)pack_sku, 
    sum(pack_qty)pack_qty, 
    sum(pack_order_num)pack_order_num
from (
select 
    sdt,
    dc_code ,
    create_by,
    pick_sku,
    pick_qty,
    pack_sku,
    pack_qty,
    0 pack_order_num
from csx_analyse_tmp.csx_analyse_tmp_pick_stat_01
union all 
select 
    sdt,
    dc_code AS dc_code,
    create_by,
    0 as pick_sku,
    0 as pick_qty,
    0 as pack_sku,
    0 as pack_qty,
    pack_order as pack_order_num
from 
csx_analyse_tmp.csx_analyse_tmp_pick_stat_02
)a where 1=1
group by 
    dc_code ,
    create_by,
    sdt;


insert overwrite table csx_analyse.ads_wms_r_d_picker_analysis_bbc_fr partition(sdt)
  select sdt as pick_sdt,
        performance_province_code,
        performance_province_name,
        dc_code ,
        shop_name,
        create_by,
        pick_sku, 
        pick_qty, 
        pack_sku, 
        pack_qty, 
        pack_order_num,
        0 csx_analyse_tmp_01,
        0 csx_analyse_tmp_02,
        current_timestamp,
        sdt
 from csx_analyse_tmp.csx_analyse_tmp_pick_stat_03 a 
 join 
 (
select 
    performance_region_code,
    performance_region_name,
    purchase_org,
    shop_code,
    shop_name,
    performance_province_code,
    performance_province_name,
    purpose,
    purpose_name
from csx_dim.csx_dim_shop
 where sdt='current'    
    and purpose='07' 
)  b on a.dc_code=b.shop_code
;

-- 库内拣货与打包明细

drop table if exists csx_analyse_tmp.csx_analyse_tmp_pick_stat_1;
create  table csx_analyse_tmp.csx_analyse_tmp_pick_stat_1
as 
select dc_code ,
	   dc_code,
       in_out_code,
       regexp_replace(to_date(finish_time),'-','') sdt,
       goods_code,
       a.goods_name,
       task_type,
       a.create_by,
       a.adjustment_qty
from csx_dwd.csx_dwd_wms_product_stock_log_di  a 
 join 
 (
select 
    performance_region_code,
    performance_region_name,
    purchase_org,
    shop_code,
    shop_name,
    performance_province_code,
    performance_province_name,
    purpose,
    purpose_name
from csx_dim.csx_dim_shop
 where sdt='current'    
    and purpose='07' ) b on a.dc_code=b.shop_code
 where sdt= regexp_replace('${edate}','-','') 
    and regexp_replace(to_date(finish_time ),'-','') between regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','')  
    and regexp_replace('${edate}','-','') 
    and task_type in ('04','02','08')

;



-- 寻找 '21'同城配,'20' 自提,'22' 快递配 单据号
-- 库内操作统计
insert overwrite table  csx_analyse.csx_analyse_fr_bbc_operation_stat_di  partition(sdt)
select    
    performance_province_code,
    performance_province_name,
    dc_code ,
	shop_name,
	-- create_by,
    goods_code ,
    goods_name,
    unit_name,
    purchase_group_code,
    purchase_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    create_by,
    sum(coalesce(case when task_type='04' then adjustment_qty end,0)) as pick_qty,
    sum(coalesce(case when task_type='08' then adjustment_qty end,0)) as pack_qty ,
    current_timestamp(),
    sdt
from csx_analyse_tmp.csx_analyse_tmp_pick_stat_1 a 
 join 
(select order_code from  csx_dwd.csx_dwd_wms_shipped_order_header_di 
where sdt between regexp_replace(add_months(trunc('${edate}','MM'),-1),'-','')  
    and regexp_replace('${edate}','-','') 
    and business_type in ('21','20','22')
group by order_code
) b on a.in_out_code=b.order_code
 join 
 (
select 
    performance_region_code,
    performance_region_name,
    purchase_org,
    shop_code,
    shop_name,
    performance_province_code,
    performance_province_name,
    purpose,
    purpose_name
from csx_dim.csx_dim_shop
 where sdt='current'    
    and purpose='07' )  d on a.dc_code=d.shop_code
left join 
(SELECT goods_code,
       goods_name,
       unit_name,
       purchase_group_code,
       purchase_group_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dim.csx_dim_basic_goods
WHERE sdt='current') c on a.goods_code=c.goods_code
group by  a.sdt,
    performance_province_code,
    performance_province_name,
    dc_code ,
	shop_name,
	create_by,
    goods_code,
    goods_name,
    unit_name,
    department_id,
    department_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    ;



    CREATE TABLE `csx_analyse.csx_analyse_fr_bbc_picker_efficiency_analysis_di`(
	  `pick_sdt` string COMMENT '拣货日期', 
	  `province` string COMMENT '省区编码', 
	  `dist_name` string COMMENT '省区名称', 
	  `dc_code` string COMMENT 'DC编码', 
	  `dc_name` string COMMENT 'DC名称', 
	  `personnel` string COMMENT '人员', 
	  `pick_sku` bigint COMMENT '拣货SKU', 
	  `pick_qty` bigint COMMENT '拣货数量', 
	  `pack_sku` bigint COMMENT '打包SKU', 
	  `pack_qty` bigint COMMENT '打包数量', 
	  `pack_order_num` bigint COMMENT '包裹数根据单号', 
	  `temp_01` string COMMENT '预留', 
	  `temp_02` string COMMENT '预留', 
	  `temp_03` string COMMENT '预留')
	COMMENT '库内拣货人员效率分析'
	PARTITIONED BY ( 
	  `sdt` string COMMENT '日期分区')
	STORED AS parquet 
    ;


  CREATE TABLE `csx_analyse.csx_analyse_fr_bbc_operation_stat_di`(
	  `performance_province_code` string COMMENT '省区编码', 
	  `performance_province_name` string COMMENT '省区名称', 
	  `dc_code` string COMMENT 'DC编码', 
	  `dc_name` string COMMENT 'dc名称', 
	  `goods_code` string COMMENT '商品编码', 
	  `goods_name` string COMMENT '商品名称', 
	  `unit_name` string COMMENT '单位', 
	  `purchase_group_code` string COMMENT '课组编码', 
	  `purchase_group_name` string COMMENT '课组名称', 
	  `classify_large_code` string COMMENT '管理大类编码', 
	  `classify_large_name` string COMMENT '管理大类名称', 
	  `classify_middle_code` string COMMENT '管理中类编码', 
	  `classify_middle_name` string COMMENT '管理中类名称', 
	  `classify_small_code` string COMMENT '管理小类编码', 
	  `classify_small_name` string COMMENT '管理小类名称', 
	  `create_by` string COMMENT '操作人', 
	  `pick_qty` decimal(23,3) COMMENT '拣货数量', 
	  `pack_qty` decimal(23,3) COMMENT '打包数量', 
	  `update_time` timestamp COMMENT '更新日期')
	COMMENT 'BBC库内操作打包拣货明细统计'
	PARTITIONED BY ( 
	  `sdt` string COMMENT '日期分区')
	STORED AS parquet 