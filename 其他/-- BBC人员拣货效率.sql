-- BBC人员拣货效率
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式

set i_date='${s_date}';
set sdate =regexp_replace(add_months(trunc(${hiveconf:i_date},'MM'),-1),'-','') ;
set edate =regexp_replace(${hiveconf:i_date},'-','') ;
-- set plan_sdt =regexp_replace(add_months(trunc(${hiveconf:i_date},'MM'),-6),'-','') ; -- 计划发货日期
set shop=('W0P9', 'W0G8', 'W0N9', 'W0N8', 'W0S4', 'W0M9', 'W0K8', 'W0R2', 'W0B6', 'W0Q6', 'W0S6', 'W0H2');


-- 库内拣货与打包

drop table if exists csx_tmp.temp_pick_stat;
create temporary table csx_tmp.temp_pick_stat
as 
select warehouse_code ,
       create_by,
       in_out_code,
       regexp_replace(to_date(finish_time),'-','') sdt,
       coalesce(count(case when task_type='04' then product_code end),0) as pick_sku, -- 拣货SKU
       coalesce(sum(case when task_type='04' then adjustment_qty end),0) as pick_qty, -- 拣货数量
       coalesce(count(case when task_type='08' then product_code end),0) as pack_sku, -- 打包SKU
       coalesce(sum(case when task_type='08' then adjustment_qty end),0) as pack_qty -- 打包数
from csx_ods.source_wms_w_d_wms_product_stock_log  
 where sdt='19990101' 
    and regexp_replace(to_date(finish_time ),'-','')>= ${hiveconf:sdate} 
    and regexp_replace(to_date(finish_time ),'-','')<=${hiveconf:edate} 
    and warehouse_code in ${hiveconf:shop}
    and task_type in ('04','02','08')
group by 
    warehouse_code ,
    create_by,
    in_out_code,
    regexp_replace(to_date(finish_time),'-','')
;

--select create_by,sum(pick_qty),sum(pick_sku),sum(pack_qty),sum(pack_sku) from csx_tmp.temp_pick_stat where warehouse_code='W0B6' and sdt>='20200901' group by create_by;

-- 寻找 '21'同城配,'20' 自提,'22' 快递配 单据号
drop table if exists csx_tmp.temp_pick_stat_01;
create temporary table csx_tmp.temp_pick_stat_01
as 
select
    a.sdt,
    warehouse_code ,
    create_by,
    -- a.in_out_code,
    -- order_code,
    sum(pick_sku)as pick_sku, 
    sum(pick_qty)as pick_qty, 
    sum(pack_sku)as pack_sku, 
    sum(pack_qty)as pack_qty
from csx_tmp.temp_pick_stat a 
 join 
(select order_code from csx_ods.source_wms_r_d_shipped_order_header 
where sdt between ${hiveconf:sdate}  and ${hiveconf:edate}
    and business_type in ('21','20','22')
group by order_code) b on a.in_out_code=b.order_code
-- where order_code is  null 
-- and a.warehouse_code='W0B6' 
-- and a.sdt>='20200901'
group by a.sdt,
    warehouse_code ,
    create_by
    -- a.in_out_code,
    -- order_code
;

-- 包裹打包数
drop table if exists csx_tmp.temp_pick_stat_02;
create temporary table csx_tmp.temp_pick_stat_02
as
 select warehouse_code,
 create_by,
 regexp_replace(to_date(update_time),'-','') as sdt,
 count(distinct order_code) as pack_order_num
 from  csx_ods.source_wms_r_d_wms_package_product 
 where sdt=${hiveconf:edate} 
 and regexp_replace(to_date(update_time),'-','')>= ${hiveconf:sdate} 
 and regexp_replace(to_date(update_time),'-','')<= ${hiveconf:edate}
 group by
 warehouse_code,
 create_by,
 regexp_replace(to_date(update_time),'-','')
 ;
 
-- 汇总打包数
drop table if exists csx_tmp.temp_pick_stat_03;
create temporary table csx_tmp.temp_pick_stat_03
as
select  
    sdt,
    warehouse_code ,
    create_by,
    sum(pick_sku)pick_sku, 
    sum(pick_qty)pick_qty, 
    sum(pack_sku)pack_sku, 
    sum(pack_qty)pack_qty, 
    sum(pack_order_num)pack_order_num
from (
select 
    sdt,
    warehouse_code ,
    create_by,
    pick_sku,
    pick_qty,
    pack_sku,
    pack_qty,
    0 pack_order_num
from csx_tmp.temp_pick_stat_01
union all 
select 
    sdt,
    warehouse_code,
    create_by,
    0 as pick_sku,
    0 as pick_qty,
    0 as pack_sku,
    0 as pack_qty,
    pack_order_num as pack_order_num
from 
csx_tmp.temp_pick_stat_02
)a where 1=1
group by 
    warehouse_code ,
    create_by,
    sdt;


insert overwrite table csx_tmp.ads_wms_r_d_picker_analysis partition(sdt)
 select sdt as pick_sdt,
        dist_code,
        dist_name,
        warehouse_code ,
        shop_name,
        create_by,
        pick_sku, 
        pick_qty, 
        pack_sku, 
        pack_qty, 
        pack_order_num,
        0 temp_01,
        0 temp_02,
        0 temp_03,
        sdt
 from csx_tmp.temp_pick_stat_03 a 
 join 
 (select location_code,shop_name,dist_code,dist_name from csx_dw.csx_shop where sdt='current' )b on a.warehouse_code=b.location_code
;
-- 
-- create table csx_tmp.ads_wms_r_d_picker_analysis(
-- pick_sdt	string	         comment'拣货日期',
-- dist_code	string	 comment'省区编码',
-- dist_name	string	 comment'省区名称',
-- dc_code	string	     comment'DC编码',
-- dc_name	string	     comment'DC名称',
-- personnel	string	 comment'人员',
-- pick_sku	bigint	 comment'拣货SKU',
-- pick_qty	bigint	 comment'拣货数量',
-- pack_sku	bigint	 comment'打包SKU',
-- pack_qty	bigint	 comment'打包数量',
-- pack_order_num	bigint	 comment'包裹数根据单号',
-- temp_01 string comment '预留',
-- temp_02 string comment '预留',
-- temp_03 string comment '预留'
-- ) comment '库内人员效率分析'
-- partitioned by (sdt string comment '日期分区');
--  
--  
