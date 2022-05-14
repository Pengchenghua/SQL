-- BBC人员拣货效率
-- 省区编码，省区名称，库存DC编码，库存DC名称、商品编码、商品名称、商品数量、是否打包，打包数量，打包人，是否分拣，分拣数量，分拣人。

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
from csx_ods.source_wms_w_d_wms_product_stock_log  a 
 join 
 (select shop_id from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose='07' ) b on a.warehouse_code=b.shop_id
 where sdt='19990101' 
    and regexp_replace(to_date(finish_time ),'-','')>= ${hiveconf:sdate} 
    and regexp_replace(to_date(finish_time ),'-','')<=${hiveconf:edate} 
  --  and warehouse_code in ${hiveconf:shop}
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
from csx_dw.dws_wms_r_d_package_product_detail 
where status in(3,1)
    and sdt between ${hiveconf:sdate} and ${hiveconf:edate}
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
    dc_code AS warehouse_code,
    create_by,
    0 as pick_sku,
    0 as pick_qty,
    0 as pack_sku,
    0 as pack_qty,
    pack_order as pack_order_num
from 
csx_tmp.temp_pick_stat_02
)a where 1=1
group by 
    warehouse_code ,
    create_by,
    sdt;


insert overwrite table csx_tmp.ads_wms_r_d_picker_analysis_bbc_fr partition(sdt)
 select sdt as pick_sdt,
        province_code,
        province_name,
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
 (
select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' or shop_id ='W0J8') then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' or shop_id ='W0J8') then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620'  then '' else city_code end  city_code,
    case when purchase_org ='P620'  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        when purchase_org='P620' AND purpose='07' then '900004'
        -- WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        -- when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
        when purchase_org='P620' AND purpose='07' then '平台BBC'

    --   WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
    --   when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市' 
    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    and purpose='07' )  b on a.warehouse_code=b.shop_id
;


-- 省区编码，省区名称，库存DC编码，库存DC名称、商品编码、商品名称、商品数量、是否打包，打包数量，打包人，是否分拣，分拣数量，分拣人。


  create table csx_tmp.report_fr_r_d_sales_forecast_detail
(   region_code string comment '大区编码',
    region_name string comment '大区名称',
    province_code string comment'省区编码',
    province string comment '省区',
    city_group_code string comment '城市组编码',
    city_group_name string comment'城市组名称',
    require_delivery_date string comment '要求送货日期',
    recep_order_time string comment '接单时间',    
    order_status string comment '订单状态', 
    order_time string comment '订单时间', 
    order_no string comment '订单号', -- 完成订单数
    sap_sub_cus_code string comment '子客户号', -- 子客户数
    sap_sub_cus_name string comment '子客户名称',
    customer_site_name string comment '客户站点', -- 站点数
    sap_cus_code string comment '客户编码', -- 客户数
    sap_cus_name string comment '客户名称',
    order_kind_type string comment '订单类型',
    order_amt  decimal(30,6) '订单金额', -- 明日订单总金额
    update_time timestamp() comment '更新时间'    
    )comment '每日简报导出-实时订单明细';