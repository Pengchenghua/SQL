set sdt='20210701';
set edt='20210731';
-- 来源订单：1 采购导入、10    智能补货、2 直送、14    临时地采
drop table if exists  csx_tmp.temp_entry_00 ;
create table csx_tmp.temp_entry_00 as
select  sdt,
    origin_order_no,
    order_code,
    dc_code,
    goods_code,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_code,
    division_name,
    supplier_code,
    vendor_name,
    joint_purchase,
    joint_purchase_flag,                --商品联采标识
    product_level,
    product_level_name,               --商品定制 5 剔除
    sum(receive_qty) receive_qty,
    sum(receive_amt) receive_amt,
    sum(no_tax_receive_amt) as no_tax_receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(shipped_amt) shipped_amt,
    sum(no_tax_shipped_amt) as no_tax_shipped_amt
from 
(
select sdt,
    origin_order_code as  origin_order_no,
    order_code,
    receive_location_code as dc_code,
    goods_code,
    supplier_code,
    a.category_small_code,
    (receive_qty) receive_qty,
    (price/(1+tax_rate/100)*receive_qty) as no_tax_receive_amt,
    (price*receive_qty) as receive_amt,
    0 shipped_qty,
    0 shipped_amt,
    0 no_tax_shipped_amt
from csx_dw.dws_wms_r_d_entry_batch a 
where sdt>=${hiveconf:sdt} 
    and sdt<=${hiveconf:edt} 
   -- and  regexp_replace( to_date(receive_time ),'-','')>='20210701'
    and order_type_code like 'P%'
   -- and business_type !='02'
    and receive_status in ('2')
union all 
select regexp_replace(to_date(a.send_time),'-','') as sdt,
    origin_order_no , 
    order_no as  order_code,
    shipped_location_code as dc_code,
    goods_code,
    supplier_code,
    a.category_small_code,
    0 receive_qty,
    0 no_tax_receive_amt,
    0 receive_amt,
    (shipped_qty) shipped_qty,
    (price*shipped_qty) as shipped_amt,
    (price/(1+tax_rate/100)*shipped_qty) as no_tax_shipped_amt
from csx_dw.dws_wms_r_d_ship_detail  a 
  where 1=1
    -- regexp_replace( to_date(send_time),'-','') >='20210801' 
--     and  regexp_replace( to_date(send_time),'-','') <='20210831'
    and sdt<=${hiveconf:edt} 
    and sdt>=${hiveconf:sdt} 
    and order_type_code like 'P%'
    and business_type_code in ('05')
    and status in ('6','7','8')
) a 
 join 
(select shop_code,
    product_code,
    joint_purchase_flag,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    b.division_code,
    b.division_name,
    b.category_small_code,
    a.product_level,
    a.product_level_name
from csx_dw.dws_basic_w_a_csx_product_info a 
join 
(select goods_id,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_code,
    division_name,
    category_small_code
    from csx_dw.dws_basic_w_a_csx_product_m
        where sdt='current' 
    
    ) b on a.product_code=b.goods_id
    where sdt='current' ) c on a.dc_code=c.shop_code and a.goods_code=c.product_code and a.category_small_code=c.category_small_code
 join 
(select vendor_id,vendor_name,joint_purchase from  csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') d on a.supplier_code=d.vendor_id
where  classify_middle_code in ('B0304','B0305') 
group by origin_order_no,
    order_code,
    dc_code,
    goods_code,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_code,
    division_name,
    supplier_code,
    vendor_name,
    joint_purchase,
    joint_purchase_flag,
    sdt,
    product_level,
    product_level_name
;



-- 关联采购订单
-- select * from csx_tmp.temp_entry_01 where sales_region_name ='大宗' and province_name='福建省'; 
drop table if exists csx_tmp.temp_entry_01;
create  table csx_tmp.temp_entry_01 as 
select sdt,
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    supplier_code,
    vendor_name,
    origin_order_no,
    dc_code,
    goods_code,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    source_type,
    source_type_name,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    joint_purchase,         --供应商联采标识 
    joint_purchase_flag ,    --商品联采标识
    product_level,
    product_level_name,
    price,
    order_qty,
    order_amt,
    update_time,
    create_by
from 
(select sdt,
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    a.dc_code,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    origin_order_no,
    a.order_code,
    a.goods_code,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_code,
    division_name,
    supplier_code,
    vendor_name,
    source_type,
    source_type_name,
     joint_purchase,         --供应商联采标识 
    joint_purchase_flag ,    --商品联采标识
    product_level,
    product_level_name,
    (receive_qty) receive_qty,
    (receive_amt) receive_amt,
    (no_tax_receive_amt) as no_tax_receive_amt,
    (shipped_qty) shipped_qty,
    (shipped_amt) shipped_amt,
    (no_tax_shipped_amt) as no_tax_shipped_amt,
    price,
    order_qty,
    order_amt,
    update_time,
    create_by
from csx_tmp.temp_entry_00 a 
join 
(select  order_code,
        source_type,
        source_type_name,
        header_remark,
        items_remark,
        goods_code,
        price,
        order_qty,
        order_qty*price as order_amt,
        update_time,
        create_by
    from csx_dw.dws_scm_r_d_header_item_price 
    where super_class in ('2','1')  
    and source_type in ('1','2','10','14')
  
)b on a.origin_order_no=b.order_code and a.goods_code=b.goods_code
join 
(select 
    sales_province_code,
    sales_province_name,
    case when purchase_org ='P620' and purpose!='07' then '9' else  sales_region_code end sales_region_code,
    case when purchase_org ='P620' and purpose!='07' then '大宗' else  sales_region_name end sales_region_name,
    a.shop_id,
    company_code,
    case when a.shop_id in ('W0H4','W0G1','W0J8')  then '' else city_code end  city_code,
    case when a.shop_id in ('W0H4','W0G1','W0J8')  then '' else city_name end  city_name,
    case when a.shop_id in ('W0H4') then '900001' when a.shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when a.shop_id in ('W0H4') then '大宗二' when a.shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m a 
join 
(select shop_id  from csx_ods.source_basic_frozen_dc where sdt=regexp_replace(date_sub(current_date(),1),'-','')  and is_frozen_dc='1') b on a.shop_id=b.shop_id
 where sdt='current'    
    and  table_type=1 
    and purpose  in ('01','02','03','07','08','06') 
) d on a.dc_code=d.shop_id
) j 
;

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.ads_fr_r_d_frozen_purch_join_ratio partition(sdt)
select
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    supplier_code,
    vendor_name,
    origin_order_no,
    dc_code,
    goods_code,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    source_type,
    source_type_name,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    joint_purchase as joint_purchase_supplier,         --供应商联采标识 
    joint_purchase_flag as joint_purchase_goods ,    --商品联采标识
    product_level,
    product_level_name,
    price,
    order_qty,
    order_amt,
    update_time as order_update_time,
    create_by as order_create_by,
    current_timestamp(),
    sdt
from 
 csx_tmp.temp_entry_01 
 ;


-- 关联采购订单&DC类型&复用供应商
-- select * from csx_tmp.temp_entry_01 where sales_region_name ='大宗' and province_name='福建省'; 
drop table  csx_tmp.temp_entry_01;
create  table csx_tmp.temp_entry_01 as 
select sdt,
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    origin_order_no,
    dc_code,
    goods_code,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    source_type,
    source_type_name,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    joint_purchase,         --供应商联采标识 
    joint_purchase_flag ,    --商品联采标识
    product_level,
    product_level_name,
    price,
    order_qty,
    order_amt,
    update_time,
    create_by
from 
(select sdt,
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    a.dc_code,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    origin_order_no,
    a.order_code,
    a.goods_code,
    goods_name,
    unit_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    division_code,
    division_name,
    supplier_code,
    vendor_name,
    source_type,
    source_type_name,
     joint_purchase,         --供应商联采标识 
    joint_purchase_flag ,    --商品联采标识
    product_level,
    product_level_name,
    (receive_qty) receive_qty,
    (receive_amt) receive_amt,
    (no_tax_receive_amt) as no_tax_receive_amt,
    (shipped_qty) shipped_qty,
    (shipped_amt) shipped_amt,
    (no_tax_shipped_amt) as no_tax_shipped_amt,
    price,
    order_qty,
    order_amt,
    update_time,
    create_by
from csx_tmp.temp_entry_00 a 
join 
(select  order_code,
        source_type,
        source_type_name,
        header_remark,
        items_remark,
        goods_code,
        price,
        order_qty,
        order_qty*price as order_amt,
        update_time,
        create_by
    from csx_dw.dws_scm_r_d_header_item_price 
    where super_class in ('2','1')  
    and source_type in ('1','2','10','14')
  
)b on a.origin_order_no=b.order_code and a.goods_code=b.goods_code
join 
(select 
    sales_province_code,
    sales_province_name,
    case when purchase_org ='P620' and purpose!='07' then '9' else  sales_region_code end sales_region_code,
    case when purchase_org ='P620' and purpose!='07' then '大宗' else  sales_region_name end sales_region_name,
    a.shop_id,
    company_code,
    case when a.shop_id in ('W0H4','W0G1','W0J8')  then '' else city_code end  city_code,
    case when a.shop_id in ('W0H4','W0G1','W0J8')  then '' else city_name end  city_name,
    case when a.shop_id in ('W0H4') then '900001' when a.shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when a.shop_id in ('W0H4') then '大宗二' when a.shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m a 
join 
(select shop_id  from csx_ods.source_basic_frozen_dc where sdt='20210913' and is_frozen_dc='1') b on a.shop_id=b.shop_id
 where sdt='current'    
    and  table_type=1 
    and purpose  in ('01','02','03','07','08','06') 
) d on a.dc_code=d.shop_id
) j 
;


-- source_type,source_type_name  
--1 采购导入
--2 直送
--3 一键代发
--4 项目合伙人
--5 无单入库
--8 云超物流采购
--10    智能补货
--11    商超直送
--13    云超门店采购
--14    临时地采
--15    联营直送
--16    永辉生活


select * from  csx_tmp.temp_entry_01   ;




CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_purch_join_ratio`(
  `sdt` string COMMENT '日期,入库按照关单日期，出库按照出库日期', 
  `sales_province_code` string comment '大区', 
  `sales_province_name` string comment '大区', 
  `sales_region_code` string comment '销售省区', 
  `sales_region_name` string comment '销售省区', 
  `city_code` string comment '城市物理', 
  `city_name` string comment '城市物理', 
  `province_code` string comment '省区物理', 
  `province_name` string comment '省区物理', 
  `purpose` string comment 'DC用途', 
  `supplier_code` string comment '供应商编码', 
  `vendor_name` string comment '供应商名称', 
  `order_code` string comment '采购订单号', 
  `dc_code` string comment 'DC编码', 
  `goods_code` string comment '商品编码', 
  `goods_name` string comment '商品名称', 
  `unit_name` string comment '单位', 
  `classify_large_code` string comment '管理一级分类', 
  `classify_large_name` string comment '管理一级分类', 
  `classify_middle_code` string comment '管理二级', 
  `classify_middle_name` string comment '管理二级', 
  `classify_small_code` string comment '管理三级', 
  `classify_small_name` string comment '管理三级', 
  `source_type` int comment '来源采购订单编码', 
  `source_type_name` string comment '来源采购订单名称', 
  `receive_qty` decimal(38,6) comment '入库量', 
  `receive_amt` decimal(38,6) comment '入库额', 
  `no_tax_receive_amt` decimal(38,6) comment '未税入库额', 
  `shipped_qty` decimal(38,6) comment '出库量', 
  `shipped_amt` decimal(38,6) comment '出库额', 
  `no_tax_shipped_amt` decimal(38,6) comment '未税入库额', 
  `joint_purchase_supplier` int comment '供应商联采标识', 
  `joint_purchase_goods` int comment '商品联采标识', 
  `product_level` string comment '商品等级', 
  `product_level_name` string comment '商品等级', 
  `price` decimal(38,6) comment '订单售价', 
  `order_qty` decimal(38,6) comment '订单量/额', 
  `order_amt` decimal(38,6) comment '订单量/额', 
  `order_update_time` string comment '订单更新日期', 
  `create_by` string comment '创建人',
  update_time string comment '数据更新时间'
  )comment '冻品联采采购分析占比'
partitioned by (sdt string comment '按日分区')
STORED AS parquet 
 ;