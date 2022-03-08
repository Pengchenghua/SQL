-- 财务采购入库订单表
-- 需要增加采购订单状态
drop table csx_tmp.report_fr_r_m_financial_purchase_detail;
CREATE TABLE `csx_tmp.report_fr_r_m_financial_purchase_detail`(
  `sdt` string COMMENT '收货日期(包含出库日期)', 
  `purchase_org` string COMMENT '采购组织', 
  `purchase_org_name` string COMMENT '采购组织名称', 
  `order_code` string COMMENT '采购订单号',
   receive_code string comment '入库/出库单号',
   batch_code string COMMENT '批次单号',
  `sales_province_code` string COMMENT '省区编码', 
  `sales_province_name` string COMMENT '省区名称', 
  `source_type_code` string COMMENT '来源采购订单类型', 
  `source_type_name` string COMMENT '来源采购订单名称', 
  `super_class_name` string COMMENT '订单类型', 
  `dc_code` string COMMENT 'DC编码',  
  `shop_name` string COMMENT 'DC名称', 
  `goods_code` string COMMENT '商品编码', 
  `goods_name` string COMMENT '商品名称', 
  `unit_name` string COMMENT '单位', 
  `brand_name` string COMMENT '品牌', 
  `division_code` string COMMENT '部类编码', 
  `division_name` string COMMENT '部类名称', 
  `department_id` string COMMENT '课组编码', 
  `department_name` string COMMENT '课组名称', 
  `classify_large_code` string COMMENT '管理一级编码', 
  `classify_large_name` string COMMENT '管理一级名称', 
  `classify_middle_code` string COMMENT '管理二级编码', 
  `classify_middle_name` string COMMENT '管理二级名称', 
  `classify_small_code` string comment '管理三级编码', 
  `classify_small_name` string COMMENT '管理三级名称', 
  `category_large_code` string COMMENT '大类编码', 
  `category_large_name` string COMMENT '大类名称', 
  `supplier_code` string COMMENT '供应商编码', 
  `vendor_name` string COMMENT '供应商名称', 
  `send_dc_code` string COMMENT '发货DC编码', 
  `send_dc_name` string COMMENT '发货DC名称', 
   settle_location_code string COMMENT '结算DC',
  `local_purchase_flag` string COMMENT '是否地采', 
  `business_type_name` string COMMENT '业务类型名称', 
  `order_qty` decimal(30,6) comment '订单数量', 
  `order_price1` decimal(30,6) COMMENT '单价1', 
  `order_price2` decimal(30,6) COMMENT'单价2', 
  `receive_qty` decimal(38,6) comment '入库数量', 
  `receive_amt` decimal(38,6) comment '入库金额', 
  `no_tax_receive_amt` decimal(38,6) comment '入库不含税金额', 
  `shipped_qty` decimal(38,6) comment '出库数量', 
  `shipped_amt` decimal(38,6) comment '出库金额', 
  `no_tax_shipped_amt` decimal(38,6) comment '出库不含税金额', 
  `receive_sdt` string COMMENT '收货日期', 
   order_create_date string COMMENT '订单日期',
  `yh_reuse_tag` string cOMMENT '是否永辉复用', 
   `daily_source` string COMMENT'日采标识 ', 
  `pick_gather_flag` string comment '已拣代收', 
  `urgency_flag` string comment '紧急补货', 
  `has_change` string comment '有无变更', 
  `entrust_outside` string comment '委外标识', 
  `order_business_type` string COMMENT '业务类型 0缺省 1基地订单', 
  order_type string comment'订单类型',
  extra_flag string comment '补货标识',
  timeout_cancel_flag string comment '超时订单取消',
  joint_purchase_flag string comment '集采供应商',
  `supplier_type_code` string COMMENT '供应商类型编码', 
  `supplier_type_name` string COMMENT '供应商类型名称',  
  `business_owner_code` STRING comment '业态归属编码',
  `business_owner_name` STRING comment '业态归属名称',
  `special_customer` STRING comment '专项客户',
  `borrow_flag`STRING comment'是否借用',
  direct_trans_flag STRING comment '是否直供',
  supplier_classify_code STRING comment '供应商类型编码  0：基础供应商   1:农户供应商',
  order_goods_status string COMMENT '订单商品状态 状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)',
  `purpose` string COMMENT 'DC类型编码', 
  `purpose_name` string COMMENT 'DC类型名称',
   
   update_time string COMMENT '数据更新时间'
  )comment '财务采购入库订单表(供应商配送、供应商退货)'
  partitioned by (months string COMMENT '月分区、统计月')
  stored as parquet
  ;


SET hive.execution.engine=mr; 
--- 财务采购商品入库明细 20211009
set s_date='20200101';
set e_date='20201231';

drop table csx_tmp.temp_entry_00 ;
create table csx_tmp.temp_entry_00 as
select   order_no,
    receive_no,
    batch_code,
    dc_code,
    business_type,
    business_type_name,
    goods_code,
    goods_name,
    unit_name,
    brand_name,
    department_id,
    department_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    supplier_code,
    vendor_name,
    send_dc_code,
    (receive_qty) receive_qty,
    (receive_amt) receive_amt,
    (no_tax_receive_amt) as no_tax_receive_amt,
    (shipped_qty) shipped_qty,
    (shipped_amt) shipped_amt,
    (no_tax_shipped_amt) as no_tax_shipped_amt,
    receive_sdt
from 
(
select origin_order_code order_no,
    receive_location_code as dc_code,
    order_code receive_no,
    batch_code ,
    business_type,
    business_type_name,
    goods_code,
    supplier_code,
    send_location_code as send_dc_code,
    (receive_qty) receive_qty,
    (price/(1+tax_rate/100)*receive_qty) as no_tax_receive_amt,
    (price*receive_qty) as receive_amt,
    0 shipped_qty,
    0 shipped_amt,
    0 no_tax_shipped_amt,
    regexp_replace( to_date(receive_time ),'-','') receive_sdt
from csx_dw.dws_wms_r_d_entry_batch a 

where (sdt>=${hiveconf:s_date} or sdt='19990101')
    and regexp_replace( to_date(receive_time ),'-','')<= ${hiveconf:e_date}
    and  regexp_replace( to_date(receive_time ),'-','')>=${hiveconf:s_date}
    and order_type_code like 'P%'
    -- and business_type !='02'
    and receive_status in ('1','2')
union all 
select origin_order_no order_no, 
    shipped_location_code  as dc_code,
    order_no as receive_no,
    batch_code,
    business_type_code as business_type,
    business_type_name,
    goods_code,
    supplier_code,
    receive_location_code  as send_dc_code,
    0 receive_qty,
    0 no_tax_receive_amt,
    0 receive_amt,
    (shipped_qty) shipped_qty,
    (price*shipped_qty) as shipped_amt,
    (price/(1+tax_rate/100)*shipped_qty) as no_tax_shipped_amt,
    sdt receive_sdt
from csx_dw.dws_wms_r_d_ship_batch
where sdt >=   ${hiveconf:s_date}
    and sdt <=${hiveconf:e_date}
    and order_type_code like 'P%'
  --  and business_type_code in ('05')
    and status in ('6','7','8')
) a 
join 
(SELECT goods_id,
       goods_name,
       unit_name,
       brand_name,
       department_id,
       department_name,
       division_code,
       division_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       category_large_code,
       category_large_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') b on a.goods_code=b.goods_id
join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code=c.vendor_id
where substr(dc_code,1,1) !='L'
;

drop table csx_tmp.temp_order_table ;
create temporary table csx_tmp.temp_order_table as 
select sales_province_code,
    sales_province_name,
    purchase_org,
    purchase_org_name,
    sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    b.order_code,
    super_class,
    source_type,
    source_type_name,
    local_purchase_flag,
    a. order_no,
    receive_no,
    a.batch_code,
    dc_code,
    shop_name,
    b.settle_location_code,
    business_type,
    business_type_name,
    a.goods_code,
    goods_name,
    unit_name,
    brand_name,
    department_id,
    department_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    supplier_code,
    vendor_name,
    send_dc_code,
    order_qty,
    order_price1,
    order_price2,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    receive_sdt ,    --发货日期&收货日期
    order_create_date,      --订单创建日期
    daily_source,   --日采标识
    pick_gather_flag,  --已拣代收
    urgency_flag,      --紧急补货
    has_change  ,---有无变更 
    entrust_outside,   --委外标识 
    order_business_type ,    --业务类型 基地订单标识    
    order_type ,         -- 订单类型
    extra_flag,         --补货标识
    timeout_cancel_flag,  -- 超时订单取消
    order_goods_status   --订单商品状态状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)
from csx_tmp.temp_entry_00 a 
left join 
(select  order_code,
    super_class,
    source_type,
    settle_location_code,
    case when source_type = 1 then '采购导入'
    when source_type = 2 then '直送客户'
    when source_type = 3 then '一键代发'
    when source_type = 4 then '项目合伙人'
    when source_type = 5 then '无单入库'
    when source_type = 6 then '寄售调拨'
    when source_type = 7 then '自营调拨'
    when source_type = 8 then '云超物流采购'
    when source_type = 9 then '工厂调拨'
    when source_type = 10 then '智能补货'
    when source_type = 11 then '商超直送'
    when source_type = 12 then 'WMS调拨'
    when source_type = 13 then '云超门店采购'
    when source_type = 14 then '临时地采'
    when source_type = 15 then '联营直送'
    when source_type = 16 then '永辉生活'
    when source_type = 17 then 'RDC调拨'
    when source_type = 18 then '城市服务商'
    else '其他' end as source_type_name      , --订单来源名称 source_type_name,
    a.goods_code,
    a.order_qty,                                --订单数量
    price_include_tax as order_price1 ,         --单价1
    price2_include_tax as order_price2,         --单价2     
    local_purchase_flag,
    daily_source,   --日采标识
    pick_gather_flag,  --已拣代收
    urgency_flag,      --紧急补货
    has_change  ,---有无变更 
    entrust_outside,   --委外标识 
    '0' order_business_type ,    --业务类型 基地订单标识
    to_date(a.create_time) as order_create_date,              --订单创建时间
    order_type ,         -- 订单类型
    extra_flag,
    timeout_cancel_flag,
    a.items_status as order_goods_status
    from csx_dw.dws_scm_r_d_order_detail  a
        where super_class in ('2','1')          --供应商配送、供应商入库
    -- and source_type in ('1','10')
   
)b on a.order_no=b.order_code and a.goods_code=b.goods_code
left join 
(select sales_province_code,
    sales_province_name,
    purchase_org,
    purchase_org_name,
    case when (purchase_org ='P620' and purpose!='07') or shop_id='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id ,
    shop_name ,
    company_code ,
    company_name ,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市' 
    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1
) d on a.dc_code=d.shop_id
;


drop table csx_tmp.temp_purch_table;
create temporary table csx_tmp.temp_purch_table as
select receive_sdt  as sdt,
    purchase_org,
    purchase_org_name,
    -- sales_region_code,
    -- sales_region_name ,
    j.order_code,
    receive_no,
    batch_code,
    sales_province_code,
    sales_province_name,
    source_type_name,
    CASE
            WHEN j.super_class='1'
                THEN '供应商订单'
            WHEN super_class='2'
                THEN '供应商退货订单'
            WHEN super_class='3'
                THEN '配送订单'
            WHEN super_class='4'
                THEN '返配订单'
                ELSE super_class
        END super_class_name  ,
    dc_code,
    shop_name,
    settle_location_code,
    goods_code,
    goods_name,
    unit_name,
    brand_name,
    division_code,
    division_name,
    department_id,
    department_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    j.supplier_code,
    vendor_name,
    j.send_dc_code,
    send_dc_name,
    local_purchase_flag,
    business_type_name,
    order_qty,
    order_price1,
    order_price2,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    receive_sdt,
    order_create_date,      --订单日期
    yh_reuse_tag,
    daily_source,   --日采标识
    pick_gather_flag,  --已拣代收
    urgency_flag,      --紧急补货
    has_change  ,---有无变更 
    entrust_outside,   --委外标识 
    order_business_type ,    --业务类型 基地订单标识  
    order_type ,         -- 订单类型
    extra_flag,         --补货标识
    timeout_cancel_flag,  -- 超时订单取消
    order_goods_status,
    case when purpose ='07' then '20' when yh_reuse_tag='是' then '21' when  division_code in ('11','10') then '11' when  division_code in ('12','13','14','15') then '12' end supplier_type_code ,
    case when purpose ='07' then 'BBC' when yh_reuse_tag='是' then '复用供应商' when  division_code in ('11','10') then '生鲜' when  division_code in ('12','13','14','15') then '食百' end  supplier_type_name,
    purpose,
    purpose_name,
    current_timestamp(),
    substr(receive_sdt,1,6) mon
from csx_tmp.temp_order_table j 
left join  
(select company_code,supplier_code,yh_reuse_tag 
from csx_tmp.ads_fr_r_m_supplier_reuse 
where months=substr(${hiveconf:e_date},1,6) ) s on j.company_code=s.company_code and j.supplier_code=s.supplier_code
left join
(select 
    shop_id as send_dc_code,
    shop_name as send_dc_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
) m on if(j.send_dc_code='',0,j.send_dc_code)=m.send_dc_code
;

drop table csx_tmp.temp_order_dtl;
create temporary table csx_tmp.temp_order_dtl as 
select  sdt,
    purchase_org,
    purchase_org_name,
    -- sales_region_code,
    -- sales_region_name ,
    order_code,
    receive_no,
    batch_code,
    sales_province_code,
    sales_province_name,
    source_type_name,
   super_class_name  ,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    unit_name,
    brand_name,
    division_code,
    division_name,
    department_id,
    department_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    a.supplier_code,
    vendor_name,
    send_dc_code,
    send_dc_name,
    settle_location_code,
    local_purchase_flag,
    business_type_name,
    order_qty,
    order_price1,
    order_price2,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    receive_sdt,
    order_create_date,
    yh_reuse_tag,
    daily_source,      --日采标识
    pick_gather_flag,  --已拣代收
    urgency_flag,      --紧急补货
    has_change  ,      ---有无变更 
    entrust_outside,   --委外标识 
    order_business_type ,    --业务类型 基地订单标识
    order_type ,         -- 订单类型
    extra_flag,         --补货标识
    timeout_cancel_flag,  -- 超时订单取消
    supplier_type_code ,
    supplier_type_name,
    business_owner_code ,       --业态归属
    business_owner_name ,       --业态归属名称
    special_customer ,          --专项客户
    borrow_flag ,               --是否借资质
    direct_trans_flag,          --是否直供
    supplier_classify_code,     --供应商类型编码 0：基础供应商   1:农户供应商
    a.order_goods_status,
    purpose,
    purpose_name ,
    current_timestamp(),
    mon
from csx_tmp.temp_purch_table a 
left join 
(select supplier_code,
        purchase_org_code,  
       business_owner_code  ,     -- 业态归属编码
       business_owner_name ,      --业态归属名称
       special_customer  ,        --专项客户
       borrow_flag ,              -- 是否借用
       direct_trans_flag,           -- 是否直供
       supplier_classify_code       -- 供应商类型编码  0：基础供应商   1:农户供应商
from csx_dw.dws_basic_w_a_supplier_purchase_info where sdt='current'
group by  supplier_code,
          purchase_org_code,  
          business_owner_code  , 
          business_owner_name ,  
          special_customer  ,    
          borrow_flag ,          
          direct_trans_flag,       
          supplier_classify_code   ) b on a.supplier_code=b.supplier_code and a.purchase_org=b.purchase_org_code
;

-- SHOW CREATE TABLE  csx_tmp.temp_order_dtl;


insert overwrite table  csx_tmp.report_fr_r_m_financial_purchase_detail partition(months)
select 
    sdt,
    purchase_org,
    purchase_org_name,
    order_code,
    receive_no,
    batch_code,
    sales_province_code,
    sales_province_name,
    source_type_name,
    super_class_name  ,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    unit_name,
    brand_name,
    division_code,
    division_name,
    department_id,
    department_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    a.supplier_code,
    vendor_name as supplier_name,
    send_dc_code,
    send_dc_name,
    settle_location_code,
    local_purchase_flag,
    business_type_name,
    order_qty,
    order_price1,
    order_price2,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    receive_sdt,
    order_create_date,
    yh_reuse_tag,
    daily_source,      --日采标识
    pick_gather_flag,  --已拣代收
    urgency_flag,      --紧急补货
    has_change  ,      ---有无变更 
    entrust_outside,   --委外标识 
    order_business_type ,    --业务类型 基地订单标识 
    order_type ,         -- 订单类型
    extra_flag,         --补货标识
    timeout_cancel_flag,  -- 超时订单取消
    joint_purchase,
    supplier_type_code ,
    supplier_type_name,
    business_owner_code , 
    business_owner_name , 
    special_customer , 
    borrow_flag ,
    direct_trans_flag,
    supplier_classify_code,
    order_goods_status,
    purpose,
    purpose_name ,
    current_timestamp(),
    mon
from csx_tmp.temp_order_dtl a
left join 
(select vendor_id,joint_purchase
from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id
;
