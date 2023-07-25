-- ******************************************************************** 
-- @功能描述：采购订单流
-- @创建者： 彭承华 
-- @创建者日期：2022-08-30 13:46:16 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 

drop table csx_analyse_tmp.csx_analyse_tmp_entry_00;
create temporary table csx_analyse_tmp.csx_analyse_tmp_entry_00 as
select   order_no,
    receive_no,
    batch_code,
    dc_code,
    receive_dc_code,
    send_dc_code,
    business_type,
    business_type_name,
    goods_code,
    supplier_code,
    (receive_qty) receive_qty,
    (receive_amt) receive_amt,
    (no_tax_receive_amt) as no_tax_receive_amt,
    (shipped_qty) shipped_qty,
    (shipped_amt) shipped_amt,
    (no_tax_shipped_amt) as no_tax_shipped_amt,
    receive_sdt
from 
(select original_order_code	 order_no,
    receive_dc_code	 as dc_code,
    receive_dc_code,
    send_dc_code  as send_dc_code,
    order_code receive_no,
    batch_code ,
    business_type_code as business_type,
    business_type_name,
    goods_code,
    supplier_code,
    (receive_qty) receive_qty,
    (price/(1+tax_rate/100)*receive_qty) as no_tax_receive_amt,
    (price*receive_qty) as receive_amt,
    0 shipped_qty,
    0 shipped_amt,
    0 no_tax_shipped_amt,
    sdt receive_sdt
from csx_dws.csx_dws_wms_entry_batch_di a 
where sdt>='${sdate}' and  sdt<='${edate}'
    and entry_type like 'P%'
    and receive_status in ('2')
union all 
select original_order_code order_no, 
    send_dc_code  as dc_code,
    receive_dc_code,
    send_dc_code  as send_dc_code,
    order_code as receive_no,
    batch_code,
    business_type_code as business_type,
    business_type_name,
    goods_code,
    supplier_code,
    0 receive_qty,
    0 no_tax_receive_amt,
    0 receive_amt,
    (shipped_qty) shipped_qty,
    (price*shipped_qty) as shipped_amt,
    (price/(1+tax_rate/100)*shipped_qty) as no_tax_shipped_amt,
    if(sdt='20000101' ,regexp_replace(to_date(finish_time ),'-',''),sdt)  receive_sdt
from csx_dws.csx_dws_wms_shipped_batch_di
where  ((sdt>='${sdate}' and  sdt<='${edate}' ) 
        or (sdt='20000101' and regexp_replace(to_date(finish_time ),'-','') between '${sdate}' and '${edate}')
    )
    and shipped_type like 'P%'
    and status!=9
) a 
where substr(dc_code,1,1) !='L'
;


drop table csx_analyse_tmp.csx_analyse_tmp_order_table ;
create temporary table csx_analyse_tmp.csx_analyse_tmp_order_table as 
select 
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    purchase_org,
    purchase_org_name,
    performance_region_code,
    performance_region_name,
    d.company_code,
    d.company_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    b.order_code,
    super_class,
    source_type as source_type_code,
    source_type_name,
    local_purchase_flag,
    a.order_no,
    receive_no,
    a.batch_code,
    dc_code,
    d.shop_name,
    receive_dc_code,
   -- j.shop_name as receive_dc_name,
    settle_dc_code,
    settle_dc_name,
    settle_company_code,
    settle_company_name,
    business_type,
    business_type_name,
    a.goods_code,
    supplier_code,
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
    receive_sdt ,    --  发货日期&收货日期
    order_create_date ,      --  订单创建日期
    daily_source,               --  日采标识
    pick_gather_flag,  --  已拣代收
    urgency_flag,      -- 紧急补货
    has_change  ,       -- 有无变更 
    entrust_outside,   -- 委外标识 
    order_business_type ,    -- 业务类型 基地订单标识    
    order_type ,         -- 订单类型
    extra_flag,         -- 补货标识
    timeout_cancel_flag,  -- 超时订单取消
    joint_purchase_flag,
    order_goods_status   --订单商品状态状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)
from csx_analyse_tmp.csx_analyse_tmp_entry_00 a 
left join 
(select  order_code,
    super_class,
    source_type,
    settle_location_code as settle_dc_code,                     -- 发货 DC
    shop_name as settle_dc_name,
    s.company_code as settle_company_code,
    s.company_name as settle_company_name,
    config_value as source_type_name      ,                     -- 订单来源名称 source_type_name,
    demand_source,                                              -- 需求来源(1-m端[商超] 2-b端[企业购])
    channel,                                                    -- '来源渠道（pc app)
    a.goods_code,
    a.order_qty,                                                -- 订单数量
    price_include_tax as order_price1 ,                         -- 单价1
    price2_include_tax as order_price2,                         -- 单价2     
    local_purchase_flag,
    daily_source,                                               -- 日采标识
    pick_gather_flag,                                           --   已拣代收
    urgency_flag,                                               -- 紧急补货
    has_change  ,                                               -- 有无变更 
    entrust_outside,                                            -- 委外标识 
    business_type order_business_type ,                         -- 业务类型 1 基地订单标识
    to_date(a.create_time) as order_create_date,                -- 订单创建时间
    order_type ,                                                -- 订单类型
    extra_flag,
    timeout_cancel_flag,
    a.items_status as order_goods_status,
    joint_purchase_flag                                       -- 联采标识(0-否、1-是)
    from csx_dws.csx_dws_scm_order_detail_di  a
     left join 
 (select config_key,config_value
    from csx_ods.csx_ods_csx_b2b_scm_scm_configuration_df a 
 where a.config_type = 'PURCHASE_ORDER_SOURCE_TYPE'
    and sdt=regexp_replace(date_sub(current_date(),1),'-','')) b on a.source_type=b.config_key
    left join 
(select 
    shop_code ,
    shop_name ,
    company_code,
    company_name
from csx_dim.csx_dim_shop
 where sdt='current'    ) s on a.settle_location_code=s.shop_code
        where super_class in ('2','1')          -- 供应商配送、供应商入库
        and sdt>='20190101'
)b on a.order_no=b.order_code and a.goods_code=b.goods_code
left join 
(select 
    purchase_org,
    purchase_org_name,
    performance_region_code,
    performance_region_name,
    shop_code ,
    shop_name ,
    company_code ,
    company_name ,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    performance_province_code ,
    performance_province_name ,
    performance_city_code , 
    performance_city_name 
from csx_dim.csx_dim_shop
 where sdt='current'    ) d on a.dc_code=d.shop_code

;

drop table  csx_analyse_tmp.csx_analyse_tmp_order_table_01;
create temporary table csx_analyse_tmp.csx_analyse_tmp_order_table_01 as 
select 
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    purchase_org,
    purchase_org_name,
    performance_region_code,
    performance_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,    
    order_code,
    super_class,
    source_type_code,
    source_type_name,
    local_purchase_flag,
    a.order_no,
    receive_no,
    a.batch_code,
    dc_code,
    a.shop_name,
    receive_dc_code,
    j.shop_name as receive_dc_name,
    settle_dc_code,
    settle_dc_name,
    settle_company_code,
    settle_company_name,
    business_type,
    business_type_name,
    a.goods_code,
    a.supplier_code,
    supplier_name,
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
    receive_sdt ,               -- 发货日期&收货日期
    order_create_date,          -- 订单创建日期
    daily_source,               -- 日采标识
    pick_gather_flag,           -- 已拣代收
    urgency_flag,               -- 紧急补货
    has_change  ,               -- 有无变更 
    entrust_outside,            -- 委外标识 
    order_business_type ,       -- 业务类型 基地订单标识    
    order_type ,                -- 订单类型
    extra_flag,                 -- 补货标识
    timeout_cancel_flag,        -- 超时订单取消
    joint_purchase_flag,        -- 集采订单标识
    joint_purchase as supplier_joint ,            -- 供应商集采标识
    business_owner_code,        -- 业态归属
    business_owner_name,        -- 业态归属
    special_customer,           -- 专项
    supplier_classify_code,     -- 供应商类型
    supplier_classify_name,     -- 供应商类型 
    borrow_flag,                -- 是否借用
    lock_flag,                  -- 是否锁定
    frozen_flag,                -- 是否冻结
    finance_frozen,             -- 是否财务冻结
    direct_trans_flag,           -- 是否直供
    order_goods_status,         -- 订单商品状态状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)
    purpose,
    purpose_name
from csx_analyse_tmp.csx_analyse_tmp_order_table a 
left join 
(select supplier_code,
        supplier_name,
        joint_purchase 
    from csx_dim.csx_dim_basic_supplier 
        where sdt='current') b on a.supplier_code=b.supplier_code
left join
(select supplier_code,
        purchase_org_code,
        business_owner_code,        -- 业态归属
        business_owner_name,        -- 业态归属
        special_customer,           -- 专项
        supplier_classify_code,     -- 供应商类型
        supplier_classify_name,     -- 供应商类型 
        borrow_flag,                -- 是否借用
        lock_flag,                  -- 是否锁定
        frozen_flag,                -- 是否冻结
        finance_frozen,             -- 是否财务冻结
        direct_trans_flag           -- 是否直供
    from csx_dim.csx_dim_basic_supplier_purchase
        where sdt='current') c on a.supplier_code=c.supplier_code
         and a.purchase_org=c.purchase_org_code
  left join 
 (select 
     shop_code ,
     shop_name  
 from csx_dim.csx_dim_shop
  where sdt='current'    ) j on if(a.receive_dc_code='',0,receive_dc_code)=j.shop_code
;
        


insert overwrite table  csx_analyse.csx_analyse_scm_purchase_order_flow_di partition(sdt)
select 
    purchase_org purchase_org_code,
    purchase_org_name,
    order_code,
    receive_no,
    batch_code,
    performance_region_code,
    performance_region_name ,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    province_code,
    province_name,
    city_code,
    city_name,
    source_type_code,
    source_type_name,
    super_class  as super_class_code,
    CASE
            WHEN super_class='1'
                THEN '供应商订单'
            WHEN super_class='2'
                THEN '供应商退货订单'
            WHEN super_class='3'
                THEN '调拨订单'
            WHEN super_class='4'
                THEN '调拨退货订单'
                ELSE super_class
        END super_class_name    ,
    a.dc_code,
    a.shop_name as dc_name,
    a.goods_code,
    goods_bar_code,
    goods_name,
    `spu_goods_code`  , 
    `spu_goods_name`  , 
    coalesce(`spu_goods_status`,'')spu_goods_status,
    unit_name,
    brand_name,
    division_code,
    division_name,
    purchase_group_code,
    purchase_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    csx_purchase_level_code,  -- '产品采购级别编号,来自码表，彩食鲜自己的采购级别', 
    csx_purchase_level_name,  --
    a.supplier_code,
    supplier_name,
    send_dc_code,
    coalesce(j.shop_name,'') send_dc_name,
    settle_dc_code,
    settle_dc_name,
    settle_company_code,
    settle_company_name,
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
    daily_source,      -- 日采标识
    pick_gather_flag,  -- 已拣代收
    urgency_flag,      -- 紧急补货
    has_change  ,      -- 有无变更 
    entrust_outside,   -- 委外标识 
    order_business_type ,    -- 业务类型 基地订单标识 
    order_type ,         -- 订单类型
    extra_flag,         -- 补货标识
    timeout_cancel_flag,  -- 超时订单取消
    joint_purchase_flag,
    supplier_joint,
    business_owner_code , 
    business_owner_name , 
    special_customer , 
    coalesce(borrow_flag ,'')borrow_flag,
    direct_trans_flag,
    supplier_classify_code,
    `valuation_category_code`  , 
	`valuation_category_name`  ,
    order_goods_status,
    purpose,
    purpose_name ,
    if(is_dc_tag=1,1,0) as is_supply_stock_tag,    -- 是否供应链仓
    short_name as central_purchase_short_name,                -- 集采简称
    case when is_flag=0 
        and start_date < receive_sdt
        and is_dc_tag=1 
        and enable_date < receive_sdt
        and supplier_joint=1
        then 1 else 0 end is_central_order_tag,  -- 集采品类、生效时间小于当前 时间，供应链仓，生效时间小于当前时间，供应商标识集采
    current_timestamp(),
    receive_sdt
from csx_analyse_tmp.csx_analyse_tmp_order_table_01 a
join 
(SELECT goods_code,
       goods_bar_code,
       goods_name,
      `spu_goods_code`  , 
      `spu_goods_name`  , 
      `spu_goods_status`,
       unit_name,
       brand_name,
       purchase_group_code,
       purchase_group_name,
       division_code,
       division_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       a.classify_small_code,
       classify_small_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       `valuation_category_code`  , 
	   `valuation_category_name`  , 
       `csx_purchase_level_code`  , -- '产品采购级别编号,来自码表，彩食鲜自己的采购级别', 
       `csx_purchase_level_name` ,-- '产品采购级别名称,01-全国商品,02-一般商品,03-OEM商品', 
       short_name,
       start_date,
       end_date,
       is_flag
FROM   csx_dim.csx_dim_basic_goods a 
left join
(select short_name,
        classify_small_code,
        start_date,
        end_date,
        is_flag
     from csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
    )  b on a.classify_small_code=b.classify_small_code
WHERE sdt='current') b on a.goods_code=b.goods_code
left join 
 (select dc_code,
    regexp_replace(to_date(enable_time),'-','') enable_date ,
    '1' is_dc_tag
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current'
 ) c on a.dc_code=c.dc_code
 left join 
(select 
    shop_code ,
    shop_name  
from csx_dim.csx_dim_shop
 where sdt='current'    ) j on if(a.send_dc_code='',0,send_dc_code)=j.shop_code
where source_type_code is not null


;

