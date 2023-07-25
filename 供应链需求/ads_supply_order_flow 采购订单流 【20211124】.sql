--ads_supply_order_flow 采购订单流【20211124】
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET hive.optimize.sort.dynamic.partition=true;
 

 set sdate='20210101';
 set edate='20210501';
 
 drop table if exists csx_tmp.temp_scm_header ;
 create temporary table csx_tmp.temp_scm_header as 
 select  
   sdt,
   order_type, --订单类型
   a.order_code        ,
   shipped_order_code,  --出库单号
   received_order_code, --入库单号
   source_order_code   ,
   super_class         ,
   original_order_code ,
   a.link_order_code   ,
   source_location_code as shipped_location_code      ,
   source_location_name as shipped_location_name      ,
   target_location_code as receive_location_code      ,
   target_location_name as receive_location_name      ,
   settle_location_code ,
   settle_location_name ,
   supplier_code        ,
   supplier_name        ,
   a.goods_code         ,
   goods_name           ,
   goods_bar_code as bar_code             ,
   spec                 ,
   pack_qty             ,
   unit                 ,
   category_code        ,
   category_name        ,
   category_large_code  ,
   category_large_name  ,
   category_middle_code ,
   category_middle_name ,
   category_small_code  ,
   category_small_name  ,
   header_status  as order_status ,
   joint_purchase_flag, --  是否联采
   items_close_time,    --  明细关单时间
   source_type            ,
   case when source_type='1'     then '采购导入'
        when source_type='2'     then '直送'
        when source_type='3'     then '一键代发'
        when source_type='4'     then '项目合伙人'
        when source_type='5'     then '无单入库'
        when source_type='6'     then '寄售调拨'
        when source_type='7'     then '自营调拨'
        when source_type='8'     then '云超采购'
        when source_type='9'     then '工厂采购'
        when source_type='10'    then '智能补货'
        when source_type='11'    then '商超直送'
        when source_type='12'    then 'WMS调拨'
        when source_type='13'    then '云超门店采购'
        when source_type='14'    then '临时地采'
        when source_type='15'    then '联营直送'
    else '其他' end   source_type_name       ,
   purchase_org_code      ,
   purchase_org_name      ,
   purchase_group_code    ,
   purchase_group_name    ,
   direct_flag            ,
   zm_direct_flag         ,
   customer_direct_flag   , --是否直送
   local_purchase_flag    , --是否地采
   is_compensation        ,
   addition_order_flag    ,
   system_status          ,
   last_delivery_date     ,
   return_reason_code     ,
   return_reason_name     ,
   create_by              ,
   create_time            ,
   update_time            ,
   fixed_price_type       ,
   tax_code               ,
   tax_rate               ,
   timeout_cancel_flag,
   pick_gather_flag ,		  -- 是否已拣代收0 不是已检代收 1 是
   urgency_flag	,		  --是否是紧急补货(0 否 1 是)
   has_change		,		  --有变更 0 无变更 1有变更
   entrust_outside	,		  --委外标识 0 非委外 1 委外
   performance_order_code,    --履约单号
   if(a.price_include_tax=0,price2_include_tax,price_include_tax) as order_price ,
   if(price_free_tax=0,price2_free_tax,price_free_tax) as no_tax_price,             --未税单价金额
   a.order_qty ,
   if(a.amount_include_tax =0,amount2_include_tax,amount_include_tax)       as order_amt  ,
   if(amount_free_tax=0,amount2_free_tax,amount_free_tax)  as order_amt_no_tax
 from
     csx_dw.dws_scm_r_d_order_detail a
 where
   a.sdt>=${hiveconf:sdate} and sdt<${hiveconf:edate}
 ;
 
 
 
 
 drop table if exists   csx_tmp.temp_entry_item ;
 create temporary table csx_tmp.temp_entry_item as 
 select
   order_code as receive_no                        ,
   '' as receive_batch_code                , --入库批次为空
   origin_order_code                               ,
   goods_code                                      ,
   produce_date                                    ,    --生产日期
   price       as receive_price                    ,
   (receive_qty) receive_qty                    ,
   (amount)      as receive_amt                 ,
   super_class as receive_super_class              ,
   sale_channel                                    ,
   compensation_type                               ,
   run_type                                        ,
   order_type_code    as receive_type_code         ,
   order_type_name         as receive_type              ,
   business_type as receive_business_type_code,
   business_type_name      as receive_business_type     ,
   to_date(receive_time) as receive_time                                    ,
   return_flag                                     ,
   close_time                                      ,
   receive_status,
   entity_flag          --实物退货标识
 from
  csx_dw.dws_wms_r_d_entry_detail
 where
   ((sdt>=${hiveconf:sdate} and sdt <= ${hiveconf:edate} )     or sdt='19990101')

;

-- 日期更改为关单日期，如果未关单取完成时间
 drop table if exists  csx_tmp.temp_shipped_item;
 create temporary table csx_tmp.temp_shipped_item as 
  SELECT 
       order_no AS shipped_no,
       '' AS shipped_batch_code,  --出库批次为空
       origin_order_no,
       goods_code,
       (shipped_qty)AS shipped_qty,
       price AS shipped_price,
       (shipped_amount) AS shipped_amt,
       super_class AS shipped_super_class,
       order_type_code as shipped_type_code,
       wms_order_type_name as shipped_type,
       business_type_code AS shipped_business_type_code,
       business_type_name AS shipped_business_type,
       if(sdt='19990101',regexp_replace(to_date(finish_time),'-',''),sdt) as shipped_date,
       sdt,
       status AS shipped_status
FROM csx_dw.dws_wms_r_d_ship_detail a 
 
WHERE (( sdt>=${hiveconf:sdate}
  AND sdt<=${hiveconf:edate})
     or sdt='19990101')
   --  and substr(order_type_code,1,1) !='S'

;
 
 insert overwrite table csx_tmp.ads_supply_order_flow partition    (sdt)
 select
   order_type, --订单类型
   purchase_org_code                                                 , --采购组织编码
   purchase_org_name                                                 , --采购组织名称
   a.order_code                                                      , -- 订单号
   source_order_code                                                 , --源单号
   super_class                                                       , -- '单据类型(1-供应商订单、2-供应商退货订单、3-配送订单、4-返配订单)'
   original_order_code                                               , -- '原订单号'
   a.link_order_code                                                 , --关联单号
   shipped_location_code                                             , --发货地点编码
   shipped_location_name                                             , --发货地点名称
   receive_location_code                                             , --收货地点编码
   receive_location_name                                             , --收货地点名称
   settle_location_code                                              , --结算地点编码
   settle_location_name                                              , --结算地点名称
   supplier_code                                                     , --供应商编码
   supplier_name                                                     , --供应商名称
   a.goods_code                                                      , --商品编码
   goods_name                                                        , --商品名称
   bar_code                                                          , --商品条码
   spec                                                              , --规格
   pack_qty                                                          , --件装数
   unit                                                              , --单位
   category_code                                                     , --部类编码
   category_name                                                     , --部类名称
   purchase_group_code                                               , --课组编码
   purchase_group_name                                               , --课组名称
   category_large_code                                               , --大类编码
   category_large_name                                               , --大类名称
   category_middle_code                                              , --中类编码
   category_middle_name                                              , --中类名称
   category_small_code                                               , --小类编码
   category_small_name                                               , --小类名称
   fixed_price_type                                                  , --定价类型(1-移动平均价、2-系统调拨协议价、3-系统进价、4-批次价、5-手工价)
   tax_code                                                          , --税码
   tax_rate                                                          , --税率
   no_tax_price,             --未税单价金额
   a.order_price                                                     , -- 含税单价
   a.order_qty                                                       , -- 下单数量
   a.order_amt                                                       , --含税总金额
   a.order_amt_no_tax                                                , --未税订单金额
   nvl(b.receive_no ,'')                                  as receive_no                 , --入库单号
   nvl(b.receive_batch_code ,'')                          as receive_batch_code         , --收货批次号(为空)
   nvl(produce_date ,'')                                  as produce_date               , --生产日期
   coalesce(receive_qty ,0)                               as receive_qty                , --收货数量
   coalesce(receive_price ,0)                             as receive_price              , -- 入库单价
   coalesce(receive_amt ,0)                               as receive_amt                , --入库金额
   coalesce(receive_super_class ,'')                      as receive_super_class        , --收货类型
   nvl(sale_channel ,'')                                  as sale_channel               , --销售渠道
   nvl(compensation_type ,'')                             as compensation_type          , --申偿类型
   nvl(run_type ,'')                                      as run_type                   , --经营类型
   nvl(receive_type_code ,'')                             as receive_type_code          , --入库类型
   nvl(receive_type ,'')                                  as receive_type               , --入库类型名称
   nvl(receive_business_type_code ,'')                    as receive_business_type_code , --入库业务类型
   nvl(receive_business_type ,'')                         as receive_business_type      , --入库业务类型名称
   nvl(regexp_replace(to_date(b.receive_time),'-',''),'') as receive_date               , --收货时间
   nvl(regexp_replace(to_date(b.close_time),'-','') ,'')  as receive_close_date         , --入库关单时间
   nvl(receive_status ,'')                                as receive_status             , --入库单据状态
   -- 以下出库数据
   coalesce(shipped_no , '')                              as shipped_no                , --出库单号
   coalesce(c.shipped_batch_code, '')                     as shipped_batch_code        , --出库批次(为空)
   coalesce(c.shipped_price , 0)                          as shipped_price             , --出库单价
   coalesce(c.shipped_amt , 0)                            as shipped_amt               , --出库金额
   coalesce(shipped_qty , 0)                              as shipped_qty               , --出库数量
   coalesce(nvl(c.shipped_super_class,'') , '')           as shipped_super_class       , -- 出库类型
   coalesce(shipped_type_code , '')                       as shipped_type_code         , -- 出库单据类型
   coalesce(shipped_type , '')                            as shipped_type              , --出库单据类型名称
   coalesce(shipped_business_type_code , '')              as shipped_business_type_code, --出库业务类型
   coalesce(c.shipped_business_type , '')                 as shipped_business_type     , --出库业务类型名称
   shipped_date                                                  as shipped_date              , --出库日期
   coalesce(shipped_status ,'') shipped_status,                                           -- 出库单据状态
   order_status         ,                                                                   --单据状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)
   source_type          ,                                                                   --来源类型(1-采购导入、2-直送、3-一键代发、4-项目合伙人、5-无单入库、6-寄售调拨、7-自营调拨、8-云超采购、9-工厂采购)
   source_type_name     ,                                                                   --来源类型名称
   direct_flag          ,                                                                   --是否直通(0-否、1-是)
   zm_direct_flag       ,                                                                   --是否账面直通(0-否、1-是)
   customer_direct_flag ,                                                                   --是否直送(0-否、1-是)
   local_purchase_flag  ,                                                                   --是否地采(0-否、1-是)
   is_compensation      ,                                                                   --是否申偿(1是,0否)
   addition_order_flag  ,                                                                   --是否加配单(0-否、1-是)
   timeout_cancel_flag ,                                                            --是否超时取消'是否超时取消(0-否、1-是)', 
   joint_purchase_flag,                                                             --是否联采商品
   joint_purchase,                                                                  --是否联采供应商
   system_status        ,                                                                   --系统状态(1-订单已提交、2-已同步WMS、3-WMS已回传、4-修改已提交、5-修改已同步WMS、6-修改成功、7-修改失败)
   last_delivery_date   ,                                                                   --预计到货日期
   return_reason_code   ,                                                                   --退货原因编码
   return_reason_name   ,                                                                   --退货原因名称
   pick_gather_flag ,		  -- 是否已拣代收0 不是已检代收 1 是
   urgency_flag	,		  --是否是紧急补货(0 否 1 是)
   has_change		,		  --有变更 0 无变更 1有变更
   entrust_outside	,		  --委外标识 0 非委外 1 委外
   performance_order_code,    --履约单号
   create_by            ,                                                                   --创建者
   create_time          ,                                                                   -- 创建时间
   update_time          ,                                                                   -- 更新时间
   case when super_class in ('1','2') then settle_location_code when super_class ='3'then receive_location_code
       when super_class ='4' then shipped_location_code end location_code,
   case when super_class in ('1','2') then settle_location_name when super_class ='3'then receive_location_name
        when super_class ='4' then shipped_location_name end location_name ,
   a.sdt
 from  csx_tmp.temp_scm_header   a  --订单
 left join  csx_tmp.temp_entry_item b  --入库单
    on a.received_order_code    =b.receive_no     and a.goods_code=b.goods_code
 left join  csx_tmp.temp_shipped_item c   --出库单
    on   a.shipped_order_code  =c.shipped_no and a.goods_code=c.goods_code
 left join 
 (select vendor_id ,
     joint_purchase   --是否联采供应商
 from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') d on a.supplier_code=d.vendor_id
   ;
   
 
 --插入DW库
 insert overwrite table csx_dw.ads_supply_order_flow partition    (sdt)  
 select * from  csx_tmp.ads_supply_order_flow;


-- csx_tmp.ads_supply_order_flow 这个表数据同步csx_dw.ads_supply_order_flow

CREATE TABLE `csx_dw.ads_supply_order_flow`(
   order_type string COMMENT '订单类型(0-普通供应商订单 1-囤货订单 2-日采订单 3-计划订单)',
  `purchase_org_code` string COMMENT '采购组织编码', 
  `purchase_org_name` string COMMENT '采购组织名称', 
  `order_code` string COMMENT '订单号', 
  `source_order_code` string COMMENT '原单号', 
  `super_class` string COMMENT '单据类型(1-供应商订单、2-供应商退货订单、3-配送订单、4-返配订单)', 
  `original_order_code` string COMMENT '原订单号', 
  `link_order_code` string COMMENT '关联单号', 
  `shipped_location_code` string COMMENT '发货地点编码', 
  `shipped_location_name` string COMMENT '发货地点名称', 
  `receive_location_code` string COMMENT '收货地点编码', 
  `receive_location_name` string COMMENT '收货地点名称', 
  `settle_location_code` string COMMENT '结算地点编码', 
  `settle_location_name` string COMMENT '结算地点名称', 
  `supplier_code` string COMMENT '供应商编码', 
  `supplier_name` string COMMENT '供应商名称', 
  `goods_code` string COMMENT '商品编码', 
  `goods_name` string COMMENT '商品名称', 
  `bar_code` string COMMENT '商品条码', 
  `spec` string COMMENT '规格', 
  `pack_qty` decimal(26,6) COMMENT '件装数', 
  `unit` string COMMENT '单位', 
  `category_code` string COMMENT '部类编码', 
  `category_name` string COMMENT '部类名称', 
  `purchase_group_code` string COMMENT '采购组编码', 
  `purchase_group_name` string COMMENT '采购组名称', 
  `category_large_code` string COMMENT '大类编码', 
  `category_large_name` string COMMENT '大类名称', 
  `category_middle_code` string COMMENT '中类编码', 
  `category_middle_name` string COMMENT '中类名称', 
  `category_small_code` string COMMENT '小类编码', 
  `category_small_name` string COMMENT '小类名称', 
  `fixed_price_type` int COMMENT '定价类型(1-移动平均价、2-系统调拨协议价、3-系统进价、4-批次价、5-手工价)', 
  `tax_code` string COMMENT '税码', 
  `tax_rate` decimal(26,6) COMMENT '税率', 
   no_tax_price decimal(26,6) comment '未税单价',
  `order_price` decimal(26,6) COMMENT '含税单价', 
  `order_qty` decimal(26,6) COMMENT '下单数量', 
  `order_amt` decimal(26,6) COMMENT '含税订单金额', 
  `order_amt_no_tax` decimal(26,6) COMMENT '未税总金额', 
  `receive_no` string COMMENT '入库单号', 
  `receive_batch_code` string COMMENT '收货批次号', 
  `produce_date` string COMMENT '生产日期', 
  `receive_qty` decimal(26,6) COMMENT '收货数量', 
  `receive_price` decimal(26,6) COMMENT '价格', 
  `receive_amt` decimal(26,6) COMMENT '金额', 
  `receive_super_class` int COMMENT '收货类型 1-正常收货 2-无单收货 3-异常收货地点', 
  `sale_channel` int COMMENT '销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端', 
  `compensation_type` int COMMENT '申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿', 
  `run_type` int COMMENT '经营类型 1-联营 2-自营', 
  `receive_type_code` string COMMENT '入库类型', 
  `receive_type` string COMMENT '入库类型名称', 
  `receive_business_type_code` string COMMENT '收货业务类型', 
  `receive_business_type` string COMMENT '收货业务类型名称', 
  `receive_date` string COMMENT '最早收货日期', 
  `receive_close_date` string COMMENT '关单日期', 
  `receive_status` string COMMENT '收货状态 0-待收货 1-收货中 2-已关单 3-已取消', 
  `shipped_no` string COMMENT '出库单号 规则OU+年（2位）+月（2位）+日（2位）+6位流水', 
  `shipped_batch_code` string COMMENT '出库批次', 
  `shipped_price` decimal(26,6) COMMENT '出库价格', 
  `shipped_amt` decimal(26,6) COMMENT '出库金额', 
  `shipped_qty` decimal(26,6) COMMENT '批次出库数量', 
  `shipped_super_class` int COMMENT '类型 1-正常出库单 2-异常出库单', 
  `shipped_type_code` string COMMENT '订单类型', 
  `shipped_type` string COMMENT '订单类型名称', 
  `shipped_business_type_code` string COMMENT '业务类型', 
  `shipped_business_type` string COMMENT '业务类型名称', 
  `shipped_date` string COMMENT '出库日期', 
  `shipped_status` string COMMENT '状态 0-初始 1-已集波 2-分配中 3-已分配 4-拣货中 5-拣货完成 6-已发货 7-已完成', 
  `order_status` int COMMENT '状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)', 
  `source_type` int COMMENT '来源类型(1-采购导入、2-直送、3-一键代发、4-项目合伙人、5-无单入库、6-寄售调拨、7-自营调拨、8-云超采购、9-工厂采购、10-智能补货、11-商超直送、12-WMS调拨、13-云超门店采购、14-临时地采、15-联营直送)', 
  `source_type_name` string COMMENT '来源类型名称', 
  `direct_flag` string COMMENT '是否直通(0-否、1-是)', 
  `zm_direct_flag` string COMMENT '是否账面直通(0-否、1-是)', 
  `customer_direct_flag` string COMMENT '是否直送(0-否、1-是)', 
  `local_purchase_flag` string COMMENT '是否地采(0-否、1-是)', 
  `is_compensation` int COMMENT '是否申偿(1是,0否)', 
  `addition_order_flag` int COMMENT '是否加配单(0-否、1-是)', 
   timeout_cancel_flag int COMMENT '是否超时取消(0-否、1-是)', 
   joint_purchase_flag int comment '是否地采',  
   joint_supplier_flag int comment '供应商是否集采',       
  `system_status` int COMMENT '系统状态(1-订单已提交、2-已同步WMS、3-WMS已回传、4-修改已提交、5-修改已同步WMS、6-修改成功、7-修改失败)', 
  `last_delivery_date` string COMMENT '预计到货日期', 
  `return_reason_code` string COMMENT '退货原因编码', 
  `return_reason_name` string COMMENT '退货原因名称', 
    pick_gather_flag  int comment'是否已拣代收0 不是已检代收 1 是',
	urgency_flag	int comment '是否是紧急补货(0 否 1 是)',
	has_change		int comment '有变更 0 无变更 1有变更',
	entrust_outside	int comment '委外标识 0 非委外 1 委外',
	performance_order_code string comment '履约单号',
  `order_create_by` string COMMENT '订单创建者', 
  `order_create_time` string COMMENT '订单创建时间', 
  `order_update_time` string COMMENT '订单更新时间', 
  `location_code` string COMMENT '地点编码，经过转换，super_class类型1、2取结算地点，3 收货地点、4 发货地点', 
  `location_name` string COMMENT '地点编码，经过转换，super_class类型1、2取结算地点，3 收货地点、4 发货地点')
COMMENT '供应链订单流 sdt 订单更新日期'
PARTITIONED BY ( 
  `sdt` string COMMENT '订单创建日期')
 
STORED AS parquet 
 
