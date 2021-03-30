
set orderedate ='2021-03-11';
select  regexp_replace(to_date(date_sub(${hiveconf:orderedate},30)),'-','') ;
select * from csx_tmp.tmp_sale;
show create table csx_dw.ads_supply_order_flow ;
create table csx_tmp.tmp_sale as 
select
	purchase_org_code,
	purchase_org_name,
	location_code,
	location_name,
    super_class ,
	order_code,
	link_order_code,     
	receive_location_code                             ,
	receive_location_name                             ,
	settle_location_code                              ,
	settle_location_name                              ,
	shipped_location_code                             ,
	shipped_location_name                             ,
	supplier_code                                     ,
	supplier_name                                     ,
	a.goods_code                                        ,
	bar_code                                          ,
	goods_name                                        ,
	spec                                              ,
	pack_qty                                          ,
	unit                                              ,
	category_code                                     ,
	category_name                                     ,
	classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
	purchase_group_code                               ,
	purchase_group_name                               ,
	category_large_code                               ,
	category_large_name                               ,
	category_middle_code                              ,
	category_middle_name                              ,
	a.category_small_code                               ,
	category_small_name                               ,
	tax_code                                          ,
	tax_rate                                          ,
	order_price                   ,
	order_qty                                         ,
	order_amt                                         ,
	receive_qty                                       ,
	receive_amt                                       ,
    no_tax_receive_amt,
	shipped_date ,
    shipped_qty                                  ,
    shipped_amt                                 ,
    no_tax_shipped_amt,	
	source_type                                                                   ,
    source_type_name ,
    order_status ,
	local_purchase_flag,	
	receive_date ,
	order_create_date,
    order_update_date ,
    avg_sales_qty,
    order_create_by
from
(
select
	purchase_org_code,
	purchase_org_name,
	location_code,
	location_name,
	case
		when super_class='1'
			then '供应商订单'
		when super_class='2'
			then '供应商退货订单'
		when super_class='3'
			then '配送订单'
		when super_class='4'
			then '返配订单'
	end super_class ,
	order_code,
	link_order_code,     
	receive_location_code                             ,
	receive_location_name                             ,
	settle_location_code                              ,
	settle_location_name                              ,
	shipped_location_code                             ,
	shipped_location_name                             ,
	supplier_code                                     ,
	supplier_name                                     ,
	goods_code                                        ,
	bar_code                                          ,
	goods_name                                        ,
	spec                                              ,
	pack_qty                                          ,
	unit                                              ,
	category_code                                     ,
	category_name                                     ,
	classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
	purchase_group_code                               ,
	purchase_group_name                               ,
	category_large_code                               ,
	category_large_name                               ,
	category_middle_code                              ,
	category_middle_name                              ,
	a.category_small_code                               ,
	category_small_name                               ,
	tax_code                                          ,
	tax_rate                                          ,
	order_amt/ order_qty as order_price                   ,
	order_qty   as order_qty                                         ,
	order_amt   as order_amt                                         ,
	receive_qty as receive_qty                                       ,
	receive_amt as receive_amt                                       ,
	receive_amt/(1+tax_rate/100) as no_tax_receive_amt,
	shipped_date ,
	shipped_qty   as   shipped_qty                                  ,
	shipped_amt      as shipped_amt                                 ,
	shipped_amt/(1+tax_rate/100) as no_tax_shipped_amt,	
	source_type                                                                   ,
	concat(cast(source_type as string) ,' ', source_type_name)as source_type_name ,
    case
		when order_status=1
			then '已创建'
		when order_status=2
			then '已发货'
		when order_status=3
			then '部分入库'
		when order_status=4
			then '已完成'
		when order_status=5
			then '已取消'
	end order_status ,
	if(local_purchase_flag='0','否','是') as local_purchase_flag,	
	receive_date ,
	to_date(order_create_time) order_create_date,
	to_date(order_update_time) order_update_date,
	order_create_by 
from
	csx_dw.ads_supply_order_flow a 
join 
(select shop_code,product_code from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' and joint_purchase_flag=1) m on a.location_code=m.shop_code and a.goods_code=m.product_code
left outer join
(select classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
    category_small_code 
from csx_dw.dws_basic_w_a_manage_classify_m 
where sdt='current') b on a.category_small_code=b.category_small_code
where
	sdt  = regexp_replace(to_date(${hiveconf:orderedate}),'-','')
) a 
left join 
(select dc_code,goods_code,sum(sales_qty)/30 as avg_sales_qty 
    from csx_dw.dws_sale_r_d_detail where sdt  >=  regexp_replace(to_date(date_sub(${hiveconf:orderedate},30)),'-','') 
        and sdt<=  regexp_replace(to_date(${hiveconf:orderedate}),'-','')
    group by  dc_code,goods_code) b on a.location_code=b.dc_code and a.goods_code=b.goods_code


;
drop table csx_tmp.report_supply_frozen_order_flow;
CREATE TABLE `csx_tmp.report_supply_frozen_order_flow`(
  `purchase_org_code` string COMMENT '采购组织编码', 
  `purchase_org_name` string COMMENT '采购组织名称', 
  `order_code` string COMMENT '订单号', 
  `source_order_code` string COMMENT '原单号', 
  `super_class_code` string COMMENT '单据类型(1-供应商订单、2-供应商退货订单、3-配送订单、4-返配订单)', 
  `super_class_name` string COMMENT '单据类型(1-供应商订单、2-供应商退货订单、3-配送订单、4-返配订单)', 
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
  classify_large_code string comment '管理分类一级',
   classify_large_name string comment '管理分类一级',
   classify_middle_code string comment '管理分类二级' ,
   classify_middle_name string comment '管理分类二级' ,
   classify_small_code string comment '管理分类三级',
   classify_small_name string comment '管理分类三级',
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
  `order_status_name` int COMMENT '状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)', 
  `source_type` int COMMENT '来源类型(1-采购导入、2-直送客户、3-一键代发、4-项目合伙人、5-无单入库、6-寄售调拨、7-自营调拨、8-云超采购、9-工厂采购)', 
  `source_type_name` string COMMENT '来源类型名称', 
  `direct_flag` string COMMENT '是否直通(0-否、1-是)', 
  `zm_direct_flag` string COMMENT '是否账面直通(0-否、1-是)', 
  `customer_direct_flag` string COMMENT '是否客户直送(0-否、1-是)', 
  `local_purchase_flag` string COMMENT '是否地采(0-否、1-是)', 
  `is_compensation` int COMMENT '是否申偿(1是,0否)', 
  `addition_order_flag` int COMMENT '是否加配单(0-否、1-是)', 
  `system_status` int COMMENT '系统状态(1-订单已提交、2-已同步WMS、3-WMS已回传、4-修改已提交、5-修改已同步WMS、6-修改成功、7-修改失败)', 
  `last_delivery_date` string COMMENT '预计到货日期', 
  `return_reason_code` string COMMENT '退货原因编码', 
  `return_reason_name` string COMMENT '退货原因名称', 
  `order_create_by` string COMMENT '订单创建者', 
  `order_create_time` string COMMENT '订单创建时间', 
  `order_update_time` string COMMENT '订单更新时间', 
  `location_code` string COMMENT '地点编码，经过转换，super_class类型1、2取结算地点，3 收货地点、4 发货地点', 
  `location_name` string COMMENT '地点编码，经过转换，super_class类型1、2取结算地点，3 收货地点、4 发货地点',
  `avg_sales_qty` decimal(26,6) comment '30日日均销量',
  `avg_sales_value` decimal(26,6) comment '30日均销售额')
COMMENT '冻品采购订单跟踪 sdt 订单更新日期'
PARTITIONED BY ( 
  `sdt` string comment '订单日期')

STORED AS parquet 
;

select distinct business_type_code,business_type_name from csx_dw.dws_sale_r_d_detail where sdt>='20210101';





set orderedate ='2021-03-15';
--select  regexp_replace(to_date(date_sub(${hiveconf:orderedate},30)),'-','') ;
-- select * from csx_tmp.tmp_sale;
-- show create table csx_dw.ads_supply_order_flow ;

set orderedate ='2021-03-16';
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.report_supply_frozen_order_flow partition(sdt)
select
purchase_org_code,
	purchase_org_name,
	a.order_code,
	a.source_order_code,
	super_class,
	super_class_name ,
	a.original_order_code,
	link_order_code,
	a.shipped_location_code,
	a.shipped_location_name,
	receive_location_code                             ,
	receive_location_name                             ,
	settle_location_code                              ,
	settle_location_name                              ,
	supplier_code                                     ,
	supplier_name                                     ,
	a.goods_code                                        ,
	bar_code                                          ,
	goods_name                                        ,
	spec                                              ,
	pack_qty                                          ,
	unit                                              ,
	category_code                                     ,
	category_name                                     ,
	classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
	purchase_group_code                               ,
	purchase_group_name                               ,
	category_large_code                               ,
	category_large_name                               ,
	category_middle_code                              ,
	category_middle_name                              ,
	a.category_small_code                               ,
	category_small_name                               ,
	a.fixed_price_type,
	tax_code                                          ,
	tax_rate                                          ,
	order_amt/ order_qty as order_price                   ,
	order_qty   as order_qty                                         ,
	order_amt   as order_amt                                         ,
	receive_no,
	receive_batch_code,
    produce_date,
    a.receive_price,
	receive_qty ,
	receive_amt ,
	receive_amt/(1+tax_rate/100) as no_tax_receive_amt,
	receive_super_class,
	-- 出库类型
	sale_channel  , 
  compensation_type  , 
  run_type  , 
  receive_type_code  , 
  receive_type , 
  receive_business_type_code, 
  receive_business_type, 
  receive_date , 
  receive_close_date , 
  receive_status , 
  shipped_no  , 
  shipped_batch_code , 
  shipped_price  , 
  shipped_amt , 
  shipped_qty , 
  shipped_super_class, 
  shipped_type_code , 
  shipped_type , 
  shipped_business_type_code  , 
  shipped_business_type  , 
  shipped_date  , 
  shipped_status , 
  order_status,
  order_status_name ,
	source_type , 
 source_type_name , 
  direct_flag  , 
  zm_direct_flag  , 
  customer_direct_flag,
  local_purchase_flag ,
  is_compensation  ,
  addition_order_flag,
  system_status  ,
  last_delivery_date,
  return_reason_code,
  return_reason_name,
  order_create_by ,
  order_create_time ,
  order_update_time ,
  location_code ,
  location_name ,
    avg_sales_qty,
    order_create_by,
  regexp_replace( to_date(order_create_time),'-','') sdt
from
(
select
	purchase_org_code,
	purchase_org_name,
	a.order_code,
	a.source_order_code,
	super_class,
	case
		when super_class='1'
			then '供应商订单'
		when super_class='2'
			then '供应商退货订单'
		when super_class='3'
			then '配送订单'
		when super_class='4'
			then '返配订单'
	end super_class_name ,
	a.original_order_code,
	link_order_code,
	a.shipped_location_code,
	a.shipped_location_name,
	receive_location_code                             ,
	receive_location_name                             ,
	settle_location_code                              ,
	settle_location_name                              ,
	supplier_code                                     ,
	supplier_name                                     ,
	goods_code                                        ,
	bar_code                                          ,
	goods_name                                        ,
	spec                                              ,
	pack_qty                                          ,
	unit                                              ,
	category_code                                     ,
	category_name                                     ,
	classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
	purchase_group_code                               ,
	purchase_group_name                               ,
	category_large_code                               ,
	category_large_name                               ,
	category_middle_code                              ,
	category_middle_name                              ,
	a.category_small_code                               ,
	category_small_name                               ,
	a.fixed_price_type,
	tax_code                                          ,
	tax_rate                                          ,
	order_amt/ order_qty as order_price                   ,
	order_qty   as order_qty                                         ,
	order_amt   as order_amt                                         ,
	receive_no,
	receive_batch_code,
    produce_date,
    a.receive_price,
	receive_qty ,
	receive_amt ,
	receive_amt/(1+tax_rate/100) as no_tax_receive_amt,
	receive_super_class,
	-- 出库类型
	sale_channel  , 
  compensation_type  , 
  run_type  , 
  receive_type_code  , 
  receive_type , 
  receive_business_type_code, 
  receive_business_type, 
  receive_date , 
  receive_close_date , 
  receive_status , 
  shipped_no  , 
  shipped_batch_code , 
  shipped_price  , 
  shipped_amt , 
  shipped_qty , 
  shipped_super_class, 
  shipped_type_code , 
  shipped_type , 
  shipped_business_type_code  , 
  shipped_business_type  , 
  shipped_date  , 
  shipped_status , 
  order_status,
    case
		when order_status=1
			then '已创建'
		when order_status=2
			then '已发货'
		when order_status=3
			then '部分入库'
		when order_status=4
			then '已完成'
		when order_status=5
			then '已取消'
	end order_status_name ,
	source_type , 
  concat(cast(source_type as string) ,' ', source_type_name)as source_type_name , 
  direct_flag  , 
  zm_direct_flag ,  
  customer_direct_flag,
  local_purchase_flag ,
  is_compensation  ,
  addition_order_flag,
  system_status  ,
  last_delivery_date,
  return_reason_code,
  return_reason_name,
  order_create_by ,
  order_create_time ,
  order_update_time ,
  location_code ,
  location_name 
from
	csx_dw.ads_supply_order_flow a 
join 
(select shop_code,product_code from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' and joint_purchase_flag=1) m on a.receive_location_code=m.shop_code and a.goods_code=m.product_code
 join
(select classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
    category_small_code 
from csx_dw.dws_basic_w_a_manage_classify_m 
where sdt='current' and classify_middle_code='B0304') b on a.category_small_code=b.category_small_code
where
	sdt  = regexp_replace(to_date(${hiveconf:orderedate}),'-','')
) a 
left join 
(select dc_code,goods_code,sum(sales_qty)/30 as avg_sales_qty ,sum(sales_value)/30 as avg_sales_value
    from csx_dw.dws_sale_r_d_detail where sdt  >  regexp_replace(to_date(date_sub(${hiveconf:orderedate},30)),'-','') 
        and sdt<=  regexp_replace(to_date(${hiveconf:orderedate}),'-','')
        and business_type_code in ('1','3','6','5')  -- 日配、批发内购、BBC、省区大宗
    group by  dc_code,goods_code) b on a.receive_location_code=b.dc_code and a.goods_code=b.goods_code




