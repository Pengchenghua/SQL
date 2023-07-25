CREATE TABLE csx_dw.wms_order_m_tmp
(
	goods_id string COMMENT	'商品编码',
	goods_name string COMMENT	'商品名称',
	bar_code string COMMENT	'商品条码',
	category_code	string COMMENT	'部类编码',
	category_name	string COMMENT	'部类名称',
	category_large_code	string COMMENT	'大类编码',
	category_large_name		string COMMENT '大类名称',
	category_middle_code	string COMMENT	'中类编码',
	category_middle_name	string COMMENT	'中类名称',
	category_small_code		string COMMENT '小类编码',
	category_small_name	string COMMENT	'小类名称',
	unit	string COMMENT	'单位',
			
	source_system string COMMENT '来源系统',  
	super_class  string COMMENT '类型',  		
	order_code string COMMENT '出入库单号',			
	entry_type  string COMMENT '订单类型', 		
	business_type string COMMENT '业务类型',		
	return_flag string COMMENT '退货标识', 		
	direct_flag string COMMENT '账面直通标识',		
	shipper_code string COMMENT '货主编码',		
	shipper_name string COMMENT '货主名称',		
	supplier_code string COMMENT '发货供应商编码',		
	supplier_name string COMMENT '发货供应商名称',		
	send_dc_code string COMMENT '发货地点编码',		
	send_dc_name string COMMENT '发货地点名称',		
	receive_dc_code string COMMENT '收货地点编码',	
	receive_dc_name string COMMENT '收货地点名称',	
	reservoir_area_code string COMMENT '出入库库区编码',
	reservoir_area_name string COMMENT	'出入库库区名称', 
	store_location_code	 string COMMENT  '出入库储位编码',
	store_location_name	 string COMMENT '出入库储位名称',
	shelf_reservoir_area_code string COMMENT '上架库区编码',
	shelf_reservoir_area_name string COMMENT  '上架库区名称',
	shelf_store_location_code string COMMENT '上架储位编码',	
	shelf_store_location_name string COMMENT '上架储位名称',
	settle_dc_code string COMMENT '结算地点编码',	
	settle_dc_name string COMMENT '结算地点名称',	
	produce_date string COMMENT	'生产日期',
	status string COMMENT '收货状态 0-待收货 1-收货中 2-已关单 、出库状态状态 0-初始 1-已集波 2-分配中 3-已分配 4-拣货中 5-拣货完成 6-已发货 8-已收货 9-已取消',
	plan_receive_date string COMMENT '预计收货日期/计划出库日期',		
	shelf_status string COMMENT '上架状态 0-待上架 1-上架中 2-已上架',		
	all_receive_flag string COMMENT '是否全量收货 0-否 1-是',			
	all_shelf_flag string COMMENT  '是否全量上架 0-否 1-是',			
	all_shipped_flag string COMMENT  '全量出库 0-否 1-是',		
	distribute_shortage_flag string COMMENT '分配短缺 0-否 1-是',	
	link_operate_order_code string COMMENT '关联操作订单',	
	origin_order_code string COMMENT '来源单号',		
	link_order_code string COMMENT '关联单号',		
	receive_time string COMMENT '收货时间',			
	send_time string COMMENT '发货时间',				
	remark string COMMENT '备注',					
	close_time string COMMENT '关单时间',				
	close_by string COMMENT '关单人',				
	auto_status string COMMENT '是否自动执行 0-否 1-是',			
	sale_channel string COMMENT '销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端',			--销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端
	compensation_type string COMMENT '申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿',		--申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿
	outside_order_code string COMMENT	'外部订单',

	batch_code string COMMENT '收货/发货批次号',	
	in_or_out_qty  decimal COMMENT '入库收货数量  出库数量',	
	price decimal COMMENT '价格',	
	amount decimal COMMENT	'金额',
	shelf_qty decimal COMMENT '上架数量',	
	biz_time string COMMENT '业务时间',
	posting_time string COMMENT '过账时间',	
	before_price decimal COMMENT '调整前单价',	
	before_qty decimal COMMENT '调整前数量',	
	before_amt decimal COMMENT '调整前金额',	
	after_price decimal COMMENT '调整后单价',	
	after_qty decimal COMMENT '调整后数量',	
	after_amt decimal COMMENT '调整后金额',	
	tax_rate decimal COMMENT '税率（百分）',	
	txn_qty decimal COMMENT '操作数量',		
	txn_price decimal COMMENT '记账单价',	
	txn_amt	decimal COMMENT '记账金额',	
	posting_flag  string COMMENT  '是否过账 0-否 1-是',	
	in_or_out	string COMMENT '1出库或0入库',  
	in_out_type	 string COMMENT '出入库类型',
	batch_no string COMMENT	'关联出入库批次号', 
	create_time string COMMENT 	'创建时间',
	create_by	string COMMENT  '创建者',
	update_time	 string COMMENT '更新时间',
	update_by	string COMMENT '更新者'
)
COMMENT '商品出入库批次明细信息'
PARTITIONED BY ( sdt string COMMENT '日期分区' , province_code  string COMMENT '省份分区' )
STORED AS TEXTFILE 
LOCATION 'hdfs://nameservice1/user/hive/warehouse/csx_dw.db/wms_order_m_tmp'






drop table b2b_tmp.wms_order_temp01;
create temporary table b2b_tmp.wms_order_temp01
as 
select 
	t2.product_code as product_code,--	商品编号
	t2.product_bar_code as product_bar_code,--	商品条码
	t2.product_name as product_name, 	--商品名称
	t2.unit as unit , -- 单位
	t1.source_system as source_system, --入库来源系统
	t1.super_class as super_class, -- 收货类型 1-正常收货 2-无单收货 3-异常收货地点
	t1.entry_type as entry_type, --入库订单类型
	t1.business_type as business_type, --入库业务类型
	t2.order_code as order_code, --入库单号
	t1.return_flag as return_flag, -- 入库退货标识
	t2.batch_code as batch_code,  --入库批次号
	t2.produce_date  as produce_date, --生产日期
	t2.receive_qty as receive_qty, -- 入库数量
	t2.price as price, --入库单价
	t2.amount as amount, --入库金额
	t1.plan_receive_date as  plan_receive_date, --预计入库收货日期
	t2.shelf_qty as shelf_qty , --入库上架数量
		t1.shipper_code as shipper_code,	--货主编号
		t1.shipper_name as shipper_name,	--货主名称
	t1.supplier_code as supplier_code,		--发货供应商编码
	t1.supplier_name as supplier_name,		--发货供应商名称
	t1.send_location_code as send_location_code,	--发货地点编码
	t1.send_location_name as send_location_name,	--发货地点名称
	coalesce(t1.receive_location_code,t2.location_code)	 as receive_location_code,--收货地点编码
	coalesce(t1.receive_location_name,t2.location_name) as receive_location_name,--收货地点名称
	t2.reservoir_area_code as reservoir_area_code,--	收货库区编码
	t2.reservoir_area_name as reservoir_area_name,--	收货库区名称
	t2.store_location_code as store_location_code,--	收货储位编码
	t2.store_location_name	as store_location_name,--收货储位名称
	t2.shelf_reservoir_area_code as shelf_reservoir_area_code,--	上架库区编码
	t2.shelf_reservoir_area_name as shelf_reservoir_area_name,--	上架库区名称
	t2.shelf_store_location_code as shelf_store_location_code,--	上架储位编码
	t2.shelf_store_location_name as shelf_store_location_name,--	上架储位名称
	t1.settlement_dc as settlement_dc,		--结算DC
	t1.settlement_dc_name as settlement_dc_name,	--结算DC名称
	t1.shelf_status as shelf_status,			--上架状态 0-待上架 1-上架中 2-已上架'
	t1.all_receive_flag as all_receive_flag,		--是否全量收货 0-否 1-是'
	t1.all_shelf_flag as all_shelf_flag,			--是否全量上架 0-否 1-是
	t1.origin_order_code as origin_order_code,		--入库来源单号 、供应链关联主键
	t1.link_operate_order_code as link_operate_order_code,--关联操作订单
	t1.link_order_code as link_order_code,		--入库关联单号
	t1.receive_time as receive_time ,			--收货时间
	t1.close_time as close_time,				--关单时间
	t1.close_by as close_by,				--关单人
	t1.outside_order_code as outside_order_code	,	--外部订单

	t2.remark	as 	remark,	--备注
	t2.create_time as create_time,--	创建时间
	t2.create_by as create_by,--	创建者
	t2.update_time as update_time,--	更新时间
	t2.update_by  as update_by,--	更新者
	t1.auto_status as auto_status,	--是否自动执行 0-否 1-是
	t1.sale_channel as sale_channel,	--销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端
	t1.compensation_type as compensation_type	--申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿  -- 目前全部为1

from
(
	select
		source_system,  	--来源系统
		super_class,  		-- 收货类型 1-正常收货 2-无单收货 3-异常收货地点
		order_code, 		-- 入库单号 规则IN+年（2位）+月（2位）+日（2位）+6位流水
		entry_type, 		--订单类型
		business_type,		--业务类型
		return_flag, 		-- 退货标识
		shipper_code,		-- 货主编码
		shipper_name,		--货主名称
		supplier_code,		--发货供应商编码
		supplier_name,		--发货供应商名称
		send_location_code,	--发货地点编码
		send_location_name,	--发货地点名称
		receive_location_code,--收货地点编码
		receive_location_name,--收货地点名称
		plan_receive_date,	--预计收货日期
		settlement_dc,		--结算DC
		settlement_dc_name,	--结算DC名称
		receive_status,		--收货状态 0-待收货 1-收货中 2-已关单
		shelf_status,			--上架状态 0-待上架 1-上架中 2-已上架'
		all_receive_flag,		--是否全量收货 0-否 1-是'
		all_shelf_flag,			--是否全量上架 0-否 1-是
		link_operate_order_code,--关联操作订单
		origin_order_code,		--来源单号
		link_order_code,		--关联单号

		receive_time,			--收货时间
		close_time,				--关单时间
		close_by,				--关单人
		outside_order_code,		--外部订单
		auto_status,	--是否自动执行 0-否 1-是
	 	sale_channel,	--销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端
		compensation_type	--申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿  -- 目前全部为1
	from
		csx_ods.wms_entry_order_header_ods		--入库单头信息表 
	where
		sdt=regexp_replace(date_sub(current_date, 1), '-', '') 		
		and
		split(close_time, ' ')[0] >= date_sub(current_date, 1) 	--	根据关单时间来取昨日入库数据  有些数据发货时间前一天、关单时间后一天、以收货时间算校验数据发现批次信息有、但是头信息为空的
		and 
		split(close_time, ' ')[0] < current_date
		and 
		receive_status<>'0'   			--收货状态 为收货中和已关单的才会有批次号
) as t1
right join 
(
	select
		id,--	主键
		order_code,--	入库单号
		batch_code,--	收货批次号
		product_code,--	商品编号
		product_bar_code,--	商品条码
		product_name,	
		unit,--	单位
		produce_date,--	生产日期
		receive_qty,--	收货数量
		price,--	价格
		amount,--	金额
		location_code,--	收货地点编码
		location_name,--	收货地点名称
		reservoir_area_code,--	收货库区编码
		reservoir_area_name,--	收货库区名称
		store_location_code,--	收货储位编码
		store_location_name	,--收货储位名称
		shelf_reservoir_area_code,--	上架库区编码
		shelf_reservoir_area_name,--	上架库区名称
		shelf_store_location_code,--	上架储位编码
		shelf_store_location_name,--	上架储位名称
		shelf_qty,--	上架数量
		remark,--	备注
		create_time,--	创建时间
		create_by,--	创建者
		update_time,--	更新时间
		update_by --	更新者
	from
		csx_ods.wms_entry_batch_detail_ods  --入库批次详情表   --取批次信息 
	where
		sdt=regexp_replace(date_sub(current_date, 1), '-', '') 
		and 
		split(update_time, ' ')[0] >= date_sub(current_date, 1) 	--取昨日时间 昨日763条（疑问、这里的批次要是取昨日、那么出库的商品用到了很早之前的批次怎么处理）
		and 
		split(update_time, ' ')[0] < current_date

) as t2 on t1.order_code=t2.order_code ;



--出库

drop table b2b_tmp.wms_order_temp02;
create temporary table b2b_tmp.wms_order_temp02
as 
select
	t3.source_system as source_system,	--出库来源系统
	t3.super_class as super_class,	--出库类型 1-正常出库单 2-异常出库单
	t3.order_type as order_type,	--订单类型 1-普通单 2-福利单
	t3.order_code as order_code,	--出库单号 规则OU+年（2位）+月（2位）+日（2位）+6位流水
	t3.shipped_type as shipped_type,	--出库订单类型
	t3.business_type as business_type,	--业务类型
	t3.return_flag as return_flag,	--退货标识
	t3.shipper_code as shipper_code,	--货主编号
	t3.shipper_name as shipper_name,	--货主名称
	t3.supplier_code as supplier_code,	--收货供应商编码		这里有数据的都是退供出库的、
	t3.supplier_name as supplier_name,	--收货供应商名称
	t3.send_location_code as send_location_code,--	发货地点编码
	t3.send_location_name as send_location_name,	--发货地点名称
	t3.receive_location_code as receive_location_code,	--收货地点编码
	t3.receive_location_name as receive_location_name,	--收货地点名称
			t3.settlement_dc as settlement_dc,		--结算DC
		t3.settlement_dc_name  as settlement_dc_name,	--结算DC名称		
	t3.customer_code as customer_code,	--编号
	t3.customer_name as customer_name,	--名称
	t3.sub_customer_code as sub_customer_code,	--子号
	t3.sub_customer_address as sub_customer_address,	--子地址
	t3.shop_type as shop_type,	--门店类型
	t3.shop_code as shop_code,	--门店编号
	t3.shop_name as shop_name,	--门店名称
	t3.wave_code as wave_code,	--波次号
	t3.all_shipped_flag as all_shipped_flag,	--全量出库 0-否 1-是
	t3.distribute_shortage_flag as distribute_shortage_flag,	--分配短缺 0-否 1-是
	t3.all_received_flag as all_received_flag,	--全量收货 0-否 1-是
	t3.link_operate_order_code as link_operate_order_code,	--关联操作订单
	t3.origin_order_code as origin_order_code,	--来源单号
	t3.link_in_out_order_code as link_in_out_order_code,	--关联出入库单号
	t3.link_order_code as link_order_code,	--关联单号
	t3.external_order_code as external_order_code,	--外部单号
	t3.send_time as send_time,	--发货时间
	t3.plan_date as plan_date,	--计划出库日期
	t3.remark as remark,	--备注
	t3.auto_status as auto_status,	--是否自动执行 0-否 1-是
	t3.sale_channel as sale_channel,	--销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端
	t3.compensation_type as compensation_type,	--申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿  -- 目前全部为1
	t4.batch_code as batch_code,--	出库批次号
	t4.product_code as product_code,--	商品编号
	t4.product_bar_code as product_bar_code,--	商品条码
	t4.product_name as product_name,	
	t4.unit as unit,--	单位
	t4.shipped_qty as shipped_qty,--	出库数量
	t4.price as price,--	价格
	t4.amount as amount,--	金额
	t4.location_code as location_code,--	出库地点编码
	t4.location_name as location_name,--	出库地点名称
	t4.reservoir_area_code as reservoir_area_code,--	出库库区编码
	t4.reservoir_area_name as reservoir_area_name,--	出库库区名称
	t4.store_location_code as store_location_code,--	出库储位编码
	t4.store_location_name as store_location_name,--	出库储位名称
	t4.create_time as create_time,--	创建时间			--以批次表为基准
	t4.create_by as create_by,--	创建者
	t4.update_time as update_time,--	更新时间
	t4.update_by as update_by--	更新者
from
(
	select
	--	id,	--主键
		source_system,	--来源系统
		super_class,	--类型 1-正常出库单 2-异常出库单
		order_type,	--订单类型 1-普通单 2-福利单
		order_code,	--出库单号 规则OU+年（2位）+月（2位）+日（2位）+6位流水
		shipped_type,	--订单类型
		business_type,	--业务类型
		return_flag,	--退货标识
		shipper_code,	--货主编号
		shipper_name,	--货主名称
		supplier_code,	--收货供应商编码		这里有数据的都是退供出库的、
		supplier_name,	--收货供应商名称
		send_location_code,--	发货地点编码
		send_location_name,	--发货地点名称
		receive_location_code,	--收货地点编码
		receive_location_name,	--收货地点名称
					settlement_dc ,		--结算DC
		settlement_dc_name ,	--结算DC名称		
		customer_code,	--编号
		customer_name,	--名称
		sub_customer_code,	--子号
		sub_customer_address,	--子地址
		shop_type,	--门店类型
		shop_code,	--门店编号
		shop_name,	--门店名称
		wave_code,	--波次号
		all_shipped_flag,	--全量出库 0-否 1-是
		distribute_shortage_flag,	--分配短缺 0-否 1-是
		all_received_flag,	--全量收货 0-否 1-是
		link_operate_order_code,	--关联操作订单
		origin_order_code,	--来源单号
		link_in_out_order_code,	--关联出入库单号
		link_order_code,	--关联单号
		external_order_code,	--外部单号
		send_time,	--发货时间
		plan_date,	--计划出库日期
		remark,	--备注
		auto_status,	--是否自动执行 0-否 1-是
		sale_channel,	--销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端
		compensation_type,	--申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿  -- 目前全部为1
		create_time,	--创建时间
		create_by,	--创建者
		update_time,	--更新时间
		update_by	--更新者
	from
		csx_ods.wms_shipped_order_header_ods   -- 出库头信息 获取订单类型
	where
		sdt=regexp_replace(date_sub(current_date, 1), '-', '') 
		and 
		split(send_time, ' ')[0] >= date_sub(current_date, 1) 	--因为订单是按发货时间才会有出库批次号、所以以发货时间作为出库固定节点
		and 
		split(send_time, ' ')[0] < current_date
		and  
		status  in ('6','8')		--获取会产品批次号的订单、
)as t3
right join
(
	select
--		id,--	主键
		order_code,--	出库单号
		batch_code,--	出库批次号
		product_code,--	商品编号
		product_bar_code,--	商品条码
		product_name,	
		unit,--	单位
		shipped_qty,--	出库数量
		price,--	价格
		amount,--	金额
		location_code,--	出库地点编码
		location_name,--	出库地点名称
		reservoir_area_code,--	出库库区编码
		reservoir_area_name,--	出库库区名称
		store_location_code,--	出库储位编码
		store_location_name,--	出库储位名称
	
		create_time,--	创建时间
		create_by,--	创建者
		update_time,--	更新时间
		update_by --	更新者
	from
		csx_ods.wms_shipped_batch_detail_ods  -- 出库批次明细表  获取出库批次信息和金额数量信息 （只有已发货的才会有批次信息）
	where
		sdt=regexp_replace(date_sub(current_date, 1), '-', '') 
		and
		split(update_time, ' ')[0] >= date_sub(current_date, 1) 	--批次记录创建和修改时间唯一 不会变动 所以去创建和修改都可
		and 
		split(update_time, ' ')[0] < current_date

)  as t4 on t3.order_code=t4.order_code






-- 出入库+盘点 数据合并  （后面和成本核算库的日志表和成本核算库的余额表进行关联） 

drop table b2b_tmp.wms_order_temp03;
create table b2b_tmp.wms_order_temp03
as
select
	*
from 
(select
		product_code as goods_id,	--商品编码
		product_name as goods_name,		--商品名称
		product_bar_code as bar_code,	--	商品条码
		unit,	--单位
		source_system , --入库来源系统
		super_class,	--入库类型 1-正常收货 2-无单收货 3-异常收货地点
		''order_type,	--订单类型 1-普通单 2-福利单
		entry_type as in_or_out_type,	--入库订单类型
		business_type,--业务类型
		order_code,	--入库单号
		return_flag,	--退货标识
		batch_code,	--入库批次号
		produce_date,	--生产日期		(出库字段中没有)
		receive_qty as in_or_out_qty,	--入库数量
		price,			----入库单价
		amount,	----入库金额
		plan_receive_date as in_or_out_plan_date,	--计划出库日期/--预计收货日期  可注释为计划出入库日期

		shipper_code,	--货主编号
		shipper_name,	--货主名称
		supplier_code as in_or_out_supplier_code,	--发货/收货供应商编码
		supplier_name as in_or_out_supplier_name,	--发货/收货供应商名称
		send_location_code as send_dc_code,			--发货地点编码
		send_location_name as send_dc_name,			--发货地点编码
		receive_location_code as receive_dc_code,	--收货地点编码
		receive_location_name as receive_dc_name,--收货地点名称
		reservoir_area_code,--	收货出库库区编码
		reservoir_area_name,--	收货库区名称
		store_location_code,--	收货储位编码
		store_location_name,--	收货储位名称
		settlement_dc as settle_dc_code,		--结算DC
		settlement_dc_name as settle_dc_name,	--结算DC名称
		all_receive_flag as all_receive_flag ,		--是否全量收货 0-否 1-是'
		origin_order_code,		--入库来源单号 、供应链关联主键
		link_order_code,		--入库关联单号
		link_operate_order_code,	--关联操作订单	
		outside_order_code	as external_order_code,	--外部订单
		receive_time as in_or_out_time,			--收货时间
		remark, --备注
		''all_shipped_flag,	--全量出库 0-否 1-是
		auto_status,	--是否自动执行 0-否 1-是
	 	sale_channel,	--销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端
		compensation_type,	--申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿  -- 目前全部为1
		close_time,				--关单时间
		close_by,				--关单人
		'0' in_or_out,  --入库标识 
		create_time,--	创建时间
		create_by,--	创建者
		update_time,--	更新时间
		update_by ,--	更新者
 
		''customer_code,	--编号
		''customer_name,	--名称
		''sub_customer_code,	--子号
		''sub_customer_address,	--子地址
		''shop_type,	--门店类型
		''shop_code,	--门店编号
		''shop_name,	--门店名称
		''wave_code	--波次号
	from 
		b2b_tmp.wms_order_temp01 

	

union all 
select
		product_code as goods_id,--	商品编号
		product_bar_code as bar_code,--	商品条码
		product_name as goods_name,	
		unit,--	单位
		source_system,	--来源系统
		super_class,	--类型 1-正常出库单 2-异常出库单
		order_type,		--订单类型 1-普通单 2-福利单		
		shipped_type as in_or_out_type,	--订单类型
		business_type,	--业务类型
		order_code,		--出库单号 规则OU+年（2位）+月（2位）+日（2位）+6位流水
		return_flag,	--退货标识
		batch_code,--	出库批次号
		''produce_date,	--生产日期
		shipped_qty as in_or_out_qty,--	出库数量
		price,--	价格
		amount,--	出库金额
		plan_date as in_or_out_plan_date,	--计划出入库日期,	
		shipper_code,	--货主编号
		shipper_name,	--货主名称
		supplier_code ,	--收货供应商编码		这里有数据的都是退供出库的、
		supplier_name,	--收货供应商名称
		send_location_code as in_or_out_supplier_code,	--发货/收货供应商编码
		send_location_name as in_or_out_supplier_name,	--发货/收货供应商名称
		location_code as send_dc_code,--	出库地点编码
		location_name as send_dc_name,--	出库地点名称
		receive_location_code as receive_dc_code,	--收货地点编码
		receive_location_name as receive_dc_name,	--收货地点名称
		reservoir_area_code,--	出库库区编码
		reservoir_area_name,--	出库库区名称
		store_location_code,--	出库储位编码
		store_location_name,--	出库储位名称
		settlement_dc as settle_dc_code,		--结算DC
		settlement_dc_name as settle_dc_name,	--结算DC名称		
		all_received_flag as all_receive_flag,	--全量收货 0-否 1-是

		origin_order_code,	--来源单号
		link_order_code,	--关联单号
		link_operate_order_code,	--关联操作订单	
		send_time as in_or_out_time,	--发货时间
		remark,	--备注
		all_shipped_flag,	--全量出库 0-否 1-是
		external_order_code,	--外部单号
		''close_time,				--关单时间
		''close_by,				--关单人
		auto_status,	--是否自动执行 0-否 1-是
		sale_channel,	--销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端
		compensation_type,	--申偿类型 1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿  -- 目前全部为1
		'1' in_or_out,  --入库标识 
		create_time,	--创建时间
		create_by,	--创建者
		update_time,	--更新时间
		update_by,	--更新者

		customer_code,	--编号
		customer_name,	--名称
		sub_customer_code,	--子号
		sub_customer_address,	--子地址
		shop_type,	--门店类型
		shop_code,	--门店编号
		shop_name,	--门店名称
		wave_code	--波次号
	from
		b2b_tmp.wms_order_temp02
) as h1
	



