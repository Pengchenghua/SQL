-- scm_order_header 订单表头
-- scm_order_items 订单明细
-- scm_order_product_price 价格表
-- wms_entry_order_m  入库明细
-- wms_shipped_order_m 出库明细
DROP TABLE IF EXISTS temp.scm_order_01
;

-- select * from temp.scm_order_01 where order_code='TO99B2191023000095';
CREATE TEMPORARY TABLE
IF NOT EXISTS temp.scm_order_01 AS
	SELECT
		a.order_code                                  ,
		super_class                                   ,
		original_order_code                           ,
		link_order_code                               ,
		shipped_order_code                            ,
		received_order_code                           ,
		direct_flag                                   ,
		zm_direct_flag                                ,
		customer_direct_flag                          ,
		local_purchase_flag                           ,
		addition_order_flag                           ,
		a.status                                      ,
		system_status                                 ,
		source_type                                   ,
		supplier_code        as vender_code           ,
		supplier_name        as vender_name           ,
		source_location_code as send_location_code    ,
		source_location_name as send_location_name    ,
		target_location_code as receive_location_code ,
		target_location_name as receive_location_name ,
		settle_location_code                          ,
		settle_location_name                          ,
		purchase_org_code                             ,
		purchase_org_name                             ,
		is_compensation                               ,
		last_delivery_date                            ,
		order_create_time                             ,
		order_update_time                             ,
		b.source_order_code                           ,
		b.product_code   as goods_code                  ,
		product_name     as goods_name                  ,
		product_bar_code as bar_code                    ,
		spec                                            ,
		pack_qty                                        ,
		unit                                            ,
		qty AS order_qty                                ,
		fixed_price_type                                ,
		tax_code                                        ,
		tax_rate                                        ,
		tax_rate2                                       ,
		tax_code2                                       ,
		system_price                                    ,
		manual_price                                    ,
		system_transfer_price                           ,
		manual_transfer_price                           ,
		batch_price                                     ,
		price1_include_tax                              ,
		price1_free_tax                                 ,
		price2_include_tax                              ,
		price2_free_tax                                 ,
		amount1_include_tax                             ,
		amount1_free_tax                                ,
		amount2_include_tax                             ,
		amount2_free_tax                                ,
		price1_enable_type                              ,
		price2_enable_type                              ,
		price_markup_proportion                         ,
		purchase_group_code as dept_code                ,
		purchase_group_name as dept_name                ,
		order_amount                                    ,
		order_un_amount                                 ,
		a.category_code                                 ,
		a.category_name                                 ,
		big_classify_code   as category_large_code        ,
		big_classify_name   as category_large_name        ,
		small_classify_code as category_small_code        ,
		small_classify_name as category_small_name
	FROM
		(
			SELECT
				order_code                                                          ,
				super_class                                                         ,
				original_order_code                                                 ,
				link_order_code                                                     ,
				shipped_order_code                                                  ,
				received_order_code                                                 ,
				direct_flag                                                         ,
				zm_direct_flag                                                      ,
				customer_direct_flag                                                ,
				local_purchase_flag                                                 ,
				addition_order_flag                                                 ,
				status                                                              ,
				system_status                                                       ,
				source_type                                                         ,
				supplier_code                                                       ,
				supplier_name                                                       ,
				source_location_code                                                ,
				source_location_name                                                ,
				target_location_code                                                ,
				target_location_name                                                ,
				settle_location_code                                                ,
				settle_location_name                                                ,
				purchase_org_code                                                   ,
				purchase_org_name                                                   ,
				category_code                                                       ,
				category_name                                                       ,
				is_compensation                                                     ,
				last_delivery_date                                                  ,
				regexp_replace (to_date(create_time) ,'-' ,'') AS order_create_time ,
				regexp_replace (to_date(update_time) ,'-' ,'') AS order_update_time
			FROM
				csx_ods.scm_order_header_ods
			WHERE
				sdt             ='20191114'
				AND create_time>='2019-11-01 00:00:00'
		)
		a
		JOIN
			(
				SELECT
					order_code          ,
					source_order_code   ,
					product_code        ,
					product_name        ,
					product_bar_code    ,
					spec                ,
					pack_qty            ,
					unit                ,
					qty                 ,
					gift_count          ,
					purchase_group_code ,
					purchase_group_name ,
					category_code       ,
					category_name       ,
					big_classify_code   ,
					big_classify_name   ,
					small_classify_code ,
					small_classify_name ,
					status
				FROM
					csx_ods.scm_order_items_ods
				WHERE
					sdt='20191114'
			)
			b
			ON
				a.order_code=b.order_code
		LEFT OUTER JOIN
			(
				SELECT
					order_code                                                                                            ,
					product_code                                                                                          ,
					fixed_price_type                                                                                      ,
					tax_code                                                                                              ,
					tax_rate                                                                                              ,
					tax_rate2                                                                                             ,
					tax_code2                                                                                             ,
					system_price                                                                                          ,
					manual_price                                                                                          ,
					system_transfer_price                                                                                 ,
					manual_transfer_price                                                                                 ,
					batch_price                                                                                           ,
					price1_include_tax                                                                                    ,
					price1_free_tax                                                                                       ,
					price2_include_tax                                                                                    ,
					price2_free_tax                                                                                       ,
					amount1_include_tax                                                                                   ,
					amount1_free_tax                                                                                      ,
					amount2_include_tax                                                                                   ,
					amount2_free_tax                                                                                      ,
					create_time                                                                                           ,
					create_by                                                                                             ,
					update_time                                                                                           ,
					update_by                                                                                             ,
					price1_enable_type                                                                                    ,
					price2_enable_type                                                                                    ,
					coalesce( if ( amount1_include_tax=0,amount2_include_tax,amount1_include_tax ) ,0) as order_amount    ,
					coalesce( if ( amount1_free_tax   =0,amount2_free_tax,amount1_free_tax ) ,0)       as order_un_amount ,
					price_markup_proportion
				FROM
					csx_ods.scm_order_product_price_ods
				WHERE
					sdt='20191114'
			)
			c
			ON
				b.order_code      =c.order_code
				AND b.product_code=c.product_code
	;
	
	-- select * from temp.scm_order_01;
	-- super_class 单据类型(1-供应商订单、2-供应商退货订单、3-配送订单、4-返配订单)
	-- source_type 来源类型(1-采购导入、2-直送、3-一键代发、4-项目合伙人、5-无单入库、6-寄售调拨、7-自营调拨、8-云超采购、9-工厂采购)
	-- status 状态(1-已创建、2-已发货、3-部分入库、4-已完成、5-已取消)
	-- system_status 系统状态(1-订单已提交、2-已同步WMS、3-WMS已回传、4-修改已提交、5-修改已同步WMS、6-修改成功、7-修改失败)
	SELECT
		order_create_time as order_date , --订单日期
		a.order_code                    , --订单号
		a.super_class                   , --订单单据类型
		CASE
			WHEN a.super_class='1'
				THEN '供应商订单'
			WHEN a.super_class='2'
				THEN '供应商退货订单'
			WHEN a.super_class='3'
				THEN '配送订单'
			WHEN a.super_class='4'
				THEN '返配订单'
				ELSE a.super_class
		END super_class_name     , --订单单据类型名称
		original_order_code      , --源订单号
		link_order_code          , --关联订单号
		shipped_order_code       , --出库单号
		received_order_code      , --入库单号
		direct_flag              , --是否直通
		zm_direct_flag           , --是否帐面直通
		customer_direct_flag     , --是否直送
		local_purchase_flag      , --是否地采
		addition_order_flag      , --是否加配
		a.status AS order_status , --订单单据状态
		system_status            , --系统状态
		source_type              , --订单来源
		CASE
			WHEN source_type='1'
				THEN '采购导入'
			WHEN source_type='2'
				THEN '直送'
			WHEN source_type='3'
				THEN '一键代发'
			WHEN source_type='4'
				THEN '项目合伙人'
			WHEN source_type='5'
				THEN '无单入库'
			WHEN source_type='6'
				THEN '寄售调拨'
			WHEN source_type='7'
				THEN '自营调拨'
			WHEN source_type='8'
				THEN '云超采购'
			WHEN source_type='9'
				THEN '工厂采购'
				ELSE source_type
		END source_type_name    , --订单来源名称
		vender_code             , --供应商号
		vender_name             , --供应商名称
		''org_vender_code		,--原供应商号(接入旧系统字段)
		a.send_location_code    , --发货地点
		a.send_location_name    , --发货名称
		a.receive_location_code , --收货地点编码
		a.receive_location_name , --收货地点名称
		a.settle_location_code  , --结算地点编码
		a.settle_location_name  , --结算地点名称
		a.purchase_org_code     , --采购组织
		a.purchase_org_name     , --采购组织名称
		is_compensation         , --是否申偿
		last_delivery_date      , --最迟送货日期
		--order_create_time           , --订单创建日期
		order_update_time           , --订单更新日期
		a.goods_code                , --商品编码
		a.goods_name                , --商品名称
		bar_code                    , --商品条码
		spec                        , --规格
		pack_qty                    , --件装数
		unit                        , --单位
		order_qty                   , --订单数量
		fixed_price_type            , --定价类型(1-移动平均价、2-系统调拨协议价、3-系统进价、4-批次价、5-手工价)
		tax_code                    , --税码
		tax_rate                    , --税率
		tax_rate2                   , --税码2
		tax_code2                   , --税率2
		system_price                , --系统进价
		manual_price                , --手工价
		system_transfer_price       , --系统调拨协议价、3-系统进价、4-批次价、5-手工价
		manual_transfer_price       , --手工调拨价
		batch_price                 , --批次价
		price1_include_tax          , --单价1（含税）
		price1_free_tax             , --单价1(未税)
		price2_include_tax          , --单价2(含税)
		price2_free_tax             , --单价2(未税)
		amount1_include_tax         , --金额1(含税)
		amount1_free_tax            , --金额1(未税)
		amount2_include_tax         , --金额2(含税)
		amount2_free_tax            , --金额2(未税)
		order_amount                , --订单额(含税)
		order_un_amount             , --订单额(未税)
		price_markup_proportion     , --加价比例
		dept_code                   , --课组编码
		dept_name                   , --课组名称
		a.category_code             , --部类编码
		a.category_name             , --部类名称
		category_large_code         , --大类编码
		category_large_name         , --大类名称
		category_small_code         , --小类编码
		category_small_name         , --小类名称
		receive_batch_code          , --入库批次号
		direct_price                , --帐面直通进价
		direct_amount               , --帐面直通金额
		return_flag                 , --退货标识
		receive_super_class         , --收货类型( 1-正常收货 2-无单收货 3-异常收货地点)
		receive_store_location_code , --收货库存编码
		receive_store_location_name , --收货库存名称
		shelf_store_location_type   , --上架至（上架储位类型）
		shelf_area_code             , --收货储位编码
		shelf_area_name             , --收货储位名称
		shelf_store_location_code   , --上架储位编码
		shelf_store_location_name   , --上架储位
		receive_status              , --收货状态 0-待收货 1-收货中 2-已关单 3-已取消
		shelf_status                , --上架状态 0-待上架 1-上架中 2-已上架
		all_receive_flag            , --全量收货
		all_shelf_flag              , --全量上架
		--close_by                      , --关单人
		receive_compensation_type , --1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿
		business_type             , --业务类型
		entry_type                , --订单类型（P01、T06）
		assess_type               , --评估类型
		assess_type_name          , --评估类型名称
		source_system             , --来源系统
		receive_qty               , --收货数量
		shelf_qty                 , --上架数量
		receive_pass_qty          , --申偿通过数量
		receive_reject_qty        , --申偿拒绝数量
		receive_price             , --入库进价
		recevie_add_price_percent , --入库加价比例
		receive_amount            , --入库金额
		receive_date              , --收货时间
		receive_close_time        , --关单时间
		--    received_create_time          , --入库时间
		--   recevied_update_time          , --入库更新时间
		shipped_batch_code , --出库批次号
		--   order_no                      , --出库单号
		--   link_scm_order_no             , --关联采购单号
		shipped_qty               , --出库数量
		picking_side_qty          , --拣选面出库数
		store_location_qty        , --储位出库数
		shipped_price             , --出库单价
		shipped_add_price_percent , --加价比例
		shipped_amount            , --出库金额
		--shipped_location_code         , --出库地点
		--shipped_location_name         , --出库地点名称
		shipped_area_code           , --出库库区
		shipped_area_name           , --出库库区名称
		shipped_store_location_code , --出库储位编码
		shipped_store_location_name , --出库储位名称
		all_send_flag               , --全量发货
		running_model               , --运转模式
		picking_type                , --拣货类型 1-移动端拣货 2-电子称拣货,
		tc_picking_flag             , --TC商品是否拣货 0-否 1-是
		price_type                  , --价格类型 1-成本 2-协议价 3-系统进价
		shipped_source_system       , --来源系统
		shipped_super_class         , --出库单据类型
		shipped_type                , --订单类型
		shipped_business_type       , --业务类型
		shipped_return_flag         , --退货标识
		transfer_location_code      , --转配地点编码
		transfer_location_name      , --转配地点
		--  shipped_receive_location_code , --收货地点编码
		--  shipped_receive_location_name , --收货地点名称
		shipped_status , --出库单状态
		-- wave_code                , --出库波次
		-- distribute_shortage_flag , --分配短缺
		all_received_flag , --全量收货
		-- packages_number          , --包裹数
		shipped_finish_time  --出库完成时间
		--  shipped_create_time           , --出库创建时间
		--  shipped_update_time             --出库更新时间
	FROM
		temp.scm_order_01 a
		LEFT OUTER JOIN
			(
				SELECT
					a.order_code                       , -- 入库单号
					a.batch_code as receive_batch_code , -- 批次号
					a.goods_code                       , -- 商品编码
					-- direct_flag,
					direct_price                       , -- 帐面直通进价
					direct_amount                      , --帐面直通金额
					entry_type                         , -- 订单类型（P01、T06）
					return_flag                        , -- 退货标识
					super_class AS receive_super_class , -- 收货类型( 1-正常收货 2-无单收货 3-异常收货地点)
					--receive_area_code,
					--receive_area_name,
					receive_store_location_code , -- 收货库存编码
					receive_store_location_name , -- 收货库存名称
					shelf_store_location_type   , --上架至（上架储位类型）
					shelf_area_code             , -- 收货储位编码
					shelf_area_name             , -- 收货储位名称
					shelf_store_location_code   , --上架储位编码
					shelf_store_location_name   , -- 上架储位
					receive_status              , --收货状态 0-待收货 1-收货中 2-已关单 3-已取消
					shelf_status                , --上架状态 0-待上架 1-上架中 2-已上架
					all_receive_flag            , --全量收货
					all_shelf_flag              , -- 全量上架
					--print_times,
					origin_order_code                                                      , -- 来源单号
					regexp_replace (to_date(receive_time ) ,'-' ,'')    receive_date       , -- 收货时间
					regexp_replace (to_date(close_time) ,'-' ,'')    as receive_close_time , -- 关单时间
					close_by                                                               , -- 关单人
					auto_status                                                            , --是否自动执行
					sale_channel                                                           , -- 销售渠道
					compensation_type as receive_compensation_type                         , --1.寄售申偿，2.云超销售申偿，3.自营小店申偿，4.调拨申偿
					--outside_order_code,
					--run_type,
					business_type    , -- 业务类型
					assess_type      , -- 评估类型
					assess_type_name , -- 评估类型名称
					--tax_type,
					--tax_rate,
					--tax_code,
					--price_type,
					source_system                                          , --来源系统
					coalesce(a.receive_qty,0)                      receive_qty               , -- 收货数量
					coalesce(a.shelf_qty,0)                        shelf_qty                 , --上架数量
					coalesce(a.pass_qty,0)                         receive_pass_qty          , --申偿通过数量
					coalesce(a.reject_qty,0)                       receive_reject_qty        , -- 申偿拒绝数量
					coalesce(a.price,0)                            receive_price             , --收货进价
					a.add_price_percent                            as recevie_add_price_percent , --入库加价比例
					coalesce(a.amount,0)                           AS receive_amount            , --收货金额
					regexp_replace (to_date(create_time) ,'-' ,'') AS received_create_time      ,
					regexp_replace (to_date(update_time) ,'-' ,'') AS recevied_update_time
				FROM
					csx_dw.wms_entry_order_m a
			)
			b
			ON
				a.order_code    =b.origin_order_code
				AND a.goods_code=b.goods_code
		LEFT OUTER JOIN
			(
				SELECT
					batch_code as shipped_batch_code , --出库批次号
					order_no                         , --出库单号
					link_scm_order_no                , --关联采购单号
					goods_code                       , -- 商品编码
					--order_shipped_qty,
					coalesce(shipped_qty,0)       shipped_qty        , -- 出库数量
					coalesce(picking_side_qty,0)  picking_side_qty   , -- 拣选面出库数
					coalesce(store_location_qty,0)store_location_qty , -- 储位出库数
					-- receive_qty,
					coalesce(pass_qty,0)   as shipped_pass_qty          , --通过申偿量
					coalesce(reject_qty,0) as shelf_reject_qty          , --拒绝申偿量
					coalesce(price ,0)     as shipped_price             , -- 单价
					add_price_percent      as shipped_add_price_percent , --加价比例
					coalesce(amount ,0)    as shipped_amount            , -- 出库金额
					--direct_price,
					--direct_amount,
					shipped_location_code                  , --出库地点
					shipped_location_name                  , --出库地点名称
					shipped_area_code                      , --出库库区
					shipped_area_name                      , --出库库区名称
					shipped_store_location_code            , --出库储位编码
					shipped_store_location_name            , --出库储位名称
					all_send_flag                          , --全量发货
					running_model                          , --运转模式
					picking_type                           , --拣货类型 1-移动端拣货 2-电子称拣货,
					tc_picking_flag                        , --TC商品是否拣货 0-否 1-是
					price_type                             , --价格类型 1-成本 2-协议价 3-系统进价
					source_system as shipped_source_system , --来源系统
					super_class   as shipped_super_class   , --出库单据类型
					shipped_type                           , --订单类型
					business_type as shipped_business_type , --业务类型
					return_flag   as shipped_return_flag   , --退货标识
					transfer_location_code                 , --转配地点编码
					transfer_location_name                 , --转配地点
					--supplier_code,
					--supplier_name,
					receive_location_code as shipped_receive_location_code , --收货地点编码
					receive_location_name as shipped_receive_location_name , --收货地点名称
					--customer_code,
					--customer_name,
					--station_code,
					--station_name,
					status as shipped_status , --出库单状态
					wave_code                , --出库波次
					distribute_shortage_flag , --分配短缺
					all_received_flag        , --全量收货
					--print_times,
					packages_number                                                       , --包裹数
					link_operate_order_no                                                 ,
					origin_order_no                                                       ,
					link_in_out_order_no                                                  ,
					link_order_no                                                         ,
					external_order_no                                                     ,
					send_time                                                             ,
					auto_status                                                           ,
					sale_channel                                                          ,
					compensation_type as shipped_compensation_type                        ,
					plan_date                                                             ,
					order_type                                                            ,
					regexp_replace (to_date(finish_time) ,'-' ,'') AS shipped_finish_time ,
					regexp_replace (to_date(create_time) ,'-' ,'') AS shipped_create_time ,
					regexp_replace (to_date(update_time) ,'-' ,'') AS shipped_update_time
				FROM
					csx_dw.wms_shipped_order_m a
			)
			c
			ON
				a.order_code    =c. origin_order_no
				AND a.goods_code=c.goods_code
	;