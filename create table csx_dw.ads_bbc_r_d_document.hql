create table csx_dw.ads_bbc_r_d_document_stat
	(
		years string comment '年'                                  ,
		months string comment '月'                                 ,
		calday string comment '销售日期'                              ,
		dist_code string comment '省区编码'                           ,
		dist_name string comment '省区名称'                           ,
		location_code string comment '地点编码'                       ,
		shop_name string comment '地点编码'                           ,
		order_sku              Bigint comment '下单SKU'                     ,
		receive_sku            Bigint comment '收货SKU'                     ,
		plan_qty               decimal(26,6) comment '订单数量'               ,
		receive_qty            decimal(26,6) comment '收货数量'               ,
		shelf_qty              decimal(26,6) comment '上架数量'               ,
		shelf_sku              Bigint comment '上架SKU'                     ,
		inv_sku                Bigint comment '库存SKU'                     ,
		qty                    decimal(26,6) comment '库存数量'               ,
		amt                    decimal(26,6) comment '库存金额'               ,
		th_foods_sku           Bigint comment '退货区食百SKU'                  ,
		th_fresh_sku           Bigint comment '退货区生鲜SKU'                  ,
		th_foods_qty           decimal(26,6) comment '退货区食百数量'            ,
		th_fresh_qty           decimal(26,6) comment '退货区生鲜数量'            ,
		th_foods_amt           decimal(26,6) comment '退货区食百金额'            ,
		th_fresh_amt           decimal(26,6) comment '退货区生鲜金额'            ,
		fresh_loss_amt         decimal(26,6) comment '生鲜报损额 117A  -117B冲销',
		foods_loss_amt         decimal(26,6) comment '食百报损额'              ,
		fresh_inventory_profit decimal(26,6) comment '生鲜盘盈额取过帐 115A'      ,
		foods_inventory_profit decimal(26,6) comment '省区编码'               ,
		fresh_inventory_loss   decimal(26,6) comment '生鲜盘亏额取过帐 116A'      ,
		foods_inventory_loss   decimal(26,6) comment '食百盘亏额'               ,
		order_num              bigint comment '订单数'                      ,
		shipped_sku            bigint comment '出库SKU'                      ,
		shipped_qty            decimal(26,6) comment '发货数量'               ,
		diff_shipped_qty       decimal(26,6) comment '差异数量，订单数量>发货数量'               ,
		stock_out_sku          bigint comment '缺货SKU'                      ,
		diff_shipped_amt       decimal(26,6) comment '缺货金额'               ,
		bbc_express_amt        decimal(26,6) comment '快递发货金额'               ,
		bbc_city_amt           decimal(26,6) comment '同城配发货金额'               ,
		bbc_pick_amt           decimal(26,6) comment '自提发货金额'               ,
		bbc_wholesale_amt      decimal(26,6) comment '一件代发发货金额'               ,
		return_city_amt        decimal(26,6) comment '同城退货金额'                ,
		return_express_amt     decimal(26,6) comment '快递退货金额'               ,
		return_pick_amt        decimal(26,6) comment '自提退货金额'               ,
		return_wholesale_amt   decimal(26,6) comment '一件代发退货金额'               ,
		return_noorder_amt     decimal(26,6) comment '无单退货额'               ,
		bbc_express_sale       decimal(26,6) comment '快递实际销售额：发货金额-退货金额'               ,
		bbc_city_sale          decimal(26,6) comment '同城实际销售额：同城发货金额-退货金额'               ,
		bbc_pick_sale          decimal(26,6) comment '自提实际销售额：自提发货金额-退货金额'               ,
		bbc_wholesale_sale     decimal(26,6) comment '一件代发实际金额：一件代发发货金额-退货金额'                ,
		pick_qty               decimal(26,6) comment '拣货数量'               ,
		pick_sku               bigint comment '拣货SKU'                      ,
		pack_qty               decimal(26,6) comment '打包数量'               ,
		pack_sku               bigint comment '打包SKU'
	)
	comment 'BBC每日单据统计' partitioned by
	(
		sdt string comment '分区日期'
	)
	stored as parquet
;