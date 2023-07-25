-- 1、商超订单满足率
-- 2、提取订单表
--订单：	出库DC、、订单号、订单SKU、订单量
-- 出库：出库量、出库额
/*  	col_name	data_type	comment
1	order_no	string	交易单号
2	buyer_user_phone	string	买手手机号
3	buyer_shop_phone	string	买家门店手机号
4	buyer_shop_name	string	买家门店名称
5	buyer_shop_code	string	买家门店编码
6	merchant_type	string	商家业务类型，比如：HQBULK：红旗大宗，QYG：企业购，CSXMALL：彩食鲜商城
7	merchant_code	string	商家编码
8	merchant_name	string	商家名称
9	out_supplier_code	string	外部供应商编码
10	out_supplier_name	string	外部供应商名称
11	department_code	string	工作部门编码
12	department_name	string	工作部门名称
13	order_status	string	订单状态：CREATED:待支付 PAID:待确认 CONFIRMED:待截单 CUTTED:待出库 STOCKOUT:配送中 SITE:服务站签收 FETCHED:已自提 HOME:买家已签收 R_APPLY:退货申请 R_PERMIT:退货中 R_BACK:退货回库 R_PAY:退款中 R_SUCCESS:退货成功 R_REJECT:退货关闭（拒绝退货）SUCCESS:已完成 CANCELLED:已取消
14	order_type	string	订单类型(1001:大宗业务,1002:B2B2B ,1003:小B业务)
15	dc_code	string	履约dc编码
16	dc_name	string	履约dc名称
17	stock_dc_code	string	库存dc编码
18	stock_dc_name	string	库存dc名称
19	pay_mode	string	支付方式，WECHAT:微信 ALIPAY:支付宝 UNIONPAY:银联 OFFLINE:线下转账 CREDIT:授信支付
20	paid_time	string	支付时间
21	receive_mode	string	收货方式，HOME:送货上门，SITE:门店自提
22	receive_username	string	收货人姓名
23	receive_phone	string	收货人电话
24	receive_address	string	收货地址
25	out_order_no	string	外部订单号(第三方订单号)
26	order_time	string	下单时间
27	require_delivery_time	string	要求送货时间
28	require_delivery_time_end	string	要求发货时间截止
29	require_delivery_date	int	要求送货日期
30	delivery_no	string	出库配送单号
31	delivery_time	string	出库配送时间
32	cancel_reason	string	取消原因
33	receive_time	string	签收时间
34	finish_time	string	完成时间
35	order_date	int	下单日期
36	sap_cus_code	string	SAP买家编码
37	sap_cus_name	string	SAP买家名称
38	sap_sub_cus_code	string	SAP买家子账号
39	sap_sub_cus_name	string	sap子名称
40	sap_sale_office	string	SAP销售办公室
41	sap_sale_organization	string	SAP销售组织
42	sap_order_type	string	SAP订单类型
43	sap_pro_order_type	string	SAP采购订单类型
44	created_time	string	创建时间
45	creator	string	创建者
46	updated_time	string	更新时间
47	updater	string	更新者
48	parent_order_no	string	父订单号
49	order_group_code	string	订单分组编码(多个分组以逗号隔开)
50	order_group_name	string	订单分组名称(多个分组以逗号隔开)
51	is_print_delivery_order	int	是否打印出库单(0:未打印 1:已打印)
52	is_print_pick_order	int	是否打印拣货单(0:未打印 1:已打印)
53	deal_status	int	备注处理状态(0:待处理 1:已处理)
54	deal_time	string	备注处理时间
55	deal_person	string	备注处理人
56	receive_order_person	string	接单人
57	receive_order_time	string	接单时间
58	pay_type	int	0:线上支付,1:线下支付
59	created_user_id	bigint	下单用户ID
60	scale_status	int	精度状态(0不控制，1保留一位小数)
61	order_kind	string	订单类型：NORMAL-普通单，WELFARE-福利单
62	order_mode	int	订单模式：0-配送,1-直送，2-自提，3-直通
63	is_picking	int	是否分拣：0-否，1-是
64	order_tags	string	订单标签
65	quotation_price_nos	string	询价单号
66	is_partner_order	string	是否是合伙人订单
67	scm_order_nos	string	采购单号
68	wms_finish_time	string	wms入库时间
69	finance_finish_time	string	销售凭证上传时间
70	item_no	int	行项目号
71	goods_code	string	产品编码
72	goods_name	string	产品名称
73	out_product_code	string	外部商品编码
74	self_product_name	string	自建商品名称
75	spec	string	规格
76	unit	string	单位
77	supplier_code	string	供应商编码
78	supplier_name	string	供应商名称
79	category_large_code	string	大类编码
80	category_large_name	string	大类名称
81	category_middle_code	string	商品中类编码
82	category_middle_name	string	商品中类名称
83	category_small_code	string	小类编码
84	category_small_name	string	商品小类名称
85	stock_loc_code	string	库位编码
86	durability_day	int	保质天数
87	bar_code	string	商品条码
88	promotion_activity_code	string	促销活动编号
89	spec_remarks	string	规格备注
90	pick_status	int	拣货状态(0:待拣配,1:已拣配)
91	delivery_sys	string	拣发货系统(MALL:中台发货 WMS:红草发货)
92	pick_time	string	拣配时间
93	voice_ai	int	是否语音下单 0:否  1:是
94	purchase_unit	string	下单单位
95	purchase_unit_rate	decimal(20,6)	单位换算比例
96	item_status	int	订单行状态(0:已删除 1:初始状态 2:新增)
97	update_status_time	string	修改订单行状态时间
98	buyer_remarks_rate	decimal(20,6)	加工备注上浮比例
99	area_product_name	string	商品区域名称
100	coupons_price	decimal(20,6)	优惠券分摊金额
101	child_product_code	string	子商品编码
102	refund_no	string	退款单号
103	out_refund_no	string	外部退款单号
104	is_refund	int	是否有发起过退货，0：否，1：是
105	refund_order_type	int	退货单类型(0:差异单 1:退货单）
106	approve_time	string	审批时间
107	refund_apply_time	string	退货申请时间
108	refund_paid_time	string	最后一次退款时间
109	refund_finish_time	string	最后一次退货完成时间
110	refund_freight	decimal(20,6)	退款运费
111	refund_paid_value	decimal(20,6)	已退款金额
112	refund_reason	string	退货原因
113	refund_qty	decimal(20,6)	退货数量
114	refund_value	decimal(20,6)	退货金额
115	paid_value	decimal(20,6)	已支付金额
116	origin_freight	decimal(20,6)	原运费
117	reduce_freight	decimal(20,6)	减免运费
118	real_freight	decimal(20,6)	实际运费
119	tax_rate	decimal(20,6)	税费比例
120	purchase_qty	decimal(20,6)	购买数量
121	send_qty	decimal(20,6)	发货数量
122	sign_qty	decimal(20,6)	签收数量
123	origin_price	decimal(20,6)	正常售价
124	sap_price	decimal(20,6)	自建商品的SAP价格
125	promotion_price	decimal(20,6)	促销单价（如果没有促销，值和正常售价一样）
126	middle_price	decimal(20,6)	中台报价(计算销售毛利价格)
127	cost_price	decimal(20,6)	商品成本价即进价(财务记账价格,移动平均价或批次价格)
128	promotion_cost_price	decimal(20,6)	促销成本价（如果没有促销，值和中台报价一样）
129	supplier_cost_rate	decimal(20,6)	供应商摊分比率
130	origin_value	decimal(20,6)	原总金额（促销单价*购买数量）
131	reduce_value	decimal(20,6)	减免金额
132	real_value	decimal(20,6)	总计金额
133	sdt	string	日期分区, 按下单日期分区
134		NULL	NULL
135	# Partition Information	NULL	NULL
136	# col_name            	data_type           	comment             
137		NULL	NULL
 */

-- 明细数据量大，无法导出
SELECT order_no,
       dc_code,
       dc_name,
       sap_cus_code,
       sap_cus_name,
       goods_code,
       goods_name,
       purchase_qty*1.00 purchase_qty,
       send_qty*1.00 send_qty,
       sign_qty*1.00 sign_qty,
       coalesce(sign_qty/purchase_qty *1.00,0) as qty_sign_rate,
       origin_price,
       promotion_price,
       middle_price,
       origin_value *1.00 origin_value,
       real_value*1.00 real_value
FROM csx_dw.order_new_m
WHERE sdt>='20191001'
  AND order_status IN ('HOME',
                       'SUCCESS');
                
                
-- 单据签收率                
SELECT order_no,
       dc_code,
       dc_name,
       sap_cus_code,
       sap_cus_name,
       count(goods_code)*1.00  order_goods_cn,
       count(case when sign_qty<>0 then goods_code end )*1.00 sign_goods_cn,
       count(case when sign_qty<>0 then goods_code end )/count(goods_code) goods_sign_rate,
       sum(purchase_qty)*1.00 purchase_qty,
       sum(send_qty)*1.00 send_qty,
       sum(sign_qty)*1.00 sign_qty,
       sum(sign_qty)/sum(purchase_qty)*1.00 qty_sign_rate,
       sum(real_value)*1.00 real_value,
       order_date,
       require_delivery_date,
       regexp_replace(to_date(receive_time),'-','') as receive_date,
       sdt
FROM csx_dw.order_new_m
WHERE sdt>='20191001'
  AND order_status IN ('HOME',
                       'SUCCESS')
group BY order_no,
       dc_code,
       dc_name,
       sap_cus_code,
       sap_cus_name,
       order_status,
       order_date,
    require_delivery_date,
       to_date(receive_time),
       sdt;
       
       
-- 汇总签收率                
SELECT  dc_code,
       dc_name,
       count(sap_cus_code) order_cn,
       count(goods_code)*1.00 order_goods_cn,
       count(CASE
                 WHEN sign_qty<>0 THEN goods_code
             END)*1.00 sign_goods_cn,
       count(CASE
                 WHEN sign_qty<>0 THEN goods_code
             END)/count(goods_code) goods_sign_rate,
       sum(purchase_qty)*1.00 purchase_qty,
       sum(send_qty)*1.00 send_qty,
       sum(sign_qty)*1.00 sign_qty,
       sum(sign_qty)/sum(purchase_qty)*1.00 qty_sign_rate,
       sum(real_value)*1.00 real_value,
       concat(min(sdt),'-',max(sdt)) AS Date_range
FROM csx_dw.order_new_m
WHERE sdt>='20191001'
  AND sdt<='20191021'
  AND order_status IN ('HOME',
                       'SUCCESS')
GROUP BY dc_code,
         dc_name;