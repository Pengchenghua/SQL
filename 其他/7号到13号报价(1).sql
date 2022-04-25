--福利单-中台已直接处理好(客户单品门店层级直接有采购报价销售成本)但库存移动平均价，销售价格需取销售表，或自己计算
/*select a.customer_number,a.customer_name,a.warehouse_code
,a.delivery_date,a.query_time,a.query_user_id,a.query_user_name,a.quotation_time
,a.quote_user_id,a.quote_user_name,a.valid_begin_time
,a.valid_end_time,a.status,a.delivery_mode
,b.product_code,b.product_name,b.unit,b.big_category_code
,b.big_category_name,b.mid_category_code,b.mid_category_name
,b.small_category_code,b.small_category_name,b.number
,b.purchase_unit_price,b.purchase_total_price,b.cost_total_price
from 
(select * from csx_dw.quotation_prices where sdt='20190708')a
join (select * from csx_dw.quotation_prices_info where sdt='20190708')b on a.quotation_number=b.quotation_number
*/

---所有订单的采购报价的计算逻辑
csx_dw.middle_office_prices
csx_dw.purchase_prices



--经OH183260000003核查，中台订单中订单金额是使用订单数量*商品单价
--SAP中开票时按照发货数量*商品单价，所以订单数量和发货数量在底层表中同时保留

--（1+仓储费率+运营费率）*采购报价
--1有经过报价系统的订单
--订单表可使用后，用订单表替换b2b_tmp.temp_sale0715
--采购报价时间逻辑调整，发货日期在报价开始时间之后，最新一次更新的即为当前的采购报价,去掉结束时间限制
set i_sdate = concat(substr(regexp_replace(date_sub(current_date,1),'-','') ,1,6),'07');--开始时间
set i_date='20190913';---结束时间限制


drop table b2b_tmp.temp_sale0715;
CREATE table b2b_tmp.temp_sale0715
as
select a.order_no,		--订单号
refund_no,		--退货单号
regexp_replace(to_date(delivery_time),'-','') as edate,		-- 这个特别注意、 是以分区时间取值的    个人觉得应该用出库配送时间作为发货时间（delivery_time）
dc_code,		--履约dc编码
''prefer_dc_code,	--库存dc编码
receive_mode,	--'收货方式，HOME:送货上门，SITE:门店自提',    订单表中只有home的方式和空白方式
order_mode,		--订单模式：0-配送,1-直送，2-自提，3-直通
order_kind,		--订单类型：NORMAL-普通单，WELFARE-福利单',
sap_cus_code cust_id,	--sap客户编码	
sap_sub_cus_code,		--sap子客户编码
product_code goodsid,	--商品编码
self_product_name,	--自建商品名称', 这个基本上数据基本上为空  20190910那天数据共2万9千多条、不为空的465条
tax_rate,		--税率
estimate_arrive_time,		--预计到达时间
case when refund_no is null or refund_no='' then purchase_qty else -1*refund_qty end purchase_qty, -- 购买数量
case when refund_no is null or refund_no='' then send_qty else -1*refund_qty end send_qty,		--	发货数量
case when refund_no is null or refund_no='' then sign_qty else -1*refund_qty end sign_qty,		--签收数量
origin_price,		-- 正常售价
promotion_price,	--促销价格
origin_cost_price,	--正常进价
case when refund_no<>''then -1*refund_value else origin_value end total_sale,	--原总金额
case when refund_no<>''then -1*refund_value else real_value end real_sale,	--总计金额
sdt	
from csx_dw.order_m_tmp a 	--订单表
where 
	sdt>=${hiveconf:i_sdate} 				-- 日期大于 20190901
	and a.product_code is not null;			--商品编码 检查数据没有null和空白 

-- 订单表 csx_dw.order_m_tmp  （宽表） 
--查看订单表中、有下单数量、但是为毛发货数量是因为没有发货、
 

 
 
drop table b2b_tmp.temp_order;
CREATE  table b2b_tmp.temp_order
as
select a.order_no,--订单号
refund_no,--退货单号
edate,-- 这个特别注意、 是以分区时间取值的    个人觉得应该用出库配送时间昨晚发货时间（delivery_time）
dc_code,--履约dc编码
prefer_dc_code,	--库存dc编码
receive_mode,	--'收货方式，HOME:送货上门，SITE:门店自提',    订单表中只有home的方式和空白方式
a.order_kind,--订单类型：NORMAL-普通单，WELFARE-福利单'
cust_id,	--sap客户编码
sap_sub_cus_code,--sap子客户编码
goodsid,--商品编码
self_product_name,--自建商品名称', 这个基本上数据基本上为空  20190910那天数据共2万9千多条、不为空的465条
tax_rate,--税率
origin_price,-- 正常售价
promotion_price,--促销价格
origin_cost_price,--正常进价
purchase_qty,-- 购买数量
send_qty,--	发货数量
total_sale,--原总金额
real_sale,	--总计金额
a.sdt,
order_mode,--订单模式：0-配送,1-直送，2-自提，3-直通

b.basis_type,	--基准类型【最新进价，含税移动平均价，上一次报价】',
b.goods_name,	--商品编码', 
b.unit,			--单位
b.category_large_name,	--大类
b.category_large_code,
b.category_middle_name,	--中类
b.category_middle_code,
b.category_small_name,	--小类
b.category_small_code,
b.guide_price,		--指导价格
b.purchase_price,	--采购报价	 在采购报价表中没有为0的数据
b.price_begin_time,	--报价开始时间
b.import_time,		--导入时间
b.price_end_time,	--报价结束时间
b.last_put_supplier,	--最近入库供应商', 
b.last_purchase_price,	--最近进价（含税）
b.moving_average_price,	--移动平均价
case when order_mode=3 then cut_through_price		--直通订单中台报价',
when order_mode=1 then through_price			--直达订单中台报价', 
when order_mode=0 then distribute_price			--配送订单中台报价
when order_mode=2 then take_delivery_price end middle_office_prices		-- '自提订单中台报价'   上面类型不同都是直接作为中台报价
from 
(
	select  * from b2b_tmp.temp_sale0715 where sdt>=${hiveconf:i_sdate}	-- 日期大于等于 20190901 这个条件可以省略、上面建临时表的时候已经限制了日期条件
) a  
 join 		
 (
	select *,case when type='福利单' then 'WELFARE' else 'NORMAL' end order_kind
	from csx_dw.goods_prices_m			--采购报价表
	where sdt=regexp_replace(current_date,'-','')		--因为数据是全量、所以去头一天数据
 ) b
 on (a.dc_code=b.warehouse_code and a.goodsid=b.goods_code and a.order_kind=b.order_kind);

 --使用商品编码、仓库编码、订单类型进行关联

 

drop table b2b_tmp.temp_res011;
CREATE table b2b_tmp.temp_res011
as
select a.order_no,--订单号
a.refund_no,--退货单号
edate,-- 这个特别注意、 是以分区时间取值的    个人觉得应该用出库配送时间昨晚发货时间（delivery_time）
dc_code,--履约dc编码
prefer_dc_code,--库存dc编码
receive_mode,--'收货方式，HOME:送货上门，SITE:门店自提',    订单表中只有home的方式和空白方式
order_kind,--订单类型：NORMAL-普通单，WELFARE-福利单'
cust_id,--sap客户编码
sap_sub_cus_code,--sap子客户编码
a.goodsid,	--商品编码
self_product_name,	--自建商品名称', 这个基本上数据基本上为空  20190910那天数据共2万9千多条、不为空的465条
tax_rate,			--税率
origin_price,		-- 正常售价
promotion_price,	--促销价格
origin_cost_price,	--正常进价
purchase_qty,	--购买数量
send_qty,	--发货数量
total_sale,--原总金额
real_sale,	--总计金额
order_mode,	--订单模式：0-配送,1-直送，2-自提，3-直通
basis_type,	--基准类型【最新进价，含税移动平均价，上一次报价】',
guide_price,	--指导价格
purchase_price,	--采购报价
a.last_put_supplier,--最近入库供应商', 
a.last_purchase_price,--最近进价（含税）
a.moving_average_price,	--移动平均价
middle_office_prices,	--中台报价
sdt
from 
(
	--这里取得是时间大于报价开始时间得所有数据
	select 
		* 
	from 
		b2b_tmp.temp_order 
	where 
		edate>=regexp_replace(to_date(price_begin_time),'-','') --报价开始时间、时间大于报价开始时间 算采购报价
	
)a 
join 	--自join  
(
	select 
		order_no,
		refund_no,
		goodsid,
		max(import_time)import_time  --最大导入时间			这里的逻辑是取商品订单最大的导入时间 
	from
		b2b_tmp.temp_order
	where 
		edate>=regexp_replace(to_date(price_begin_time),'-','')	--报价开始时间  时间大于报价开始时间 算采购报价
	group by 
		order_no,refund_no,goodsid
)b 
on 
(	
	a.order_no=b.order_no 		--订单号
	and 
	coalesce(a.refund_no,0)=coalesce(b.refund_no,0) 	--退货单号
	and 
	a.goodsid=b.goodsid 		--商品编码
	and 
	to_date(a.import_time)=to_date(b.import_time)	--a表得导入时间 等于 B表得最大导入时间  

);
--上面这个临时表得用于 获取每个订单商品得去重、得到最新每个订单的数据


--2如果一个商品有采购报价不适用当前订单,则其采购报价/中台报价就是移动平均价

--这里插入的逻辑是订单时间在采购报价之前、 业务逻辑取的是 上一次报价的采购报价 
--这种情况的采购报价、移动平均价和中台报价都是取得正常进价的值 、所以数据是一样的
insert into b2b_tmp.temp_res011
select 
a.order_no,	--订单号
a.refund_no,	--退货单号
a.edate,-- 这个特别注意、 是以分区时间取值的    个人觉得应该用出库配送时间昨晚发货时间（delivery_time）
a.dc_code,--履约dc编码
a.prefer_dc_code,--库存dc编码
a.receive_mode,--'收货方式，HOME:送货上门，SITE:门店自提',    订单表中只有home的方式和空白方式
a.order_kind,--订单类型：NORMAL-普通单，WELFARE-福利单'
a.cust_id,--sap客户编码
a.sap_sub_cus_code,--sap子客户编码
a.goodsid,--商品编码
self_product_name,	--自建商品名称', 这个基本上数据基本上为空  20190910那天数据共2万9千多条、不为空的465条
a.tax_rate,				--税率
a.origin_price,			-- 正常售价
a.promotion_price,		--促销价格
a.origin_cost_price,	--正常进价
a.purchase_qty,			--购买数量
a.send_qty,				--发货数量
a.total_sale,			--原总金额
a.real_sale,			--总计金额
a.order_mode,			--订单模式：0-配送,1-直送，2-自提，3-直通
'订单发生在报价前'basis_type,		--基准类型【最新进价，含税移动平均价，上一次报价】',
''guide_price,				--指导价格
a.origin_cost_price purchase_price,		--采购报价	 在采购报价表中没有为0的数据
''last_put_supplier,			--最近入库供应商', 
''last_purchase_price,			--最近进价（含税）
a.origin_cost_price moving_average_price,	--移动平均价	
a.origin_cost_price middle_office_prices, 	--中台报价
sdt 
from 
(
	select distinct 
		order_no,	--订单号
		refund_no,	--退货单号
		edate,		-- 这个特别注意、 是以分区时间取值的    个人觉得应该用出库配送时间昨晚发货时间（delivery_time）
		dc_code,	--履约dc编码
		prefer_dc_code,		--库存dc编码
		receive_mode,	--'收货方式，HOME:送货上门，SITE:门店自提',    订单表中只有home的方式和空白方式
		order_kind,		--订单类型：NORMAL-普通单，WELFARE-福利单'
		cust_id,	--sap客户编码
		sap_sub_cus_code,	--sap子客户编码
		goodsid,	--商品编码
		tax_rate,	--税率
		origin_price,	-- 正常售价
		promotion_price,	--促销价格
		origin_cost_price,		--正常进价
		purchase_qty,		--购买数量
		send_qty,		--发货数量
		total_sale,	--原总金额
		real_sale,	--总计金额
		order_mode	--订单模式：0-配送,1-直送，2-自提，3-直通
	from 
		b2b_tmp.temp_order 
	where 
		edate<regexp_replace(to_date(price_begin_time),'-','') 	
		--日期小于采购报价时间 、业务取上一次报价时间、最近进价的字段作用就体现出来了、查看采购报价表、最近进价字段中有2千多条为0
)a 
left join 
	b2b_tmp.temp_res011 b 
on 
(	
	a.order_no=b.order_no 
	and 
	coalesce(a.refund_no,0)=coalesce(b.refund_no,0) 
	and 
	a.goodsid=b.goodsid
)
where b.purchase_price is null;

/*
--全部订单中该部分数据量太大，暂时不取
--3如果一个商品没有采购报价，则其采购报价/中台报价就是移动平均价，import_time为null的处理
insert into b2b_tmp.temp_res011
select a.order_no,a.refund_no,a.edate,a.dc_code,a.prefer_dc_code,a.receive_mode,a.order_kind,a.cust_id,a.sap_sub_cus_code,
a.goodsid,a.tax_rate,a.origin_price,a.promotion_price,a.origin_cost_price,
a.purchase_qty,a.send_qty,
a.total_sale,a.real_sale,order_mode,
'没有报价'basis_type,
''status,''guide_price,origin_cost_price purchase_price,
''last_put_supplier,''last_purchase_price,a.origin_cost_price moving_average_price,
origin_cost_price middle_office_prices,sdt
from b2b_tmp.temp_sale0715  a  
left join (select warehouse_code,product_code,max(import_time) import_time
from csx_ods.purchase_prices_ods where sdt=regexp_replace(date_sub(current_date,1),'-','') 
group by warehouse_code,product_code) b
 on (a.dc_code=b.warehouse_code and a.goodsid=b.product_code)
 where b.import_time is null;
 */
 
drop table b2b_tmp.temp_sapsale;
CREATE temporary table b2b_tmp.temp_sapsale
as
select 
	shop_id,goods_code goodsid,
	sdt,							--应该去销售表中得销售日期、但是看一下数据、销售日期和分区时间是一致得、因为销售表是每日一个增量
	sum(a.sales_qty)qty,
	sum(a.sales_value)sale,
	sum(a.profit)profit
from 
	csx_dw.sale_b2b_item a 
where 
	sdt>=${hiveconf:i_sdate} 
group by shop_id,goods_code,sdt;



--明细
select e.sales_province,e.sales_city,
a.order_no,a.refund_no,a.edate,a.dc_code,a.prefer_dc_code,a.receive_mode,a.order_kind,a.cust_id,a.sap_sub_cus_code,
b.dept_id,b.dept_name,b.catg_m_id,b.catg_m_name,a.goodsid,b.goodsname,
case when self_product_name is not null and self_product_name<>'' then '是' else '否' end self_product,
a.tax_rate,a.origin_price,a.promotion_price,a.origin_cost_price,
a.purchase_qty,a.send_qty,
a.total_sale,a.real_sale,a.order_mode,
basis_type,
guide_price,purchase_price,
last_put_supplier,last_purchase_price,moving_average_price,
middle_office_prices,
c.price,c.cost_avg,c.profit_avg 
from 
	b2b_tmp.temp_res011 a 
join 
(
	select 
		goodsid,goodsname,dept_id,dept_name,catg_m_id,catg_m_name from dim.dim_goods where edate='9999-12-31'
)b 
on a.goodsid=b.goodsid 
left join 
(
	select * from csx_dw.customer_m where sdt=regexp_replace(date_sub(current_date,1),'-','')
)e 
on lpad(a.cust_id,10,'0')=lpad(e.customer_no,10,'0')
left join 
(
	select 
		sdt,shop_id,goodsid,sale/qty price,(sale-profit)/qty cost_avg,
		profit/qty profit_avg 
	from b2b_tmp.temp_sapsale where qty>0
)c 
on (a.dc_code=c.shop_id and a.goodsid=c.goodsid and a.edate=c.sdt)
where a.send_qty<>0 and receive_mode is not null and edate<=${hiveconf:i_date};




--汇总

select e.sales_province,dept_id,dept_name,catg_m_id,catg_m_name,a.goodsid,b.goodsname,a.cust_id,a.moving_average_price,c.cost_avg, a.purchase_price
,a.middle_office_prices,promotion_price,c.profit_avg,
sum(send_qty)send_qty from 
(select * from b2b_tmp.temp_res011 where send_qty<>0 and receive_mode is not null and edate<=${hiveconf:i_date}) a 
join (select goodsid,goodsname,dept_id,dept_name,catg_m_id,catg_m_name from dim.dim_goods where edate='9999-12-31')b on a.goodsid=b.goodsid
 left join (select * from csx_dw.customer_m where sdt=regexp_replace(date_sub(current_date,1),'-',''))e 
 on lpad(a.cust_id,10,'0')=lpad(e.customer_no,10,'0')
left join (select sdt,goodsid,shop_id,sale/qty price,(sale-profit)/qty cost_avg,
profit/qty profit_avg from b2b_tmp.temp_sapsale where qty>0)c on (a.dc_code=c.shop_id and a.goodsid=c.goodsid and a.edate=c.sdt)
group by e.sales_province,dept_id,dept_name,catg_m_id,catg_m_name,a.goodsid,b.goodsname,a.cust_id,a.moving_average_price,c.cost_avg, a.purchase_price
,a.middle_office_prices,promotion_price,c.profit_avg
order by dept_id,a.goodsid;


--对标门店进价售价
--生鲜取最近一次进货价格，12209条
drop table b2b_tmp.temp_pur;
CREATE temporary table b2b_tmp.temp_pur
as
select a.shop_id_in,a.goodsid,round(sum(pur_order_total_value)/sum(pur_order_qty),2) last_storage_price
from 
(select * from b2b.ord_orderflow_t 
where sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-3),'-','') ,1,6),'01')
 and  pur_order_qty<>0 and pur_order_total_value<>0 and 
 shop_id_in in ('9337','9109','9300','9272','9241','9012','9423','9340','9344','9448','9149')
and pur_grp like 'H%')a 
join 
(select shop_id_in,goodsid,max(sdt)sdt from b2b.ord_orderflow_t 
where sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-3),'-','') ,1,6),'01')
 and  pur_order_qty<>0 and pur_order_total_value<>0 and 
 shop_id_in in ('9337','9109','9300','9272','9241','9012','9423','9340','9344','9448','9149')
and pur_grp like 'H%'
group by shop_id_in,goodsid)b on (a.shop_id_in=b.shop_id_in and a.goodsid=b.goodsid and a.sdt=b.sdt)
group by a.shop_id_in,a.goodsid;


select case when e.last_storage_price is not null then '近3个月最后一次入库价' else '商品资料表价' end price_type,
a.dc_code,a.goodsid,b.goodsname,b.dept_id,b.dept_name,catg_m_id,catg_m_name,
moving_average_price,purchase_price,middle_office_prices,origin_cost_price,promotion_price,send_qty,
d.shop_id,d.shop_name,
case when b.dept_id like 'H%'and e.last_storage_price is not null then e.last_storage_price
when new_pur_price>0 then new_pur_price when new_pur_price=0 and original_pur_price>0 then original_pur_price
else price_no_lvl end pur_price,
case when new_sale_price>0 then new_sale_price when new_sale_price=0 and original_sale_price>0 then original_sale_price
else price_no_lvl end sale_price
from 
(select dc_code,goodsid,
moving_average_price,purchase_price,middle_office_prices,origin_cost_price,promotion_price,
sum(send_qty)send_qty
 from b2b_tmp.temp_res011 where basis_type='含税移动平均价' and edate<=${hiveconf:i_date}
 group by dc_code,goodsid,
moving_average_price,purchase_price,middle_office_prices,origin_cost_price,promotion_price)a
join (select goodsid,goodsname,dept_id,dept_name,catg_m_id,catg_m_name from dim.dim_goods where edate='9999-12-31')b on a.goodsid=b.goodsid
join 
(select 'W0A3' dc_code, '9109' shop_id
union all 
select 'W0A7' dc_code, '9300' shop_id
union all
select 'W0B2' dc_code, '9337' shop_id
union all
select 'W0A5' dc_code, '9272' shop_id
union all
select 'W0A6' dc_code, '9241' shop_id
union all
select 'W0A8' dc_code, '9012' shop_id
union all
select 'W0F3' dc_code, '9423' shop_id
union all
select 'W0F2' dc_code, '9340' shop_id
union all
select 'W0F7' dc_code, '9344' shop_id
union all
select 'W0F4' dc_code, '9448' shop_id
union all
select 'W0A2' dc_code, '9149' shop_id)c on a.dc_code=c.dc_code
left join (select * from dw.shop_goods_fct
where sdt=regexp_replace(date_sub(current_date,1),'-','')
and shop_id in ('9337','9109','9300','9272','9241','9012','9423','9340','9344','9448','9149'))d 
on (a.goodsid=d.goodsid and c.shop_id=d.shop_id)
left join b2b_tmp.temp_pur e on (a.goodsid=e.goodsid and c.shop_id=e.shop_id_in)
;










--新表添加对标门店    只做有报价得商品和对标商品、和订单无关、、所以直接替换商品报价表即可~

drop table b2b_tmp.temp_pur;

--临时表取需要的对标门店
CREATE temporary table b2b_tmp.temp_pur
as
select 
a.shop_id_in,
a.goodsid,
round(sum(pur_order_total_value)/sum(pur_order_qty),2) last_storage_price	--四舍五入取值（round函数） 最后平均价
from 
(
	select 
		* 
	from 
		b2b.ord_orderflow_t 
	where 
		sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-3),'-','') ,1,6),'01')
 	and  
 	pur_order_qty<>0 and pur_order_total_value<>0 
 	and 
 	shop_id_in in ('9337','9109','9300','9272','9241','9012','9423','9340','9344','9448','9149','9201','9753')
	and 
	pur_grp like 'H%'
)a 
join 
(
	select 
		shop_id_in,
		goodsid,
		max(sdt)sdt 
	from 
		b2b.ord_orderflow_t 
	where 
		sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-3),'-','') ,1,6),'01')
 		and  
 		pur_order_qty<>0 
 		and 
 		pur_order_total_value<>0 
 		and 
 		shop_id_in in ('9337','9109','9300','9272','9241','9012','9423','9340','9344','9448','9149')
		and 
		pur_grp like 'H%'
	group by 
	shop_id_in,
	goodsid
)b 
on (a.shop_id_in=b.shop_id_in and a.goodsid=b.goodsid and a.sdt=b.sdt)
group by 
	a.shop_id_in,
	a.goodsid;








--查询
select 
	case when e.last_storage_price is not null then '近3个月最后一次入库价' else '商品资料表价' end price_type,	--进价来源
	case when dc_code='W0A5' then '江苏' 
		 when dc_code='W0A6' then '四川'
		 when dc_code='W0A7'  then '重庆'
		 when dc_code ='W0A3' then  '北京'
		 when dc_code= 'W0B2' then '上海' 
		 when dc_code= 'W0A2' then '安徽'
		 when dc_code= 'W0A8' then '福建'
		 when dc_code= 'W0F3' then '福建'
		 when dc_code= 'W0F2' then '福建'
		 when dc_code= 'W0F7' then '福建'
		 when dc_code= 'W0F4' then '福建'
		 when dc_code= 'W0K9' then '河北'
		 when dc_code= 'W0J7' then '深圳'
	end  province,	--省
	case when dc_code='W0A5' then '苏州' 
		 when dc_code='W0A6' then '成都'
		 when dc_code='W0A7'  then '重庆'
		 when dc_code ='W0A3' then  '北京'
		 when dc_code= 'W0B2' then '上海' 
		 when dc_code= 'W0A2' then '合肥'
		 when dc_code= 'W0A8' then '福州'
		 when dc_code= 'W0F3' then '厦门'
		 when dc_code= 'W0F2' then '泉州'
		 when dc_code= 'W0F7' then '南平'
		 when dc_code= 'W0F4' then '莆田'
		 when dc_code= 'W0K9' then '石家庄'
		 when dc_code= 'W0J7' then '深圳'
	end  city,	--市

	a.dc_code,		--发货DC 履约DC
	a.goodsid,		
	b.goodsname,
	b.dept_id,
	b.dept_name,
	catg_m_id,
	catg_m_name,
	moving_average_price,	--移动平均
	purchase_price,			--采购
	cut_through_price,	--'直通订单中台报价
	through_price,		--直达订单中台报价
	distribute_price,	--配送订单中台报价
	take_delivery_price,--自提订单中台报价
	price_begin_time, 	--报价开始时间
	price_end_time,		--报价结束时间
	last_put_time,		--最后入库时间
	last_purchase_price,	--最新进价（含税）
	d.shop_id,		--门店编码
	d.shop_name,	--门店名称
	case when 
		b.dept_id like 'H%'
		and 
		e.last_storage_price is not null 	
		then 
		e.last_storage_price			 
	when 
		new_pur_price>0 	--新进价 当新进价有值取新进价
		then 
		new_pur_price 
	when 
		new_pur_price=0 
		and 
		original_pur_price>0 	--原进价  当新进价为0取原进价作为进价
		then 
		original_pur_price
	else 
		price_no_lvl 		--价格	(条件金额或百分数 ) 无等级存在'
	end 
		pur_price,		--进货价格		
	case when 
			new_sale_price>0 	--新售价
		then 
			new_sale_price 
	when 
		new_sale_price=0 		
		and 
		original_sale_price>0		--原售价 	
	then 
		original_sale_price
	else 
		price_no_lvl 	--价格	(条件金额或百分数 ) 无等级存在'
	end 
		sale_price 		--销售价格	


from 
(
	select 
		case when type='福利单' then 'WELFARE' else 'NORMAL' end order_kind,
		warehouse_code as dc_code,		--履约DC
		goods_code as goodsid,		--商品id
		moving_average_price,--移动平均
		purchase_price,		--采购报价
		cut_through_price,	--'直通订单中台报价
		through_price,		--直达订单中台报价
		distribute_price,	--配送订单中台报价
		take_delivery_price,--自提订单中台报价	
		price_begin_time, 	--报价开始时间
		price_end_time,		--报价结束时间
		last_put_time,		--最后入库时间
		last_purchase_price	--最新进价（含税）


 	from 
 		csx_dw.goods_prices_m		--商品报价表、每天都是全量、
 	where 
 		sdt='20190920'		

 		and 
 	    type='普通单'
 	group by 
 		warehouse_code,
 		goods_code,
		moving_average_price,
		purchase_price,
		cut_through_price,	--'直通订单中台报价
		through_price,		--直达订单中台报价
		distribute_price,	--配送订单中台报价
		take_delivery_price,--自提订单中台报价			
		price_begin_time, 	--报价开始时间
		price_end_time,		--报价结束时间
		last_put_time,		--最后入库时间
		last_purchase_price	--最新进价（含税）

)a
join 
(
	select 
		goodsid,		--商品编码
		goodsname,		--商品名称
		dept_id,		--课组
		dept_name,		--课组名称
		catg_m_id,		--部类
		catg_m_name 
	from 
		dim.dim_goods 		--商品资料表
	where 
		edate='9999-12-31'
)b on a.goodsid=b.goodsid
join 
(
	select 
		'W0A3' dc_code, '9109' shop_id
	union all 
	select 
		'W0A7' dc_code, '9300' shop_id
	union all
	select 
		'W0B2' dc_code, '9337' shop_id
	union all
	select 
		'W0A5' dc_code, '9272' shop_id
	union all
	select 
		'W0A6' dc_code, '9241' shop_id
	union all
	select 
		'W0A8' dc_code, '9012' shop_id
	union all
	select 
		'W0F3' dc_code, '9423' shop_id
	union all
	select 
		'W0F2' dc_code, '9340' shop_id
	union all
	select 
		'W0F7' dc_code, '9344' shop_id
	union all
	select 
		'W0F4' dc_code, '9448' shop_id
	union all
	select 
		'W0A2' dc_code, '9149' shop_id
	union all
	select
		'W0J7' dc_code, '9753' shop_id
	union all
	select
		'W0K9' dc_code, '9201' shop_id 
)c on a.dc_code=c.dc_code
left join 
(
	select
		 * 
	from 
		dw.shop_goods_fct	--门店商品事实表		
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','')
		and 
		shop_id in ('9337','9109','9300','9272','9241','9012','9423','9340','9344','9448','9149')
)d 
on (a.goodsid=d.goodsid and c.shop_id=d.shop_id)
left join 
	b2b_tmp.temp_pur e 				--对标门店临时表  创建表得时候只取了对标门店信息
	on (a.goodsid=e.goodsid and c.shop_id=e.shop_id_in)
;











id string comment '主键ID',
order_no string comment '交易单号',
refund_no string comment '退货单号',
edate date comment '发货日期',
dc_code string comment '履约dc编码',
prefer_dc_code string comment '库存dc编码',
receive_mode string comment '收货方式',
order_kind string comment '订单类型',
cust_id string comment '客户编码',
sap_sub_cus_code string comment 'SAP买家客户子账号',
goodsid string comment '商品编码',
tax_rate decimal(26,4) comment '税费比例',
origin_price decimal(26,4) comment'正常售价',
promotion_price decimal(26,4) comment'促销单价',
origin_cost_price decimal(26,4) comment'正常进价',
purchase_qty decimal(26,4) comment '购买数量',
send_qty decimal(26,4) comment '发货数量',
total_sale decimal(26,4) comment '原总金额',
real_sale decimal(26,4) comment '总计金额',
order_mode string comment '订单模式',
basis_type string comment '基准类型',
status string comment '状态',
guide_price decimal(26,4) comment '销售指导价',
purchase_price decimal(26,4) comment '采购报价',
last_put_supplier string comment '最近入库供应商',
last_purchase_price decimal(26,4) comment '最近进价（含税）',
moving_average_price decimal(26,4) comment '移动平均价',
middle_office_prices decimal(26,4) comment '中台报价',
sdt string comment '时间分区'





--中台报价配置，不同的配送类型加上不一样的费率
--订单模式：0-配送,1-直送，2-自提，3-直通

order_kind订单类型：NORMAL-普通单，WELFARE-福利单
order_mode订单模式：0-配送,1-直送，2-自提，3-直通