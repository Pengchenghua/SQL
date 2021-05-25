--采购关联工厂、再关联入库批次表，获得采购入库到入库批次环节，满足可关联销售表在销售表中标记是否采购入库异常目的


--第一步
--使用工厂生产数据通过工单加原料关联成本核算凭证明细表的原单号和商品，在凭证中原料和成品都会产生凭证，并且凭证号是一致的。
--通过获取的凭证号和商品再去关联物流的入库批次表，可以得到工厂加工商品的原料的采购订单号。
--这里取能关联出采购订单号的工厂生产数据，存在部分获取不到采购单号的数据。
--临时表1：工厂-凭证明细-入库批次明细
drop table csx_tmp.tmp_factory_order_to_scm_11;
create temporary table csx_tmp.tmp_factory_order_to_scm_11
as
select 
  factory_order_code,
  a.goods_code,
  product_code,
  fact_values,
  product_price,
  goods_reality_receive_qty,
  fact_qty,
  source_order_no as scm_order_code
from 
(
  select 
    order_code as factory_order_code,
	a.goods_code,
	a.product_code,
	fact_values,
	product_price,
	goods_reality_receive_qty,
	fact_qty,
    credential_no as factory_credential_no
  from 
  --工厂加工
  (
    select 
	  product_code,--原料编号
	  goods_code,--成品商品编号
	  order_code,
	  sum(fact_values) as fact_values,--原料金额
	  max(product_price) as product_price,
	  sum(goods_reality_receive_qty) as goods_reality_receive_qty,
	  sum(fact_qty) as fact_qty
    from csx_dw.dws_mms_r_a_factory_order
	where sdt >= '20210301' and mrp_prop_key in('3061','3010') 
	group by product_code,goods_code,order_code
  )a 
  --凭证明细底表,通过工单和原料关联出凭证号
  left join 
  (
    select 
	  distinct
	  source_order_no,
	  credential_no,
	  product_code
    from csx_ods.source_cas_r_d_accounting_credential_item
	where sdt = '19990101' and move_type = '119A'
  )b on a.order_code = b.source_order_no and a.product_code = b.product_code
)a 
--入库批次明细表,通过关联凭证号和原料编码关联获得采购单号 能关联到的为工厂加工
left join 
(
  select 
    distinct 
    source_order_no,
    credential_no,
    goods_code
  from csx_dw.dws_wms_r_d_batch_detail  
--119A-原料转成品，PO-采购单
  where move_type = '119A' and source_order_no like 'PO%'
)b on a.factory_credential_no = b.credential_no and a.product_code = b.goods_code
where b.source_order_no is not null;


--第二步
--在工厂加工关联到采购单号的基础上，用采购入库表去做关联，关联上的用工厂工单号做订单编码，工厂成品做商品编码，关联不上的
--直接用采购的采购订单做订单编码，采购商品编码做商品编码。
--临时表2：采购入库表区分是否工厂加工得到最终可关联的订单编码及商品编码
drop table csx_tmp.tmp_factory_order_to_scm_12;
create temporary table csx_tmp.tmp_factory_order_to_scm_12
as
select 
  c.province_code DC_province_code,--省区编码
  c.province_name DC_province_name,--省区
  c.city_group_code DC_city_group_code,--城市组编码
  c.city_group_name DC_city_group_name,--城市组
  a.target_location_code DC_DC_code, --DC编码
  c.shop_name DC_DC_name,  --DC名称
  a.order_code as scm_order_code,
  a.goods_code as scm_goods_code,
  a.order_qty,
  a.received_qty,
  a.received_price,
  a.received_amount,
  coalesce(b.factory_order_code,'') as factory_order_code,
  coalesce(b.goods_code,'') as factory_goods_code,
  coalesce(b.product_code,'') as product_code,
  coalesce(b.fact_values,'') as fact_values,
  coalesce(b.product_price,'') as product_price,
  coalesce(b.goods_reality_receive_qty,'') as goods_reality_receive_qty,
  coalesce(b.fact_qty,'') as fact_qty,
  case when b.fact_qty is not null then '是' end as is_fact, --是否有原料价
  --关联上的用工厂工单号做订单编码，否则用采购的采购订单做订单编码
  case when b.factory_order_code is null then a.order_code 
    else b.factory_order_code end as order_code,
  --关联上的工厂成品做商品编码，否则用采购商品编码做商品编码
  case when b.factory_order_code is null then a.goods_code 
    else b.goods_code end as goods_code
from 
  --采购入库表
  (
    select 
      target_location_code,
	  order_code,
  	  goods_code,
  	  sum(order_qty) as order_qty,
  	  sum(received_qty) as received_qty,
  	  max(received_price1) as received_price,
  	  sum(received_amount) as received_amount
    from csx_dw.dws_scm_r_d_order_received
    where ((sdt >= '20210301' and sdt <= '20210601')
      or (sdt = '19990101' and substr(order_time,1,10) >= '20210301' 
  	  and substr(order_time,1,10) <= '20210601'))
    group by target_location_code,order_code,goods_code
  )a 
--第一步结果表 工厂工厂-凭证明细-入库批次明细
left join csx_tmp.tmp_factory_order_to_scm_11 b on a.order_code = b.scm_order_code and a.goods_code = b.product_code
left join 
  (select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current')c on c.shop_id = a.target_location_code	
where c.province_name in('重庆市','安徽省');



--第三步
--在第二步基础上，用选择好的订单编码，商品编码再次关联物流入库批次表，获取销售出库数据，拿到销售出库凭证号，
--再利用商品加凭证号，即可关联出销售数据
--临时表3：采购入库到最终入库批次明细（可根据商品于凭证号关联到销售表）
drop table csx_tmp.tmp_factory_order_to_scm_13;
create table csx_tmp.tmp_factory_order_to_scm_13
as
select 
  a.DC_province_code,--省区编码
  a.DC_province_name,--省区
  a.DC_city_group_code,--城市组编码
  a.DC_city_group_name,--城市组
  a.DC_DC_code, --DC编码
  a.DC_DC_name,  --DC名称  
  a.scm_order_code,
  a.scm_goods_code,
  a.order_qty,
  a.received_qty,
  a.received_price,
  a.received_amount,
  a.factory_order_code,
  a.factory_goods_code,
  a.product_code,
  e.goods_name product_name,
  a.fact_values,
  a.product_price,
  a.goods_reality_receive_qty,
  a.fact_qty,
  a.fact_values/a.fact_qty as fact_price,
  a.is_fact, --是否有原料价
  a.order_code, --批次入库单号或工单号
  a.goods_code,
  b.credential_no,
  b.batch_no,
  c.goods_name,--商品名称
  c.unit,--单位
  c.unit_name,--单位名称
  c.division_code,--部类编码
  c.division_name,--部类名称
  c.department_id ,--课组编码
  c.department_name ,--课组名称
  c.classify_middle_code,--管理中类编码
  c.classify_middle_name, --管理中类名称 
  d.received_qty_ls,d.received_value_ls,d.received_price_ls,
  d.received_qty_last,d.received_value_last,d.received_price_last,
  d.received_qty_yc,d.received_value_yc,d.received_price_yc,
  d.received_price_hight, --入库价异常高
  d.received_price_low, --入库价异常低
  d.received_price_up, --入库价突涨
  d.received_price_down  --入库价突降
from csx_tmp.tmp_factory_order_to_scm_12 a 
--入库批次明细表
left join 
  (
    select
      goods_code,
      credential_no,
	  batch_no,
      source_order_no,
      sum(qty) as qty
    from csx_dw.dws_wms_r_d_batch_detail
    where move_type in ('107A', '108A')
    group by goods_code,credential_no,batch_no,source_order_no
  )b on a.order_code = b.source_order_no and a.goods_code = b.goods_code
--商品维表
left join 
  (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')c on c.goods_id = a.goods_code	
--采购入库异常标签表  
left join 
  (select * from csx_tmp.tmp_goods_received_d where scm_sdt>='20210301'
  )d on d.order_code = a.scm_order_code and d.goods_code = a.scm_goods_code
--商品维表
left join 
  (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')e on e.goods_id = a.product_code  
;







