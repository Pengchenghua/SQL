--采购关联工厂、再关联入库批次表、最后关联销售获取销售数据

--第一步
--使用工厂生产数据通过工单加原料关联成本核算凭证明细表的原单号和商品，在凭证中原料和成品都会产生凭证，并且凭证号是一致的。
--通过获取的凭证号和商品再去关联物流的入库批次表，可以得到工厂加工商品的原料的采购订单号。
--这里取能关联出采购订单号的工厂生产数据，存在部分获取不到采购单号的数据。
drop table csx_tmp.tmp_factory_order_to_scm_1;
create temporary table csx_tmp.tmp_factory_order_to_scm_1
as
select 
  factory_order_code,
  a.goods_code,
  product_code,
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
	product_price,
	goods_reality_receive_qty,
	fact_qty,
    credential_no as factory_credential_no
  from 
  (
    select 
	  product_code,
	  goods_code,
	  order_code,
	  max(product_price) as product_price,
	  sum(goods_reality_receive_qty) as goods_reality_receive_qty,
	  sum(fact_qty) as fact_qty
    from csx_dw.dws_mms_r_a_factory_order
	where sdt >= '20210401' and mrp_prop_key in('3061','3010') 
	group by product_code,goods_code,order_code
  ) a 
  left join 
  (
    select 
	  distinct
	  source_order_no,
	  credential_no,
	  product_code
    from csx_ods.source_cas_r_d_accounting_credential_item --凭证明细底表,通过工单和原料关联出凭证号
	where sdt = '19990101' and move_type = '119A'
  ) b on a.order_code = b.source_order_no and a.product_code = b.product_code
) a 
left join 
(
  select 
    distinct 
    source_order_no,
    credential_no,
    goods_code
  from csx_dw.dws_wms_r_d_batch_detail                --入库批次明细表,通过关联凭证号和原料编码关联获得采购单号
  where move_type = '119A' and source_order_no like 'PO%'
) b on a.factory_credential_no = b.credential_no and a.product_code = b.goods_code
where b.source_order_no is not null
;


--第二步
--在工厂加工关联到采购单号的基础上，用采购入库表去做关联，关联上的用工厂工单号做订单编码，工厂成品做商品编码，关联不上的
--直接用采购的采购订单做订单编码，采购商品编码做商品编码。
drop table csx_tmp.tmp_factory_order_to_scm_2;
create temporary table csx_tmp.tmp_factory_order_to_scm_2
as
select 
  a.order_code as scm_order_code,
  a.goods_code as scm_goods_code,
  a.order_qty,
  a.received_qty,
  a.received_price,
  a.received_amount,
  coalesce(factory_order_code,'') as factory_order_code,
  coalesce(b.goods_code,'') as factory_goods_code,
  coalesce(product_code,'') as product_code,
  coalesce(product_price,'') as product_price,
  coalesce(goods_reality_receive_qty,'') as goods_reality_receive_qty,
  coalesce(fact_qty,'') as fact_qty,
  case when b.factory_order_code is null then a.order_code 
    else b.factory_order_code end as order_code,
  case when b.factory_order_code is null then a.goods_code 
    else b.goods_code end as goods_code
from 
(
  select 
    order_code,
	goods_code,
	sum(order_qty) as order_qty,
	sum(received_qty) as received_qty,
	max(received_price1) as received_price,
	sum(received_amount) as received_amount
  from csx_dw.dws_scm_r_d_order_received
  where ((sdt >= '20210401' and sdt <= '20210430')
    or (sdt = '19990101' and substr(order_time,1,10) >= '20210401' 
	  and substr(order_time,1,10) <= '20210430'))
  group by order_code,goods_code
) a 
left join csx_tmp.tmp_factory_order_to_scm_1 b 
on a.order_code = b.scm_order_code and a.goods_code = b.product_code
;



--第三步
--在第二步基础上，用选择好的订单编码，商品编码再次关联物流入库批次表，获取销售出库数据，拿到销售出库凭证号，
--再利用商品加凭证号，即可关联出销售数据
drop table csx_tmp.tmp_factory_order_to_scm_3;
create temporary table csx_tmp.tmp_factory_order_to_scm_3
as
select 
  scm_order_code,
  scm_goods_code,
  a.order_qty,
  received_qty,
  received_price,
  received_amount,
  factory_order_code,
  factory_goods_code,
  product_code,
  product_price,
  goods_reality_receive_qty,
  fact_qty,
  order_code,
  a.goods_code,
  b.credential_no,
  purchase_price,
  middle_office_price,
  cost_price,
  sales_price,
  c.order_qty as sales_order_qty,
  sales_qty,
  sales_value,
  sales_cost,
  profit,
  middle_office_cost,
  front_profit
from csx_tmp.tmp_factory_order_to_scm_2 a 
left join 
(
  select
    goods_code,
    credential_no,
    source_order_no,
    sum(qty) as qty
  from csx_dw.dws_wms_r_d_batch_detail
  where move_type in ('107A', '108A')
  group by goods_code, credential_no, source_order_no
) b on a.order_code = b.source_order_no and a.goods_code = b.goods_code
left join 
(
  select 
    split(id,'&')[0] as credential_no,
    goods_code,
	sum(purchase_price*sales_qty)/sum(sales_qty) as purchase_price,
	sum(middle_office_price*sales_qty)/sum(sales_qty) as middle_office_price,
	sum(cost_price*sales_qty)/sum(sales_qty) as cost_price,
	sum(sales_price*sales_qty)/sum(sales_qty) as sales_price,
	sum(order_qty) as order_qty,
	sum(sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(sales_cost) as sales_cost,
	sum(profit) as profit,
	sum(middle_office_cost) as middle_office_cost,
	sum(front_profit) as front_profit
  from csx_dw.dws_sale_r_d_detail 
  where sdt >= '20210401'
  group by split(id,'&')[0],goods_code
) c on a.goods_code = c.goods_code and b.credential_no = c.credential_no
;







