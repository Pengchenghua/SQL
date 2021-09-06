--工厂销售成本
drop table csx_tmp.tmp_sales_batch_detail_01;
create temporary table csx_tmp.tmp_sales_batch_detail_01
as
select
  a.order_no,
  a.goods_code,
  sales_qty,
  sales_value,
  sales_cost,
  b.qty,
  b.price,
  b.source_order_no
from 
(
  select
    case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end as order_no,
    goods_code,
	sum(sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(sales_cost) as sales_cost
  from csx_dw.dws_sale_r_d_detail  --销售单工单+商品关联批次表,找到批次表的来源单号为工单及移动类型为销售出库、退货入库
  where sdt >= '20210801' and sdt < '20210901'
  group by case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end,goods_code
) a
join
(
  select
    case when substr(link_wms_order_no,1,2) in ('03','04','06') then substr(link_wms_order_no,1,10) 
	  else link_wms_order_no end as link_wms_order_no,--107A 108A 对应销售单号
    move_type,
    goods_code,
    qty,
    price,
    source_order_no
  from csx_dw.dws_wms_r_d_batch_detail
  where move_type in ('107A','108A') --107A 销售出库 108A 退货入库
    and source_order_no like 'WO%'
) b on a.order_no = b.link_wms_order_no and a.goods_code = b.goods_code
;




--工单推采购原料
drop table csx_tmp.tmp_sales_batch_detail_02;
create temporary table csx_tmp.tmp_sales_batch_detail_02
as
select
  order_no,
  a.goods_code,
  sales_qty,
  sales_value,	
  sales_cost,
  a.qty,
  a.price,
  order_code,
  goods_plan_receive_qty,
  goods_reality_receive_qty,
  p_total,
  fact_qty,
  fact_values
from
(
  select 
    order_no,
    goods_code,
    sales_qty,
    sales_value,
    sales_cost,
    qty,
    price,
    source_order_no
  from csx_tmp.tmp_sales_batch_detail_01  --销售订单商品
) a
left join
(
  select 
    goods_code,--成品
    order_code,--工单号
    sum(goods_plan_receive_qty) as goods_plan_receive_qty,  --商品计划生产数量
	  sum(goods_reality_receive_qty) as goods_reality_receive_qty,  --商品实际产量
    sum(p_total) as p_total,  --计划成本小计(工单计划成本)
    sum(fact_qty) as fact_qty,  --原料数量
    sum(fact_values) as fact_values  --原料金额成本(原料成本即入库成本)
  from csx_dw.dws_mms_r_a_factory_order
  group by goods_code,order_code
) b on a.source_order_no = b.order_code and a.goods_code = b.goods_code
;















































