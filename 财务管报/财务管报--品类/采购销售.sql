--工厂销售成本

-- PO\KN source_order_type_code 类型
-- 109A移库无法关联采购入库单 
-- KN 包含商品转换、原料转成品
drop table csx_tmp.tmp_scm_sales_batch_detail_01;
create temporary table csx_tmp.tmp_scm_sales_batch_detail_01
as
select
  a.order_no,
  a.goods_code,
  sales_qty,
  sales_value,
  sales_cost,
  b.qty,
  b.price,
  b.source_order_no,
  wms_batch_no
from 
(
  select
    case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end as order_no,
    goods_code,
  sum(sales_qty) as sales_qty,
  sum(sales_value) as sales_value,
  sum(sales_cost) as sales_cost
  from csx_dw.dws_sale_r_d_detail  a --销售单工单+商品关联批次表,找到批次表的来源单号为工单及移动类型为销售出库、退货入库
   join 
   
  (select goods_id from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and classify_middle_code in ('B0304','B0305')) B ON A.goods_code=b.goods_id
  
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
    source_order_no,
  wms_batch_no
  from csx_dw.dws_wms_r_d_batch_detail
  where move_type in ('107A','108A') --107A 销售出库 108A 退货入库
    and source_order_no like 'PO%'
) b on a.order_no = b.link_wms_order_no and a.goods_code = b.goods_code
;




--工单推采购原料
drop table csx_tmp.tmp_scm_sales_batch_detail_02;
create temporary table csx_tmp.tmp_scm_sales_batch_detail_02
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
  received_qty,
  received_price1
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
    source_order_no,
  wms_batch_no
  from csx_tmp.tmp_scm_sales_batch_detail_01 a --销售订单商品
  join 
  (select goods_id from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and classify_middle_code in ('B0304','B0305')) B ON A.goods_code=b.goods_id
  
) a
left join
(
  select 
    order_code,--成品
    goods_code,--工单号
    batch_order_code,
  received_qty,
  received_price1
  from csx_dw.dws_scm_r_d_order_received  --order_code、goods_code、batch_order_code唯一
) b on a.source_order_no = b.order_code and a.goods_code = b.goods_code and a.wms_batch_no = b.batch_order_code
;
select sum( received_qty*received_price1),sum(price*received_qty) from csx_tmp.tmp_scm_sales_batch_detail_02;
select sum(price*qty) from csx_tmp.tmp_scm_sales_batch_detail_01;

-- source_order_type_code 无法找到，盘点类型
select * from csx_dw.dws_wms_r_d_batch_detail where batch_no ='CB20210801006351' and goods_code='1153249'
;
select * from csx_dw.dws_wms_r_d_batch_detail where batch_no='CB20210628103929' and goods_code='1214076';
select * from csx_dw.dws_wms_r_d_batch_detail where batch_no='CB20210803020424' and goods_code='1175095';
-- 202A转码
-- 119A移库
-- 111A 盘盈
--1、没有关联单据号(source_order_no)、link_wms_batch_no 属于盘点类型
2、没有关联单据号(link_wms_order_no) 转码
source_order_type_code  单据类型，如为空 盘点造成的，无法找到单据类型



select  
    channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(coalesce(raw_no_tax_amt,0)) as raw_no_tax_amt,
    sum(coalesce(raw_amt,0)) as raw_amt,
    sum(coalesce(finished_no_tax_amt,0)) as finished_no_tax_amt,
    sum(coalesce(finished_amt,0)) as finished_amt
from 
(select   channel_code,
    channel_name,
    c.business_type_code,
    business_type_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    link_wms_batch_no,
    a.batch_no,
    goods_code,
    -- sum(case when a.in_or_out=1 and a.move_type in ('119A','109A','119B','109B') then coalesce(if(move_type  in ('119A','109A'),amt_no_tax,amt_no_tax*-1),0) end ) as raw_no_tax_amt,   --原料领用成本未税
    -- sum(case when a.in_or_out=1 and a.move_type in ('119A','109A','119B','109B') then coalesce(if(move_type in ('119A','109A'),amt,amt*-1),0) end ) as raw_amt,                 -- 原料领用成本含税
    sum(case when a.in_or_out=0 and a.move_type in ('120A','120B','202A','202B') then coalesce(if(move_type in ('202A','120A'),amt_no_tax,amt_no_tax*-1),0) end ) as finished_no_tax_amt,   --成品未税
    sum(case when a.in_or_out=0 and a.move_type in ('120A','120B','202A','202B') then coalesce(if(move_type in ('202A','120A'),amt,amt*-1),0) end ) as finished_amt                    --成品含税
from csx_dw.dws_wms_r_d_batch_detail a 
join
    (select 
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_small_code
    from csx_dw.dws_basic_w_a_manage_classify_m 
        where sdt='current' 
         and classify_middle_code in ('B0304','B0305')
    ) b 
    on a.category_small_code=b.category_small_code

join 
( select  channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    wms_batch_no ,
    batch_no
from csx_tmp.temp_fac_sale_01
    group by  channel_code,
    channel_name,
    business_type_code,
    business_type_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    wms_batch_no,
    batch_no) c on a.batch_no=c.batch_no
where a.move_type in ('119A','119B','120A','120B')
group by
    channel_code,
    channel_name,
    c.business_type_code,
    business_type_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    link_wms_batch_no,
    goods_code,
     a.batch_no
) a 
   group by 
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
  grouping sets (
    ( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),
    (channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  -- 业务中类合计
        ( channel_code, 
    channel_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),  -- 渠道三级分类
        ( channel_code, 
    channel_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  --渠道+二级分类合计
        ( 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),   --三级分类汇总
        (
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  --二级分类汇总
        ( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name),  -- 一级分类汇总
         (channel_code,
        channel_name , classify_large_code, 
    classify_large_name),())
 ;



-- 采购入库
select sum(qty*price)
from (
select
  a.credential_no,
  a.order_no,
  a.goods_code,
  b.qty,
  b.price,
  b.source_order_no
from 
csx_tmp.temp_fina_sale_00  a
join
(
  select
    case when substr(link_wms_order_no,1,2) in ('03','04','06') then substr(link_wms_order_no,1,10) 
	  else link_wms_order_no end as link_wms_order_no,--107A 108A 对应销售单号
    move_type,
    goods_code,
    qty,
    price,
    source_order_no,
    credential_no
  from csx_dw.dws_wms_r_d_batch_detail
  where move_type in ('107A','108A') --107A 销售出库 108A 退货入库
    and source_order_no like 'PO%'
) b on a.credential_no = b.credential_no and a.goods_code = b.goods_code

) a;

