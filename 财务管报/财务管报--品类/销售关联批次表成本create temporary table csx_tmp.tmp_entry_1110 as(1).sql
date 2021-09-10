  create temporary table csx_tmp.tmp_entry_1110 as 
  select
    dc_code,
    case when substr(link_wms_order_no,1,2) in ('03','04','06') then substr(link_wms_order_no,1,10) 
	  else link_wms_order_no end as link_wms_order_no,--107A 108A 对应销售单号
    move_type,
    goods_code,
    b.goods_name,
    classify_middle_name,
    qty,
    price,
    source_order_no,
	wms_batch_no,
	a.batch_no,
	a.link_wms_batch_no
  from csx_dw.dws_wms_r_d_batch_detail a
   join
  (select goods_id,goods_name,classify_middle_code,classify_middle_name 
  from csx_dw.dws_basic_w_a_csx_product_m 
  where sdt='current' and classify_middle_code in ('B0304','B0305')
  ) B ON A.goods_code=b.goods_id
  where move_type in ('107A','108A') --107A 销售出库 108A 退货入库
  and sdt>='20210801'
    and sdt<'20210901'
    
;


select * from csx_dw.dws_wms_r_d_batch_detail where batch_no='CB20210812116101' and goods_code='1245473'
--and  move_type in ('107A','108A')
;

select * from  csx_tmp.tmp_entry_1110;

SELECT * FROM csx_tmp.tmp_entry_1111;
create temporary table csx_tmp.tmp_entry_1111 as 
select
  a.order_no,
  a.goods_code,
  sales_qty,
  sales_value,
  sales_cost,
  b.qty,
  b.price,
  b.source_order_no,
  wms_batch_no,
  classify_middle_code,
  classify_middle_name,
  batch_no,
  link_wms_batch_no
from 
(
  select
    case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end as order_no,
    goods_code,
    b.classify_middle_code,
    b.classify_middle_name ,
    sum(sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(sales_cost) as sales_cost
  from csx_dw.dws_sale_r_d_detail  a --销售单工单+商品关联批次表,找到批次表的来源单号为工单及移动类型为销售出库、退货入库
   join
  (select goods_id,classify_middle_code,classify_middle_name 
  from csx_dw.dws_basic_w_a_csx_product_m 
  where sdt='current' and classify_middle_code in ('B0304','B0305')
  ) B ON A.goods_code=b.goods_id
  where sdt >= '20210801' and sdt < '20210901'
  group by case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end,goods_code,
      b.classify_middle_code,b.classify_middle_name 
) a
left join
(
  select
    case when substr(link_wms_order_no,1,2) in ('03','04','06') then substr(link_wms_order_no,1,10) 
	  else link_wms_order_no end as link_wms_order_no,--107A 108A 对应销售单号
    move_type,
    goods_code,
    qty,
    price,
    source_order_no,
	wms_batch_no,
	a.batch_no,
	a.link_wms_batch_no
  from csx_dw.dws_wms_r_d_batch_detail a
  where move_type in ('107A','108A') --107A 销售出库 108A 退货入库
  --   and source_order_no like 'WO%'
) b on a.order_no = b.link_wms_order_no and a.goods_code = b.goods_code
;





SELECT * FROM csx_tmp.tmp_entry_1112 where credential_no='PZ20210801140294';


-- 通过销售凭证关联三级帐的凭证号107A，
drop table csx_tmp.tmp_entry_1112 ;
create temporary table csx_tmp.tmp_entry_1112 as 
select
a.credential_no,
  a.order_no,
  a.goods_code,
  sales_qty,
  sales_value,
  sales_cost,
  b.qty,
  b.price,
  b.source_order_no,
  wms_batch_no,
  classify_middle_code,
  classify_middle_name,
  batch_no,
  link_wms_batch_no
from 
(
  select
    split(id,'&')[0] as credential_no,
    case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end as order_no,
    goods_code,
 b.classify_middle_code,
 b.classify_middle_name ,
 sum(sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(sales_cost) as sales_cost
  from csx_dw.dws_sale_r_d_detail  a --销售单工单+商品关联批次表,找到批次表的来源单号为工单及移动类型为销售出库、退货入库
   join
  (select goods_id,classify_middle_code,classify_middle_name 
  from csx_dw.dws_basic_w_a_csx_product_m 
  where sdt='current' and classify_middle_code in ('B0304','B0305')
  ) B ON A.goods_code=b.goods_id
  where sdt >= '20210801' and sdt < '20210901'
    and a.sales_type!='fanli'
  group by case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end,goods_code,
      b.classify_middle_code,b.classify_middle_name ,
      split(id,'&')[0]
) a
left join
(
  select
    case when substr(link_wms_order_no,1,2) in ('03','04','06') then substr(link_wms_order_no,1,10) 
	  else link_wms_order_no end as link_wms_order_no,--107A 108A 对应销售单号
    move_type,
    goods_code,
    qty,
    price,
    source_order_no,
	wms_batch_no,
	a.batch_no,
	a.link_wms_batch_no,
	credential_no,
	a.source_order_type_code,
  from csx_dw.dws_wms_r_d_batch_detail a
  where move_type in ('107A') --107A 销售出库 108A 退货入库
  --   and source_order_no like 'WO%'
   and source_order_type_code in ('PO','KN')
) b on a.credential_no = b.credential_no and a.goods_code = b.goods_code
;





  select
    split(id,'&')[0] as credential_no,
    case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end as order_no,
    goods_code,
 b.classify_middle_code,
 b.classify_middle_name ,
 sum(sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(sales_cost) as sales_cost
  from csx_dw.dws_sale_r_d_detail  a --销售单工单+商品关联批次表,找到批次表的来源单号为工单及移动类型为销售出库、退货入库
   join
  (select goods_id,classify_middle_code,classify_middle_name 
  from csx_dw.dws_basic_w_a_csx_product_m 
  where sdt='current' and classify_middle_code in ('B0304','B0305')
  ) B ON A.goods_code=b.goods_id
  where sdt >= '20210801' and sdt < '20210901'
    and a.sales_type!='fanli'
  group by case when sales_type = 'bbc' then substr(order_no, 7, 10)
      else order_no end,goods_code,
      b.classify_middle_code,b.classify_middle_name ,
      split(id,'&')[0]
;





select * from csx_dw.dws_wms_r_d_batch_detail where batch_no='CB20210816019016' and goods_code='1277751'

;
select * from csx_dw.dws_wms_r_d_batch_detail where batch_no ='CB20210801006351' and goods_code='1153249'
;

select * from csx_dw.dws_wms_r_d_batch_detail where batch_no='CB20210628103929' and goods_code='1214076';


select * from csx_dw.dws_wms_r_d_batch_detail where batch_no='CB20210803020424' and goods_code='1175095';


select * from csx_dw.dws_wms_r_d_batch_detail where credential_no='PZ20210803182312' and goods_code='1175095';

