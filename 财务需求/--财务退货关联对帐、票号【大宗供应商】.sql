--财务退货关联对帐、票号【大宗供应商】

select  years,
	months,
	`sdt`   , 
  `purchase_org`  , 
  `purchase_org_name`  , 
  j.order_code ,
   receive_code ,
   batch_code  ,
  `sales_province_code` , 
  `sales_province_name` , 
  `source_type_name` , 
  `super_class_name` ,
  `dc_code`   ,
  `shop_name` ,
  `goods_code`,
  `goods_name`,
  `unit_name` ,
  `brand_name`,
  `division_code` ,
  `division_name` ,
  `department_id` ,
  `department_name`  , 
  `classify_large_code` ,
  `classify_large_name` ,
  `classify_middle_code`, 
  `classify_middle_name`, 
  `classify_small_code` ,
  `classify_small_name` ,
  `category_large_code` ,
  `category_large_name` ,
  `supplier_code`,
  `supplier_name`  ,
  `send_dc_code` ,
  `send_dc_name` ,
   settle_location_code   ,
   local_purchase_flag, 
  `business_type_name`  , 
  `order_qty`   , 
  `order_price1`,
  `order_price2`,
  `receive_qty` , 
  `receive_amt` , 
  `no_tax_receive_amt`  , 
  `shipped_qty` ,
  `shipped_amt` ,
  `no_tax_shipped_amt`  , 
  `receive_sdt`  , 
   order_create_date  ,
  `yh_reuse_tag` , 
  `daily_source`,
  pick_gather_flag, 
  urgency_flag, 
  has_change, 
  entrust_outside, 
  order_business_type  , 
  order_type ,   -- 订单类型订单类型(0-普通供应商订单 1-囤货订单 2-日采订单 3-计划订单)',
  extra_flag, --'补货标识',
  timeout_cancel_flag, --  '超时订单取消',
  joint_purchase_flag, -- '集采供应商',
  `supplier_type_code` ,             --'供应商类型编码', 
  `supplier_type_name` ,             --'供应商类型名称',  
  `business_owner_code`,             -- '业态归属编码',
  `business_owner_name`,             -- '业态归属名称',
  special_customer ,--'专项客户',
  borrow_flag ,--'是否借用',
  direct_trans_flag, -- '是否直供',
  supplier_classify_code ,-- '供应商类型编码  0：基础供应商   1:农户供应商',
   order_goods_status ,  -- '订单商品状态 状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)',
  `purpose` ,-- 'DC类型编码', 
  `purpose_name`, -- 'DC类型名称',
  j.order_code,original_order_code,check_ticket_no,statement_no,payment_no,invoice_bill_no,invoice_bill_code
from (
select substr(months,1,4) years,
	months,
	`sdt`   , 
  `purchase_org`  , 
  `purchase_org_name`  , 
  `order_code`  ,
   receive_code ,
   batch_code  ,
  `sales_province_code` , 
  `sales_province_name` , 
  `source_type_name` , 
  `super_class_name` ,
  `dc_code`   ,
  `shop_name` ,
  `goods_code`,
  `goods_name`,
  `unit_name` ,
  `brand_name`,
  `division_code` ,
  `division_name` ,
  `department_id` ,
  `department_name`  , 
  `classify_large_code` ,
  `classify_large_name` ,
  `classify_middle_code`, 
  `classify_middle_name`, 
  `classify_small_code` ,
  `classify_small_name` ,
  `category_large_code` ,
  `category_large_name` ,
  `supplier_code`,
  `supplier_name`  ,
  `send_dc_code` ,
  `send_dc_name` ,
   settle_location_code   ,
  if(`local_purchase_flag`='1','是','否') local_purchase_flag, 
  `business_type_name`  , 
  `order_qty`   , 
  `order_price1`,
  `order_price2`,
  `receive_qty` , 
  `receive_amt` , 
  `no_tax_receive_amt`  , 
  `shipped_qty` ,
  `shipped_amt` ,
  `no_tax_shipped_amt`  , 
  `receive_sdt`  , 
   order_create_date  ,
  `yh_reuse_tag` , 
  `daily_source`,
  if(`pick_gather_flag`='1','是','否')  pick_gather_flag, 
  if(`urgency_flag`='1','是','否')urgency_flag, 
  if(`has_change`='1','是','否')has_change, 
  if(`entrust_outside`='1','是','否')entrust_outside, 
  if(`order_business_type`='1','基地订单','') order_business_type  , 
  case when order_type='0' then '普通供应商订单'
      when order_type='1' then '囤货订单'
      when order_type='2' then '日采订单'
      when order_type='3' then '计划订单'
      else order_type end order_type ,   -- 订单类型订单类型(0-普通供应商订单 1-囤货订单 2-日采订单 3-计划订单)',
  if(extra_flag='1','是','否')extra_flag, --'补货标识',
  if(timeout_cancel_flag='1','是','否')timeout_cancel_flag, --  '超时订单取消',
  if(joint_purchase_flag='1','是','否')joint_purchase_flag, -- '集采供应商',
  `supplier_type_code` ,             --'供应商类型编码', 
  `supplier_type_name` ,             --'供应商类型名称',  
  `business_owner_code`,             -- '业态归属编码',
  `business_owner_name`,             -- '业态归属名称',
  if(`special_customer`='1','是','否')special_customer ,--'专项客户',
  if(`borrow_flag` = '1','是','否')borrow_flag ,--'是否借用',
  if(direct_trans_flag = '1','是','否')direct_trans_flag, -- '是否直供',
  case when supplier_classify_code='0' then '基础供应商'
      when supplier_classify_code='1' then '农户供应商'
      else supplier_classify_code end supplier_classify_code ,-- '供应商类型编码  0：基础供应商   1:农户供应商',
  case when order_goods_status='1' then '已创建'
      when order_goods_status='2' then '已发货'
      when order_goods_status='3' then '入库中'
      when order_goods_status='4' then '已完成'
      when order_goods_status='5' then '已取消'
      end order_goods_status ,  -- '订单商品状态 状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)',
  `purpose` ,-- 'DC类型编码', 
  `purpose_name` -- 'DC类型名称',
  from 
   csx_tmp.report_fr_r_m_financial_purchase_detail
  where order_code like 'RP%'
    and send_dc_code in ('W0K7','W0H4')
   
   ) j
left join 
(

select  order_code,original_order_code,b.check_ticket_no,b.statement_no,b.payment_no,invoice_bill_no,invoice_bill_code
from (
select distinct order_code,original_order_code from csx_dw.dws_scm_r_d_order_detail 
where 1=1
) a 
left join 
csx_tmp.ads_fr_r_d_po_reconciliation_report b on a.original_order_code=b.purchase_order_no 
left join 
csx_dw.dwd_pss_r_d_statement_invoice c on b.check_ticket_no=c.check_ticket_no
group by order_code,original_order_code,b.check_ticket_no,b.statement_no,b.payment_no,invoice_bill_no,invoice_bill_code
)a  on j.order_code=a.order_code;


show create table csx_dw.dwd_pss_r_d_statement_check_ticket;

show create table csx_dw.dwd_pss_r_d_statement_payment;

show create table csx_dw.dwd_pss_r_d_statement_statement_account;

show create table csx_dw.dwd_pss_r_d_statement_invoice;

'20034399','20043536','20045813','20047386','20049806','20052361','20052380','20053233','20053359','20055065'