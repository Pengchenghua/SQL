show create table csx_dw.dwd_pss_r_d_statement_source_bill ;
--  财务信息 （对账信息——对帐表——勾票表——付款表）
-- 对帐信息表 
drop table csx_tmp.temp_pss_01;
create table csx_tmp.temp_pss_01 as 
select a.purchase_no,           --采购单号
       a.bill_no,               ----1 入库单(批次单号) 2、结算单
       a.check_ticket_no,       ----勾票单号
       a.statement_no,      --对帐单号
       a.payment_no,        --实际付款单号
       a.statement_date,        --对帐日期
       a.finance_statement_date,    --财务对帐日期
       a.pay_create_date,   ---付款生成日期
       a.paid_date,         --付款日期
       b.sign_date,         --供应商签单日期
       c.audit_date,        --票核日期
       c.invoice_sub_date,  --发票录入日期
       payment_date,      --付款日期
       review_date,          --审核时间 
       payment_status
from 
(select purchase_no,  --采购单号
    bill_no,   --1 入库单(批次单号) 2、结算单
    payment_no,     --实际付款单号
    check_ticket_no,    --勾票单号
    happen_date , --发生日期（入库单日期）结算单(归属日期)
    statement_no,  --对帐单号
    payment_status,     --付款状态 0-未生成 1 已生成未审核 2 已生成已审核  3已发起付款 4 已付款成功
   to_date(statement_time) as  statement_date, --对帐日期
   to_date(finance_statement_time) as  finance_statement_date, --财务对帐日期
   to_date(pay_create_time) as  pay_create_date,        --付款生成日期
   to_date(paid_time) paid_date               --付款日期
from csx_dw.dwd_pss_r_d_statement_source_bill 
where sdt>='20190101'
) a 
left join
--对帐单
(select statement_no,   --对帐单号
        check_ticket_no, --勾票单号
        to_date(sign_date) as sign_date,     -- 供应商签单日期
        to_date(audit_date) as audit_date  --票核日期
from csx_dw.dwd_pss_r_d_statement_statement_account  
where sdt>='20190101'
group by
        statement_no,   --对帐单号
        check_ticket_no, --勾票单号
        to_date(sign_date) ,     -- 供应商签单日期
        to_date(audit_date)
) b on a.statement_no=b.statement_no
left join
--勾票表
(
select check_ticket_no,     --勾票单号
        to_date(check_date) as check_date,         --勾票日期
        to_date(invoice_sub_date)as invoice_sub_date,   --发票录入日期
        to_date(audit_date) as audit_date          --票核日期
from csx_dw.dwd_pss_r_d_statement_check_ticket 
where sdt>='20190101' 
group by check_ticket_no,     --勾票单号
        to_date(check_date) ,         --勾票日期
        to_date(invoice_sub_date),   --发票录入日期
        to_date(audit_date) ) c on a.check_ticket_no=c.check_ticket_no 
left join 

--付款表
(
select payment_no,      --付款单号
    to_date(payment_time ) as payment_date,       --付款时间
    to_date(review_time) as review_date         --审核时间 
from csx_dw.dwd_pss_r_d_statement_payment
where sdt>='20190101' 
group by 
 payment_no,      --付款单号
    to_date(payment_time ) ,       --付款时间
    to_date(review_time)            
) d on a.payment_no=d.payment_no
;


-- 采购订单 创建日期 入库日期 、关单日期  管理分类
drop table  csx_tmp.temp_pss_00 ;
create table csx_tmp.temp_pss_00 as 
select source_bill_no,  --采购订单
    b.order_code,
    c.order_code as entry_order_no,
    in_out_no,          --批次单号
    company_code,
    company_name,
    happen_place_code,
    settle_place_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    order_create_date,
    supplier_code,
    receive_date	,
    receive_close_date,
    post_date
from 
(select source_bill_no,  --采购订单
    in_out_no,          --批次单号
    company_code,
    company_name,
    happen_place_code,
    settle_place_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from csx_dw.dwd_pss_r_d_settle_inout_detail a  --采购订单
left join 
(SELECT goods_id,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m    --商品资料表
WHERE sdt='current') b on a.product_code=b.goods_id
where sdt>='20190101'
    and a.source_bill_type=1
group by 
     source_bill_no,  --采购订单
    in_out_no,          --批次单号
    company_code,
    company_name,
    happen_place_code,
    settle_place_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
) a 

left join
-- 入库批次表
(select batch_code,
    order_code,
    to_date(receive_time) as receive_date	,
    to_date(close_time) receive_close_date,
    to_date(post_time) post_date,
    origin_order_code
    from csx_dw.dws_wms_r_d_entry_batch 
  --  where batch_code='TK190925001725'
  where (sdt>='20190101' or sdt='19990101')
 group by  batch_code,to_date(receive_time) 	,
    to_date(close_time),
    order_code,
    origin_order_code,
    to_date(post_time)
    ) as c on a.in_out_no=c.batch_code
left join  
-- 采购订单表头
(select order_code,to_date(create_time)as order_create_date,supplier_code ,received_order_code
from csx_dw.dwd_scm_r_d_order_header 
    where  (sdt>='20190101' or sdt='19990101')
group by  order_code,to_date(create_time),supplier_code,received_order_code) b on c.origin_order_code=b.order_code   
;

    --   a.statement_date,        --对帐日期
    --   a.finance_statement_date,    --财务对帐日期
    --   a.pay_create_date,   ---付款生成日期
    --   a.paid_date,         --付款日期
    --   b.sign_date,         --供应商签单日期
    --   c.audit_date,        --票核日期
    --   c.invoice_sub_date,  --发票录入日期
    --   payment_date,      --付款日期
    --   review_date,          --审核时间 
    --   ;

-- 如果付款日期为空，则按当前日期进行计算（搜索下载日或T-1日）；如果被减日期也为空，则计算结果为空
drop table  csx_tmp.temp_pss_02;
create  table csx_tmp.temp_pss_02 as 
select 
    order_code ,
    entry_order_no,
    in_out_no as batch_co,          --批次单号
    company_code,
    company_name,
    happen_place_code as receive_dc_id,
    d.shop_name as receive_dc_name ,
    settle_place_code as settle_dc_id,
    f.shop_name as settle_dc_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    order_create_date,
    a.supplier_code,
    supplier_name,
    reconciliation_tag,
    dic_value as reconciliation_tag_name,
    receive_date	,       --收货日期
    receive_close_date,     --关单日期
    post_date,              --过帐日期
    check_ticket_no,       --勾票单号
    statement_no,      --对帐单号
    payment_no,        --实际付款单号
    statement_date,        --对帐日期
    finance_statement_date,     --财务对帐日期
    pay_create_date,   ---付款生成日期
    payment_date,         --付款日期
    sign_date,         --供应商签单日期
    audit_date,        --票核日期
    invoice_sub_date,  --发票录入日期
    review_date,        --付款审核日期
    case when coalesce(finance_statement_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(finance_statement_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(finance_statement_date,'')),'') end as finance_days,  --财务对帐天数
    case when coalesce(invoice_sub_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(invoice_sub_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(invoice_sub_date,'')),'') end as invoice_sub_days,     --发票录入天数
    case when coalesce(audit_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(audit_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(audit_date,'')),'') end as audit_days,                 --票核天数
    case when coalesce(pay_create_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(pay_create_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(pay_create_date,'')),'') end as pay_create_days,       --付款生成天数
    case when coalesce(review_date,'')='' then '' 
        when coalesce(payment_date,'')='' then datediff(date_sub(current_date() ,1),coalesce(review_date,''))
        else coalesce(datediff(coalesce(payment_date,''),coalesce(review_date,'')),'')end as review_days,               --付款审核天数
    payment_status,          --  单据状态
    case when payment_status=0 then '单据未生成'
        when   payment_status=1 then '单据已生成未审核' 
        when  payment_status=2 then '单据已生成已审核'
        when  payment_status=3 then '单据已发起付款'
        when  payment_status=4 then '单据已付款成功'
        else '5' end   payment_status_name   --付款状态 0-未生成 1 已生成未审核 2 已生成已审核  3已发起付款 4 已付款成功
from csx_tmp.temp_pss_00  a 
left join 
csx_tmp.temp_pss_01 b on order_code=purchase_no and a.in_out_no=b.bill_no
left join
(select supplier_code,supplier_name,reconciliation_tag,dic_value from csx_ods.source_basic_w_a_md_supplier_info a 
 left join
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20210626' and dic_type='CONCILIATIONNFLAG' ) b on a.reconciliation_tag=b.dic_key 
 where sdt='20210626') c on a.supplier_code=c.supplier_code
 left join 
 (select shop_id,shop_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') d on a.happen_place_code=d.shop_id
  left join 
 (select shop_id,shop_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') f on a.settle_place_code=f.shop_id
 
;

-- show create table csx_tmp.temp_pss_02 ;
SET hive.execution.engine=mr; 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.ads_fr_r_d_po_reconciliation_report partition(sdt)
select *,current_timestamp(),regexp_replace(order_create_date,'-','') from csx_tmp.temp_pss_02 where order_create_date>='2021-01-01' and order_create_date<'2021-06-01';

-- show create table csx_tmp.temp_pss_02 ;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.ads_fr_r_d_po_reconciliation_report partition(sdt)
select *,current_timestamp(),order_create_date from csx_tmp.temp_pss_02 where order_create_date>='2021-01-01';

drop table csx_tmp.ads_fr_r_d_po_reconciliation_report;
CREATE TABLE `csx_tmp.ads_fr_r_d_po_reconciliation_report`(
  `purchase_order_no` string comment '采购订单号', 
  `entry_order_no` string comment '入库单号', 
  `batch_no` string comment '批次单号', 
  `company_code` string comment '公司代码', 
  `company_name` string comment '公司代码名称', 
  `receive_dc_code` string comment '入库dc', 
  `receive_dc_name` string comment '入库DC名称', 
  `settle_dc_code` string comment '结算dc', 
  `settle_dc_name` string comment '结算dc', 
  `classify_large_code` string comment '一级管理分类', 
  `classify_large_name` string comment '一级管理分类', 
  `classify_middle_code` string comment '二级管理分类', 
  `classify_middle_name` string comment '二级管理分类', 
  `classify_small_code` string comment '三级管理分类', 
  `classify_small_name` string comment '三级管理分类', 
  `order_create_date` string comment '订单创建日期', 
  `supplier_code` string comment '供应商', 
  `supplier_name` string comment '供应商', 
  `reconciliation_tag` string comment '对帐日标识', 
  `reconciliation_tag_name` string comment '对帐日标识名称', 
  `receive_date` string comment '收货日期指批次收货日期', 
  `receive_close_date` string comment '入库单关单日期', 
  `post_date` string comment '单据过帐日期', 
  `check_ticket_no` string comment'勾票单号', 
  `statement_no` string comment '对帐单号', 
  `payment_no` string comment '实际付款单号', 
  `statement_date` string comment '对帐日期', 
  `finance_statement_date` string comment '财务对帐日期', 
  `pay_create_date` string comment'付款生成日期', 
  `payment_date` string comment '付款日期', 
  `sign_date` string comment'供应商签单日期', 
  `audit_date` string comment'票核日期', 
  `invoice_sub_date` string comment'发票录入日期', 
  `review_date` string comment '付款审核日期', 
  `finance_days` string comment '财务对帐天数' , 
  `invoice_sub_days` string comment'发票录入天数', 
  `audit_days` string comment '票核天数', 
  `pay_create_days` string COMMENT'付款生成日期天数', 
  `review_days` string comment '付款审核天数,以上计算天数：如果付款日期为空，则按当前日期进行计算（搜索下载日或T-1日）；如果被减日期也为空，则计算结果为空', 
  `payment_status` int COMMENT '付款单单据状态',
  `payment_status_name` int COMMENT '付款单单据状态',
  update_time TIMESTAMP COMMENT '插入时间') comment '采购订单对帐查询报表'
   partitioned by (sdt string comment 'order_create_date采购订单日期分区')
	STORED AS parquet