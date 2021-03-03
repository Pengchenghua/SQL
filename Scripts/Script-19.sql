SELECT
	b.review_id,
	b.product_code,
	b.barcode,
	b.product_name,
	b.purchasing_group_code,
	b.region_code,
	a.review_time as '首次提交申请时间',
	c.update_time as '最后审核时间'
FROM
	csx_b2b_master_data.md_review_detail_history_info a 
	INNER JOIN
	csx_b2b_master_data.md_product_apply_review_view b
		ON (a.review_id = b.review_id and a.review_status = 20
	and a.review_detail_id = b.review_detail_id)
	INNER JOIN
	csx_b2b_master_data.md_review_detail_history_info c
		ON (a.review_id = c.review_id 
		and a.review_detail_id = c.review_detail_id
		and c.review_status = 40 and c.next_review_flow_node_id = 0)		
	 where a.review_id = '100100015953' 
	 	GROUP BY review_id,product_code;
	 	
	 select * from csx_b2b_master_data.md_review_detail_history_info;
	 select distinct data_month from data_sync.data_ending_inventory dei ;
	 select * from  csx_b2b_master_data.md_product_launch_reviewed_view ;
	 
	('W039','W0A7','W0M9','W0H9')
;

select sum(price*receive_qty)/10000 from csx_b2b_wms.wms_entry_order_item weoi
	join 
(
select order_code from csx_b2b_wms.wms_entry_order_header weoh  
where receive_location_code in ('W039','W0A7','W0H9','W0M9','W0T7','W0X2')
and close_time>='2020-07-01 00:00:00' and close_time<'2020-11-13 00:00:00'
and entry_type like 'P%'
and business_type !='02'
and receive_status =2
group by order_code
) a on weoi.order_code =a.order_code

 ;
select max(create_time) from data_sync.data_ending_inventory dei ;

select * from csx_basic_data.md_all_shop_info masi where table_type =1 and location_code ='W0A8';
select sum(price*shipped_qty)/10000 from csx_b2b_wms.wms_shipped_order_item wsoi 
	join 
(
select order_code from csx_b2b_wms.wms_shipped_order_header wsoh   
where shipped_location_code in ('W039','W0A7','W0H9','W0M9','W0T7','W0X2')
and send_time >='2020-07-01 00:00:00' and send_time<'2020-11-13 00:00:00'
and shipped_type = 'P02'
and business_type !='70'
and receive_status =7
group by order_code
) a on weoi.order_code =a.order_code

 ;
select * from csx_b2b_scm.scm_product_trace_header spth ;

	select * from csx_b2b_wms.wms_entry_order_item weoi ;
	select * from csx_b2b_wms.wms_entry_order_trace weot ;
	select * from csx_b2b_wms.wms_entry_order_item weoi ;

select * from csx_b2b_wms.wms_shipped_order_header wsoh 
where send_time >='';



select * from csx_b2b_wms.wms_bills_config ;
where business_type  like '%福利%'
;
select wms_order_type,wms_biz_type,biz_type from csx_b2b_accounting.accounting_transfer_config ;
	
select * from csx_basic_data.md_shop_info msi  ;

SELECT goods_id,
       goods_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       department_id,
       department_name,
    ;   
select * from csx_b2b_factory.factory_task_order_tranfer_item WHERE order_code ='WO200327004469'  ;
select * from csx_b2b_factory.factory_material_detail fmd where order_code ='WO200327004469';
select * from csx_b2b_factory.factory_setting_task_order_bom where order_code ='WO200327004469';
select * from csx_b2b_factory.factory_task_order_tranfer_item where order_code ='WO200327004469';


SELECT * FROM csx_b2b_factory.factory_mr_receive_order where task_order_code ='WO201028003119';
select * from csx_basic_data.md_supplier_info msi ;
select * from csx_basic_data.md_dic md ;
select * from csx_b2b_accounting.accounting_credential_header ach where wms_order_no ='IN201125000460';
select * from csx_b2b_wms.wms_entry_order_header weoh  where order_code ='IN201026000607';

select * from csx_b2b_crm.customer c where customer_number ='106773';
select * from data_sync.data_ending_inventory dei where reservoir_area_code ='TH02' and product_code ='318' and data_month ='202011';

select * from csx_b2b_scm.scm_order_header soh where order_code ='POW0A7201103000288';
select * from csx_b2b_wms.wms_shipped_order_item wsoi where order_code ='OW200412000056';

select * from data_sync.data_sync_sale_order;

select * from csx_b2b_wms.wms_entry_order_header weoh where order_code ='RH20122800000242';
select * from csx_b2b_wms.wms_entry_batch_detail webd where order_code ='IN200128000146';
select * from csx_b2b_wms.wms_entry_order_item weoi  where order_code ='RH20122800000242';
select * from csx_basic_data.md_supplier_info msi ;

select * from csx_basic_data.erp_employee ee ;

select * from csx_basic_data.md_category_info mci   ;
select
	a.id,
	a.order_code,
	a.link_order_code,
	source_location_code,
	source_location_name,
	target_location_code,
	target_location_name,
	source_type,
	status,
	supplier_code,
	supplier_name,
	purchase_org_code,
	purchase_org_name,
	purchase_group_code,
	purchase_group_name,
	super_class,
	a.product_code,
	a.product_name,
	product_status,
	local_purchase_flag,
	gift_qty,
	order_qty,
	shipped_qty,
	received_qty,
	price_include_tax,
	amount_include_tax,
	shipped_amount,
	received_amount,
	system_status,
	remark,
	a.create_by,
	a.create_time,
	a.update_by,
	a.update_time,
	b.link_order_code,
	b.trace_qty,
	b.trace_price,
	b.trace_amount,
	b.trace_status,
	b.order_status,
	b.`action`,
	b.create_by,
	b.create_time,
	b.update_by,
	b.update_time
from
	csx_b2b_scm.scm_order_trace_header a
join csx_b2b_scm.scm_order_trace_item b on
	a.order_code = b.order_code
	and a.product_code = b.product_code
where 	
	a.update_time >= '2020-10-01 00:00:00'
	and a.update_time <'2020-12-01 00:00:00'
	and a.order_code = 'POW0R8201015002395' 
	and a.update_time =b.update_time ;
	
select * from csx_b2b_scm.scm_product_received_dtl  spprd  where purchase_order_code ='POW0A6200613001179';

SELECT * FROM csx_b2b_wms.wms_bills_config t  ;
SELECT * FROM csx_b2b_wms.wms_entry_order_header weoh ;

select * from csx_b2b_scm.scm_order_header soh where create_time >='2020-12-20 00:00:00';
select * from csx_basic_data.md_dic md ;
select * from csx_b2b_scm.scm_order_trace_header where create_time >='2020-12-20 00:00:00';
select
	*
from
	csx_basic_data.md_supplier_info msi
where
supplier_code = '20023966'
--	and	clear_flag = 1
  ;
  
with temp_entry as 
(select
	source_type,
	super_class,
	a.order_code ,
	supplier_code ,
	supplier_name,
	receive_location_code,
	receive_location_name,
	origin_order_code,
	product_code ,
	sum(receive_qty)receive_qty,
	sum(receive_amt) as receive_amt
from 
(select
	t1.order_code ,
	supplier_code ,
	supplier_name,
	receive_location_code,
	receive_location_name,
	origin_order_code,
	t2.product_code ,
	sum(t2.receive_qty)receive_qty,
	sum(t2.receive_qty*t2.price) as receive_amt
from
	csx_b2b_wms.wms_entry_order_header t1
join 
	csx_b2b_wms.wms_entry_order_item t2 on t1.order_code =t2.order_code 
where
	-- t1.close_time >= '2020-11-01 00:00:00' and t1.close_time <'2021-01-02 00:00:00'
	 sign_date >= '2020-12-01'
	and sign_date <'2021-01-01'
	and t1.receive_status =2
	and (t1.entry_type like 'P%'or t1.entry_type like 'T%')
	group by t1.order_code ,
	supplier_code ,
	supplier_name,
	receive_location_code,
	receive_location_name,
	origin_order_code,
	t2.product_code )a
join 
(select s.order_code ,
source_type,
super_class
from csx_b2b_scm.scm_order_header as s
	where price_remedy_flag='0'
	and create_time>='2020-10-01 00:00:00'
	and s.received_order_code !=''
group by s.order_code) s on a.origin_order_code=s.order_code 
group by 
	a.order_code ,
	supplier_code ,
	supplier_name,
	receive_location_code,
	receive_location_name,
	origin_order_code,
	product_code   


) ,
temp_shipped as 
(select	
	s.order_code ,
	source_type,
	super_class,
	a.order_code ,
	supplier_code ,
	supplier_name,
	send_location_code,
	origin_order_code,
	product_code ,
	sum(shipped_qty)shipped_qty,
	sum(shipped_amt) as shipped_amt
from 
(select t3.order_code ,
	t3.supplier_code ,
	t3.supplier_name ,
	t3.send_location_code ,
	t3.origin_order_code ,
	t4.product_code ,
	sum(t4.shipped_qty)shipped_qty ,
	sum(t4.price*  t4.shipped_qty)shipped_amt
from csx_b2b_wms.wms_shipped_order_header t3
join 
csx_b2b_wms.wms_shipped_order_item  t4 on t3.order_code =t4.order_code
where t3.send_time  >= '2020-12-01 00:00:00'
	and t3.send_time  <'2021-01-01 00:00:00'
	and (t3.shipped_type like '%P%' or shipped_type like 'T%')
group by  t3.order_code ,
	t3.supplier_code ,
	t3.supplier_name ,
	t3.send_location_code ,
	t3.origin_order_code ,
	t4.product_code )a
left join 
(select  shipped_order_code,
	s.order_code ,
	source_type,
	super_class
from csx_b2b_scm.scm_order_header as s
	where 
	s.create_time >='2020-12-01 00:00:00' ) s on a.order_code=s.shipped_order_code 

)
,
(select * from csx_b2b_scm.scm_order_header soh);
	

select *
from csx_b2b_scm.scm_order_header as s
	where 
shipped_order_code ='OU201227000182'
;

select
	
	sum(t2.receive_qty)receive_qty,
	sum(t2.receive_qty*t2.price) as receive_amt
from
	csx_b2b_wms.wms_entry_order_header t1
join 
	csx_b2b_wms.wms_entry_order_item t2 on t1.order_code =t2.order_code 
where
	-- t1.close_time >= '2020-11-01 00:00:00' and t1.close_time <'2021-01-05 00:00:00'
	 sign_date >= '2020-12-01'
	and sign_date <'2021-01-01'
	
	and t1.receive_status= 2
	and (t1.entry_type like 'P%'or t1.entry_type like 'T%');

select * from data_sync.data_ending_inventory dei ;
;