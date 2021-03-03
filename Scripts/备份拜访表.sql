create table csx_dw.p_customer_visit_backup
as 
SELECT * FROM csx_dw.p_customer_visit ;

alter table csx_dw.p_customer_visit add columns(mark_shop_code STRING COMMENT '对标门店编码');

ALTER TABLE csx_dw.p_customer_visit CHANGE column attribute_name attribute_name STRING COMMENT '企业属性' after customer_name;

show CREATE  TABLE csx_dw.p_customer_visit 

alter table csx_dw.p_customer_visit partition(sdt='20191120') add  columns(mark_shop_code STRING COMMENT '对标门店编码');

select * from csx_dw.p_customer_visit where sdt='20191117';
alter table csx_dw.p_customer_visit drop partition (sdt='20191120');


select * from csx_dw.p_customer_visit where sdt='20191120';

select * from csx_ods.yszx_customer_relation_ods where sap_cus_code ='100563' and sdt='20191120';


select
	customer_no,
	agreement_dc_code ,
	agreement_dc_name ,
	inventory_dc_code ,
	inventory_dc_name ,
	sum(cust_number)cust_number
from
	(
	select
		'1' note,
		sap_cus_code as customer_no,
		agreement_dc_code ,
		agreement_dc_name ,
		inventory_dc_code ,
		inventory_dc_name ,
		COUNT(DISTINCT sap_sub_cus_code )cust_number
	from
		csx_ods.yszx_customer_relation_new_ods
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),	1)),'-','')
	group by
		sap_cus_code,
		agreement_dc_code ,
		agreement_dc_name ,
		inventory_dc_code ,
		inventory_dc_name
union all

;


	select
		'2' note,
		sap_cus_code as customer_no,
		agreement_dc_code ,
		agreement_dc_name ,
		group_concat(inventory_dc_code) as inventory_dc_code ,
		COUNT(DISTINCT sap_sub_cus_code )cust_number
	from
		csx_ods.yszx_customer_relation_ods
	where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),		1)),		'-',		'')
		and ( inventory_dc_name not like '%安徽%'
		and inventory_dc_name not like '%北京%')
		and sap_cus_code ='100563'
	group by
		sap_cus_code,
		agreement_dc_code ,
		agreement_dc_name,group_concat(inventory_dc_code) ;

select * from csx_ods.yszx_customer_relation_ods where
		sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (),	1)),'-','')
		--and inventory_dc_name  like '%北京%'
		
		)a
group by
	customer_no,
	agreement_dc_code ,
	agreement_dc_name ,
	inventory_dc_code ,
	inventory_dc_name ;