select * from csx_dw.shop_m where sdt='20200309' and shop_id='9961';

select a.* from csx_ods.wshop_factory_configuration_ods a  where sdt='20200309'
left join 
(select * from dim.dim_shop where edate='9999-12-31') as b on a.rt_shop_code=b.shop_id ;

select * from csx_ods.source_crm_w_a_sys_province;

-- 门店基础表
select
	rt_shop_code,
	org_short_name,
	org_full_name,
	province as province_code,
	prefecture_city,
	county_city,
	delivery_cent_type,
	jda_tity,
	,
	shop_status
from
	csx_ods.source_basic_w_a_md_all_shop_info
where
	sdt = '20200309' ;

-- 大区省市县主数据
select a.code,a.name,b.* ,c.* from 
(SELECT * FROM csx_ods.source_basic_w_a_base_address_info  where sdt='20200309' and type=2)a 
left join 
(SELECT * FROM csx_ods.source_basic_w_a_base_address_info  where sdt='20200309' and type=3 )b on a.code=b.parent_code 

left join 
(SELECT * FROM csx_ods.source_basic_w_a_base_address_info  where sdt='20200309' and type=4)c on b.code=c.parent_code


;
--码表
SELECT * from csx_ods.source_basic_w_a_md_dic where sdt='20200309';
-- 公司代码表
select * from csx_ods.source_basic_w_a_md_company_code where sdt='20200309';
-- 采购组织表
select * from csx_ods.source_basic_w_a_base_purchase_org_info where sdt='20200309';
-- 门店表
select * from csx_ods.source_basic_w_a_md_all_shop_info where sdt='20200309';

--仓库类型
select * from csx_ods.source_basic_w_a_md_shop_configuration  where sdt='20200309';
SELECT dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','') and dic_type ='WAREHOUSETYPE';


SELECT dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','') and dic_type ='YTSATTRI';

SELECT *
   FROM csx_ods.source_basic_w_a_base_address_info
   WHERE sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and type=1;

select province_code,province_name,location_code as shop_id,shop_name,
concat(location_code,'_',shop_name) as full_shop
from csx_dw.csx_shop where sdt='current' and table_type=1;
select * from csx_dw.csx_shop where sdt='current';

SELECT * FROM csx_dw.wms_shipped_order;
