-- yszx_customer_receipt_config，客户回单配置表


-- 客户子客户回单配置信息 


with 
-- 按主客户配置的
customer_receipt_config_cus as
( 
select
  '主客户配置' as flag,
  a.sap_cus_code,
  b.sign_company_code as sap_merchant_code,  -- 签约公司编码
  b.sign_company_name as sap_merchant_name,
  b.sub_customer_code as sap_sub_cus_code,
  b.sub_customer_name as sap_sub_cus_name,
  a.receipt_type,  -- `回单类型 -1-无回单类型 0-不回单 1-当日回单 2-周期回单 3-周回单 4-半月回单 5-月回单`,
  a.receipt_value,
  a.approve_status,  -- `审批状态 0-审批中 1-审批通过 2-审批拒绝`,
  a.receipt_proofs,
  a.update_time,
  a.create_time,
  a.create_by,
  a.update_by
from 
  (
  select *
  from csx_ods.csx_ods_b2b_mall_prod_yszx_customer_receipt_config_df
  where sap_sub_cus_code=''  -- 子客户为空，即按主客户配置的
  )a 
  left join 
  (
  	select *
  	from csx_dim.csx_dim_csms_yszx_customer_relation
  	where sdt='current'
  	and sub_customer_status=1  		-- 子客户状态 0-禁用 1-正常
  )b on a.sap_cus_code=b.customer_code 
), 

-- 按子客户配置的
customer_receipt_config_sub as
( 
select 
  '子客户配置' as flag,
  sap_cus_code,
  sap_merchant_code,  -- 签约公司编码
  sap_merchant_name,
  sap_sub_cus_code,
  sap_sub_cus_name,
  receipt_type,  -- `回单类型 -1-无回单类型 0-不回单 1-当日回单 2-周期回单 3-周回单 4-半月回单 5-月回单`,
  receipt_value,
  approve_status,  -- `审批状态 0-审批中 1-审批通过 2-审批拒绝`,
  receipt_proofs,
  update_time,
  create_time,
  create_by,
  update_by
from csx_ods.csx_ods_b2b_mall_prod_yszx_customer_receipt_config_df
where sap_sub_cus_code<>''  -- 子客户不为空，即按子客户配置的
), 

-- 均未配置的
customer_receipt_config_no as
( 
select 
  '未配置' as flag,
  a.customer_code as sap_cus_code,
  a.sign_company_code as sap_merchant_code,  -- 签约公司编码
  a.sign_company_name as sap_merchant_name,
  a.sub_customer_code as sap_sub_cus_code,
  a.sub_customer_name as sap_sub_cus_name,
  null as receipt_type,  -- `回单类型 -1-无回单类型 0-不回单 1-当日回单 2-周期回单 3-周回单 4-半月回单 5-月回单`,
  '' as receipt_value,
  null as approve_status,  -- `审批状态 0-审批中 1-审批通过 2-审批拒绝`,
  null as receipt_proofs,
  null as update_time,
  null as create_time,
  '' as create_by,
  '' as update_by
from 
(
select *  
from csx_dim.csx_dim_csms_yszx_customer_relation
where sdt='current'
and sub_customer_status=1  		-- 子客户状态 0-禁用 1-正常
)a 
left join 
(
select sap_cus_code
from csx_ods.csx_ods_b2b_mall_prod_yszx_customer_receipt_config_df
group by sap_cus_code
)b on a.customer_code=b.sap_cus_code
where b.sap_cus_code is null 
), 


-- 按主客户配置的+按子客户配置的
customer_receipt_config_all as
( 
-- 均未配置的
select * from customer_receipt_config_no
union all 
-- 按子客户配置的
select * from customer_receipt_config_sub
union all 
-- 按主客户配置的
select a.* from customer_receipt_config_cus a left join customer_receipt_config_sub b on a.sap_cus_code=b.sap_cus_code and a.sap_sub_cus_code=b.sap_sub_cus_code 
where b.sap_sub_cus_code is null
)

select
flag,
	b.performance_region_name  as `大区`,
	b.performance_province_name  as `省区`,
	b.performance_city_name  as `城市`,
sap_cus_code  as `主客户编码`,
b.customer_name  as `主客户名称`,
sap_merchant_code  as `签约公司编码`,
sap_merchant_name  as `签约公司名称`,
sap_sub_cus_code  as `子客户编码`,
regexp_replace(regexp_replace(sap_sub_cus_name,'\n',''),'\r','')  as `子客户名称`,
-- receipt_type  as `回单类型 -1-无回单类型 0-不回单 1-当日回单 2-周期回单 3-周回单 4-半月回单 5-月回单`,
case 
when receipt_type=-1 then '无回单类型'
when receipt_type=0 then '不回单'
when receipt_type=1 then '日回单'
when receipt_type=2 then '周期回单'
when receipt_type=3 then '周回单'
when receipt_type=4 then '半月回单'
when receipt_type=5 then '月回单'
else receipt_type end as `回单类型`,
receipt_value  as `回单日`,
-- approve_status  as `审批状态 0-审批中 1-审批通过 2-审批拒绝`,
case 
when approve_status=0 then '审批中'
when approve_status=1 then '审批通过'
when approve_status=2 then '审批拒绝'
else approve_status end as `审批状态`,

receipt_proofs  as `客户凭证`,
update_time  as `更新时间`,
create_time  as `创建时间`,
create_by  as `创建者`,
update_by  as `更新者`,
c.last_sale_date  as `最近销售日期`

from customer_receipt_config_all a
left join
(
select 
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	channel_name,
	customer_code,
	customer_name     --  客户名称
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and customer_type_code=4
)b on a.sap_cus_code=b.customer_code 
left join  -- 客户首单日期
(
  select 
    customer_code,first_sale_date,last_sale_date
  from csx_dws.csx_dws_crm_customer_active_di
  where sdt ='current' 
)c on c.customer_code=a.sap_cus_code
where c.last_sale_date>='20250101'
and performance_province_name='福建省'
;
		