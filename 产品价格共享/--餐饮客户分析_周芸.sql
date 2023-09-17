-- 每个客户销售额，毛利，净利，开始配送时间，配送频率，级别，回款情况
with aa as 
(select 
-- substr(sdt,1,6) mon ,
 a.performance_region_code	 	,
 a.performance_region_name	 	,
 a.performance_province_code	 	,
 a.performance_province_name	 	,
 a.performance_city_code	 	,
 a.performance_city_name	 	,   
 a.customer_code,
 a.customer_name,
 first_category_name,
 second_category_code	,
 second_category_name	,
 third_category_code	,
 third_category_name	,
  sale_mons,
 sale_amt,
  profit,
 first_sign_date,
 first_sale_date			,
 last_sale_date ,
 first_business_sale_date,
  business_type_list
from
(select 
-- substr(sdt,1,6) mon ,
 a.performance_region_code	 	,
 a.performance_region_name	 	,
 a.performance_province_code	 	,
 a.performance_province_name	 	,
 a.performance_city_code	 	,
 a.performance_city_name	 	,   
 a.customer_code,
 a.customer_name,
 first_category_name,
 second_category_code	,
 second_category_name	,
 third_category_code	,
 third_category_name	,
 count(distinct substr(sdt,1,6)) as sale_mons,
 sum(sale_amt) as sale_amt,
 sum(profit) as profit,
 first_sign_date,
 first_sale_date			,
 last_sale_date ,
 -- first_business_sale_date,
 concat_ws(',',collect_set(business_type_name)) business_type_list
from csx_dws.csx_dws_sale_detail_di a 
left join (select shop_code,shop_low_profit_flag from csx_dim.csx_dim_shop where sdt='current') d on a.inventory_dc_code=d.shop_code
left join 
(SELECT 
    customer_code			,
   -- business_type_code			,
   -- channel_code			,
    performance_province_code	,
    sign_date			,   -- 最新签约时间
    first_sign_date			,
    first_sale_date			,
    last_sale_date			
 from   csx_dws.csx_dws_crm_customer_active_di
     where sdt='current') b on a.performance_province_code=b.performance_province_code and a.customer_code=b.customer_code
 where sdt>='20230101' and sdt<='20230831'
 --and a.business_type_code=1 
 --and shop_low_profit_flag=0
 and a.channel_code in ('1','7','9')
 and first_category_name like '%餐饮%'
-- and a.customer_code='122891'
 
group by
-- substr(sdt,1,6) ,
 a.performance_region_code	 	,
 a.performance_region_name	 	,
 a.performance_province_code	 	,
 a.performance_province_name	 	,
 a.performance_city_code	 	,
 a.performance_city_name	 	,   
 a.customer_code,
 a.customer_name,
 second_category_code	,
 second_category_name	,
 third_category_code	,
 third_category_name ,
  first_sign_date,
 first_sale_date			,
 last_sale_date ,
 first_category_name
 -- ,first_business_sale_date
 )a
 left join 
 (
SELECT 
customer_code			,
business_type_code			,
channel_code			,
performance_province_code	,
business_sign_date			,
first_business_sign_date			,
first_business_sale_date			,
last_business_sale_date			
 from csx_dws.csx_dws_crm_customer_business_active_di
 where sdt='current'
 and  business_type_code=1
 ) c on a.customer_code=c.customer_code  and a.performance_province_code=c.performance_province_code

 ),
 bb as (select substr(sdt,1,6) mon,performance_province_code,customer_code,concat_ws(',',collect_set(customer_level)) customer_level_list,concat_ws(',',collect_set(customer_level_name)) 	customer_level_name,concat_ws(',',collect_set(credit_code)) credit_code_list,
    concat_ws(',',collect_set(account_period_name)) account_period_list,
    sum(receivable_amount) receivable_amount,
    sum(overdue_amount)overdue_amount,
    max(max_overdue_day)max_overdue_day
 from csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
    where sdt='20230904'
   -- and receivable_amount!=0
    group by  performance_province_code,customer_code,substr(sdt,1,6)
 ),
 cc as (select province_code,customer_no,customer_large_level from csx_analyse.csx_analyse_report_sale_customer_level_mf where month='202308' and tag=1 ) 
 select a.*,customer_level_list,credit_code_list,customer_level_name,account_period_list,receivable_amount,overdue_amount,max_overdue_day,customer_large_level,account_period_code,account_period_name,account_period_value
 from aa a 
 left join bb b on a.customer_code=b.customer_code and a.performance_province_code=b.performance_province_code 
 left join cc c on a.customer_code=c.customer_no and a.performance_province_code=c.province_code
 left join 
 (select
  customer_code customer_no,
  performance_province_code province_code,
  channel_code,
  --  渠道编码
  concat_ws(',', collect_set(account_period_code)) account_period_code,
  --  账期类型
  concat_ws(',', collect_set(account_period_name)) account_period_name,
  --  账期名称
  concat_ws(
    ',',
    collect_set(cast(account_period_value as string))
  ) account_period_value --  帐期天数
from
  csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
 -- and customer_code = '126197'
group by
  customer_code,
  performance_province_code,
  channel_code) d on a.customer_code=d.customer_no and a.performance_province_code=d.province_code