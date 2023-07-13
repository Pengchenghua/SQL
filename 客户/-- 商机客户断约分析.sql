-- 商机客户断约分析
with aa as 
(select c.performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    bloc_code,
    bloc_name,
    case when c.bloc_code ='' or c.bloc_code is null then a.customer_code else c.bloc_code end new_customer_code,
    case when c.bloc_code ='' or c.bloc_code is null then a.customer_name else c.bloc_name end new_customer_name,
    a.customer_code,
    customer_name,
    sum(sale_amt) / 10000 sale_amt,
    sum(profit) / 10000 profit,
    count(distinct sdt) as sale_days,
    substr(sdt,1,6) month
from
  csx_dws.csx_dws_sale_detail_di a 
   left join 
  (select customer_code,performance_region_name,
        performance_province_name,
        performance_city_name,bloc_code,bloc_name ,
        bloc_province_name
    from    csx_dim.csx_dim_crm_customer_info 
      where sdt='current'      
        and customer_type_code=4 
       -- and( bloc_code is not null and bloc_code !='')
    ) c on a.customer_code=c.customer_code
where
  sdt >= '20220101'
  and sdt < '20230601'
  and business_type_code = 1  
group by  c.performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    sdt,
     bloc_code,
    bloc_name,
  substr(sdt,1,6),
   a.customer_code,
  customer_name,
  case when c.bloc_code ='' or c.bloc_code is null then a.customer_code else c.bloc_code end ,
    case when c.bloc_code ='' or c.bloc_code is null then a.customer_name else c.bloc_name end   
  ),
bb as (select  customer_code,
        business_type_code,
		first_business_sale_date,
		last_business_sale_date,
		business_sign_date,
		-- 至今距离天数
		datediff(to_date(date_sub(current_timestamp(),1)),to_date(from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd')))) date_diff,
		sale_business_active_days,  -- 销售业务类型活跃天数(即历史至今有销售的日期)
		sale_business_total_amt/10000 sale_business_total_amt 	-- 销售业务类型总金额
  from  csx_dws.csx_dws_crm_customer_business_active_di 
  where sdt='current' 
    and business_type_code=1
  ),
  dd as 
    (select
        bloc_code,
        a.customer_code,
        case when bloc_code ='' or bloc_code is null then a.customer_code else bloc_code end new_customer_code,
        mIN(contract_begin_date) contract_begin_date,
        max(contract_end_date) contract_end_date,
        sum(cast (estimate_contract_amount as int )) estimate_contract_amount,
        max(contract_cycle) contract_cycle,
        mIN(htqsrq) htqsrq, -- as `合同开始日期`,
        max(htzzrq) htzzrq --as `合同终止日期` ,
from
  csx_dim.csx_dim_crm_business_info a
  left join 
  (select customer_code,performance_region_name,
        performance_province_name,
        performance_city_name,bloc_code,bloc_name 
    from csx_dim.csx_dim_crm_customer_info where sdt='current'      
        and customer_type_code=4 
    ) c on a.customer_code=c.customer_code
    left join 
  (select 
    customer_no,
   mIN(htqsrq) htqsrq, -- as `合同开始日期`,
   max(htzzrq) htzzrq --as `合同终止日期` , 
  --  ,htjey
from   csx_analyse.csx_analyse_report_weaver_contract_df 
where sdt='20230607'
  -- and ywlx=1
    and ywlxmc='日配'
    group by customer_no
    ) b on a.customer_code=b.customer_no
    
where
  sdt = 'current'
  and status=1
  	and business_stage=5
	and regexp_replace(to_date(business_sign_time),'-','')>='20220101'
 -- and customer_code in ('131162', '131187')
  and business_attribute_code = 1
 -- and  c.bloc_code='BLOC-000004'
 group by bloc_code,case when bloc_code ='' or bloc_code is null then a.customer_code else bloc_code end,a.customer_code
  )

select 
    performance_region_name,
    performance_province_name,
     aa.bloc_code,
    bloc_name,
    aa.new_customer_code,
    aa.new_customer_name,
--     aa.customer_code,
--   customer_name ,
   sum(sale_days)sale_days,
   sum(sale_months) sale_months,
   max(business_sign_date)business_sign_date,
   min(first_business_sale_date) first_business_sale_date,
   max(last_business_sale_date) last_business_sale_date,
   max(date_diff) date_diff,
   max(contract_cycle) contract_cycle,
   max(sale_business_active_days) sale_business_active_days,
   sum(sale_business_total_amt) sale_business_total_amt,
   sum(sale_amt) sale_amt,
   sum(profit) profit,
   max(contract_begin_date) contract_begin_date,
   max(contract_end_date) contract_end_date,
   max(htqsrq) htqsrq,
   max(htzzrq) htzzrq,
   sum(estimate_contract_amount ) estimate_contract_amount,
   sum(sale_202301)sale_202301,
   SUM(profit_202301) profit_202301,
   sum(sale_202302)sale_202302,
   SUM(profit_202302) profit_202302, 
   sum(sale_202303)sale_202303,
   SUM(profit_202303) profit_202303
  from
 (
select 
    performance_region_name,
    performance_province_name,
  --  performance_city_name,
    aa.bloc_code,
    bloc_name,
    aa.new_customer_code,
    aa.new_customer_name,
   aa.customer_code,
   customer_name ,   
   sum(sale_days)sale_days,
   sum(sale_months)  sale_months,
   max(business_sign_date) business_sign_date,
   min(first_business_sale_date) first_business_sale_date,
   max(last_business_sale_date) last_business_sale_date,
   max(date_diff) date_diff,
   max(sale_business_active_days) sale_business_active_days,
   sum(sale_business_total_amt) sale_business_total_amt,
   sum(sale_amt) sale_amt,
   sum(profit) profit,
   sum(sale_202301 ) sale_202301,
   sum(sale_202302 ) sale_202302,
   sum(sale_202303 ) sale_202303,
   sum(profit_202301 ) profit_202301,
   sum(profit_202302 ) profit_202302,
   sum(profit_202303 ) profit_202303     
  from
  (select 
    performance_region_name,
    performance_province_name,
  --  performance_city_name,
    aa.bloc_code,
    bloc_name,
    aa.new_customer_code,
    aa.new_customer_name,
   aa.customer_code,
   customer_name ,   
   sum(sale_days)sale_days,
   count(distinct month )  sale_months,
  -- max(business_sign_date) business_sign_date,
  -- min(first_business_sale_date) first_business_sale_date,
  -- max(last_business_sale_date) last_business_sale_date,
  --  max(date_diff) date_diff,
  -- max(sale_business_active_days) sale_business_active_days,
  -- sum(sale_business_total_amt) sale_business_total_amt,
   sum(sale_amt) sale_amt,
   sum(profit) profit,
   sum(case when month='202301' then  sale_amt end ) sale_202301,
   sum(case when month='202302' then  sale_amt end ) sale_202302,
   sum(case when month='202303' then  sale_amt end ) sale_202303,
   sum(case when month='202301' then  profit end ) profit_202301,
   sum(case when month='202302' then  profit end ) profit_202302,
   sum(case when month='202303' then  profit end ) profit_202303     
  from aa 
   group by  performance_region_name,
    performance_province_name,
 --   performance_city_name,
    aa.bloc_code,
    bloc_name,
    aa.new_customer_code,
    aa.new_customer_name,
    aa.customer_code,
    customer_name 
  )aa 
  left join   bb as b on aa.customer_code=b.customer_code
   where last_business_sale_date<'20230401'
  group by  performance_region_name,
    performance_province_name,
 -- performance_city_name,
    aa.bloc_code,
    bloc_name,
    aa.new_customer_code,
    aa.new_customer_name,
    aa.customer_code,
    customer_name 
  )aa     
  left join   dd on aa.bloc_code=dd.bloc_code and aa.new_customer_code=dd.new_customer_code and aa.customer_code=dd.customer_code
 -- where aa.new_customer_code='102225'
  group by performance_region_name,
    performance_province_name,
     aa.bloc_code,
    aa.bloc_name,
    aa.new_customer_code,
    aa.new_customer_name
    -- aa.customer_code,
    -- aa.customer_name
--  )aa
 
