-- 销售员&销售经理销售报表
-- 人员信息
drop table csx_analyse_tmp.csx_analyse_tmp_user;
create  table csx_analyse_tmp.csx_analyse_tmp_user as 
select
  a.sales_id,
  a.sales_user_name,
  a.sales_user_number,
  a.position,
  leader_position_type,
  b.dic_value as position_name,
  sales_service_manager_id,
  sales_service_manager_name,
  sales_service_manager_work_no,
    -- 主管
  sales_supervisor_user_id,
  sales_supervisor_name,
  sales_supervisor_work_no,
  -- 销售经理
  sales_manager_id,
  sales_manager_name,
  sales_manager_work_no,
  -- 城市经理
  sales_city_manager_id,
  sales_city_manager_name,
  sales_city_manager_work_no,
  -- 省区总
  district_manager_id,
  district_manager_name,
  district_manager_work_no,
  province_id,
  status,
  rank
from
(
  select
    user_id as sales_id,
    user_name as sales_user_name,
    user_number as sales_user_number,
    user_position as position,
    leader_position_type	,
    -- 服务管家
    first_value(case when leader_position_type = 'CUSTOMER_SERVICE_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as sales_service_manager_id,
    first_value(case when leader_position_type = 'CUSTOMER_SERVICE_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as        sales_service_manager_name,
    first_value(case when leader_position_type = 'CUSTOMER_SERVICE_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as sales_service_manager_work_no,
    -- 主管
    first_value(case when leader_position_type = 'SALES_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as sales_supervisor_user_id,
    first_value(case when leader_position_type = 'SALES_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as sales_supervisor_name,
    first_value(case when leader_position_type = 'SALES_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as sales_supervisor_work_no,
    -- 销售经理
    first_value(case when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as sales_manager_id,
    first_value(case when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as        sales_manager_name,
    first_value(case when leader_position_type = 'SALES_CITY_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as sales_manager_work_no,
    -- 城市经理
    first_value(case when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as sales_city_manager_id,
    first_value(case when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as sales_city_manager_name,
    first_value(case when leader_position_type = 'SALES_PROV_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as sales_city_manager_work_no,
    -- 省区总
    first_value(case when leader_position_type = 'AREA_MANAGER' then leader_user_id end, true) over(partition by user_id order by distance) as district_manager_id,
    first_value(case when leader_position_type = 'AREA_MANAGER' then leader_user_name end, true) over(partition by user_id order by distance) as district_manager_name,
    first_value(case when leader_position_type = 'AREA_MANAGER' then leader_user_number end, true) over(partition by user_id order by distance) as district_manager_work_no,
    province_id,
    status,
    row_number() over(partition by user_id order by distance desc) as rank
  from  csx_dim.csx_dim_uc_user_extend 
  where sdt = 'current'
    and user_source_business=1
)  a 
left join 
(select distinct dic_type,memo,dic_key,dic_value 
    from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df 
     where sdt=regexp_replace(date_sub(current_date(),1),'-','') 
        and dic_type='POSITION'
) b  
on a.position=dic_key
where a.rank = 1
and position is not null and position!=''
and position not in ('POSITION-25941')
;

-- 关联销售经理
create table csx_analyse_tmp.csx_analyse_tmp_sales_sale_01 as 
select substr(sdt,1,6) month,
performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,customer_code
,customer_name
,a.sales_user_number
,a.sales_user_name
,coalesce(b.sales_manager_name,'无经理') sales_manager_name
,coalesce(b.sales_manager_work_no,'') sales_manager_work_no
-- ,first_business_sign_date            --首次业务类型签约日期
-- ,is_new_sign_business                -- 是否新签业务类型
,first_business_sale_date            -- 首次业务类型销售日期
,is_new_transaction_business         -- 是否新成交业务类型
-- ,first_sign_date
-- ,is_new_sign_customer         -- 是否新签约客户(1是 0否)
-- ,first_sale_date                 -- 首次销售日期
--,is_new_transaction_customer  -- 是否新成交客户(1是 0否)
,sum(case when sdt='20231129' then sale_amt end) yesterday_sale_amt
,sum(case when sdt='20231129' then sale_cost end) yesterday_sale_cost
,sum(case when sdt='20231129' then profit end) yesterday_profit
,sum(sale_amt) sale_amt
,sum(sale_cost) sale_cost
,sum(profit) profit
 from   csx_ads.csx_ads_sale_customer_business_1d a 
 left join (select sales_user_name,	sales_user_number,	sales_manager_name,	sales_manager_work_no from csx_analyse_tmp.csx_analyse_tmp_user) b on a.sales_user_number=b.sales_user_number
 where ((sdt<='20231129' AND SDT>='20231101') or (sdt<='20231029' AND SDT>='20231001'))
 and performance_province_code='32'
 and channel_code in ('1','7','9')
 group by performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,a.sales_user_number
,a.sales_user_name
,customer_code
,customer_name
,coalesce(b.sales_manager_name,'无经理') 
,coalesce(b.sales_manager_work_no,'') 
,is_new_transaction_business
, substr(sdt,1,6)
,first_business_sale_date

;

-- drop table csx_analyse_tmp.csx_analyse_tmp_sales_sale;
create table csx_analyse_tmp.csx_analyse_tmp_sales_sale as 
with sale as (
select performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,a.sales_user_number
,a.sales_user_name
,sales_manager_name
,sales_manager_work_no
,sum(yesterday_sale_amt) yesterday_sale_amt
,sum(yesterday_sale_cost) yesterday_sale_cost
,sum(yesterday_profit) yesterday_profit
,sum(case when is_new_transaction_business=0 then yesterday_sale_amt  end ) yesterday_old_sale_amt
,sum(case when is_new_transaction_business=0 then yesterday_sale_cost end ) yesterday_old_sale_cost
,sum(case when is_new_transaction_business=0 then yesterday_profit    end ) yesterday_old_profit
,sum(case when is_new_transaction_business=1 then yesterday_sale_amt  end ) yesterday_new_sale_amt
,sum(case when is_new_transaction_business=1 then yesterday_sale_cost end ) yesterday_new_sale_cost
,sum(case when is_new_transaction_business=1 then yesterday_profit    end ) yesterday_new_profit
,sum(case when month='202311' then  sale_amt end ) sale_amt
,sum(case when month='202311' then  sale_cost end) sale_cost
,sum(case when month='202311' then  profit   end ) profit
,sum(case when month='202311' and is_new_transaction_business=0  then  sale_amt end ) old_sale_amt
,sum(case when month='202311' and is_new_transaction_business=0  then  sale_cost end) old_sale_cost
,sum(case when month='202311' and is_new_transaction_business=0  then  profit   end ) old_profit
,sum(case when month='202311' and is_new_transaction_business=1  then  sale_amt end ) new_sale_amt
,sum(case when month='202311' and is_new_transaction_business=1  then  sale_cost end) new_sale_cost
,sum(case when month='202311' and is_new_transaction_business=1  then  profit   end ) new_profit
,sum(case when month='202310' then  sale_amt end ) last_sale_amt
,sum(case when month='202310' then  sale_cost end) last_sale_cost
,sum(case when month='202310' then  profit   end ) last_profit
,sum(case when month='202310' and is_new_transaction_business=0  then  sale_amt end ) last_old_sale_amt
,sum(case when month='202310' and is_new_transaction_business=0  then  sale_cost end) last_old_sale_cost
,sum(case when month='202310' and is_new_transaction_business=0  then  profit   end ) last_old_profit
,sum(case when month='202310' and is_new_transaction_business=1  then  sale_amt end ) last_new_sale_amt
,sum(case when month='202310' and is_new_transaction_business=1  then  sale_cost end) last_new_sale_cost
,sum(case when month='202310' and is_new_transaction_business=1  then  profit   end ) last_new_profit
,count(case when month='202311'  and sale_amt is not null then  customer_code end ) customer_cn
,count(case when month='202311'  and sale_amt is not null and is_new_transaction_business=0  then  customer_code end ) old_customer_cn
,count(case when month='202311'  and sale_amt is not null and is_new_transaction_business=1 then  customer_code end )  new_customer_cn
,count(case when month='202310'  and sale_amt is not null then  customer_code end ) last_customer_cn
,count(case when month='202310'  and sale_amt is not null and is_new_transaction_business=0  then  customer_code end ) last_old_customer_cn
,count(case when month='202310'  and sale_amt is not null and is_new_transaction_business=1 then  customer_code end )  last_new_customer_cn
,0 sign_customer_cn
,0 contract_amt
from  csx_analyse_tmp.csx_analyse_tmp_sales_sale_01 a 
group by performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,a.sales_user_number
,a.sales_user_name
,sales_manager_name
,sales_manager_work_no
union all 
select 
performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,owner_user_number sales_user_number
,owner_user_name sales_user_name
,coalesce(b.sales_manager_name,'无经理')  sales_manager_name
,coalesce(b.sales_manager_work_no,'') sales_manager_work_no
,0 yesterday_sale_amt
,0 yesterday_sale_cost
,0 yesterday_profit
,0 yesterday_old_sale_amt
,0 yesterday_old_sale_cost
,0 yesterday_old_profit
,0 yesterday_new_sale_amt
,0 yesterday_new_sale_cost
,0 yesterday_new_profit
,0 sale_amt
,0 sale_cost
,0 profit
,0 old_sale_amt
,0 old_sale_cost
,0 old_profit
,0 new_sale_amt
,0 new_sale_cost
,0 new_profit
,0 last_sale_amt
,0 last_sale_cost
,0 last_profit
,0 last_old_sale_amt
,0 last_old_sale_cost
,0 last_old_profit
,0 last_new_sale_amt
,0 last_new_sale_cost
,0 last_new_profit
,0 customer_cn
,0 old_customer_cn
,0 new_customer_cn
,0 last_customer_cn
,0 last_old_customer_cn
,0 last_new_customer_cn
,COUNT(customer_code) sign_customer_cn
,sum(cast(estimate_contract_amount as decimal(26,6))) as contract_amt
 from csx_dim.csx_dim_crm_business_info a 
 left join 
 (select sales_user_name,	sales_user_number,	sales_manager_name,	sales_manager_work_no 
    from csx_analyse_tmp.csx_analyse_tmp_user) b on a.owner_user_number=b.sales_user_number
 
 where sdt='current' 
and performance_province_name = '重庆市'
	and business_sign_time >= '2023-11-01 00:00:00'
 group by performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,owner_user_number   
,owner_user_name    
,coalesce(b.sales_manager_name,'无经理') 
,coalesce(b.sales_manager_work_no,'') 
)
select 
case when sales_user_number is  null and business_type_code is null and sales_manager_work_no is   null and performance_city_name is null   then '0'       -- 省区 汇总层级 
    when sales_user_number is   null and business_type_code is not null and sales_manager_work_no is   null and performance_city_name is null   then '1'       -- 省区业务汇总层级 
    when sales_user_number is   null and business_type_code is null and sales_manager_work_no is   null   then '2'       -- 城市 汇总层级
    when sales_user_number is   null and business_type_code is not null  and sales_manager_work_no is   null   then '3'  -- 城市业务类型汇总层级
    when sales_user_number is   null and business_type_code is  null  and sales_manager_work_no is not  null   then '4'  -- 销售经理汇总层级
    when sales_user_number is   null and business_type_code is not null  and sales_manager_work_no is not  null   then '5'   -- 销售经理业务汇总层级
    when sales_user_number is  not  null and business_type_code is  null  and sales_manager_work_no is not  null   then '6'   -- 销售员汇总层级
    when sales_user_number is  not  null and business_type_code is not null  and sales_manager_work_no is not  null   then '7'   -- 销售员业务汇总层级
end level_id
,performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,a.sales_manager_name
,a.sales_manager_work_no
,a.sales_user_number
,a.sales_user_name
, (yesterday_sale_amt) yesterday_sale_amt
, (yesterday_sale_cost) yesterday_sale_cost
, (yesterday_profit) yesterday_profit
, (yesterday_old_sale_amt  ) yesterday_old_sale_amt
, (yesterday_old_sale_cost  ) yesterday_old_sale_cost
, (yesterday_old_profit  ) yesterday_old_profit
, (yesterday_new_sale_amt  ) yesterday_new_sale_amt
, (yesterday_new_sale_cost  ) yesterday_new_sale_cost
, (yesterday_new_profit  ) yesterday_new_profit
, (sale_amt) sale_amt
, (sale_cost) sale_cost
, (profit) profit
, if(coalesce(last_sale_amt,0)=0,0,sale_amt/last_sale_amt-1) as sale_growth_rate
,if(coalesce(last_profit,0)=0,0,profit/last_profit-1) as profit_growth_rate
,coalesce(profit,0)-coalesce(last_profit,0)  as profit_growth_amt
,if(coalesce(sale_amt,0)=0,0,profit/sale_amt) as profit_rate
,if(coalesce(sale_amt,0)=0,0,profit/sale_amt)-if(coalesce(last_sale_amt,0)=0,0,last_profit/last_sale_amt)  as diff_profit_rate
, (old_sale_amt) old_sale_amt
, (old_sale_cost) old_sale_cost
, (old_profit) old_profit
,if(coalesce(last_old_sale_amt,0)=0,0,old_sale_amt/last_old_sale_amt-1) as old_sale_growth_rate
,if(coalesce(last_old_profit,0)=0,0,old_profit/last_old_profit-1) as old_profit_growth_rate
,coalesce(old_profit,0)-coalesce(last_old_profit,0)  as old_profit_growth_amt
,if(coalesce(old_sale_amt,0)=0,0,old_profit/old_sale_amt) as old_profit_rate
,if(coalesce(old_sale_amt,0)=0,0,old_profit/old_sale_amt)-if(coalesce(last_old_sale_amt,0)=0,0,last_old_profit/last_old_sale_amt)  as diff_old_profit_rate
, (new_sale_amt) new_sale_amt
, (new_sale_cost) new_sale_cost
, (new_profit) new_profit
,if(coalesce(last_new_sale_amt,0)=0,0,new_sale_amt/last_new_sale_amt-1) as new_sale_growth_rate
,if(coalesce(last_new_profit,0)=0,0,new_profit/last_new_profit-1) as new_profit_growth_rate
,coalesce(new_profit,0)-coalesce(last_new_profit,0)  as new_profit_growth_amt
,if(coalesce(new_sale_amt,0)=0,0,new_profit/new_sale_amt) as new_profit_rate
,if(coalesce(new_sale_amt,0)=0,0,new_profit/new_sale_amt)-if(coalesce(last_new_sale_amt,0)=0,0,last_new_profit/last_new_sale_amt)  as diff_new_profit_rate
, (last_sale_amt) last_sale_amt
, (last_sale_cost) last_sale_cost
, (last_profit) last_profit
, (last_old_sale_amt) last_old_sale_amt
, (last_old_sale_cost) last_old_sale_cost
, (last_old_profit) last_old_profit
, (last_new_sale_amt) last_new_sale_amt
, (last_new_sale_cost) last_new_sale_cost
, (last_new_profit) last_new_profit
, (customer_cn) customer_cn
, (old_customer_cn) old_customer_cn
, (new_customer_cn)  new_customer_cn
, (last_customer_cn) last_customer_cn
, (last_old_customer_cn) last_old_customer_cn
, (last_new_customer_cn)  last_new_customer_cn
 coalesce(customer_cn,0)-coalesce(last_customer_cn,0) diff_customer_cn
, coalesce(old_customer_cn,0)-coalesce(last_old_customer_cn,0) diff_old_customer_cn
, coalesce(new_customer_cn,0)-coalesce(last_new_customer_cn,0) diff_new_customer_cn
,sign_customer_cn
,contract_amt
,0 plan_sale_amt
,0 plan_profit
,
from (
select performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
-- ,customer_code
-- ,customer_name
,a.sales_user_number
,a.sales_user_name
,sales_manager_name
,sales_manager_work_no 
,sum(yesterday_sale_amt) yesterday_sale_amt
,sum(yesterday_sale_cost) yesterday_sale_cost
,sum(yesterday_profit) yesterday_profit
,sum(yesterday_old_sale_amt  ) yesterday_old_sale_amt
,sum(yesterday_old_sale_cost  ) yesterday_old_sale_cost
,sum(yesterday_old_profit  ) yesterday_old_profit
,sum(yesterday_new_sale_amt  ) yesterday_new_sale_amt
,sum(yesterday_new_sale_cost  ) yesterday_new_sale_cost
,sum(yesterday_new_profit  ) yesterday_new_profit
,sum(sale_amt) sale_amt
,sum(sale_cost) sale_cost
,sum(profit) profit
,sum(old_sale_amt) old_sale_amt
,sum(old_sale_cost) old_sale_cost
,sum(old_profit) old_profit
,sum(new_sale_amt) new_sale_amt
,sum(new_sale_cost) new_sale_cost
,sum(new_profit) new_profit
,sum(last_sale_amt) last_sale_amt
,sum(last_sale_cost) last_sale_cost
,sum(last_profit) last_profit
,sum(last_old_sale_amt) last_old_sale_amt
,sum(last_old_sale_cost) last_old_sale_cost
,sum(last_old_profit) last_old_profit
,sum(last_new_sale_amt) last_new_sale_amt
,sum(last_new_sale_cost) last_new_sale_cost
,sum(last_new_profit) last_new_profit
,sum(customer_cn) customer_cn
,sum(old_customer_cn) old_customer_cn
,sum(new_customer_cn)  new_customer_cn
,sum(last_customer_cn) last_customer_cn
,sum(last_old_customer_cn) last_old_customer_cn
,sum(last_new_customer_cn)  last_new_customer_cn
,sum(sign_customer_cn) sign_customer_cn
,sum(contract_amt) contract_amt
 from sale a 
 group by performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,a.sales_user_number
,a.sales_user_name
,sales_manager_name
,sales_manager_work_no
grouping sets
((performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
,a.sales_user_number
,a.sales_user_name
,sales_manager_name
,sales_manager_work_no
),
(performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
-- ,business_type_code
-- ,business_type_name
,a.sales_user_number
,a.sales_user_name
,sales_manager_name
,sales_manager_work_no
),
(performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
-- ,a.sales_user_number
-- ,a.sales_user_name
,sales_manager_name
,sales_manager_work_no
),
(performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
-- ,business_type_code
-- ,business_type_name
-- ,a.sales_user_number
-- ,a.sales_user_name
,sales_manager_name
,sales_manager_work_no
),
(performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
,business_type_code
,business_type_name
-- ,a.sales_user_number
-- ,a.sales_user_name
-- ,b.sales_manager_name
-- ,b.sales_manager_work_no
),
(performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
,performance_city_code
,performance_city_name
-- ,business_type_code
-- ,business_type_name
-- ,a.sales_user_number
-- ,a.sales_user_name
-- ,b.sales_manager_name
-- ,b.sales_manager_work_no
),
(performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
-- ,performance_city_code
-- ,performance_city_name
,business_type_code
,business_type_name
-- ,a.sales_user_number
-- ,a.sales_user_name
-- ,b.sales_manager_name
-- ,b.sales_manager_work_no
),
(performance_region_code
,performance_region_name
,performance_province_code
,performance_province_name
-- ,performance_city_code
-- ,performance_city_name
-- ,business_type_code
-- ,business_type_name
-- ,a.sales_user_number
-- ,a.sales_user_name
-- ,b.sales_manager_name
-- ,b.sales_manager_work_no
)
) 
) a 
;



http://10.0.74.193:8080/webroot/decision/view/report?viewlet=%25E7%259C%258B%25E6%259D%25BF%252F%25E9%2594%2580%25E5%2594%25AE%25E5%2591%2598%25E7%259B%25AE%25E6%25A0%2587%25E8%25BE%25BE%25E6%2588%2590%25E8%25BF%259B%25E5%25BA%25A6.cpt&ref_t=design&op=view&ref_c=ad56b9bb-8222-4372-905e-374b05f5af4a
create table csx_analyse.csx_analyse_fr_sale_sales_kanban_di as (
    level_id string comment '层级',
    performance_region_code string comment '大区编码',
    performance_region_name string comment '大区名称',
    performance_province_code string comment '省区编码',
    performance_province_name string comment '省区名称',
    performance_city_code string comment '城市编码',
    performance_city_name string comment '城市名称',
    business_type_code int comment '业务类型编码',
    business_type_name string comment '业务类型名称',
    sales_manager_name string comment '销售经理',
    sales_manager_work_no string comment '销售经理工号',
    sales_user_number string comment '销售员工号',
    sales_user_name string comment '销售员',
    yesterday_sale_amt decimal(38, 6) comment '昨日销售额',
    yesterday_sale_cost decimal(38, 6) comment '昨日销售成本',
    yesterday_profit decimal(38, 6) comment '昨日毛利额',
    yesterday_old_sale_amt decimal(38, 6) comment '昨日销售额-老客',
    yesterday_old_sale_cost decimal(38, 6) comment '昨日销售成本-老客',
    yesterday_old_profit decimal(38, 6) comment '昨日毛利额-老客',
    yesterday_new_sale_amt decimal(38, 6) comment '昨日销售额-新客',
    yesterday_new_sale_cost decimal(38, 6) comment '昨日销售成本-新客',
    yesterday_new_profit decimal(38, 6) comment '昨日毛利额-新客',
    sale_amt decimal(38, 6) comment '月至今销售额',
    sale_cost decimal(38, 6) comment '月至今销售成本',
    profit decimal(38, 6) comment '月至今毛利额',
    sale_growth_rate decimal(38, 6) comment '环期销售增长率',
    profit_growth_rate decimal(38, 6) comment '毛利额增长率',
    profit_growth_amt decimal(38, 6) comment '毛利额增长额',
    profit_rate decimal(38, 6) comment '月至今毛利率',
    diff_profit_rate decimal(38, 6) comment '环比毛利率差',
    old_sale_amt decimal(38, 6) comment '月至今销售额-老客',
    old_sale_cost decimal(38, 6) comment '月至今销售成本-老客',
    old_profit decimal(38, 6) comment '月至今毛利额-老客',
    old_sale_growth_rate decimal(38, 6) comment '老客-销售额环比增长率',
    old_profit_growth_rate decimal(38, 6) comment '老客-毛利额增长率',
    old_profit_growth_amt decimal(38, 6) comment '老客-毛利额增长额',
    old_profit_rate decimal(38, 6) comment '老客-毛利率',
    diff_old_profit_rate decimal(38, 6) comment '老客-环比毛利率差',
    new_sale_amt decimal(38, 6) comment '月至今销售额-新客',
    new_sale_cost decimal(38, 6) comment '月至今销售成本-新客',
    new_profit decimal(38, 6) comment '月至今毛利额-新客',
    new_sale_growth_rate decimal(38, 6) comment '新客-环比销售额增长率',
    new_profit_growth_rate decimal(38, 6) comment '新客-环比毛利额增长率',
    new_profit_growth_amt decimal(38, 6) comment '新客-环比毛利额增长额',
    new_profit_rate decimal(38, 6) comment '新客-毛利率',
    diff_new_profit_rate decimal(38, 6) comment '新客-环比毛利率差',
    last_sale_amt decimal(38, 6) comment '环期月至今销售额',
    last_sale_cost decimal(38, 6) comment '环期月至今销售成本',
    last_profit decimal(38, 6) comment '环期月至今毛利额',
    last_old_sale_amt decimal(38, 6) comment '环期月至今销售额-老客',
    last_old_sale_cost decimal(38, 6) comment '环期月至今销售成本-老客',
    last_old_profit decimal(38, 6) comment '环期月至今毛利额-老客',
    last_new_sale_amt decimal(38, 6) comment '环期月至今销售额-新客',
    last_new_sale_cost decimal(38, 6) comment '环期月至今销售成本-新客',
    last_new_profit decimal(38, 6) comment '环期月至今毛利额-新客',
    customer_cn bigint comment '客户成交数',
    old_customer_cn bigint comment '客户成交数-老客',
    new_customer_cn bigint comment '客户成交数-新客',
    last_customer_cn bigint comment '环期客户成交数',
    last_old_customer_cn bigint comment '环期客户成交数-老客',
    last_new_customer_cn bigint comment '环期客户成交数-新客',
    diff_customer_cn bigint comment '环比客户数',
    diff_old_customer_cn bigint comment '老客-环比客户数',
    diff_new_customer_cn bigint comment '新客-环比客户数',
    sign_customer_cn bigint comment '签约客户数-取商机',
    contract_amt decimal(38, 6) comment '签约合同金额-取商机',
    plan_sale_amt DECIMAL(38, 6) COMMENT '月度销售额预算',
    plan_profit DECIMAL(38, 6) COMMENT '月度毛利额预算',
    sale_month string COMMENT '销售月份'
) COMMENT '销售员销售看板' partitioned by (sdt string COMMENT '日期分区')