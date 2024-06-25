/*
月份 大区 省区 城市 工号 姓名 岗位 销售额 月度毛利额 月度基准毛利额目标 "毛利额达成率
月度毛利额/月度基准毛利额目标" 新客户履约额 应收金额 逾期金额 "逾期率
（取账款逾期率，以逾期率做排名）" "投标保证金逾期
（作为减分项）" "商机质量
（25%以上商机&商机金额（取月中+月底平均值）" 
*/
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_sale_performance;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_performance as 
-- 销售
with sale as(
    select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        if(substr(sdt, 1, 6) = substr(first_business_sale_date, 1, 6),1,0) as is_new_customer
    from csx_dws.csx_dws_sale_detail_di a
        left join (
            select customer_code,
                business_type_code,
                first_business_sale_date
            from csx_dws.csx_dws_crm_customer_business_active_di
            where sdt = '20240617'
        ) b on a.customer_code = b.customer_code
        and a.business_type_code = b.business_type_code
    where sdt >= '20240301'
        and sdt <= '20240531' --   
        and a.business_type_code != 4
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        a.business_type_code,
        if(substr(sdt, 1, 6) = substr(first_business_sale_date, 1, 6),1,0)
),
-- 销售基准毛利目标
person_target as 
(select smt,
    work_no,
    sales_name,
    region_name,
    province_name,
    city_group_code,
    city_group_name,
    sale_amt,	
    profit,
    profit_basic,	
    profit_target_rate	
from   csx_analyse.csx_analyse_tc_person_profit_target_rate 
    where smt>='202403'
),
-- 逾期率
over_rate as 
(select substr(sdt,1,6) as sale_month,
    region_name,
    province_name,
    city_group_name,
    customer_code, 
    customer_attribute_code,
    sales_employee_code,
    sales_employee_name,
    sum(overdue_amount) as overdue_amount,
    sum(receivable_amount) as receivable_amount
from csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
    where sdt in ('20240331','20240430','20240531')
    
    group by substr(sdt,1,6),
    customer_code, 
    region_name,
    province_name,
    customer_attribute_code,
    sales_employee_code,
    sales_employee_name,
    city_group_name
     
),
-- 商机质量
sj_info as 
(select  
    substr(regexp_replace(to_date(business_sign_time),'-',''),1,6) sale_month,
    business_number,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    business_sign_time,
    to_date(business_sign_time) as business_sign_date,
    business_attribute_code,
    customer_id,
    customer_code,
    owner_user_number,
    owner_user_name,
    estimate_contract_amount
from csx_dim.csx_dim_crm_business_info
    where sdt in ('20240531')
    and status=1
    and business_stage>=2
    and to_date(business_sign_time) >= '2024-03-01'
    and to_date(business_sign_time) <= '2024-05-31'
    and business_attribute_code in (1,2,5) -- 商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
),
user_info as 
  (select
    user_id,
    user_number,
    user_name,
    user_position,
    name user_position_name,
    city_name,
    province_name
  from
    csx_dim.csx_dim_uc_user a 
    left join 
(select dic_key as code,dic_value as name
       from csx_dim.csx_dim_csx_basic_data_md_dic
       where sdt='current'
       and dic_type = 'POSITION')  b on a.user_position=b.code
  where sdt='20240617'
    and delete_flag = '0'
)

select sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    sales_user_number,
    sales_user_name,
    b.user_position_name,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit,
    sum(profit_basic) profit_basic,
    sum(new_cust_sale) new_cust_sale,
    sum(receivable_amount) receivable_amount,
    sum(overdue_amount) overdue_amount,
    sum(business_number_count) business_number_count,
    sum(estimate_contract_amount) estimate_contract_amount
from (
select sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    sales_user_number,
    sales_user_name,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit,
    sum(profit_basic) profit_basic,
    sum(if(is_new_customer=1,a.sale_amt,0)) new_cust_sale,
    0 as business_number_count,
    0 as estimate_contract_amount,
    0 as overdue_amount,
    0 as receivable_amount
from sale a
left  join person_target as b on a.sale_month = b.smt and a.sales_user_number = b.work_no
group by sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    sales_user_number,
    sales_user_name
union all 
select sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    owner_user_number,
    owner_user_name,
    0 as sale_amt,
    0 as profit,
    0 as profit_basic,
    0 as new_cust_sale,
    count(business_number) as business_number_count,
    sum(estimate_contract_amount) as estimate_contract_amount,
    0 as overdue_amount,
    0 as receivable_amount
 from sj_info
 group by 
    sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    owner_user_number,
    owner_user_name
union all 
select sale_month,
    region_name,
    province_name,
    city_group_name,
    sales_employee_code,
    sales_employee_name,
    0 as sale_amt,
    0 as profit,
    0 as profit_basic,
    0 as new_cust_sale,
    0 business_number_count,
    0 estimate_contract_amount,
    (overdue_amount) as overdue_amount,
    (receivable_amount) as receivable_amount
from over_rate d 
where receivable_amount>0
)a 
left join user_info b on a.sales_user_number=b.user_number
-- where performance_city_name='福州市'
group by  a.sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    sales_user_number,
    sales_user_name,
    b.user_position_name
    

    ;


-- 计算得分
with middle_jg as 
(select
  sale_month,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  user_position_name,
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
  business_number_count,
  estimate_contract_amount,
  dense_rank()over(partition by performance_province_name,sale_month order by sale_amt desc ) as sale_rnk,
  dense_rank()over(partition by performance_province_name,sale_month order by profit_basic_rate desc ) as profit_rnk,
  dense_rank()over(partition by performance_province_name,sale_month order by new_cust_sale desc ) new_rnk,
  dense_rank()over(partition by performance_province_name,sale_month order by over_rate asc ) as over_rnk,
  dense_rank()over(partition by performance_province_name,sale_month order by business_number_count desc ) business_rnk,
  dense_rank()over(partition by performance_province_name,sale_month order by estimate_contract_amount desc ) estimate_contract_rnk
from
  (
  select
  sale_month,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  user_position_name,
  sale_amt,
  profit,
  profit_basic,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  business_number_count,
  estimate_contract_amount,
  if(coalesce(profit_basic,0)=0,0,profit/profit_basic) as profit_basic_rate,
  if(overdue_amount=0,0,overdue_amount/receivable_amount) as over_rate
from
 csx_analyse_tmp.csx_analyse_tmp_hr_sale_performance
where 1=1 
 and  performance_province_name not like '平台%' 
 and  performance_region_name <>''
 and user_position_name='销售员'
--   and sale_month='202405'
  ) a 
  )
  select    a.sale_month,
  a.performance_region_name,
  a.performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  user_position_name,
  rank()over(partition by performance_province_name,sale_month order by (seq_score+ profit_score+ new_rnk_score+ over_rnk_score+   business_rnk_score+  estimate_contract_rnk_score) desc ) total_rnk,
  (seq_score+ profit_score+ new_rnk_score+ over_rnk_score+   business_rnk_score+  estimate_contract_rnk_score) total_score,
  -- 销售25、毛利25、新客20、逾期20、商机5
   seq_score,
   profit_score,
    new_rnk_score,
   over_rnk_score,
   business_rnk_score,
   estimate_contract_rnk_score,
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
  business_number_count,
  estimate_contract_amount,
  sale_rnk,
  profit_rnk,
  new_rnk,
  over_rnk,
  business_rnk,
  estimate_contract_rnk
  
from(
  select  a.sale_month,
  a.performance_region_name,
  a.performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  user_position_name,
  -- 销售25、毛利25、新客20、逾期20、商机5
  CASE
    WHEN sale_rnk = 1 THEN 25
    when sale_rnk=max_sale_rnk then 0 
    ELSE 25 - (sale_rnk - 1) *(25/(max_sale_rnk-1) )
  END  AS seq_score,
   CASE
    WHEN profit_rnk = 1 THEN 25
    when profit_rnk=max_profit_rnk then 0 
    ELSE 25 - (profit_rnk - 1) *(25/(max_profit_rnk-1) )
  END  AS profit_score,
  CASE
    WHEN new_rnk = 1 THEN 20
    when new_rnk=max_new_rnk then 0 
    ELSE 20 - (new_rnk - 1) *(20 /(max_new_rnk-1) )
  END  AS new_rnk_score,
  CASE
    WHEN over_rnk = 1 THEN 20
    when over_rnk=max_over_rnk then 0 
    ELSE 20 - (over_rnk - 1) *(20 / (max_over_rnk-1) )
  END  AS over_rnk_score,
  CASE
    WHEN business_rnk = 1 THEN 5
    when business_rnk=max_business_rnk then 0 
    ELSE 5 - (business_rnk - 1) *(5/(max_business_rnk-1) )
  END  AS business_rnk_score,
   CASE
    WHEN estimate_contract_rnk = 1 THEN 5
    when estimate_contract_rnk=max_estimate_contract_rnk then 0 
    ELSE 5 - (estimate_contract_rnk - 1) *(5/(max_estimate_contract_rnk-1) )
  END  AS estimate_contract_rnk_score,
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
  business_number_count,
  estimate_contract_amount,
  sale_rnk,
  profit_rnk,
  new_rnk,
  over_rnk,
  business_rnk,
  estimate_contract_rnk  
from middle_jg a 
left join 
(select sale_month,
  performance_region_name,
  performance_province_name,
  count(distinct sales_user_number ) sale_cnt
  from middle_jg
  group by sale_month,
  performance_region_name,
  performance_province_name)b on a.sale_month=b.sale_month and a.performance_province_name=b.performance_province_name
 left join 
(select sale_month,
  performance_region_name,
  performance_province_name,
  max(sale_rnk) max_sale_rnk,
  max(profit_rnk)max_profit_rnk,
  max(new_rnk)max_new_rnk,
  max(over_rnk) max_over_rnk,
  max(business_rnk) max_business_rnk,
  max(estimate_contract_rnk) max_estimate_contract_rnk
  from middle_jg
  group by sale_month,
  performance_region_name,
  performance_province_name)c on a.sale_month=c.sale_month and a.performance_province_name=c.performance_province_name
) a 