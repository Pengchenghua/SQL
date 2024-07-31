-- 销售经理&高级&总监绩效

-- 1、先计算个人销售数量
-- 2、团队销售

-- 销售经理角色信息
-- drop table csx_analyse_tmp.csx_analyse_tmp_user_info ;
-- create table csx_analyse_tmp.csx_analyse_tmp_user_info as 
-- select user_id,
--     user_number,
--     user_phone,
--     user_email,
--     user_name,
--     user_position,
--     sale_positon_name,
--     leader_user_id,
--     leader_user_number,
--     leader_user_name,
--     leader_user_position,
--     name as position_name
-- from (
--         select *,
--             row_number() over(partition by user_id order by distance desc  ) ranks
--         from csx_dim.csx_dim_uc_user_extend a
--         where sdt = 'current'
--           --  and distance in(1, 0)
--             and status = 0
--     ) a
--     left join (
--         select dic_key as code,
--             dic_value as name
--         from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
--         where sdt = '20240702'
--             and dic_type = 'POSITION'
--     ) c on a.leader_user_position = c.code
--      left join (
--         select dic_key as code,
--             dic_value as sale_positon_name
--         from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
--         where sdt = '20240702'
--             and dic_type = 'POSITION'
--     ) d on a.user_position = d.code
-- where c.name in ('销售经理', '高级销售经理', '销售总监')
--     ;

  -- drop table csx_analyse_tmp.csx_analyse_tmp_hr_user_info;
create table    csx_analyse_tmp.csx_analyse_tmp_hr_user_info as 
SELECT
    sales_user_id,
    sales_user_name,
    sales_user_number,
    sales_user_position,
    c.name sales_user_position_name,
    sales_manager_user_id,
    sales_manager_user_name,
    sales_manager_user_number,
    sales_high_manager_user_id,
    sales_high_manager_user_name,
    sales_high_manager_user_number,
    sales_director_user_id,
    sales_director_user_name,
    sales_director_user_number
  FROM
  (
    SELECT
      user_id AS sales_user_id,
      user_name AS sales_user_name,
      user_number AS sales_user_number,
      user_position AS sales_user_position,
       -- 销售经理
      first_value(CASE WHEN leader_user_position = 'POSITION-25844' THEN leader_user_id END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_manager_user_id,
      first_value(CASE WHEN leader_user_position = 'POSITION-25844' THEN leader_user_name END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_manager_user_name,
      first_value(CASE WHEN leader_user_position = 'POSITION-25844' THEN leader_user_number END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_manager_user_number,
      -- 城市经理
      first_value(CASE WHEN leader_user_position = 'POSITION-26623' THEN leader_user_id END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_high_manager_user_id,
      first_value(CASE WHEN leader_user_position = 'POSITION-26623' THEN leader_user_name END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_high_manager_user_name,
      first_value(CASE WHEN leader_user_position = 'POSITION-26623' THEN leader_user_number END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_high_manager_user_number,
      -- 省区总
      first_value(CASE WHEN leader_user_position = 'POSITION-26064' THEN leader_user_id END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_director_user_id,
      first_value(CASE WHEN leader_user_position = 'POSITION-26064' THEN leader_user_name END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_director_user_name,
      first_value(CASE WHEN leader_user_position = 'POSITION-26064' THEN leader_user_number END, true) over(PARTITION BY user_id ORDER BY distance) AS sales_director_user_number,
      row_number() over(PARTITION BY user_id ORDER BY distance DESC) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt = 'current'
    and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
  ) tmp  
  left join (
        select dic_key as code,
            dic_value as name
        from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
        where sdt = '20240702'
            and dic_type = 'POSITION'
    ) c on tmp.sales_user_position = c.code
  WHERE tmp.rank = 1
  ;
  


-- 统计销售数据
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_performance;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_performance as 
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
        left join
        (
            select customer_code,
                business_type_code,
                first_business_sale_date
            from csx_dws.csx_dws_crm_customer_business_active_di
            where sdt = '20240630'
        ) b on a.customer_code = b.customer_code
        and a.business_type_code = b.business_type_code
    where sdt >= '20240401'
        and sdt <= '20240630' --   
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
    where sdt in ('20240430','20240531','20240630')
    
    group by substr(sdt,1,6),
    customer_code, 
    region_name,
    province_name,
    customer_attribute_code,
    sales_employee_code,
    sales_employee_name,
    city_group_name
     
)
select sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    a.sales_user_number,
    a.sales_user_name,
    b.sales_user_position,
    b.sales_user_position_name,
    b.sales_manager_user_id,
    b.sales_manager_user_name,
    b.sales_manager_user_number,
    b.sales_high_manager_user_id,
    b.sales_high_manager_user_name,
    b.sales_high_manager_user_number,
    b.sales_director_user_id,
    b.sales_director_user_name,
    b.sales_director_user_number,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit,
    sum(profit_basic) profit_basic,
    sum(new_cust_sale) new_cust_sale,
    sum(receivable_amount) receivable_amount,
    sum(overdue_amount) overdue_amount
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
    region_name,
    province_name,
    city_group_name,
    sales_employee_code,
    sales_employee_name,
    0 as sale_amt,
    0 as profit,
    0 as profit_basic,
    0 as new_cust_sale,
    (overdue_amount) as overdue_amount,
    (receivable_amount) as receivable_amount
from over_rate d 
where receivable_amount>0
)a 
left join csx_analyse_tmp.csx_analyse_tmp_hr_user_info  b on a.sales_user_number=b.sales_user_number
-- where performance_city_name='福州市'
group by  a.sale_month,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    a.sales_user_number,
    a.sales_user_name,
    b.sales_user_position,
    b.sales_user_position_name,
    b.sales_manager_user_id,
    b.sales_manager_user_name,
    b.sales_manager_user_number,
    b.sales_high_manager_user_id,
    b.sales_high_manager_user_name,
    b.sales_high_manager_user_number,
    b.sales_director_user_id,
    b.sales_director_user_name,
    b.sales_director_user_number
    
;


-- 计算个人得分
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_person ; 
create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_person as 
with middle_jg as 
(select
  sale_month,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  sales_user_position_name,
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
 
  dense_rank()over(partition by sale_month order by sale_amt desc ) as sale_rnk,
  dense_rank()over(partition by sale_month order by profit_basic_rate desc ) as profit_rnk,
  dense_rank()over(partition by sale_month order by new_cust_sale desc ) new_rnk,
  dense_rank()over(partition by sale_month order by over_rate asc ) as over_rnk
from
  (
  select
  sale_month,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  sales_user_position_name,
  sale_amt,
  profit,
  profit_basic,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  if(coalesce(profit_basic,0)=0,0,profit/profit_basic) as profit_basic_rate,
  if(overdue_amount=0,0,overdue_amount/receivable_amount) as over_rate
from
   csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_performance
where
  performance_province_name not like '%平台%'
  and sales_user_position in (
    'POSITION-26064',
    'POSITION-26623',
    'POSITION-25844'
  )
--   and sale_month='202405'
  ) a 
  )
  select    a.sale_month,
  a.performance_region_name,
  a.performance_province_name,
--   performance_city_name,
  sales_user_number,
  sales_user_name,
  sales_user_position_name,
  rank()over(partition by sale_month order by (sale_score+ profit_score+ new_rnk_score+ over_rnk_score) desc ) total_rnk,
  (sale_score+ profit_score+ new_rnk_score+ over_rnk_score) total_score,
  -- 销售25、毛利25、新客20、逾期20、商机5
  sale_score,
  profit_score,
  new_rnk_score,
  over_rnk_score,
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
  sale_rnk,
  profit_rnk,
  new_rnk,
  over_rnk
  
from(
  select  a.sale_month,
  a.performance_region_name,
  a.performance_province_name,
--   performance_city_name,
  sales_user_number,
  sales_user_name,
  sales_user_position_name,
  -- 销售15、毛利15、新客10、逾期10
  CASE
    WHEN sale_rnk = 1 THEN 15
    when sale_rnk=max_sale_rnk then 0 
    ELSE 15 - (sale_rnk - 1) *(15/(max_sale_rnk-1) )
  END  AS sale_score,
   CASE
    WHEN profit_rnk = 1 THEN 15
    when profit_rnk=max_profit_rnk then 0 
    ELSE 15 - (profit_rnk - 1) *(15/(max_profit_rnk-1) )
  END  AS profit_score,
  CASE
    WHEN new_rnk = 1 THEN 10
    when new_rnk=max_new_rnk then 0 
    ELSE 10 - (new_rnk - 1) *(10 /(max_new_rnk-1) )
  END  AS new_rnk_score,
  CASE
    WHEN over_rnk = 1 THEN 10
    when over_rnk=max_over_rnk then 0 
    ELSE 10 - (over_rnk - 1) *(10 / (max_over_rnk-1) )
  END  AS over_rnk_score,
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
  sale_rnk,
  profit_rnk,
  new_rnk,
  over_rnk
from middle_jg a 
left join 
(select sale_month,
   max(sale_rnk) max_sale_rnk,
  max(profit_rnk)max_profit_rnk,
  max(new_rnk)max_new_rnk,
  max(over_rnk) max_over_rnk
  from middle_jg
  group by sale_month)c on a.sale_month=c.sale_month
) a 

;
 

-- 计算团队得分
create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_team as 
with middle_jg as 
(select
  sale_month,
  performance_region_name,
  performance_province_name,
--   performance_city_name,
  sales_user_number,
  sales_user_name,
  sales_user_position_name,
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
 
  dense_rank()over(partition by sale_month order by sale_amt desc ) as sale_rnk,
  dense_rank()over(partition by sale_month order by profit_basic_rate desc ) as profit_rnk,
  dense_rank()over(partition by sale_month order by new_cust_sale desc ) new_rnk,
  dense_rank()over(partition by sale_month order by over_rate asc ) as over_rnk
from
(
  -- 销售经理
  select sale_month,
    performance_region_name,
    performance_province_name,
    -- performance_city_name,
    sales_user_name,
    sales_user_number,
    sales_user_position_name,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit,
    sum(profit_basic) profit_basic,
    sum(new_cust_sale) new_cust_sale,
    sum(receivable_amount) receivable_amount,
    sum(overdue_amount) overdue_amount,
    if(sum(coalesce(profit_basic,0))=0,0,sum(profit)/sum(profit_basic)) as profit_basic_rate,
    if(sum(overdue_amount)=0,0,sum(overdue_amount)/sum(receivable_amount)) as over_rate
from
  (
  -- 销售经理
  select sale_month,
    performance_region_name,
    performance_province_name,
    -- performance_city_name,
    sales_manager_user_name as sales_user_name,
    sales_manager_user_number as sales_user_number,
    '销售经理' as sales_user_position_name,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit,
    sum(profit_basic) profit_basic,
    sum(new_cust_sale) new_cust_sale,
    sum(receivable_amount) receivable_amount,
    sum(overdue_amount) overdue_amount,
    if(sum(coalesce(profit_basic,0))=0,0,sum(profit)/sum(profit_basic)) as profit_basic_rate,
    if(sum(overdue_amount)=0,0,sum(overdue_amount)/sum(receivable_amount)) as over_rate
from
   csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_performance a
where
  performance_province_name not like '%平台%'
  and sales_manager_user_id is not null 
 group by sale_month,
    performance_region_name,
    performance_province_name,
    -- performance_city_name,
    sales_manager_user_name,
    sales_manager_user_number
union all 
-- 销售高级经理  
  select sale_month,
    performance_region_name,
    performance_province_name,
    -- performance_city_name,
    sales_high_manager_user_name,
    sales_high_manager_user_number,
    '高级销售经理' as sales_user_position_name,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit,
    sum(profit_basic) profit_basic,
    sum(new_cust_sale) new_cust_sale,
    sum(receivable_amount) receivable_amount,
    sum(overdue_amount) overdue_amount,
    if(sum(coalesce(profit_basic,0))=0,0,sum(profit)/sum(profit_basic)) as profit_basic_rate,
    if(sum(overdue_amount)=0,0,sum(overdue_amount)/sum(receivable_amount)) as over_rate
from
   csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_performance a
where
  performance_province_name not like '%平台%'
  and sales_high_manager_user_id is not null 
--   and sale_month='202405'
group by  sale_month,
    performance_region_name,
    performance_province_name,
    -- performance_city_name,
    sales_high_manager_user_name,
    sales_high_manager_user_number
union all 
-- 销售总监
  select sale_month,
    performance_region_name,
    performance_province_name,
    -- performance_city_name,
    sales_director_user_name,
    sales_director_user_number,
    '销售总监' as sales_user_position_name,
    sum(a.sale_amt) sale_amt,
    sum(a.profit) profit,
    sum(profit_basic) profit_basic,
    sum(new_cust_sale) new_cust_sale,
    sum(receivable_amount) receivable_amount,
    sum(overdue_amount) overdue_amount,
    if(sum(coalesce(profit_basic,0))=0,0,sum(profit)/sum(profit_basic)) as profit_basic_rate,
    if(sum(overdue_amount)=0,0,sum(overdue_amount)/sum(receivable_amount)) as over_rate
from
   csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_performance a
where
  performance_province_name not like '%平台%'
  and sales_director_user_id is not null 
  group by sale_month,
    performance_region_name,
    performance_province_name,
    -- performance_city_name,
    sales_director_user_name,
    sales_director_user_number
  ) a
  group by 
    sale_month,
    performance_region_name,
    performance_province_name,
    -- performance_city_name,
    sales_user_name,
    sales_user_number,
    sales_user_position_name
  ) a 
  )
  select    a.sale_month,
  a.performance_region_name,
  a.performance_province_name,
--   performance_city_name,
  sales_user_number,
  sales_user_name,
  sales_user_position_name,
  rank()over(partition by sale_month order by (sale_score+ profit_score+ new_rnk_score+ over_rnk_score) desc ) total_rnk,
  (sale_score+ profit_score+ new_rnk_score+ over_rnk_score) total_score,
  -- 销售25、毛利25、新客20、逾期20、商机5
  sale_score,
  profit_score,
  new_rnk_score,
  over_rnk_score,
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
  sale_rnk,
  profit_rnk,
  new_rnk,
  over_rnk
  
from(
  select  a.sale_month,
  a.performance_region_name,
  a.performance_province_name,
--   performance_city_name,
  sales_user_number,
  sales_user_name,
  sales_user_position_name,
  -- 销售25、毛利25、新客20、逾期20、商机5
  CASE
    WHEN sale_rnk = 1 THEN 25
    when sale_rnk=max_sale_rnk then 0 
    ELSE 25 - (sale_rnk - 1) *(25/(max_sale_rnk-1) )
  END  AS sale_score,
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
  sale_amt,
  profit,
  profit_basic,
  profit_basic_rate,
  new_cust_sale,
  receivable_amount,
  overdue_amount,
  over_rate,
  sale_rnk,
  profit_rnk,
  new_rnk,
  over_rnk
from middle_jg a 
left join 
(select sale_month,
   max(sale_rnk) max_sale_rnk,
  max(profit_rnk)max_profit_rnk,
  max(new_rnk)max_new_rnk,
  max(over_rnk) max_over_rnk
  from middle_jg
  group by sale_month)c on a.sale_month=c.sale_month
) a 
;

-- 合并个人与团队

select a.sale_month,
  a.performance_region_name,
  a.performance_province_name,
--   performance_city_name,
  a.sales_user_number,
  a.sales_user_name,
  a.sales_user_position_name,
  dense_rank()over(partition by a.sale_month order by coalesce(person_total_score,0)+coalesce(a.total_score,0)  desc ) as all_rnk,
  coalesce(person_total_score,0)+coalesce(a.total_score,0) as all_total_score,
  coalesce(person_total_rnk,0) as person_total_rnk,
  coalesce(person_total_score,0) as person_total_score,
  coalesce(person_sale_amt,0) as person_sale_amt, 
  coalesce(person_sale_rnk,0) as person_sale_rnk, 
  coalesce(person_sale_score,0) as person_sale_score,
  a.sale_amt as team_sale_amt,
  a.sale_score as team_sale_score,
  a.sale_rnk as team_sale_rnk,
  coalesce( person_profit, 0) as person_profit,
  coalesce( person_profit_rnk, 0) as person_profit_rnk,
  coalesce( person_profit_score, 0) as person_profit_score,
  profit as team_profit,
  profit_rnk as team_profit_rnk,
  profit_score as team_profit_score,
  person_new_cust_sale,
  person_new_rnk,
  person_new_rnk_score,
  new_cust_sale team_new_cust_sale,
  new_rnk team_new_rnk,
  new_rnk_score as team_new_rnk_score,
  person_over_rate,
  person_overdue_amount,
  person_over_rnk,
  person_over_rnk_score,
  overdue_amount as team_overdue_amount,
  over_rate as team_over_rate,
  over_rnk as team_over_rnk,
  over_rnk_score as team_over_rnk_score
  
  from csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_team a 
  left join 
  (
select  a.sale_month,
  a.performance_region_name,
  a.performance_province_name,
--   performance_city_name,
  sales_user_number,
  sales_user_name,
  sales_user_position_name,
  total_rnk as person_total_rnk,
  total_score as person_total_score,
  -- 销售25、毛利25、新客20、逾期20、商机5
  sale_score as person_sale_score,
  profit_score as person_profit_score,
  new_rnk_score as person_new_rnk_score,
  over_rnk_score as person_over_rnk_score,
  sale_amt as person_sale_amt,
  profit as person_profit,
  profit_basic as person_profit_basic,
  profit_basic_rate as person_profit_basic_rate,
  new_cust_sale person_new_cust_sale,
  receivable_amount as person_receiveable_amount,
  overdue_amount as person_overdue_amount,
  over_rate as person_over_rate,
  sale_rnk as person_sale_rnk,
  profit_rnk as person_profit_rnk,
  new_rnk person_new_rnk,
  over_rnk as person_over_rnk
 from csx_analyse_tmp.csx_analyse_tmp_hr_sale_manager_person a) b on a.sales_user_number=b.sales_user_number and a.sale_month=b.sale_month