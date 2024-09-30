-- 管家红黑榜
-- 管家销售额毛利额
create table csx_analyse_tmp.csx_analyse_tmp_hr_service_sale as 
with sales_info as 
(
    select distinct customer_code as customer_no,
      service_manager_user_number service_user_work_no,
      service_manager_user_name service_user_name,
      -- service_manager_user_id service_user_id,
      business_attribute_code attribute_code,
      business_attribute_name attribute_name,
     case when  business_attribute_code=1 then 1 
        when business_attribute_code=2 then 2 
        when business_attribute_code=5 then 6 
        end business_type_code,
      service_manager_user_position,
      sales_user_name,
      sales_user_number,
      sales_user_position,
      count()over(partition by customer_code,business_attribute_code) as cnt,
      row_number() over(partition by customer_code, business_attribute_code    order by service_manager_user_number asc  ) as ranks
    from csx_dim.csx_dim_crm_customer_business_ownership
    where sdt= '20240831'
   -- and customer_code='237857'
    --  and service_manager_user_id <> 0 -- and customer_code='111207'
    --  and business_attribute_code='1'
),
    sale as 
    (select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        if(b.customer_code is not null, 1, 0) as new_customer_flag,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
    left join 
    -- 关联商机新客
      (select a.customer_no customer_code,
              business_type_code
        from
        (
        select customer_no,business_type_code from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di 
        where smonth in ('202408')
        union all
        select customer_no,business_type_code from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
        where smonth in  ('202408')
         )a) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
    where sdt >= '20240801'
        and sdt <= '20240831'   
        and (a.business_type_code in ('1','2','6')  -- 1-日配、2-福利、6-BBC
            or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209') and a.business_type_code =4)
            )
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        if(b.customer_code is not null, 1, 0) 
    )
    select  sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        a.sales_user_name,
        a.sales_user_number,
        a.sales_user_position,
        b.service_user_work_no,
        b.service_user_name,
        b.service_manager_user_position,
        new_customer_flag,
        sale_amt/c.cnt as avg_sale_amt,
        profit/c.cnt as avg_profit,
        c.cnt,
        sale_amt,
        profit
    from sale a 
    left join 
    sales_info b on a.customer_code=b.customer_no and a.business_type_code=b.business_type_code
    left join 
    (select distinct
        customer_no,
        business_type_code,
        cnt 
    from sales_info) c on a.customer_code=c.customer_no and a.business_type_code=c.business_type_code
    where new_customer_flag!=1
  --  where leader_user_name='谢志晓'
  ;
  


-- 逾期系数 取中台逾期

 -- drop table csx_analyse_tmp.csx_analyse_tmp_hr_sales_over;
create table csx_analyse_tmp.csx_analyse_tmp_hr_service_over as 
with over_rate as 
(select substr(sdt,1,6) as sale_month,
    performance_region_name as region_name,
    performance_province_name as province_name,
    performance_city_name as city_group_name,
    customer_code, 
    customer_name,
    business_attribute_code,
    business_attribute_name as customer_attribute_code,
    credit_business_attribute_name,
    credit_business_attribute_code,
    channel_name,
    sales_employee_code,
    sales_employee_name,
    sum(overdue_amount) as overdue_amount,
    sum(receivable_amount) as receivable_amount
from 
   -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
     csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df a
    where sdt in ('20240831')
    and ( channel_name in ('大客户','业务代理') 
      or (sales_employee_code in ('81244592','81079752','80897025','81022821','81190209') 
      and a.channel_name in ('项目供应商','前置仓'))
      )
    and receivable_amount>0
    group by substr(sdt,1,6),
    performance_region_name ,
    performance_province_name ,
    performance_city_name ,
    customer_code, 
    customer_name,
    business_attribute_name,
    credit_business_attribute_name,
    channel_name,
    sales_employee_code,
    sales_employee_name ,
    business_attribute_code,
    credit_business_attribute_code
    
),
sales_info as 
(
    select distinct customer_code as customer_no,
      service_manager_user_number service_user_work_no,
      service_manager_user_name service_user_name,
      service_manager_user_id service_user_id,
      business_attribute_code attribute_code,
      business_attribute_name attribute_name,
     case when  business_attribute_code=1 then 1 
        when business_attribute_code=2 then 2 
        when business_attribute_code=5 then 6 
        end business_type_code,
      service_manager_user_position,
      sales_user_name,
      sales_user_number,
      sales_user_position,
      count()over(partition by customer_code,business_attribute_code) as cnt,
      row_number() over(partition by customer_code, business_attribute_code    order by service_manager_user_id asc  ) as ranks
    from csx_dim.csx_dim_crm_customer_business_ownership
    where sdt= '20240831'
   -- and customer_code='237857'
    --  and service_manager_user_id <> 0 -- and customer_code='111207'
    --  and business_attribute_code='1'
)
select  sale_month,
    region_name,
    a.province_name,
    city_group_name,
    customer_code, 
    customer_name,
    customer_attribute_code,
    credit_business_attribute_name,
    channel_name,
    sales_employee_code,
    sales_employee_name,
    b.service_user_work_no,
    b.service_user_name,
    b.service_manager_user_position,
    overdue_amount,
    receivable_amount
from over_rate a 
left join 
sales_info b on a.customer_code=b.customer_no and a.credit_business_attribute_code=b.attribute_code
;

-- 客户评价
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation;
create table csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation as 
with tmp_study_fm_user_form_data as (
  select
    a.id,
    a.form_key,
    regexp_replace(a.original_data, '\\\{|\\\}', '') as original_data,
    b.tag_code as customer_code,
    a.user_id,
    a.create_time
  from
      csx_ods.csx_ods_csx_b2b_study_fm_user_form_data_df a
    LEFT JOIN csx_ods.csx_ods_csx_b2b_study_questionnaire_paper_df b ON a.paper_id = b.id
  WHERE
    b.is_delete = 0
    and create_time<'2024-09-09 00:00:00'   -- 次月需要限制创建日期
    AND a.form_key = 'Gznp3ieG'
),
tmp_study_fm_user_form_data_clean as (
  select
    t1.id,
    t1.form_key,
    t1.key_element as form_item_id,
    coalesce(t2.type, '') as type,
    coalesce(t2.label, '') as label,
    t1.value_element as answer,
    t1.customer_code,
    t1.user_id
  from
    (
      select
        id,
        form_key,
        trim(split(original_data_element, ':') [ 0 ]) as key_element,
        trim(split(original_data_element, ':') [ 1 ]) as value_element,
        customer_code,
        user_id
      from
        (
          select
            id,
            form_key,
            trim(regexp_replace(original_data_element, '\\\"', '')) as original_data_element,
            customer_code,
            user_id
          from
            tmp_study_fm_user_form_data lateral view explode(split(original_data, ",")) t as original_data_element
        ) tmp
    ) t1
    left join csx_ods.csx_ods_csx_b2b_study_fm_user_form_item_df t2 on t1.key_element = t2.form_item_id
    and t1.form_key = t2.form_key
)
select
  a.id,
  a.form_key,
  a.form_item_id,
  a.type,
  a.label,
  coalesce(b.answer, a.answer) as answer,
  a.customer_code,
  a.user_id,
  a.create_time
  rn
from
  (
    select
      *,
      dense_rank()over(partition by customer_code order by id desc ) as rn 
    from
      tmp_study_fm_user_form_data_clean
    WHERE
      form_item_id not like '%label'
  ) a
  left join (
    select
      id,
      form_key,
      split(form_item_id, 'label') [ 0 ] as original_form_item_id,
      answer
    from
      tmp_study_fm_user_form_data_clean
    WHERE
      form_item_id like '%label'
  ) b on a.id = b.id
  and a.form_key = b.form_key
  and a.form_item_id = b.original_form_item_id
  where a.type='RADIO'
    and rn=1 ;




with service_evaluation as ( 
with sales_info as   
(  
    select    
        performance_region_name,  
        performance_province_name,
    
        a.customer_code as customer_no, 
        customer_name,
        service_manager_user_number as service_user_work_no,  
        service_manager_user_name as service_user_name,  
        service_manager_user_position,  
        count(*) over(partition by customer_code) as cnt,  
        row_number() over(partition by customer_code order by service_manager_user_number asc) as ranks  
    from      csx_dim.csx_dim_crm_customer_business_ownership  a 
    join 
    (select customer_code
    from  csx_dws.csx_dws_sale_detail_di 
        where sdt>='20240601' and sdt<='20240831'
    group by customer_code ) b on a.customer_code=b.customer_code
    where sdt = '20240831'  
        and business_attribute_code in ('1','2','5')
      --  and service_manager_user_position='CUSTOMER_SERVICE_MANAGER'
    group by performance_region_name,  
        a.customer_code, 
        customer_name,
        service_manager_user_number ,  
        service_manager_user_name ,  
        -- service_manager_user_id ,
        service_manager_user_position,
        performance_province_name
)  
select   
    performance_region_name,  
    service_user_work_no,
    customer_no,  
    sum(cast(answer_score as decimal(26,1))) as answer_score  
from (  
    select   
        si.performance_region_name,  
        si.service_user_work_no, 
        si.customer_no,
        si.customer_name,
        case   
            when se.answer = '非常满意' then 10  
            when se.answer = '满意' then 8  
            when se.answer = '一般' then 6  
            when se.answer = '不满意' then 2  
            when se.answer = '非常不满意' then 0  
            else 30   
        end as answer_score  
    from sales_info si  
    left join csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation  se on se.customer_code = si.customer_no  
) a  
where service_user_work_no is not null and  service_user_work_no!=''
group by performance_region_name, 
  service_user_work_no,
  customer_no
),
 full_total as(
select sale_month,
    performance_region_name,
    service_user_work_no,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    dense_rank()over(PARTITION BY performance_region_name order by sale_achieve_rate  desc )  sale_rank,
    sale_weight,
    plan_profit,
    profit,
    profit_achieve_rate,
    dense_rank()over(partition by performance_region_name order by profit_achieve_rate  desc ) profit_rank,
    profit_weight,
    coalesce(overdue_rate,0)overdue_rate,
    dense_rank()over(partition by performance_region_name order by coalesce(overdue_rate,0) asc  ) overdue_rank,
    overdue_amount,
    receivable_amount,
    overdue_weight,
    answer_score,
    answer_rank,
    answer_weight,
    customer_cnt,
    all_answer_score
from (
select sale_month,
    performance_region_name,
    service_user_work_no,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    dense_rank()over(PARTITION BY performance_region_name order by coalesce(sale_achieve_rate ,0) desc ) sale_rank,
    sale_weight,
    plan_profit,
    profit,
    profit_achieve_rate,
    dense_rank()over(partition by performance_region_name order by coalesce(profit_achieve_rate,0)  desc ) profit_rank,
    profit_weight,
    coalesce(overdue_rate,0) as overdue_rate,
    dense_rank()over(partition by performance_region_name order by coalesce(overdue_rate ,0) asc  ) as overdue_rank,
    overdue_amount,
    receivable_amount,
    overdue_weight,
    answer_score,
    dense_rank()over(partition by performance_region_name order by coalesce(answer_score,0) desc  ) as answer_rank,
    answer_weight,
    customer_cnt,
    all_answer_score
from 
(select sale_month,
    performance_region_name,
    service_user_work_no,
    b.user_name sales_user_name,
    b.user_position	,
    b.sub_position_name,
    begin_date,
    sum(plan_sales_amt)plan_sales_amt,
    coalesce(sum(avg_sale_amt),0)/10000 sale_amt,
    coalesce(sum(avg_sale_amt),0)/10000 /coalesce(sum(plan_sales_amt),0) as sale_achieve_rate,
    0.2 as sale_weight,
    sum(plan_profit) plan_profit,
    sum(avg_profit)/10000 profit,
    sum(avg_profit)/10000/sum(plan_profit) as profit_achieve_rate,
    0.3 as profit_weight,
    coalesce(sum(overdue_amount)/sum(receivable_amount),0) as overdue_rate,
    sum(overdue_amount)/10000 overdue_amount,
    sum(receivable_amount)/10000 receivable_amount,
    0.2 as overdue_weight,
    sum(answer_score) answer_score,
    0.3 as answer_weight,
    sum(customer_cnt) customer_cnt,
    sum(all_answer_score) all_answer_score
from (
-- 目标表
select smt as sale_month,
    concat(performance_region_name,'大区') as performance_region_name,
    sales_user_number service_user_work_no, 
    cast(plan_sales_amt as decimal(26,6)) plan_sales_amt,
    cast(plan_profit as decimal(26,6))  plan_profit,
    0 avg_sale_amt,
    0 avg_profit,
    0 overdue_amount,
    0 receivable_amount,
    0 answer_score,
    0 customer_cnt,
    0 all_answer_score
from 
     csx_analyse.csx_analyse_source_write_hr_service_red_black_target_mf a 
where smt='202408' 
    and sale_month='202408'
    union all 
select  '202408'sale_month,
        performance_region_name,
        service_user_work_no,
        0 plan_sales_amt,
        0 plan_profit,
        avg_sale_amt,
        avg_profit,
        0 overdue_amount,
        0 receivable_amount,
        0 answer_score,
        0 customer_cnt,
        0 all_answer_score
    from csx_analyse_tmp.csx_analyse_tmp_hr_service_sale
     where service_user_work_no !='' or service_user_work_no is not null 
 union all 
 select '202408' sale_month,
    region_name as performance_region_name,
    service_user_work_no,
    0 plan_sales_amt,
    0 plan_profit,
    0 avg_sale_amt,
    0 avg_profit,
    sum(overdue_amount) overdue_amount,
    sum(receivable_amount) receivable_amount,
    0 answer_score,
    0 customer_cnt,
    0 all_answer_score
from  
 csx_analyse_tmp.csx_analyse_tmp_hr_service_over
 where service_user_work_no !=''
    group by sale_month,
    region_name,
    service_user_work_no
  union all 
  select 
    '202408' sale_month,
    performance_region_name,  
    service_user_work_no,      
    0 plan_sales_amt,
    0 plan_profit,
    0 avg_sale_amt,
    0 avg_profit,
    0 overdue_amount,
    0 receivable_amount,
    avg(answer_score) answer_score,
    count(distinct customer_no) as customer_cnt,
    sum(answer_score) all_answer_score
  from   
   service_evaluation a 
   
    group by performance_region_name,  
    service_user_work_no
) a 
left join
csx_analyse_tmp.csx_analyse_tmp_hr_sale_info b on a.service_user_work_no=b.user_number
 -- where plan_sales_amt <> 0
group by sale_month,
    performance_region_name,
    service_user_work_no,
    b.user_name ,
    b.user_position	,
    b.sub_position_name,
    begin_date
) a 
 where plan_sales_amt<>0
)as a
)
-- select * from full_total 
,
max_rnk as 
(
select sale_month,
  performance_region_name,
  max(sale_rank) max_sale_rank,
  max(overdue_rank) max_overdue_rank,
  max(answer_rank) max_answer_rank
  from full_total
  group by sale_month,
  performance_region_name
)  

select a.sale_month,
  a.performance_region_name,
  service_user_work_no,
  sales_user_name,
  user_position	,
  sub_position_name,
  begin_date,
  case when (dense_rank()over(partition by a.performance_region_name order by (sale_score+profit_score+overdue_score+new_answer_score) desc  ))<11 then '红榜'
        when dense_rank()over(partition by a.performance_region_name order by (sale_score+profit_score+overdue_score+new_answer_score) asc  )<11 then '黑榜'
  else '' end  as top_rank,
  dense_rank()over(partition by a.performance_region_name order by (sale_score+profit_score+overdue_score+new_answer_score) desc  ) as total_rank,
  dense_rank()over(partition by a.performance_region_name order by (sale_score+profit_score+overdue_score+new_answer_score) asc  ) as last_total_rank,
  (sale_score+profit_score+overdue_score+new_answer_score) total_score,
  plan_sales_amt,
  sale_amt,
  sale_achieve_rate,
  sale_rank,
  sale_weight,
  sale_score,
  plan_profit,
  profit,
  profit_achieve_rate,
  profit_rank,
  profit_weight,
  profit_score,
  overdue_rate,
  overdue_rank,
  overdue_amount,
  receivable_amount,
  overdue_weight,
  overdue_score,
  answer_score,
  answer_rank,
  answer_weight,
  new_answer_score,
  customer_cnt,
  all_answer_score
from
( select a.sale_month,
    a.performance_region_name,
    service_user_work_no,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    sale_rank,
    sale_weight,
    CASE  WHEN sale_rank = 1 THEN 20
        when sale_rank=max_sale_rank then 0 
        ELSE 20 - (sale_rank - 1) *(20/(max_sale_rank-1) )
     END  AS sale_score,
    plan_profit,
    profit,
    profit_achieve_rate,
    profit_rank,
    profit_weight,
    CASE
    WHEN profit_rank = 1 THEN 30
    when profit_rank=max_sale_rank then 0 
    ELSE 30 - (profit_rank - 1) *(30/(max_sale_rank-1) )
  END  AS profit_score,
    overdue_rate,
    overdue_rank,
    overdue_amount,
    receivable_amount,
    overdue_weight,
    CASE
    WHEN overdue_rank = 1 THEN 20
    when overdue_rank=max_overdue_rank then 0 
    ELSE 20 - (overdue_rank - 1) *(20/(max_overdue_rank-1) )
  END  AS overdue_score,
  answer_score,
  answer_rank,
  answer_weight,
  CASE
    WHEN answer_rank = 1 THEN 30
    when answer_rank=max_answer_rank then 0 
    ELSE 30 - (answer_rank - 1) *(30/(max_answer_rank-1) ) 
    end new_answer_score,
    customer_cnt,
    all_answer_score
from
     full_total a 
     left join max_rnk b on a.performance_region_name=b.performance_region_name and a.sale_month=b.sale_month
) a 
-- where performance_region_name='华东大区'
order by performance_region_name,
dense_rank()over(partition by a.performance_region_name order by (sale_score+profit_score+overdue_score+new_answer_score) desc  )
;



-- 客户评价明细
 
with sales_info as   
(  
    select    
        performance_region_name,  
        performance_province_name,
    
        a.customer_code as customer_no, 
        customer_name,
        service_manager_user_number as service_user_work_no,  
        service_manager_user_name as service_user_name,  
        -- if(service_manager_user_id=0,'',service_manager_user_id) as service_user_id,  
        -- business_attribute_code as attribute_code,  
        -- business_attribute_name as attribute_name,  
        -- case   
        --     when business_attribute_code = 1 then 1   
        --     when business_attribute_code = 2 then 2   
        --     when business_attribute_code = 5 then 6   
        -- end as business_type_code,  
        service_manager_user_position,  
        -- sales_user_name,  
        -- sales_user_number,  
        -- sales_user_position,  
        count(*) over(partition by customer_code) as cnt,  
        row_number() over(partition by customer_code order by service_manager_user_number asc) as ranks  
    from      csx_dim.csx_dim_crm_customer_business_ownership  a 
    join 
    (select customer_code
    from  csx_dws.csx_dws_sale_detail_di 
        where sdt>='20240601' and sdt<='20240831'
    group by customer_code ) b on a.customer_code=b.customer_code
    where sdt = '20240831'  
        and business_attribute_code in ('1','2','5')
      --  and service_manager_user_position='CUSTOMER_SERVICE_MANAGER'
    group by performance_region_name,  
        a.customer_code, 
        customer_name,
        service_manager_user_number ,  
        service_manager_user_name ,  
        -- service_manager_user_id ,
        service_manager_user_position,
        performance_province_name
) 
 select performance_region_name,
 performance_province_name,
 service_user_work_no,
 service_user_name,
 customer_no,
 customer_name,
 sum(answer_score) answer_score,
 if(answer_flag=1,'是','否') answer_flag
 from ( 
    select   
        si.performance_region_name,  
        performance_province_name,
        si.service_user_work_no, 
        si.service_user_name,
        si.customer_no,
        si.customer_name,
        if(se.answer is not null ,1,0) as answer_flag,
        case   
            when se.answer = '非常满意' then 10  
            when se.answer = '满意' then 8  
            when se.answer = '一般' then 6  
            when se.answer = '不满意' then 2  
            when se.answer = '非常不满意' then 0  
            else 30   
        end as answer_score  
    from sales_info si  
    left join csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation  se on se.customer_code = si.customer_no  
) a 

where service_user_work_no is not null and  service_user_work_no!=''
-- and a.customer_no='176210'
group by performance_region_name,
 service_user_work_no,
 service_user_name,
 customer_no,
 customer_name,
 if(answer_flag=1,'是','否') ,
 performance_province_name

;


-- 客户评价对公司建议
with sales_info as   
(  
    select    
        performance_region_name,  
        performance_province_name,
    
        customer_code as customer_no, 
        customer_name,
        service_manager_user_number as service_user_work_no,  
        service_manager_user_name as service_user_name,  
        -- if(service_manager_user_id=0,'',service_manager_user_id) as service_user_id,  
        -- business_attribute_code as attribute_code,  
        -- business_attribute_name as attribute_name,  
        -- case   
        --     when business_attribute_code = 1 then 1   
        --     when business_attribute_code = 2 then 2   
        --     when business_attribute_code = 5 then 6   
        -- end as business_type_code,  
        service_manager_user_position,  
        -- sales_user_name,  
        -- sales_user_number,  
        -- sales_user_position,  
        count(*) over(partition by customer_code) as cnt,  
        row_number() over(partition by customer_code order by service_manager_user_number asc) as ranks  
    from      csx_dim.csx_dim_crm_customer_business_ownership  
    where sdt = '20240831'  
        and business_attribute_code in ('1','2','5')
      --  and service_manager_user_position='CUSTOMER_SERVICE_MANAGER'
    group by performance_region_name,  
        customer_code, 
        customer_name,
        service_manager_user_number ,  
        service_manager_user_name ,  
        -- service_manager_user_id ,
        service_manager_user_position,
        performance_province_name
)  ,
tmp_study_fm_user_form_data as (
  select
    a.id,
    a.form_key,
    regexp_replace(a.original_data, '\\\{|\\\}', '') as original_data,
    b.tag_code as customer_code,
    a.user_id
  from
      csx_ods.csx_ods_csx_b2b_study_fm_user_form_data_df a
    LEFT JOIN csx_ods.csx_ods_csx_b2b_study_questionnaire_paper_df b ON a.paper_id = b.id
  WHERE
    b.is_delete = 0
    AND a.form_key = 'Gznp3ieG'
),
tmp_study_fm_user_form_data_clean as (
  select
    t1.id,
    t1.form_key,
    t1.key_element as form_item_id,
    coalesce(t2.type, '') as type,
    coalesce(t2.label, '') as label,
    t1.value_element as answer,
    t1.customer_code,
    t1.user_id
  from
    (
      select
        id,
        form_key,
        trim(split(original_data_element, ':') [ 0 ]) as key_element,
        trim(split(original_data_element, ':') [ 1 ]) as value_element,
        customer_code,
        user_id
      from
        (
          select
            id,
            form_key,
            trim(regexp_replace(original_data_element, '\\\"', '')) as original_data_element,
            customer_code,
            user_id
          from
            tmp_study_fm_user_form_data lateral view explode(split(original_data, ",")) t as original_data_element
        ) tmp
    ) t1
    left join csx_ods.csx_ods_csx_b2b_study_fm_user_form_item_df t2 on t1.key_element = t2.form_item_id
    and t1.form_key = t2.form_key
)
select
--   a.id,
--   a.form_key,
--   a.form_item_id,
  c.performance_region_name,
  c.performance_province_name,
  a.type,
  a.label,
  coalesce(b.answer, a.answer) as answer,
  a.customer_code,
  c.customer_name,
  c.service_user_work_no,
  c.service_user_name,
  rn
from
  (
    select
      *,
      dense_rank()over(partition by customer_code order by id desc ) as rn 
    from
      tmp_study_fm_user_form_data_clean
    WHERE
      form_item_id not like '%label'
  ) a
  left join (
    select
      id,
      form_key,
      split(form_item_id, 'label') [ 0 ] as original_form_item_id,
      answer
    from
      tmp_study_fm_user_form_data_clean
    WHERE
      form_item_id like '%label'
  ) b on a.id = b.id
  and a.form_key = b.form_key
  and a.form_item_id = b.original_form_item_id
  left join sales_info c on a.customer_code=c.customer_no
  where a.type='TEXTAREA'
  and rn=1 
  ;



  -- 客户评价信息
  

  -- 客户评价信息
  with sales_info as   
(  
    select    
        performance_region_name,  
        performance_province_name,
    
        a.customer_code as customer_no, 
        customer_name,
        service_manager_user_number as service_user_work_no,  
        service_manager_user_name as service_user_name,  
        -- if(service_manager_user_id=0,'',service_manager_user_id) as service_user_id,  
        -- business_attribute_code as attribute_code,  
        -- business_attribute_name as attribute_name,  
        -- case   
        --     when business_attribute_code = 1 then 1   
        --     when business_attribute_code = 2 then 2   
        --     when business_attribute_code = 5 then 6   
        -- end as business_type_code,  
        service_manager_user_position,  
        -- sales_user_name,  
        -- sales_user_number,  
        -- sales_user_position,  
        count(*) over(partition by customer_code) as cnt,  
        row_number() over(partition by customer_code order by service_manager_user_number asc) as ranks  
    from      csx_dim.csx_dim_crm_customer_business_ownership  a
    join 
    (select customer_code
    from  csx_dws.csx_dws_sale_detail_di 
        where sdt>='20240601' and sdt<='20240831'
    group by customer_code ) b on a.customer_code=b.customer_code
    where sdt = '20240831'  
        and business_attribute_code in ('1','2','5')
        and service_manager_user_number !=''
    group by performance_region_name,  
        a.customer_code, 
        customer_name,
        service_manager_user_number ,  
        service_manager_user_name ,  
        -- service_manager_user_id ,
        service_manager_user_position,
        performance_province_name
)  
select
--   a.id,
--   a.form_key,
  a.form_item_id,
  coalesce(c.performance_region_name,m.performance_region_name)performance_region_name,
  coalesce(c.performance_province_name,m.performance_province_name) performance_province_name,
  a.type,
  a.label,
  answer,
  a.customer_code,
  coalesce(c.customer_name,m.customer_name) customer_name,
  c.service_user_work_no,
  c.service_user_name,
  a.create_time,
  rn
from
   csx_analyse_tmp.csx_analyse_tmp_hr_service_evaluation a 
  left join sales_info c on a.customer_code=c.customer_no
  left join 
  (select customer_code,
    customer_name, 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name
   from csx_dim.csx_dim_crm_customer_info 
  where sdt='current' 
  ) m on a.customer_code=m.customer_code
  where a.type='RADIO'
 and a.customer_code in ('216209','249615','111921')
 -- and create_time<'2024-09-09 00:00:00'
  and rn=1 
  order by performance_region_name,
  performance_province_name,
  a.customer_code,
  form_item_id

;

-- 以下评价明细需要限制日期
  with sales_info as   
(  
    select    
        performance_region_name,  
        performance_province_name,
    
        a.customer_code as customer_no, 
        customer_name,
        service_manager_user_number as service_user_work_no,  
        service_manager_user_name as service_user_name,  
        -- if(service_manager_user_id=0,'',service_manager_user_id) as service_user_id,  
        -- business_attribute_code as attribute_code,  
        -- business_attribute_name as attribute_name,  
        -- case   
        --     when business_attribute_code = 1 then 1   
        --     when business_attribute_code = 2 then 2   
        --     when business_attribute_code = 5 then 6   
        -- end as business_type_code,  
        service_manager_user_position,  
        -- sales_user_name,  
        -- sales_user_number,  
        -- sales_user_position,  
        count(*) over(partition by customer_code) as cnt,  
        row_number() over(partition by customer_code order by service_manager_user_number asc) as ranks  
    from      csx_dim.csx_dim_crm_customer_business_ownership  a
    join 
    (select customer_code
    from  csx_dws.csx_dws_sale_detail_di 
        where sdt>='20240601' and sdt<='20240831'
    group by customer_code ) b on a.customer_code=b.customer_code
    where sdt = '20240831'  
        and business_attribute_code in ('1','2','5')
        and service_manager_user_number !=''
    group by performance_region_name,  
        a.customer_code, 
        customer_name,
        service_manager_user_number ,  
        service_manager_user_name ,  
        -- service_manager_user_id ,
        service_manager_user_position,
        performance_province_name
)  ,
tmp_study_fm_user_form_data as (
  select
    a.id,
    a.form_key,
    regexp_replace(a.original_data, '\\\{|\\\}', '') as original_data,
    b.tag_code as customer_code,
    a.user_id,
    a.create_time
  from
      csx_ods.csx_ods_csx_b2b_study_fm_user_form_data_df a
    LEFT JOIN csx_ods.csx_ods_csx_b2b_study_questionnaire_paper_df b ON a.paper_id = b.id
  WHERE
    b.is_delete = 0
    and create_time<'2024-09-09 00:00:00'   -- 限制日期
    AND a.form_key = 'Gznp3ieG'
),
tmp_study_fm_user_form_data_clean as (
  select
    t1.id,
    t1.form_key,
    t1.key_element as form_item_id,
    coalesce(t2.type, '') as type,
    coalesce(t2.label, '') as label,
    t1.value_element as answer,
    t1.customer_code,
    t1.user_id,
    t1.create_time
  from
    (
      select
        id,
        form_key,
        trim(split(original_data_element, ':') [ 0 ]) as key_element,
        trim(split(original_data_element, ':') [ 1 ]) as value_element,
        customer_code,
        user_id,
        tmp.create_time
      from
        (
          select
            id,
            form_key,
            trim(regexp_replace(original_data_element, '\\\"', '')) as original_data_element,
            customer_code,
            user_id,
            create_time
          from
            tmp_study_fm_user_form_data lateral view explode(split(original_data, ",")) t as original_data_element
        ) tmp 
    ) t1
    left join   csx_ods.csx_ods_csx_b2b_study_fm_user_form_item_df t2 on t1.key_element = t2.form_item_id
    and t1.form_key = t2.form_key
)
select
--   a.id,
--   a.form_key,
  a.form_item_id,
  coalesce(c.performance_region_name,m.performance_region_name)performance_region_name,
  coalesce(c.performance_province_name,m.performance_province_name) performance_province_name,
  a.type,
  a.label,
  coalesce(b.answer, a.answer) as answer,
  a.customer_code,
  coalesce(c.customer_name,m.customer_name) customer_name,
  c.service_user_work_no,
  c.service_user_name,
  a.create_time,
  rn
from
  (
    select
      *,
      dense_rank()over(partition by customer_code order by id desc ) as rn 
    from
      tmp_study_fm_user_form_data_clean
    WHERE
      form_item_id not like '%label'
  ) a
  left join (
    select
      id,
      form_key,
      split(form_item_id, 'label') [ 0 ] as original_form_item_id,
      answer
    from
      tmp_study_fm_user_form_data_clean
    WHERE
      form_item_id like '%label'
  ) b on a.id = b.id
  and a.form_key = b.form_key
  and a.form_item_id = b.original_form_item_id
  left join sales_info c on a.customer_code=c.customer_no
  left join 
  (select customer_code,
    customer_name, 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name
   from csx_dim.csx_dim_crm_customer_info 
  where sdt='current' 
  ) m on a.customer_code=m.customer_code
  where a.type='RADIO'
 -- and a.customer_code in ('216209','249615','111921')
  and create_time<'2024-09-09 00:00:00'
  and rn=1 
  order by performance_region_name,
  performance_province_name,
  a.customer_code,
  form_item_id