--销售经理红黑榜
-- 销售员红黑榜1.0
-- 销售员信息
-- 只有一名销售经理的省区/城市：河北（詹娟），陕西（曹杰），苏州（章明新）、广东深圳（何渊）合肥胡艳

-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_sale_info;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_info as 
with position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
leader_info as 
  (  select a.*,
    c.name as leader_user_position_name,
    b.name as leader_source_user_position_name 
    from 
    (SELECT
      *,
      row_number() over(PARTITION BY user_id ORDER BY distance asc) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt = 'current'
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    left join position_dic b on user_position=b.code
    left join position_dic c on a.user_position_type=c.code
    where rank=1
  )
select a.user_id,
  a.user_number,
  a.user_name,
  coalesce(a.user_position,source_user_position)user_position ,
  replace(c.name,'（旧）','') user_position_name,
  d.name as sub_position_name,
  a.begin_date,
  a.source_user_position,
  a.leader_user_id,
  a.new_leader_user_id,
  a.province_id,
  a.province_name,
  a.city_code,
  a.city_name,
  b.user_number leader_user_number,
  b.user_name leader_user_name,
  b.user_position_type leader_user_position,
  b.leader_user_position_name,
  b.user_position leader_source_user_position,
  b.leader_source_user_position_name  
from 
 (select
  user_id,
  user_number,
  user_name,
  coalesce(user_position,source_user_position)  user_position,
  begin_date,
  source_user_position,
  if(a.user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'), user_id, leader_user_id ) leader_user_id,
   case when a.province_id='6' then '1000000565219'
      when a.city_code='320500' then '1000000567463'
      when a.province_id='26' then '1000000426003'
      when a.city_code='440300' then '1000000426252'
      when a.city_code='340100' then '1000000596953'
      else ''
  end new_leader_user_id,
  province_id,
  province_name,
  city_code,
  city_name
  from 
     csx_dim.csx_dim_uc_user a 
  left  join 
    (select distinct
        employee_name,
        employee_code,
        begin_date,
        record_type_name
    from csx_dim.csx_dim_basic_employee 
        where sdt='current' 
        and card_type=0 
      --  and record_type_code	!=4
    )b on a.user_number=b.employee_code
    
    where
    sdt = 'current'
 --  and status=0 
 -- and (user_position like 'SALES%'
  )a 
 left join leader_info  b on a.leader_user_id=b.user_id
 left join position_dic c on a.user_position=c.code
 left join position_dic d on a.source_user_position=d.code
  ;

  
-- 1.0 -- 销售明细
-- drop table  csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale ;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale as 
with 
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
            or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209','81102471') and a.business_type_code =4)
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
        sales_user_name,
        sales_user_number,
        sales_user_position,
        user_position_name,
        sub_position_name,
        begin_date,
        leader_user_number,
        leader_user_name,
        leader_source_user_position_name,
        new_leader_user_number,
        new_leader_user_name,
        new_customer_flag,
        sale_amt,
        profit
    from sale a 
    left join 
    csx_analyse_tmp.csx_analyse_tmp_hr_sale_info b on a.sales_user_number=b.user_number
 ;
 
 
-- select * from csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days;
-----应收周转天数用期末城市 销售取含税计算
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days;
create table csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days as 
select
    c.performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    c.sales_user_id,
    c.sales_user_number,
    c.sales_user_name,
    c.sales_user_position,
    c.leader_user_number,
    c.leader_user_name,
    c.leader_user_position,
    c.leader_source_user_position,
    c.new_leader_user_number,
    c.new_leader_user_name,
  DATEDIFF('2024-09-01','2024-08-01')as accounting_cnt,
  coalesce(sum(sale_amt),0) sale_amt,
  coalesce(sum(excluding_tax_sales),0) excluding_tax_sales,
  sum(qm_receivable_amount) qm_receivable_amount,
  sum(qc_receivable_amount) qc_receivable_amount,
  sum(qm_receivable_amount+qc_receivable_amount)/2 receivable_amount,
  if(sum(qm_receivable_amount+qc_receivable_amount)/2 =0 or coalesce(sum(sale_amt),0)=0,0,
        DATEDIFF('2024-09-01','2024-08-01')/(coalesce(sum(sale_amt),0)/(sum(qm_receivable_amount+qc_receivable_amount)/2 ))) as turnover_days
from 
( select
    c.performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    c.sales_user_id,
    c.sales_user_number,
    c.sales_user_name,
    c.sales_user_position,
    c.leader_user_number,
    c.leader_user_name,
    c.leader_user_position,
    c.leader_source_user_position,
    c.new_leader_user_number,
    c.new_leader_user_name,
    sum(b.excluding_tax_sales) excluding_tax_sales,
    sum(sale_amt) sale_amt,
    sum(a.qm_receivable_amount) qm_receivable_amount,
    sum(qc_receivable_amount) qc_receivable_amount
  from 
   ( 
  	 select
         channel_name,
         customer_code,
         sum(if(sdt='20240831',receivable_amount,0))  qm_receivable_amount,
         sum(if(sdt='20240731',receivable_amount,0))  qc_receivable_amount
         --应收账款
       from 
         -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
	csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
       where (sdt='20240831' or sdt='20240731')  
        group by  
         channel_name ,
         customer_code
  	   )a
  LEFT join (
  	select 			
          customer_code,
          sum(sale_amt) sale_amt,
  	    sum(sale_amt_no_tax) as excluding_tax_sales
      from   csx_dws.csx_dws_sale_detail_di
           where sdt >='20240801'   and sdt <='20240831'
  		and  channel_code in ('1','7','9') 
  		group by customer_code
  		)b on a.customer_code=b.customer_code 
  LEFT join
          (select customer_code,
                customer_name,
                channel_code,
                channel_name,
                m.sales_user_id,
                m.sales_user_number,
                m.sales_user_name,
                m.sales_user_position,
                performance_region_code,
                performance_region_name,
                performance_province_code,
                performance_province_name,
                performance_city_code,
                performance_city_name,
                p.leader_user_number,
                p.leader_user_name,
                p.leader_user_position,
                p.leader_source_user_position_name leader_source_user_position,
                p.new_leader_user_number,
                p.new_leader_user_name
            from  csx_dim.csx_dim_crm_customer_info m 
            left join csx_analyse_tmp.csx_analyse_tmp_hr_sale_info p on m.sales_user_number=p.user_number
              where sdt= '20240831'
                and (dev_source_code!=3 
                    or   (sales_user_number in ('81244592','81079752','80897025','81022821','81190209','81102471')
                    and dev_source_code=3)
                    )
             and channel_code  in ('1','7','9')
           ) c on a.customer_code=c.customer_code 
 where c.customer_code is not null 
  group by    c.performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    c.sales_user_id,
    c.sales_user_number,
    c.sales_user_name,
    c.sales_user_position,
    c.leader_user_number,
    c.leader_user_name,
    c.leader_user_position,
    c.leader_source_user_position,
    c.new_leader_user_number,
    c.new_leader_user_name
 )c	
group by  performance_region_name,
    c.performance_province_name,
    c.performance_city_name,
    c.sales_user_id,
    c.sales_user_number,
    c.sales_user_name,
    c.sales_user_position,
    c.leader_user_number,
    c.leader_user_name,
    c.leader_user_position,
    c.leader_source_user_position,
    c.new_leader_user_number,
    c.new_leader_user_name;
    
     


with sales_manager_sale as 
(select sale_month,
      performance_region_name,
      sales_manager_number,
      new_leader_user_number,
      new_leader_user_name,
      sum(if (new_sales_flag=1 and begin_date is not null ,1,0)) as sales_cnt,
      sum(sale_amt) sale_amt,
      sum(profit) profit,
      sum(new_customer_sale_amt) new_customer_sale_amt,
      sum(new_customer_profit) new_customer_profit
from 
(select sale_month,
      performance_region_name,
      sales_user_number,
      begin_date,
      sales_user_position,
      user_position_name,
      sub_position_name,
      if(substr(begin_date,1,6)>='202401',0,1 ) new_sales_flag,
      leader_user_number sales_manager_number,
      leader_user_name,
      leader_source_user_position_name leader_user_position_name,
      new_leader_user_number,
      new_leader_user_name,
      sum(sale_amt) sale_amt,
      sum(profit)profit,
      sum(if(new_customer_flag=1,sale_amt,0)) as new_customer_sale_amt,
      sum(if(new_customer_flag=1,profit,0)) as new_customer_profit
 from    csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale
 -- where sales_user_position in ('SALES_MANAGER','SALES','SALES_CITY_MANAGER')
  group by sale_month,
      performance_region_name,
      sales_user_number,
      begin_date,
      sales_user_position,
      user_position_name,
      sub_position_name,
      if(substr(begin_date,1,6)>='202401',0,1 ) ,
      leader_user_number ,
      leader_user_name,
      leader_source_user_position_name ,
      new_leader_user_number,
      new_leader_user_name
      )a 
      group by sale_month,
      performance_region_name,
      sales_manager_number,
      new_leader_user_number,
      new_leader_user_name
),
sales_sale as  (
select smt,
    performance_region_name,
    sales_user_number,
    b.user_name sales_user_name,
    b.user_position	,
    b.sub_position_name,
    begin_date,
    max(sales_team_number) sales_team_number,
    sum(sales_user_base_profit)  sales_user_base_profit,
    sum(plan_sales_amt)   plan_sales_amt,
    sum(plan_profit)   plan_profit,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    coalesce(sum(new_customer_sale_amt),0) as new_customer_sale_amt,
    sum(new_customer_profit) as new_customer_profit,
    sum(sales_cnt) as sales_cnt,
    sum(trunover_sale_amt )trunover_sale_amt,
    sum(qc_receivable_amount)qc_receivable_amount,
    sum(qm_receivable_amount) qm_receivable_amount,
    sum(turnover_days) turnover_days
from 
(-- 目标表
    select smt,
    concat(performance_region_name,'大区') as performance_region_name,
    sales_user_number,
    cast(sales_team_number as decimal(26,6)) sales_team_number,
    cast(sales_user_base_profit as decimal(26,6)) sales_user_base_profit,
    cast(plan_sales_amt as decimal(26,6)) plan_sales_amt,
    cast(plan_profit as decimal(26,6)) plan_profit,
    0 sale_amt,
    0 profit,
    0 as new_customer_sale_amt,
    0 as new_customer_profit,
    0 as sales_cnt,
    0 trunover_sale_amt,
    0 qm_receivable_amount,
    0 qc_receivable_amount,
    0 turnover_days
from 
     csx_analyse.csx_analyse_source_write_hr_sales_manager_red_black_target_mf a 
where smt='202408' 
    and sale_month='202408'
union all 

select '202408'sale_month,
    performance_region_name,
    sales_manager_number,
    0 sales_team_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    (sale_amt) sale_amt,
    (profit )profit,
    new_customer_sale_amt,
    new_customer_profit,
    sales_cnt,
    0 trunover_sale_amt,
    0 qm_receivable_amount,
    0 qc_receivable_amount,
    0 turnover_days
from 
    sales_manager_sale    
  where sales_manager_number != coalesce(new_leader_user_number,'')
 union all
 
 select '202408'sale_month,
    performance_region_name,
    leader_user_number sales_manager_number,
    0 sales_team_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    0 sale_amt,
    0 profit,
    0 new_customer_sale_amt,
    0 new_customer_profit,
    0 sales_cnt,
    sum(sale_amt) as trunover_sale_amt,
    sum(qm_receivable_amount) qm_receivable_amount,
    sum(qc_receivable_amount) qc_receivable_amount,
    if(sum(qm_receivable_amount+qc_receivable_amount)/2 =0 or coalesce(sum(sale_amt),0)=0,0,
        DATEDIFF('2024-08-01','2024-07-01')/(coalesce(sum(sale_amt),0)/(sum(qm_receivable_amount+qc_receivable_amount)/2 ))) as turnover_days
from  csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days
      where leader_user_number != coalesce(new_leader_user_number,'')
      group by  performance_region_name,
    leader_user_number 
  union all 
  -- 单独处理城市经理
select '202408'sale_month,
    performance_region_name,
    new_leader_user_number as sales_manager_number,
    0 sales_team_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    (sale_amt) sale_amt,
    (profit )profit,
    new_customer_sale_amt,
    new_customer_profit,
    sales_cnt,
    0 trunover_sale_amt,
    0 qm_receivable_amount,
    0 qc_receivable_amount,
    0 turnover_days
from 
    sales_manager_sale    
  where new_leader_user_number is not null 
 
union all 
   select '202408'sale_month,
    performance_region_name,
    new_leader_user_number as  sales_manager_number,
    0 sales_team_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    0 sale_amt,
    0 profit,
    0 new_customer_sale_amt,
    0 new_customer_profit,
    0 sales_cnt,
    sum(sale_amt) as trunover_sale_amt,
    sum(qm_receivable_amount) qm_receivable_amount,
    sum(qc_receivable_amount) qc_receivable_amount,
    if(sum(qm_receivable_amount+qc_receivable_amount)/2 =0 or coalesce(sum(sale_amt),0)=0,0,
        DATEDIFF('2024-09-01','2024-08-01')/(coalesce(sum(sale_amt),0)/(sum(qm_receivable_amount+qc_receivable_amount)/2 ))) as turnover_days
from  csx_analyse_tmp.csx_analyse_tmp_hr_red_black_turnover_days
      where new_leader_user_number is not null 
      group by performance_region_name,
      new_leader_user_number
)a 
left join   
   csx_analyse_tmp.csx_analyse_tmp_hr_sale_info b on a.sales_user_number=b.user_number	
--   left join 
--       csx_analyse_tmp.csx_analyse_tmp_hr_sale_info c on a.new=b.user_number	

group by smt,
    performance_region_name,
    sales_user_number,
    b.user_name,
    b.user_position	,
    b.sub_position_name,
    begin_date
),
sales_manager_cnt as 
(select count(sales_user_number) as cnt ,
    max(sale_rank) max_sale_rank,
    max(profit_rank) max_profit_rank,
    max(new_cust_rank) max_cust_rank,
    max(turnover_rank) max_turnover_rank
from 
(select sales_user_number,
    dense_rank()over( order by sale_amt/10000/plan_sales_amt  desc ) sale_rank,
    dense_rank()over( order by profit/10000/plan_profit  desc ) profit_rank,
    dense_rank()over( order by nvl(new_customer_sale_amt/10000/sales_team_number,0) desc)  as new_cust_rank,
    dense_rank()over( order by turnover_days asc)  as turnover_rank
from sales_sale
 where plan_sales_amt<>0
-- group by sales_user_number
)a 
),
score as  (
select performance_region_name,
    sales_user_number,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    sales_team_number,
    sales_user_base_profit,
    plan_sales_amt,
    sale_amt,
    sale_achieve_rate,
    sale_rank,
    sale_weight,
    CASE  WHEN sale_rank = 1 THEN 30
        when sale_rank=max_sale_rank then 0 
        ELSE 30 - (sale_rank - 1) *(30/(max_sale_rank-1) )
     END  AS sale_score,
    plan_profit,
    profit,
    profit_achieve_rate,
    profit_rank,
    profit_weight,
    CASE  WHEN profit_rank = 1 THEN 30
        when profit_rank=max_profit_rank then 0 
        ELSE 30 - (profit_rank - 1) *(30/(max_profit_rank-1) )
     END  AS profit_score,
    new_customer_sale_amt,
    sales_cnt,
    avg_new_customer_amt,
    new_cust_rank,
    new_customer_weight,
    CASE  WHEN new_cust_rank = 1 THEN 20
        when new_cust_rank=max_cust_rank then 0 
        ELSE 20 - (new_cust_rank - 1) *(20/(max_cust_rank-1) )
     END  AS new_cust_score,
     turnover_days,
     turnover_rank,
     turnover_weight,
     CASE  WHEN turnover_rank = 1 THEN 20
        when turnover_rank=max_turnover_rank then 0 
        ELSE 20 - (turnover_rank - 1) *(20/(max_turnover_rank-1) )
     END  AS turnover_score,
    trunover_sale_amt,
    qc_receivable_amount,
    qm_receivable_amount
  from  (
select performance_region_name,
    sales_user_number,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    (sales_team_number) sales_team_number,
    (sales_user_base_profit)  sales_user_base_profit,
    (plan_sales_amt)   plan_sales_amt,
    (sale_amt)/10000 sale_amt,
    if(plan_sales_amt=0,0,sale_amt/10000 /plan_sales_amt) as sale_achieve_rate,
    dense_rank()over( order by sale_amt/10000/plan_sales_amt  desc ) sale_rank,
    0.3 as sale_weight,
    (plan_profit)   plan_profit,
    (profit)/10000 profit,
    if(plan_profit=0,0, profit/10000/plan_profit)  profit_achieve_rate,
    dense_rank()over(order by profit/10000/plan_profit  desc ) profit_rank,
    0.3 as profit_weight,
    coalesce(new_customer_sale_amt,0)/10000 as new_customer_sale_amt,
    (sales_team_number) as sales_cnt,
    nvl(new_customer_sale_amt/10000/sales_team_number,0) as avg_new_customer_amt,
    dense_rank()over( order by nvl(new_customer_sale_amt/10000/sales_team_number,0) desc)  as new_cust_rank,
    0.2 as new_customer_weight,
    (turnover_days) turnover_days,
    dense_rank()over( order by turnover_days asc)  as turnover_rank,
    0.2 as turnover_weight,
    (trunover_sale_amt )/10000 trunover_sale_amt,
    (qc_receivable_amount)/10000  qc_receivable_amount,
    (qm_receivable_amount)/10000 qm_receivable_amount,
    cnt,
    max_turnover_rank,
    max_cust_rank,
    max_profit_rank,
    max_sale_rank
from sales_sale 
left join sales_manager_cnt on 1=1 
where plan_sales_amt<>0
)a
)
-- select * from sales_manager_cnt
select performance_region_name,
    sales_user_number,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    -- sales_team_number,
    -- sales_user_base_profit,
    case when (total_rank/max(total_rank)over()<=0.10)  then '红榜'
        when (last_total_rank/max(last_total_rank)over()<=0.10) then '黑榜'
        else '' end  as top_rank,
    total_rank,
    last_total_rank,
    total_score,
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
    new_customer_sale_amt,
    sales_cnt,
    avg_new_customer_amt,
    new_cust_rank,
    new_customer_weight,
    new_cust_score,
    turnover_days,
    turnover_rank,
    turnover_weight,
    turnover_score,
    trunover_sale_amt,
    qc_receivable_amount,
    qm_receivable_amount
  from 
(select performance_region_name,
    sales_user_number,
    sales_user_name,
    user_position	,
    sub_position_name,
    begin_date,
    -- sales_team_number,
    sales_user_base_profit,
    dense_rank()over( order by (sale_score+profit_score+new_cust_score+turnover_score) desc  ) as total_rank,
    dense_rank()over( order by (sale_score+profit_score+new_cust_score+turnover_score) asc  ) as last_total_rank,
    (sale_score+profit_score+new_cust_score+turnover_score) total_score,
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
    new_customer_sale_amt,
    sales_team_number as sales_cnt,
    avg_new_customer_amt,
    new_cust_rank,
    new_customer_weight,
    new_cust_score,
    turnover_days,
    turnover_rank,
    turnover_weight,
    turnover_score,
    trunover_sale_amt,
    qc_receivable_amount,
    qm_receivable_amount
  from  score a 
) a 
order by total_rank asc 