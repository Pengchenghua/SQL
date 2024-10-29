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
    
 select a.*,b.name,replace(b.name,'（旧）','') ,c.sub_name from   csx_analyse_tmp.csx_analyse_tmp_hr_sale_info  a 
 left join 
 (select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt='20240921'
       and dic_type = 'POSITION'
    ) b on a.user_position	=b.code
 left join 
 (select dic_key as code,dic_value as sub_name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt='20240921'
       and dic_type = 'POSITION'
    ) c on a.source_user_position=c.code
 where user_name='李佩丽'
 
 
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
        where smonth in ('202409')
        union all
        select customer_no,business_type_code from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
        where smonth in  ('202409')
         )a) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
    where sdt >= '20240901'
        and sdt <= '20240930'   
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
 
-- select * from csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale

-- 逾期系数 取SAP逾期

 -- drop table csx_analyse_tmp.csx_analyse_tmp_hr_sales_over;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sales_over as 
with 
over_rate as 
(select substr(sdt,1,6) as sale_month,
    performance_region_name as region_name,
    performance_province_name as province_name,
    performance_city_name as city_group_name,
    customer_code, 
    customer_name,
    business_attribute_name as customer_attribute_code,
    credit_business_attribute_name,
    channel_name,
    sales_employee_code,
    sales_employee_name,
    sum(overdue_amount) as overdue_amount,
    sum(receivable_amount) as receivable_amount
from 
   -- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
    csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df a
    where sdt in ('20240931')
    and ( channel_name in ('大客户','业务代理') 
      or (sales_employee_code in ('81244592','81079752','80897025','81022821','81190209','81102471')
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
    sales_employee_name     
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
    begin_date,
    user_position_name,
    sub_position_name,
    leader_user_number,
    leader_user_name,
    leader_user_position,
    overdue_amount,
    receivable_amount
from over_rate a 
left join 
csx_analyse_tmp.csx_analyse_tmp_hr_sale_info b on a.sales_employee_code=b.user_number
-- where sales_employee_name='谢志晓'
;

--  2.0 新签合同金额明细
-- drop table  csx_analyse_tmp.csx_analyse_tmp_business ;
create table csx_analyse_tmp.csx_analyse_tmp_business as 
with business as  
(select  business_number,
    customer_id,
    a.customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    business_stage,
    business_sign_time,
    estimate_contract_amount,
    htjey/10000 htjey,
    htqsrq,  --  合同起始日期
	  htzzrq,  --  合同终止日期
    yue,
    create_time,
    case when day(to_date(create_time)) between 1 and 15 then '月中' end days_note,
    
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    contract_number,
    -- 年化，不足一年按照一年计算，超一年/12
    if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) as tran_year,
    -- 年化金额，先取合同金额再取商机金额
    (if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,b.htjey/10000 ,a.estimate_contract_amount)/if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) ) tran_contract_amount,
    rn
from 
 ( select business_number,
    customer_id,
    customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    business_stage,
    business_sign_time,
    estimate_contract_amount,
    create_time,
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    contract_number,
    contract_begin_date,
    contract_end_date,
    row_number()over(partition by customer_id,business_attribute_code,owner_user_number) rn 
  from     csx_dim.csx_dim_crm_business_info a 
    where sdt='current'
     and status=1
     and business_type_code in (1,2,6)
     and business_stage >= 2
     and substr(create_time, 1, 10) >=  trunc('${i_sdate}', 'MM')
     and substr(create_time, 1, 10) <= '${i_sdate}'
     and approval_status_code!=3
 )a 
left join 
-- 可以取最新日期关联合同号
(select 
    t1.htbh,--  合同编码
   (case when length(trim(t1.customernumber))>0 then trim(t1.customernumber) else t3.customer_code end) as customer_no,  --  客户编码
  htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
	round(datediff(htzzrq,htqsrq)/30.5,0) yue
from 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_df 
   where sdt= regexp_replace(date_sub(current_date,1),'-','') 
   and length(htbh)>0) t1 
 left join 
   (select * 
   from csx_ods.csx_ods_ecology_154_uf_xshttzv2_dt4_df 
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t2 
   on t1.id=t2.mainid 
left join 
   (select * 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t3 
   on t2.khmc=t3.customer_name
   )b   on b.customer_no=a.customer_code  and b.htbh=a.contract_number   

)
 select days_note,
    a.business_number,
    customer_id,
    a.customer_code,
    customer_name,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    user_position_name,
    sub_position_name,
    begin_date,
    owner_province_id,
    owner_province_name,
    owner_city_code,
    owner_city_name	,
    performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.business_type_code,
    business_attribute_code	,
    business_attribute_name	,
    business_stage,
    business_sign_time,
    estimate_contract_amount,   -- 商机签约金额
    htjey ,          -- 泛微合同金额
    htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
    yue,
    create_time,
    contract_cycle_int,
    contract_cycle_desc,
    contract_number,
    tran_year,
    tran_contract_amount,   -- 年化金额
    rn,
    max_sdt,
    after_date
from business a 
left join csx_analyse_tmp.csx_analyse_tmp_hr_sale_info s on a.owner_user_number=s.user_number
 left join 
 -- 判断是否三个月以上断约客户
 (select 	performance_province_name, 
    business_type_code,
	after_date, 
	a.customer_code,
	max_sdt
from
(		select 
		    performance_province_name,
		    business_type_code,
			customer_code,
			max(sdt) max_sdt,
			regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd'),'yyyy-MM-dd'),90) as string),'-','') as after_date
		from 
			csx_dws.csx_dws_sale_detail_di 
		where 
			sdt between '20220101' and '20240930'
			and business_type_code=1           --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9')    --  渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and order_channel_code not in (4,6)
		group by 
			performance_province_name,
		    business_type_code,
			customer_code
		) a
	   where 1=1
	    and after_date>='20240901'  -- 大于当月的正在履约
			group by performance_province_name, 
			business_type_code,
			after_date, 
			a.customer_code,
			max_sdt 
	)b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code and a.performance_province_name=b.performance_province_name
	left join   
( select customer_code,customer_name
    from csx_dim.csx_dim_crm_customer_info
     where sdt= 'current'
        and channel_code  in ('1','7','9')
) c on a.customer_code=c.customer_code     
	where after_date<'20240901' or after_date is null    
;

-- select *
--  from csx_analyse_tmp.csx_analyse_tmp_business ;


-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_full_total ;
create table csx_analyse_tmp.csx_analyse_tmp_hr_full_total as 
with tmp_sales_sale as 
(
select smt,
      performance_region_name,
    sales_user_number,
    b.user_name sales_user_name,
    b.user_position	,
    b.sub_position_name,
    begin_date,
    leader_user_name,
    sum(sales_user_base_profit)  sales_user_base_profit,
    sum(plan_sales_amt)   plan_sales_amt,
    sum(plan_profit)   plan_profit,
    sum(sale_amt) sale_amt,
    sum(profit) profit,
    sum(new_customer_sale_amt) as new_customer_sale_amt,
    sum(new_customer_profit) as new_customer_profit,
    sum(overdue_amount) overdue_amount,
    sum(receivable_amount) receivable_amount,
    sum(middle_customer_cn) as middle_customer_cn,
    sum(middle_contract_amt) middle_contract_amt,
    sum(end_customer_cn) as end_customer_cn,
    sum(end_contract_amt) end_contract_amt
from 
(-- 目标表
  select smt,
    concat(performance_region_name,'大区') as performance_region_name,
    sales_user_number,
    cast(sales_user_base_profit as decimal(26,6)) sales_user_base_profit,
    cast(plan_sales_amt as decimal(26,6)) plan_sales_amt,
    cast(plan_profit as decimal(26,6)) plan_profit,
    0 sale_amt,
    0 profit,
    0 as new_customer_sale_amt,
    0 as new_customer_profit,
    0 overdue_amount,
    0 receivable_amount,
    0 as middle_customer_cn,
    0 middle_contract_amt,
    0 as end_customer_cn,
    0 end_contract_amt
from 
   csx_analyse.csx_analyse_source_write_hr_sales_red_black_target_mf a 
   left join 
   csx_analyse_tmp.csx_analyse_tmp_hr_sale_info b on a.sales_user_number=b.user_number	
where smt='202409' 
    and sale_month='202409'
union all 

select sale_month,
    performance_region_name,
    sales_user_number,

    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    sum(sale_amt) sale_amt,
    sum(profit )profit,
    sum(if(new_customer_flag=1,sale_amt,0)) as new_customer_sale_amt,
    sum(if(new_customer_flag=1,profit,0)) as new_customer_profit,
    0 overdue_amount,
    0 receivable_amount,
    0 as middle_customer_cn,
    0 middle_contract_amt,
    0 as end_customer_cn,
    0 end_contract_amt
from  csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale
 -- where sales_user_number='81214954'
group by sale_month,
    performance_region_name,
    sales_user_number
    
union all  
select  sale_month,
    region_name performance_region_name,
    sales_employee_code,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    0 sale_amt,
    0 profit,
    0 as new_customer_sale_amt,
    0 as new_customer_profit,
    sum(overdue_amount) overdue_amount,
    sum(receivable_amount) receivable_amount,
    0 as middle_customer_cn,
    0 middle_contract_amt,
    0 as end_customer_cn,
    0 end_contract_amt
from csx_analyse_tmp.csx_analyse_tmp_hr_sales_over
    group by   sale_month,
    region_name,
    sales_employee_code
union all 

select sale_month,
    performance_region_name,
    owner_user_number,
    0 sales_user_base_profit,
    0 plan_sales_amt,
    0 plan_profit,
    0 sale_amt,
    0 profit,
    0 as new_customer_sale_amt,
    0 as new_customer_profit,
    0 overdue_amount,
    0 receivable_amount,
    middle_customer_cn,
    middle_contract_amt,
    end_customer_cn,
    end_contract_amt
from (
select substr(regexp_replace(to_date(create_time),'-',''),1,6) sale_month,
    performance_region_name,
    owner_user_number,
    sum(if(days_note='月中',1,0)) as middle_customer_cn,
    sum(if(days_note='月中',tran_contract_amount,0)) middle_contract_amt,
    count(business_number) as end_customer_cn,
    sum(tran_contract_amount) end_contract_amt
from  csx_analyse_tmp.csx_analyse_tmp_business
where rn=1
group by substr(regexp_replace(to_date(create_time),'-',''),1,6)  ,
    performance_region_name,
    owner_user_number	
)a 
)a 
left join   
   csx_analyse_tmp.csx_analyse_tmp_hr_sale_info b on a.sales_user_number=b.user_number	
group by smt,
      performance_region_name,
    sales_user_number,
    b.user_name,
    b.user_position	,
    b.sub_position_name,
    begin_date,
    leader_user_name
)
select * from tmp_sales_sale  
;


-- 计算得分结果集


-- 计算得分结果集


-- 计算得分结果集
with tmp_performance as 
(select smt
,performance_region_name
,sales_user_number
,sales_user_name
,user_position
,sub_position_name
,begin_date
,leader_user_name
,sales_user_base_profit
,plan_sales_amt
,sale_amt/10000 sale_amt
,sale_amt/10000/plan_sales_amt as sale_achieve_rate
,dense_rank()over(partition by performance_region_name order by sale_amt/10000/plan_sales_amt  desc ) sale_rank
,0.2 as sale_weight
,plan_profit
,profit/10000 profit
,profit/10000/plan_profit as profit_achieve_rate
,dense_rank()over(partition by performance_region_name order by profit/10000/plan_profit  desc ) profit_rank
,0.2 as profit_weight
,new_customer_sale_amt/10000  new_customer_sale_amt
,dense_rank()over(partition by performance_region_name order by new_customer_sale_amt desc)  as new_cust_rank
,0.2 as new_customer_weight
,new_customer_profit/10000 new_customer_profit
,overdue_amount/10000  overdue_amount
,receivable_amount/10000 receivable_amount
,coalesce(overdue_amount/receivable_amount,0) as overdue_rate
,dense_rank()over(partition by performance_region_name order by coalesce(overdue_amount/receivable_amount,0) asc ) as overdue_rank
,0.2 as overdue_weight
,middle_customer_cn
,end_customer_cn
,(coalesce(middle_customer_cn,0)+coalesce(end_customer_cn,0))/2 avg_customer_cn
,dense_rank()over(partition by performance_region_name order by (coalesce(middle_customer_cn,0)+coalesce(end_customer_cn,0))/2 desc ) business_cnt_rnk
,0.1 as business_cnt_weight
,middle_contract_amt
,end_contract_amt
,(coalesce(middle_contract_amt,0)+coalesce(end_contract_amt,0))/2 avg_customer_contract_amt
,dense_rank()over(partition by performance_region_name order by (coalesce(middle_contract_amt,0)+coalesce(end_contract_amt,0))/2 desc ) business_amt_rnk
,0.1 as business_amt_weight
from  csx_analyse_tmp.csx_analyse_tmp_hr_full_total
where plan_sales_amt<>0
) ,
tmp_max_rnk as 
(
select smt,
  performance_region_name,
  max(sale_rank) max_sale_rank,
  max(profit_rank) max_profit_rank,
  max(new_cust_rank )max_new_cust_rank,
  max(overdue_rank) max_overdue_rank,
  max(business_cnt_rnk) max_business_cnt,
  max(business_amt_rnk) max_business_amt
  from tmp_performance
  group by smt,
  performance_region_name
)  
,
tmp_score as (
select 
a.smt
,a.performance_region_name
,sales_user_number
,sales_user_name
,user_position
,sub_position_name
,begin_date
,leader_user_name
--,sales_user_base_profit
,plan_sales_amt
,sale_amt
,sale_achieve_rate
,sale_rank
,sale_weight
,CASE
    WHEN sale_rank = 1 THEN 20
    when sale_rank=max_sale_rank then 0 
    ELSE 20 - (sale_rank - 1) *(20/(max_sale_rank-1) )
  END  AS sale_score
,plan_profit
,profit
,profit_achieve_rate
,profit_rank
,profit_weight
,CASE
    WHEN profit_rank = 1 THEN 20
    when profit_rank=max_profit_rank then 0 
    ELSE 20 - (profit_rank - 1) *(20/(max_profit_rank-1) )
  END  AS profit_score
,new_customer_sale_amt
,new_cust_rank
,new_customer_weight
,CASE
    WHEN new_cust_rank = 1 THEN 20
    when new_cust_rank=max_new_cust_rank then 0 
    ELSE 20 - (new_cust_rank - 1) *(20/(max_new_cust_rank-1) )
  END  AS new_cust_score
,new_customer_profit
,overdue_amount
,receivable_amount
,overdue_rate
,overdue_rank
,overdue_weight
,CASE
    WHEN overdue_rank = 1 THEN 20
    when overdue_rank=max_overdue_rank then 0 
    ELSE 20 - (overdue_rank - 1) *(20/(max_overdue_rank-1) )
  END  AS overdue_score
,middle_customer_cn
,end_customer_cn
,avg_customer_cn
,business_cnt_rnk
,business_cnt_weight
,CASE
    WHEN business_cnt_rnk = 1 THEN 10
    when business_cnt_rnk=max_business_cnt then 0 
    ELSE 10 - (business_cnt_rnk - 1) *(10/(max_business_cnt-1) )
  END  AS business_cnt_score
,middle_contract_amt
,end_contract_amt
,avg_customer_contract_amt
,business_amt_rnk
,business_amt_weight
,CASE
    WHEN business_amt_rnk = 1 THEN 10
    when business_amt_rnk=max_business_amt then 0 
    ELSE 10 - (business_amt_rnk - 1) *(10/(max_business_amt-1) )
  END  AS business_amt_score
from tmp_performance a
left join   tmp_max_rnk c on a.smt=c.smt and a.performance_region_name=c.performance_region_name
) 
select a.smt
,a.performance_region_name
,sales_user_number
,sales_user_name
,user_position
,sub_position_name
,begin_date
,leader_user_name
--,sales_user_base_profit
,case when (total_rank/max(total_rank)over( partition by performance_region_name )<0.1) then '红榜'
        when (low_rank/max(low_rank)over(partition by performance_region_name)<0.1) then '黑榜'
        else '' end  as top_rank
,total_rank
,total_score
,plan_sales_amt
,sale_amt
,sale_achieve_rate
,sale_rank
,sale_weight
,sale_score
,plan_profit
,profit
,profit_achieve_rate
,profit_rank
,profit_weight
,profit_score
,new_customer_sale_amt
,new_cust_rank
,new_customer_weight
,new_cust_score
,new_customer_profit
,overdue_amount
,receivable_amount
,overdue_rate
,overdue_rank
,overdue_weight
,overdue_score
,middle_customer_cn
,end_customer_cn
,avg_customer_cn
,business_cnt_rnk
,business_cnt_weight
,business_cnt_score
,middle_contract_amt
,end_contract_amt
,avg_customer_contract_amt
,business_amt_rnk
,business_amt_weight
,business_amt_score
,lave_customer_cn
,lave_score
from 

(select a.smt
,a.performance_region_name
,sales_user_number
,sales_user_name
,user_position
,a.sub_position_name
,begin_date
,leader_user_name
--,sales_user_base_profit

,dense_rank()over(partition by performance_region_name order by (sale_score+profit_score+new_cust_score+overdue_score+business_cnt_score+business_amt_score+if(lave_customer_cn>0,overdue_score*0.2*-1,0) ) desc ) as total_rank
,dense_rank()over(partition by performance_region_name order by (sale_score+profit_score+new_cust_score+overdue_score+business_cnt_score+business_amt_score+if(lave_customer_cn>0,overdue_score*0.2*-1,0) ) asc  ) as low_rank
,(sale_score+profit_score+new_cust_score+overdue_score+business_cnt_score+business_amt_score) as total_score
,plan_sales_amt
,sale_amt
,sale_achieve_rate
,sale_rank
,sale_weight
,sale_score
,plan_profit
,profit
,profit_achieve_rate
,profit_rank
,profit_weight
,profit_score
,new_customer_sale_amt
,new_cust_rank
,new_customer_weight
,new_cust_score
,new_customer_profit
,overdue_amount
,receivable_amount
,overdue_rate
,overdue_rank
,overdue_weight
,overdue_score
,middle_customer_cn
,end_customer_cn
,avg_customer_cn
,business_cnt_rnk
,business_cnt_weight
,business_cnt_score
,middle_contract_amt
,end_contract_amt
,avg_customer_contract_amt
,business_amt_rnk
,business_amt_weight
,business_amt_score
,lave_customer_cn
,lave_write_off_amount
,if(lave_customer_cn>0,overdue_score*0.2*-1,0) as lave_score
from tmp_score a 
left join 
(select  
  follow_up_user_code,
  follow_up_user_name,
  sub_position_name,
  user_position_name,
  sum(lave_write_off_amount)lave_write_off_amount,
  count(distinct customer_code) as lave_customer_cn  
from
  csx_analyse_tmp.csx_analyse_tmp_hr_red_black_break_contract a
  left join (
    select
      *,
      substr(sdt, 1, 6) sale_month
    from
      csx_analyse_tmp.csx_analyse_tmp_hr_sale_info --  where user_number ='80879367'

  ) b on a.follow_up_user_code = b.user_number -- and a.sale_month=b.sale_month
         where   is_oveder_flag='是'
    group by  follow_up_user_code,
  follow_up_user_name,
  sub_position_name,
  user_position_name) b on b.follow_up_user_code=a.sales_user_number
)a 
order by performance_region_name,
total_rank