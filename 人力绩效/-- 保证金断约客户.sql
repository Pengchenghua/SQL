-- 保证金断约客户 3.0
-- 查找实际中没有包含主的
-- 查找实际中没有包含主的
drop table csx_analyse_tmp.csx_analyse_tmp_incidental;
create table csx_analyse_tmp.csx_analyse_tmp_incidental as 
with temp_company_credit as 
  ( select
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  status,
  is_history_compensate
from
    csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
  -- and status=1
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
  is_history_compensate
) 
--  select * from temp_company_credit where customer_code='243205'

 
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    a.customer_name,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    business_type_name,
    -- business_attribute_name,
    break_contract_date,
    break_contract,
    d.create_time,
    coalesce(business_type_name,f.business_attribute_name,j.business_attribute_name) as new_business_type_name
from (
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    customer_name,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    real_perform_custom2 new_real_customer_code,
    business_type_name,
    break_contract_date,
    break_contract
from (
  select
    belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    receiving_customer_code as customer_code,
    receiving_customer_name as customer_name,
    sum(cast(lave_write_off_amount as decimal(26, 2))) lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    business_type_name,
    break_contract_date,
    break_contract,
    coalesce(real_perform_customer_code,'') real_perform_customer_code,
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code) as new_real_customer_code
  from
     csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di a 
    left join 
    -- 历史业务类型手工导入
    (  select incidental_expenses_no,
      customer_code,
      busniess_type_code business_type_name 
    from csx_analyse_tmp.csx_analyse_tmp_incidental_customer_history) b on a.receiving_customer_code=b.customer_code and a.incidental_expenses_no=b.incidental_expenses_no
  where
    self_employed = 1
    and cast(lave_write_off_amount as decimal(26, 2)) > 0
    and business_scene_code in (2,3)
    -- 是否回款
  --  and break_contract =1
  group by
    belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    receiving_customer_name,
    responsible_person,
    responsible_person_number,
    payment_company_code,
    receiving_customer_code,
    credit_customer_code,
    follow_up_user_code,
    follow_up_user_name	,
    coalesce(real_perform_customer_code,''),
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code),
    business_type_name,
    break_contract_date,
    break_contract
) a 
LATERAL VIEW EXPLODE(
    SPLIT(
      new_real_customer_code, ',')
    )t as real_perform_custom2
)a 
   left join 
   (select customer_code,create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') 
   ) d
   on a.new_real_customer_code=d.customer_code
   left join 
  (select * 
  from  temp_company_credit
    where status=1
    ) f on a.customer_code=f.customer_code and a.credit_customer_code=f.credit_code  and f.company_code=a.payment_company_code
   left join 
  (select * 
  from  temp_company_credit
    where is_history_compensate=1
     and status!=1
    ) j on a.customer_code=j.customer_code and a.credit_customer_code=j.credit_code  and j.company_code=a.payment_company_code
   )
--select * from temp_incidental
;

-- 处理主客户没有在实际履约客户
drop table  csx_analyse_tmp.csx_analyse_tmp_incidental_01;
create table csx_analyse_tmp.csx_analyse_tmp_incidental_01 as 

with temp_company_credit as 
  ( select
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  status,
  is_history_compensate
from
    csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
  -- and status=1
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
  is_history_compensate
) ,
  temp_incidental_01 as 
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    credit_code as new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    break_contract,
    customer_name
from 
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    credit_customer_code credit_code,
    t.business_attribute_name,
    break_contract_date,
    break_contract,
    a.customer_name
from csx_analyse_tmp.csx_analyse_tmp_incidental a 
left join 
  temp_company_credit t on a.customer_code=t.customer_code and a.credit_customer_code=t.credit_code  and t.company_code=a.payment_company_code
 where coalesce(real_perform_customer_code,'')='' 

  union all 

select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    f.credit_code,
    f.business_attribute_name,
    break_contract_date,
    break_contract,
    a.customer_name
from csx_analyse_tmp.csx_analyse_tmp_incidental a 
left join 
  temp_company_credit f on a.new_real_customer_code=f.customer_code and a.new_business_type_name=f.business_attribute_name  and f.company_code=a.payment_company_code
  where coalesce(real_perform_customer_code,'') !='' 
)a
),

temp_incidental_02 as (
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    business_type_name,
    new_business_type_name,
    new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    break_contract,
    customer_name,
    '2' aa
from (
select  a.belong_region_code,
    a.belong_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.payment_company_code,
    a.credit_customer_code,
    a.customer_code,
    a.lave_write_off_amount,
    a.follow_up_user_code,
    a.follow_up_user_name	,
    a.responsible_person,
    a.responsible_person_number,
    a.real_perform_customer_code,
    a.customer_code as new_real_customer_code,
    a.create_time,
    a.business_type_name,
    a.new_business_type_name,
    a.credit_customer_code as new_real_credit_code,
    a.business_attribute_name,
    a.break_contract_date,
    a.customer_name,
    break_contract,
    '2' as aa
from 
  temp_incidental_01 a
left join 
(select credit_customer_code,
        payment_company_code,
        new_real_customer_code,
        lave_write_off_amount
from temp_incidental_01) b on a.payment_company_code=b.payment_company_code 
    and a.credit_customer_code=b.credit_customer_code 
    and a.customer_code=b.new_real_customer_code 
    and a.lave_write_off_amount=b.lave_write_off_amount
where b.new_real_customer_code is null
    and a.new_real_customer_code !=''
) a 
)
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    business_type_name,
    new_business_type_name,
    new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    break_contract,
    customer_name 
from temp_incidental_02
union all
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    business_type_name,
    new_business_type_name,
    new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    break_contract,
    customer_name 
from temp_incidental_01 a 

;


with 
receive_amt as (
select  a.company_code,
    a.customer_code,
    a.credit_code,
    max_paid_date,
    receivable_amount
from 
  (select
    a.company_code,
    a.customer_code,
    credit_code ,
    sum(receivable_amount) receivable_amount
  from
     csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df a
  where
    1=1
   -- and receivable_amount<=0
   -- and a.sdt>=c.max_paid_date
   and sdt='20240924'
  group by
    a.company_code,
    a.customer_code,
    a.credit_code
    ) a 
join 
    (select
          customer_code,
          company_code,
          credit_code,
          regexp_replace(to_date(max(paid_time)),'-','') max_paid_date
        from
          csx_dwd.csx_dwd_sss_close_bill_account_record_di
          where pay_amt>0
        group by
          customer_code,
          company_code,
          credit_code) c on a.company_code=c.company_code and a.customer_code=c.customer_code and a.credit_code=c.credit_code
),
-- 归档合同 
temp_contract_info as 
(select 
    htbh,--  合同编码
    company_code, -- 签约主体
    customer_no,  --  客户编码
    customer_name,
    htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
    ywlx,
	case when ywlx=0 then '日配'
	   when ywlx=1 then '福利'
	   when ywlx=2 then '大宗'
	   when ywlx=3 then '内购批发'
	   when ywlx=4 then 'BBC'
	   when ywlx=5 then 'M端'
	   when ywlx =6 then 'OEM代工'
	   when ywlx=7 then '代仓代配'
	   end business_type_name,
	create_time,
	yue
from 
   (
select 
    t1.htbh,--  合同编码
    wfqyztxz company_code, -- 签约主体
   (case when length(trim(t1.customernumber))>0 then trim(t1.customernumber) else t3.customer_code end) as customer_no,  --  客户编码
    cast(htjey as decimal(26,6)) htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
	coalesce(t1.ywlx,t2.ywlx) as ywlx,
	round(datediff(htzzrq,htqsrq)/30.5,0) yue
from 
   (select * 
   from   csx_ods.csx_ods_ecology_154_uf_xshttzv2_df 
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
   union all 
   -- 旧合同归档
 select htbh,
    company_code,
    customer_no,
    cast(htjey as decimal(26,6)) htjey,
    htqsrq,
    htzzrq,
    ywlx,
    round(datediff(htzzrq,htqsrq)/30.5,0) yue
from csx_analyse.csx_analyse_dws_crm_w_a_uf_xshttz  
        where sdt='20240923'
   )a 
   left join 
   (select customer_code,
        customer_name,
        create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t3 
   on a.customer_no=t3.customer_code
   ),
-- -- 取断约客户
-- 合同结束日期
 business as  
(select  a.company_code,
    business_number,
    credit_code,
    customer_id,
    a.customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    approval_status_name,
    business_stage,
    business_sign_time,
    estimate_contract_amount,
    htjey/10000 htjey,
    if(b.customer_no is not null ,htqsrq, contract_begin_date) contract_begin_date,  --  合同起始日期
	if(b.customer_no is not null ,htzzrq, contract_end_date) contract_end_date, --  合同终止日期
    yue,
    d.create_time,
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    contract_number,
    -- 年化，不足一年按照一年计算，超一年/12
    if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) as tran_year,
    -- 年化金额，先取合同金额再取商机金额
    (if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,b.htjey/10000 ,a.estimate_contract_amount)/if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) ) tran_contract_amount
from 
 ( select business_number,
    customer_id,
    customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    approval_status_code,
    approval_status_name,
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
    credit_code,
    a.company_code
  from   csx_dim.csx_dim_crm_business_info a 
    where sdt='current'
     and status=1
     and business_type_code in (1,2,6)
     and business_stage = 5
 )a 
left join 
-- 可以取最新日期关联合同号
   temp_contract_info b   on b.customer_no=a.customer_code  and b.htbh=a.contract_number  
   left join 
   (select customer_code,create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') 
   ) d
   on a.customer_code=d.customer_code

),
temp_sale as 
(select customer_code,
    credit_code,
    company_code,
    sign_company_code,
    max(sdt) max_sdt
 from csx_dws.csx_dws_sale_detail_di 
   group by 
    customer_code,
    credit_code,
    company_code,
    sign_company_code
)
-- select * from business where customer_code='103207'

,
temp_result as 
(select 
    a.belong_region_name,
    a.performance_province_name,
    a. payment_company_code,
    a.credit_customer_code,
    a.customer_code as sign_customer_code,
    a.new_real_customer_code as customer_code,
    a.customer_name,
    create_time,
    e.sales_user_number,
    e.sales_user_name,
    a.new_business_type_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
    a.new_real_credit_code,
    b.max_paid_date  as receive_sdt,
    coalesce(regexp_replace(to_date(c.contract_end_date),'-','') , regexp_replace(to_date(f.contract_end_date),'-',''),regexp_replace(to_date(h.contract_end_date),'-','')) contract_end_date,
    coalesce(regexp_replace(to_date(a.break_contract_date),'-',''),'') break_contract_date,
    max_sdt max_sale_sdt,
    receivable_amount
from 
    (select  belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name,
        payment_company_code,
        credit_customer_code,
        a.customer_code,
        lave_write_off_amount,
        follow_up_user_code,
        follow_up_user_name	,
        responsible_person,
        responsible_person_number,
        real_perform_customer_code,
        new_real_customer_code,
        create_time,
        new_business_type_name,
        new_real_credit_code,
        business_attribute_name,
        break_contract_date,
        customer_name
    from csx_analyse_tmp.csx_analyse_tmp_incidental_01 a
        where break_contract!=1
    )a  
left join 
    (select * from receive_amt where receivable_amount<=0) b on a.new_real_customer_code=b.customer_code and a.payment_company_code=b.company_code and a.new_real_credit_code=b.credit_code

left join 
(select * from 
(select
  company_code,
  customer_code,
  credit_code,
  contract_end_date,
  contract_begin_date,
  business_attribute_code	,
  business_attribute_name	,
  business_type_code, 
  row_number() over(partition by customer_code,credit_code,company_code,business_attribute_code order by contract_end_date desc) rn
from business
  where create_time>='2023-02-09'
 )a 
  where rn=1 
 )c  on a.new_real_customer_code=c.customer_code and a.payment_company_code=c.company_code and a.new_real_credit_code=c.credit_code 
  -- 客户创建时间23年2月9号，关联按照客户+公司+日配+业务
  left join 
(select * from 
(select
  company_code,
  customer_no ,
  htqsrq contract_begin_date,
  htzzrq contract_end_date,
  business_type_name, 
  row_number() over(partition by customer_no,company_code,business_type_name order by coalesce(htzzrq,'') desc) rn
from temp_contract_info a 
-- left join 
-- temp_contract_info b on a.customer_code=b.customer_code and a.company_code=b.company_code and a.
 where create_time<'2023-02-09'
 )a 
  where rn=1 
  )f  on a.new_real_customer_code=f.customer_no and a.payment_company_code=f.company_code  and f.business_type_name=a.new_business_type_name
left join 
(select * from 
(select
  company_code,
  customer_no ,
  htqsrq contract_begin_date,
  htzzrq contract_end_date,
  business_type_name, 
  row_number() over(partition by customer_no,company_code,business_type_name order by coalesce(htzzrq,'') desc) rn
from temp_contract_info a 
 )a 
  where rn=1 
  )h  on a.new_real_customer_code=h.customer_no and a.payment_company_code=h.company_code  and h.business_type_name=a.new_business_type_name
 left join 
(select customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        performance_province_name,
        performance_city_name
from   csx_dim.csx_dim_crm_customer_info
where sdt='current') e on a.new_real_customer_code=e.customer_code
left join temp_sale j on a.new_real_customer_code=j.customer_code and a.new_real_credit_code=j.credit_code and a.payment_company_code=j.sign_company_code
 -- where a.new_real_customer_code='112189'
)

-- select 
--     a.belong_region_name,
--     a.performance_province_name,
--     a.payment_company_code,
--     a.credit_customer_code,
--     a.sign_customer_code,
    
--     a.customer_name,
--   -- a.create_time,
--     -- b.sales_user_number,
--     -- b.sales_user_name,
--     a.new_business_type_name,
--     a.responsible_person,
--     a.responsible_person_number,
--     a.lave_write_off_amount,
    
--     max(a.receive_sdt) receive_sdt,
--     max(a.contract_end_date) contract_end_date,
--     max(a.break_contract_date) break_contract_date,
--     max(max_sdt) max_sdt,
--     max(a.max_sale_sdt) max_sale_sdt,
--     b.receivable_amount,
--     if(b.receivable_amount>0,'否','是') is_receive_oveder_flag,
--   case when date_add(from_unixtime(unix_timestamp(max(max_sdt),'yyyyMMdd'),'yyyy-MM-dd'),30)>'2024-09-25' or b.receivable_amount>0 then '否'
--     when date_add(from_unixtime(unix_timestamp(max(max_sdt),'yyyyMMdd'),'yyyy-MM-dd'),30)<='2024-09-25' and b.receivable_amount < 0 then '是'
--     else '否' end  as is_oveder_flag,
--     concat_ws(',',collect_set(a.customer_code) ) as full_real_customer_code ,
--     concat_ws(',',collect_set(a.new_real_credit_code) ) full_new_real_credit_code
--  from (
select * ,
  if(receivable_amount>0,'否','是') is_receive_oveder_flag,
  case when date_add(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'),'yyyy-MM-dd'),30)>'2024-09-25' or receivable_amount>0 then '否'
    when date_add(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'),'yyyy-MM-dd'),30)<='2024-09-25' and receivable_amount < 0 then '是'
    else '否' end  as is_oveder_flag
  
from 
(
select 
    a.belong_region_name,
    a.performance_province_name,
    a.payment_company_code,
    a.credit_customer_code,
    a.sign_customer_code,
    a.customer_code,
    a.customer_name,
    a.create_time,
    a.sales_user_number,
    a.sales_user_name,
    a.new_business_type_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
    a.new_real_credit_code,
    a.receive_sdt,
    a.contract_end_date,
    a.break_contract_date,
    sort_array(array(receive_sdt,contract_end_date,break_contract_date))[size(array(receive_sdt,contract_end_date,break_contract_date))-1] as max_sdt,
    a.max_sale_sdt,
    b.receivable_amount
 from temp_result a 
 left join 
 (select * from receive_amt ) b on a.customer_code=b.customer_code and a.payment_company_code=b.company_code and a.new_real_credit_code=b.credit_code
)a
   
 )a 
  left join 
   
    (select * from receive_amt  ) b on a.sign_customer_code=b.customer_code and a.payment_company_code=b.company_code and a.credit_customer_code=b.credit_code
group by a.belong_region_name,
    a.performance_province_name,
    a.payment_company_code,
    a.credit_customer_code,
    a.sign_customer_code,
    
    a.customer_name,
   -- a.create_time,
    -- b.sales_user_number,
    -- b.sales_user_name,
    a.new_business_type_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
     b.receivable_amount
 ;



-- 保证金断约客户
with incidental as (
select  payment_company_code,
    credit_customer_code,
    customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    real_perform_custom2 new_real_customer_code,
    business_type_name
from (
  select
    payment_company_code,
    credit_customer_code,
    receiving_customer_code as customer_code,
    sum(cast(lave_write_off_amount as decimal(26, 2))) lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    business_type_name,
    coalesce(real_perform_customer_code,'') real_perform_customer_code,
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code) as new_real_customer_code
  from
          csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di a 
        left join 
        (  select incidental_expenses_no,
          customer_code,
          busniess_type_code business_type_name 
        from csx_analyse_tmp.csx_analyse_tmp_incidental_customer_history) b on a.receiving_customer_code=b.customer_code and a.incidental_expenses_no=b.incidental_expenses_no
  where
    self_employed = 1
    and cast(lave_write_off_amount as decimal(26, 2)) > 0
    and business_scene_code in (2,3)
  group by
    responsible_person,
    responsible_person_number,
    payment_company_code,
    receiving_customer_code,
    credit_customer_code,
    follow_up_user_code,
    follow_up_user_name	,
    coalesce(real_perform_customer_code,''),
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code),
    business_type_name
) a 
LATERAL VIEW EXPLODE(
    SPLIT(
      new_real_customer_code, ',')
    )t as real_perform_custom2
),
break_contract_info as 
(select payment_company_code,
    credit_customer_code,
    customer_code,
    responsible_person_number,
    break_contract,
    break_contract_date,
    real_perform_custom2 new_real_customer_code,
    business_type_name,
    rn
from 
 ( select
    payment_company_code,
    credit_customer_code,
    receiving_customer_code as customer_code,
    responsible_person,
    responsible_person_number,
    break_contract,
    break_contract_date,
    business_type_name,
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code) new_real_customer_code,
    row_number()over(partition by payment_company_code,credit_customer_code,receiving_customer_code order by break_contract_date desc ) rn 
 from
        csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di a 
         left join 
        (  select incidental_expenses_no,customer_code,busniess_type_code business_type_name from csx_analyse_tmp.csx_analyse_tmp_incidental_customer_history) b on a.receiving_customer_code=b.customer_code and a.incidental_expenses_no=b.incidental_expenses_no
  where
    self_employed = 1
    and cast(lave_write_off_amount as decimal(26, 2)) > 0
    and business_scene_code in (2,3)

  )a 
  LATERAL VIEW EXPLODE(
    SPLIT(
      new_real_customer_code, ',')
    )t as real_perform_custom2
    where rn=1 
)
,
-- 应收小于0，客户未有应收属于停止合作，判断最晚核销日期，根据核销日期小于SAP应收快照日期
receive_amt as (
  select
    a.company_code,
    a.customer_code,
    credit_code,
    min(sdt) receive_sdt
  from
     csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df a
    join (
      select
        distinct customer_code,
        payment_company_code,
        credit_customer_code,
        real_perform_customer_code
      from
        incidental
    ) b on a.customer_code=b.real_perform_customer_code
       and b.payment_company_code=a.company_code 
       and a.credit_code=b.credit_customer_code
    left join 
    (select
          customer_code,
          company_code,
          credit_code,
          regexp_replace(to_date(max(paid_time)),'-','') max_paid_date
        from
          csx_dwd.csx_dwd_sss_close_bill_account_record_di
          where pay_amt>0
        group by
          customer_code,
          company_code,
          credit_code) c on a.company_code=c.company_code and a.customer_code=c.customer_code and a.credit_code=c.credit_code
  where
    1=1
    and receivable_amount<=0
    and a.sdt>=c.max_paid_date
  group by
    a.company_code,
    a.customer_code,
    a.credit_code
),
-- -- 取断约客户
-- 合同结束日期
 business as  
(select  company_code,
    business_number,
    credit_code,
    customer_id,
    a.customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    approval_status_name,
    business_stage,
    business_sign_time,
    estimate_contract_amount,
    htjey/10000 htjey,
    if(b.customer_no is not null ,htqsrq, contract_begin_date) contract_begin_date,  --  合同起始日期
	  if(b.customer_no is not null ,htzzrq, contract_end_date) contract_end_date, --  合同终止日期
    yue,
    d.create_time,
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    contract_number,
    -- 年化，不足一年按照一年计算，超一年/12
    if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) as tran_year,
    -- 年化金额，先取合同金额再取商机金额
    (if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,b.htjey/10000 ,a.estimate_contract_amount)/if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) ) tran_contract_amount
from 
 ( select business_number,
    customer_id,
    customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    approval_status_code,
    approval_status_name,
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
    credit_code,
    company_code
  from   csx_dim.csx_dim_crm_business_info a 
    where sdt='current'
     and status=1
     and business_type_code in (1,2,6)
     and business_stage = 5
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
   from   csx_ods.csx_ods_ecology_154_uf_xshttzv2_df 
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
   left join 
   (select customer_code,create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') 
   ) d
   on a.customer_code=d.customer_code

)
-- select * from business where customer_code='103207'

select 
    e.performance_province_name,
    e.performance_city_name,
    a. payment_company_code,
    a.credit_customer_code,
    a.customer_code as sign_customer_code,
    a.new_real_customer_code as customer_code,
    e.customer_name,
    e.sales_user_number,
    e.sales_user_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
    coalesce(b.receive_sdt,'')receive_sdt,
    if (coalesce(regexp_replace(to_date(c.contract_end_date),'-',''),'')!='' ,  regexp_replace(to_date(c.contract_end_date),'-','') , regexp_replace(to_date(f.contract_end_date),'-','')) contract_end_date,
    if(d.break_contract=1,1,0) break_contract,
    coalesce(regexp_replace(to_date(d.break_contract_date),'-',''),'') break_contract_date,
   sort_array(array(b.receive_sdt,regexp_replace(to_date(c.contract_end_date),'-',''),regexp_replace(to_date(d.break_contract_date),'-','')))[size(array(b.receive_sdt,regexp_replace(to_date(c.contract_end_date),'-',''),regexp_replace(to_date(d.break_contract_date),'-','')))-1] as max_sdt
from incidental a 
left join 
receive_amt b on a.new_real_customer_code=b.customer_code and a.payment_company_code=b.company_code and a.credit_customer_code=b.credit_code

left join 
(select * from 
(select
  company_code,
  customer_code,
  credit_code,
  contract_end_date,
  contract_begin_date,
  business_attribute_code	,
  business_attribute_name	,
  business_type_code, 
  row_number() over(partition by customer_code,credit_code,company_code order by contract_end_date desc) rn
from business
  where create_time>='2023-02-09'
 )a 
  where rn=1 
 )c  on a.new_real_customer_code=c.customer_code and a.payment_company_code=c.company_code and a.credit_customer_code=c.credit_code 
  -- 客户创建时间23年2月9号，关联按照客户+公司+日配
  left join 
(select * from 
(select
  company_code,
  customer_code,
  credit_code,
  contract_end_date,
  contract_begin_date,
  business_attribute_code	,
  business_attribute_name	,
  business_type_code, 
  row_number() over(partition by customer_code,company_code,business_attribute_code order by contract_end_date desc) rn
from business
 where create_time<'2023-02-09'
 )a 
  where rn=1 
  )f  on a.new_real_customer_code=f.customer_code and a.payment_company_code=f.company_code  and f.business_attribute_name=a.business_type_name
left join break_contract_info d on a.new_real_customer_code=d.new_real_customer_code and a.payment_company_code=d.payment_company_code and a.credit_customer_code=d.credit_customer_code
left join 
(select customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        performance_province_name,
        performance_city_name
from   csx_dim.csx_dim_crm_customer_info
where sdt='current') e on a.new_real_customer_code=e.customer_code
-- where a.customer_code='232833'
;

--
--1、按照有有实际履约与无实际履约的客户
--2、

-- 2.0版本
-- 保证金断约客户
-- 保证金断约客户
with temp_company_credit as 
  ( select
  customer_code,
  credit_code,
  customer_name,
  business_attribute_code,
  business_attribute_name,
  company_code,
  status,
  is_history_compensate
from
    csx_dim.csx_dim_crm_customer_company_details
where
  sdt = 'current'
  -- and status=1
group by customer_code,
    credit_code,
    customer_name,
    business_attribute_code,
    business_attribute_name,
    company_code,
    status,
  is_history_compensate
) 
--  select * from temp_company_credit where customer_code='243205'

 ,
 temp_incidental as 
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    a.customer_name,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    business_type_name,
    -- business_attribute_name,
    break_contract_date,
    break_contract,
    d.create_time,
    coalesce(business_type_name,f.business_attribute_name,j.business_attribute_name) as new_business_type_name
from (
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    customer_code,
    customer_name,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    real_perform_custom2 new_real_customer_code,
    business_type_name,
    break_contract_date,
    break_contract
from (
  select
    belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    receiving_customer_code as customer_code,
    receiving_customer_name as customer_name,
    sum(cast(lave_write_off_amount as decimal(26, 2))) lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    business_type_name,
    break_contract_date,
    break_contract,
    coalesce(real_perform_customer_code,'') real_perform_customer_code,
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code) as new_real_customer_code
  from
     csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di a 
    left join 
    -- 历史业务类型手工导入
    (  select incidental_expenses_no,
      customer_code,
      busniess_type_code business_type_name 
    from csx_analyse_tmp.csx_analyse_tmp_incidental_customer_history) b on a.receiving_customer_code=b.customer_code and a.incidental_expenses_no=b.incidental_expenses_no
  where
    self_employed = 1
    and cast(lave_write_off_amount as decimal(26, 2)) > 0
    and business_scene_code in (2,3)
    -- 是否回款
    and break_contract =1
  group by
    belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    receiving_customer_name,
    responsible_person,
    responsible_person_number,
    payment_company_code,
    receiving_customer_code,
    credit_customer_code,
    follow_up_user_code,
    follow_up_user_name	,
    coalesce(real_perform_customer_code,''),
    if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code),
    business_type_name,
    break_contract_date,
    break_contract
) a 
LATERAL VIEW EXPLODE(
    SPLIT(
      new_real_customer_code, ',')
    )t as real_perform_custom2
)a 
   left join 
   (select customer_code,create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') 
   ) d
   on a.new_real_customer_code=d.customer_code
   left join 
  (select * 
  from  temp_company_credit
    where status=1
    ) f on a.customer_code=f.customer_code and a.credit_customer_code=f.credit_code  and f.company_code=a.payment_company_code
   left join 
  (select * 
  from  temp_company_credit
    where is_history_compensate=1
     and status!=1
    ) j on a.customer_code=j.customer_code and a.credit_customer_code=j.credit_code  and j.company_code=a.payment_company_code
   )
--select * from temp_incidental
,
temp_incidental_01 as 
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    credit_code as new_real_credit_code,
    business_attribute_name,
    break_contract_date,
    customer_name
from 
(
select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    credit_customer_code credit_code,
    t.business_attribute_name,
    break_contract_date,
    a.customer_name
from temp_incidental a 
left join 
  temp_company_credit t on a.customer_code=t.customer_code and a.credit_customer_code=t.credit_code  and t.company_code=a.payment_company_code
 where coalesce(real_perform_customer_code,'')='' 

union all 

select  belong_region_code,
    belong_region_name,
    performance_province_code,
    performance_province_name,
    payment_company_code,
    credit_customer_code,
    a.customer_code,
    lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number,
    real_perform_customer_code,
    new_real_customer_code,
    create_time,
    a.business_type_name,
    new_business_type_name,
    f.credit_code,
    f.business_attribute_name,
    break_contract_date,
    a.customer_name
from temp_incidental a 
left join 
  temp_company_credit f on a.new_real_customer_code=f.customer_code and a.new_business_type_name=f.business_attribute_name  and f.company_code=a.payment_company_code
  where coalesce(real_perform_customer_code,'') !='' 
)a 
)
-- select * from temp_incidental_01 where new_real_customer_code='112189'
 ,
-- break_contract_info as 
-- (select payment_company_code,
--     credit_customer_code,
--     customer_code,
--     responsible_person_number,
--     break_contract,
--     break_contract_date,
--     real_perform_custom2 new_real_customer_code,
--     business_type_name,
--     rn
-- from 
--  ( select
--     payment_company_code,
--     credit_customer_code,
--     receiving_customer_code as customer_code,
--     responsible_person,
--     responsible_person_number,
--     break_contract,
--     break_contract_date,
--     business_type_name,
--     if(coalesce(real_perform_customer_code,'') !='', real_perform_customer_code,receiving_customer_code) new_real_customer_code,
--     row_number()over(partition by payment_company_code,credit_customer_code,receiving_customer_code order by break_contract_date desc ) rn 
--  from
--         csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di a 
--          left join 
--         (  select incidental_expenses_no,
--             customer_code,
--             busniess_type_code business_type_name 
--         from csx_analyse_tmp.csx_analyse_tmp_incidental_customer_history) b on a.receiving_customer_code=b.customer_code and a.incidental_expenses_no=b.incidental_expenses_no
--   where
--     self_employed = 1
--     and cast(lave_write_off_amount as decimal(26, 2)) > 0
--     and business_scene_code in (2,3)
--     and break_contract=1
--   )a 
--   LATERAL VIEW EXPLODE(
--     SPLIT(
--       new_real_customer_code, ',')
--     )t as real_perform_custom2
--     where rn=1 
-- )


-- ,
-- 应收小于0，客户未有应收属于停止合作，判断最晚核销日期，根据核销日期小于SAP应收快照日期
receive_amt as (
select  a.company_code,
    a.customer_code,
    a.credit_code,
    max_paid_date,
    receivable_amount
from 
  (select
    a.company_code,
    a.customer_code,
    credit_code ,
    sum(receivable_amount) receivable_amount
  from
     csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df a
  where
    1=1
   -- and receivable_amount<=0
   -- and a.sdt>=c.max_paid_date
   and sdt='20240924'
  group by
    a.company_code,
    a.customer_code,
    a.credit_code
    ) a 
join 
    (select
          customer_code,
          company_code,
          credit_code,
          regexp_replace(to_date(max(paid_time)),'-','') max_paid_date
        from
          csx_dwd.csx_dwd_sss_close_bill_account_record_di
          where pay_amt>0
        group by
          customer_code,
          company_code,
          credit_code) c on a.company_code=c.company_code and a.customer_code=c.customer_code and a.credit_code=c.credit_code
),
-- 归档合同 
temp_contract_info as 
(select 
    htbh,--  合同编码
    company_code, -- 签约主体
    customer_no,  --  客户编码
    customer_name,
    htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
    ywlx,
	case when ywlx=0 then '日配'
	   when ywlx=1 then '福利'
	   when ywlx=2 then '大宗'
	   when ywlx=3 then '内购批发'
	   when ywlx=4 then 'BBC'
	   when ywlx=5 then 'M端'
	   when ywlx =6 then 'OEM代工'
	   when ywlx=7 then '代仓代配'
	   end business_type_name,
	create_time,
	yue
from 
   (
select 
    t1.htbh,--  合同编码
    wfqyztxz company_code, -- 签约主体
   (case when length(trim(t1.customernumber))>0 then trim(t1.customernumber) else t3.customer_code end) as customer_no,  --  客户编码
    cast(htjey as decimal(26,6)) htjey, --  合同金额（元）
	htqsrq,  --  合同起始日期
	htzzrq,  --  合同终止日期
	coalesce(t1.ywlx,t2.ywlx) as ywlx,
	round(datediff(htzzrq,htqsrq)/30.5,0) yue
from 
   (select * 
   from   csx_ods.csx_ods_ecology_154_uf_xshttzv2_df 
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
   union all 
   -- 旧合同归档
 select htbh,
    company_code,
    customer_no,
    cast(htjey as decimal(26,6)) htjey,
    htqsrq,
    htzzrq,
    ywlx,
    round(datediff(htzzrq,htqsrq)/30.5,0) yue
from csx_analyse.csx_analyse_dws_crm_w_a_uf_xshttz  
        where sdt='20240923'
   )a 
   left join 
   (select customer_code,
        customer_name,
        create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') ) t3 
   on a.customer_no=t3.customer_code
   ),
-- -- 取断约客户
-- 合同结束日期
 business as  
(select  a.company_code,
    business_number,
    credit_code,
    customer_id,
    a.customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    approval_status_name,
    business_stage,
    business_sign_time,
    estimate_contract_amount,
    htjey/10000 htjey,
    if(b.customer_no is not null ,htqsrq, contract_begin_date) contract_begin_date,  --  合同起始日期
	if(b.customer_no is not null ,htzzrq, contract_end_date) contract_end_date, --  合同终止日期
    yue,
    d.create_time,
    contract_cycle_int,
    contract_cycle_desc,
    business_type_code,
    contract_number,
    -- 年化，不足一年按照一年计算，超一年/12
    if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) as tran_year,
    -- 年化金额，先取合同金额再取商机金额
    (if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,b.htjey/10000 ,a.estimate_contract_amount)/if(if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0)) > 12 ,if(b.htbh is not null and coalesce(b.htjey ,0) <> 0 ,yue ,regexp_extract(a.contract_cycle_desc ,'[0-9]+' ,0))/12,1) ) tran_contract_amount
from 
 ( select business_number,
    customer_id,
    customer_code,
    owner_user_number,
    owner_user_name	,
    owner_user_position,
    owner_province_id,
    owner_province_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    owner_city_code,
    owner_city_name	,
    business_attribute_code	,
    business_attribute_name	,
    approval_status_code,
    approval_status_name,
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
    credit_code,
    a.company_code
  from   csx_dim.csx_dim_crm_business_info a 
    where sdt='current'
     and status=1
     and business_type_code in (1,2,6)
     and business_stage = 5
 )a 
left join 
-- 可以取最新日期关联合同号
   temp_contract_info b   on b.customer_no=a.customer_code  and b.htbh=a.contract_number  
   left join 
   (select customer_code,create_time 
   from csx_dim.csx_dim_crm_customer_info  
   where sdt=regexp_replace(date_sub(current_date,1),'-','') 
   ) d
   on a.customer_code=d.customer_code

),
temp_sale as 
(select customer_code,
    credit_code,
    company_code,
    sign_company_code,
    max(sdt) max_sdt
 from csx_dws.csx_dws_sale_detail_di 
   group by 
    customer_code,
    credit_code,
    company_code,
    sign_company_code
)
-- select * from business where customer_code='103207'

,
temp_result as 
(select 
    a.belong_region_name,
    a.performance_province_name,
    a. payment_company_code,
    a.credit_customer_code,
    a.customer_code as sign_customer_code,
    a.new_real_customer_code as customer_code,
    a.customer_name,
    create_time,
    e.sales_user_number,
    e.sales_user_name,
    a.new_business_type_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
    a.new_real_credit_code,
    b.max_paid_date  as receive_sdt,
    coalesce(regexp_replace(to_date(c.contract_end_date),'-','') , regexp_replace(to_date(f.contract_end_date),'-',''),regexp_replace(to_date(h.contract_end_date),'-','')) contract_end_date,
    coalesce(regexp_replace(to_date(a.break_contract_date),'-',''),'') break_contract_date,
    max_sdt max_sale_sdt,
    receivable_amount
from 
    (select  belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name,
        payment_company_code,
        credit_customer_code,
        a.customer_code,
        lave_write_off_amount,
        follow_up_user_code,
        follow_up_user_name	,
        responsible_person,
        responsible_person_number,
        real_perform_customer_code,
        new_real_customer_code,
        create_time,
        new_business_type_name,
        new_real_credit_code,
        business_attribute_name,
        break_contract_date,
        customer_name
    from temp_incidental_01 a
    )a  
left join 
    (select * from receive_amt where receivable_amount<=0) b on a.new_real_customer_code=b.customer_code and a.payment_company_code=b.company_code and a.new_real_credit_code=b.credit_code

left join 
(select * from 
(select
  company_code,
  customer_code,
  credit_code,
  contract_end_date,
  contract_begin_date,
  business_attribute_code	,
  business_attribute_name	,
  business_type_code, 
  row_number() over(partition by customer_code,credit_code,company_code,business_attribute_code order by contract_end_date desc) rn
from business
  where create_time>='2023-02-09'
 )a 
  where rn=1 
 )c  on a.new_real_customer_code=c.customer_code and a.payment_company_code=c.company_code and a.new_real_credit_code=c.credit_code 
  -- 客户创建时间23年2月9号，关联按照客户+公司+日配+业务
  left join 
(select * from 
(select
  company_code,
  customer_no ,
  htqsrq contract_begin_date,
  htzzrq contract_end_date,
  business_type_name, 
  row_number() over(partition by customer_no,company_code,business_type_name order by coalesce(htzzrq,'') desc) rn
from temp_contract_info a 
-- left join 
-- temp_contract_info b on a.customer_code=b.customer_code and a.company_code=b.company_code and a.
 where create_time<'2023-02-09'
 )a 
  where rn=1 
  )f  on a.new_real_customer_code=f.customer_no and a.payment_company_code=f.company_code  and f.business_type_name=a.new_business_type_name
left join 
(select * from 
(select
  company_code,
  customer_no ,
  htqsrq contract_begin_date,
  htzzrq contract_end_date,
  business_type_name, 
  row_number() over(partition by customer_no,company_code,business_type_name order by coalesce(htzzrq,'') desc) rn
from temp_contract_info a 
 )a 
  where rn=1 
  )h  on a.new_real_customer_code=h.customer_no and a.payment_company_code=h.company_code  and h.business_type_name=a.new_business_type_name
 left join 
(select customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        performance_province_name,
        performance_city_name
from   csx_dim.csx_dim_crm_customer_info
where sdt='current') e on a.new_real_customer_code=e.customer_code
left join temp_sale j on a.new_real_customer_code=j.customer_code and a.new_real_credit_code=j.credit_code and a.payment_company_code=j.sign_company_code
 -- where a.new_real_customer_code='112189'
)
select * ,
  if(receivable_amount>0,'否','是') is_receive_oveder_flag,
  case when date_add(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'),'yyyy-MM-dd'),30)>'2024-09-25' or receivable_amount>0 then '否'
    when date_add(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'),'yyyy-MM-dd'),30)<='2024-09-25' and receivable_amount < 0 then '是'
    else '否' end  as is_oveder_flag,
    row_number() over(partition by sign_customer_code,credit_customer_code,payment_company_code order by max_sdt desc) as rn
  
from 
(
select 
    a.belong_region_name,
    a.performance_province_name,
    a.payment_company_code,
    a.credit_customer_code,
    a.sign_customer_code,
    a.customer_code,
    a.customer_name,
    a.create_time,
    a.sales_user_number,
    a.sales_user_name,
    a.new_business_type_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
    a.new_real_credit_code,
    a.receive_sdt,
    a.contract_end_date,
    a.break_contract_date,
    sort_array(array(receive_sdt,contract_end_date,break_contract_date))[size(array(receive_sdt,contract_end_date,break_contract_date))-1] as max_sdt,
    
    a.max_sale_sdt,
    b.receivable_amount
 from temp_result a 
 left join 
 (select * from receive_amt ) b on a.customer_code=b.customer_code and a.payment_company_code=b.company_code and a.new_real_credit_code=b.credit_code
)a 
 ;

ywlx	
-- 旧表
0日配
1福利
2大宗
3内购批发
4BBC
5M端
6OEM代工
7代仓代配

-- 新表
0 日配
1 福利
2 大宗贸易
3 内购
4 BBC
5 M端