-- 服务管家红黑榜
-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_service_performance ;

-- 销售BD数据
-- drop table  csx_analyse_tmp.csx_analyse_tmp_hr_sale_detail ;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_detail as
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
        business_attribute_code,
        first_business_sale_date ,
        last_business_sale_date,
        next_sale_date,
        sum(sale_amt) sale_amt,
        sum(profit) profit,
        min(sdt) min_sdt,
        max(sdt) max_sdt
    from csx_dws.csx_dws_sale_detail_di a
    left join 
    (select customer_code,
        business_type_code,
        business_attribute_code,
        first_business_sale_date ,
        last_business_sale_date,
        regexp_replace(cast(date_add(from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd'),'yyyy-MM-dd'),90) as string),'-','') as next_sale_date
    from  csx_dws.csx_dws_crm_customer_business_active_di 
    where sdt='current'
    ) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
    where sdt >= '20240601'
        and sdt <= '20240630'   
        and a.business_type_code in ('1','2','6')  -- 1-日配、2-福利、6-BBC
    group by substr(sdt, 1, 6) ,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        business_attribute_code,
        first_business_sale_date ,
        last_business_sale_date,
        next_sale_date
;

-- 管家销售额毛利额

-- 管家销售额毛利额
with sales_info as 
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
    where sdt= '20240731'
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
        where smonth in ('202407')
        union all
        select customer_no,business_type_code from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
        where smonth in  ('202407')
         )a) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
    where sdt >= '20240701'
        and sdt <= '20240731'   
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
-- 断约客户明细 90天未履约      
with sale as 
(select a.*,b.max_sdt as max_sale_sdt,
    b.next_sale_date b_nex_sale_date,
    if((sale_amt>0 and b.next_sale_date<='20240630' and a.next_sale_date!=b.next_sale_date and a.min_sdt>=c.business_sign_date) 
        or (sale_month= substr(first_business_sale_date,1,6)),1,0 ) as new_customer 

from csx_analyse_tmp.csx_analyse_tmp_hr_sale_detail a 
left join 
(select * from 
(select
  business_number,
  customer_code,
  business_attribute_code,
  business_sign_time,
  regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
  row_number()over(partition by customer_code order by business_sign_time desc ) as rn
from
  `csx_dim`.`csx_dim_crm_business_info`
where
  sdt = 'current'
  and to_date(business_sign_time) <= '2024-06-30'
  and to_date(business_sign_time) >=date_add(from_unixtime(unix_timestamp('2024-06-30','yyyy-MM-dd'),'yyyy-MM-dd'),-90)
  and business_attribute_code=1
  and status=1 
  and business_stage=5
  )a 
  where rn=1 
) c on a.customer_code=c.customer_code and a.business_attribute_code=c.business_attribute_code
left join 
(select      '1' as type,
		    performance_province_name,
		    business_type_code,
			customer_code,
			max(sdt) max_sdt,
			regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd'),'yyyy-MM-dd'),90) as string),'-','') as next_sale_date
		from 
		(select 
		    performance_province_name,
		    business_type_code,
		    sdt,
			customer_code,
			sum(sale_amt)sale_amt
		from 
			csx_dws.csx_dws_sale_detail_di 
		where 
			sdt between '20240101' and '20240630'
			and business_type_code=1           --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--	and channel_code in('1','7','9')    --  渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and order_channel_code not in (4,6)
		--	and customer_code='103145'
		-- 	and sdt='20240201'
		group by 
			performance_province_name,
		    business_type_code,
			customer_code,
			sdt
			)a 
		where sale_amt>0
		group by  performance_province_name,
		    business_type_code,
			customer_code
union all 
select  '2'type,
        performance_province_name,
        business_type_code,
        customer_code,
        last_business_sale_date,
        regexp_replace(cast(date_add(from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd'),'yyyy-MM-dd'),90) as string),'-','') as next_sale_date
    from  csx_dws.csx_dws_crm_customer_business_active_di 
    where sdt='current'
        and last_business_sale_date <='20240101'
        and business_type_code=1
) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
   )
   select * from sale where new_customer=1

-- 断约客户明细 90天未履约      
select 	performance_province_name, 
    business_type_code,
		after_date, 
		a.customer_code,
		max_sdt
from
		(
		select 
		    performance_province_name,
		    business_type_code,
			customer_code,
			max(sdt) max_sdt,
			regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd'),'yyyy-MM-dd'),90) as string),'-','') as after_date
		from 
			csx_dws.csx_dws_sale_detail_di 
		where 
			sdt between '20240101' and '20240630'
			and business_type_code=1           --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9')    --  渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and order_channel_code not in (4,6)
		group by 
			performance_province_name,
		    business_type_code,
			customer_code
		) a
	   where 1=1
	    and after_date<='20240630'
			group by performance_province_name, 
			business_type_code,
			after_date, 
			a.customer_code,
			max_sdt 
;
-- 销售员信息表
--drop table csx_analyse_tmp.csx_analyse_tmp_hr_sale_info;

create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_info as 
select a.user_id,
  a.user_number,
  a.user_name,
  coalesce(a.user_position,source_user_position)user_position ,
  replace(c.name,'（旧）','') user_position_name,
  a.begin_date,
  a.source_user_position,
  a.leader_user_id,
  a.province_name,
  a.city_name,
  b.user_number leader_user_number,
  b.user_name leader_user_name,
  b.user_position_type leader_user_position,
  b.user_position leader_source_user_position
from 
 (select
  user_id,
  user_number,
  user_name,
  coalesce(user_position,source_user_position)  user_position,
  begin_date,
  source_user_position,
  if(a.user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),user_id, leader_user_id) leader_user_id,
  province_name,
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
   and status=0 
 -- and (user_position like 'SALES%'
  )a 
  left join 
  (  select * from 
    (SELECT
      *,
      row_number() over(PARTITION BY user_id ORDER BY distance asc) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt = 'current'
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    where rank=1)b  on   a.leader_user_id=b.user_id
 left join 
 (select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt='20240821'
       and dic_type = 'POSITION'
    ) c on a.user_position	=c.code
    ;
    
-- 1.0 销售&毛利&新客
create table csx_analyse_tmp.csx_analyse_tmp_hr_sale as 
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
        where smonth in ('202407')
        union all
        select customer_no,business_type_code from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
        where smonth in  ('202407')
         )a) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
    where sdt >= '20240701'
        and sdt <= '20240731'   
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
      ;

-- 00 用户信息

create table csx_analyse_tmp.csx_analyse_tmp_hr_sale_info as 
select a.user_id,
  a.user_number,
  a.user_name,
  a.user_position,
  a.begin_date,
  a.source_user_position,
  a.leader_user_id,
  a.province_name,
  a.city_name,
  b.user_number leader_user_number,
  b.user_name leader_user_name,
  b.user_position_type leader_user_position,
  b.user_position leader_source_user_position
from 
 (select
  user_id,
  user_number,
  user_name,
  coalesce(user_position,source_user_position)  user_position,
  begin_date,
  source_user_position,
  if(a.user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),user_id, leader_user_id) leader_user_id,
  province_name,
  city_name
  from 
  csx_dim.csx_dim_uc_user a 
  left  join 
    (select employee_name,
        employee_code,
        begin_date,
        record_type_name
    from csx_dim.csx_dim_basic_employee 
        where sdt='current' 
      --  and record_type_code	!=4
    )b on a.user_number=b.employee_code
    where
    sdt = 'current'
   and status=0 
 -- and (user_position like 'SALES%'
  )a 
  left join 
  (  select * from 
    (SELECT
      *,
      row_number() over(PARTITION BY user_id ORDER BY distance asc) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt = 'current'
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    where rank=1)b  on   a.leader_user_id=b.user_id
    

--  2.0 新签合同金额明细
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
    htqsrq,  --  合同起始日期
	  htzzrq,  --  合同终止日期
    yue,
    create_time,
    case when day(to_date(create_time)) between 1 and 15 then '月中' else '月底' end days_note,
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
    contract_end_date
  from csx_dim.csx_dim_crm_business_info a 
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
    owner_province_id,
    owner_province_name,
    owner_city_code,
    owner_city_name	,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.business_type_code,
    business_attribute_code	,
    business_attribute_name	,
    approval_status_name,
    business_stage,
    business_sign_time,
    estimate_contract_amount,   -- 商机签约金额
    htjey/10000 htjey,          -- 泛微合同金额
    htqsrq,  --  合同起始日期
	  htzzrq,  --  合同终止日期
    yue,
    create_time,
    contract_cycle_int,
    contract_cycle_desc,
    contract_number,
    tran_year,
    tran_contract_amount   -- 年化金额
from business a 
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
			sdt between '20220101' and '20240630'
			and business_type_code=1           --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9')    --  渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and order_channel_code not in (4,6)
		group by 
			performance_province_name,
		    business_type_code,
			customer_code
		) a
	   where 1=1
	        and after_date>='20240701'  -- 大于当月的正在履约
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
	where after_date<'20240701' or after_date is null    
;


-- 商机新客明细
select a.*,c.sales_user_number,c.sales_user_name,b.sale_amt
from
(
select * from csx_analyse.csx_analyse_sale_d_customer_sign_new_about_di 
where smonth in ('20240731')
union all
select * from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
where smonth in  ('20240731')
 )a
LEFT join
  (
     select *
     from csx_dim.csx_dim_crm_customer_info
     where sdt='current'
           and channel_code  in ('1','7','9')
  ) c  on a.customer_no=c.customer_code 
left join 
   (
     select 
              substr(sdt,1,6) smonth,
               customer_code,
               business_type_code,
                sum(sale_amt) as sale_amt
     from   csx_dws.csx_dws_sale_detail_di
     where  sdt>='20240701' and sdt<='20240731'
                and business_type_code in (1,2,6) and channel_code in ('1','7','9') 
     group by substr(sdt,1,6),
			 customer_code,
             business_type_code
             )b on a.customer_no=b.customer_code and a.business_type_code=b.business_type_code 
			   and a.smonth=b.smonth 
;

-- 1.0 销售明细
--drop table  csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale ;
create table csx_analyse_tmp.csx_analyse_tmp_hr_sales_sale as 
with sales_info as 
(select a.*,
 b.user_number leader_user_number,
 b.user_name leader_user_name,
 b.user_position_type leader_user_position,
 b.user_position leader_source_user_position
 from 
 (select
  user_id,
  user_number,
  user_name,
  user_position,
  begin_date,
  source_user_position,
  if(a.user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),user_id, leader_user_id) leader_user_id,
  province_name,
  city_name
  from 
  csx_dim.csx_dim_uc_user a 
  left  join 
    (select employee_name,
        employee_code,
        begin_date,
        record_type_name
    from csx_dim.csx_dim_basic_employee 
        where sdt='current' 
      --  and record_type_code	!=4
    )b on a.user_number=b.employee_code
    where
    sdt = 'current'
   and status=0 
 -- and (user_position like 'SALES%'
  )a 
  left join 
  (  select * from 
    (SELECT
      *,
      row_number() over(PARTITION BY user_id ORDER BY distance asc) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt = 'current'
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    where rank=1)b  on   a.leader_user_id=b.user_id
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
        where smonth in ('202407')
        union all
        select customer_no,business_type_code from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
        where smonth in  ('202407')
         )a) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
    where sdt >= '20240701'
        and sdt <= '20240731'   
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
        sales_user_name,
        sales_user_number,
        sales_user_position,
        begin_date,
        if(a.sales_user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),sales_user_number, leader_user_number) leader_user_number,
        if(a.sales_user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),sales_user_name,leader_user_name) leader_user_name,
        if(a.sales_user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),sales_user_position,leader_user_position)leader_user_position,
        new_customer_flag,
        sale_amt,
        profit
    from sale a 
    left join 
    sales_info b on a.sales_user_number=b.user_number
 ;
-- 逾期率
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
    where sdt in ('20240731')
    and ( channel_name in ('大客户','业务代理') or (sales_employee_code in ('81244592','81079752','80897025','81022821','81190209') and a.channel_name ='项目供应商'))
    and overdue_amount>0
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
),
sales_info as 
(select a.*,
 b.user_number leader_user_number,
 b.user_name leader_user_name,
 b.user_position_type leader_user_position,
 b.user_position leader_source_user_position
 from 
 (select
  user_id,
  user_number,
  user_name,
  user_position,
  source_user_position,
  begin_date,
  record_type_name,
  if(a.user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),user_id, leader_user_id) leader_user_id,
  province_name,
  city_name
  from 
       csx_dim.csx_dim_uc_user a 
   left  join 
    (select employee_name,
        employee_code,
        begin_date,
        record_type_name
    from csx_dim.csx_dim_basic_employee 
        where sdt='current' 
      --  and record_type_code	!=4
    )b on a.user_number=b.employee_code
    where
    sdt = 'current'
 --  and status=0 
 -- and (user_position like 'SALES%'
  )a 
  left join 
  (  select * from 
    (SELECT
      *,
      row_number() over(PARTITION BY user_id ORDER BY distance asc) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt = 'current'
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    where rank=1)b  on   a.leader_user_id=b.user_id
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
    user_position,
    leader_user_number,
    leader_user_name,
    leader_user_position,
    overdue_amount,
    receivable_amount
from over_rate a 
left join 
sales_info b on a.sales_employee_code=b.user_number

/*
销售BD看板
1、先取销售、毛利达成情况
2、商机新客
3、账款逾期、保证金
4、商机质量

*/
 create table csx_analyse_tmp.csx_analyse_tmp_hr_service_performance as 
with sales_info as 
(select a.*,
 b.user_number leader_user_number,
 b.user_name leader_user_name,
 b.user_position_type leader_user_position,
 b.user_position leader_source_user_position
 from 
 (select
  user_id,
  user_number,
  user_name,
  user_position,
  source_user_position,
  if(a.user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),user_id, leader_user_id) leader_user_id,
  province_name,
  city_name
  from 
  csx_dim.csx_dim_uc_user a 
    where
    sdt = 'current'
   and status=0 
 -- and (user_position like 'SALES%'
  )a 
  left join 
  (  select * from 
    (SELECT
      *,
      row_number() over(PARTITION BY user_id ORDER BY distance asc) AS rank
    FROM     csx_dim.csx_dim_uc_user_extend 
    WHERE sdt = 'current'
   -- and  leader_user_position in ('POSITION-26064','POSITION-26623','POSITION-25844')
   -- and user_position_type='SALES'
    AND status=0
    )a 
    where rank=1)b  on   a.leader_user_id=b.user_id
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
        where smonth in ('202407')
        union all
        select customer_no,business_type_code from  csx_analyse.csx_analyse_sale_d_customer_new_about_di
        where smonth in  ('202407')
         )a) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code 
    where sdt >= '20240701'
        and sdt <= '20240731'   
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
        sales_user_name,
        sales_user_number,
        sales_user_position,
        if(a.sales_user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),sales_user_number, leader_user_number) leader_user_number,
        if(a.sales_user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),sales_user_name,leader_user_name) leader_user_name,
        if(a.sales_user_position in ('SALES_CITY_MANAGER','SALES_MANAGER'),sales_user_position,leader_user_position)leader_user_position,
        new_customer_flag,
        sale_amt,
        profit
    from sale a 
    left join 
    sales_info b on a.sales_user_number=b.user_number
  --  where leader_user_name='谢志晓'
),
-- 逾期率
over_rate as 
(select substr(sdt,1,6) as sale_month,
    performance_region_name as region_name,
    performance_province_name as province_name,
    performance_city_name as city_group_name,
    customer_code, 
    business_attribute_name as customer_attribute_code,
    sales_employee_code,
    sales_employee_name,
    sum(overdue_amount) as overdue_amount,
    sum(receivable_amount) as receivable_amount
from 
-- csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di
  csx_analyse.csx_analyse_fr_sap_subject_customer_credit_account_analyse_df
    where sdt in ('20240531')
    
    group by substr(sdt,1,6),
    performance_region_name ,
    performance_province_name ,
    performance_city_name ,
    customer_code, 
    business_attribute_name,
    sales_employee_code,
    sales_employee_name     
),


履约保证金 
应收余额<=0，是指SAP，还是中台应收

合同日期
末次履约时间 倒推30天 
应收小于等于0 最后日期
比较取最晚日期，
根据最晚日期判断逾期，最晚+30 <核销日期 

-- 找出断约客户逾期

with incidental as (
  select
    payment_company_code,
    credit_customer_code,
    receiving_customer_code as customer_code,
    sum(cast(lave_write_off_amount as decimal(26, 2))) lave_write_off_amount,
    follow_up_user_code,
    follow_up_user_name	,
    responsible_person,
    responsible_person_number
  from
        csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di
  where
    self_employed = 1
    and cast(lave_write_off_amount as decimal(26, 2)) > 0
    and business_scene_code in (2,3)
  group by
    responsible_person,
    responsible_person_number,
    payment_company_code,
    receiving_customer_code,
    credit_customer_code

),
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
        credit_customer_code
      from
        incidental
    ) b on a.customer_code=b.customer_code and b.payment_company_code=a.company_code and a.credit_code=b.credit_customer_code
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

-- with cust_info as 
-- (
--   select
--   performance_province_name,
--   payment_company_code,
--   receiving_customer_code,
--   receiving_customer_name,
--   lave_write_off_amount,
--   responsible_person,
--   responsible_person_number,
--   follow_up_user_code,
--   follow_up_user_name,
--   credit_customer_code,
--   is_break_contract,
--   break_contract_time,
--   cust_task_rn
-- from
--   (select
--     *,
--     row_number() over(
--       partition by incidental_expenses_no,
--       break_contract_time
--     ) task_rn,
--     row_number() over(
--       partition by receiving_customer_code,
--       credit_customer_code,
--       payment_company_code
--     --  lave_write_off_amount -- break_contract_time
--       order by
--         break_contract_time desc
--     ) cust_task_rn
--   from
--     csx_analyse.csx_analyse_fr_sss_incidental_write_off_todo_df
--   where
--     sdt = '20240819'
--     and is_break_contract = '1'
--     and self_employed = '1'
--     and lave_write_off_amount > 0
-- )a 
-- where
--   1 = 1 
--   and cust_task_rn=1
-- )
--  ,

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

select 
    e.performance_province_name,
    e.performance_city_name,
    a. payment_company_code,
    a.credit_customer_code,
    a.customer_code,
    e.customer_name,
    e.sales_user_number,
    e.sales_user_name,
    a.responsible_person,
    a.responsible_person_number,
    a.lave_write_off_amount,
    coalesce(b.receive_sdt,'')receive_sdt,
    coalesce(regexp_replace(to_date(c.contract_end_date),'-',''),regexp_replace(to_date(f.contract_end_date),'-',''),'') contract_end_date,
    if(d.break_contract=1,1,0) break_contract,
    coalesce(regexp_replace(to_date(d.break_contract_date),'-',''),'') break_contract_date,
   sort_array(array(b.receive_sdt,regexp_replace(to_date(c.contract_end_date),'-',''),regexp_replace(to_date(d.break_contract_date),'-','')))[size(array(b.receive_sdt,regexp_replace(to_date(c.contract_end_date),'-',''),regexp_replace(to_date(d.break_contract_date),'-','')))-1] as max_sdt
from incidental a 
left join 
receive_amt b on a.customer_code=b.customer_code and a.payment_company_code=b.company_code and a.credit_customer_code=b.credit_code

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
 )c  on a.customer_code=c.customer_code and a.payment_company_code=c.company_code and a.credit_customer_code=c.credit_code 
  -- 客户创建时间23年2月9号，关联按照客户+公司
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
  row_number() over(partition by customer_code,company_code order by contract_end_date desc) rn
from business
 where create_time<'2023-02-09'
 )a 
  where rn=1 
  )f  on a.customer_code=f.customer_code and a.payment_company_code=f.company_code 
 left join 
 (select * from 
 ( select
    payment_company_code,
    credit_customer_code,
    receiving_customer_code as customer_code,
    responsible_person,
    responsible_person_number,
    break_contract,
    break_contract_date,
    row_number()over(partition by payment_company_code,credit_customer_code,receiving_customer_code order by break_contract_date desc ) rn 
 from
        csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di
  where
    self_employed = 1
    and cast(lave_write_off_amount as decimal(26, 2)) > 0
    and business_scene_code in (2,3)

  )a where rn=1 
)d on a.customer_code=d.customer_code and a.payment_company_code=d.payment_company_code and a.credit_customer_code=d.credit_customer_code
left join 
(select customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        performance_province_name,
        performance_city_name
from   csx_dim.csx_dim_crm_customer_info
where sdt='current') e on a.customer_code=e.customer_code
-- where a.customer_code='120497'
;


select
  a.*,
  b.receive_sdt
from
  incidental a 
  left join 
  receive_amt b on a.customer_code=b.customer_code and a.payment_company_code=b.company_code

)
-- 管家信息
service_info as 
(select customer_no,
  service_user_work_no,
  service_user_name,
  begin_date,
  service_user_id,
  attribute_code,
  attribute_name,
  sales_user_name,
  sales_user_number,
  sales_user_position,
  ranks
from (
    select distinct customer_code as customer_no,
      service_manager_user_number service_user_work_no,
      service_manager_user_name service_user_name,
      service_manager_user_id service_user_id,
      business_attribute_code attribute_code,
      business_attribute_name attribute_name,
      service_manager_user_position,
      sales_user_name,
      sales_user_number,
      sales_user_position,
      row_number() over(partition by customer_code, business_attribute_code    order by service_manager_user_id asc  ) as ranks
    from csx_dim.csx_dim_crm_customer_business_ownership
    where sdt= '20240531'
    --  and service_manager_user_id <> 0 -- and customer_code='111207'
    --  and business_attribute_code='1'
  ) a
 -- where customer_no='104275'
  distribute by customer_no,
  attribute_code sort by customer_no,
  attribute_code,
  ranks
 )a 
 left join 
 (select employee_code,
  begin_date	
  from csx_dim.csx_dim_basic_employee 
   where sdt='current') b on a.service_user_work_no=b.employee_code
 )
 select sale_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
   -- cast(business_type_code as string ) business_type_code,
    service_user_work_no,
    service_user_name,
    begin_date,
    sum(sale_amt)sale_amt,
    sum(profit)profit,
    sum(last_sale_amt)last_sale_amt,
    sum(last_profit)last_profit,
    coalesce( (sum(profit)/sum(sale_amt)-sum(last_profit)/sum(last_sale_amt))/(sum(last_profit)/sum(last_sale_amt)),0) as diff_profit_rate,
    sum(refund_total_amt)refund_total_amt,
    coalesce(sum(refund_total_amt)/sum(sale_amt),0) as refund_rate,
    sum(visit_cnt)visit_cnt,
    sum(total_cust) total_cust,
    coalesce(sum(visit_cnt)/sum(total_cust ),0) avg_visit_rate
from
(
select sale_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    b.service_user_work_no as service_user_work_no,
    service_user_name as service_user_name,
    sale_amt,
    profit,
    last_sale_amt,
    last_profit,
    (refund_total_amt)refund_total_amt,
    (visit_cnt)visit_cnt,
    0 total_cust
from
(select sale_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    cast(business_type_code as string ) business_type_code,
    a.customer_code,
    -- b.new_service_user_work_no as service_user_work_no,
    -- new_service_user_name as service_user_name,
    -- new_service_manager_user_position as service_user_position,
    sale_amt,
    profit,
    lag(sale_amt,1,0)over (partition by a.customer_code order by sale_month asc ) as last_sale_amt,
    lag(profit,1,0)over (partition by a.customer_code order by sale_month asc ) as last_profit,
    0 refund_total_amt,
    0 visit_cnt
from sale a 

-- where customer_code='100326'
union all 
select sale_month,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        cast(business_type_code as string )business_type_code,
        customer_code,
        0 sale_amt,
        0 profit,
        0 last_sale_amt,
        0 last_profit,
        refund_total_amt,
        0 visit_cnt
from retrun
)a 
left join
service_info b on a.customer_code=b.customer_no and a.business_type_code=b.attribute_code

union all
select sale_month,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        visit_user_number,
        visit_user_name,
        visit_user_position,
        0  as sale_amt,
        0  as profit,
        0  as last_sale_amt,
        0  as last_profit,
        0  as refund_total_amt,
        count(distinct sdt ) visit_cnt,
        count (distinct customer_id) total_cust
from visit_info
group by  sale_month,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        visit_user_number,
        visit_user_name,
        visit_user_position
) a 
where service_user_position='CUSTOMER_SERVICE_MANAGER'
and sale_month>='202403'
group by sale_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    service_user_work_no,
    service_user_name,
    service_user_position
    ;


-- 计算分数
with midd_jg as 
(select sale_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
   -- cast(business_type_code as string ) business_type_code,
    service_user_work_no,
    service_user_name,
    service_user_position,
    (sale_amt)sale_amt,
    (profit)profit,
    (last_sale_amt)last_sale_amt,
    (last_profit)last_profit,
    diff_profit_rate,
    dense_rank()over(partition by performance_province_name ,sale_month order by diff_profit_rate desc ) as diff_profit_rnk,
    refund_total_amt,
    refund_rate,
    dense_rank()over(partition by performance_province_name ,sale_month order by refund_rate asc ) as refund_rnk,
    (visit_cnt) visit_cnt,
    (total_cust) total_cust,
    avg_visit_rate,
    dense_rank()over(partition by performance_province_name ,sale_month order by avg_visit_rate desc  ) as avg_visit_rate_rnk
from  csx_analyse_tmp.csx_analyse_tmp_hr_service_performance a
)
select a.sale_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
   -- cast(business_type_code as string ) business_type_code,
    service_user_work_no,
    service_user_name,
    service_user_position,
    rank()over(partition by performance_province_name,sale_month order by (diff_profit_rnk_score+refund_rnk_score+avg_visit_rate_rnk_score) desc ) as total_rnk,
    (diff_profit_rnk_score+refund_rnk_score+avg_visit_rate_rnk_score) as total_score,
    diff_profit_rnk_score,
    refund_rnk_score,
    avg_visit_rate_rnk_score,
    (sale_amt)sale_amt,
    (profit)profit,
    (last_sale_amt)last_sale_amt,
    (last_profit)last_profit,
    diff_profit_rate,
    diff_profit_rnk,
    refund_total_amt,
    refund_rate,
    refund_rnk,
    (visit_cnt) visit_cnt,
    (total_cust) total_cust,
    avg_visit_rate,
    avg_visit_rate_rnk
    
from (
select a.sale_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
   -- cast(business_type_code as string ) business_type_code,
    service_user_work_no,
    service_user_name,
    service_user_position,
    CASE
    WHEN diff_profit_rnk = 1 THEN 40
    when diff_profit_rnk = max_diff_profit_rnk then 0 
    ELSE 40 - (diff_profit_rnk - 1) *(40/(max_diff_profit_rnk-1) )
  END  AS diff_profit_rnk_score,
    CASE
    WHEN refund_rnk = 1 THEN 20
    when refund_rnk = max_refund_rnk then 0 
    ELSE 20 - (refund_rnk - 1) *( 20/(max_refund_rnk -1) )
  END  AS refund_rnk_score,
    CASE
    WHEN avg_visit_rate_rnk = 1 THEN 10
    when avg_visit_rate_rnk=max_avg_visit_rate_rnk then 0 
    ELSE 10 - (avg_visit_rate_rnk - 1) *(10 /(max_avg_visit_rate_rnk-1) )
  END  AS avg_visit_rate_rnk_score,
    (sale_amt)sale_amt,
    (profit)profit,
    (last_sale_amt)last_sale_amt,
    (last_profit)last_profit,
    diff_profit_rate,
    diff_profit_rnk,
    refund_total_amt,
    refund_rate,
    refund_rnk,
    (visit_cnt) visit_cnt,
    (total_cust) total_cust,
    avg_visit_rate,
    avg_visit_rate_rnk    
from  midd_jg a
left join 
(select sale_month,
    performance_region_name,
    performance_province_name,
    max(diff_profit_rnk)max_diff_profit_rnk,
    max(refund_rnk)max_refund_rnk ,
    max(avg_visit_rate_rnk) max_avg_visit_rate_rnk
from  midd_jg
group by sale_month,
    performance_region_name,
    performance_province_name
)b on a.sale_month=b.sale_month and a.performance_province_name=b.performance_province_name
) a 

;





-- 换品率
csx_analyse.csx_analyse_report_replace_goods_profit_df 
rp_service_user_work_no_new这个字段是服务管家、
;


-- 管家销售额毛利额
with sale as 
(select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a
    where sdt >= '20240401'
        and sdt <= '20240630'   
        and business_type_code in (1,2,6)
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
        business_type_name
),
service_info as 
(select sale_month,new_attribute_name,
  customer_no,
  service_user_work_no,
  service_user_name,
  service_user_id,
  attribute_code,
  attribute_name,
  sales_user_name,
  sales_user_number,
  sales_user_position,
  service_manager_user_position,
--   if(sales_user_position='CUSTOMER_SERVICE_MANAGER',sales_user_number,service_user_work_no) as new_service_user_work_no,
--   if(sales_user_position='CUSTOMER_SERVICE_MANAGER',sales_user_name,service_user_name) as new_service_user_name,
--   if(sales_user_position='CUSTOMER_SERVICE_MANAGER',sales_user_position,service_manager_user_position) new_service_manager_user_position,
  ranks
from (
    select substr(sdt,1,6) as sale_month,
      customer_code as customer_no,
      service_manager_user_number service_user_work_no,
      service_manager_user_name service_user_name,
      service_manager_user_id service_user_id,
      business_attribute_code attribute_code,
      business_attribute_name attribute_name,
      case when business_attribute_code='1' then '日配业务'
            when business_attribute_code='2' then '福利业务'
            when business_attribute_code='5' then 'BBC'
        ELSE business_attribute_name END new_attribute_name,
      service_manager_user_position,
      sales_user_name,
      sales_user_number,
      sales_user_position,
      row_number() over(partition by customer_code, business_attribute_code    order by service_manager_user_id asc  ) as ranks
    from csx_dim.csx_dim_crm_customer_business_ownership
    where sdt in ('20240430','20240530','20240630')
    group by customer_code,
      service_manager_user_number,
      service_manager_user_name,
      service_manager_user_id,
      business_attribute_code,
      business_attribute_name,
      service_manager_user_position,
      sales_user_name,
      sales_user_number,
      sales_user_position
    --  and service_manager_user_id <> 0 -- and customer_code='111207'
    --  and business_attribute_code='1'
     ) a
 -- where customer_no='104275'
  distribute by customer_no,
  attribute_code sort by customer_no,
  attribute_code,
  ranks
  ) 
  select a.sale_month,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        business_type_name,
        a.customer_code,
        customer_name,
        b.sales_user_name,
        b.sales_user_number,
        b.sales_user_position,
        service_user_work_no,
        service_user_name,
        service_user_id,
        (sale_amt) sale_amt,
        (profit) profit
    from sale  a 
    left join
    service_info b on a.customer_code=b.customer_no and a.business_type_name=b.new_attribute_name and a.sale_month=b.sale_month
   -- where sale_month='202406'
   where service_user_name !=''
   

  -- 销售员销售&毛利 
  select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.business_type_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a
    where sdt >= '20240401'
        and sdt <= '20240630'   
        and business_type_code in (1,2,6)
        and sales_user_position='SALES'
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
        business_type_name
        ;




月份	大区	省区	城市	工号	姓名	岗位	入职时间	司龄	个人基准毛利额（对应薪酬）	上级销售经理	计划销售额	计划毛利额	计划毛利率
红黑板目标 

-- 销售人员月度目标
CREATE table data_analysis_prd.source_write_hr_sales_red_black_target
(
id bigint primary key auto_increment,
sale_month varchar(10) not null  comment '月份',
performance_region_name varchar(64) comment '大区',
performance_province_name varchar(64) comment '省区',
performance_city_name varchar(64) comment '城市',
sales_user_number varchar(64) comment '工号',
sales_user_name varchar(64) comment '姓名',
sales_user_position varchar(64) comment '岗位',
entry_time  varchar(64) comment '入职时间',
sales_age  int comment '司龄',
sales_user_base_profit decimal(10,2) comment '个人基准毛利额（对应薪酬）', 
sales_manager_number varchar(64) comment '上级销售经理工号',
sales_manager_name varchar(64) comment '上级销售经理姓名',
plan_sales_amt decimal(10,2) comment '计划销售额',
plan_profit     decimal(10,2) comment '计划毛利额',
plan_profit_rate decimal(10,2) comment '计划毛利率',
create_time timestamp not null default current_timestamp comment '创建时间',
update_time timestamp not null default current_timestamp on update current_timestamp comment '更新时间',
create_by varchar(64) not null default 'sys' comment '创建人',
update_by varchar(64) not null default 'sys' comment '更新人',
primary key (id) using btree
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='销售人员月度目标表';

-- hive 表
CREATE table csx_analyse.csx_analyse_source_write_hr_sales_red_black_target_mf 
(
id string comment 'id',
sale_month string   comment '月份',
performance_region_name string comment '大区',
performance_province_name string comment '省区',
performance_city_name string comment '城市',
sales_user_number string comment '工号',
sales_user_name string comment '姓名',
sales_user_position string comment '岗位',
entry_time  string comment '入职时间',
sales_age  string comment '司龄',
sales_user_base_profit string comment '个人基准毛利额（对应薪酬）', 
sales_manager_number string comment '上级销售经理工号',
sales_manager_name string comment '上级销售经理姓名',
plan_sales_amt string comment '计划销售额',
plan_profit     string comment '计划毛利额',
plan_profit_rate string comment '计划毛利率',
create_time string  comment '创建时间',
update_time string comment '更新时间',
create_by string comment '创建人',
update_by string comment '更新人'
)COMMENT'MYSQL销售人员月度目标表'
partitioned by (smt string comment '同步月份')
stored as AVRO
;

-- 销售经理月度目标
月份	大区	省区	城市	工号	姓名	岗位	入职时间	司龄	个人基准毛利额目标（对应薪酬）	团队基准毛利额目标（对应薪酬）经理团队人数	计划销售额	计划毛利额	计划毛利率	


CREATE table data_analysis_prd.source_write_hr_sales_manager_red_black_target
(
id bigint primary key auto_increment,
sale_month varchar(10) not null  comment '月份',
performance_region_name varchar(64) comment '大区',
performance_province_name varchar(64) comment '省区',
performance_city_name varchar(64) comment '城市',
sales_user_number varchar(64) comment '工号',
sales_user_name varchar(64) comment '姓名',
sales_user_position varchar(64) comment '岗位',
entry_time  varchar(64) comment '入职时间',
sales_age  int comment '司龄',
sales_user_base_profit decimal(10,2) comment '个人基准毛利额（对应薪酬）', 
sales_team_base_profit decimal(10,2) comment '团队基准毛利额',
sales_team_number int comment '经理团队人数',
plan_sales_amt decimal(10,2) comment '计划销售额',
plan_profit     decimal(10,2) comment '计划毛利额',
plan_profit_rate decimal(10,2) comment '计划毛利率',
create_time timestamp not null default current_timestamp comment '创建时间',
update_time timestamp not null default current_timestamp on update current_timestamp comment '更新时间',
create_by varchar(64) not null default 'sys' comment '创建人',
update_by varchar(64) not null default 'sys' comment '更新人'
 )
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='销售经理月度目标表';

-- HIVE   data_analysis_prd.source_write_hr_sales_manager_red_black_target
CREATE table csx_analyse.csx_analyse_source_write_hr_sales_manager_red_black_target_mf
(
id string comment 'id',
sale_month string  comment '月份',
performance_region_name string comment '大区',
performance_province_name string comment '省区',
performance_city_name string comment '城市',
sales_user_number string comment '工号',
sales_user_name string comment '姓名',
sales_user_position string comment '岗位',
entry_time  string comment '入职时间',
sales_age  string comment '司龄',
sales_user_base_profit string comment '个人基准毛利额（对应薪酬）', 
sales_team_base_profit string comment '团队基准毛利额',
sales_team_number string comment '经理团队人数',
plan_sales_amt string comment '计划销售额',
plan_profit     string comment '计划毛利额',
plan_profit_rate string comment '计划毛利率',
create_time timestamp   comment '创建时间',
update_time timestamp   comment '更新时间',
create_by string   comment '创建人',
update_by string   comment '更新人'
 )COMMENT'销售经理月度目标表'
partitioned by (smt string comment '同步月份')
stored as AVRO
;

-- 服务管家月度目标
月份	大区	省区	城市	工号	姓名	岗位	入职时间	司龄	个人基准毛利额目标（对应薪酬）	计划销售额	计划毛利额	计划毛利率	


CREATE table data_analysis_prd.source_write_hr_service_red_black_target
(
    id bigint primary key auto_increment  ,
    sale_month varchar(10) not null  comment '月份',
    performance_region_name varchar(64) comment '大区',
    performance_province_name varchar(64) comment '省区',
    performance_city_name varchar(64) comment '城市',
    sales_user_number varchar(64) comment '工号',
    sales_user_name varchar(64) comment '姓名',
    sales_user_position varchar(64) comment '岗位',
    entry_time  varchar(64) comment '入职时间',
    sales_age  int comment '司龄',
    sales_user_base_profit decimal(10,2) comment '个人基准毛利额（对应薪酬）', 
    plan_sales_amt decimal(10,2) comment '计划销售额',
    plan_profit     decimal(10,2) comment '计划毛利额',
    plan_profit_rate decimal(10,2) comment '计划毛利率',
    create_time timestamp not null default current_timestamp comment '创建时间',
    update_time timestamp not null default current_timestamp on update current_timestamp comment '更新时间',
    create_by varchar(64) not null default 'sys' comment '创建人',
    update_by varchar(64) not null default 'sys' comment '更新人'
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='服务管家月度目标表';


hive data_analysis_prd.source_write_hr_service_red_black_target 

CREATE table csx_analyse.csx_analyse_source_write_hr_service_red_black_target_mf
(
    id string comment ,
    sale_month string  comment '月份',
    performance_region_name string comment '大区',
    performance_province_name string comment '省区',
    performance_city_name string comment '城市',
    sales_user_number string comment '工号',
    sales_user_name string comment '姓名',
    sales_user_position string comment '岗位',
    entry_time  string comment '入职时间',
    sales_age  string comment '司龄',
    sales_user_base_profit string comment '个人基准毛利额（对应薪酬）', 
    plan_sales_amt string comment '计划销售额',
    plan_profit     string comment '计划毛利额',
    plan_profit_rate string comment '计划毛利率',
    create_time timestamp comment '创建时间',
    update_time timestamp  comment '更新时间',
    create_by string comment '创建人',
    update_by string comment '更新人'
)
COMMENT'服务管家月度目标表'
partitioned by (smt string comment '同步月份')
stored as AVRO;