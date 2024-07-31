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


-- 销售员信息


-- ======断约客户明细==========
select 	performance_province_name, 
			after_month, 
			a.customer_code,
			c.customer_name
			from
				(
				select 
					customer_code,
					substr(regexp_replace(cast(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90) as string),'-',''),1,6) as after_month
				from 
					csx_dws.csx_dws_sale_detail_di 
				where 
					sdt between '20210901' and '20230630'
					and business_type_code=1 --  业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') --  渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and order_channel_code not in (4,6)

				group by 
					customer_code
				) a
		left join   (
			  select *
			  from csx_dim.csx_dim_crm_customer_info
			  where sdt= 'current'
				and channel_code  in ('1','7','9')
			        ) c on a.customer_code=c.customer_code 
	   where after_month='202306'
			group by performance_province_name, 
			after_month, 
			a.customer_code,
			c.customer_name;

create table csx_analyse_tmp.csx_analyse_tmp_hr_service_performance as 
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
        business_attribute_code,
        first_business_sale_date ,
        last_business_sale_date,
        diff_days,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a
    left join 
    (select customer_code,
        business_type_code,
        business_attribute_code,
        first_business_sale_date ,
        last_business_sale_date,
        datediff(current_date(),from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd'),'yyyy-MM-dd')) as diff_days
    from  csx_dws.csx_dws_crm_customer_business_active_di 
    where sdt='current'
    ) b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
    left join 
    -- 商机 
    (select customer_code,
        business_attribute_code,
        business_sign_time,
        row_number()over(partition by customer_code,business_attribute_code order by business_sign_time desc ) as rnk 
    from csx_dim.csx_dim_crm_business_info
    where sdt='current'
        and status=1
        and business_stage=5
        and business_attribute_code=1
        and to_date(business_sign_time)>='2024-05-01'
        and to_date(business_sign_time)>='2024-05-31'
      )
    
    where sdt >= '20240501'
        and sdt <= '20240531'   
        and business_type_code in ('1','2','6')  -- 1-日配、2-福利、6-BBC
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        a.business_type_code
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
-- 保证金
(select performance_province_code,
  performance_province_name,
  receiving_customer_code as customer_code,
  receiving_customer_name as customer_name,
  business_scene_code,        -- 业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约
  follow_up_user_code,
  follow_up_user_name,
  sum(lave_write_off_amount) as lave_write_off_amount
from csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di
 where sdt='20240531'
  and self_employed=1
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