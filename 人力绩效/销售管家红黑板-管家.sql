-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_service_performance ;
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
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a
    where sdt >= '20240201'
        and sdt <= '20240531'   
        and business_type_code = 1
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

-- 退货明细
retrun as 
(select substr(sdt,1,6) sale_month,
        performance_region_name,
        performance_province_name,
        performance_city_name,
        case 
	        when business_type_name='项目供应商' then 4
	        when business_type_name= '日配' then 1  
	        when business_type_name= '福利' then 2  
	        when business_type_name= '大宗贸易'  then 3 
	        when business_type_name= '内购' then 4  end as business_type_code,
        customer_code,
        customer_name,
        refund_total_amt,
        responsible_department_name
  from  csx_analyse.csx_analyse_fr_ts_return_order_detail_di
where sdt>='20240301' 
  and sdt<='20240531'
and responsible_department_name='服务管家'
),
-- 调价明细
adjust as (
	select 
    substr(sdt,1,6) sale_month,
  	original_order_code,
		adjust_price_order_code,
    customer_code,
		product_code,
		adjusted_total_amount,
		(case when adjust_reason_code='10' then '报价错误-报价失误' 
				when adjust_reason_code='11' then '报价错误-报价客户不认可' 
				when adjust_reason_code='20' then '客户对账差异-税率调整' 
				when adjust_reason_code='21' then '客户对账差异-其他' 
				when adjust_reason_code='30' then '后端履约问题-商品等级/规格未达要求' 
				when adjust_reason_code='31' then '后端履约问题-商品质量问题折扣处理' 
				when adjust_reason_code='32' then '后端履约问题-其他' 
				when adjust_reason_code='40' then '发货后报价类型' 
				when adjust_reason_code='50' then '无原单退款' 
				when adjust_reason_code='60' then '其他' 
				when adjust_reason_code='70' then '单据超90天未处理' end) as adjust_reason,
		row_number() over(partition by adjust_price_order_code,product_code order by update_time desc,adjusted_total_amount desc) as rno 				
	from csx_dwd.csx_dwd_sss_customer_credit_adjust_price_item_di 
	where sdt>='20240301'
  ),
-- 拜访 
visit_info 
as (select substr(sdt,1,6) sale_month,
  a.customer_id,
  a.customer_code,
  business_attribute_code,
  sales_user_id,
  sales_user_name,
  visit_user_id,
  visit_user_number,
  visit_user_name,
  visit_user_position,
  visit_time,
  performance_region_code ,
  performance_region_name,
  performance_province_code ,
  performance_province_name,
  performance_city_code ,
  performance_city_name ,
  sdt
 from 
`csx_dws`.`csx_dws_crm_customer_visit_record_di` a 
left join 
(select
    user_id,
    user_name,
    province_id,
    province_name,
    city_code,
    city_name,
    performance_region_code ,
    performance_region_name,
    performance_province_code ,
    performance_province_name,
    performance_city_code ,
    performance_city_name 
from
  csx_dim.csx_dim_uc_user a
left join 
(
  select distinct
    performance_region_code ,
    performance_region_name,
    performance_province_code ,
    performance_province_name,
    performance_city_code ,
    performance_city_name ,
    sales_city_code,
    sales_province_id
  from  csx_dim.csx_dim_crm_customer_info
  where sdt='20240531' 
  -- customer_type_code	int	客户类型编码(1线索 4合作)
 )b on a.city_code=b.sales_city_code and province_id=sales_province_id
   where
      sdt = 'current'
      and user_id is not null
      and user_id <> 0
)b on a.visit_user_id=b.user_id
where sdt>='20240301'
  and sdt<='20240531'
  and visit_user_position='CUSTOMER_SERVICE_MANAGER'
),
-- 管家信息
service_info as 
(select customer_no,
  service_user_work_no,
  service_user_name,
  service_user_id,
  attribute_code,
  attribute_name,
  sales_user_name,
  sales_user_number,
  sales_user_position,
  if(sales_user_position='CUSTOMER_SERVICE_MANAGER',sales_user_number,service_user_work_no) as new_service_user_work_no,
  if(sales_user_position='CUSTOMER_SERVICE_MANAGER',sales_user_name,service_user_name) as new_service_user_name,
  if(sales_user_position='CUSTOMER_SERVICE_MANAGER',sales_user_position,service_manager_user_position) new_service_manager_user_position,
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
 )
 select sale_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
   -- cast(business_type_code as string ) business_type_code,
    service_user_work_no,
    service_user_name,
    service_user_position,
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
    b.new_service_user_work_no as service_user_work_no,
    new_service_user_name as service_user_name,
    new_service_manager_user_position as service_user_position,
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