drop table if exists csx_tmp.tmp_report_sale_r_m_customer_results_1;
create temporary table if not exists csx_tmp.tmp_report_sale_r_m_customer_results_1
as
select 
  a.customer_no,
  channel_name,
  sales_region_code,
  sales_region_name,
  sales_province_code,
  sales_province_name,
  sign_date,
  calday,
  csx_week,
  month_of_year,
  start_date,
  end_date,
  case when sign_date < start_date then 1 
    else 0 end as is_original_customer,
  case when sign_date >= start_date and sign_date <= end_date then 1 
    else 0 end as is_new_sign,

  case when sales_value is null then 0 
    else sales_value end as sales_value,
  case when profit is null then 0 
    else profit end as profit,
	
  case when sign_date < start_date and sales_value is not null then sales_value 
    else 0 end as original_sales_value,
  case when sign_date < start_date and profit is not null then profit 
    else 0 end as original_profit,
	
  case when sign_date >= start_date and sign_date <= end_date and sales_value is not null then sales_value 
    else 0 end as new_sales_value,
  case when sign_date >= start_date and sign_date <= end_date and profit is not null then profit 
    else 0 end as new_profit
from 
(
  select 
    customer_no,
	'B端' as channel_name,
	sales_region_code,
	sales_region_name,
	sales_province_code,
    sales_province_name,
	regexp_replace(to_date(first_sign_time),'-','') as sign_date
  from csx_dw.dws_crm_w_a_customer 
  where sdt = 'current' and sign_time <> '' and channel_code in ('1','7','9')
) a 
left join 
(
  select 
    calday,
    csx_week,
	month_of_year,
    min(calday) over(partition by month_of_year,csx_week) as start_date,
    max(calday) over(partition by month_of_year,csx_week) as end_date
  from 
  (
    select 
      calday,
      lead(week_of_year,1,'99991231') over(order by calday) as csx_week,
	  month_of_year
    from csx_dw.dws_basic_w_a_date 
    where calday >= regexp_replace(add_months(trunc(current_date,'MM'),-1),'-','') 
	  and calday <= regexp_replace(current_date, '-', '')
  ) a where a.calday <> regexp_replace(current_date, '-', '')
) b on 1 = 1
left join 
(
  select 
    customer_no,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sdt
  from csx_dw.ads_sale_r_d_customer 
  where sdt >= regexp_replace(add_months(trunc(current_date,'MM'),-1),'-','') 
    and sdt <= regexp_replace(date_sub(current_date, 1), '-', '')
  group by customer_no,sdt
) c on a.customer_no = c.customer_no and b.calday = c.sdt
;



insert overwrite table csx_dw.report_sale_r_m_customer_performance partition(month) 
select 
  concat(sales_region_code,'&',sales_province_code,'&',channel_name,'&',month_of_year,'&',csx_week) as biz_id,
  sales_region_code as region_code,
  sales_region_name as region_name,
  sales_province_code as province_code,
  sales_province_name as province_name,
  channel_name,
  row_number() over(partition by sales_region_code,sales_province_code,channel_name,month_of_year order by csx_week asc) week_ranks,
  csx_week,
  start_date,
  end_date,
  original_customer_nums,
  new_sign_nums,
  sales_value,
  profit,
  original_sales_value,
  original_profit,
  new_sales_value,
  new_profit,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
  ${hiveconf:author} as create_by,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
  month_of_year as month
from 
(
select
    channel_name,
	sales_region_code,
	sales_region_name,
    sales_province_code,
    sales_province_name,
    csx_week,
    month_of_year,
    start_date,
    end_date,
    count(distinct is_original_customer) as original_customer_nums,
    count(distinct is_new_sign) as new_sign_nums,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
	sum(original_sales_value) as original_sales_value,
    sum(original_profit) as original_profit,
    sum(new_sales_value) as new_sales_value,
	sum(new_profit) as new_profit
from 
(
  select 
    customer_no,
    channel_name,
	sales_region_code,
	sales_region_name,
    sales_province_code,
    sales_province_name,
    sign_date,
    csx_week,
    month_of_year,
    start_date,
    end_date,
    case when sum(is_original_customer) >0 then customer_no 
      else null end as is_original_customer,
    case when sum(is_new_sign) >0 then customer_no
      else null end as is_new_sign,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
	sum(original_sales_value) as original_sales_value,
    sum(original_profit) as original_profit,
    sum(new_sales_value) as new_sales_value,
	sum(new_profit) as new_profit
  from csx_tmp.tmp_report_sale_r_m_customer_results_1
  group by customer_no,channel_name,sales_region_code,sales_region_name,
    sales_province_code,sales_province_name,sign_date,csx_week,
	month_of_year,start_date,end_date
) a 
group by 
    channel_name,
	sales_region_code,
	sales_region_name,
    sales_province_code,
    sales_province_name,
    csx_week,
    month_of_year,
    start_date,
    end_date
) a 

union all 

select 
  concat(region_code,'&',province_code,'&',channel_name,'&',month_of_year,'&',csx_week) as biz_id,
  region_code,
  region_name,
  province_code,
  province_name,
  channel_name,
  row_number() over(partition by region_code,province_code,channel_name,month_of_year order by csx_week asc) week_ranks,
  csx_week,
  start_date,
  end_date,
  0 as original_customer_nums,
  0 as new_sign_nums,
  sales_value,
  profit,
  0 as original_sales_value,
  0 as original_profit,
  0 as new_sales_value,
  0 as new_profit,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
  ${hiveconf:author} as create_by,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
  month_of_year as month
from 
(
  select 
    region_code,
	region_name,
	province_code,
	province_name,
	channel_name,
	csx_week,
	start_date,
	end_date,
	month_of_year,
    sum(sales_value) as sales_value,
    sum(profit) as profit
  from 
  (
    select 
      region_code,
  	  region_name,
  	  province_code,
  	  province_name,
  	  'M端' as channel_name,
      sum(sales_value) as sales_value,
      sum(profit) as profit,
      sdt
    from csx_dw.dws_sale_r_d_detail 
    where channel_code = '2' and sdt >= regexp_replace(add_months(trunc(current_date,'MM'),-1),'-','') 
      and sdt <= regexp_replace(date_sub(current_date, 1), '-', '')
    group by region_code,region_name,province_code,province_name,sdt
  ) a 
  left join 
  (
    select 
      calday,
      csx_week,
  	  month_of_year,
      min(calday) over(partition by month_of_year,csx_week) as start_date,
      max(calday) over(partition by month_of_year,csx_week) as end_date
    from 
    (
      select 
        calday,
        lead(week_of_year,1,'99991231') over(order by calday) as csx_week,
  	  month_of_year
      from csx_dw.dws_basic_w_a_date 
      where calday >= regexp_replace(add_months(trunc(current_date,'MM'),-1),'-','') 
  	  and calday <= regexp_replace(current_date, '-', '')
    ) a where a.calday <> regexp_replace(current_date, '-', '')
  ) b on a.sdt = b.calday
  group by region_code,
	region_name,
	province_code,
	province_name,
	channel_name,
	csx_week,
	start_date,
	end_date,
	month_of_year
) a 
;