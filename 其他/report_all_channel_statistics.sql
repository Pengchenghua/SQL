set mapreduce.job.queuename=caishixian;

-- 全渠道统计
drop table b2b_tmp.channel_sales_statistics;
create temporary table b2b_tmp.channel_sales_statistics
as 
select 
channel_name,
supervisor,
sales_province,
sum(sales_value) as sales_value,
sum(ring_sales_value) as ring_sales_value,
sum(profit) as profit,
sum(order_customers) as order_customers
from 
(
  select 
    case when (qdflag='B端' or qdflag='平台') then '大客户' 
      when qdflag = '供应链(S端)' then '供应链'
      when qdflag='M端' then '商超'
      else qdflag end as channel_name,
    case when qdflag='平台' then '' else manage end as supervisor,
    case when qdflag= '平台' then '平台' else dist end as sales_province,
    sum(xse) as sales_value,
    0 as ring_sales_value,
    sum(mle) as profit,
    count(distinct cust_id, sdt) as order_customers
  from csx_dw.sale_warzone01_detail_dtl 
  where sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and
    sdt <= regexp_replace(date_sub(current_date,1),'-','')
  group by  case when (qdflag='B端' or qdflag='平台') then '大客户' 
      when qdflag = '供应链(S端)' then '供应链'
      when qdflag='M端' then '商超'
      else qdflag end, 
    case when qdflag='平台' then '' else manage end,
    case when qdflag= '平台' then '平台' else dist end
  union all 
  select 
    case when (qdflag='B端' or qdflag='平台') then '大客户' 
      when qdflag = '供应链(S端)' then '供应链'
      when qdflag='M端' then '商超'
      else qdflag end as channel_name,
    case when qdflag='平台' then '' else manage end as supervisor,
    case when qdflag= '平台' then '平台' else dist end as sales_province,
    0 as sales_value,
    sum(xse) as ring_sales_value,
    0 profit,
    0 as order_customers 
  from csx_dw.sale_warzone01_detail_dtl 
  where sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and
    sdt <= regexp_replace(add_months(date_sub(current_date,1),-1),'-','')
  group by case when (qdflag='B端' or qdflag='平台') then '大客户' 
      when qdflag = '供应链(S端)' then '供应链'
      when qdflag='M端' then '商超'
      else qdflag end, 
    case when qdflag='平台' then '' else manage end,
    case when qdflag= '平台' then '平台' else dist end
)a group by channel_name, supervisor, sales_province;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
insert overwrite table csx_dw.report_all_channel_statistics partition(sdt)
select 
  channel_name,
  supervisor,
  sales_province,
  sales_value,
  ring_increase_rate,
  profit,
  profit_rate,
  order_customers,
  regexp_replace(date_sub(current_date,1),'-','') as sdt
from
(
  select 
    channel_name,
    '' as supervisor,
    sales_province,
    sales_value,
    cast((sales_value - ring_sales_value)/ring_sales_value as decimal(26, 6)) as ring_increase_rate,
    profit,
    cast(profit/sales_value as decimal(26, 6)) as profit_rate,
    order_customers
  from b2b_tmp.channel_sales_statistics where channel_name in ('商超','大客户')
  union all
  select 
    'all' as channel_name,
    '' as supervisor,
    'all' as sales_province,
    sum(sales_value) as sales_value,
    cast((sum(sales_value) - sum(ring_sales_value))/sum(ring_sales_value) as decimal(26, 6)) 
      as ring_increase_rate,
    sum(profit) as profit,
    cast(sum(profit)/sum(sales_value) as decimal(26, 6)) as profit_rate,
    sum(order_customers) as order_customers
  from b2b_tmp.channel_sales_statistics
  union all
  select 
    channel_name,
    '' as supervisor,
    'all' as sales_province,
    sum(sales_value) as sales_value,
    cast((sum(sales_value) - sum(ring_sales_value))/sum(ring_sales_value) as decimal(26, 6)) 
      as ring_increase_rate,
    sum(profit) as profit,
    cast(sum(profit)/sum(sales_value) as decimal(26, 6)) as profit_rate,
    sum(order_customers) as order_customers
  from b2b_tmp.channel_sales_statistics
  group by channel_name 
)a
order by case when channel_name='all' then 1 
  when channel_name='商超'then 2 
  when channel_name='大客户' then 3 
  when channel_name='大宗'then 4 
  when channel_name='供应链' then 5 else 6 end, sales_province;