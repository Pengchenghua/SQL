set day='2020-11-01';
select  dayofweek(${hiveconf:day}),date_add(${hiveconf:day},1-case when dayofweek(${hiveconf:day}) =1 then 6 else dayofweek(${hiveconf:day}) -2 end) as week_first_day,
date_add(${hiveconf:day},8-case when dayofweek(${hiveconf:day}) =6 then 1 else dayofweek(${hiveconf:day}) -6 end) as week_last_day,    -- 本周最后一天_周日
date_add(${hiveconf:day},1-case when dayofweek(${hiveconf:day}) =1 then 7 else dayofweek(${hiveconf:day}) -1 end) as week_first_day
;

select regexp_replace(date_sub(current_date, 
      pmod(datediff(date_sub(current_date, 1), '2016-12-31'), 7)+1), '-', '')
      ;


drop table csx_tmp.temp_date_m;
create table csx_tmp.temp_date_m as      
select 
	  a.calday,a.dow,a.calweek,
      regexp_replace(date_sub(date_add(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'), 
      pmod(datediff(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'),0), '2012-01-07'), 7)+1),7),6), '-', '') as new_week_first,
    regexp_replace(date_add(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'), 
      pmod(datediff(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'),0), '2012-01-07'), 7)+1),7), '-', '') as new_week_last,
   case when   date_sub(date_add(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'), 
      pmod(datediff(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'),0), '2012-01-07'), 7)+1),7),6) in ('2018-12-29','2019-12-28') then 1 
        else  weekofyear(date_sub(date_add(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'), 
      pmod(datediff(date_sub(from_unixtime(unix_timestamp(a.calday,'yyyyMMdd'),'yyyy-MM-dd'),0), '2012-01-07'), 7)+1),7),6))+1 end as new_weeknum
from csx_dw.dws_w_a_date_m a 
where calday>='20190101' ;



select
    j.new_weeknum,
	province_code ,
	province_name,
	city_name ,
	a.city_real,
	a.dc_code,
	a.dc_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大客户' 
		when channel in ('2') then '商超'
		else '大客户' 
		end channel_name ,
	customer_no,
	customer_name,
	first_category,
	c.new_classify_name,
	goods_code ,
	goods_name,
	unit ,
	classify_small_code,
	classify_small_name,
	b.category_small_code,
	b.category_small_name,
	avg(cost_price) cost_price ,
	avg(sales_price) sales_price ,
	sum(sales_qty) qty,
	sum(sales_value) sales_value ,
	sum(profit) profit ,
	sum(profit)/sum(a.sales_value) profit_rate
from
	csx_dw.dws_sale_r_d_customer_sale a
join (select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current'
	and classify_middle_code = 'B0304') b on a.category_small_code = b.category_small_code
join 
csx_tmp.temp_date_m j on a.sdt=j.calday 
left join csx_tmp.new_customer_classify c on	a.second_category_code = c.second_category
where
	sdt >= '20200101'
	and sdt <= '20201031'
group by province_code ,
	province_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大客户' 
		when channel in ('2') then '商超'
		else '大客户' 
		end ,
	customer_no,
	customer_name,
	first_category,
	c.new_classify_name,
	goods_code ,
	goods_name,
	unit ,
	classify_small_code,
	classify_small_name,
	b.category_small_code,
	b.category_small_name,
	city_name ,
	a.city_real,
	a.dc_code,
	a.dc_name,
 	j.new_weeknum
	 order by  j.new_weeknum;
