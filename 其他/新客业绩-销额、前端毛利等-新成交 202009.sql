

 -- 1、最小、最大成交日期 -all  
 -- customer_sale表只有2019年以后数据，本报表是关于新客因此可使用
drop table b2b_tmp.tmp_new_sale_1;
create temporary table b2b_tmp.tmp_new_sale_1
as 
select
customer_no,
min(sales_date) as min_sales_date,
max(sales_date) as max_sales_date,
sum(sales_value) as sum_sales_value,
sum(profit)/sum(sales_value) as profit_rate,
count(distinct sales_date) as count_day
from csx_dw.dws_sale_r_d_customer_sale
group by customer_no;


--01、每天-销售员销额、最终前端毛利统计
drop table b2b_tmp.temp_new_day_cust_1;
create temporary table b2b_tmp.temp_new_day_cust_1
as
select 
a.sdt,a.province_name,a.city_real,b.fourth_supervisor_name,b.first_supervisor_name,b.sales_name,b.work_no,
a.customer_no,b.customer_name,b.attribute,
b.first_category,b.second_category,b.third_category,b.sign_time,m.min_sales_date,
sales_cost,zt_cost,sales_value,profit,prorate,front_profit,fnl_prorate
from 
(select sdt,substr(sales_date,1,6) smonth,province_name,city_real,customer_no,
sum(sales_cost)sales_cost,
sum(coalesce(middle_office_price,0)*sales_qty) zt_cost,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200901' and sdt<=regexp_replace(date_sub(current_date,1),'-','')
and sales_type in ('qyg','sapqyg','sapgc','sc','bbc') 
and channel in('1','7')
group by sdt,substr(sales_date,1,6),province_name,city_real,customer_no)a 
join 
	(select customer_no,customer_name,sales_name,work_no,
	first_supervisor_name,fourth_supervisor_name,first_category,second_category,third_category,attribute,sales_province,sales_city,
	regexp_replace(substr(sign_time,1,10),'-','') sign_time
	from csx_dw.dws_crm_w_a_customer_m_v1
	where sdt=regexp_replace(date_sub(current_date,1),'-','')
	and channel = '大'
	and create_time is not null
    and sign_time is not null
	--and attribute <> '福利'
	)b on b.customer_no =a.customer_no 
join 
	(select * from b2b_tmp.tmp_new_sale_1 
	where min_sales_date>='20200901'
	)m on m.customer_no =a.customer_no;


insert overwrite directory '/tmp/gonghuimin/xinke1' row format delimited fields terminated by '\t' 
select * from b2b_tmp.temp_new_day_cust_1;


--02、销额、最终前端毛利统计
drop table b2b_tmp.temp_new_cust_1;
create temporary table b2b_tmp.temp_new_cust_1
as
select 
a.province_name,a.city_real,a.fourth_supervisor_name,a.first_supervisor_name,a.sales_name,a.work_no,
a.customer_no,a.customer_name,a.attribute,
a.first_category,a.second_category,a.third_category,a.sign_time,a.min_sales_date,
sum(sales_cost)sales_cost,
sum(zt_cost)zt_cost,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate
from b2b_tmp.temp_new_day_cust_1 a 
group by a.province_name,a.city_real,a.fourth_supervisor_name,a.first_supervisor_name,a.sales_name,a.work_no,
a.customer_no,a.customer_name,a.attribute,
a.first_category,a.second_category,a.third_category,a.sign_time,a.min_sales_date;

insert overwrite directory '/tmp/gonghuimin/xinke2' row format delimited fields terminated by '\t' 
select * from b2b_tmp.temp_new_cust_1;


--03、销售员销额、最终前端毛利统计
drop table b2b_tmp.temp_new_sales_1;
create temporary table b2b_tmp.temp_new_sales_1
as
select 
a.attribute,a.province_name,a.city_real,a.fourth_supervisor_name,a.first_supervisor_name,a.sales_name,a.work_no,
count(distinct a.customer_no) count_sale,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate
from b2b_tmp.temp_new_day_cust_1 a 
where a.attribute in ('日配','福利')
group by a.attribute,a.province_name,a.city_real,a.fourth_supervisor_name,a.first_supervisor_name,a.sales_name,a.work_no;

insert overwrite directory '/tmp/gonghuimin/xinke3' row format delimited fields terminated by '\t' 
select * from b2b_tmp.temp_new_sales_1 order by attribute desc;

