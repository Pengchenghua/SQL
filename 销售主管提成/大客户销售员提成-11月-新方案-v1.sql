
-- 新建表 销售提成_销售员收入组
--drop table csx_tmp.sales_income_info;
create table if not exists `csx_tmp.sales_income_info` (
  `cust_type` STRING comment '销售员类别',
  `sales_name` STRING comment '业务员名称',
  `work_no` STRING comment '业务员工号',
  `income_type` STRING comment '业务员收入组类'
) comment '销售提成_销售员收入组'
partitioned by (sdt string comment '日期分区')
row format delimited fields terminated by ','
stored as textfile;

--load data inpath '/tmp/raoyanhua/sales_income_info_11.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20201130');
--select * from csx_tmp.sales_income_info where sdt='20201130';




-- 新建表 202006以前合伙人清单(合伙人、断约--断约月及以前为合伙人)+202007及以后用月末最后一天信息表中合伙人属性，当月取最新
--drop table csx_tmp.tmp_cust_partner;
create table if not exists `csx_tmp.tmp_cust_partner` (
  `cust_type` STRING comment '类别',
  `province_name` STRING comment '省区',
  `city_name` STRING comment '城市',  
  `customer_no` STRING comment '编号',
  `customer_name` STRING comment '名称',
  `break_date` STRING comment '断约年月'
) comment '合伙人清单_v2'
partitioned by (sdt string comment '日期分区')
row format delimited fields terminated by ','
stored as textfile;


create table if not exists `csx_tmp.tmp_cust_partner1` (
  `cust_type` STRING comment '类别',
  `province_name` STRING comment '省区',
  `city_name` STRING comment '城市',  
  `customer_no` STRING comment '编号',
  `customer_name` STRING comment '名称',
  `break_date` STRING comment '断约年月',
  `sdt` STRING comment '分区'
) comment '合伙人清单_v2'
row format delimited fields terminated by ','
stored as textfile;

load data inpath '/tmp/raoyanhua/cust_partner.csv' overwrite into table csx_tmp.tmp_cust_partner1;

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.tmp_cust_partner partition (sdt) 
select * from csx_tmp.tmp_cust_partner1;

--有销售的销售员名单及收入组
select b.work_no,b.sales_name,c.income_type,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(front_profit) front_profit
from
(
select province_code,province_name,customer_no,substr(sdt,1,6) smonth,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(front_profit) front_profit
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20201101'
and sdt<'20201201'
and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
and (channel in('1','7'))
group by province_code,province_name,customer_no,substr(sdt,1,6)
)a	
left join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='20201130') b on b.customer_no=a.customer_no
left join (select distinct work_no,income_type from csx_tmp.sales_income_info where sdt='20201031') c on c.work_no=b.work_no
group by b.work_no,b.sales_name,c.income_type;

--★★★★★★★★~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~★★★★★★★★
--★★★★★★★★首先确认需对哪些销售员补充收入组★★★★★★★★
--★★★★★★★★~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~★★★★★★★★


-- 昨日、昨日月1日，上月1日，上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
	
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');


set i_sdate_11 ='20201130';
set i_sdate_12 ='20201201';
	
set i_sdate_22 ='20201101';				
set i_sdate_23 ='20201130';


				

--7月及以后用
drop table csx_tmp.tmp_cust_partner2;
create table csx_tmp.tmp_cust_partner2
as
select distinct
  customer_no,
  substr(sdt,1,6)sdt
from csx_dw.csx_partner_list
where sdt<'202007'
union all
select distinct
  customer_no,
  substr(sdt,1,6)sdt
from csx_dw.dws_crm_w_a_customer_m_v1 
--where sdt= regexp_replace(date_sub(current_date,1),'-','')
where sdt in('20200731','20200831','20200930','20201031','20201130')
and source<>'dev' 
and customer_no<>''
and attribute_code='5';



---每日销售员提成系数（销额提成比例、前端毛利提成比例）
drop table csx_tmp.tmp_salesname_rate_ytd;
create table csx_tmp.tmp_salesname_rate_ytd
as
select sdt,work_no,sales_name,income_type,ytd,
case when ((ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
			or (ytd>10000000 and ytd<=20000000 and income_type in('Q2','Q3','Q4','Q5'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q3','Q4','Q5'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q4','Q5'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q5'))) then 0.002
	 when ((ytd>10000000 and ytd<=20000000 and income_type in('Q1'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q2'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q3'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q4'))
			or (ytd>50000000 and income_type in('Q5'))) then 0.0025
	 when ((ytd>20000000 and ytd<=30000000 and income_type in('Q1'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q2'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q3'))
			or (ytd>50000000 and income_type in('Q3','Q4'))) then 0.003
	 when ((ytd>30000000 and ytd<=40000000 and income_type in('Q1'))
			or (ytd>40000000 and income_type in('Q2'))) then 0.0035
	 when (ytd>40000000 and income_type in('Q1')) then 0.004			
else 0.002 end sale_rate,

case when ((ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
			or (ytd>10000000 and ytd<=20000000 and income_type in('Q2','Q3','Q4','Q5'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q3','Q4','Q5'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q4','Q5'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q5'))) then 0.1
	 when ((ytd>10000000 and ytd<=20000000 and income_type in('Q1'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q2'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q3'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q4'))
			or (ytd>50000000 and income_type in('Q5'))) then 0.125
	 when ((ytd>20000000 and ytd<=30000000 and income_type in('Q1'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q2'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q3'))
			or (ytd>50000000 and income_type in('Q3','Q4'))) then 0.15
	 when ((ytd>30000000 and ytd<=40000000 and income_type in('Q1'))
			or (ytd>40000000 and income_type in('Q2'))) then 0.175
	 when (ytd>40000000 and income_type in('Q1')) then 0.2			
else 0.1 end profit_rate
from 
	(select a.sdt,b.work_no,b.sales_name,coalesce(c.income_type,'Q1')income_type,
	sum(a.sales_value)over(PARTITION BY b.work_no,b.sales_name,substr(a.sdt,1,4) order by a.sdt ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING )ytd
	from 
		(select sdt,customer_no,substr(sdt,1,6) smonth,
				if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					) as sdt_last,  --sdt所在月最后1日，当月为昨日
		sum(sales_value) sales_value
		from csx_dw.dws_sale_r_d_customer_sale
		where sdt>='20200101' and sdt<${hiveconf:i_sdate_12} --昨日月1日
		and sales_type in('qyg','sapqyg','sapgc','sc','bbc')  
		and item_channel_code in('1','7')
		and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
						'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
		--签呈不考核，不算提成
		and customer_no not in('111118','103717','102755','104023','105673','104402')
		and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
		--签呈仅4月不考核，不算提成
		--and customer_no not in('PF0320','105177')
		group by sdt,customer_no,substr(sdt,1,6),
				if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					)
		)a 
	left join   --CRM信息取每月最后一天
		(select *
		from csx_dw.dws_crm_w_a_customer_m_v1 
		where sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
		and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					)  --sdt为每月最后一天
		)b on b.customer_no=a.customer_no and b.sdt=a.sdt_last 	
	left join (select distinct work_no,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_23}) c on c.work_no=b.work_no   --上月最后1日
	left join (select distinct customer_no,substr(sdt,1,6) smonth from csx_tmp.tmp_cust_partner2 ) d on d.customer_no=a.customer_no and d.smonth=a.smonth
	where d.customer_no is null
	)a;


--01、本月每天-销售员销额、最终前端毛利统计
drop table csx_tmp.temp_new_cust_00;
create table csx_tmp.temp_new_cust_00
as
select 
a.sales_province dist,a.customer_no cust_id,b.customer_name cust_name,b.work_no,b.sales_name,a.smonth,
coalesce(c.sale_rate,0.002) sale_rate,coalesce(c.profit_rate,0.1) profit_rate,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate,
round(sum(a.sales_value)*coalesce(c.sale_rate,0.002)+if(sum(a.front_profit)<0,0,coalesce(sum(a.front_profit),0)*coalesce(c.profit_rate,0.1)),2) salary
from 
(select sdt,substr(sales_date,1,6) smonth,sales_province,customer_no,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) as front_profit,
sum(front_profit)/sum(sales_value) as fnl_prorate
from csx_dw.dws_sale_r_d_customer_sale
where sdt>=${hiveconf:i_sdate_22} and sdt<${hiveconf:i_sdate_12} --昨日月1日
and sales_type in ('qyg','sapqyg','sapgc','sc','bbc') 
and item_channel_code in('1','7')
and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
--签呈不考核，不算提成
and customer_no not in('111118','103717','102755','104023','105673','104402')
and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
--签呈仅4月不考核，不算提成
--and customer_no not in('PF0320','105177')
--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
and customer_no not in('104179','112092')
--9月签呈 重庆 合伙人，9月剔除逾期和销售
--and customer_no not in('114265','114248','114401','111933','113080','113392')
--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
--and customer_no not in('109484')
group by sdt,substr(sales_date,1,6),sales_province,customer_no)a
left join 
	(select distinct customer_no,customer_name,work_no,sales_name
	from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_23}) b on b.customer_no=a.customer_no   --上月最后1日
left join 
	(select  work_no,sales_name,sdt,max(sale_rate) sale_rate,max(profit_rate) profit_rate
	from csx_tmp.tmp_salesname_rate_ytd where sdt>=${hiveconf:i_sdate_22} and sdt<${hiveconf:i_sdate_12}  --上月1日，昨日月1日
	group by work_no,sales_name,sdt
	)c on c.work_no=b.work_no and c.sales_name=b.sales_name and c.sdt=a.sdt
group by a.sales_province,a.customer_no,b.customer_name,b.work_no,b.sales_name,c.sale_rate,c.profit_rate,a.smonth;

--大前端毛利扣点后结果
drop table csx_tmp.temp_new_cust_01;
create table csx_tmp.temp_new_cust_01
as
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.smonth,
sum(a.sales_value) sales_value,
sum(a.profit) profit,
sum(a.profit)/sum(a.sales_value) prorate,
sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)) fnl_profit,
(sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)))/sum(a.sales_value) fnl_prorate,
--(a.fnl_prorate-coalesce(z.rate,0))*a.sales_value fnl_profit,
--(a.fnl_prorate-coalesce(z.rate,0))*a.sales_value/a.sales_value fnl_prorate,
round(sum(a.sales_value*coalesce(a.sale_rate,0.002))+
	  if((sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)))<=0,0,sum(coalesce(a.front_profit-a.sales_value*coalesce(z.rate,0),0)*coalesce(a.profit_rate,0.1))),2) salary	  
--round(a.sales_value*coalesce(a.sale_rate,0.002)+if((a.fnl_prorate-coalesce(z.rate,0))*a.sales_value<0,0,coalesce((a.fnl_prorate-coalesce(z.rate,0))*a.sales_value,0)*coalesce(a.profit_rate,0.1)),2) salary
from csx_tmp.temp_new_cust_00 a
left join
(  --福建区域大扣点 20200115
select '104824'cust_id, 0.02 rate
union all
select '104847'cust_id, 0.02 rate
union all
select '104854'cust_id, 0.02 rate
union all
select '104859'cust_id, 0.02 rate
union all
select '104870'cust_id, 0.02 rate
union all
select 'PF0649'cust_id, 0.09 rate
union all
select '102784'cust_id, 0.01 rate
union all
select '102901'cust_id, 0.01 rate
union all
select '102734'cust_id, 0.01 rate
union all
select '103372'cust_id, 0.03 rate
union all
select '103048'cust_id, 0.02 rate
union all
select '105249'cust_id, 0.02 rate
union all
select '106369'cust_id, 0.01 rate
union all
select '105150'cust_id, 0.1 rate
union all
select '105177'cust_id, 0.1 rate
union all
select '105182'cust_id, 0.1 rate
union all
select '105164'cust_id, 0.1 rate
union all
select '105181'cust_id, 0.1 rate
union all
select '105156'cust_id, 0.1 rate
union all
select '105165'cust_id, 0.1 rate
union all
select '106423'cust_id, 0.1 rate
union all
select '106721'cust_id, 0.1 rate
union all
select '106805'cust_id, 0.1 rate
union all
select '107404'cust_id, 0.1 rate
union all
select '105567'cust_id, 0.06 rate
union all
select '105399'cust_id, 0.01 rate
)z on z.cust_id=a.cust_id
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.smonth
;



--结果表1 


--02、销售员逾期系数
--当月提成 

drop table csx_tmp.temp_new_cust_salary;
create table csx_tmp.temp_new_cust_salary
as
select 
a.smonth,a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,
a.sales_value,a.profit,a.profit/a.sales_value prorate,
a.fnl_profit,a.fnl_profit/a.sales_value fnl_prorate,
a.salary,
b.receivable_amount,b.over_amt,
--b.over_rate cust_over_rate,
--if(a.salary<0 or b.over_rate is null,a.salary,a.salary*(1-coalesce(if(b.over_rate<=0.5,b.over_rate,1),0)) ) salary_1,
c.over_rate sale_over_rate,
if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ) salary_2
from  csx_tmp.temp_new_cust_01 a
left join (select distinct customer_no from csx_tmp.tmp_cust_partner2 where sdt=substr(${hiveconf:i_sdate_23},1,6)) d on d.customer_no=a.cust_id
left join csx_tmp.temp_cust_over_rate b on b.customer_no=a.cust_id
left join csx_tmp.temp_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
where d.customer_no is null; 

insert overwrite directory '/tmp/raoyanhua/tc_kehu' row format delimited fields terminated by '\t'
select * from csx_tmp.temp_new_cust_salary;

--销售员当月提成
insert overwrite directory '/tmp/raoyanhua/tc_xiaoshou' row format delimited fields terminated by '\t'
select smonth,dist,work_no,sales_name,
sum(sales_value)sales_value,
sum(profit)profit,
sum(profit)/sum(sales_value) prorate,
sum(fnl_profit)fnl_profit,
sum(fnl_profit)/sum(sales_value)fnl_prorate,
sum(salary)salary,
sum(receivable_amount)receivable_amount,
sum(over_amt)over_amt,
--sum(salary_1)salary_1,
sale_over_rate,
sum(salary_2)salary_2
from csx_tmp.temp_new_cust_salary
group by smonth,dist,work_no,sales_name,sale_over_rate ;




/*
-- 大提成：月度新
select b.sales_province,b.customer_no,b.customer_name,b.attribute,b.work_no,b.sales_name,b.sign_date,
	a.first_sales_date
from
(
select attribute,customer_no,customer_name,channel,sales_name,work_no,sales_province,
regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date,estimate_contract_amount*10000 estimate_contract_amount
from csx_dw.dws_crm_w_a_customer_m_v1
where sdt='current'
and customer_no<>''
and channel_code in('1','7')
)b
join
--最早销售月 新客月、新客季度
	(select customer_no,
	min(first_sales_date) first_sales_date
	from csx_tmp.ads_sale_w_d_customer_company_sales_date
	where sdt = 'current'
	group by customer_no
	having min(first_sales_date)>='20201001' and min(first_sales_date)<'20201101'
	)a on b.customer_no=a.customer_no;



---截至上月销售员的累计销售额
drop table csx_dw.dws_cust_ytd_sale;
create table csx_dw.dws_cust_ytd_sale
as
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select b.work_no,b.sales_name,
sum(a.sales_value)sales_value,
sum(a.profit)profit
from 
(select customer_no,substr(sdt,1,6) smonth,
sum(sales_value) sales_value,
sum(profit)profit
 from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200101' and sdt<${hiveconf:i_sdate_22} 
and sales_type in('qyg','sapqyg','sapgc','sc','bbc')  
and item_channel_code in('1','7')
group by customer_no,substr(sdt,1,6))a 
left join (select * from csx_dw.dws_crm_w_a_customer_m_v1 where sdt=${hiveconf:i_sdate_23}) b on b.customer_no=a.customer_no   --上月最后1日
left join (select distinct customer_no,substr(sdt,1,6) smonth from csx_tmp.tmp_cust_partner2 ) d on d.customer_no=a.customer_no and d.smonth=a.smonth
where d.customer_no is null
group by b.work_no,b.sales_name;




