
--客户数/签约客户数
 select
	sales_province_code,
	sales_province,
	SUM(all_cust) all_cust ,
	sum(sign_cust) sign_cust ,
	sum(last_all_cust) last_all_cust ,
	sum(last_sign_cust) last_sign_cust
from
	(
	select
		sales_province_code,
		sales_province,
		count(customer_no) as all_cust,
		count(case when substr(regexp_replace(to_date(sign_time), '-', ''), 1, 6)= substr(regexp_replace(CURRENT_DATE(), '-', ''), 1, 6) then customer_no end)as sign_cust ,
		0 last_all_cust ,
		0 last_sign_cust
	from
		csx_dw.customer_m
		
	where
		sdt = '20200113'
		and channel = '大客户'
		and customer_no != ''
	group by
		sales_province_code,
		sales_province
union ALL
	select
		sales_province_code,
		sales_province,
		0 all_cust,
		0 sign_cust,
		count(customer_no) as last_all_cust,
		count(case when substr(regexp_replace(to_date(sign_time), '-', ''), 1, 6)= substr(regexp_replace(add_months(CURRENT_DATE(),-1), '-', ''), 1, 6) then customer_no end)as last_sign_cust
	from
		csx_dw.customer_m
	where
		sdt = '20191213'
		and channel = '大客户'
		and customer_no != ''
	group by
		sales_province_code,
		sales_province ) a
group by
	sales_province_code,
	sales_province;
-- 成交客户数
 select
	province_code,
	province_name,
	sum(sale_cust) sale_cust ,
	sum(mom_cust) mom_cust
from
	(
	select
		province_code,
		province_name,
		COUNT(DISTINCT customer_no ) as sale_cust,
		0 mom_cust
	from
		csx_dw.customer_sales
	where
		sdt >= '20200101'
		and sdt <= '20200113'
		and channel in ('1',
		'3',
		'7')
	group by
		province_code,
		province_name
union all
	select
		province_code,
		province_name,
		0 sale_cust,
		COUNT(DISTINCT customer_no ) as mom_cust
	from
		csx_dw.customer_sales
	where
		sdt >= '20191201'
		and sdt <= '20191231'
		and channel in ('1',
		'3',
		'7')
	group by
		province_code,
		province_name )a
group by
	province_code,
	province_name;
--拜访数据
 select
	sales_province_code,
	sales_province_name,
	count(DISTINCT customer_id ) as visit_cust
from
	csx_dw.customer_visit
where
	sdt >= '20200101'
	and customer_no = ''
group by
	sales_province_code,
	sales_province_name ;


--客户类型销售
 DROP table csx_dw.provinces_attribute_sale;

CREATE table csx_dw.provinces_attribute_sale as
select
	attribute_name ,
	sale,
	profit,
	profit / sale profit_rate,
	cust_num,
	ring_sale,
	ring_profit ,
	ring_profit / ring_sale ring_profit_rate,
	ring_cust_num ,
	cust_num-ring_cust_num as diff_cust_num,
	(sale-ring_sale)/ ring_sale as mom_sale_ratio,
	sale / sum(sale)over() as sale_ratio
from
	(
	select
		attribute_name ,
		sum(case when sdt >= '20200101' and sdt <= '20200114' then sales_value end ) sale,
		sum(case when sdt >= '20200101' and sdt <= '20200114' then profit end ) profit,
		COUNT(DISTINCT case when sdt >= '20200101' and sdt <= '20200114' then customer_no end ) cust_num,
		sum(case when sdt >= '20191201' and sdt <= '20191214' then sales_value end ) as ring_sale,
		sum(case when sdt >= '20191201' and sdt <= '20191214' then profit end )as ring_profit,
		COUNT(DISTINCT case when sdt >= '20191201' and sdt <= '20191214' then customer_no end )as ring_cust_num
	from
		csx_dw.customer_sales
	where
		sdt >= '20191201'
		and sdt <= '20200114'
	group by
		attribute_name )a ;

select
	attribute_name ,
	sale / 10000 as sale ,
	profit / 10000 as profit,
	profit_rate,
	cust_num,
	ring_sale / 10000 as ring_sale,
	ring_profit / 10000 as ring_profit ,
	ring_profit_rate,
	ring_cust_num ,
	diff_cust_num,
	mom_sale_ratio,
	sale_ratio
from
	csx_dw.provinces_attribute_sale;
-- 回款金额&应收款
 select
	SUM(cash_amt)/ 10000 cash_amt ,
	SUM(case when sdt = '20200114' then ac_all-ac_wdq end ) ac_all
from
	csx_dw.receivables_collection
where
	sdt >= '20200101'
	and sdt <= '20200114';

-- 应收款逾期客户数'逾期客户数'as note,
select sales_province,sales_province_code,sum(ac_15d )ac_15d ,sum(ac_30d) as ac_30d,sum(ac_60d)as ac_60d,sum(ac_90d)as ac_90d,sum(ac_120d)as ac_120d,
sum(ac_180d)as ac_180d,sum(ac_365d)as　ac_365d,sum(ac_2y )as ac_2y ,sum(ac_3y )as ac_3y ,sum(ac_over3y )as ac_over3y 
from
(SELECT regexp_replace(kunnr,'(^0*)','')as customer_no,sum(ac_15d )ac_15d ,sum(ac_30d) as ac_30d,sum(ac_60d)as ac_60d,sum(ac_90d)as ac_90d,sum(ac_120d)as ac_120d,
sum(ac_180d)as ac_180d,sum(ac_365d)as　ac_365d,sum(ac_2y )as ac_2y ,sum(ac_3y )as ac_3y ,sum(ac_over3y )as ac_over3y 	
FROM csx_dw.account_age_dtl_fct_new  cc where sdt='20200115' group by regexp_replace(kunnr,'(^0*)','')) a 
join 
(select cm.sales_province ,cm.sales_province_code,cm.customer_no from csx_dw.customer_m  cm where sdt='20200115' ) b 
on a.customer_no=b.customer_no
group by sales_province,sales_province_code
;

-- 应收款逾期客户数'逾期客户数'as note,
select sales_province,sales_province_code,b.customer_name,a.customer_no,
from
(SELECT regexp_replace(kunnr,'(^0*)','')as customer_no,sum(	)ac_all,sum(ac_wdq)ac_wdq,sum(ac_15d )ac_15d ,sum(ac_30d) as ac_30d,sum(ac_60d)as ac_60d,sum(ac_90d)as ac_90d,sum(ac_120d)as ac_120d,
sum(ac_180d)as ac_180d,sum(ac_365d)as　ac_365d,sum(ac_over365d )as ac_over365d 
FROM csx_dw.account_age_dtl_fct  cc where sdt='20200115' group by regexp_replace(kunnr,'(^0*)','')) a 
join 
(select cm.sales_province ,cm.sales_province_code,cm.customer_no,customer_name,sales_name from csx_dw.customer_m  cm where sdt='20200115'  and cm.customer_no!='') b 
on a.customer_no=b.customer_no
group by sales_province,sales_province_code
;

-- 销售员TOP10 逻辑规则： 负毛利剔除、取日配客户
select
	sales_name,
	work_no,
	province_code,
	province_name,
	sum(sale)/10000 as sale,
	sum(profit)/10000 as profit,
	sum(profit)/sum(sale) as profit_rate,
	sum(cust_num)as cust_num,
	sum(last_sale)/10000 as last_sale,
	sum(last_profit)/10000 as last_profit,
	sum(last_cust_num)as last_cust_num ,
	sum(sale-last_sale)/sum(last_sale) as ring_sale_ratio
from 
(
select
		sales_name,
		work_no,
		province_code,
		province_name,
		sum(sales_value)sale,
		sum(profit)profit,
		COUNT(DISTINCT customer_no)cust_num ,
		0 as last_sale ,
		0 as last_profit,
		0 as last_cust_num
	from
		csx_dw.customer_sales
	where
		sdt >= '20200101'
		and province_code = '32'
		and attribute_name in('日配客户')
	group by
		sales_name,
		work_no,
		province_code,
		province_name
union all
	select
		sales_name,
		work_no,
		province_code,
		province_name,
		0 as sale ,
		0 as profit,
		0 as cust_num,
		sum(sales_value)as last_sale,
		sum(profit)as last_profit,
		COUNT(DISTINCT customer_no)as last_cust_num
	from
		csx_dw.customer_sales
	where
		sdt >= '20191201'
		and sdt <= '20191215'
		and province_code = '32'
		and attribute_name in('日配客户')
	group by
		sales_name,
		work_no,
		province_code,
		province_name )a 
--where profit>0
group by
		sales_name,
		work_no,
		province_code,
		province_name
HAVING SUM(profit)>0
order by SUM(sale) desc
;
-- 客户TO10

select province_code,sales_name,cust_id,cust_name,cust_num,sale,profit,profit/sale*1.00 prorate,
rank()over(order by sale desc) as desc_rank,
sale/sum(sale)over(PARTITION by province_code,attribute_name)as ratio
from
(
select    
	customer_no cust_id,
	customer_name as cust_name,
	attribute_name,
	province_code,
	province_name,
	sales_name,
	count(distinct case when sales_value <> 0 then sdt end )cust_num,
	sum(sales_value)sale,
	sum(profit)profit
from
	csx_dw.customer_sales
where
	sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and sdt>=regexp_replace(to_date(trunc(date_sub(current_timestamp(),1),'MM')),'-','')
and channel  in ('1','7','3')
and province_code='32'
and attribute_name in ('日配客户')
group by customer_no ,
	customer_name ,
	attribute_name,
	province_code,
	province_name,
	sales_name
)a
where profit>0
order by sale desc
limit 10
;
--select  to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'MM'))
--;
select province_code,sales_name,cust_id,cust_name,case when sign_date>=to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'MM')) then 'new' else 'old' end note,
cust_num,sale,profit,profit/sale*1.00 prorate,
rank()over(order by sale desc) as desc_rank,
sale/sum(sale)over(PARTITION by province_code,attribute_name)as ratio,
sign_date
from
(
select    
	customer_no cust_id,
	customer_name as cust_name,
	to_date(sign_time) as sign_date,
	attribute_name,
	province_code,
	province_name,
	sales_name,
	count(distinct case when sales_value <> 0 then sdt end )cust_num,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit
from
	csx_dw.customer_sales
where
	sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and sdt>=regexp_replace(to_date(trunc(date_sub(current_timestamp(),1),'MM')),'-','')
and channel  in ('1','7','3')
and province_code='32'
and attribute_name in ('日配客户')
group by customer_no ,
	customer_name ,
	attribute_name,
	province_code,
	province_name,
	sales_name,
	to_date(sign_time)
)a
where profit>0
order by sale desc
limit 10
;

-- 负毛利客户

select province_code,sales_name,cust_id,cust_name,case when sign_date>=to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'MM')) then 'new' else 'old' end note,
cust_num,sale,profit,profit/sale*1.00 prorate,
rank()over(order by sale desc) as desc_rank,
sale/sum(sale)over(PARTITION by province_code,attribute_name)as ratio,
sign_date
from
(
select    
	customer_no cust_id,
	customer_name as cust_name,
	to_date(sign_time) as sign_date,
	attribute_name,
	province_code,
	province_name,
	sales_name,
	count(distinct case when sales_value <> 0 then sdt end )cust_num,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit
from
	csx_dw.customer_sales
where
	sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and sdt>=regexp_replace(to_date(trunc(date_sub(current_timestamp(),1),'MM')),'-','')
and channel  in ('1','7','3')
and province_code='32'
and attribute_name in ('日配客户')
group by customer_no ,
	customer_name ,
	attribute_name,
	province_code,
	province_name,
	sales_name,
	to_date(sign_time)
)a
where profit<0
order by profit asc
limit 10
;

-- 负毛利商品TOP10


select goods_code,
	goods_name,
	unit,	
	province_code,
	province_name,
goods_num,
sales_cost/qty as avg_sales_cost,
sale/qty as avg_price,
sales_cost,qty,sale,profit,
rank()over(order by sale desc) as desc_rank,
rank()over(order by profit asc )as profit_asc
from
(
select    
	goods_code,
	goods_name,
	unit,	
	province_code,
	province_name,
	count(distinct case when sales_value <> 0 then sdt end )as goods_num,
	sum(sales_sales_cost)/10000 sales_cost,
	sum(sales_qty) qty,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit
from
	csx_dw.sale_goods_m1
where
	sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and sdt>=regexp_replace(to_date(trunc(date_sub(current_timestamp(),1),'MM')),'-','')
and channel  in ('1','7','3')
and province_code='32'
-- and attribute_name in ('日配客户')
group by goods_code,
	goods_name,
	unit,	
	province_code,
	province_name
)a
where profit<0
order by profit asc
limit 10
;

-- 库区\课组

select province_code,province_name ,department_id,department_name,
concat(department_id,department_name)as dept,reservoir_area_code,
concat(reservoir_area_code,reservoir_area_name) as area,sum(amt)as amt from csx_dw.wms_accounting_stock_m a 
join 
(select * from csx_dw.shop_m where sdt='current')b 
on regexp_replace(a.dc_code,'(^E)','9')=regexp_replace(b.shop_id,'(^E)','9')
where a.sdt='20200115' and province_code='500000'
group by province_code,province_name ,department_id,department_name,
concat(department_id,department_name),
concat(reservoir_area_code,reservoir_area_name) ,reservoir_area_code
;


select receive_location_code,0 receive_amt,sum(receive_amount) amt,sum(receive_qty) from csx_dw.order_flow where receive_sdt='20200113' and goods_code='480' group by receive_location_code;



select *  from csx_dw.wms_entry_order_all_m 
where sdt='20200113' and receive_location_code='W048'  and goods_code='480';

--回款与应收款
select note, abs(sum(cash_amt)) as cash_amt from (
select '回款额' as note,round(SUM(cash_amt )/10000,0)as  cash_amt from csx_dw.receivables_collection where sdt>='20200101' and sdt<='20200114'
union all 
select '应收款' as note,round(SUM(case when sdt='20200114' then ac_all-ac_wdq end )/10000 ,0)*1.00 as cash_amt
from csx_dw.receivables_collection where sdt>='20200101' and sdt<='20200114'
) a 
group by note ;
select * from csx_dw.wms_accounting_stock_m where sdt='20200116' and dc_name like '福建%';



REFRESH csx_dw.factory_order;
select
	transfer_out_province_code province_code,
	transfer_out_province_name province_name,
	sum(plan_receive_qty)plan_qty,
	sum(reality_receive_qty)reality_qty,
	sum(user_qty)user_qty,
	sum(return_qty)return_qty,
	sum(goods_reality_receive_qty)goods_user_qty
from
	csx_dw.factory_order
where
	sdt >= '20200120'
	and sdt <= '20200121'
	and transfer_out_province_code = '500000'
group by
	transfer_out_province_code,
	transfer_out_province_name;

--库存表
-- BZ	标准区
-- PD	盘点待区
-- PD	盘点待处理区
-- TH	退货区
-- TS	在途区
drop table temp.temp_dept_rk;
create temporary table temp.temp_dept_rk
as 
select province_code,province_name ,department_id,dept, area,
amt,sum(amt )over(partition by department_id,province_code)as all_amt
from (
select province_code,province_name ,department_id,department_name,
concat(reservoir_area_type_code,reservoir_area_type_name) area,
concat(department_id,department_name)as dept,
sum(amt)as amt from csx_dw.wms_accounting_stock_m a 
join 
(select * from csx_dw.shop_m where sdt='current')b 
on regexp_replace(a.dc_code,'(^E)','9')=regexp_replace(b.shop_id,'(^E)','9')
join
(select reservoir_area_code,reservoir_area_type_code,reservoir_area_type_name,warehouse_code from csx_ods.wms_reservoir_area_ods) c on 
regexp_replace(a.dc_code,'(^E)','9')=regexp_replace(c.warehouse_code,'(^E)','9') and a.reservoir_area_code=c.reservoir_area_code
where a.sdt='20200115' and province_code='500000'
group by province_code,province_name ,department_id,department_name,
concat(reservoir_area_type_code,reservoir_area_type_name)
)a 
;
create temporary table temp.temp_rk_01 as 
 select province_code,province_name,department_id,department_name,sum(a.amount)amount from
(select receive_location_code,goods_code,sum(price*receive_qty)amount from csx_dw.wms_entry_order_all_m where sdt='20200115'
 group by receive_location_code,goods_code )a
join 
(select goods_id,department_id,department_name from csx_dw.goods_m where sdt='20200116')  b 
on a.goods_code=b.goods_id
join
(select shop_id,province_code,province_name from csx_dw.shop_m where sdt='current' ) c 
on regexp_replace(a.receive_location_code,'(^E)','9')=regexp_replace(c.shop_id,'(^E)','9')
group by province_code,province_name,department_id,department_name;

create temporary table temp.temp_rk_02 as 
select c.province_code,c.province_name,dept_id,dept_name,sum(sales_value)sale,sum(case when sdt='20200115' then sales_value end ) as day_sae
from csx_dw.sale_goods_m1 a 
join
(select shop_id,province_code,province_name from csx_dw.shop_m where sdt='current' ) c 
on regexp_replace(a.shop_id,'(^E)','9')=regexp_replace(c.shop_id,'(^E)','9')
and  sdt>='20200101' and sdt<='20200115'
group by c.province_code,c.province_name,dept_id,dept_name
;

create table csx_dw.provinces_kanban_inv
as 
select a.province_code,a.province_name,a.department_id,a.dept,a.area,a.area,a.amt,a.all_amt,b.amount,c.day_sae,c.sale from 
temp.temp_dept_rk a 
left join 
 temp.temp_rk_01  b on a.province_code=b.province_code and a.department_id=b.department_id
 left join 
 temp.temp_rk_02 c on a.province_code=c.province_code and a.department_id=c.dept_id;
 
 select * from csx_dw.provinces_kanban;
 
 select * from csx_ods.wms_reservoir_area;
 
select * from csx_dw.customer_sale_m ;
SELECT regexp_replace(to_date(trunc(now(),'MM')),'-','');

-- 负毛利商品TOP10


select goods_code,
	goods_name,
	unit,	
	province_code,
	province_name,
goods_num,
cost*10000/qty as avg_cost,
sale*10000/qty as avg_price,
cost,qty,sale,profit,
rank()over(order by sale desc) as desc_rank,
rank()over(order by profit asc )as profit_asc
from
(
select    
	goods_code,
	goods_name,
	unit,	
	province_code,
	province_name,
	count(distinct case when sales_value <> 0 then sdt end )as goods_num,
	sum(sales_cost)/10000 cost,
	sum(sales_qty) qty,
	sum(sales_value)/10000 sale,
	sum(profit)/10000 profit
from
	csx_dw.sale_goods_m1
where
	sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and sdt>=regexp_replace(to_date(trunc(date_sub(current_timestamp(),1),'MM')),'-','')
and channel  in ('1','7','3')
and province_code='32'
and return_flag!='X'
-- and attribute_name in ('日配客户')
group by goods_code,
	goods_name,
	unit,	
	province_code,
	province_name
)a
where profit<0
order by profit asc
limit 10
;
select cast (`limit` as decimal (26,0)) as  province_code,province from csx_ods.sys_province_ods_v2;
select * from csx_dw.provinces_kanban where sdt='20200206';

select --sales_province_code province_code,
note, 
sum(cash_amt)over(PARTITION by note) as cash_amt 
from (
select '回款额' as note,--sales_province_code,
round(SUM(cash_amt )/10000,0)as  cash_amt 
from csx_dw.receivables_collection
where sdt>=regexp_replace(to_date(trunc(date_sub(now(),1),'MM')),'-','') 
and sdt<=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
and  sales_province_code='32'
--group by sales_province_code
union all 
select '应收款' as note,--sales_province_code,
round(SUM(case when sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') then ac_all-ac_wdq end )/10000 ,0)*1.00 as cash_amt
from csx_dw.receivables_collection where 
sdt>=regexp_replace(to_date(trunc(date_sub(now(),1),'MM')),'-','') 
and sdt<=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
and sales_province_code='32'
union all 
select * from csx_dw.report_account_age where dt='20200206'
--group by sales_province_code
) a 
--where sales_province_code='32'
--group by note
;
select * from csx_dw.receivables_collection;

select province_code,sales_name,customer_no,customer_name,note,
cust_num,sale,profit,prorate,
desc_rank,
ratio,
sign_date
from
 csx_dw.provinces_kanban_cust_lose a 
where profit<0
and province_code='32'
and type='up'
order by profit asc
limit 10;