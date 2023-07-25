-- 销售频次分析
drop table `csx_dw.provinces_kanban_frequency`;
CREATE TABLE `csx_dw.provinces_kanban_frequency`(
 `province_code` string comment '省区编码', 
 `province_name` string comment'省区名称', 
 `sale` decimal(38,6) comment '销售额', 
 `cust_num` bigint comment '数', 
 `sale_num` bigint comment '交易频次', 
 `zones` string comment '组别', 
 `zone_num` string comment '区间')comment '看板频次分析'
 partitioned by (sdt string comment '分区日期')
 ;
 
 set
mapreduce.job.queuename = caishixian;
set hive.exec.dynamic.partition.mode=nonstrict;
drop table if exists temp.cust_num;
create temporary table if not exists temp.cust_num
as
select 
province_code,
	province_name,
	customer_no,
	customer_name,
	sale,
	sale_num,
	case when sale_num in (1,2) then '1'
		when sale_num >=3 and sale_num<=5 then '2'
		when sale_num >=6 and sale_num<=10 then '3'
		when sale_num >=11 and sale_num<=15 then '4'
		when sale_num >=16 and sale_num<=20 then '5'
		when sale_num >=21 then '6'
		else '7'
	end zones,
	case when sale_num in (1,2) then '1-2'
		when sale_num >=3 and sale_num<=5 then '3-5'
		when sale_num >=6 and sale_num<=10 then '6-10'
		when sale_num >=11 and sale_num<=15 then '11-15'
		when sale_num >=16 and sale_num<=20 then '16-20'
		when sale_num >=21 then '21以上'
	end zone_num
from(
select
	province_code,
	province_name,
	customer_no,
	customer_name,
	sum(sales_value)sale,
	COUNT(distinct sdt) sale_num
from
	csx_dw.customer_sale_m
where
	sdt >= '20200201'
	and sdt <= '20200205'
	and channel in ('1','3','7')
	group by province_code,
	province_name,
	customer_no,
	customer_name
	)a
;
insert overwrite table csx_dw.provinces_kanban_frequency partition(sdt)
select 
	province_code,
	province_name,
	sum(sale)sale,
	COUNT(customer_no)cust_num,
	SUM(sale_num)sale_num,
	zones,
	zone_num,
	regexp_replace(date_sub(current_date(),1),'-','')
	from temp.cust_num a group by province_code,
	province_name,
	zones,
	zone_num
	;
	
 
 
 -- 数
 drop table `csx_dw.provinces_kanban_cust`;
 CREATE TABLE `csx_dw.provinces_kanban_cust`(
`province_code` string, 
`province_name` string, 
`cust_num` bigint, 
`mom_cust_num` bigint, 
`sing_num` bigint, 
`mom_sing_num` bigint, 
`big_dept_cust` bigint, 
`mom_big_cust` bigint)
comment '看板成交'
 partitioned by (sdt string comment '分区日期')
 ;
 
 set sdate=regexp_replace(date_sub(current_date,1),'-','');
set mdate=trunc(date_sub(current_date,1),'MM');
set l_sdate=regexp_replace(add_months(date_sub(current_date,1),-1),'-','');
set l_mdate=add_months(trunc(date_sub(current_date,1),'MM'),-1);
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists temp.cust_num_01;
create temporary table if not exists temp.cust_num_01
as 
select a.province_code,a.province_name,cust_num,mom_cust_num,sing_num,mom_sing_num,big_dept_cust,mom_big_cust from
(select  nvl(sales_province_code,'') province_code,sales_province province_name,
count(case when trunc(to_date(sign_time),'MM')=${hiveconf:mdate} then customer_no end)as sing_num,
count(customer_no )as cust_num
from csx_dw.customer_m
where sdt=${hiveconf:sdate}
and customer_no!=''
and sales_province_code!=''
group by sales_province_code,sales_province
)a
left join
(select nvl(sales_province_code,'') province_code,sales_province province_name,
count(case when trunc(to_date(sign_time),'MM')=${hiveconf:l_mdate} then customer_no end)as mom_sing_num,
count(customer_no )as mom_cust_num
from csx_dw.customer_m
where sdt=${hiveconf:l_sdate}
and customer_no!=''
and sales_province_code!=''
group by sales_province_code,sales_province
)b on a.province_code=b.province_code 
left join
(select province_code,province_name,big_dept_cust,mom_big_cust from csx_dw.provinces_kanban 
where sdt=${hiveconf:sdate} and workshop_name='物流' and province_code!=''
) c on a.province_name=c.province_name
where a.province_code!=''

;
insert overwrite table csx_dw.provinces_kanban_cust partition(sdt)
select *,regexp_replace(date_sub(current_date(),1),'-','') from temp.cust_num_01;
--show create table  csx_dw.provinces_kanban_cust;

-- 帐龄分析
drop table `csx_dw.provinces_kanban_account_age`;
CREATE TABLE `csx_dw.provinces_kanban_account_age`(
`province_name` string, 
`province_code` string, 
`ac_15d` decimal(38,6), 
`ac_30d` decimal(38,6), 
`ac_60d` decimal(38,6), 
`ac_90d` decimal(38,6), 
`ac_120d` decimal(38,6), 
`ac_180d` decimal(38,6), 
`ac_365d` decimal(38,6), 
`ac_over365d` decimal(38,6))
comment '帐龄分析'
partitioned by (sdt string comment'日期分区');

-- 帐龄分析
insert overwrite table csx_dw.provinces_kanban_account_age partition(sdt)
 
select nvl(sales_province,'') province_name ,nvl(sales_province_code,'') province_code,sum(ac_15d )ac_15d ,sum(ac_30d)as ac_30d,sum(ac_60d)as ac_60d,sum(ac_90d)as ac_90d,sum(ac_120d)as ac_120d,
sum(ac_180d)as ac_180d,sum(ac_365d)as ac_365d,sum(ac_over365d )as ac_over365d ,	regexp_replace(date_sub(current_date(),1),'-','')
from
(SELECT regexp_replace(kunnr,'(^0*)','')as customer_no,sum(ac_15d )ac_15d ,sum(ac_30d) as ac_30d,sum(ac_60d)as ac_60d,sum(ac_90d)as ac_90d,sum(ac_120d)as ac_120d,
sum(ac_180d)as ac_180d,sum(cc.ac_over365d)as ac_over365d,sum(cc.ac_365d)	ac_365d
FROM csx_dw.account_age_dtl_fct cc where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')  and cc.ac_all>0
group by regexp_replace(kunnr,'(^0*)','')) a 
join 
(select cm.sales_province ,cm.sales_province_code,cm.customer_no from csx_dw.customer_m  cm where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') ) b 
on a.customer_no=b.customer_no
--and sales_province_code='32'
group by sales_province,sales_province_code
;

--逾期
drop table csx_dw.provinces_kanban_account_cust;
create table csx_dw.provinces_kanban_account_cust
as
select 
  regexp_replace(kunnr, '(^0*)', '') as customer_no,customer_name,sales_province_code,sales_province,sales_name,attribute_name,
  sum(ac_all) ac_all ,
  sum(ac_wdq) ac_wdq ,
  sum((ac_all-ac_wdq)/10000 )as over_amt ,
  sum(ac_15d)ac_15d  ,
  sum(ac_30d) ac_30d ,
  sum(ac_60d)ac_60d  ,
  sum(ac_90d) ac_90d ,
  sum(ac_120d)ac_120d,
  sum(ac_180d)ac_180d,
  sum(ac_365d)ac_365d,
  sum(ac_over365d)ac_over365d  
FROM csx_dw.account_age_dtl_fct cc
	join
		(
			select
				cm.sales_province     ,
				cm.sales_province_code,
				cm.customer_no        ,
				customer_name         ,
				sales_name,
				cm.`attribute` as attribute_name
			from
				csx_dw.customer_m cm
			where
				sdt                =regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
				and cm.customer_no!=''
		)
		b
		on
			regexp_replace(kunnr, '(^0*)', '')=b.customer_no
			and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
			and sales_province_code='32'
group by regexp_replace(kunnr, '(^0*)', '') ,customer_name,sales_province_code,sales_province,sales_name,attribute_name
;


-- 销售员TOP10 
select
	sales_name,
	work_no,
	province_code,
	province_name,
	sale,
	profit,
	profit_rate,
	cust_num,
	last_sale,
	last_profit,
	last_cust_num ,
	ring_sale_ratio
from 
csx_dw.provinces_kanban_sales_top10 a 
where province_code='32'
and profit>0 
order by sale desc
limit 10
;
drop table `csx_dw.provinces_kanban_sales_top10`;
CREATE TABLE `csx_dw.provinces_kanban_sales_top10`(
 `sales_name` string, 
 `work_no` string, 
 `province_code` string, 
 `province_name` string, 
 `channel_name` string, 
 `sale` decimal(38,6), 
 `profit` decimal(38,6), 
 `profit_rate` decimal(38,6), 
 `cust_num` bigint, 
 `last_sale` decimal(38,6), 
 `last_profit` decimal(38,6), 
 `last_cust_num` bigint, 
 `ring_sale_ratio` decimal(38,6),
 rank_sale string comment '销售排名'
 )
 comment '销售员TOP10'
 PARTITIONed BY (sdt string comment'日期分区')
 STORED AS PARQUET
 ;
  -- 销售员TOP10 
insert overwrite table `csx_dw.provinces_kanban_sales_top10` partition(sdt)
select
	sales_name,
	work_no,
	province_code,
	province_name,
	channel_name,
	sale,
	profit,
	profit_rate,
	cust_num,
	last_sale,
	last_profit,
	last_cust_num ,
	ring_sale_ratio,
	row_number()over(partition by province_code,channel_name order by sale desc) as rank_sale,
	${hiveconf:edate}
from 
(
 SELECT 
 	sales_name,
	work_no,
	province_code,
	province_name,
	channel_name,
	sum(sale) as sale,
	sum(profit) as profit,
	sum(profit)/sum(sale) as profit_rate,
	sum(cust_num)as cust_num,
	sum(last_sale) as last_sale,
	sum(last_profit) as last_profit,
	sum(last_cust_num)as last_cust_num ,
	(sum(sale)-sum(last_sale))/sum(last_sale) as ring_sale_ratio
from (
select
		sales_name,
		work_no,
		province_code,
		province_name,
		case when channel in ('1','3','7') then '大' 
		    when channel in ('2') then '商超'
		    else channel end channel_name,
		sum(sales_value)sale,
		sum(profit)profit,
		COUNT(DISTINCT customer_no)cust_num ,
		0 as last_sale ,
		0 as last_profit,
		0 as last_cust_num
	from
		csx_dw.customer_sales
	where
		sdt >= regexp_replace(${hiveconf:mdate},'-','')
	and 	sdt<=${hiveconf:edate}
	group by
		sales_name,
		work_no,
		province_code,
		province_name,
		case when channel in ('1','3','7') then '大' 
		    when channel in ('2') then '商超'else channel end 
union all
	select
		sales_name,
		work_no,
		province_code,
		province_name,
		case when channel in ('1','3','7') then '大' 
		    when channel in ('2') then '商超' else channel end channel_name,
		0 as sale ,
		0 as profit,
		0 as cust_num,
		sum(sales_value)as last_sale,
		sum(profit)as last_profit,
		COUNT(DISTINCT customer_no)as last_cust_num
	from
		csx_dw.customer_sales
	where
		sdt >= regexp_replace(${hiveconf:l_mdate},'-','') 
	and	sdt<=${hiveconf:l_edate}

		-- and province_code = '32'
		-- and attribute_name in('日配')
	group by
		sales_name,
		work_no,
		province_code,
		province_name,
		case when channel in ('1','3','7') then '大' 
		    when channel in ('2') then '商超' else channel end )a 
--where profit>0
group by
		sales_name,
		work_no,
		channel_name,
		province_code,
		province_name
) a
;

--负毛利TOP10
drop table `csx_dw.provinces_kanban_cust_lose`;
CREATE TABLE `csx_dw.provinces_kanban_cust_lose`(
type string comment 'lose 负毛利 up 销售top'
`province_code` string, 
`sales_name` string, 
`cust_id` string, 
`cust_name` string, 
`note` string, 
`attribute_name` string, 
`cust_num` bigint, 
`sale` decimal(38,6), 
`profit` decimal(38,6), 
`prorate` decimal(38,6), 
`desc_rank` int, 
`ratio` decimal(38,6), 
`sign_date` string)comment '负毛利'
partitioned by (sdt string comment'日期分区')
;

--负毛利TOP10
insert overwrite  table csx_dw.provinces_kanban_cust_lose partition(sdt)
select 'lose' type,
province_code,
sales_name,
customer_no,
customer_name,
case when sign_date>=to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'MM')) then 'new' else 'old' end note,
attribute_name,
cust_num,
sale,
profit,
profit/sale*1.00 prorate,
rank()over(order by sale desc) as desc_rank,
sale/sum(sale)over(PARTITION by province_code,attribute_name)as ratio,
sign_date,
regexp_replace(date_sub(current_date(),1),'-','')
from
(
select    
	customer_no ,
	customer_name ,
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
--and province_code='32'
--and attribute_name in ('日配')
group by customer_no ,
	customer_name ,
	attribute_name,
	province_code,
	province_name,
	sales_name,
	to_date(sign_time)
)a
where profit<0
;
insert overwrite  table csx_dw.provinces_kanban_cust_lose partition(sdt)
-- TOP10 
select 'up' type ,
province_code,
sales_name,
customer_no,
customer_name,
case when sign_date>=to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'MM')) then 'new' else 'old' end note,
attribute_name,
cust_num,
sale,
profit,
profit/sale*1.00 prorate,
rank()over(order by sale desc) as desc_rank,
sale/sum(sale)over(PARTITION by province_code,attribute_name)as ratio,
sign_date,
regexp_replace(date_sub(current_date(),1),'-','')
from
(
select    
	customer_no ,
	customer_name ,
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
and attribute_name in ('日配')
group by customer_no ,
	customer_name ,
	attribute_name,
	province_code,
	province_name,
	sales_name,
	to_date(sign_time)
)a
where profit>0
;

--负毛利商品
DROP table `csx_dw.provinces_kanban_goods_lose`;
CREATE TABLE `csx_dw.provinces_kanban_goods_lose`(
 `goods_code` string, 
 `goods_name` string, 
 `unit` string, 
 `province_code` string, 
 `province_name` string, 
 `goods_num` bigint, 
 `avg_cost` decimal(38,6), 
 `avg_price` decimal(38,6), 
 `cost` decimal(38,6), 
 `qty` decimal(36,6), 
 `sale` decimal(38,6), 
 `profit` decimal(38,6), 
 `desc_rank` int comment'销售额排名', 
 `profit_asc` int comment '毛利额排名')
 comment'商品负毛利'
 partitioned by (sdt string comment'日期分区')
 STORED AS PARQUET;
 --插入负毛利商品数据
-- drop table csx_dw.provinces_goods_lose;
insert overwrite table csx_dw.provinces_kanban_goods_lose partition(sdt)
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
rank()over(order by profit asc )as profit_asc,
regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
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
	csx_dw.customer_sale_m
where
	sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and sdt>=regexp_replace(to_date(trunc(date_sub(current_timestamp(),1),'MM')),'-','')
and channel  in ('1','7','3')
and province_code='32'
-- and attribute_name in ('日配')
group by goods_code,
	goods_name,
	unit,	
	province_code,
	province_name
)a
where profit<0
;
 
 CREATE TABLE `csx_dw.provinces_attribute_sale`(
`attribute_name` string comment '属性', 
province_code string comment '省区编码',
province_name string comment'省区名称',
`sale` decimal(36,6) comment '本月销售额', 
`profit` decimal(36,6) comment '本月毛利额', 
`profit_rate` decimal(38,6) comment '本月毛利率', 
`cust_num` bigint comment '本月数', 
`ring_sale` decimal(36,6) comment '上月销售额', 
`ring_profit` decimal(36,6) comment '上月毛利额', 
`ring_profit_rate` decimal(38,6) comment '上月毛利率', 
`ring_cust_num` bigint comment '上月数', 
`diff_cust_num` bigint comment '环比差异', 
`mom_sale_ratio` decimal(38,6) comment '销售环比', 
`sale_ratio` decimal(38,6) comment '销售占比')
comment '省区属性销售占比'
partitioned by (sdt string comment'日期分区')

set edate=regexp_replace(date_sub(current_date,1),'-','');
set sdate=trunc(date_sub(current_date,1),'MM');
set l_edate=regexp_replace(add_months(date_sub(current_date,1),-1),'-','');
set l_sdate=add_months(trunc(date_sub(current_date,1),'MM'),-1);
set hive.exec.dynamic.partition.mode=nonstrict;
--DROP table if EXISTS temp.provinces_attribute_sale;

insert overwrite   table csx_dw.provinces_attribute_sale partition(sdt)
select
	attribute_name ,
	province_code,
	province_name,
	channel_name,
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
	sale / sum(sale)over(partition by province_code) as sale_ratio,
	${hiveconf:edate}
from
	(
	select
		nvl(attribute_name,'')attribute_name ,
		province_code,
		province_name,
		case when channel in ('1','3','7') then '大' when channel in ('2') then '商超' else channel_name end channel_name,
		sum(case when sdt >= regexp_replace(${hiveconf:mdate},'-','') and sdt <= ${hiveconf:edate}  then sales_value end ) sale,
		sum(case when sdt >= regexp_replace(${hiveconf:mdate},'-','') and sdt <= ${hiveconf:edate}  then profit end ) profit,
		COUNT(DISTINCT case when sdt >= regexp_replace(${hiveconf:mdate},'-','') and sdt <= ${hiveconf:edate}  then customer_no end ) cust_num,
		sum(case when sdt >=  regexp_replace(to_date(${hiveconf:l_mdate}),'-','') and sdt <= ${hiveconf:l_edate}  then sales_value end ) as ring_sale,
		sum(case when sdt >=  regexp_replace(to_date(${hiveconf:l_mdate}),'-','') and sdt <= ${hiveconf:l_edate}  then profit end )as ring_profit,
		COUNT(DISTINCT case when sdt >=  regexp_replace(to_date(${hiveconf:l_mdate}),'-','')  and sdt <= ${hiveconf:l_edate}  then customer_no end )as ring_cust_num
	from
		csx_dw.customer_sales
	where
	sdt>=	 regexp_replace(${hiveconf:l_mdate},'-','')  and sdt <= ${hiveconf:edate} 
	group by
		attribute_name,
		province_code,
		province_name,
		case when channel in ('1','3','7') then '大' when channel in ('2') then '商超' else channel_name end )a ;
	
select * from csx_dw.provinces_attribute_sale;

drop table  `csx_dw.provinces_attribute_sale`;
 CREATE TABLE `csx_dw.provinces_attribute_sale`(
`attribute_name` string comment '属性', 
province_code string comment '省区编码',
province_name string comment'省区名称',
channel_name string comment'渠道',
`sale` decimal(36,6) comment '本月销售额', 
`profit` decimal(36,6) comment '本月毛利额', 
`profit_rate` decimal(38,6) comment '本月毛利率', 
`cust_num` bigint comment '本月数', 
`ring_sale` decimal(36,6) comment '上月销售额', 
`ring_profit` decimal(36,6) comment '上月毛利额', 
`ring_profit_rate` decimal(38,6) comment '上月毛利率', 
`ring_cust_num` bigint comment '上月数', 
`diff_cust_num` bigint comment '环比差异', 
`mom_sale_ratio` decimal(38,6) comment '销售环比', 
`sale_ratio` decimal(38,6) comment '销售占比')
comment '省区属性销售占比'
partitioned by (sdt string comment'日期分区')
 ;
 
 drop table `csx_dw.provinces_kanban_account_cust`;
 CREATE TABLE `csx_dw.provinces_kanban_account_cust`(
`customer_no` string, 
`customer_name` string, 
`province_code` string, 
`province_name` string, 
`sales_name` string, 
`attribute_name` string, 
`ac_all` decimal(36,6), 
`ac_wdq` decimal(36,6), 
`over_amt` decimal(36,6), 
`ac_15d` decimal(36,6), 
`ac_30d` decimal(36,6), 
`ac_60d` decimal(36,6), 
`ac_90d` decimal(36,6), 
`ac_120d` decimal(36,6), 
`ac_180d` decimal(36,6), 
`ac_365d` decimal(36,6), 
`ac_over365d` decimal(36,6),
max_day string comment'逾期最大天数')
comment '逾期TOP10'
PARTITIONed by (sdt string comment'日期分区')
STORED AS PARQUET
;

insert overwrite  table csx_dw.provinces_kanban_account_cust partition(sdt)
select 
  a.customer_no,
  customer_name,
  sales_province_code province_code,
  sales_province province_name,
  sales_name,attribute_name,
  ac_all ,
  ac_wdq ,
  over_amt ,
  ac_15d ,
  ac_30d ,
  ac_60d ,
  ac_90d ,
  ac_120d,
  ac_180d,
  ac_365d,
  ac_over365d,
  case when ac_over365d>10 then 366 
    when ac_365d>10 then 365 
    when ac_180d>10 then 180 
    when ac_120d>10 then 120 
    when ac_90d>10 then 90 
    when ac_60d>10 then 60 
    when ac_30d>10 then 30 
    when ac_15d>10 then 15 else 0 end max_day ,
     regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
    from (
select 
  regexp_replace(kunnr, '(^0*)', '') as customer_no,
   sum(ac_all) ac_all ,
  sum(ac_wdq) ac_wdq ,
  sum(ac_all-ac_wdq )as over_amt ,
  sum(ac_15d)ac_15d  ,
  sum(ac_30d) ac_30d ,
  sum(ac_60d)ac_60d  ,
  sum(ac_90d) ac_90d ,
  sum(ac_120d)ac_120d,
  sum(ac_180d)ac_180d,
  sum(ac_365d)ac_365d,
  sum(ac_over365d)ac_over365d 
  --sort_array(array(ac_over365d , ac_365d ,ac_180d ,ac_120d ,ac_90d ,ac_60d ,ac_30d,ac_15d)) max_day,
 FROM csx_dw.account_age_dtl_fct cc
where 
sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
group by regexp_replace(kunnr, '(^0*)', '')
)a
	join
		(
			select
				cm.sales_province     ,
				cm.sales_province_code,
				cm.customer_no        ,
				customer_name         ,
				sales_name,
				cm.`attribute` as attribute_name
			from
				csx_dw.customer_m cm
			where
				sdt                =regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
				and cm.customer_no!=''
		)
		b
		on
			a.customer_no=b.customer_no;
			
INVALIDATE METADATA csx_dw.provinces_kanban_account_age;
invalidate metadata csx_dw.provinces_kanban_sales_top10;
invalidate metadata csx_dw.provinces_kanban_account_cust;
invalidate metadata csx_dw.provinces_kanban_cust;
invalidate metadata csx_dw.provinces_kanban_goods_lose;
invalidate metadata  csx_dw.provinces_kanban_cust_lose;
INVALIDATE METADATA csx_dw.provinces_kanban_frequency ;
INVALIDATE METADATA csx_dw.provinces_kanban_sales_top10;
