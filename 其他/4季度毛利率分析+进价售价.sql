
-- 新建表 合伙人与断约 基本信息表
--drop table csx_tmp.partner_info;
CREATE TABLE IF NOT EXISTS `csx_tmp.partner_info` (
  `cust_group` STRING COMMENT '类型',
  `province_name` STRING COMMENT '省区',
  `city_name` STRING COMMENT '城市',  
  `customer_no` STRING COMMENT '编号',
  `customer_name` STRING COMMENT '名称',
  `break_date` STRING COMMENT '断约时间'
) COMMENT '合伙人与断约基本信息表'
PARTITIONED BY (sdt string COMMENT '日期分区')
ROW format delimited fields terminated by ','
STORED AS TEXTFILE;


drop table csx_tmp.tmp_cust_partner2;
create table csx_tmp.tmp_cust_partner2
as
select distinct customer_no,sdt
from csx_tmp.tmp_cust_partner
union all
select distinct customer_no,substr(sdt,1,6) sdt
				from csx_dw.dws_crm_w_a_customer_m_v1
				where sdt in('20200731','20200831','20200930')   --月最后一天
				and attribute='合伙人'
				and customer_no<>'';

--1、省区、业务类型：销售额、毛利趋势
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select coalesce(business_type_name,'总计') business_type_name,coalesce(region_name,'全国') region_name,coalesce(province_name,'合计') province_name,coalesce(smonth,'合计') smonth,
	sum(sales_value)/10000 sales_value,
	sum(profit)/10000 profit,
	GROUPING__ID
from (
select region_name,province_name,business_type_name,substr(sdt,1,6) smonth,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(profit)/abs(sum(sales_value)) prorate 
from csx_dw.dws_sale_r_d_detail
where sdt >= '20200101' and sdt < '20210101'  --上月
and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
and channel_code in('1','7','9')
group by region_name,province_name,business_type_name,substr(sdt,1,6)
)a
group by business_type_name,region_name,province_name,smonth
grouping sets(smonth,(smonth,region_name),(smonth,region_name,province_name),(business_type_name,smonth),(business_type_name,region_name,smonth),(business_type_name,region_name,province_name,smonth))
order by GROUPING__ID; 



--2、新老客毛利趋势
insert overwrite directory '/tmp/raoyanhua/linshi02' row format delimited fields terminated by '\t'
select a.region_name,a.province_name,a.customer_no,d.customer_name,
	a.smonth,a.squarter,b.new_month,b.new_quarter,a.business_type_name,
	--f.classify_middle_code,     --管理中类编号
	--f.classify_middle_name,     --管理中类名称
	sum(a.sales_value)/10000 sales_value,
	sum(a.profit)/10000 profit
	--sum(a.front_profit)/10000 front_profit
	--count(distinct customer_no) count_cust
from 
	(select 
		channel_name,region_name,province_name,substr(sdt,1,6) smonth,
		concat(substr(from_unixtime(unix_timestamp(sdt,'yyyymmdd'),'yyyy-mm-dd'),1,4),'Q',
					(floor(substr(from_unixtime(unix_timestamp(sdt,'yyyymmdd'),'yyyy-mm-dd'),6,2)/3.1))+1) squarter,
		--coalesce(origin_order_no,order_no) order_no_new,
		business_type_code,
		business_type_name,
		customer_no,
		goods_code,
		sales_value,profit,
		sales_qty,front_profit
	--from csx_dw.dws_sale_r_d_customer_sale 
	from csx_dw.dws_sale_r_d_detail
	where sdt >= '20200101' and sdt < '20210101'  --上月
	and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
	and channel_code in('1','7','9')
	--and business_type_code in('1','2')			--(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	)a 
--最早销售月 新客月、新客季度
left join
	(select customer_no,
	substr(min(first_sales_date),1,6) new_month,
	concat(substr(from_unixtime(unix_timestamp(min(first_sales_date),'yyyymmdd'),'yyyy-mm-dd'),1,4),'Q',
				(floor(substr(from_unixtime(unix_timestamp(min(first_sales_date),'yyyymmdd'),'yyyy-mm-dd'),6,2)/3.1))+1) new_quarter
	from csx_tmp.ads_sale_w_d_customer_company_sales_date
	where sdt = 'current'
	group by customer_no
	)b on b.customer_no=a.customer_no
left join
	(
	select * 
	from csx_dw.dws_crm_w_a_customer
	where sdt = 'current'	
	)d on d.customer_no=a.customer_no
left join (select *  from  csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current') f on f.goods_id = a.goods_code 
group by a.region_name,a.province_name,a.customer_no,d.customer_name,
	a.smonth,a.squarter,b.new_month,b.new_quarter,a.business_type_name
	--f.classify_middle_code,     --管理中类编号
	--f.classify_middle_name     --管理中类名称	
;


--3、新老客品类毛利
insert overwrite directory '/tmp/raoyanhua/linshi03' row format delimited fields terminated by '\t'
select coalesce(a.region_name,'全国') region_name,
	coalesce(a.province_name,'合计') province_name,
	a.smonth,a.squarter,b.new_month,b.new_quarter,a.business_type_name,
	f.classify_middle_code,     --管理中类编号
	f.classify_middle_name,     --管理中类名称
	sum(a.sales_value)/10000 sales_value,
	sum(a.profit)/10000 profit
	--sum(a.front_profit)/10000 front_profit
	--count(distinct customer_no) count_cust
from 
	(select 
		channel_name,region_name,province_name,substr(sdt,1,6) smonth,
		concat(substr(from_unixtime(unix_timestamp(sdt,'yyyymmdd'),'yyyy-mm-dd'),1,4),'Q',
					(floor(substr(from_unixtime(unix_timestamp(sdt,'yyyymmdd'),'yyyy-mm-dd'),6,2)/3.1))+1) squarter,
		--coalesce(origin_order_no,order_no) order_no_new,
		business_type_code,
		business_type_name,
		customer_no,
		goods_code,
		sales_value,profit,
		sales_qty,front_profit
	--from csx_dw.dws_sale_r_d_customer_sale 
	from csx_dw.dws_sale_r_d_detail
	where sdt >= '20200101' and sdt < '20210101'  --上月
	and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
	and channel_code in('1','7','9')
	--and business_type_code in('1','2')			--(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	)a 
--最早销售月 新客月、新客季度
left join
	(select customer_no,
	substr(min(first_sales_date),1,6) new_month,
	concat(substr(from_unixtime(unix_timestamp(min(first_sales_date),'yyyymmdd'),'yyyy-mm-dd'),1,4),'Q',
				(floor(substr(from_unixtime(unix_timestamp(min(first_sales_date),'yyyymmdd'),'yyyy-mm-dd'),6,2)/3.1))+1) new_quarter
	from csx_tmp.ads_sale_w_d_customer_company_sales_date
	where sdt = 'current'
	group by customer_no
	)b on b.customer_no=a.customer_no
left join
	(
	select * 
	from csx_dw.dws_crm_w_a_customer
	where sdt = 'current'	
	)d on d.customer_no=a.customer_no
left join (select *  from  csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current') f on f.goods_id = a.goods_code 
group by a.region_name,a.province_name,a.smonth,a.squarter,b.new_month,b.new_quarter,
	a.business_type_name,f.classify_middle_code,f.classify_middle_name
grouping sets (
(a.smonth,a.squarter,b.new_month,b.new_quarter,a.business_type_name,f.classify_middle_code,f.classify_middle_name),
(a.region_name,a.smonth,a.squarter,b.new_month,b.new_quarter,a.business_type_name,f.classify_middle_code,f.classify_middle_name),
(a.region_name,a.province_name,a.smonth,a.squarter,b.new_month,b.new_quarter,
	a.business_type_name,f.classify_middle_code,f.classify_middle_name)
);	
;

