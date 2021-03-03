--============================================================================================================
--M端代加工数据_月环比

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_21 =regexp_replace(add_months(date_sub(current_date,1),-1),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');

--昨日分区样式
set i_sdate_1 =regexp_replace(date_sub(current_date,1),'-','');


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.ads_fr_m_not_subs_process_month partition(sdt)

				
select
	a.province_name, 
	c.region_name,
	a.department_type,
	(case when b.sales_belong_flag like '%云超%' then '云超'
		when b.sales_belong_flag like '%云创%' then '云创'
		when a.customer_name rlike '红旗|中百' then '关联方'
		when b.sales_belong_flag='' or b.sales_belong_flag is null then '外部'
		else '其他'
	end) as sales_belong_type,
	a.month_type,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	coalesce(sum(a.profit)/sum(a.sales_value),0) as profit_rate,
	${hiveconf:i_sdate_1} as update_time,
	substr(${hiveconf:i_sdate_1},1,6) as sdt
from	
	(	
	select
		province_code,province_name,customer_no,customer_name,
		if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
		(case when sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} then '本月'
			when sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21} then '上月'
			else '其他'
		end) as month_type,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		(sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} or sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21})
		and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and (dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5','W0X5') or (dc_code in ('W0M4') and department_code not in ('H03','H01')))--数据不含代加工DC
		and province_name not like '平台%'				
	group by
		province_code,province_name,customer_no,customer_name,
		if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他'), --熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他		
		(case when sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} then '本月'
			when sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21} then '上月'
			else '其他'
		end)
	) as a 
	left join
		(
		select 
			shop_id,company_code,sales_belong_flag
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt = 'current'
		group by
			shop_id,company_code,sales_belong_flag
		) b on a.customer_no = concat('S', b.shop_id)
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) c on c.province_code=a.province_code
group by
	a.province_name,
	c.region_name,
	a.department_type,
	(case when b.sales_belong_flag like '%云超%' then '云超'
		when b.sales_belong_flag like '%云创%' then '云创'
		when a.customer_name rlike '红旗|中百' then '关联方'
		when b.sales_belong_flag='' or b.sales_belong_flag is null then '外部'
		else '其他'
	end),
	a.month_type
;



INVALIDATE METADATA csx_tmp.ads_fr_m_not_subs_process_month;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.ads_fr_m_not_subs_process_month  M端非代加工月环比数据

drop table if exists csx_tmp.ads_fr_m_not_subs_process_month;
create table csx_tmp.ads_fr_m_not_subs_process_month(
`province_name`            string              COMMENT    '省份名称',
`region_name`              string              COMMENT    '大区名称',
`department_type`          string              COMMENT    '课组类型',
`sales_belong_type`        string              COMMENT    '业态类型',
`month_type`               string              COMMENT    '月份类型',
`sales_value`              decimal(26,6)       COMMENT    '销售额',
`profit`                   decimal(26,6)       COMMENT    '毛利额',
`profit_rate`              decimal(26,6)       COMMENT    '毛利率',
`update_time`              string              COMMENT    '更新时间'
) COMMENT 'zhangyanpeng:M端非代加工月环比数据'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/		