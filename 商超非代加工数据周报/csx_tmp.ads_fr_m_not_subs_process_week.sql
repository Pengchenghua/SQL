
--============================================================================================================
--M端代加工数据_周环比

--昨日分区样式
set i_sdate_1 =regexp_replace(date_sub(current_date,1),'-','');

--本周五日期
set i_sdate_5 =regexp_replace(if(pmod(datediff(current_date,'1920-01-04'),7)=6,
								date_sub(current_date,1),
								date_sub(current_date,pmod(datediff(current_date,'1920-01-04'),7)+2)),'-','');
--本周第一天
set i_sdate_6 =regexp_replace(date_sub(if(pmod(datediff(current_date,'1920-01-04'),7)=6,
								date_sub(current_date,1),
								date_sub(current_date,pmod(datediff(current_date,'1920-01-04'),7)+2)),6),'-','');
--上周五日期
set i_sdate_15 =regexp_replace(date_sub(if(pmod(datediff(current_date,'1920-01-04'),7)=6,
								date_sub(current_date,1),
								date_sub(current_date,pmod(datediff(current_date,'1920-01-04'),7)+2)),13),'-','');
								
--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.ads_fr_m_not_subs_process_week partition(sdt)
								
select
	province_name as key_name,
	if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	if(sdt>=${hiveconf:i_sdate_6},'本周','上周') as week_type,
	sum(sales_value) as sales_value, 
	sum(profit) as profit,
	coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
	${hiveconf:i_sdate_1} as update_time,
	${hiveconf:i_sdate_1} as sdt	
from 
	csx_dw.dws_sale_r_d_customer_sale
where 
	sdt between ${hiveconf:i_sdate_15} and ${hiveconf:i_sdate_5}
	and channel = '2' --1-大 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
	and (dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5','W0X5') or (dc_code in ('W0M4') and department_code not in ('H03','H01')))--数据不含代加工DC 
	and province_name not like '平台%'
group by
	province_name,
	if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他'),--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	if(sdt>=${hiveconf:i_sdate_6},'本周','上周')
	
union all

select
	b.region_name as key_name,
	a.department_type,
	a.week_type,				
	sum(a.sales_value) as sales_value, 
	sum(a.profit) as profit,
	coalesce(sum(a.profit)/sum(a.sales_value),0) as profit_rate,
	${hiveconf:i_sdate_1} as update_time,
	${hiveconf:i_sdate_1} as sdt
from	
	(	
	select
		province_code,
		province_name,
		if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
		if(sdt>=${hiveconf:i_sdate_6},'本周','上周') as week_type,
		sum(sales_value) as sales_value, 
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between ${hiveconf:i_sdate_15} and ${hiveconf:i_sdate_5}
		and channel = '2' --1-大 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and (dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5','W0X5') or (dc_code in ('W0M4') and department_code not in ('H03','H01')))--数据不含代加工DC
		and province_name not like '平台%'
	group by
		province_code,province_name,
		if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他'),--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
		if(sdt>=${hiveconf:i_sdate_6},'本周','上周')
	) as a	
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
		) b on b.province_code=a.province_code
group by
	b.region_name,
	department_type,
	a.week_type	
;
	
		
INVALIDATE METADATA csx_tmp.ads_fr_m_not_subs_process_week;	


	
/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.ads_fr_m_not_subs_process_week  M端非代加工周报数据

drop table if exists csx_tmp.ads_fr_m_not_subs_process_week;
create table csx_tmp.ads_fr_m_not_subs_process_week(
`key_name`                 string              COMMENT    '指标名称',
`department_type`          string              COMMENT    '课组类型',
`week_type`                string              COMMENT    '周期类型',
`sales_value`              decimal(26,6)       COMMENT    '销售额',
`profit`                   decimal(26,6)       COMMENT    '毛利额',
`profit_rate`              decimal(26,6)       COMMENT    '毛利率',
`update_time`              string              COMMENT    '更新时间'
) COMMENT 'zhangyanpeng:M端非代加工周报数据'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	


