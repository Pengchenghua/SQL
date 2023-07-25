-- 帐龄表：csx_dw.account_age_dtl_fct_new 
-- 表：csx_dw.dws_crm_w_a_customer_m_v1
-- 帐期表 ：csx_dw.dws_crm_r_a_customer_account_day 

set e_date='${enddate}';



drop table csx_tmp.temp_account_age;
CREATE temporary table csx_tmp.temp_account_age
as
select
    channel ,
    hkont ,
    account_name ,
    comp_code ,
    comp_name ,
    dist ,
    sales_city ,
    customer_no ,
    customer_name ,
    first_category ,
    second_category ,
    third_category ,
    work_no ,
    sales_name ,
    first_supervisor_name,
    -- credit_limit ,
    -- temp_credit_limit ,
    zterm ,
    diff ,
    ac_all ,
    ac_wdq ,
    ac_15d ,
    ac_30d ,
    ac_60d ,
    ac_90d ,
    ac_120d ,
    ac_180d ,
    ac_365d ,
    ac_2y ,
    ac_3y ,
    ac_over3y,
    	last_sales_date,
	        	last_to_now_days,
	        	customer_active_sts_code,
	        	customer_active_sts,
	        	sdt
from
    (
		select
			case
				when b.channel is null then '其他'
				else b.channel
			end channel , 
			hkont , 
			a.account_name , 
			comp_code , 
			comp_name ,
			case
				when (b.sales_province is null
				and comp_code = '1933') then '福建省'
				when (b.sales_province is null
				and comp_name like '%北京%' )then concat(substring(comp_name, 1, 2 ), '市')
				when (b.sales_province is null )then concat(substring(comp_name, 1, 2 ), '省')
				else b.sales_province
			end as dist , 
			b.sales_city , 
			regexp_replace(kunnr, '^0*','') as customer_no, 
			b.customer_name as customer_name , 
			b.first_category , 
			b.second_category , 
			b.third_category , 
			b.work_no , 
			b.sales_name , 
			b.first_supervisor_name , 
			-- b.credit_limit , 
			-- b.temp_credit_limit , 
			zterm , 
			diff , 
			ac_all ,
			case
				when ac_all<0 then ac_all
				else ac_wdq
			end ac_wdq,
			case
				when ac_all<0 then 0
				else ac_15d
			end ac_15d,
			case
				when ac_all<0 then 0
				else ac_30d
			end ac_30d,
			case
				when ac_all<0 then 0
				else ac_60d
			end ac_60d,
			case
				when ac_all<0 then 0
				else ac_90d
			end ac_90d,
			case
				when ac_all<0 then 0
				else ac_120d
			end ac_120d,
			case
				when ac_all<0 then 0
				else ac_180d
			end ac_180d,
			case
				when ac_all<0 then 0
				else ac_365d
			end ac_365d,
			case
				when ac_all<0 then 0
				else ac_2y
			end ac_2y,
			case
				when ac_all<0 then 0
				else ac_3y
			end ac_3y,
			case
				when ac_all<0 then 0
				else ac_over3y
			end ac_over3y,
				last_sales_date,
	        	last_to_now_days,
	        	customer_active_sts_code,
	        	customer_active_sts,
	        	sdt
		from
        (
			select
				sflag ,
				hkont ,
				account_name,
				comp_code ,
				comp_name ,
				prctr ,
				shop_name ,
				kunnr ,
				-- NAME        ,
				zterm ,
				diff ,
				ac_all ,
				ac_wdq ,
				ac_15d ,
				ac_30d ,
				ac_60d ,
				ac_90d ,
				ac_120d ,
				ac_180d ,
				ac_365d ,
				ac_2y ,
				ac_3y ,
				ac_over3y ,
	        	last_sales_date,
	        	last_to_now_days,
	        	customer_active_sts_code,
	        	case when  customer_active_sts_code = 1 then '活跃'
	        		when customer_active_sts_code = 2 then '沉默'
	        		when customer_active_sts_code = 3 then '预流失'
	        		when customer_active_sts_code = 4 then '流失'
	        		else '其他'
	        		end  as  customer_active_sts,
	        	sdt
			from
				csx_dw.account_age_dtl_fct_new a
			where
				a.sdt <= regexp_replace(${hiveconf:e_date},'-','')
		        and a.sdt>='20200101'
				and a.ac_all <> 0
				and kunnr <> '0000910001'
				and hkont like '1122%'
        ) a
		left join 
		(
			SELECT customer_no,
                          customer_name,
                          sales_province,
                          sales_province_code,
                          sales_city,
                          first_category,
                          second_category,
                          third_category,
                          work_no,
                          sales_name,
                          first_supervisor_name,
                          channel
                        FROM csx_dw.dws_crm_w_a_customer_m_v1
                            WHERE sdt='current'
		) b on lpad(a.kunnr, 10, '0')= lpad(b.customer_no, 10, '0')
		
		union all
		select
			a.sflag as channel , hkont , a.account_name, comp_code , comp_name ,
			case
				when substr(comp_name, 1, 2)in('上海', '北京', '重庆') then concat(substr(comp_name, 1, 2), '市')
				when substr(comp_name, 1, 2)= '永辉' then '福建省'
				else concat(substr(comp_name, 1, 2), '省')
			end dist , 
			substr(comp_name, 1, 2) sales_city, 
			regexp_replace(kunnr, '^0*', '') as customer_no , 
			name as customer_name , 
			'个人及其他' first_category , 
			'个人及其他' second_category , 
			'个人及其他' third_category , 
			'' work_no , 
			'' sales_name , 
			'' first_supervisor_name, 
-- 			0 as credit_limit, 
-- 			0 as temp_credit_limit, 
			zterm , 
			diff , 
			ac_all,
			case
				when ac_all<0 then ac_all
				else ac_wdq
			end ac_wdq,
			case
				when ac_all<0 then 0
				else ac_15d
			end ac_15d,
			case
				when ac_all<0 then 0
				else ac_30d
			end ac_30d,
			case
				when ac_all<0 then 0
				else ac_60d
			end ac_60d,
			case
				when ac_all<0 then 0
				else ac_90d
			end ac_90d,
			case
				when ac_all<0 then 0
				else ac_120d
			end ac_120d,
			case
				when ac_all<0 then 0
				else ac_180d
			end ac_180d,
			case
				when ac_all<0 then 0
				else ac_365d
			end ac_365d,
			case
				when ac_all<0 then 0
				else ac_2y
			end ac_2y,
			case
				when ac_all<0 then 0
				else ac_3y
			end ac_3y,
			case
				when ac_all<0 then 0
				else ac_over3y
			end ac_over3y,
			last_sales_date,
	        	last_to_now_days,
	        	customer_active_sts_code,
	        	case when  customer_active_sts_code = 1 then '活跃'
	        		when customer_active_sts_code = 2 then '沉默'
	        		when customer_active_sts_code = 3 then '预流失'
	        		when customer_active_sts_code = 4 then '流失'
	        		else '其他'
	        		end  as  customer_active_sts,
	        sdt
		from
			csx_dw.account_age_dtl_fct_new a
		where
				a.sdt <= regexp_replace(${hiveconf:e_date},'-','')
		    and a.sdt>='20200101'
			and a.ac_all <> 0
			and kunnr = '0000910001' 
	) x
where
    1 = 1 
order by
    channel ,
    comp_code,
    dist ,
    customer_no ;


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.ads_fr_account_receivables partition (sdt)
select 
   a.channel  as channel_name,
   a.hkont  as hkont,
   a.account_name  as account_name,
   a.comp_code  as comp_code,
   a.comp_name as  comp_name,
   coalesce(c.province_code,'-1') as  province_code,		--省区编码
   a.dist  as province_name,
   a.sales_city as sales_city,
   '' as prctr,			--成本中心
   '' as shop_name,
   a.customer_no ,
   a.customer_name ,
   a.first_category ,
   a.second_category ,
   a.third_category ,
   a.work_no ,
   a.sales_name ,
   a.first_supervisor_name,
  
	b.credit_limit ,
	b.temp_credit_limit ,
	 payment_terms,
   payment_name,
   payment_days,
   a.zterm ,
   a.diff ,
   a.ac_all ,
   a.ac_wdq ,
   a.ac_15d ,
   a.ac_30d ,
   a.ac_60d ,
   a.ac_90d ,
   a.ac_120d ,
   a.ac_180d ,
   a.ac_365d ,
   a.ac_2y ,
   a.ac_3y ,
   a.ac_over3y,
	a.last_sales_date,
	a.last_to_now_days,
	a.customer_active_sts_code as customer_active_sts_code,  --活跃状态标签编码（1 活跃；2 沉默；3预流失；4 流失）
	a.customer_active_sts as customer_active_sts,
	'' as tmp_01,
	'' as tmp_02,
	current_timestamp() as update_time,
	a.sdt
from 
(
	select 
		*
	from 
		csx_tmp.temp_account_age
) as a 
left  join 
(	
	select 
		customer_number,
		company_code,
		payment_terms,
		payment_name,
		credit_limit,
		temp_credit_limit,
		payment_days
	from 
		csx_dw.dws_crm_r_a_customer_account_day    --账期表
	where 
		sdt='current'
) as b 	on a.customer_no =b.customer_number and a.comp_code=b.company_code
left join 
(select province_code,province_name from csx_dw.dim_area where area_rank='13')c  on a.dist = c.province_name
where a.sdt<'20200301' 
   and  a.sdt>='20200101'
;



---创建表结
  drop  table csx_tmp.ads_fr_account_receivables;
 create table csx_tmp.ads_fr_account_receivables
   (
    channel_name	string comment	'类型',
	hkont	string	comment '科目代码',
	account_name	string comment	'科目名称',
	comp_code	string	comment '公司代码',
	comp_name	string	comment '公司名称',
    province_code string COMMENT '销售省区编码',
    province_name string  COMMENT '销售省区名称',
    sales_city string COMMENT '销售城市名称',
	prctr	string comment	'利润中心',
	shop_name	string	comment '利润中心名称',
	customer_no	string comment	'编码',
	customer_name	string	comment '名称',
    first_category string  COMMENT '第一分类',
    second_category string  COMMENT '第二分类',
    third_category string COMMENT '第三分类',
    work_no string  COMMENT '销售员工号',
    sales_name string  COMMENT '销售员姓名',
    first_supervisor_name string  COMMENT '销售主管',
    credit_limit decimal(26,4)   COMMENT '信控额度',
    temp_credit_limit decimal(26,4)  COMMENT '临时信控额度',
	payment_terms string comment '付款条件',
	payment_name string comment '付款条件名称',
	payment_days string comment '帐期',
	zterm	string	comment '账期类型',
	diff	string comment	'账期',
	ac_all	decimal(26,4)	comment '全部账款',
	ac_wdq	decimal(26,4)	comment '未到期账款',
	ac_15d	decimal(26,4)	comment '15天内账款',
	ac_30d	decimal(26,4)	comment '30天内账款',
	ac_60d	decimal(26,4)	comment '60天内账款',
	ac_90d	decimal(26,4)	comment '90天内账款',
	ac_120d	decimal(26,4)	comment '120天内账',
	ac_180d	decimal(26,4)	comment '半年内账款',
	ac_365d	decimal(26,4)	comment '1年内账款',
	ac_2y	decimal(26,4)	comment '2年内账款',
	ac_3y	decimal(26,4)	comment '3年内账款',
	ac_over3y	decimal(26,4)	comment '逾期3年账款',
	last_sales_date	string	comment '最后一次销售日期',
	last_to_now_days	string comment	'最后一次销售距今天数',
	customer_active_sts_code	string comment	'活跃状态标签编码（1 活跃；2 沉默；3预流失；4 流失）',
	customer_active_sts string comment '活跃状态名称',
	tmp_01 string comment '临时使用',
	tmp_02 string comment '临时使用',
	update_time timestamp comment '更新时间'
   )comment '应收帐龄结果表-帆软使用'
   partitioned by (sdt string comment '日期分区')
   stored as parquet
   ;
