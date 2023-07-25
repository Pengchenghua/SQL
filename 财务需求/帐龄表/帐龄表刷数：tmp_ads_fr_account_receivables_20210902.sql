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
				when b.channel_name is null then '其他'
				else b.channel_name
			end channel , 
			hkont , 
			a.account_name , 
			comp_code , 
			comp_name ,
			case when kunnr in ('G7150')  THEN '平台-食百采购'
				when 
					(b.province_name is null
				and comp_code = '1933') then '福建省'
				when (b.province_name is null
				and comp_name like '%北京%' )then concat(substring(comp_name, 1, 2 ), '市')
				when (b.province_name is null )then concat(substring(comp_name, 1, 2 ), '省')
				else b.province_name
			end as dist , 
			b.sales_city_name sales_city , 
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
				a.sdt = regexp_replace(${hiveconf:e_date},'-','')
		
				and a.ac_all <> 0
				and kunnr <> '0000910001'
				and hkont like '1122%'
        ) a
		left join 
	(
  SELECT customer_no, customer_name,
  channel_code,
  channel_name, 
  province_code,
  province_name,
  city_group_code,
  city_group_name,
  sales_province_code, 
  sales_province_name, 
  sales_city_code,
  sales_city_name,	
  first_category_name  as first_category   ,
  second_category_name as second_category,
  third_category_name  as third_category  ,  
  work_no , 
  sales_name , 
  first_supervisor_name
  FROM csx_dw.dws_crm_w_a_customer
  WHERE sdt = 'current' 
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
				a.sdt = regexp_replace(${hiveconf:e_date},'-','')
		
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
   CASE WHEN a.customer_no LIKE 'V%' then c.province_code else  coalesce(d.province_code,   b.province_code ) end   as  province_code,		--省区编码
   CASE WHEN a.customer_no LIKE 'V%' then c.province_name else   coalesce(d.province_name ,  dist ) end province_name,
   CASE WHEN a.customer_no LIKE 'V%' then '-' else  coalesce(d.city_group_name,a.sales_city) end as sales_city,
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
		customer_no,
		company_code,
		payment_terms,
		payment_name,
		credit_limit,
		temp_credit_limit,
		payment_days,
		province_code,
		province_name,
		city_code,
		city_name
	from 
		csx_dw.dws_crm_w_a_customer_company   --账期表
	where 
		sdt='current'
) as b 	on a.customer_no =b.customer_no and a.comp_code=b.company_code
left join
( -- 获取管理大区、省区与城市组信息
  SELECT
    city_code, 
    area_location_code, 
    city_group_code, 
    city_group_name,
    province_code, 
    province_name, 
    region_code, 
    region_name,city_name
  FROM csx_dw.dws_sale_w_a_area_belong
) d on  a.sales_city = d.city_name
	 and a.dist=d.province_name
 left join
( -- 获取公司所在地理省区城市
  select distinct  a.code,
  a.province_code,
    a.province_name
  from csx_dw.dws_basic_w_a_company_code a
  where a.table_type=1
  and sdt='current'



) c on a.comp_code = c.code
;