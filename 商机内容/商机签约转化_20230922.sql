-- ===============================================================================================================================================================================
drop table csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_02;
create table csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_02
as
select
	a.cust_flag,
	a.quarter_of_year,a.business_sign_month,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,	
	a.business_number,
	a.customer_id,a.customer_code,a.customer_name,a.first_category_name,a.second_category_name,a.third_category_name,
	a.owner_user_number,a.owner_user_name,
	a.business_type_code,a.business_type_name,
	a.customer_acquisition_type_code,a.customer_acquisition_type_name,
	a.contract_cycle_desc,a.estimate_contract_amount,
	a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
	a.new_classify_name,a.num,a.next_sign_date,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sale_amt else null end) as sale_amt,
	sum(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.profit else null end) as profit,
	count(distinct case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as sdt_cnt,
	min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end) as min_sdt,
	from_unixtime(unix_timestamp(min(case when b.sdt>=a.business_sign_date and b.sdt<a.next_sign_date then b.sdt else null end),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_date,
	min(case when b.sdt>=a.business_sign_date then b.sdt else null end) as min_sdt_all,
	c.min_order_sdt
from
	(
	select
		cust_flag,b.quarter_of_year,
		a.business_sign_month,a.business_number,a.customer_id,a.customer_code,a.customer_name,g.first_category_name,g.second_category_name,g.third_category_name,
		a.performance_region_name,a.performance_province_name,a.performance_city_name,
		a.owner_user_number,a.owner_user_name,a.business_type_code,a.business_type_name,a.customer_acquisition_type_code,a.customer_acquisition_type_name,
		a.contract_cycle_desc,a.estimate_contract_amount,
		a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
		h.new_classify_name,
		row_number() over(partition by a.customer_code,a.business_type_name order by a.business_sign_time) as num	, --商机顺序
		regexp_replace(to_date(lead(a.business_sign_time,1,'9999-12-31')over(partition by a.customer_code,a.business_type_name order by a.business_stage,a.business_sign_time)),'-','') as next_sign_date
	from 
		(
		select
			if(to_date(business_sign_time)=to_date(first_business_sign_time),'新商机','老商机') cust_flag,business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,business_number,customer_id,customer_code,customer_name,
			first_category_name,second_category_name,third_category_name,
			performance_region_name,performance_province_name,performance_city_name,
			owner_user_number,owner_user_name,
			business_type_code,
			business_type_name,
			customer_acquisition_type_code,
			if(customer_acquisition_type_name='','非投标',customer_acquisition_type_name) as customer_acquisition_type_name,
			contract_cycle_desc,estimate_contract_amount,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(business_sign_time) as business_sign_date_2,
			regexp_replace(to_date(first_business_sign_time),'-','') first_business_sign_date,business_stage
		from csx_dim.csx_dim_crm_business_info
		where sdt='current'
			-- and channel_code in('1','7','9')
			and business_attribute_code in (1,2,5) -- 商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
			and status=1  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
			and to_date(business_sign_time) >= '${yyyy-MM-01}' 
			and performance_province_name !='平台-B'
			-- and performance_province_name='江苏苏州'
		)a
		left join
			(
			select
				customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
				sales_user_number,sales_user_name,customer_address_full
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) g on g.customer_code=a.customer_code		
		left join
			(
			select
				second_category_code,second_category_name,new_classify_name
			from
				csx_analyse.csx_analyse_fr_new_customer_classify_mf
			group by 
				second_category_code,second_category_name,new_classify_name
			) h on h.second_category_name=g.second_category_name
		left join
			(
			select
				calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end
			from
				csx_dim.csx_dim_basic_date
			) b on b.calday=a.business_sign_date
	) a 
	left join 
		(
		select 
			sdt,customer_code, 
			case when business_type_code =4 then 1 else business_type_code end business_type_code,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit,
			sum(profit)/abs(sum(sale_amt)) as profit_rate
		from 	
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='${yyyymm01}' 
			and channel_code in('1','7','9')
			and business_type_code in(1,2,6,4)
		group by 
			sdt,customer_code,case when business_type_code =4 then 1 else business_type_code end
		)b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code			
	left join 
		(
		select customer_code,order_business_type_code,
			min(sdt) min_order_sdt
		from csx_dwd.csx_dwd_oms_sale_order_detail_di -- B端无BBC
		where sdt>='${yyyymm01}' 
		group by customer_code,order_business_type_code
		)c on a.customer_code=c.customer_code and a.business_type_code=c.order_business_type_code
group by 
	a.cust_flag,
	a.quarter_of_year,
	a.performance_region_name,a.performance_province_name,a.performance_city_name,	
	a.business_number,
	a.customer_id,a.customer_code,a.customer_name,a.first_category_name,a.second_category_name,a.third_category_name,
	a.business_sign_month,
	a.owner_user_number,a.owner_user_name,
	a.business_type_code,a.business_type_name,
	a.customer_acquisition_type_code,a.customer_acquisition_type_name,
	a.contract_cycle_desc,a.estimate_contract_amount,
	a.business_sign_date,a.business_sign_date_2,a.first_business_sign_date,
	a.new_classify_name,a.num,a.next_sign_date,c.min_order_sdt
;
	
		
select * from csx_analyse_tmp.csx_analyse_tmp_cust_business_detail_02

