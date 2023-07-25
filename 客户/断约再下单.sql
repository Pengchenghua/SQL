
-- ================================================================================================================	
-- 断约再下单 激活明细 销售明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zx_jh_customer_sale;
create table csx_analyse_tmp.csx_analyse_tmp_zx_jh_customer_sale
as
select
	b.performance_region_name,b.performance_province_name,b.performance_city_name,b.sale_month as jh_month,b.customer_code,b.customer_name,
	b.first_category_name,b.second_category_name,b.third_category_name,
	a.sale_month,a.sale_amt,a.profit,a.profit_rate,c.customer_large_level,
	row_number()over(partition by b.customer_code order by a.sale_month) as rn
from
	( -- 销售明细
	select
		substr(sdt,1,6) as sale_month,customer_code,sum(sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(sale_amt)) as profit_rate
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20220101' and '20230228' -- 销售日期
		and channel_code in('1','7','9') -- 渠道编码(1:大 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)	
		and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
	group by 
		substr(sdt,1,6),customer_code
	) a 			
	join	
		(	
		select
			a.performance_region_name,a.performance_province_name,a.performance_city_name,a.customer_code,a.customer_name,a.sale_month,
			a.first_category_name,a.second_category_name,a.third_category_name,a.rn		
		from
			(
			select 
				a.performance_region_name,a.performance_province_name,a.performance_city_name,a.customer_code,b.customer_name,substr(a.sdt,1,6) as sale_month,
				b.first_category_name,b.second_category_name,b.third_category_name,row_number()over(partition by a.customer_code order by a.sdt desc) as rn
			from
				(
				select
					performance_region_name,performance_province_name,performance_city_name,customer_code,sdt,
					-- to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))) as sdt_date,
					-- to_date(from_unixtime(unix_timestamp(lead(sdt,1,null)over(partition by customer_code order by sdt desc),'yyyyMMdd'))) as lead_sdt_date,
					datediff(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),to_date(from_unixtime(unix_timestamp(lead(sdt,1,null)over(partition by customer_code order by sdt desc),'yyyyMMdd')))) as diff_days
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt between '20190101' and '20230228' -- 历史所有数据
					and channel_code in('1','7','9') -- 渠道编码(1:大 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)	
					and order_channel_code not in (4,6) -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
				) a 
				join
					(
					select 
						customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
						performance_province_name
					from 
						csx_dim.csx_dim_crm_customer_info
					where 
						sdt = 'current'
						and channel_code in('1','7','9')
						and cooperation_mode_code='01' -- 非一次性  合作模式编码(01长期,02一次性)
					) b on b.customer_code=a.customer_code	
			where
				a.diff_days>90
			) a 
		where
			rn=1
			and sale_month>='202201'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			customer_no,month,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='201901' and month<='202302'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no,month,customer_large_level
		) c on c.customer_no=a.customer_code and c.month=a.sale_month	
;
select * from csx_analyse_tmp.csx_analyse_tmp_zx_jh_customer_sale				
