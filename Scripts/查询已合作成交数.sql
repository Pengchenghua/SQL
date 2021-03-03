-- CONNECTION: name= HVIE
-- 获取已合作的客户 csx_dw.customer_simple_info_v2 
 drop
	table
		if EXISTS b2b_tmp.cust_sign;

CREATE
	TEMPORARY TABLE
		IF not EXISTS b2b_tmp.cust_sign as select
			sales_province ,
			region_province_name ,
			district_manager_name,
			district_manager_id,
			region_city ,
			sales_user_id ,
			sales_user_name,
			customer_number ,
			customer_name ,
			to_date(sign_time) sign_date
		from
			csx_dw.customer_simple_info_v2 a
		JOIN (
				select
					sales_id ,
					district_manager_name,
					district_manager_id
				from
					sale_org_v3
				WHERE
					sdt = '20190728'
					and POSITION = 'SALES'
			)b on
			a.sales_user_id = b.sales_id
			and sdt = '20190728' ;
-- 当月有销售的客户
 drop
	table
		if EXISTS b2b_tmp.sale_cust;

CREATE
	TEMPORARY TABLE
		IF not EXISTS b2b_tmp.sale_cust as select
			customer_no ,
			min(sdt) min_sdt
		from
			csx_dw.sale_b2b_item
		where
			sdt >= '20190701'
			and sdt <'20190729'
			and sales_type != 'md'
		group by
			customer_no;
--判断合作客户与未客户及签约客户数
select sales_province ,
			region_province_name ,
			district_manager_name,
			district_manager_id,
			region_city ,
			sales_user_name,
			count(customer_number)cust_cn,
			count(case when sale_sdt!=''  then customer_number end  )sale_cn
			,sum (new_sign_cust)new_sign_cn,
			sum(new_sale_cust )new_sale_cn

from 
(
select a.sales_province ,
			region_province_name ,
			district_manager_name,
			district_manager_id,
			region_city ,
			sales_user_id ,
			sales_user_name,
			customer_number ,
			customer_name ,
			sign_date,min_sdt as sale_sdt,
			case when sign_date>='2019-07-01' then '1' else '0' end  new_sign_cust,
			case when sign_date>='2019-07-01' and min_sdt>=sign_date then '1' else '0' end new_sale_cust
			FROM b2b_tmp.cust_sign a 
left join
(select customer_no,min_sdt from b2b_tmp.sale_cust) b on a.customer_number=b.customer_no
) a
group by sales_province ,
			region_province_name ,
			district_manager_name,
			district_manager_id,
			region_city ,
			sales_user_name;
			
		select * FROM b2b_tmp.cust_sign a 