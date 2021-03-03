select
	division_code,
	division_name,
	firm_id,
	firm_name,
	round(sum(sale)/10000,2) sale,
	round(sum(profit)/10000,2) profit,
	sum(profit)/sum(sale)*1.00 as prorate,
	sum(sale_sku)sale_sku,
	sum(sale_cust)sale_cust,
	sum(sale_no)sale_no,
	round(sum(sale)/sum(sale_no)/10000,2) as avg_sale_no,
	sum(sale_ratio)sale_ratio
	from (
select
	division_code,
	division_name,
	firm_id,
	firm_name,
	sale,
	profit,
	sale_sku,
	sale_cust,
	sale_no,
	sale/sum(sale)over(PARTITION by division_name) as sale_ratio
FROM (
select
	division_code,
	division_name,
	COALESCE(b.firm_id,
	division_code) as firm_id,
	COALESCE(b.firm_name,
	division_name)as firm_name,
	SUM(sales_value)sale,
	sum(profit)profit,
	ndv(DISTINCT customer_no)sale_cust,
	count(DISTINCT goods_code)sale_sku,
	cast (ndv(DISTINCT case when return_flag !='X' then order_no end) as bigint)  sale_no
from
	csx_dw.customer_sale_m a
left join (
	select
		catg_s_id,
		catg_s_name,
		firm_g1_name as firm_name,
		firm_g1_id as firm_id
	from
		dim.dim_catg
	where
		sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-',''))b on a.category_small_code=b.catg_s_id
where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <= regexp_replace(to_date('${edate}'),'-','')
	${if(len(prov)==0,"","and province_code in ('"+prov+"')")}
	${if(len(chann)==0,"","and channel in ('"+chann+"')")}
	group by 
	division_code,
	division_name,
	COALESCE(b.firm_id,
	division_code) ,
	COALESCE(b.firm_name,
	division_name)
	)a
	where 1=1 
	
union all 
select
	division_code,
	division_name,
	firm_id,
	firm_name,
	sale,
	profit,
	sale_sku,
	sale_cust,
	sale_no,
	sale/sum(sale)over() as sale_ratio
FROM (
select
	CASE when division_code in ('10','11') then '10' when division_code in('12','13','14') then '12' else division_code end division_code,
	CASE when division_code in ('10','11') then '生鲜采购部' when division_code in('12','13','14') then '食百采购部' else division_name end division_name,
	'00' as firm_id,
	'合计' firm_name,
	SUM(sales_value)sale,
	sum(profit)profit,
	ndv(DISTINCT customer_no)sale_cust,
	count(DISTINCT goods_code)sale_sku,
	cast (ndv(DISTINCT case when return_flag !='X' then order_no end) as bigint)  sale_no
from
	csx_dw.customer_sale_m a
left join (
	select
		catg_s_id,
		catg_s_name,
		firm_g1_name as firm_name,
		firm_g1_id as firm_id
	from
		dim.dim_catg
	where
		sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-',''))b on a.category_small_code=b.catg_s_id
where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <= regexp_replace(to_date('${edate}'),'-','')
${if(len(prov)==0,"","and province_code in ('"+prov+"')")}
${if(len(chann)==0,"","and channel in ('"+chann+"')")}
	group by 
	 division_code  ,
	division_name  
	)a
	where 1=1
	union all 	
select
	'00'division_code,
	'总计'division_name,
	'00' as firm_id,
	'合计' firm_name,
	SUM(sales_value)sale,
	sum(profit)profit,
	ndv(DISTINCT customer_no)sale_cust,
	count(DISTINCT goods_code)sale_sku,
	cast (ndv(DISTINCT case when return_flag !='X' then order_no end) as bigint)  sale_no,
	1.0 sale_ratio
from
	csx_dw.customer_sale_m a
where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <= regexp_replace(to_date('${edate}'),'-','')
${if(len(prov)==0,"","and province_code in ('"+prov+"')")}
${if(len(chann)==0,"","and channel in ('"+chann+"')")}
	)a
	group by division_code,
	division_name,
	firm_id,
	firm_name
	order by division_code,firm_id;