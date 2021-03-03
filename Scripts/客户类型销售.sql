select
select
	division_code,
	division_name,
	province_code ,province_name,
	firm_id,
	firm_name,
	sale,
	profit,
	sale_sku,
	sale_cust,
	sale_no,
	sale/sum(sale)over(partition by province_code,province_name) as sale_ratio
	--sum(sale)over(partition by province_code) as sale_all
	from (
	select 
	province_code ,province_name,
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
	round(sum(sale)/sum(sale_no)/10000,2) as avg_sale_no
--	sum(sale_ratio)sale_ratio
FROM (
select
	division_code,
	division_name,
	province_code ,province_name,
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
	group by 
	division_code,
	division_name,
	province_code ,province_name,
	COALESCE(b.firm_id,
	division_code) ,
	COALESCE(b.firm_name,
	division_name)
	)a
	where 1=1 	
union all 
select
	div_id as  division_code,
	div_name as division_name,
	province_code ,province_name, 
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
	CASE when division_code in ('10','11') then '10' when division_code in('12','13','14') then '12' else division_code end div_id,
	cASE when division_code in ('10','11') then '生鲜采购部' when division_code in('12','13','14') then '食百采购部' else division_name end div_name,
	province_code ,province_name,
	'00' as firm_id,
	'小计' firm_name,
	SUM(sales_value)sale,
	sum(profit)profit,
	count(DISTINCT goods_code)sale_sku,
	ndv(DISTINCT customer_no)sale_cust,
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

	group by 
	province_code ,province_name,
	 CASE when division_code in ('10','11') then '10' when division_code in('12','13','14') then '12' else division_code end   ,
	cASE when division_code in ('10','11') then '生鲜采购部' when division_code in('12','13','14') then '食百采购部' else division_name end
	)a
	where 1=1
	union all 	
select
	'00'division_code,
	'总计'division_name,
	province_code ,province_name,
	'' as firm_id,
	'' firm_name,
	SUM(sales_value)sale,
	sum(profit)profit,
	count(DISTINCT goods_code)sale_sku,
	ndv(DISTINCT customer_no)sale_cust,	
	cast (ndv(DISTINCT case when return_flag !='X' then order_no end) as bigint)  sale_no,
	1.0 sale_ratio
from
	csx_dw.customer_sale_m a
where
	sdt >= regexp_replace(to_date('${sdate}'),'-','')
	and sdt <= regexp_replace(to_date('${edate}'),'-','')
group by province_code ,province_name
	)a
	group by division_code,
	division_name,
province_code ,province_name,
	firm_id,
	firm_name
	order by 
province_code, division_code,firm_id;

select * from csx_dw.csx_shop where sdt='current';

select * from dim.dim_shop_latest;

SELECT  shop_name FROM csx_dw.csx_shop where sdt='current' and  (CASE WHEN province_code='W0H4' then 350000 else province_code end );

-- 客户类型销售

select
	province_code,
	province_name,
	division_code,
	division_name,
	case
		when b.attribute = '合伙人客户' then '合伙人'
		else channel_name
end note,
COUNT(DISTINCT goods_code)as sale_sku,
	sum(sale)sale,
	sum(profit)profit
from
	(
	select
		province_code,
		province_name,
		channel_name,
		customer_no,
		customer_name,
		division_code,
		division_name,
		goods_code,
		sum(sales_value)sale,
		sum(profit)profit
	from
		csx_dw.customer_sale_m
	where
		sdt >= '20200301'
		and sdt <= '20200321'
	group by
		province_code,
		province_name,
		division_code,
		division_name,
		customer_no,
		customer_name,
		channel_name,goods_code)a
JOIN (
	select
		customer_no,
		attribute
	from
		csx_dw.customer_m
	where
		sdt = '20200320'
		and customer_no != '' )b on
	a.customer_no = b.customer_no
group by
	province_code,
	province_name,
	division_code,
	division_name,
	case
		when b.attribute = '合伙人客户' then '合伙人'
		else channel_name
end ;

-- 汇总
select
	
	division_code,
	division_name,
	case
		when b.attribute = '合伙人客户' then '合伙人'
		when channel_name in ('供应链(生鲜)','供应链(食百)') then '供应链'
		else channel_name
end note,
COUNT(DISTINCT goods_code)as sale_sku,
	sum(sale)sale,
	sum(profit)profit
from
	(
	select
		province_code,
		province_name,
		channel_name,
		customer_no,
		customer_name,
		division_code,
		division_name,
		goods_code,
		sum(sales_value)sale,
		sum(profit)profit
	from
		csx_dw.customer_sale_m
	where
		sdt >= '20200301'
		and sdt <= '20200321'
	group by
		province_code,
		province_name,
		division_code,
		division_name,
		customer_no,
		customer_name,
		channel_name,goods_code)a
JOIN (
	select
		customer_no,
		attribute
	from
		csx_dw.customer_m
	where
		sdt = '20200320'
		and customer_no != '' )b on
	a.customer_no = b.customer_no
group by

	division_code,
	division_name,
		case
		when b.attribute = '合伙人客户' then '合伙人'
		when channel_name in ('供应链(生鲜)','供应链(食百)') then '供应链'
		else channel_name
end ;

select customer_no,is_parter,`attribute`,case when `attribute`='合伙人客户' then '是' else '否'end note  from csx_dw.customer_m where sdt='20200320' and customer_no!='' and channel !='商超'
;
    select
        province_code,
        province_name,
        sale/10000 sale,
        cust_num,
        sale_num,
        zones,
        zone_num 
    from
        csx_dw.provinces_kanban_frequency  
    where
        sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') 
       and  province_code='1';