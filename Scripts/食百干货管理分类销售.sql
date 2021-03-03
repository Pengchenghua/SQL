
	

select
	m.goods_id
	, m.goods_name
	, m.bar_code
	, m.unit_name 
	, m.standard 
	, m.classify_large_code
	, m.classify_large_name
	, m.classify_middle_code
	, m.classify_middle_name 
	, m.classify_small_code 
	, m.classify_small_name 
from
	csx_dw.dws_basic_w_a_csx_product_m m
where
	sdt = 'current'
	and category_large_code ='1257';
	
select * from csx_tmp.ads_wms_r_d_goods_dept_turnover where sdt='20201210';


-- 表1
select province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	sum(sales_value)/count(distinct customer_no) per_ticket_sales 	
	from csx_tmp.sale_01 
where sale_group !='城市服务商'
group by 
	province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name;
	



--表1 取数
select
    region_code,
    region_name,
    province_code,
    a.province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
    sales_value ,
	profit ,
	profit/sales_value as profit_rate,
	sales_value/sale_cust per_ticket_sales ,
	sale_cust/all_sale_cust penetration_rate,
	sale_cust,
	all_sale_cust
from 
(select province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	--sum(sales_value)/count( DISTINCT customer_no) per_ticket_sales ,
	count(DISTINCT customer_no) as sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
) a
left join
(select province_name,
	count(distinct customer_no) as all_sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name
) b on a.province_name=b.province_name
left join
(select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank=13) c on a.province_name=c.province_name
;



--表2 取数
select
    region_code,
    region_name,
    province_code,
    a.province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	channel,
	customer_no ,
	customer_name,
	sale_group,
    sales_value ,
	profit ,
	profit/sales_value as profit_rate,
	sales_value/sale_cust per_ticket_sales ,
	sale_cust/all_sale_cust penetration_rate,
	sale_cust,
	all_sale_cust
from 
(select province_name,
	city_group_name,
	sale_group,
	channel,
	customer_no ,
	customer_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	--sum(sales_value)/count( DISTINCT customer_no) per_ticket_sales ,
	count(DISTINCT customer_no) as sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name,
	sale_group,
	customer_no ,
	customer_name,
	channel,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
) a
left join
(select province_name,
	count(distinct customer_no) as all_sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name
) b on a.province_name=b.province_name
left join
(select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank=13) c on a.province_name=c.province_name
;

-- 表三
select
    region_code,
    region_name,
    province_code,
    a.province_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	first_category_name,
	new_classify_name,
	sale_group,
    sales_value ,
	profit ,
	profit/sales_value as profit_rate,
	sales_value/sale_cust per_ticket_sales ,
	sale_cust/all_sale_cust penetration_rate,
	sale_cust,
	all_sale_cust
from 
(select province_name,
	city_group_name,
	sale_group,
	first_category_name,
	new_classify_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	--sum(sales_value)/count( DISTINCT customer_no) per_ticket_sales ,
	count(DISTINCT a.customer_no) as sale_cust
	from csx_tmp.sale_01  a 
left join 
(select w.customer_no ,w.first_category_name,w.second_category_name,coalesce(new_classify_name,'制造业/其他') new_classify_name from csx_dw.dws_crm_w_a_customer_20200924 w 
 left join 
 (select * from csx_tmp.new_customer_classify g ) g on w.second_category_code =g.second_category 
 where sdt='current')  b on a.customer_no =b.customer_no
where sale_group !='城市服务商'
and  a.classify_middle_code 
group by 
	province_name,
	sale_group,
	first_category_name,
	new_classify_name,
	city_group_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
) a
left join
(select province_name,
	count(distinct customer_no) as all_sale_cust
	from csx_tmp.sale_01  a 
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
group by 
	province_name
) b on a.province_name=b.province_name
left join
(select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank=13) c on a.province_name=c.province_name
;

select sum(sales_value) from 
(select province_name,
	city_group_name,
	sale_group,
	a.customer_no ,
	first_category_name,
	 new_classify_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	--sum(sales_value)/count( DISTINCT customer_no) per_ticket_sales ,
	count(DISTINCT a.customer_no) as sale_cust
	from csx_tmp.sale_01  a 
left join 
(select w.customer_no ,w.first_category_name,w.second_category_name,coalesce(new_classify_name,'制造业/其他') new_classify_name,g.new_id 
from csx_dw.dws_crm_w_a_customer_20200924 w 
 left join 
 (select * from csx_tmp.new_customer_classify g ) g on w.second_category_code =g.second_category 
 where sdt='current')  b on a.customer_no =b.customer_no
where sale_group !='城市服务商'
and a.classify_large_code in ('B01','B04','B05','B06','B07','B08')
and a.classify_middle_code!='B0104'
and new_classify_name is null 
group by 
	province_name,
	sale_group,
	customer_no,
	first_category_name,	
	city_group_name,
	new_classify_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
)a ;

-- 食用油类
select
	years ,
	-- weeknum,    
	region_code,
    region_name,
    province_code,
    a.province_name,
	city_group_name,
	channel_name,
	goods_code ,
	goods_name,
	standard,
	unit_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	first_category_name,
	new_classify_name,
	sale_group,
	avg_cost,
	avg_price,
	sales_qty,
    sales_value ,
	profit ,
	profit/sales_value as profit_rate,
	sales_value/sale_cust per_ticket_sales ,
	sale_cust
from 
(select 
	years,
	--weeknum,
	province_name,
	city_group_name,
	sale_group,
	channel_name,
	goods_code ,
	goods_name,
	standard,
	unit_name,
	first_category_name,
	new_classify_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	m.category_small_code,
	m.category_small_name,
	sum(sales_cost)/sum(sales_qty) avg_cost,
	sum(sales_value)/sum(sales_qty) avg_price,
	sum(sales_qty) sales_qty,
	sum(sales_cost)as sales_cost,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	--sum(sales_value)/count( DISTINCT customer_no) per_ticket_sales ,
	count(DISTINCT a.customer_no) as sale_cust
	from csx_tmp.sale_01  a 
join
(select calday,substr(calday,1,4) as years,
	concat(substr(calday,1,4),case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) else  cast (new_weeknum as string ) end) weeknum 
from csx_tmp.temp_date_m c 
where  (calday >='20190101' and calday <='20191130') or (calday >='20200101' and calday <='20201130') ) w on	a.sdt = w.calday
join
(select
	goods_id,
	goods_name,
	standard,
	unit_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_csx_product_m
where
	sdt = 'current'
	and category_small_code in ('12570105',
	'12570104',
	'12570103',
	'12570109',
	'12570107',
	'12570102',
	'12570108',
	'12570101',
	'12570106')) m on a.goods_code =m.goods_id 
left join 
(select w.customer_no ,w.first_category_name,w.second_category_name,coalesce(new_classify_name,'制造业/其他') new_classify_name 
from csx_dw.dws_crm_w_a_customer_20200924 w 
 left join 
 (select * from csx_tmp.new_customer_classify g ) g on w.second_category_code =g.second_category 
 where sdt='current')  b on a.customer_no =b.customer_no
where sale_group !='城市服务商' 
group by 
	--weeknum,
	years,
	channel_name,
	province_name,
	city_group_name,
	sale_group,
	goods_code,
	goods_name,
	standard,
	unit_name,
	first_category_name,
	new_classify_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
) a
left join
(select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank=13) c on a.province_name=c.province_name
;




-- 大米类

(select
	years,
	-- mon,    
	region_code,
    region_name,
    province_code,
    a.province_name,
	city_group_name,
	channel_name ,
	goods_code ,
	goods_name,
	standard,
	unit_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	first_category_name,
	new_classify_name,
	sale_group,
	avg_cost,
	avg_price,
	sales_qty,
    sales_value ,
	profit ,
	profit/sales_value as profit_rate,
	sales_value/sale_cust per_ticket_sales ,
	sale_cust
from 
(select 
	years,
	--mon,
	province_name,
	city_group_name,
	channel_name ,
	sale_group,
	goods_code ,
	goods_name,
	standard,
	unit_name,
	first_category_name,
	new_classify_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	m.category_small_code,
	m.category_small_name,
	sum(sales_cost)/sum(sales_qty) avg_cost,
	sum(sales_value)/sum(sales_qty) avg_price,
	sum(sales_qty) sales_qty,
	sum(sales_cost)as sales_cost,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	--sum(sales_value)/count( DISTINCT customer_no) per_ticket_sales ,
	count(DISTINCT a.customer_no) as sale_cust
	from csx_tmp.sale_01  a 
join
(select calday,substr(calday,1,4) as years,
	substr(calday,1,6) as mon,
	concat(substr(calday,1,4),case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) else  cast (new_weeknum as string ) end) weeknum 
from csx_tmp.temp_date_m c
where (calday >='20190101' and calday <='20191130') or (calday >='20200101' and calday <='20201130') ) w on	a.sdt = w.calday
join
(select
	goods_id,
	goods_name,
	standard,
	unit_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_csx_product_m
where
	sdt = 'current'
	and  category_middle_code='110132') m on a.goods_code =m.goods_id
left join 
(select w.customer_no ,w.first_category_name,w.second_category_name,coalesce(new_classify_name,'制造业/其他') new_classify_name 
from csx_dw.dws_crm_w_a_customer_20200924 w 
 left join 
 (select * from csx_tmp.new_customer_classify g ) g on w.second_category_code =g.second_category 
 where sdt='current')  b on a.customer_no =b.customer_no
where sale_group !='城市服务商' 
group by
	years,
	-- mon,
	province_name,
	city_group_name,
	sale_group,
	channel_name,
	goods_code,
	goods_name,
	standard,
	unit_name,
	first_category_name,
	new_classify_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
) a
left join
(select province_code,province_name,region_code,region_name from csx_dw.dim_area where area_rank=13) c on a.province_name=c.province_name

;

select sum(sales_value) from csx_dw.dws_sale_r_d_customer_sale a 
 join
(select w.customer_no  
from csx_dw.dws_crm_w_a_customer_20200924 w 
 where sdt='current'
 and w.attribute_code != 5
 ) b on a.customer_no=b.customer_no 
where sdt>='20200101' and sdt<='20201130'
and a.channel in ('1','7','9')
and category_middle_code='110132'
;


select
	goods_code,
	a.category_small_code,
	m.category_small_code,
	sum(sales_value)sale
from
	csx_tmp.sale_01 a
join
(select
	goods_id,
	goods_name,
	standard,
	unit_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_csx_product_m
where
	sdt = 'current'
	and category_small_code in ('12570105',
	'12570104',
	'12570103',
	'12570109',
	'12570107',
	'12570102',
	'12570108',
	'12570101',
	'12570106')
) m on a.goods_code =m.goods_id 
where
	1 = 1
	and sale_group != '城市服务商'
--	and a.category_small_code in ('12570105',
--	'12570104',
--	'12570103',
--	'12570109',
--	'12570107',
--	'12570102',
--	'12570108',
--	'12570101',
--	'12570106')
group by
	goods_code ,
	a.category_small_code,
	m.category_small_code;
	
select * from csx_dw.dws_basic_w_a_csx_product_m  where sdt='current' and goods_id  in ('951845','255741');
select * from csx_dw.dws_wms_r_d_entry_order_all_detail  where supplier_code ='75000021';


-- 计算TOP 10 商品
 
select 
	years,
	--mon,
	province_code ,
	province_name ,
	goods_code ,
	goods_name,
	standard,
	unit_name,
	division_code ,
	division_name ,
--	first_category_name,
--	new_classify_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	 avg_cost,
	 avg_price,
	 sales_qty,
	 sales_cost,
	 sales_value ,
	 profit ,
	row_number()over(order by sales_value desc) row_num ,
	 sale_cust
from 
(
select 
	years,
	--mon,
	province_code ,
	province_name ,
	goods_code ,
	goods_name,
	standard,
	unit_name,
	division_code ,
	division_name ,
--	first_category_name,
--	new_classify_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	m.category_small_code,
	m.category_small_name,
	sum(sales_cost)/sum(sales_qty) avg_cost,
	sum(sales_value)/sum(sales_qty) avg_price,
	sum(sales_qty) sales_qty,
	sum(sales_cost)as sales_cost,
	sum(sales_value)sales_value ,
	sum(profit)profit ,
	--sum(sales_value)/count( DISTINCT customer_no) per_ticket_sales ,
	count(DISTINCT a.customer_no) as sale_cust
	from csx_tmp.sale_01  a 
join
(select calday,substr(calday,1,4) as years,
	substr(calday,1,6) as mon,
	concat(substr(calday,1,4),case when length(cast (new_weeknum as string ))<2 then concat('0',cast (new_weeknum as string )) else  cast (new_weeknum as string ) end) weeknum 
from csx_tmp.temp_date_m c
where  (calday >='20200101' and calday <='20201130') ) w on	a.sdt = w.calday
join
(select
	goods_id,
	goods_name,
	standard,
	unit_name,
	category_small_code,
	category_small_name,
	division_code ,
	division_name 
from
	csx_dw.dws_basic_w_a_csx_product_m
where
	sdt = 'current'	  
	and (category_small_code in ('12570105',
	'12570104',
	'12570103',
	'12570109',
	'12570107',
	'12570102',
	'12570108',
	'12570101',
	'12570106') or   category_middle_code='110132')
	) m on a.goods_code =m.goods_id
left join 
(select w.customer_no ,w.first_category_name,w.second_category_name,coalesce(new_classify_name,'制造业/其他') new_classify_name 
from csx_dw.dws_crm_w_a_customer_20200924 w 
 left join 
 (select * from csx_tmp.new_customer_classify g ) g on w.second_category_code =g.second_category 
 where sdt='current')  b on a.customer_no =b.customer_no
where sale_group !='城市服务商' 
group by
	years,
	province_code ,
	province_name ,
	goods_code,
	goods_name,
	standard,
	unit_name,
	division_code ,
	division_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
grouping sets ((years,
	province_code ,
	province_name ,
	goods_code,
	goods_name,
	standard,
	unit_name,
	division_code ,
	division_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name),
	(years,
	goods_code,
	goods_name,
	standard,
	unit_name,
	division_code ,
	division_name ,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name))
)a;




--- 全国进货
 select
 	n.sdt,
 	n.business_type as business_type_code,
 	case when n.business_type like 'ZN0%' then '供应商配送'
 		when n.business_type like 'ZC0%' then '云超配送'
 		else f.business_type end business_type_name ,
--	s.dist_code ,
--	s.dist_name,
	goods_code,
	m.goods_name ,
	m.brand_name ,
	m.unit_name ,
	m.classify_large_code ,
	m.classify_large_name ,
	m.classify_middle_code ,
	m.classify_middle_name ,
	m.classify_small_code ,
	m.classify_small_name ,
	sum(receive_qty)receive_qty ,
	sum(receive_qty * price ) as receive_amt
from
	csx_dw.dws_wms_r_d_entry_order_all_detail n
join (
	select
		*
	from
		csx_dw.csx_shop s
	where
		sdt = 'current') s on s.location_code = n.receive_location_code
join 
(select * from csx_dw.dws_basic_w_a_csx_product_m m where sdt='current') m on n.goods_code=m.goods_id 
left join 
(select * from csx_ods.source_wms_r_d_bills_config f where sdt='20201218' and entity_flag=0 and type_code like 'P%') f on n.business_type =f.business_type_code 
where
	n.sdt >= '20200101'
	and n.sdt <= '20201130'
	and (n.entry_type like 'P%'or n.business_type in ('ZN01','ZN02','ZN03','ZC01'))
--	and goods_code in ('7657','301136',	'1058120','1058125','900241','1250298','843623','1012114','7632','834063','946912','873888','947390','1170750','1256775','317327','1208646',
--	'1168188',
--	'762651',
--	'1168187')
and (category_small_code in ('12570105',
	'12570104',
	'12570103',
	'12570109',
	'12570107',
	'12570102',
	'12570108',
	'12570101',
	'12570106') or   category_middle_code='110132')
group by 
	n.sdt,
--	s.dist_code ,
--	s.dist_name,
	goods_code,
	m.goods_name ,
	m.brand_name ,
	m.unit_name ,
	m.classify_large_code ,
	m.classify_large_name ,
	m.classify_middle_code ,
	m.classify_middle_name ,
	m.classify_small_code ,
	m.classify_small_name,
	n.business_type,
 		case when n.business_type like 'ZN0%' then '供应商配送'
 		when n.business_type like 'ZC0%' then '云超配送'
 		else f.business_type end;
 		
 	
 	select * from csx_dw.dws_wms_r_d_entry_order_all_detail  where sdt='20200918' and goods_code ='7632';
 	
 select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	sdt
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current';
	
select
	a.*,
	m.*
from
	csx_tmp.ads_sale_r_a_sale_item a
join (
	select
		m.goods_id,
		m.category_large_code ,
		m.category_large_name,
		m.category_middle_code ,
		m.category_middle_name ,
		m.classify_middle_code ,
		m.classify_middle_name 
	from
		csx_dw.dws_basic_w_a_csx_product_m m
	where
		sdt = 'current') m on
	a.goods_code = m.goods_id
where
	province_code = '2'
	and channel_code = '1'; 
	
select min(sdt) from csx_tmp.ads_wms_r_d_warehouse_sales where business_type_code ='82';
select min(sdt) from  csx_dw.wms_shipped_order where business_type_code  = '82'and sdt>='20200101' ;

refresh csx_dw.provinces_kanban;
select distinct  dist_code ,dist_name  from csx_dw.csx_shop  where sdt='current';