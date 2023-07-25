
select
	province_code ,
	province_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end channel_name ,
	sum(case when classify_middle_code='B0304' then sales_value end ) as b_sales_value,
	sum(case when classify_middle_code='B0304' then profit end ) as b_profit,
	sum(sales_value)sales_value ,
	sum(profit) profit
from
	csx_dw.dws_sale_r_d_customer_sale a
join (
	select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current'
	--and classify_middle_code ='B0304'
) b on a.category_small_code=b.category_small_code
where
	sdt >= '20201001'
	and sdt <= '20201031'
group by province_code ,
	province_name ,
case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end ;
	
select sum(sales_value) from csx_dw.dws_sale_r_d_customer_sale where sdt like '202010%';



select 
	province_code ,
	province_name,
	channel_name ,
		goods_code ,
		goods_name ,
		unit ,
		classify_small_code ,
		classify_small_name ,
	b_sales_value,
	b_profit,
	b_sales_value/sum(b_sales_value)over(partition by province_name,channel_name ) as sale_ratio,
	sales_value ,
	profit
from (
select
	province_code ,
	province_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end channel_name ,
		goods_code ,
		goods_name ,
		unit ,
		classify_small_code ,
		classify_small_name ,
	sum(case when classify_middle_code='B0304' then sales_value end ) as b_sales_value,
	sum(case when classify_middle_code='B0304' then profit end ) as b_profit,
	sum(sales_value)sales_value ,
	sum(profit) profit
from
	csx_dw.dws_sale_r_d_customer_sale a
join (
	select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current'
	and classify_middle_code ='B0304'
) b on a.category_small_code=b.category_small_code
where
	sdt >= '20201001'
	and sdt <= '20201031'
group by province_code ,
	province_name ,
case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end,
		goods_code ,
		goods_name ,
		unit ,
		classify_small_code ,
		classify_small_name 
)a;

select
	province_code ,
	province_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end channel_name ,
	customer_no,
	customer_name,
	first_category,
	c.new_classify_name,
	goods_code ,
	p.goods_name,
	p.standard,
	unit_name ,
	classify_small_code,
	classify_small_name,
	b.category_small_code,
	p.category_small_name,
	avg(cost_price) cost_price ,
	avg(sales_price) sales_price ,
	sum(sales_qty) qty,
	sum(sales_value) sales_value ,
	sum(profit) profit ,
	sum(profit)/sum(a.sales_value) profit_rate
from
	csx_dw.dws_sale_r_d_customer_sale a
	join 
	(select
	pm.goods_id,
	pm.goods_name,
	category_small_name,
	pm.unit_name ,
	pm.standard
from
	csx_dw.dws_basic_w_a_csx_product_m pm
where
	sdt = 'current') p on a.goods_code =p.goods_id
join (select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current'
	and classify_middle_code = 'B0304') b on a.category_small_code = b.category_small_code
left join csx_tmp.new_customer_classify c on	a.second_category_code = c.second_category
where
	sdt >= '20201001'
	and sdt <= '20201031'
group by province_code ,
	province_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end ,
	customer_no,
	customer_name,
	first_category,
	c.new_classify_name,
	goods_code ,
	p.goods_name,
	p.standard,
	unit_name ,
	classify_small_code,
	classify_small_name,
	b.category_small_code,
	p.category_small_name;


select distinct second_category,second_category_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current' and channel_code ='1';





-- 按照周六-周五计算销售
select
    new_weeknum,
    date_m ,
	province_code ,
	province_name,
	city_name ,
	a.city_real,
	a.dc_code,
	a.dc_name,
	channel_name ,
	customer_no,
	customer_name,
	first_category,
	c.new_classify_name,
	goods_code ,
	goods_name,
	standard,
	unit_name ,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	round((sales_value-profit)/qty,2)cost_price ,
	round(sales_value/qty,2) sales_price ,
	qty,
	sales_value ,
	profit ,
	profit_rate
from (
select
    j.new_weeknum,
    concat(if(j.new_week_first='20191228','20200101',new_week_first),'-',new_week_last) as date_m ,
	province_code ,
	province_name,
	city_name ,
	a.city_real,
	a.dc_code,
	a.dc_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end channel_name ,
	customer_no,
	customer_name,
	first_category,
	second_category_code,
	second_category ,
	goods_code ,
	p.goods_name,
	p.standard,
	unit_name ,
	classify_small_code,
	classify_small_name,
	b.category_small_code,
	p.category_small_name,
	avg(cost_price) cost_price ,
	avg(sales_price) sales_price ,
	sum(sales_qty) qty,
	sum(sales_value) sales_value ,
	sum(profit) profit ,
	sum(profit)/sum(a.sales_value) profit_rate
from
	csx_dw.dws_sale_r_d_customer_sale a
	join 
	(select pm.goods_id,pm.goods_name,category_small_name,pm.unit_name ,pm.standard from csx_dw.dws_basic_w_a_csx_product_m pm where sdt='current') p on a.goods_code =p.goods_id
join (select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current'
	and classify_large_code = 'B06') b on a.category_small_code = b.category_small_code
join 
csx_tmp.temp_date_m j on a.sdt=j.calday 
where
	sdt >= '20200101'
	and sdt <= '20201031'
group by province_code ,
	province_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end ,
	customer_no,
	customer_name,
	first_category,
	second_category_code,
	second_category ,
	goods_code ,
	p.goods_name,
	p.standard,
	unit_name ,
	classify_small_code,
	classify_small_name,
	b.category_small_code,
	p.category_small_name,
	city_name ,
	a.city_real,
	a.dc_code,
	a.dc_name,
 	j.new_weeknum,
 	 concat(if(j.new_week_first='20191228','20200101',new_week_first),'-',new_week_last)
 )a
 left join csx_tmp.new_customer_classify c on	a.second_category_code = c.second_category

order by  new_weeknum;

	
	

--周	战报省区名称	城市	仓	基础品类小类	SKU ID 	SKU 名称	采购类型	供应商ID	供应商名称	采购价	采购量	采购金额	生产成本	仓促成本	配送成本

select new_weeknum,
dist_code,
dist_name,
prefecture_city_name,
receive_location_code as dc_code,
shop_name,
classify_small_code,
classify_small_name,
a.category_small_code ,
a.category_small_name ,
goods_code,
h.goods_name,
standard,
h.unit_name ,
--p.order_code,
if(a.business_type_code='54','地采','') as loca_pur,
supplier_code,
vendor_name,
sum(amount)/sum(receive_qty) as avg_price,
sum(receive_qty)qty,
sum(amount)amt 
from csx_dw.wms_entry_order a 
join 
(select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current'
	and classify_large_code = 'B06') b on a.category_small_code = b.category_small_code
join 
(select location_code ,shop_name,dist_code,dist_name,prefecture_city_name from csx_dw.csx_shop where sdt='current') g on receive_location_code =location_code
join 
(select sm.vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m sm where sdt='current')k on a.supplier_code =k.vendor_id
join 
(select pm.goods_id ,pm.goods_name,unit_name ,pm.standard from csx_dw.dws_basic_w_a_csx_product_m  pm where sdt='current') h on a.goods_code =h.goods_id
join csx_tmp.temp_date_m j on a.sdt=j.calday 
where sdt>='20200101'
	and sdt<'20201101'
and a.entry_type  like '采购%'
and receive_status=2
group by 
dist_code,
dist_name,
receive_location_code ,
shop_name,
prefecture_city_name,
classify_small_code,
classify_small_name,
a.category_small_code ,
a.category_small_name ,
goods_code,
h.goods_name,
h.unit_name ,
standard,
-- p.order_code,
if(a.business_type_code='54','地采','') ,
supplier_code,
vendor_name,
new_weeknum
order by new_weeknum;
	
	

select
	*
from
	csx_dw.wms_entry_order a
where
	sdt >= '20201017'
	and sdt <= '20201023'
	and receive_location_code = 'W053'
	and goods_code = '113'
	and a.entry_type  like '采购%';
	

-- 按月提取
select
    a.mon,
	a.province_code ,
	province_name,
	a.channel_name ,
	a.customer_no,
	customer_name,
	first_category,
	c.new_classify_name,
	goods_code ,
	goods_name,
	standard,
	unit_name ,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	round((sales_value-profit)/qty,2)cost_price ,
	round(sales_value/qty,2) sales_price ,
	qty,
	sales_value ,
	profit ,
	profit_rate,
	all_sales_value
from (
select
    substr(sdt,1,6) mon,
	province_code ,
	province_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end channel_name ,
	customer_no,
	customer_name,
	first_category,
	second_category_code,
	second_category ,
	goods_code ,
	p.goods_name,
	p.standard,
	unit_name ,
	classify_small_code,
	classify_small_name,
	b.category_small_code,
	p.category_small_name,
	avg(cost_price) cost_price ,
	avg(sales_price) sales_price ,
	sum(sales_qty) qty,
	sum(sales_value) sales_value ,
	sum(profit) profit ,
	sum(profit)/sum(a.sales_value) profit_rate
from
	csx_dw.dws_sale_r_d_customer_sale a
	join 
	(select pm.goods_id,pm.goods_name,category_small_name,pm.unit_name ,pm.standard from csx_dw.dws_basic_w_a_csx_product_m pm where sdt='current') p on a.goods_code =p.goods_id
 join 
(select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current'
	and classify_large_code = 'B06') b on a.category_small_code = b.category_small_code
where
	sdt >= '20200101'
	and sdt <= '20201031'
group by  substr(sdt,1,6),
	province_code ,
	province_name,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end ,
	customer_no,
	customer_name,
	first_category,
	second_category_code,
	second_category ,
	goods_code ,
	p.goods_name,
	p.standard,
	unit_name ,
	classify_small_code,
	classify_small_name,
	b.category_small_code,
	p.category_small_name
 )a
 left join (select
    substr(sdt,1,6) mon,
	province_code ,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end channel_name ,
	customer_no,
	sum(sales_value) all_sales_value 
from csx_dw.dws_sale_r_d_customer_sale a
where
	sdt >= '20200101'
	and sdt <= '20201031'
group by  substr(sdt,1,6),
	province_code ,
	case when channel in ('5','6') and customer_no like 'S%' then '商超'
		when (channel in ('5','6') and customer_no not like 'S%' ) then '大' 
		when channel in ('2') then '商超'
		else '大' 
		end ,
	customer_no
	) j on a.mon=j.mon and a.province_code =j.province_code and a.customer_no =j.customer_no and a.channel_name=j.channel_name
 left join csx_tmp.new_customer_classify c on	a.second_category_code = c.second_category
order by  mon;

select sum(sales_value) from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200401' and sdt<'20200501' and customer_no ='SW011'and province_code ='35' ;