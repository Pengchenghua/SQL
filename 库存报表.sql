drop table csx_ods.wms_h_stock_temp01;
create temporary table csx_ods.wms_h_stock_temp01
as 
select distinct
	a.*
from
(
	select 
		*,
		regexp_replace(to_date(biz_time), '-', '') as edate 
	from 
		csx_ods.wms_accounting_stock_detail_view_ods
	where 
		sdt>=regexp_replace(date_sub(current_date,1),'-','')
) a 
join 
( 
	select 
		max(id) as max_id,
		product_code,
		location_code,
		reservoir_area_code,
		regexp_replace(to_date(biz_time), '-', '') as edate 
	from 
		csx_ods.wms_accounting_stock_detail_view_ods
	where 
		sdt =regexp_replace(date_sub(current_date,1),'-','')
		and 
		regexp_replace(to_date(biz_time),'-','')<=sdt
	group by 
		product_code,
		location_code,
		reservoir_area_code,
		regexp_replace(to_date(biz_time), '-', '') 
) b on a.id=b.max_id  and a.edate=b.edate;



--库存信息每日维度详情
drop table csx_ods.wms_h_stock_temp02;
create temporary table csx_ods.wms_h_stock_temp02
as 
select
	a.*,
	b.goods_id as goods_code,
	b.bar_code as goods_bar_code,
	b.goods_name as goods_name,
	b.division_code as div_id,
	b.division_name as div_name,
	b.department_id as dept_id,
	b.department_name as dept_name,
	b.category_large_code as catg_l_code,
	b.category_large_name as catg_l_name,
	b.category_middle_code as catg_m_code,
	b.category_middle_name as catg_m_name,
	b.category_small_code as catg_s_code,
	b.category_small_name as catg_s_name,
	b.standard as standard,
	if(c.goods_code is not null, '是', '否') as is_factory_goods,
	if(c.goods_code is not null, 1, 0) as is_factory_code	
from 
	csx_ods.wms_h_stock_temp01 as a
left join
	csx_dw.goods_m as b
on  b.sdt=regexp_replace(date_sub(current_date,1),'-','') and a.product_code=b.goods_id
left join
(
	select distinct 
		goods_code 
	from 
		csx_dw.factory_bom 
  	where 
  		sdt = 'current'   
) as c on  a.product_code =c.goods_code;


--获取入库商品每日最后一次入库日期
drop table csx_ods.wms_h_entry_temp01;
create temporary table csx_ods.wms_h_entry_temp01
as 
select
	a.*,
	b.edate
from 
(
	select distinct
		*
	from
		csx_ods.wms_entry_batch_detail_ods
	where 
		sdt>='20191015'		--增量导入的时候是跨了夜（2天）所以需要去重、
) a
join
(
	select 
		max(id) as max_id,
		product_code,
		location_code,
		reservoir_area_code,
		regexp_replace(to_date(update_time), '-', '') as edate
		
	from
		csx_ods.wms_entry_batch_detail_ods
	where 
		sdt>='20191015'
	group by 
		product_code,
		location_code,
		reservoir_area_code,
		regexp_replace(to_date(update_time), '-', '')
) b on a.id=b.max_id;


--根据库存日期筛选得到最后一次入库日期的
drop table csx_ods.wms_h_entry_temp02;
create temporary table csx_ods.wms_h_entry_temp02
as 
select
	h1.location_code as location_code,
	h1.reservoir_area_code as reservoir_area_code,
	h1.product_code as product_code,
	h1.edate as edate,
	h1.max_sdt as max_sdt,
	h2.update_time as update_time,
	h2.receive_qty as receive_qty

from 
(
	select
		b.product_code as product_code,
		b.location_code as location_code, 
		b.reservoir_area_code as reservoir_area_code,
		b.edate as edate,
		max(a.edate) as max_sdt
	from 
		csx_ods.wms_h_stock_temp02 b 
	left join
		csx_ods.wms_h_entry_temp01  a
	on 
		a.product_code=b.goods_code 
		and 
		a.location_code=b.location_code 
		and 
		a.reservoir_area_code=b.reservoir_area_code 
	where  
		a.update_time<=b.posting_time
	group by
		b.product_code,
		b.location_code,
		b.reservoir_area_code,
		b.edate
) as h1
left join
	csx_ods.wms_h_entry_temp01 as h2
on 
	h1.location_code=h2.location_code
	and
	h1.reservoir_area_code=h2.reservoir_area_code
	and
	h1.product_code=h2.product_code
	and
	h1.max_sdt = h2.edate;



--最终数据插入到 hive 到mysql的表中
--truncate table csx_dw.fixation_report_product_stock_factory; --清空表数据、

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
truncate table csx_dw.fixation_report_product_stock_factory;
insert overwrite table csx_dw.fixation_report_product_stock_factory partition (sdt)
select
	row_number() over(order by a.id desc) as id,
	a.location_code as location_code,
	a.location_name as location_name,
	a.reservoir_area_code as reservoir_area_code,
	a.reservoir_area_name as reservoir_area_name,
	a.goods_code as goods_code,
	a.goods_bar_code as goods_bar_code,
	a.goods_name as goods_name,
	a.div_id as div_id,
	a.div_name as div_name,
	a.dept_id as dept_id,
	a.dept_name as dept_name,
	a.catg_l_code as catg_l_code,
	a.catg_l_name as catg_l_name,
	a.catg_m_code as catg_m_code,
	a.catg_m_name as catg_m_name,
	a.catg_s_code as catg_s_code,
	a.catg_s_name as catg_s_name,
	a.standard as unit_name,
	a.unit as unit,
	a.after_price as avg_price,
	a.after_qty as stock_qty,
	a.after_amt as stock_amt,
--	b.max_sdt as last_in_datetime,
	split(b.update_time,'\\.')[0] as last_in_datetime,
	b.receive_qty as last_in_qty,
	a.is_factory_goods as is_factory_goods,
	a.is_factory_code as is_factory_code,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as update_time,
	a.edate as sdt
	
from 
	csx_ods.wms_h_stock_temp02 a
join
	csx_ods.wms_h_entry_temp02 b 
on 
	a.location_code=b.location_code 
	and 
	a.reservoir_area_code=b.reservoir_area_code
	and 
	a.product_code=b.product_code
	and
	a.edate=b.edate;