--获取库存/每日的库存时间、 这里有一个逻辑漏洞：每日需要补充缺失商品进每日、

--补充每日缺失商品数据、

--获取历史日期
DROP TABLE IF EXISTS csx_ods.wms_h_stock_temp01;
CREATE TEMPORARY TABLE csx_ods.wms_h_stock_temp01
AS
select
	max(id) as id ,
	b.product_code,
	b.location_code,
	b.reservoir_area_code,
	a.edate
from 
(
	select distinct
		regexp_replace(to_date(biz_time),'-','') as edate
	from 
		csx_ods.wms_accounting_stock_detail_view_ods
	where 
		sdt>= regexp_replace(date_sub(CURRENT_DATE, 1), '-','') 
) as  a 
join
( 
	select 
		id,
		product_code,
		location_code,
		reservoir_area_code,
		regexp_replace(to_date(biz_time),'-','') as edate 
	from 
		csx_ods.wms_accounting_stock_detail_view_ods
	where 
		sdt>= regexp_replace(date_sub(CURRENT_DATE, 1), '-','') 
) as b  on 1=1
where 
	b.edate <= a.edate    --这个表其实就是再做笛卡尔积、后期还是比较危险的
group by 
	b.product_code,
	b.location_code,
	b.reservoir_area_code,
	a.edate
;

--获取每日的库存信息、没有的库存信息的商品信息 顺延到下一日

DROP TABLE IF EXISTS csx_ods.wms_h_stock_temp02;

CREATE TEMPORARY TABLE csx_ods.wms_h_stock_temp02
AS
select
	a.*,
	b.edate
from 
(
	select
		*
	from 
		csx_ods.wms_accounting_stock_detail_view_ods
	where 
		sdt>= regexp_replace(date_sub(CURRENT_DATE, 1), '-','') 
) as a 
join
(
	select
		id,
		product_code,
		location_code,
		reservoir_area_code,
		edate
	from 
		csx_ods.wms_h_stock_temp01
	
) as b on a.id=b.id and a.product_code=b.product_code and a.location_code=b.location_code and a.reservoir_area_code =b.reservoir_area_code;



--获取入库商品每日最后一次入库日期
DROP TABLE IF EXISTS csx_ods.wms_h_stock_temp03;
CREATE
TEMPORARY TABLE csx_ods.wms_h_stock_temp03 
AS
SELECT 
	a. product_code,
    a.location_code,
    txn_amt,
    txn_qty,
    a.reservoir_area_code,
	a.biz_time,
    regexp_replace(to_date(a.biz_time),'-','')AS biz_date
FROM
(
	SELECT 
		product_code,
        location_code,
        txn_amt,
        txn_qty,
        reservoir_area_code,
        biz_time
   FROM 
		csx_ods.wms_accounting_stock_detail_view_ods
   WHERE 
		sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (), 1)),'-','') 
) a
JOIN
 (
	SELECT 
		max(biz_time) AS max_sdt,
          product_code,
          location_code,
          reservoir_area_code,
		  regexp_replace(to_date(biz_time),'-','') as edate
   FROM 
		csx_ods.wms_accounting_stock_detail_view_ods
   WHERE 
		sdt >= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP (), 1)),'-','')
		AND 
		in_or_out = 0
   GROUP BY 
		product_code,
        location_code,
        reservoir_area_code,
		regexp_replace(to_date(biz_time),'-','')
) b ON a.biz_time = b.max_sdt
AND a.location_code = b.location_code
AND a.product_code = b.product_code
AND a.reservoir_area_code = b.reservoir_area_code ;

	
--根据库存日期筛选得到最后一次入库日期的
drop table csx_ods.wms_h_entry_temp05;
create temporary table csx_ods.wms_h_entry_temp05
as 
select
	h1.location_code as location_code,
	h1.reservoir_area_code as reservoir_area_code,
	h1.product_code as product_code,
	h1.edate as edate,
	h1.max_sdt as max_sdt,
	h2.biz_time as biz_time,
	h2.txn_qty as txn_qty

from 
(
	select
		b.product_code as product_code,
		b.location_code as location_code, 
		b.reservoir_area_code as reservoir_area_code,
		b.edate as edate,
		max(a.biz_date) as max_sdt
	from 
		csx_ods.wms_h_stock_temp02 b 
	left join
		csx_ods.wms_h_stock_temp03 a
	on 
		a.product_code=b.product_code 
		and 
		a.location_code=b.location_code 
		and 
		a.reservoir_area_code=b.reservoir_area_code 
	where  
		a.biz_time<=b.biz_time
	group by
		b.product_code,
		b.location_code,
		b.reservoir_area_code,
		b.edate
) as h1
left join
	csx_ods.wms_h_stock_temp03 as h2
on 
	h1.location_code=h2.location_code
	and
	h1.reservoir_area_code=h2.reservoir_area_code
	and
	h1.product_code=h2.product_code
	and
	h1.max_sdt = h2.biz_date;






--最终数据插入到 hive 到mysql的表中


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
truncate table csx_dw.fixation_report_product_stock_factory;
insert overwrite table csx_dw.fixation_report_product_stock_factory partition (sdt)
SELECT 
 	row_number() over(order by a.location_code desc) as id,
	a.location_code location_code,
    c.shop_name as location_name,
    a.reservoir_area_code as reservoir_area_code,
    a.reservoir_area_name as reservoir_area_name,    
    a.product_code goods_code,
    c.goods_bar_code as goods_bar_code,
    c.goods_name as goods_name,
    coalesce(d.division_code,c.bd_id) as div_id,
    coalesce(d.division_name,c.bd_name) as div_name,
	coalesce(d.department_id,c.dept_code) as dept_id,
	coalesce(d.department_name,c.dept_name) as dept_name,
    c.category_big_code as category_big_code,
    c.category_big_name as category_big_name,
    c.category_middle_code as category_middle_code,
    c.category_middle_name as category_middle_name,
    c.category_small_code as category_small_code,
    c.category_small_name as category_small_name,
    
    c.brand_name as unit_name,
    a.unit as unit,

    a.after_price as  avg_price,
    a.after_qty as stock_qty,
    a.after_amt as stock_amt,
    split(b.biz_time,'\\.')[0] as last_in_datetime,
    b.txn_qty as last_in_qty,
	if(e.goods_code is not null, '是', '否') as is_factory_goods,
	if(e.goods_code is not null, 1, 0) as is_factory_code,	
    from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as update_time,
    regexp_replace(to_date(b.biz_time),'-','') as sdt
FROM
(
  	SELECT 
  		a.location_code,
        product_code,
        a.reservoir_area_code,
        a.reservoir_area_name,
        after_price,
        after_qty,
        after_amt,
		edate
   FROM 
   		csx_ods.wms_h_stock_temp02 AS a
   UNION ALL
   SELECT 
   		b.shop_id AS location_code,
        b.goodsid AS product_code,
        b.inv_place AS reservoir_area_code,
        '' AS reservoir_area_name,
        b.cycle_unit_price AS after_price,
        b.inv_qty AS after_qty,
        b.inv_amt AS after_amt,
		sdt
   FROM 
   		csx_dw.inv_sap_setl_dly_fct b
   WHERE 
		b.sales_dist NOT IN ('612000','613000')
		and sdt>='20191101'
)AS a
LEFT JOIN
(
	SELECT 
		a.shop_code,
    	a.shop_name,
    	a.product_code AS goods_code,
    	a.product_name AS goods_name,
    	product_bar_code AS goods_bar_code,
    	case when root_category_code in ('10','11') then '10'  when root_category_code in ('12','13','14') then '11' else '15' end bd_id,
    	case when root_category_code in ('10','11') then '生鲜供应链'  when root_category_code in ('12','13','14') then '食百供应链' else '易耗品' end bd_name,
    	root_category_code AS category_code,
    	root_category_name AS category_name,
    	big_category_code AS category_big_code,
    	big_category_name AS category_big_name,
    	middle_category_code AS category_middle_code,
    	middle_category_name AS category_middle_name,
    	small_category_code AS category_small_code,
    	small_category_name AS category_small_name,
    	purchase_group_code AS dept_code,
    	purchase_group_name AS dept_name,
    	brand_name,
    	unit,
    	spec,
    	manufacturer, --生产厂商
 		CASE WHEN delivery_type='1' THEN '整件'
    	 	 WHEN delivery_type='2' THEN '小包装'
    	 	 WHEN delivery_type='3' THEN '散装'
    		 ELSE delivery_type
 			 END delivery_type , --配送方式
 		CASE WHEN business_type='0' THEN '自营'
    	 	 WHEN business_type='1' THEN '联营'
    	     ELSE business_type
 			 END business_type, --经营方式
 		product_status_name, --商品状态名称
 		supplier_code AS vendor_code,
 		supplier_name AS vendor_name,
 		logistics_mode_name, --物流模式名称
 		valid_tag_name, --有效标识名称
 		CASE WHEN sales_return_tag='0' THEN '不可退'
    	 	 WHEN sales_return_tag='1' THEN '可退'
    	 	 ELSE sales_return_tag
 			 END sales_return_tag , --退货标识
 		location_name --地点类型名称
	FROM 
		csx_ods.csx_product_info AS a
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','') 
) c ON a.location_code = c.shop_code
	AND a.product_code = c.goods_code
LEFT JOIN 
	csx_ods.wms_h_entry_temp05 AS b 
	ON a.location_code = b.location_code
	AND a.product_code = b.product_code
	AND a.reservoir_area_code = b.reservoir_area_code
left join
	csx_dw.goods_m as d 
on  d.sdt=regexp_replace(date_sub(current_date,1),'-','') and a.product_code=d.goods_id
left join
(
	select distinct 
		goods_code 
	from 
		csx_dw.factory_bom 
  	where 
  		sdt = 'current'   
) as e on  a.product_code =c.goods_code
;




--------------------------------------------------------------------------------------上面为导入数据、

--库存信息每日维度详情
drop table csx_ods.wms_h_stock_temp04;
create temporary table csx_ods.wms_h_stock_temp04
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
	csx_ods.wms_h_stock_temp02 as a

left join
(
	select distinct 
		goods_code 
	from 
		csx_dw.factory_bom 
  	where 
  		sdt = 'current'   
) as c on  a.product_code =c.goods_code;

--最终数据插入到 hive 到mysql的表中


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
	split(b.biz_time,'\\.')[0] as last_in_datetime,
	b.txn_qty as last_in_qty,
	a.is_factory_goods as is_factory_goods,
	a.is_factory_code as is_factory_code,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as update_time,
	a.edate as sdt
from 
	csx_ods.wms_h_stock_temp04 as a 
join
	csx_ods.wms_h_entry_temp05  as  b  

on 
	a.location_code=b.location_code 
	and 
	a.reservoir_area_code=b.reservoir_area_code
	and 
	a.product_code=b.product_code
	;

	