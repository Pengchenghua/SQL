
--hive
set SDATE='2019-01-01';
SET EDATE='2019-06-01';
drop table if EXISTS csx_dw.temp_inv_sale;
 create TEMPORARY table if NOT EXISTS csx_dw.temp_inv_sale as
select
	sdt,
	dc_code,
	goods_code,
	sum(sales_qty) sales_qty,
	sum(sales_value) sales_value,
	sum(sales_sales_cost) sales_sales_cost,
	sum(profit) profit,
	sum(inventory_qty) inventory_qty,
	sum(inventory_amt) inventory_amt
from
	(
	SELECT
		sdt,
		shop_id dc_code,
		goods_code,
		sum(sales_qty)as sales_qty,
		sum(sales_value) as sales_value,
		sum(sales_sales_cost)as sales_sales_cost,
		sum(profit)as profit,
		0 inventory_qty,
		0 inventory_amt
	FROM
		csx_dw.sale_goods_m1
	where
		sdt >=regexp_replace(${hiveconf:sdate},'-','')
		and sdt < regexp_replace(${hiveconf:edate},'-','')
	group by
		sdt,
		shop_id ,
		goods_code
union all
	select
		sdt,
		dc_code,
		goods_code,
		0 sales_qty,
		0 sales_value,
		0 sales_sales_cost,
		0 profit,
		sum(qty) inventory_qty,
		sum(amt) inventory_amt
	from
		csx_dw.wms_accounting_stock_m
	where
			sdt >=regexp_replace(${hiveconf:sdate},'-','')
		and sdt < regexp_replace(${hiveconf:edate},'-','')
		and reservoir_area_code not in ('B999',
		'B997',
		'PD01',
		'PD02',
		'TS01')
	group by
		dc_code,
		goods_code,
		sdt ) a
group by
	dc_code,
	sdt,
	goods_code ;
set mapreduce.job.queuename                 =caishixian;
set mapreduce.job.reduces                   =80;
set hive.map.aggr                           =true;
set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错

--set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite table  csx_dw.dc_sale_inventory PARTITION(sdt )
select sdt as saledate,
province_code,
province_name,
dc_code,
dc_name,
goods_code,
goods_bar_code,
goods_name,
spec,
unit_name,
brand_name,
case when div_id in ('12','13','14') then '12' when div_id in ('10','11') then '11' else '15' end  bd_id,
case when div_id in ('12','13','14') then '食品用品' when div_id in ('10','11') then '生鲜部' else '易耗品' end  bd_name,
dept_id,
dept_name,
div_id,
div_name,
category_large_code,
category_large_name,
category_middle_code,
category_middle_name,
category_small_code,
category_small_name,
valid_tag,
valid_tag_name,
goods_status_id,
goods_status_name,
sales_sales_cost,
sales_qty,
sales_value,
profit,
inventory_qty,
inventory_amt,
vendor_code,
vendor_name,
logistics_mode,
logistics_mode_name,
sdt 
from csx_dw.temp_inv_sale a 
left join 
(select 
shop_code,
shop_name dc_name,
product_code,
cpi.product_name goods_name,
product_bar_code goods_bar_code,
cpi.spec,
cpi.unit unit_name,
cpi.brand_name,
root_category_code as div_id,
cpi.purchase_group_code as  dept_id,
cpi.purchase_group_name as  dept_name,
cpi.root_category_name as div_name,
cpi.big_category_code as category_large_code,
cpi.big_category_name as category_large_name,
middle_category_code as category_middle_code,
cpi.middle_category_name as category_middle_name,
small_category_code as category_small_code,
cpi.small_category_name  as category_small_name,
supplier_code as vendor_code,
supplier_name as vendor_name,
cpi.des_specific_product_status as goods_status_id,
cpi.product_status_name as goods_status_name,
cpi.valid_tag as valid_tag,
cpi.valid_tag_name ,
cpi.logistics_mode ,
cpi.logistics_mode_name
from csx_ods.csx_product_info cpi where sdt='20200109') b 
on regexp_replace(a.dc_code,'^E','9')=regexp_replace(b.shop_code,'^E','9') and a.goods_code=b.product_code
left join 
(select sb.province_code,sb.province_name,sb.shop_id from csx_dw.shop_m sb where sdt='20200109') c 
on regexp_replace(a.dc_code,'^E','9')=regexp_replace(c.shop_id,'^E','9')
--where sdt>='20190301'
--WHERE a.goods_code='1017222' and dc_code='E080'
;

 select sum(sales_value)sale,SUM(inventory_amt)inventory_amt from csx_dw.dc_sale_inventory
 union all 
 select SUM(sales_value)sale ,0 inventory_amt   from csx_dw.sale_goods_m1 where sdt>='20190101' and sdt<='20191231' 
 union all 
  select 0 sale ,SUM(amt) inventory_amt from csx_dw.wms_accounting_stock_m  as a where sdt>='20190101' and sdt<='20191231' and a.reservoir_area_code not in ('B999','B997','PD01','PD02','TS01')
;



select
	sdt as sdate ,
	if(SUM(sale)-round(SUM(sale1+inventory_amt),0)!= 0,'Y','N') as note
from
	( 
select sdt,round(sum(sales_value+inventory_amt),0)sale,0 sale1,0 inventory_amt from csx_dw.dc_sale_inventory 
where sdt>=regexp_replace(to_date(add_months(current_timestamp(),-3)),'-','') and sdt<regexp_replace(to_date(current_timestamp()),'-','')
group by sdt
UNION ALL
select sdt,0 sale,SUM(sales_value)sale1 ,0 inventory_amt   from csx_dw.sale_goods_m1 
where sdt>=regexp_replace(to_date(add_months(current_timestamp(),-3)),'-','') and sdt<regexp_replace(to_date(current_timestamp()),'-','')
group by sdt
union all 
select sdt,0 sale,0 sale1 ,SUM(amt) inventory_amt from csx_dw.wms_accounting_stock_m  as a 
where sdt>=regexp_replace(to_date(add_months(current_timestamp(),-3)),'-','') and sdt<regexp_replace(to_date(current_timestamp()),'-','')
 and a.reservoir_area_code not in ('B999','B997','PD01','PD02','TS01')
 group by sdt
  ) a
group by
	sdt
order by sdt
;
select
	sdt as sdate ,
	if(SUM(sale)-round(SUM(sale1+inventory_amt),0)!= 0,'Y','N') as note
from
	( 
select substr(sdt,1,6) sdt,round(sum(sales_value+inventory_amt),0)sale,0 sale1,0 inventory_amt from csx_dw.dc_sale_inventory 
where sdt>=regexp_replace(to_date(add_months(current_timestamp(),-3)),'-','') and sdt<regexp_replace(to_date(current_timestamp()),'-','')
group by substr(sdt,1,6) 
UNION ALL
select substr(sdt,1,6) sdt,0 sale,SUM(sales_value)sale1 ,0 inventory_amt   from csx_dw.sale_goods_m1 
where sdt>=regexp_replace(to_date(add_months(current_timestamp(),-3)),'-','') and sdt<regexp_replace(to_date(current_timestamp()),'-','')
group by substr(sdt,1,6) 
union all 
select substr(sdt,1,6) sdt,0 sale,0 sale1 ,SUM(amt) inventory_amt from csx_dw.wms_accounting_stock_m  as a 
where sdt>=regexp_replace(to_date(add_months(current_timestamp(),-3)),'-','') and sdt<regexp_replace(to_date(current_timestamp()),'-','')
 and a.reservoir_area_code not in ('B999','B997','PD01','PD02','TS01')
 group by substr(sdt,1,6) 
  ) a
group by
	sdt
order by sdt