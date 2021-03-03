create table csx_dw.dc_sale_inventory_old LIKE csx_dw.dc_sale_inventory;
set mapreduce.job.queuename                 =caishixian;
set mapreduce.job.reduces                   =80;
set hive.map.aggr                           =true;
set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错


insert
	overwrite table csx_dw.dc_sale_inventory partition(sdt)
select
	saledate,
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
	bd_id,
	bd_name,
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
	cast (sales_sales_cost as decimal(26,6)) as sales_sales_cost,
	cast (sales_qty as decimal(26,6)) as sales_qty,
	cast (sales_value as decimal(26,6)) as sales_value,
	cast (profit as decimal(26,6)) as profit,
	cast (inventory_qty as decimal(26,6)) as inventory_qty,
	cast (inventory_amt as decimal(26,6)) as inventory_amt,
	vendor_code,
	vendor_name,
	logistics_mode,
	logistics_mode_name
from csx_dw.dc_sale_inventory_old
where sdt>='20200101';

select * from csx_dw.dc_sale_inventory_old;
show create table csx_dw.dc_sale_inventory;

drop TABLE csx_dw.dc_sale_inventory;

CREATE TABLE csx_dw.dc_sale_inventory (
  years string comment'年份',
  months string   comment '月份',
  saledate STRING COMMENT '销售日期',
  province_code STRING COMMENT '省区编码',
  province_name STRING COMMENT '省区名称',
  dc_code STRING COMMENT 'DC编码',
  dc_name STRING COMMENT 'DC名称',
  goods_code STRING COMMENT '商品编码',
  goods_bar_code STRING COMMENT '条码',
  goods_name STRING COMMENT '商品名称',
  spec STRING COMMENT '规格',
  unit_name STRING COMMENT '单位',
  brand_name STRING COMMENT '品牌',
  bd_id STRING COMMENT '采购部编码',
  bd_name STRING COMMENT '采购部名称',
  dept_id STRING COMMENT '课组编码',
  dept_name STRING COMMENT '课组名称',
  div_id STRING COMMENT '部类编码',
  div_name STRING COMMENT '部类名称',
  category_large_code STRING COMMENT '大类编码',
  category_large_name STRING COMMENT '大类名称',
  category_middle_code STRING COMMENT '中类编码',
  category_middle_name STRING COMMENT '中类名称',
  category_small_code STRING COMMENT '小类编码',
  category_small_name STRING COMMENT '小类名称',
  valid_tag STRING COMMENT '有效标识',
  valid_tag_name STRING COMMENT '有效标识名称',
  goods_status_id STRING COMMENT '商品状态编码',
  goods_status_name STRING COMMENT '商品状态名称',
  sales_sales_cost DECIMAL(26,6) COMMENT '销售成本',
  sales_qty DECIMAL(26,6) COMMENT '销售数量',
  sales_value DECIMAL(26,6) COMMENT '销售金额',
  profit DECIMAL(26,6) COMMENT '毛利',
  inventory_qty DECIMAL(26,6) COMMENT '结余库存量',
  inventory_amt DECIMAL(26,6) COMMENT '结余库存额',
  vendor_code STRING COMMENT '供应商号',
  vendor_name STRING COMMENT '供应商名称',
  logistics_mode STRING COMMENT '物流模式',
  logistics_mode_name STRING COMMENT ' 物流模式名称'
)COMMENT 'DC销售与结余库存'
PARTITIONED BY (
  sdt STRING COMMENT '日期分区'
)
STORED AS PARQUET
;
 
 
 set mapreduce.job.queuename                 =caishixian;
set mapreduce.job.reduces                   =80;
set hive.map.aggr                           =true;
set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错

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
		substr(sdt,1,6) >='202001'
		--and 	substr(sdt,1,6) <='201912'
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
			substr(sdt,1,6) >='202001'
		--and 	substr(sdt,1,6) <='201912'	
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

--set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite table  csx_dw.dc_sale_inventory PARTITION(sdt )
select 
SUBSTRING(sdt,1,4) as years,
SUBSTRING(sdt,1,6) as months,
sdt as saledate,
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
from csx_ods.csx_product_info cpi where sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')) b 
on regexp_replace(a.dc_code,'^E','9')=regexp_replace(b.shop_code,'^E','9') and a.goods_code=b.product_code
left join 
(select sb.province_code,sb.province_name,sb.shop_id from csx_dw.shop_m sb where sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')) c 
on regexp_replace(a.dc_code,'^E','9')=regexp_replace(c.shop_id,'^E','9')

;


select
	sdt as sdate ,
	if(SUM(sale)-round(SUM(sale1+inventory_amt),0)!= 0,'Y','N') as note
from
	( 
select substr(sdt,1,6) sdt,round(sum(sales_value+inventory_amt),0) as sale,0 sale1,0 inventory_amt from csx_dw.dc_sale_inventory 
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
order by sdt;

select SUM(sales_vlaue) 
from  csx_dw.dc_sale_inventory where sdt<='20200101'and sdt>='20191231' ;


select
	sdt as sdate ,
	sum(sale0)sale0,sum(inv0)inv0 ,sum(sale1)sale1,sum(inventory_amt)inventory_amt ,
	if(SUM(sale0)-round(SUM(sale1+inventory_amt),0)!= 0,'Y','N') as note
from
	( 
select substr(sdt,1,6) sdt,sum(sales_value)sale0,sum(inventory_amt)inv0,0 sale1,0 inventory_amt from csx_dw.dc_sale_inventory 
where sdt>='20190101' and sdt<regexp_replace(to_date(current_timestamp()),'-','')
group by substr(sdt,1,6) 
UNION ALL
select substr(sdt,1,6) sdt,0 sale0,0 inv0,SUM(sales_value)sale1 ,0 inventory_amt   from csx_dw.sale_goods_m1 
where sdt>='20190101' and sdt<regexp_replace(to_date(current_timestamp()),'-','')
group by substr(sdt,1,6) 
union all 
select substr(sdt,1,6) sdt,0 sale0,0 inv0,0 sale1 ,SUM(amt) inventory_amt from csx_dw.wms_accounting_stock_m  as a 
where sdt>='20190101' and sdt<regexp_replace(to_date(current_timestamp()),'-','')
 and a.reservoir_area_code not in ('B999','B997','PD01','PD02','TS01')
 group by substr(sdt,1,6) 
  ) a
group by
	sdt
order by sdt;

