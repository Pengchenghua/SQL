CREATE TABLE `csx_tmp.ads_wms_r_d_goods_turnover`(
  `years` string COMMENT '年份', 
  `months` string COMMENT '月份', 
  `province_code` string COMMENT '标准省区编码', 
  `province_name` string COMMENT '标准省区名称', 
  `dist_code` string COMMENT '省区编码简称', 
  `dist_name` string COMMENT '省区编码简称', 
  `city_code` string COMMENT '城市', 
  `city_name` string COMMENT '城市名称', 
  `dc_code` string COMMENT 'DC编码', 
  `dc_name` string COMMENT 'DC名称', 
  `goods_id` string COMMENT '商品编码', 
  `goods_name` string COMMENT '商品名称', 
  `standard` string COMMENT '规格', 
  `unit_name` string COMMENT '单位', 
  `brand_name` string COMMENT '品牌', 
  `dept_id` string COMMENT '课组编码', 
  `dept_name` string COMMENT '课组名称', 
  `business_division_code` string COMMENT '采购部编码', 
  `business_division_name` string COMMENT '采购部名称', 
  `division_code` string COMMENT '部类编码', 
  `division_name` string COMMENT '部类名称', 
  `category_large_code` string COMMENT '大类编码', 
  `category_large_name` string COMMENT '大类名称', 
  `category_middle_code` string COMMENT '中类编码', 
  `category_middle_name` string COMMENT '中类名称', 
  `category_small_code` string COMMENT '小类编码', 
  `category_small_name` string COMMENT '小类名称', 
  `valid_tag` string COMMENT '有效标识', 
  `valid_tag_name` string COMMENT '有效标识名称', 
  `goods_status_id` string COMMENT '商品状态编码', 
  `goods_status_name` string COMMENT '商品状态名称', 
  `sales_qty` decimal(38,6) COMMENT '月累计销售数量', 
  `sales_value` decimal(38,6) COMMENT '月累计销售额', 
  `profit` decimal(38,6) COMMENT '月累计毛利额', 
  `sales_cost` decimal(38,6) COMMENT '月累计销售成本', 
  `period_inv_qty` decimal(38,6) COMMENT '月累计库存量', 
  `period_inv_amt` decimal(38,6) COMMENT '月累计库存额', 
  `final_qty` decimal(38,6) COMMENT '期末库存量', 
  `final_amt` decimal(38,6) COMMENT '期末库存额', 
  `days_turnover` decimal(38,6) COMMENT '月周转天数', 
  `cost_30day` decimal(38,6) COMMENT '近30天成本', 
  `sales_30day` decimal(38,6) COMMENT '30天日均销售额', 
  `qty_30day` decimal(38,6) COMMENT '30天销售量', 
  `dms` decimal(38,6) COMMENT '30天日均销量', 
  `inv_sales_days` decimal(38,6) COMMENT '库存可销天数', 
  `period_inv_qty_30day` decimal(38,6) COMMENT '近30天累计库存量', 
  `period_inv_amt_30day` decimal(38,6) COMMENT '近30天累计库存额', 
  `days_turnover_30` decimal(38,6) COMMENT '近30天周转', 
  `max_sale_sdt` string COMMENT '最近一次销售日期', 
  `no_sale_days` int COMMENT '未销售天数', 
  `dc_type` string COMMENT 'DC类型', 
  `entry_qty` decimal(38,6) COMMENT '最近入库数量', 
  `entry_value` decimal(38,6) COMMENT '最近入库额', 
  `entry_sdt` string COMMENT '最近入库日期', 
  `entry_days` int COMMENT '最近入库日期天数', 
  `dc_uses` string COMMENT 'DC用途', 
  receipt_amt DECIMAL(38,6) comment '领用金额',            --领用金额
  receipt_qty  DECIMAL(38,6) comment '领用数量',            --领用数量
  material_take_amt  DECIMAL(38,6) comment '原料使用金额',      --原料使用金额
  material_take_qty  DECIMAL(38,6) comment '原料使用数量',      --原料使用数量
  frozen_days_turnover_30  DECIMAL(38,6) comment '近30天周转天数',
  joint_purchase_flag int COMMENT '联采标识 0 否 1 是',
  `update_time` timestamp COMMENT '更新日期'
  )
COMMENT '物流库存周转剔除直送、一件代发业务'
PARTITIONED BY ( 
  `sdt` string COMMENT '日期分区')
   STORED AS parquet 
;
 

--set hive.execution.engin=spark;
-- 20201026 增加配送销售，地采关联销售订单，仓配=销售-地采、
-- 20200825 增加DC用途及更改dctype 工厂 、仓库、门店
-- 20200825 调整销售业务类型，将直送、配送、一件代发 剔除，入库剔除客退入库、直送 03、货到即配 54
-- set mapreduce.job.reduces =80;

-- 20200923 更改销售表名 dws_sale_r_d_sale_item_simple_20200921
set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
set hive.map.aggr         =true;
--set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
SET edate                                   = '${enddate}';
SET sdate                                   = trunc(${hiveconf:edate},'MM');


--  创建销售与地采表关联关系
--  地采单据 ：1、销售量-地采量<=0
--  仓配量/额=销售量-地采量


-- 库存、期间库存、30天库存

DROP TABLE IF EXISTS csx_tmp.p_invt_1;

CREATE TEMPORARY TABLE  csx_tmp.p_invt_1 AS
	-- 库存查询
select
	dc_code           ,
	goods_code goods_id,
	sum	(case when sdt>=regexp_replace(${hiveconf:sdate},'-'  ,	'')	and sdt<=regexp_replace(${hiveconf:edate},'-','') then qty end)as	inv_qty,
	sum	(case when sdt>=regexp_replace(${hiveconf:sdate},'-'  ,	'')	and sdt<=regexp_replace(${hiveconf:edate},'-','') then amt end)as	inv_amt,
	sum	(case when sdt> regexp_replace(date_sub(${hiveconf:edate},30),'-' ,'') and sdt<=regexp_replace(${hiveconf:edate},'-','') then qty end)as inv_qty_30day,
	sum	(case when sdt> regexp_replace(date_sub(${hiveconf:edate},30),'-' ,'') and sdt<=regexp_replace(${hiveconf:edate},'-','') then amt end)as inv_amt_30day,
	sum	(case when sdt = regexp_replace(${hiveconf:edate},'-'  ,'') then qty	end )as qm_qty,
	sum	(case when sdt = regexp_replace(${hiveconf:edate},'-' ,	'')	then amt end)as	qm_amt 
from
	csx_dw.dws_wms_r_d_accounting_stock_m
where
	sdt>= regexp_replace(date_sub(${hiveconf:edate},31),'-','')
	and sdt<=regexp_replace(${hiveconf:edate},'-','')
	and reservoir_area_code not in ('B999',
									'B997',
									'PD01',
									'PD02',
									'TS01')
group by
	dc_code,
	goods_code
;


-- 剔除 19 直送、73一件代发 据号
-- 关联地采单据号 销量-拣货量=0 剔除
-- 最近出库日期 20200807
-- 销售类型剔除返利
drop table if exists csx_tmp.p_sale_max	;	
create temporary table	csx_tmp.p_sale_max as
select
	dc_code as shop_id,
	goods_code as goods_id,
	coalesce(max(case when warehouse_sales_qty!=0   then  a.sdt end ),'') as max_send_sdt
FROM
	csx_tmp.ads_wms_r_d_warehouse_sales a 
where 1=1
	and business_type_code NOT IN ('19','73', 'R19', 'R73')
	and sdt>='20190101'
    and sdt<=regexp_replace(${hiveconf:edate},'-','')
	and sales_type !='fanli'  
GROUP BY
	dc_code,
	goods_code	;


		
--末次入库日期及数量
-- 入库剔除 客退入库 、直送 03、货到即配 54
drop table if exists csx_tmp.p_entry_max;

create temporary table if not exists csx_tmp.p_entry_max as
select
	a.receive_location_code                          ,
	a.goods_code                                     ,
	coalesce(sum(receive_qty) ,0)      as entry_qty  ,
	coalesce(sum(price*receive_qty),0) as entry_value,
	coalesce(sdt,'')                   as entry_sdt
from
	csx_dw.dws_wms_r_d_entry_detail a
join
	(select
		receive_location_code,
		goods_code           ,
		max(sdt) as max_sdt
	from
		csx_dw.dws_wms_r_d_entry_detail
	where
		sdt              >'20181231'
		and sdt<=regexp_replace(${hiveconf:edate},'-','')
		and receive_qty !=0
		and ((order_type_code not in ('S01','S02','S03','S04','S05','S06','S07','S08','S09','S10','RS01','S13','RS02','S15','S11','RS03','RS04') and return_flag='Y') 
		    or business_type not in ('03','54'))
	group by
		receive_location_code,
		goods_code
	)	as b
		on	a.receive_location_code=b.receive_location_code	and a.goods_code =b.goods_code	and a.sdt =b.max_sdt 
		where ((order_type_code not in ('S01','S02','S03','S04','S05','S06','S07','S08','S09','S10','RS01','S13','RS02','S15','S11','RS03','RS04') and return_flag='Y') 
		    or business_type not in ('03','54'))
group by
	a.receive_location_code ,
	a.goods_code            ,
	coalesce(sdt,'')
;
	


-- 计算销售数据剔除相关的单据号 单据类型:直送 19、 一件代发 73、地采 
drop TABLE if exists csx_tmp.p_sales_data ;
create TEMPORARY TABLE csx_tmp.p_sales_data as 
select dc_code,
	goods_id,
	SUM(qty) qty,
	sum(sales_cost) as sales_cost,
	SUM(sale) sale,
	SUM(profit) profit,
	sum(qty_30day) as qty_30day,
	sum(sales_30day) as sales_30day,
	sum(sales_cost30day) as sales_cost30day
from (
SELECT dc_code,
	goods_code goods_id,
	SUM(a.warehouse_sales_qty) qty,
	sum(a.warehouse_sales_cost) as sales_cost,
	SUM(a.warehouse_sales_value) sale,
	SUM(a.warehouse_sales_value-warehouse_sales_cost) profit,
	0 qty_30day,
	0 sales_30day,
	0 sales_cost30day
FROM 	csx_tmp.ads_wms_r_d_warehouse_sales a 
WHERE sdt >= regexp_replace(${hiveconf:sdate}, '-', '')
	AND sdt <= regexp_replace(${hiveconf:edate}, '-', '')
	and a.business_type_code not in  ('19','73', 'R19',  'R73')
	-- and ((warehouse_sales_qty > 0   or warehouse_sales_qty is  null) or  a.return_flag='X')
GROUP BY dc_code,
	goods_code,
	origin_order_no
UNION ALL
SELECT dc_code,
	goods_code goods_id,
	0 qty,
	0 sales_cost,
	0 sale,
	0 profit,
	sum(warehouse_sales_qty) as qty_30day,
	sum(a.warehouse_sales_value) as sales_30day,
	sum(a.warehouse_sales_cost) as sales_cost30day
FROM csx_tmp.ads_wms_r_d_warehouse_sales a 
WHERE sdt >  regexp_replace(date_sub(${hiveconf:edate}, 30), '-', '')
	AND sdt <= regexp_replace(${hiveconf:edate}, '-', '')
	and a.business_type_code NOT IN ('19','73', 'R19',  'R73')
    --	and ((warehouse_sales_qty > 0   or warehouse_sales_qty is  null) or  a.return_flag='X')	
GROUP BY dc_code,
	goods_code
) a 
group by 
	dc_code,
	goods_id;

--关联库存与销售

drop table if exists csx_tmp.p_invt_2	;
create temporary table	if not exists csx_tmp.p_invt_2 as
	select
		substr(regexp_replace(${hiveconf:edate},'-' ,''),1,4) as years,
		substr(regexp_replace(${hiveconf:edate},'-' ,''),1,6) as months,
		b.prov_code                              ,
		b.prov_name                              ,
		dist_code                                ,
		dist_name                                ,
		a.dc_code as shop_id                     ,
		b.shop_name                              ,
		a.goods_id                                ,
		sum(qty)            sales_qty             ,
		sum(a.sale)         sales_value           ,
		sum(profit)         profit                ,
		sum(sales_cost)    as sales_cost          ,
		sum(inv_qty)       as period_inv_qty      ,
		sum(inv_amt)       as period_inv_amt      ,
		sum(inv_qty_30day)  as period_inv_qty_30day,
		sum(inv_amt_30day)  as period_inv_amt_30day,
		sum(qm_qty)        as final_qty           ,
		sum(qm_amt)        as final_amt           ,
		coalesce(sum(sales_30day),0)  as sales_30day   , -- 30天销售额
		coalesce(sum(qty_30day),0)  as qty_30day    ,   -- 30天销量
		coalesce(sum(sales_cost30day),0) as cost_30day   ,   -- 30天成本
		dc_type     ,
		dc_uses
from
	(select dc_code,
		goods_id,
		coalesce(qty,0) qty,
		coalesce(sales_cost,0) sales_cost,
		coalesce(sale,0) sale,
		coalesce(profit,0) profit,
		coalesce(qty_30day,0) qty_30day,
		coalesce(sales_30day,0) sales_30day,
		coalesce(sales_cost30day,0) sales_cost30day,
		0 as inv_qty,
		0 as inv_amt,
		0 as inv_qty_30day,
		0 as inv_amt_30day,
		0 as qm_qty,
		0 as qm_amt
	from csx_tmp.p_sales_data
	union all
	select
		a.dc_code        ,
		a.goods_id        ,
		0 qty            ,
		0 sales_cost     ,
		0 sale           ,
		0 profit         ,
		0 qty_30day      ,
		0 sales_30day    ,
		0 sales_cost30day,
		coalesce(a.inv_qty        ,0) inv_qty        ,
		coalesce(a.inv_amt        ,0) inv_amt        ,
		coalesce(a.inv_qty_30day  ,0) inv_qty_30day  ,
		coalesce(a.inv_amt_30day  ,0) inv_amt_30day  ,
		coalesce(a.qm_qty         ,0) qm_qty         ,
		coalesce(a.qm_amt,0) qm_amt
	from
		csx_tmp.p_invt_1 a)	a
join
 (select location_code shop_id,
		shop_name            ,
		dist_code            ,
		dist_name            ,
		case when a.location_code = 'W0H4'
			then 'W0H4'
		    else a.province_code
		end prov_code,
		case when a.location_code = 'W0H4'
			then '供应链平台'
			else a.province_name
		end prov_name,
		a.purpose       as dc_uses  ,
		a.location_type as dc_type
	from
		csx_dw.csx_shop a
	where
		sdt = 'current')b on dc_code= b.shop_id
	group by
		b.prov_code,
		b.prov_name,
		a.dc_code  ,
		b.shop_name,
		a.goods_id  ,
		dist_code  ,
		dist_name  ,
		dc_type    ,
		dc_uses
	;
	
	--select sum(sales_value) from   csx_dw.supply_turnover  where sdt='20200119';	

insert overwrite table csx_tmp.ads_wms_r_d_goods_turnover partition(sdt)
select
		substr(regexp_replace(${hiveconf:edate} ,'-' ,''),1,4) as years,
		substr(regexp_replace(${hiveconf:edate} ,'-'  ,''),1,6) as months  ,
		prov_code                                  ,
		prov_name                                  ,
		dist_code                                ,
		dist_name                                ,
		a.shop_id                                  ,
		shop_name                                  ,
		a.goods_id                                  ,
		goods_name                                 ,
		standard                                   ,
		c.unit_name                                ,
		brand_name                                 ,
		department_id     as dept_id                                ,
		department_name     as dept_name                             ,
		business_division_code,
		business_division_name,
		division_code        ,
        division_name        ,
        category_large_code  ,
        category_large_name  ,
        category_middle_code ,
        category_middle_name ,
        category_small_code  ,
        category_small_name  ,
        nvl(valid_tag,'')         valid_tag        ,
		nvl(valid_tag_name,'')    valid_tag_name   ,
		nvl(goods_status_id,'')   goods_status_id  ,
		nvl(goods_status_name,'') goods_status_name,
		sales_qty                                  ,
		sales_value                                ,
		profit                                     ,
		sales_cost                                 ,
		period_inv_qty                             ,
		period_inv_amt                             ,
		final_qty    ,
		final_amt    ,
		coalesce(case when sales_cost<=0 and period_inv_amt>0 then 9999 
		              when sales_cost<=0 and period_inv_amt<=0 then 0
					  when sales_cost>0 and period_inv_amt<0 then 0
		              else period_inv_amt/sales_cost 
		          end,0 ) as days_turnover,
		cost_30day ,
		sales_30day   ,
		qty_30day    ,
		if(qty_30day/30<=0,0.01,coalesce(qty_30day/30,0.01)) as dms,
		final_qty/if(qty_30day/30<=0,0.01,coalesce(qty_30day/30,0.01)) as inv_sales_days ,
		period_inv_qty_30day ,
		period_inv_amt_30day ,
		coalesce(case when (cost_30day<=0 and period_inv_amt_30day>0) then 9999 
					 when cost_30day<=0 and period_inv_amt_30day<=0 then 0
					 when cost_30day>0 and period_inv_amt_30day<0 then 0
		              else period_inv_amt_30day/cost_30day 
		        end,0)	as days_trunover_30,
		nvl(max_send_sdt,'')  max_send_sdt,
		coalesce(datediff(${hiveconf:edate},from_unixtime(unix_timestamp(max_send_sdt,'yyyyMMdd'),'yyyy-MM-dd')) ,0) as no_sale_days,
		coalesce(dc_type, '') as dc_type     ,
		coalesce(entry_qty,0) as entry_qty   ,
		coalesce(entry_value,0)as entry_value ,
		nvl(entry_sdt,'')      as entry_sdt   ,
		coalesce(datediff(${hiveconf:edate},from_unixtime(unix_timestamp(entry_sdt,'yyyyMMdd'),'yyyy-MM-dd')) ,0)    as entry_days  ,
		nvl(dc_uses,'') as dc_uses     ,
		current_timestamp(),
		regexp_replace(${hiveconf:edate},'-' ,'')   sdt
	from
			csx_tmp.p_invt_2 a
			left join
				csx_tmp.p_sale_max b
				on
					a.shop_id    =b.shop_id
					and a.goods_id=b.goods_id
			left join
				csx_tmp.p_entry_max j
				on
					a.shop_id    =j.receive_location_code
					and a.goods_id=j.goods_code
			LEFT JOIN
				(
					SELECT
						shop_code                   as shop_id          ,
						product_code                as goods_id          ,
						product_status_name         as goods_status_name,
						des_specific_product_status as goods_status_id  ,
						valid_tag                                       ,
						valid_tag_name,
						joint_purchase_flag
					FROM
						csx_dw.dws_basic_w_a_csx_product_info
					WHERE
						sdt = 'current'
				)
				d
				ON
					a.shop_id     = d.shop_id
					and a.goods_id = d.goods_id
			left join
				(
					select
						goods_id                 ,
						goods_name               ,
						standard                 ,
						unit_name                ,
						brand_name               ,
						department_id   ,
						department_name ,
						case when division_code in ('12', '13',  '14')	then '12'
							 when division_code in ('10','11') then '11'
							 else division_code
						end business_division_code,
						case
							when division_code in ('12',
												   '13',
												   '14')
								then '食品用品采购部'
							when division_code in ('10',
												   '11')
								then '生鲜采购部'
								else division_name
						end                  as business_division_name    ,
						division_code        ,
						division_name        ,
						category_large_code  ,
						category_large_name  ,
						category_middle_code ,
						category_middle_name ,
						category_small_code  ,
						category_small_name  
					from
						csx_dw.dws_basic_w_a_csx_product_m
					where
						sdt='current'
				)
				c
				ON
					a.goods_id = c.goods_id
		;




 set edt='2021-07-15';

drop table if exists csx_tmp.temp_turn_goods;
create temporary table csx_tmp.temp_turn_goods as 
SELECT
    dist_code,
    dist_name,
    province_code     ,
    province_name     ,
    a.dc_code       ,
    dc_name     ,
    a.goods_id    ,
    goods_name    ,
    standard      ,
    unit_name     ,
    brand_name    ,
    dept_id       ,
    dept_name     ,
    business_division_code,
    business_division_name,
    division_code        ,
    division_name        ,
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    a.category_small_code  ,
    category_small_name  ,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    valid_tag      ,
    valid_tag_name ,
    goods_status_id,
    goods_status_name,
    sales_qty      ,
    sales_value    ,
    profit         ,
    profit/sales_value as profit_rate,
    sales_cost     ,
    period_inv_qty ,
    period_inv_amt ,
    final_qty      ,
    final_amt      ,
    days_turnover  ,
    sales_30day     ,
    qty_30day      ,
    days_turnover_30 ,
    cost_30day ,
    dms,
    period_inv_amt_30day ,
    inv_sales_days,
    max_sale_sdt,
    no_sale_days,
    entry_qty,
    entry_value,
    entry_sdt,
    entry_days,
    receipt_amt,            --领用金额
    receipt_qty,            --领用数量
    material_take_amt,      --原料使用金额
    material_take_qty,      --原料使用数量
    case when (coalesce(a.sales_30day,0)+coalesce(material_take_amt,0)+coalesce(receipt_amt,0))<=0 and period_inv_amt_30day>0 then 9999 
        else coalesce(period_inv_amt_30day/(coalesce(a.sales_30day,0)+coalesce(material_take_amt,0)+coalesce(receipt_amt,0)),0) 
    end as frozen_days_turnover_30,
    joint_purchase_flag
FROM
   csx_tmp.ads_wms_r_d_goods_turnover a 
join 
(select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current' 
	) as m on a.category_small_code =m.category_small_code 
LEFT	join 
	(
select location_code as dc_code,
    product_code as goods_code,
    sum(case when move_type = '118A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '118B' then amt_no_tax*(1+tax_rate/100 )*-1  end) receipt_amt,
    sum(case when move_type = '118A' then txn_qty  when  move_type = '118B' then txn_qty*-1 end) receipt_qty,
    sum(case when move_type = '119A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '119B' then amt_no_tax*(1+tax_rate/100 )*-1  end) material_take_amt,
    sum(case when move_type = '119A' then txn_qty  when  move_type = '119B' then txn_qty*-1 end) material_take_qty
from csx_dw.dwd_cas_r_d_accounting_stock_detail a
where sdt>regexp_replace(to_date(date_add(${hiveconf:edt},-30)),'-','')  and sdt<=regexp_replace(to_date(${hiveconf:edt}),'-','') 
    group by location_code,
    product_code
) c on a.goods_id=c.goods_code and a.dc_code=c.dc_code
left join 
(select shop_code,shop_id,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' ) d on a.dc_code=d.shop_code and a.goods_id=d.product_code
WHERE
    sdt=regexp_replace(to_date(${hiveconf:edt}),'-','') 
and a.business_division_code='11'

;

SHOW CREATE TABLE csx_tmp.ads_wms_r_d_goods_turnover;