--set hive.execution.engin=spark;
-- 当销售数量-拣货数量<0 时，取实际接收量
-- 20201026 增加客户配送销售，地采关联销售订单，仓配=销售-地采
-- 20200825 增加DC用途及更改dctype 工厂 、仓库、门店
-- 20200825 调整销售业务类型，将客户直送、客户配送、一件代发 剔除，入库剔除客退入库、客户直送 03、货到即配 54
-- set mapreduce.job.reduces =80;

-- 20200923 更改销售表名 dws_sale_r_d_sale_item_simple_20200921
set hive.execution.engine=tez;
set tez.queue.name=caishixian;
set hive.map.aggr         =true;
--set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
SET edate                                   = '${enddate}';
SET sdate                                   = trunc(${hiveconf:edate},'MM');


-- 创建销售与地采表关联关系
-- 地采单据 ：1、销售量-地采量<=0
-- 仓配量/额=销售量-地采量

drop table if exists csx_tmp.temp_p_sale_01 ;
create temporary table if not exists csx_tmp.temp_p_sale_01 as 
SELECT a.*,
       b.pick_qty,
       if(a.sales_qty-coalesce(pick_qty,0)<0,sales_qty,a.sales_qty-coalesce(pick_qty,0)) as warehouse_qty, --仓配数量
       a.cost_price*(if(a.sales_qty-coalesce(pick_qty,0)<0,sales_qty,a.sales_qty-coalesce(pick_qty,0))) as warehouse_cost ,   --仓配成本
       a.sales_price*(if(a.sales_qty-coalesce(pick_qty,0)<0,sales_qty,a.sales_qty-coalesce(pick_qty,0))) as warehouse_sale    --仓配额
FROM csx_dw.dws_sale_r_d_sale_item_simple_20200921 a
LEFT JOIN
  (SELECT sale_order_code,
          product_code,
          qty AS pick_qty
   FROM csx_ods.source_scm_r_a_scm_local_purchase_request --地采请求表
   WHERE sdt=regexp_replace(${hiveconf:edate},'-','')
     AND status =2
     AND qty !=0) b ON a.order_no =b.sale_order_code
AND goods_code =b.product_code
WHERE sdt>='20200101'
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
  -- and return_flag!='X'
  -- and dc_code ='W0A8'
  AND business_type_code NOT IN ('19','73', 'R19',  'R73')

;

-- 库存、期间库存、30天库存

DROP TABLE IF EXISTS csx_tmp.p_invt_1;

CREATE TEMPORARY TABLE IF NOT EXISTS csx_tmp.p_invt_1 AS
	-- 库存查询
select
	dc_code           ,
	goods_code goods_id,
	sum	(case when sdt>=regexp_replace(${hiveconf:sdate},'-'  ,	'')	then qty end)as	inv_qty,
	sum	(case when sdt>=regexp_replace(${hiveconf:sdate},'-'  ,	'')	then amt end)as	inv_amt,
	sum	(case when sdt> regexp_replace(date_sub(${hiveconf:edate},31),'-' ,'') then qty end)as inv_qty_30day,
	sum	(case when sdt> regexp_replace(date_sub(${hiveconf:edate},31),'-' ,'') then amt end)as inv_amt_30day,
	sum	(case when sdt = regexp_replace(${hiveconf:edate},'-'  ,'') then qty	end )as qm_qty,
	sum	(case when sdt = regexp_replace(${hiveconf:edate},'-' ,	'')	then amt end)as	qm_amt 
from
	csx_dw.dws_wms_r_d_accounting_stock_m
where
	sdt>regexp_replace(date_sub(${hiveconf:edate},31),'-','')
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


-- 剔除 19 客户直送、73一件代发 据号
-- 关联地采单据号 销量-拣货量=0 剔除
--最近出库日期 20200807
drop table if exists csx_tmp.p_sale_max	;	
create temporary table	if not exists csx_tmp.p_sale_max as
select
	a.dc_code as shop_id,
	goods_code            as goods_id,
	coalesce(max(a.sdt),'') as max_send_sdt
FROM
	csx_tmp.temp_p_sale_01 a 
where (warehouse_qty > 0   or warehouse_qty is  null)
GROUP BY
	a.dc_code,
	goods_code	;


		
--末次入库日期及数量
-- 入库剔除 客退入库 、客户直送 03、货到即配 54
drop table if exists csx_tmp.p_entry_max;

create temporary table if not exists csx_tmp.p_entry_max as
select
	a.receive_location_code                          ,
	a.goods_code                                     ,
	coalesce(sum(receive_qty) ,0)      as entry_qty  ,
	coalesce(sum(price*receive_qty),0) as entry_value,
	coalesce(sdt,'')                   as entry_sdt
from
	csx_dw.wms_entry_order a
join
	(select
		receive_location_code,
		goods_code           ,
		max(sdt) as max_sdt
	from
		csx_dw.wms_entry_order
	where
		sdt              >'20181231'
		and sdt<=regexp_replace(${hiveconf:edate},'-','')
		and receive_qty !=0
		and (entry_type  !='客退入库' and business_type_code not in ('03','54'))
	group by
		receive_location_code,
		goods_code
	)	as b
		on	a.receive_location_code=b.receive_location_code	and a.goods_code =b.goods_code	and a.sdt =b.max_sdt 
		and (entry_type  !='客退入库' and business_type_code not in ('03','54'))
group by
	a.receive_location_code ,
	a.goods_code            ,
	coalesce(sdt,'')
;
	


--- 计算销售数据剔除相关的单据号 单据类型:客户直送 19、 一件代发 73、地采 
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
	SUM(a.warehouse_qty) qty,
	sum(a.warehouse_cost) as sales_cost,
	SUM(a.warehouse_sale) sale,
	SUM(warehouse_sale-warehouse_cost) profit,
	0 qty_30day,
	0 sales_30day,
	0 sales_cost30day
FROM 	csx_tmp.temp_p_sale_01 a 
WHERE sdt >= regexp_replace(${hiveconf:sdate}, '-', '')
	AND sdt <= regexp_replace(${hiveconf:edate}, '-', '')
	and ((warehouse_qty > 0   or warehouse_qty is  null) or  a.return_flag='X')
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
	sum(warehouse_qty) as qty_30day,
	sum(warehouse_sale) as sales_30day,
	sum(warehouse_cost) as sales_cost30day
FROM csx_tmp.temp_p_sale_01 a 
WHERE sdt >   regexp_replace(date_sub(${hiveconf:edate}, 30), '-', '')
	AND sdt <= regexp_replace(${hiveconf:edate}, '-', '')
	and ((warehouse_qty > 0   or warehouse_qty is  null) or  a.return_flag='X')	
GROUP BY dc_code,
	goods_code
) a 
group by 
	dc_code,
	goods_id;

--关联库存与销售

DROP TABLE IF EXISTS csx_tmp.p_invt_2	;
CREATE TEMPORARY TABLE	IF NOT EXISTS csx_tmp.p_invt_2 AS
	SELECT
		substr(regexp_replace(${hiveconf:edate},'-' ,''),1,4) as years,
		substr(regexp_replace(${hiveconf:edate},'-' ,''),1,6) as months,
		b.prov_code                              ,
		b.prov_name                              ,
		dist_code                                ,
		dist_name                                ,
		a.dc_code as shop_id                     ,
		b.shop_name                              ,
		a.goods_id                                ,
		SUM(qty)            sales_qty             ,
		SUM(a.sale)         sales_value           ,
		SUM(profit)         profit                ,
		SUM(sales_cost)    AS sales_cost          ,
		SUM(inv_qty)       as period_inv_qty      ,
		SUM(inv_amt)       as period_inv_amt      ,
		SUM(inv_qty_30day)  as period_inv_qty_30day,
		SUM(inv_amt_30day)  as period_inv_amt_30day,
		SUM(qm_qty)        as final_qty           ,
		SUM(qm_amt)        as final_amt           ,
		-- COALESCE(case when sum(sales_cost)=0 and SUM(inv_amt)!=0	then 9999 else SUM(inv_amt)/ SUM(sale-profit)end,0) as days_turnover,
		COALESCE(SUM(sales_30day),0)  as sales_30day   , -- 30天销售额
		COALESCE(sum(qty_30day),0)  as qty_30day    ,   -- 30天销量
		coalesce(sum(sales_cost30day),0) as cost_30day   ,   -- 30天成本
		-- coalesce(sum(qty_30day)/30,0.01) as dms, --日均销量
		-- COALESCE(case when sum(qty_30day)=0 and SUM(qm_qty)!=0	then 9999 else SUM(qm_qty)/(sum(qty_30day)/30)end ,0)as inv_sales_days, --库存可销天数
		dc_type     ,
		dc_uses
FROM
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
	UNION ALL
	SELECT
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
		coalesce(a.qm_amt0) qm_amt
	FROM
		csx_tmp.p_invt_1 a)	a
JOIN
 (SELECT location_code shop_id,
		shop_name            ,
		dist_code            ,
		dist_name            ,
		CASE WHEN a.location_code = 'W0H4'
			THEN 'W0H4'
			ELSE a.province_code
		END prov_code,
		CASE WHEN a.location_code = 'W0H4'
			THEN '供应链平台'
			ELSE a.province_name
		END prov_name,
		a.purpose       as dc_uses  ,
		a.location_type as dc_type
	FROM
		csx_dw.csx_shop a
	WHERE
		sdt = 'current')b ON dc_code= b.shop_id
	GROUP BY
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
		COALESCE(case when (sales_cost)=0 and inv_amt!=0 then 9999 else inv_amt/sales_cost end,0)as days_turnover,
		cost_30day ,
		sales_30day   ,
		qty_30day    ,
		if(qty_30day/30=0,0.01,coalesce(qty_30day/30,0.01)) dms,
		final_qty/if(qty_30day/30=0,0.01,coalesce(qty_30day/30,0.01)) inv_sales_days ,
		period_inv_qty_30day                                                                                                                 ,
		period_inv_amt_30day                                                                                                                 ,
		COALESCE(case when (cost_30day=0 and period_inv_amt_30day!=0)	then 9999 else (period_inv_amt_30day)/ (cost_30day) 	end,0)	AS days_trunover_30,
		nvl(max_send_sdt,'')                                                                                                     max_send_sdt,
		coalesce(datediff(${hiveconf:edate},from_unixtime(unix_timestamp(max_send_sdt,'yyyyMMdd'),'yyyy-MM-dd')) ,0) as no_sale_days,
		coalesce(dc_type, '')                                                                                                 as dc_type     ,
		coalesce(entry_qty,0)                                                                                                 as entry_qty   ,
		coalesce(entry_value,0)                                                                                               as entry_value ,
		nvl(entry_sdt,'')                                                                                                     as entry_sdt   ,
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
			LEFT OUTER JOIN
				(
					SELECT
						shop_code                   AS shop_id          ,
						product_code                   goods_id          ,
						product_status_name         as goods_status_name,
						des_specific_product_status AS goods_status_id  ,
						valid_tag                                       ,
						valid_tag_name
					FROM
						csx_dw.dws_basic_w_a_csx_product_info
					WHERE
						sdt = 'current'
				)
				d
				ON
					a.shop_id     = d.shop_id
					AND a.goods_id = d.goods_id
			left JOIN
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
