-- 202000415 增加DC用途及更改dctype 工厂 、仓库、门店
-- set mapreduce.job.queuename                 =caishixian;
-- set mapreduce.job.reduces =80;
set hive.map.aggr         =true;
--set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
SET edate                                   = date_sub(current_date(),1);
SET sdate                                   = trunc(date_sub(current_date(),1),'MM');

DROP TABLE IF EXISTS csx_tmp.p_invt_1;

CREATE TEMPORARY TABLE IF NOT EXISTS csx_tmp.p_invt_1 AS
-- 库存查询
select
	dc_code,
	goods_code goodsid,
	sum(case when sdt>=regexp_replace (${hiveconf:sdate}, '-' , '')then qty end)as inv_qty,
	sum(case when sdt>=regexp_replace (${hiveconf:sdate}, '-' , '')	then amt end)as inv_amt,
	sum(case when sdt> regexp_replace (date_sub(${hiveconf:edate},31), '-' , '')then qty end)as inv_qty_30day,
	sum(case when sdt> regexp_replace (date_sub(${hiveconf:edate},31), '-' , '')then amt end)as	inv_amt_30day,
	sum(case when sdt=regexp_replace (${hiveconf:edate}, '-' , '')then qty	end)as qm_qty,
	sum(case when sdt=regexp_replace (${hiveconf:edate}, '-' , '')then amt	end)as qm_amt
from
	csx_dw.dws_wms_r_d_accounting_stock_m
where
	sdt     >regexp_replace (date_sub(${hiveconf:edate},31), '-' , '')
	and sdt<=regexp_replace (${hiveconf:edate}, '-' , '')
	and reservoir_area_code not in ('B999','B997','PD01','PD02','TS01')
group by
	dc_code,
	goods_code
;

--最近销售日期
drop table if exists csx_tmp.p_sale_max
;

create temporary table if not exists csx_tmp.p_sale_max as
SELECT
	dc_code               as shop_id,
	goods_code            as goodsid,
	coalesce(max(sdt),'') as max_sale_sdt
FROM
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt>='20190101'
GROUP BY
	dc_code,
	goods_code
;

--末次入库日期及数量
drop table if exists csx_tmp.p_entry_max
;

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
		(
			SELECT
				receive_location_code,
				goods_code           ,
				MAX(sdt) as max_sdt
			from
				csx_dw.wms_entry_order
			where
				sdt              >'20181231'
				and receive_qty !=0
				and entry_type  !='客退入库'
			group by
				receive_location_code,
				goods_code
		)
		as b
		on
			a.receive_location_code=b.receive_location_code
			and a.goods_code       =b.goods_code
			and a.sdt              =b.max_sdt
group by
	a.receive_location_code ,
	a.goods_code            ,
	coalesce(sdt,'')
;

--select * from temp.p_invt_1 a where  shop_id='W0A2';
--关联库存与销售

DROP TABLE IF EXISTS csx_tmp.p_invt_2;

CREATE TEMPORARY TABLE IF NOT EXISTS csx_tmp.p_invt_2 AS
SELECT
	substr(regexp_replace (${hiveconf:edate} , '-' , ''),1,4) as years  ,
	substr(regexp_replace (${hiveconf:edate} , '-' , ''),1,6) as months ,
	b.prov_code ,
	b.prov_name ,
	dist_code ,
	dist_name ,
	a.dc_code as shop_id,
	b.shop_name ,
	a.goodsid ,
	SUM(qty)        as   sales_qty            ,
	SUM(a.sale)      as  sales_value          ,
	SUM(profit)      as  profit               ,
	SUM(sales_cost)   AS sales_cost           ,
	SUM(inv_qty)      as period_inv_qty       ,
	SUM(inv_amt)      as period_inv_amt       ,
	SUM(inv_qty_30day)as period_inv_qty_30day ,
	SUM(inv_amt_30day)as period_inv_amt_30day ,
	SUM(qm_qty)       as final_qty            ,
	SUM(qm_amt)       as final_amt            ,
	COALESCE(case when sum(sales_cost)=0 then 999	else SUM(inv_amt)/ SUM(sale-profit)	end,0)AS days_turnover,
	COALESCE(SUM(sales_30day)/30,0)  as sale_30day   ,
	COALESCE(sum(qty_30day)  /30,0)  as qty_30day    ,
	coalesce(sum(sales_cost30day),0) as cost_30day   ,
	COALESCE(case when sum(qty_30day)=0 then 999 else SUM(qm_qty)/sum(qty_30day)end ,0)as days_sale, --日均销量
	dc_type     ,
	dc_uses
FROM
	(
		SELECT
			dc_code                            ,
			goods_code       as  goodsid         ,
			SUM(sales_qty)   as qty             ,
			sum(sales_cost)  as sales_cost     ,
			SUM(sales_value) as  sale           ,
			SUM(profit)      as  profit         ,
			0                as qty_30day      ,
			0                as sales_30day    ,
			0                as sales_cost30day,
			0                as inv_qty        ,
			0                as inv_amt        ,
			0                as inv_qty_30day  ,
			0                as inv_amt_30day  ,
			0                as qm_qty         ,
			0                as qm_amt
		FROM
			csx_dw.dws_sale_r_d_customer_sale
		WHERE
			sdt     >= regexp_replace (${hiveconf:sdate}, '-' , '')
			AND sdt <= regexp_replace (${hiveconf:edate}, '-' , '')
		GROUP BY
			dc_code,
			goods_code
		UNION ALL
		SELECT
			dc_code                           ,
			goods_code    as goodsid         ,
			0             as qty             ,
			0             as sales_cost      ,
			0             as sale            ,
			0             as profit          ,
			sum(sales_qty)  as qty_30day      ,
			sum(sales_value)as sales_30day    ,
			sum(sales_cost) as sales_cost30day,
			0               as inv_qty        ,
			0               as inv_amt        ,
			0               as inv_qty_30day  ,
			0               as inv_amt_30day  ,
			0               as qm_qty         ,
			0               as qm_amt
		FROM
			csx_dw.dws_sale_r_d_customer_sale
		WHERE
			sdt      >regexp_replace (date_sub(${hiveconf:edate},30), '-' , '')
			AND sdt <= regexp_replace (${hiveconf:edate}, '-' , '')
		GROUP BY
			dc_code,
			goods_code
		UNION ALL
		SELECT
			a.dc_code        ,
			a.goodsid        ,
			0 as qty            ,
			0 as sales_cost     ,
			0 as sale           ,
			0 as profit         ,
			0 as qty_30day      ,
			0 as sales_30day    ,
			0 as sales_cost30day,
			a.inv_qty        ,
			a.inv_amt        ,
			a.inv_qty_30day  ,
			a.inv_amt_30day  ,
			a.qm_qty         ,
			a.qm_amt
		FROM
			csx_tmp.p_invt_1 a
	)
	a
	JOIN
		(
			SELECT
				location_code shop_id,
				shop_name            ,
				dist_code            ,
				dist_name            ,
				CASE WHEN a.location_code = 'W0H4'	THEN 'W0H4'	ELSE a.province_code END prov_code,
				CASE
					WHEN a.location_code = 'W0H4'
						THEN '供应链平台'
						ELSE a.province_name
				END          as   prov_name,
				a.purpose       as dc_uses  ,
				a.location_type as dc_type
			FROM
				csx_dw.csx_shop a
			WHERE
				sdt = 'current'
		)
		b
		ON
			dc_code= b.shop_id
GROUP BY
	b.prov_code,
	b.prov_name,
	a.dc_code  ,
	b.shop_name,
	a.goodsid  ,
	dist_code  ,
	dist_name  ,
	dc_type    ,
	dc_uses
;

--select sum(sales_value) from   csx_dw.supply_turnover  where sdt='20200119';	
-- set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.supply_turnover partition
	(sdt
	)
select
	substr(regexp_replace (${hiveconf:edate} , '-' , ''),1,4) as years  ,
	substr(regexp_replace (${hiveconf:edate} , '-' , ''),1,6) as months ,
	prov_code                                                           ,
	prov_name                                                           ,
	dist_code                                                           ,
	dist_name                                                           ,
	a.shop_id                                                           ,
	shop_name                                                           ,
	a.goodsid                                                           ,
	goods_name                                                          ,
	standard                                                            ,
	c.unit_name                                                         ,
	brand_name                                                          ,
	dept_id                                                             ,
	dept_name                                                           ,
	bd_id                                                               ,
	bd_name                                                             ,
	div_id                                                              ,
	div_name                                                            ,
	catg_l_id                                                           ,
	catg_l_name                                                         ,
	catg_m_id                                                           ,
	catg_m_name                                                         ,
	catg_s_id                                                           ,
	catg_s_name                                                         ,
	nvl(valid_tag,'')        as valid_tag                                         ,
	nvl(valid_tag_name,'')   as valid_tag_name                                    ,
	nvl(goods_status_id,'')  as goods_status_id                                   ,
	nvl(goods_status_name,'')as goods_status_name                                 ,
	sales_qty                                                                   ,
	sales_value                                                                 ,
	profit                                                                      ,
	sales_cost                                                                  ,
	period_inv_qty                                                              ,
	period_inv_amt                                                              ,
	-- period_inv_qty_30day,
	-- period_inv_amt_30day,
	final_qty    ,
	final_amt    ,
	days_turnover,
	sale_30day   ,
	qty_30day    ,
	-- cost_30day,
	days_sale                        ,
	nvl(max_sale_sdt,'')                                                                                                   as  max_sale_sdt,
	coalesce(datediff(date_sub(current_date(),1),from_unixtime(unix_timestamp(max_sale_sdt,'yyyyMMdd'),'yyyy-MM-dd')) ,0) as no_sale_days,
	coalesce(dc_type, '')                                                                                                 as dc_type     ,
	coalesce(entry_qty,0)                                                                                                 as entry_qty   ,
	coalesce(entry_value,0)                                                                                               as entry_value ,
	nvl(entry_sdt,'')                                                                                                     as entry_sdt   ,
	coalesce(datediff(date_sub(current_date(),1),from_unixtime(unix_timestamp(entry_sdt,'yyyyMMdd'),'yyyy-MM-dd')) ,0)    as entry_days  ,
	nvl(dc_uses,'')                                                                                                       as dc_uses     ,
	cost_30day                                                                                                                           ,
	period_inv_qty_30day                                                                                                                 ,
	period_inv_amt_30day                                                                                                                 ,
	COALESCE
		(
			case
				when cost_30day	=0
					then 999
					else (period_inv_amt_30day)/ (cost_30day)
			end,0
		)   AS days_trunover_30,
	regexp_replace (${hiveconf:edate}, '-' , '')    sdt
from
	csx_tmp.p_invt_2 a
	left join
		csx_tmp.p_sale_max b
		on
			a.shop_id    =b.shop_id
			and a.goodsid=b.goodsid
	left join
		csx_tmp.p_entry_max j
		on
			a.shop_id    =j.receive_location_code
			and a.goodsid=j.goods_code
	LEFT OUTER JOIN
		(
			SELECT
				shop_code                   AS shop_id          ,
				product_code                as   goodsid          ,
				product_status_name         as goods_status_name,
				des_specific_product_status AS goods_status_id  ,
				valid_tag                                       ,
				valid_tag_name
			FROM
				csx_dw.dws_basic_w_a_csx_product_info
			WHERE
				sdt = regexp_replace (${hiveconf:edate}, '-' , '')
		)
		d
		ON
			a.shop_id     = d.shop_id
			AND a.goodsid = d.goodsid
	left JOIN
		(
			select
				goods_id                 ,
				goods_name               ,
				standard                 ,
				unit_name                ,
				brand_name               ,
				department_id   dept_id  ,
				department_name dept_name,
				case
					when division_code in ('12',
										   '13',
										   '14')
						then '12'
					when division_code in ('10',
										   '11')
						then '11'
						else division_code
				end bd_id,
				case
					when division_code in ('12',
										   '13',
										   '14')
						then '食品用品采购部'
					when division_code in ('10',
										   '11')
						then '生鲜采购部'
						else division_name
				end                  bd_name    ,
				division_code        div_id     ,
				division_name        div_name   ,
				category_large_code  catg_l_id  ,
				category_large_name  catg_l_name,
				category_middle_code catg_m_id  ,
				category_middle_name catg_m_name,
				category_small_code  catg_s_id  ,
				category_small_name  catg_s_name
			from
				csx_dw.dws_basic_w_a_csx_product_m
			where
				sdt='current'
		)
		c
		ON
			a.goodsid = c.goods_id
;