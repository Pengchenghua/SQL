drop table csx_dw.supply_turnover
;

CREATE TABLE `csx_dw.supply_turnover`
	(
		years STRING COMMENT '年份'                              ,
		months STRING COMMENT '月份'                             ,
		`prov_code` string comment '省区编码'                      ,
		`prov_name` string comment '省区名称'                      ,
		`shop_id` string comment 'DC编码'                        ,
		`shop_name` string comment 'DC名称'                      ,
		`goodsid` string comment '商品编码'                        ,
		`goodsname` string comment '商品名称'                      ,
		`standard` string comment '规格'                         ,
		`unit_name` string comment '销售单位'                      ,
		`brand_name` string comment '品牌名称'                     ,
		`dept_id` string comment '课组编码'                        ,
		`dept_name` string comment '课组名称'                      ,
		`bd_id` string comment '采购部编码'                         ,
		`bd_name` string comment '采购部名称'                       ,
		`div_id` string comment '部类编码'                         ,
		`div_name` string comment '部类名称'                       ,
		`catg_l_id` string comment '大类编码'                      ,
		`catg_l_name` string comment '大类名称'                    ,
		`catg_m_id` string comment '中类编码'                      ,
		`catg_m_name` string comment '中类名称'                    ,
		catg_s_id string comment '小类编码'                        ,
		catg_s_name string comment '小类名称'                      ,
		valid_tag string comment '有效标识'                        ,
		valid_tag_name string comment '有效标识'                   ,
		`goods_status_id` string comment '商品状态'                ,
		`goods_status_name` string comment '商品状态名称'            ,
		`sales_qty`      decimal(38,6)comment '销售数量'           ,
		`sales_value`    decimal(38,6)comment '销售金额'           ,
		`profit`         decimal(38,6)comment '毛利额'            ,
		`sales_cost`     decimal(38,6) comment '销售成本'          ,
		`period_inv_qty` decimal(38,6)comment '期间库存量'          ,
		`period_inv_amt` decimal(38,6)comment '期间库存额'          ,
		`final_qty`      decimal(38,6)comment '期末库存量'          ,
		`final_amt`      decimal(38,6)comment '期末库存额'          ,
		`days_turnover`  decimal(38,6)comment '周转天数=期间库存额/期间成本',
		sales_30day       decimal(38,6)comment'30天日均销售'         ,
		qty_30day        decimal(38,6)comment'30天日均销量'         ,
		days_sale        decimal(38,6)comment'可销天数'            ,
		dc_type string comment '是否联营小店'
	)
	comment '供应链库存周转' partitioned by
	(
		sdt string COMMENT '日期分区'
	)
	STORED AS textfile
	
	
	--
	
	
-- 企业购归属大
-- 期末库存,以业务时间为时间节点
-- 新系统库存，需要将旧系统的sales_dist not in ('612000','613000') 北京、安徽剔除 
-- 云超库取未切换新系统的库存
-- 旧系统剔除 inv_place NOT IN ('B997','B999') 新系统剔除 reservoir_area_code not in ('PD01','PD02','TS01')
 SET
mapreduce.job.queuename = caishixian;

SET
edate = '2020-01-16';

SET
sdate = '2020-01-01';

DROP TABLE IF EXISTS temp.p_invt_1;

CREATE TEMPORARY TABLE IF NOT EXISTS temp.p_invt_1 AS
-- 库存查询
 select dc_code,goods_code goodsid, sum(qty)inv_qty,sum(amt)inv_amt,
sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then qty end )qm_qty,
sum(case when sdt=regexp_replace(${hiveconf:edate},'-','') then amt end )qm_amt
from csx_dw.wms_accounting_stock_m where sdt>=regexp_replace(${hiveconf:sdate},'-','')  and sdt<=regexp_replace(${hiveconf:edate},'-','') 
and reservoir_area_code not in ('B999','B997','PD01','PD02','TS01')
group  by dc_code,goods_code
;
--select * from temp.p_invt_1 a where  shop_id='W0A2';
--关联库存与销售
-- SELECT prov_code,prov_name,bd_id,bd_name,dept_id,dept_name,sum(sale)sale,sum
-- FROM 
-- (
 DROP TABLE IF EXISTS temp.p_invt_2;

CREATE TEMPORARY TABLE IF NOT EXISTS temp.p_invt_2 AS
SELECT
     substr(regexp_replace(${hiveconf:edate},'-',''),1,4) as years,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as months,
	b.prov_code,
	b.prov_name,
	a.shop_id,
	b.shop_name,
	a.goodsid,
	c.goodsname,
	c.standard ,
	c.unit_name ,
	c.brand_name ,
	c.dept_id,
	c.dept_name,
	c.bd_id,
	c.bd_name,
	c.div_id,
	c.div_name,
	c.catg_l_id,
	c.catg_l_name,
	c.catg_m_id,
	c.catg_m_name,
	c.catg_s_id,
	c.catg_s_name,
	valid_tag,
	valid_tag_name,
	goods_status_id ,
	goods_status_name,
	SUM(qty) sales_qty,
	SUM(a.sale)sales_value,
	SUM(profit)profit,
	COALESCE(SUM(sale-profit),	0) AS sales_cost,
	SUM(inv_qty)as period_inv_qty,
	SUM(inv_amt)as period_inv_amt,
	SUM(qm_qty)as final_qty,
	SUM(qm_amt)as final_amt,
	COALESCE(SUM(inv_amt)/ SUM(sale-profit),0) AS days_turnover,
	COALESCE(SUM(sales_30day),0) as sale_30day,
	COALESCE(sum(qty_30day),0) as qty_30day,
	COALESCE(SUM(qm_qty)/sum(qty_30day),0) as days_sale
FROM
	(
	SELECT
		shop_id,
		goods_code goodsid,
		SUM(sales_qty)qty,
		SUM(sales_value)sale,
		SUM(profit)profit,
		0 qty_30day,
		0 sales_30day,
		0 inv_qty,
		0 inv_amt,
		0 qm_qty,
		0 qm_amt
	FROM
		csx_dw.sale_goods_m1
	WHERE
		sdt >= regexp_replace(${hiveconf:sdate},'-','')
		AND sdt <= regexp_replace(${hiveconf:edate},'-','')
	GROUP BY
		shop_id,
		goods_code
UNION ALL
	SELECT
		shop_id,
		goods_code goodsid,
		0 qty,
		0 sale,
		0 profit,
		sum(sales_qty)as qty_30day,
		sum(sales_value)as sales_30day,
		0 inv_qty,
		0 inv_amt,
		0 qm_qty,
		0 qm_amt
	FROM
		csx_dw.sale_goods_m1
	WHERE
		sdt >= regexp_replace(date_sub(${hiveconf:edate},30),'-','')
		AND sdt <= regexp_replace(${hiveconf:edate},'-','')
	GROUP BY
		shop_id,
		goods_code
UNION ALL
	SELECT
		a.dc_code AS shop_id,
		a.goodsid,
		0 qty,
		0 sale,
		0 profit,
		0 qty_30day,
		0 sales_30day,
		a.inv_qty,
		a.inv_amt,
		a.qm_qty,
		a.qm_amt
	FROM
		temp.p_invt_1 a) a
JOIN (
	SELECT
		shop_id ,
		shop_name ,
		CASE WHEN a.shop_id = 'W0H4' THEN 'W0H4'
		ELSE a.province_code
END prov_code,
	CASE WHEN a.shop_id = 'W0H4' THEN '供应链平台'
	ELSE a.province_name
END prov_name
FROM
csx_dw.shop_m a
WHERE
sdt = 'current' ) b ON
	regexp_replace(a.shop_id,'^E','9')= b.shop_id
JOIN dim.dim_goods_latest c ON
	a.goodsid = c.goodsid
LEFT OUTER JOIN (
	SELECT
		shop_code AS shop_id,
		product_code goodsid,
		product_status_name  as goods_status_name,
		des_specific_product_status AS goods_status_id,
		valid_tag ,
		valid_tag_name
	FROM
		csx_ods.csx_product_info
	WHERE
		sdt = regexp_replace(${hiveconf:edate},	'-',''))d ON
	a.shop_id = d.shop_id
	AND a.goodsid = d.goodsid
GROUP BY
	b.prov_code,
	b.prov_name,
	a.shop_id,
	b.shop_name,
	a.goodsid,
	c.goodsname,
	c.standard ,
	c.unit_name ,
	c.brand_name ,
	c.dept_id,
	c.dept_name,
	c.bd_id,
	c.bd_name,
	c.div_id,
	c.div_name,
	c.catg_l_id,
	c.catg_l_name,
	c.catg_m_id,
	c.catg_m_name,
	c.catg_s_id,
	c.catg_s_name,
	valid_tag,
	valid_tag_name,
	goods_status_id ,
	goods_status_name ;
	
	set hive.exec.dynamic.partition.mode=nonstrict;
	insert overwrite table csx_dw.supply_turnover partition(sdt)
	select  substr(regexp_replace(${hiveconf:edate},'-',''),1,4) as years,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as months,
    prov_code,
	prov_name,
	shop_id,
	shop_name,
	goodsid,
	goodsname,
	standard ,
	unit_name ,
	brand_name ,
	dept_id,
	dept_name,
	bd_id,
	bd_name,
	div_id,
	div_name,
	catg_l_id,
	catg_l_name,
	catg_m_id,
	catg_m_name,
	catg_s_id,
	catg_s_name,
	valid_tag,
	valid_tag_name,
	goods_status_id ,
	goods_status_name,
	sales_qty,
    sales_value,  
    profit,      
    sales_cost,   
    period_inv_qty,
    period_inv_amt,
    final_qty,
    final_amt,    
    days_turnover,
    sales_30day,     
    qty_30day,      
    days_sale,      
    ''dc_type, 
    regexp_replace(${hiveconf:edate},'-','') sdt 
from temp.p_invt_2
;
select * from csx_dw.supply_turnover