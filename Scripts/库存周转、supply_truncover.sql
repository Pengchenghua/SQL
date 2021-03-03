-- 食百全国数据
select prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name,
		sales_qty as  sales_qty,
		sales_value/10000 as  sales_value,
		profit/10000 as profit,
		COALESCE(profit/ sales_value,0)* 1.00 AS profit_rate,
		sales_cost/10000 as sales_cost,
		period_inv_amt/10000 as period_inv_amt,
		final_amt/10000 as final_amt,
		final_qty,
		days_turnover,
		goods_sku,
		sale_sku,
		round(sale_sku/ goods_sku,4)* 1.00 pin_rate,
		negative_inventory,
		negative_amt/10000 as negative_amt,
		highet_sku,
		highet_amt/10000 as highet_amt
from 
(
select prov_code ,prov_name,'00'bd_id,'小计'bd_name,'00'dept_id,'小计'dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
		sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) 
			THEN goodsid 
			WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN final_amt WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN final_amt END) highet_amt
from (
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '12'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name
	)a group by prov_code ,prov_name
	union all 
	-- 全国课组情况 
select prov_code ,
		prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) 
			THEN goodsid 
			WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN final_amt WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN final_amt END) highet_amt
from (
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '12'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name
	)a group by prov_code ,prov_name,dept_id,dept_name,bd_id,bd_name
	union all 
	--省份明细
	select prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) 
			THEN goodsid 
			WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN final_amt WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN final_amt END) highet_amt
from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '12'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name,dept_id,dept_name,bd_id,bd_name
	union all 
	--省份课组汇总
	select prov_code ,prov_name,bd_id,bd_name,'00'dept_id,'小计'dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,		sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) 
			THEN goodsid 
			WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN final_amt WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN final_amt END) highet_amt
from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '12'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name,bd_id,bd_name
	union all 
	--省份课组汇总
	select prov_code ,prov_name,'00'bd_id,'小计'bd_name,'00'dept_id,'小计'dept_name,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) 
			THEN goodsid 
			WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN goodsid END )highet_sku,
		SUM(CASE WHEN (days_turnover>30 AND final_amt>2000 AND a.dept_id IN ('A01', 'A02', 'A03', 'A04', 'A10')) THEN final_amt WHEN (days_turnover>45 AND final_amt>2000 AND a.dept_id IN ('A05', 'A06', 'A07', 'A08', 'A09', 'P01', 'P10')) THEN final_amt END) highet_amt
from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '12'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name
	)	a 
	--group by prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name 
	order by prov_code,bd_id,dept_id
	
-- 生鲜
	-- 全国数据
select prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name,
		sales_qty as  sales_qty,
		sales_value/10000 as  sales_value,
		profit/10000 as profit,
		COALESCE(profit/ sales_value,
	0)* 1.00 AS profit_rate,
	sales_cost/10000 as sales_cost,
		period_inv_amt/10000 as period_inv_amt,
		final_amt/10000 as final_amt,
		final_qty,
		days_turnover,
		goods_sku,
		sale_sku,
		round(sale_sku/ goods_sku,
	4)* 1.00 pin_rate,
		negative_inventory,
		negative_amt/10000 as negative_amt,
		highet_sku,
		 highet_amt/10000 as highet_amt
from 
(
select prov_code ,prov_name,'00'bd_id,'小计'bd_name,'00'dept_id,'小计'dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
				COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name
	)a group by prov_code ,prov_name
	union all 
	-- 全国课组情况 
select prov_code ,
		prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name
	)a group by prov_code ,prov_name,dept_id,dept_name,bd_id,bd_name
	union all 
	--省份明细
	select prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
				COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name,dept_id,dept_name,bd_id,bd_name
	union all 
	--省份课组汇总
	select prov_code ,prov_name,bd_id,bd_name,'00'dept_id,'小计'dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,		sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
				COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name,bd_id,bd_name
	union all 
	--省份课组汇总
	select prov_code ,prov_name,'00'bd_id,'小计'bd_name,'00'dept_id,'小计'dept_name,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
				COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name
	)	a 
	--group by prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name 
	order by prov_code,bd_id,dept_id;
	
	-- 生鲜(小店联营)
	-- 全国数据
select prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name,
		sales_qty as  sales_qty,
		sales_value/10000 as  sales_value,
		profit/10000 as profit,
		COALESCE(profit/ sales_value,
	0)* 1.00 AS profit_rate,
	sales_cost/10000 as sales_cost,
		period_inv_amt/10000 as period_inv_amt,
		final_amt/10000 as final_amt,
		final_qty,
		days_turnover,
		goods_sku,
		sale_sku,
		round(sale_sku/ goods_sku,
	4)* 1.00 pin_rate,
		negative_inventory,
		negative_amt/10000 as negative_amt,
		highet_sku,
		 highet_amt/10000 as highet_amt
from 
(
select prov_code ,prov_name,'00'bd_id,'小计'bd_name,'00'dept_id,'小计'dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
from (
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11' and shop_id like 'E%'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name
	)a group by prov_code ,prov_name
	union all 
	-- 全国课组情况 
select prov_code ,
		prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
			COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		'00' AS prov_code,
		'全国' AS prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11' and shop_id like 'E%'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name
	)a group by prov_code ,prov_name,dept_id,dept_name,bd_id,bd_name
	union all 
	--省份明细
	select prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
			COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11' and shop_id like 'E%'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name,dept_id,dept_name,bd_id,bd_name
	union all 
	--省份课组汇总
	select prov_code ,prov_name,bd_id,bd_name,'00'dept_id,'小计'dept_name,
SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,		sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11' and shop_id like 'E%'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name,bd_id,bd_name
	union all 
	--省份课组汇总
	select prov_code ,prov_name,'00'bd_id,'小计'bd_name,'00'dept_id,'小计'dept_name,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.profit, 0)) profit,
				sum(sales_cost) sales_cost,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		round( SUM(period_inv_amt)/ SUM(sales_cost),2) AS days_turnover,
		COUNT( goodsid )goods_sku,
		COUNT( CASE WHEN a.sales_value <> 0 THEN goodsid END )sale_sku,
		COUNT( CASE WHEN a.final_amt<0 THEN goodsid END )negative_inventory,
		SUM(CASE WHEN final_amt<0 THEN final_amt END ) negative_amt,
		COUNT(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END )highet_sku,
		SUM(CASE WHEN days_turnover>15 AND final_amt>500 THEN final_amt END ) highet_amt
		from (
	SELECT
		 prov_code,
		 prov_name,
		 bd_id,
		 bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt)/ SUM(sales_cost),2),0) AS days_turnover
	FROM
	csx_dw.supply_turnover a
	WHERE
		a.bd_id = '11' and shop_id like 'E%'
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,prov_code,prov_name
	)a group by prov_code ,prov_name
	)	a 
	--group by prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name 
	order by prov_code,bd_id,dept_id;
	
--食百明细
SELECT
prov_code     ,
prov_name     ,
shop_id       ,
shop_name     ,
goodsid       ,
goodsname     ,
standard      ,
unit_name     ,
brand_name    ,
dept_id       ,
dept_name     ,
bd_id         ,
bd_name       ,
div_id        ,
div_name      ,
catg_l_id     ,
catg_l_name   ,
catg_m_id     ,
catg_m_name   ,
catg_s_id      ,
catg_s_name    ,
valid_tag      ,
valid_tag_name ,
goods_status_id,
goods_status_name,
sales_qty      ,
sales_value    ,
profit         ,
sales_cost     ,
period_inv_qty ,
period_inv_amt ,
final_qty      ,
final_amt      ,
days_turnover  ,
sale_30day     ,
qty_30day      ,
days_sale      ,
max_sale_sdt,
no_sale_days
FROM
	csx_dw.supply_turnover
WHERE 
	bd_id = '11'
and sdt='20200315'
--and  shop_id like 'E%'
;
--生鲜明细
refresh csx_dw.supply_turnover;
refresh csx_dw.supply_turnover_province;
refresh csx_dw.sale_goods_m1;

SELECT
dc_type,
	prov_code     ,
prov_name     ,
shop_id       ,
shop_name     ,
goodsid       ,
goodsname     ,
standard      ,
unit_name     ,
brand_name    ,
dept_id       ,
dept_name     ,
bd_id         ,
bd_name       ,
div_id        ,
div_name      ,
catg_l_id     ,
catg_l_name   ,
catg_m_id     ,
catg_m_name   ,
catg_s_id      ,
catg_s_name    ,
valid_tag      ,
valid_tag_name ,
goods_status_id,
goods_status_name,
sales_qty      ,
sales_value    ,
profit         ,
sales_cost     ,
period_inv_qty ,
period_inv_amt ,
final_qty      ,
final_amt      ,
days_turnover  ,
sale_30day     ,
qty_30day      ,
days_sale      ,
max_sale_sdt,
no_sale_days,
entry_qty,
entry_value,
entry_sdt,
entry_days
FROM
	csx_dw.supply_turnover
WHERE
	bd_id = '12' 	
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
--and  shop_id like 'E%'
;
select
	prov_code,
	prov_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name,
	sales_qty,
	sales_value,
	profit,
	profit_rate,
	sales_cost,
	period_inv_amt,
	final_amt,
	final_qty,
	days_turnover,
	goods_sku,
	sale_sku,
	pin_rate,
	negative_inventory,
	negative_amt,
	highet_sku,
	highet_amt,
	no_sale_sku,
	no_sale_amt
from
	csx_dw.supply_turnover_province
where
	sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	and type = '12'
	
;


select dc_type,
prov_code,
prov_name         ,
shop_id           ,
shop_name         ,
bd_id             ,
bd_name           ,
dept_id           ,
dept_name         ,
sales_qty         ,
sales_value       ,
profit            ,
profit_rate       ,
sales_cost        ,
period_inv_amt    ,
final_amt         ,
final_qty         ,
days_turnover     ,
goods_sku         ,
sale_sku          ,
pin_rate          ,
negative_inventory,
negative_amt,
highet_sku,
highet_amt,
no_sale_sku,
no_sale_amt from csx_dw.supply_turnover_dc where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
--and prov_code='500000'
order by shop_id ,bd_id,dept_id;

refresh csx_dw.customer_sale_m;
refresh csx_dw.sale_goods_m1;
select substr(category_small_code,1,2),SUM(sales_value) from csx_dw.sale_goods_m1 where sdt>='20200101' and sdt<='20200121'
group by  substr(category_small_code,1,2);
select bd_id,bd_name,sum(sales_value) from csx_dw.supply_turnover where sdt='20200121'
group by  bd_id,bd_name;


SELECT
		 prov_code,
		prov_name,
		prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt) / SUM(sales_cost),2),0) AS days_turnover,
		max(a.max_sale_sdt) as max_sale_sdt,
		datediff(to_date(date_sub(current_timestamp(),1)),from_unixtime(unix_timestamp(max(a.max_sale_sdt),'yyyyMMdd'),'yyyy-MM-dd')) as no_sale_days
	FROM
	csx_dw.supply_turnover a
	WHERE
	 sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') AND prov_code='W0H4'
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,
	 prov_code  ,
	prov_name  ;

--米面粮油   dc
select
	province_code,
	province_name,
	dc_code,
	dc_name,
	goods_code,
	bar_code,
	goods_name,
	unit,
	department_id,
	department_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name ,
	qty ,
	amt
from
	(
	select
		province_code,
		province_name,
		dc_code,
		dc_name,
		goods_code,
		bar_code,
		goods_name,
		unit,
		department_id,
		department_name,
		category_large_code,
		category_large_name,
		category_middle_code,
		category_middle_name,
		sum(qty)qty,
		sum(amt)amt
	from
		csx_dw.wms_accounting_stock_m a
	join (
		select
			shop_id,
			province_code,
			province_name
		from
			csx_dw.shop_m
		where
			sdt = 'current') b on
		regexp_replace(a.dc_code,
		'(^E)',
		'9')= regexp_replace(b.shop_id,
		'(^E)',
		'9')
	where
		sdt = '20200209'
		and (category_middle_code in('110119',
		'110120',
		'110123',
		'110132',
		'124005',
		'125701')
		or category_large_code in('1240'))
		and reservoir_area_code not in ('PD01',
		'PD02',
		'TS01',
		'B999',
		'B997')
	GROUP by
		province_code,
		province_name,
		dc_code,
		dc_name,
		goods_code,
		bar_code,
		goods_name,
		unit,
		department_id,
		department_name,
		category_large_code,
		category_large_name,
		category_middle_code,
		category_middle_name ) a
where
	qty>0 ;



select * from dim.dim_catg where sdt='20200125';

-- 粮油、米面
select province_code,province_name,goods_code,bar_code,goods_name,unit,department_id,department_name,category_large_code,category_large_name,qty ,amt  
from (
select province_code,province_name,dc_code,dc_name,goods_code,bar_code,goods_name,unit,department_id,department_name,
category_large_code,category_large_name,
category_middle_code,category_middle_name,
sum(qty)qty,sum(amt)amt 
from csx_dw.wms_accounting_stock_m  a 
join 
(select shop_id,province_code,province_name from csx_dw.shop_m where sdt='current') b on regexp_replace(a.dc_code,'(^E)','9')=regexp_replace(b.shop_id,'(^E)','9')
where sdt='20200209' 
and (category_middle_code in('110119','110120','110123','110132','124005','125701') or category_large_code in('1240'))
and reservoir_area_code not in ('PD01','PD02','TS01','B999','B997')
GROUP by  province_code,province_name,dc_code,dc_name,goods_code,bar_code,goods_name,unit,department_id,department_name,
category_large_code,category_large_name,
category_middle_code,category_middle_name
) a where amt>10
;

--销售

SELECT
		 prov_code,
		prov_name,
		prov_name,
		bd_id,
		bd_name,
		dept_id,
		dept_name,
		a.goodsid,
		SUM(COALESCE(a.sales_qty, 0))sales_qty,
		SUM(COALESCE(a.sales_value, 0)) sales_value,
		SUM(COALESCE(a.sales_cost, 0)) sales_cost,
		SUM(COALESCE(a.profit, 0)) profit,
		SUM(COALESCE(a.period_inv_amt, 0)) period_inv_amt,
		SUM(COALESCE(a.final_amt, 0)) final_amt,
		SUM(COALESCE(final_qty, 0)) final_qty,
		COALESCE(round( SUM(period_inv_amt) / SUM(sales_cost),2),0) AS days_turnover,
		max(a.max_sale_sdt) as max_sale_sdt,
		datediff(to_date(date_sub(current_timestamp(),1)),from_unixtime(unix_timestamp(max(a.max_sale_sdt),'yyyyMMdd'),'yyyy-MM-dd')) as no_sale_days
	FROM
	csx_dw.supply_turnover a
	WHERE
	 sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','') AND prov_code='W0H4'
	group by a.goodsid,bd_id,bd_name,dept_id,dept_name,
	 prov_code  ,
	prov_name  ;

-- 库存明细
select
	province_code,
	province_name,
	--dc_code,dc_name,
	goods_code,
	bar_code,
	goods_name,
	unit,
	department_id,
	department_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name ,
	qty ,
	amt
from
	(
	select
		province_code,
		province_name,
--		dc_code,
--		dc_name,
		goods_code,
		bar_code,
		goods_name,
		unit,
		department_id,
		department_name,
		category_large_code,
		category_large_name,
		category_middle_code,
		category_middle_name,
		sum(qty)qty,
		sum(amt)amt
	from
		csx_dw.wms_accounting_stock_m a
	join (
		select
			shop_id,
			province_code,
			province_name
		from
			csx_dw.shop_m
		where
			sdt = 'current') b on
		regexp_replace(a.dc_code,
		'(^E)',
		'9')= regexp_replace(b.shop_id,
		'(^E)',
		'9')
	where
		sdt = '20200202'
		and (category_middle_code in('124001',
'124002',
'125001',
'125003',
'125002',
'134203')
		or category_small_code in('13640804',
'13420603',
'12410801'))
		and reservoir_area_code not in ('PD01',
		'PD02',
		'TS01',
		'B999',
		'B997')
	GROUP by
		province_code,
		province_name,
--		dc_code,
--		dc_name,
		goods_code,
		bar_code,
		goods_name,
		unit,
		department_id,
		department_name,
		category_large_code,
		category_large_name,
		category_middle_code,
		category_middle_name ) a
where
	qty>0 ;
	

select
	prov_code,
	prov_name,
	shop_id,
	shop_name,
	'00'bd_id,
	'合计'bd_name,
	'00'dept_id,
	'合计'dept_name,
	sum(sales_qty)sales_qty,
	sum(sales_value)sales_value,
	sum(profit)profit,
	--profit_rate,
	sum(sales_cost)sales_cost,
	sum(period_inv_amt)period_inv_amt,
	sum(final_amt)final_amt,
	sum(final_qty)final_qty,
	--days_turnover,
	sum(goods_sku)goods_sku,
	sum(sale_sku)sale_sku,
	--pin_rate,
	sum(negative_inventory)negative_inventory,
	sum(negative_amt)negative_amt,
	sum(highet_sku)highet_sku,
	sum(highet_amt)highet_amt,
	sum(no_sale_sku)no_sale_sku,
	sum(no_sale_amt)no_sale_amt
from
	csx_dw.supply_turnover
where
	sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by 
	prov_code,
	prov_name,
	shop_id,
	shop_name
union all 
	-- 部类汇总
select
	prov_code,
	prov_name,
	shop_id,
	shop_name,
	bd_id,
	bd_name,
	'00'dept_id,
	'小计'dept_name,
	sum(sales_qty)sales_qty,
	sum(sales_value)sales_value,
	sum(profit)profit,
	--profit_rate,
	sum(sales_cost)sales_cost,
	sum(period_inv_amt)period_inv_amt,
	sum(final_amt)final_amt,
	sum(final_qty)final_qty,
	--days_turnover,
	sum(goods_sku)goods_sku,
	sum(sale_sku)sale_sku,
	--pin_rate,
	sum(negative_inventory)negative_inventory,
	sum(negative_amt)negative_amt,
	sum(highet_sku)highet_sku,
	sum(highet_amt)highet_amt,
	sum(no_sale_sku)no_sale_sku,
	sum(no_sale_amt)no_sale_amt
from
	csx_dw.supply_turnover
where
	sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	group by 	prov_code,
	prov_name,
	shop_id,
	shop_name,
	bd_id,
	bd_name
union all 
select
	prov_code,
	prov_name,
	shop_id,
	shop_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name,
	sum(sales_qty)sales_qty,
	sum(sales_value)sales_value,
	sum(profit)profit,
	--profit_rate,
	sum(sales_cost)sales_cost,
	sum(period_inv_amt)period_inv_amt,
	sum(final_amt)final_amt,
	sum(final_qty)final_qty,
	--days_turnover,
	sum(goods_sku)goods_sku,
	sum(sale_sku)sale_sku,
	--pin_rate,
	sum(negative_inventory)negative_inventory,
	sum(negative_amt)negative_amt,
	sum(highet_sku)highet_sku,
	sum(highet_amt)highet_amt,
	sum(no_sale_sku)no_sale_sku,
	sum(no_sale_amt)no_sale_amt
from
	csx_dw.supply_turnover
where
	sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
group by prov_code,
	prov_name,
	shop_id,
	shop_name,
	bd_id,
	bd_name,
	dept_id,
	dept_name
;

SELECT * from csx_dw.wms_accounting_stock_m where sdt='20200324' and sys='old';
select * from csx_dw.supply_turnover_dc where sdt='20200227';

select * from csx_dw.customer_sale_m where sdt='20200218' and dc_code in ('W0A8');



SELECT distinct shop_name FROM csx_dw.supply_turnover_dc where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),2)),'-','');

select * from csx_ods.wms_accounting_stock_detail_ods where location_code='W0A6' and product_code='967534';

refresh csx_dw.customer_sale_m ;
select * from csx_dw.customer_sale_m   where sdt>='20200301' and sdt<='20200323' and goods_code='707' and dc_code='W0H4' ;

select * from csx_dw.dws_csms_r_d_yszx_order_m_new   where sdt>='20200301' and sdt<='20200323' and goods_code='707' and dc_code='W0H4' ;
