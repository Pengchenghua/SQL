select
	shop_id,
	goodsid,
	sum(inv_qty)inv_qty,
	sum(period_inv_amt)period_inv_amt,
	sum(case when sdt = date_format (${edate}, '%Y%m%d') then inv_qty end )final_qty,
	sum(case when sdt = date_format (${edate}, '%Y%m%d') then period_inv_amt end )qm_amt
from
	(
	select
		a.location_code as shop_id,
		a.product_code as goodsid,
		update_time as sdt,
		sum(after_qty) inv_qty,
		sum(after_amt) period_inv_amt
	from
		(
		select
			location_code,
			location_name,
			a.product_code,
			product_name,
			purchase_group_name,
			purchase_group_code,
			after_qty,
			after_amt,
			after_price,
			tax_rate,
			date_format (posting_time,'%Y%m%d')posting_time,
			date_format (a.biz_time,'%Y%m%d')update_time,
			id,
			reservoir_area_code,
			-- regexp_replace(to_date(biz_time),'-','') biz_date,
			company_code
		from
				csx_b2b_accounting.accounting_stock_detail_view 
			--   WHERE sdt =date_format (${edate},'%Y%m%d') ) a
		join (
			select
				product_code,
				location_code,
				max(id)max_id,
				reservoir_area_code,
				-- regexp_replace(to_date(biz_time),'-','') as biz_date,
				company_code
			from
				csx_b2b_accounting.accounting_stock_detail_view a
			where
				date_format(a.biz_time, '%Y%m%d') < date_format(date_add('${edate}', interval 1 day ), '%Y%m%d')
				--   AND sdt =date_format (${edate},'%Y%m%d')
				--   and reservoir_area_code not in ('PD01','PD02','TS01')
				group by product_code,
				location_code,
				reservoir_area_code,
				company_code) b on
			a.id = b.max_id
		group by
			a.product_code,
			a.location_code,
			update_time )a
	group by
		shop_id,
		goodsid
		;
	
	
	select * from csx_b2b_accounting.accounting_stock_detail where 
	posting_time < '2019-11-01 00:00:00'
	and company_code = '2211'
	and location_code ='99B2'
	and product_code ='932022'
	
