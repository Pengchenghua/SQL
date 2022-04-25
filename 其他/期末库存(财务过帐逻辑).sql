-- 库存按照过帐日期查询 商品组别
select
	a.shipper_code       ,
	location_code        ,
	shop_name            ,
	a.company_code       ,
	a.reservoir_area_code,
	c.reservoir_area_name,
	a.product_code       ,
	product_name         ,
	purchase_group_code  ,
	purchase_group_name  ,
	qty                  ,
	amt_no_tax
from
	(
		SELECT
			shipper_code       ,
			location_code      ,
			company_code       ,
			reservoir_area_code,
			product_code       ,
			sum(
			IF
			(
				in_or_out = 0, txn_qty,
				IF
				(
					in_or_out = 1,- txn_qty, 0
				)
			)
			) AS qty ,
			sum(
			IF
			(
				in_or_out = 1, -amt_no_tax, amt_no_tax
			)
			) AS amt_no_tax
		FROM
			csx_b2b_accounting.accounting_stock_detail
		where
			posting_time < '2020-04-01 00:00:00'
		GROUP BY
			shipper_code       ,
			location_code      ,
			company_code       ,
			reservoir_area_code,
			product_code
	)
	a
	left join
		(
			select *
			from
				csx_basic_data.csx_product_info cpi
		)
		b
		on
			a.product_code     =b.product_code
			and a.location_code=shop_code
	LEFT JOIN
		(
			select
				warehouse_code     ,
				reservoir_area_code,
				reservoir_area_name
			from
				csx_b2b_wms.wms_reservoir_area wra
		)
		c
		on
			a.reservoir_area_code=c.reservoir_area_code
			and c.warehouse_code =location_code
;