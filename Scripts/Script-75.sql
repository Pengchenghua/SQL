select * from csx_b2b_accounting.accounting_stock_detail_view where 
	posting_time < '2019-11-01 00:00:00'
	and company_code = '2211'
	and location_code ='W080'
	and product_code ='557';