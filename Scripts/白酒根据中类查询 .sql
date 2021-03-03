-- CONNECTION: name= HVIE
-- CONNECTION: name= HVIE
 set
hive.exec.max.dynamic.partitions.pernode = 1000;

set
hive.exec.dynamic.partition.mode = nonstrict;

set
hive.exec.dynamic.partition = true;

insert
	overwrite table
		csx_dw.baijiu_sale partition(sdt) select
			prov_code,
			prov_name,
			a.shop_id,
			shop_name,
			a.cust_id,
			cust_name,
			source_id_new,
			source_name_new,
			type_1_id_new,
			type_1_name_new,
			a.goodsid,
			goodsname,
			bar_code,
			brand_name,
			d_qty,
			d_sale,
			d_profit,
			w_qty,
			w_sale,
			w_profit,
			m_qty,
			m_sale,
			m_profit,
			y_qty,
			y_sale,
			y_profit,
			last_w_qty,
			last_w_sale,
			last_w_profit,
			last_m_qty,
			last_m_sale,
			last_m_profit,
			regexp_replace(to_date(date_sub(current_timestamp(),
			1)),
			'-',
			'') sdt
		from
			(
			select
				a.shop_id,
				regexp_replace(a.cust_id,
				'(^0*)',
				'')cust_id,
				a.goodsid,
				sum(case when sdt = regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '') then a.qty end )d_qty,
				sum(case when sdt = regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '') then a.tax_salevalue end )d_sale,
				sum(case when sdt = regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '') then a.tax_profit end )d_profit,
				sum(case when sdt >= regexp_replace(to_date(date_sub(next_day(date_sub(current_timestamp(), 1), 'MO'), 7)), '-', '') then a.qty end )w_qty,
				sum(case when sdt >= regexp_replace(to_date(date_sub(next_day(date_sub(current_timestamp(), 1), 'MO'), 7)), '-', '') then a.tax_salevalue end )w_sale,
				sum(case when sdt >= regexp_replace(to_date(date_sub(next_day(date_sub(current_timestamp(), 1), 'MO'), 7)), '-', '') then a.tax_profit end )w_profit,
				sum(case when sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(), 1), 'MM')), '-', '') then a.qty end )m_qty,
				sum(case when sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(), 1), 'MM')), '-', '') then a.tax_salevalue end )m_sale,
				sum(case when sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(), 1), 'MM')), '-', '') then a.tax_profit end )m_profit,
				sum(a.qty)y_qty,
				sum(a.tax_salevalue)y_sale,
				sum(a.tax_profit)y_profit,
				sum(case when sdt >= regexp_replace(to_date(date_sub(next_day(date_sub(current_timestamp(), 1), 'MO'), 14)), '-', '') and sdt <= regexp_replace(to_date(date_sub(current_timestamp(), 8)), '-', '') then a.qty end )as last_w_qty,
				sum(case when sdt >= regexp_replace(to_date(date_sub(next_day(date_sub(current_timestamp(), 1), 'MO'), 14)), '-', '') and sdt <= regexp_replace(to_date(date_sub(current_timestamp(), 8)), '-', '') then a.tax_salevalue end )as last_w_sale,
				sum(case when sdt >= regexp_replace(to_date(date_sub(next_day(date_sub(current_timestamp(), 1), 'MO'), 14)), '-', '') and sdt <= regexp_replace(to_date(date_sub(current_timestamp(), 8)), '-', '')then a.tax_profit end )as last_w_profit,
				sum(case when sdt >= regexp_replace(to_date(add_months(trunc(date_sub(current_timestamp(), 1), 'MM'),-1)), '-', '') and sdt <= regexp_replace(to_date(add_months(date_sub(current_timestamp(), 1),-1)), '-', '') then a.qty end )as last_m_qty,
				sum(case when sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(), 1), 'MM')), '-', '') and sdt <= regexp_replace(to_date(add_months(date_sub(current_timestamp(), 1),-1)), '-', '') then a.tax_salevalue end ) as last_m_sale,
				sum(case when sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(), 1), 'MM')), '-', '') and sdt <= regexp_replace(to_date(add_months(date_sub(current_timestamp(), 1),-1)), '-', '') then a.tax_profit end )as last_m_profit
			from
				(select sdt,a.shop_id,shopid_orig,a.vendor_id,a.cust_id,goodsid, 
sum(qty)qty,
sum(a.tax_sales_costvalue)tax_sales_costvalue,
sum(a.tax_salevalue)tax_salevalue,
sum(a.tax_profit)tax_profit
from  csx_ods.sale_b2b_dtl_fct	 a
where  sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(),
				1),
				'YY')),
				'-',
				'')
and sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and shop_id<>'W098'
and sflag in('qyg','qyg_c') 
  and a.catg_m_id = '123203'
group by shop_id,cust_id,shopid_orig,sdt,a.vendor_id,goodsid
union all
select sdt,a.shop_id,shopid_orig,a.vendor_id,a.cust_id,goodsid, 
sum(qty)qty,
sum(a.tax_sales_costvalue)tax_sales_costvalue,
sum(a.tax_salevalue)tax_salevalue,
sum(a.tax_profit)tax_profit
from  csx_ods.sale_b2b_dtl_fct a
left join 
(select concat('S',shop_id)cust_id from dim.dim_shop where edate='9999-12-31' and sales_dist_new between '600000' and '690000' ) c
    on a.cust_id=c.cust_id where c.cust_id is null
       and a.sflag in ('gc') and a.catg_m_id = '123203'
and sdt >= regexp_replace(to_date(trunc(date_sub(current_timestamp(),
				1),
				'YY')),
				'-',
				'')
				and sdt<=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
and shop_id<>'W098'
 group by a.sdt,a.shop_id,a.vendor_id,goodsid,a.cust_id,shopid_orig
) a
group by a.shop_id,
				cust_id,
				a.goodsid )a 
left join (
				select shop_id,
				shop_name,
				prov_code,
				prov_name
			from
				dim.dim_shop a
			where
				edate = '9999-12-31'
				and sales_dist_new between '610000' and '690000') b on
			a.shop_id = b.shop_id
		left join (
				select a.cust_id,
				a.cust_name,
				a.source_id_new,
				a.source_name_new,
				a.type_1_id_new,
				a.type_1_name_new
			from
				csx_ods.b2b_customer_new a ) c on
			regexp_replace(a.cust_id,
			'(^0*)',
			'')= regexp_replace(c.cust_id,
			'(^0*)',
			'')
	left 	join (
				select a.goodsid,
				a.goodsname,
				a.bar_code,
				a.brand_name
			from
				dim.dim_goods a
			where
				a.edate = '9999-12-31') d on
			a.goodsid = d.goodsid;
