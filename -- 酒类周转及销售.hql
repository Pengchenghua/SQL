-- 酒类周转及销售	
select * from csx_tmp.ads_wms_r_d_goods_turnover  
where sdt in (select distinct regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd'))),'-','') 
		from csx_dw.dws_basic_w_a_date where calday>='20200101' and calday<'20210813')
and classify_middle_code ='B0401'
;



select
    substr(sdt,1,6)mon,
    sdt,
	a.province_code ,
	province_name,
	city_group_code ,
	city_group_name ,
	dc_code ,
	dc_name ,
	a.channel_name ,
	a.customer_no,
	customer_name,
	first_category_name ,
	second_category_name ,
	goods_code ,
	goods_name,
	unit ,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	sum(sales_cost)/sum(sales_qty) as avg_cost ,
	sum(sales_value)/sum(sales_qty) as avg_price ,
	sum(sales_qty) as qty,
	sum(sales_value) sales_value,
	sum(profit) profit ,
	sum(profit)/sum(sales_value) profit_rate
	from csx_dw.dws_sale_r_d_detail a
where sdt>='2020101'
 and sdt<'20210813'
 and classify_middle_code ='B0401'
 group by  substr(sdt,1,6),
	a.province_code ,
	province_name,
	a.channel_name ,
	a.customer_no,
	customer_name,
	first_category_name ,
	second_category_name ,
	goods_code ,
	goods_name,
	unit ,
	classify_small_code,
	classify_small_name,
	category_small_code,
	category_small_name,
	city_group_code ,
	city_group_name ,
	dc_code ,
	dc_name,
sdt;
 