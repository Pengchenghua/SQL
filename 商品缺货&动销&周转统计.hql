set sdate= '2020-10-01';
set edate= '2021-02-28';

drop table csx_tmp.tmp_goods_01;
create temporary table csx_tmp.tmp_goods_01 as 

select
	shop_code,
	--sdt,
	substr(a.sdt,1,6) as mon,
	a.root_category_code,
	count(product_code) as goods_sku
from
	csx_dw.dws_basic_w_a_csx_product_info a
join 
 (select distinct regexp_replace(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd')),'-','') dt
	    from csx_dw.dws_basic_w_a_date 
	    where calday>= regexp_replace(to_date(${hiveconf:sdate}),'-','')
	    and calday <=  regexp_replace(to_date(${hiveconf:edate}),'-','')) b on a.sdt=b.dt
	 where a.des_specific_product_status in ('0','2','1')
	 
	--and shop_code = 'W0A8'
	and stock_properties_name = '存储'
group by
	shop_code,
	a.root_category_code,
--	sdt
   substr(a.sdt,1,6)
;
 
 drop table csx_tmp.tmp_goods_02 ;
 create temporary table csx_tmp.tmp_goods_02 as 
-- 动销SKU
select
	dc_code,
    mon,
    a.division_code,
	count( distinct case when a.sales_value>0 then goods_code end) sales_sku
from (
select
	dc_code,
    substr(sdt,1,6) as mon,
    a.division_code,
	a.goods_code,
	sum(a.sales_value) sales_value
from
	csx_dw.dws_sale_r_d_detail a
	join
	(select 
	shop_code,
	product_code
from
	csx_dw.dws_basic_w_a_csx_product_info a
	where sdt='current' 
	and a.stock_properties_name ='存储' 
	and a.root_category_code in ('11','10','12','13')
	and a.des_specific_product_status in ('0','2','1')
	)m on a.dc_code=m.shop_code and a.goods_code=m.product_code
where
	sdt >= regexp_replace(to_date(${hiveconf:sdate}),'-','')
	and sdt <= regexp_replace(to_date(${hiveconf:edate}),'-','')
--	and dc_code = 'W0A8'
group by
	dc_code,
a.division_code ,
a.goods_code,
 substr(sdt,1,6) 
 )a 
 group by
	dc_code,
a.division_code ,
mon
 ;
 
 -- 周转
 drop table  csx_tmp.tmp_goods_03;
 create temporary table csx_tmp.tmp_goods_03 as  
select
	a.dc_code ,
	substr(a.sdt,1,6) as mon,
	a.division_code,
	count(case when a.final_qty!=0 or qty_30day!=0 then  a.goods_id end ) as sku,
	sum(a.period_inv_amt_30day)as period_inv_amt_30day,
	sum(a.cost_30day) as cost_30day,
	sum(a.period_inv_amt_30day)/ sum(a.cost_30day) as turnover_day
from
	csx_tmp.ads_wms_r_d_goods_turnover a
join
(select 
	shop_code,
	product_code
from
	csx_dw.dws_basic_w_a_csx_product_info a
	where sdt='current' 
	    and a.stock_properties_name ='存储'
	    and a.root_category_code in ('11','10','12','13')
	    and a.des_specific_product_status in ('0','2','1')
	)m on a.dc_code=m.shop_code and a.goods_id=m.product_code
join 
 (select distinct regexp_replace(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd')),'-','') dt
	    from csx_dw.dws_basic_w_a_date 
	    where calday>= regexp_replace(to_date(${hiveconf:sdate}),'-','')
	    and calday <=  regexp_replace(to_date(${hiveconf:edate}),'-','')) b on a.sdt=b.dt
group by
	a.dc_code ,
	a.division_code,
		substr(a.sdt,1,6);

drop table  csx_tmp.tmp_goods_04;
 create temporary table csx_tmp.tmp_goods_04 as 
--缺货率问题：库存为0的商品，我取的口径是近3个月所在dc有销售记录的商品（排除从20201001后已经不在售卖的商品）
 select
	a.dc_code,
	substr(sdt,1,6) as mon,
	division_code,
	count( goods_code) as stock_sku,
	count( if(qq = '缺货', goods_code, null)) stock_out_sku,
	count( if(qq = '缺货', goods_code, null))/ count( goods_code) as stock_out_rate
	--缺货率

	from (
	select
		a.dc_code,
		a.dc_name,
		a.sdt,
		a.goods_code,
		a.goods_name,
		division_code,
		if(a.qty = 0 or a.qty<c.sales_qty,'缺货','正常') qq
	from
		(
		select
			a.dc_code,
			dc_name,
			sdt,
			a.goods_code,
			goods_name,
			a.division_code ,
			sum(qty) qty
		from
			csx_dw.dws_wms_r_d_accounting_stock_m  a
--	left join 
--	(
--	select
--		b.classify_large_code,
--		b.classify_large_name,
--		b.category_small_code
--	from
--		csx_dw.dws_basic_w_a_manage_classify_m b
--	where
--		sdt = 'current') b on
--	a.category_small_code = b.category_small_code
	where
	sdt >= regexp_replace(to_date(${hiveconf:sdate}),'-','')
	and sdt <=  regexp_replace(to_date(${hiveconf:edate}),'-','')
	and a.division_code in ('10','11','12','13')
	group by
			a.dc_code,
			dc_name,
			sdt,
			a.goods_code,
			goods_name,
			a.division_code) a
	join (
		select
			dc_code,
			goods_code
		from
			csx_dw.dws_sale_r_d_detail 
		where
			sdt >= regexp_replace(to_date(${hiveconf:sdate}),'-','')
		and sdt <=  regexp_replace(to_date(${hiveconf:edate}),'-','')
		group by
			dc_code,
			goods_code ) cc on
		a.dc_code = cc.dc_code
		and a.goods_code = cc.goods_code
	join 	
	(select 
	shop_code,
	product_code
from
	csx_dw.dws_basic_w_a_csx_product_info a
	where sdt='current' and a.stock_properties_name ='存储' 
	    and a.root_category_code in ('11','10','12','13')
  and	a.des_specific_product_status in ('0','2','1')
	)m on a.dc_code=m.shop_code and a.goods_code=m.product_code

	left join (
		select
			dc_code,
			goods_code,
			goods_name,
			sdt,
			sum(sales_qty) as sales_qty
		from
			csx_dw.dws_sale_r_d_detail 
		where
			sdt >= regexp_replace(to_date(${hiveconf:sdate}),'-','')
			-- and sdt <=  regexp_replace(to_date('${edate}'),'-','')
			and sales_value > 0
			and return_flag = ''
			and substr(order_no,1,2) <> 'OC'
			--退货单排除
			and logistics_mode_code = '2'
			--物流模式：2-配送,1-直送，2-自提，3-直通
			and operation_mode = 1
			--自营，联营(非自营)，1自营，0联营(非自营)

			group by dc_code,
			goods_code,
			goods_name,
			sdt ) c on
		a.dc_code = c.dc_code
		and a.goods_code = c.goods_code
		and cast(a.sdt as int) + 1 = cast(c.sdt as int) )a
	where 1=1 
--	and a.dc_code in('W0A8','W0A7','W0A3')
group by
	a.dc_code,
	division_code ,
	substr(sdt,1,6) 
	;
-- 汇总数据

select 
    sales_province_code,
    sales_province_name,
    shop_code,
    shop_name,
	a.mon,
	root_category_code,
	division_name,
	goods_sku,
    sales_sku,
    sales_sku/goods_sku as pin_rate, 
    turnover_day,
    stock_out_rate
from csx_tmp.tmp_goods_01 a 
left join csx_tmp.tmp_goods_02 b on a.shop_code=b.dc_code and a.mon=b.mon and a.root_category_code=b.division_code
left join csx_tmp.tmp_goods_03 c on a.shop_code=c.dc_code and a.mon=c.mon and a.root_category_code=c.division_code
left join csx_tmp.tmp_goods_04 d on a.shop_code=d.dc_code and a.mon=d.mon and a.root_category_code=d.division_code
left join (select shop_id,shop_name,sales_province_code,sales_province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') e on a.shop_code=e.shop_id
left join (select distinct division_code,division_name from csx_dw.dws_basic_w_a_category_m where sdt='current') f on a.root_category_code=f.division_code
;

