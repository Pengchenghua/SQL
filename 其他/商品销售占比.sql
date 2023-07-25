drop table csx_dw.temp_goods_sale
;

create temporary table csx_dw.temp_goods_sale as
select
	province_code ,
	province_name ,
	case
		when division_code in ('11',
							   '10')
			then '11'
		when division_code in ('12',
							   '13',
							   '14')
			then '12'
			else division_code
	end bd_id,
	case
		when division_code in ('11',
							   '10')
			then '生鲜采购部'
		when division_code in ('12',
							   '13',
							   '14')
			then '食百采购部'
			else division_name
	end bd_name           ,
	goods_code            ,
	goods_name            ,
	unit                  ,
	sdt                   ,
	is_factory_goods_name ,
	customer_name         ,
	sum(sales_qty)   as qty  ,
	sum(sales_value)    sale ,
	sum(profit)         profit
from
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt        >= '20200301'
	and sdt    <= '20200331'
	and channel = '1'
group by
	province_code ,
	province_name ,
	case
		when division_code in ('11',
							   '10')
			then '11'
		when division_code in ('12',
							   '13',
							   '14')
			then '12'
			else division_code
	end ,
	case
		when division_code in ('11',
							   '10')
			then '生鲜供应链'
		when division_code in ('12',
							   '13',
							   '14')
			then '食百供应链'
			else division_name
	end          ,
	goods_code   ,
	goods_name   ,
	unit         ,
	sdt          ,
	customer_name,
	division_code,
	division_name,
	is_factory_goods_name
;


-- 计算商品频次、数、销售额
drop table csx_dw.temp_goods_sale_01
;

create temporary table csx_dw.temp_goods_sale_01 as
select
	province_code                          ,
	province_name                          ,
	bd_id                                  ,
	bd_name                                ,
	is_factory_goods_name                  ,
	goods_code                             ,
	goods_name                             ,
	unit                                   ,
	sum(sale_goods)      as sale_goods     ,
	sum(sale_cust_goods) as sale_cust_goods,
	sum(cust_no)         as cust_no        ,
	sum(sale)            as sale
from
	(
		SELECT
			province_code                                                                                                    ,
			province_name                                                                                                    ,
			bd_id                                                                                                            ,
			bd_name                                                                                                          ,
			is_factory_goods_name                                                                                            ,
			goods_code                                                                                                       ,
			goods_name                                                                                                       ,
			unit                                                                                                             ,
			count(DISTINCT sdt)over(PARTITION BY province_code, province_name, bd_id, bd_name, goods_code) as sale_goods     ,
			0                                                                                              as sale_cust_goods,
			0                                                                                              as cust_no        ,
			0                                                                                              as sale
		FROM
			csx_dw.temp_goods_sale
			--where goods_code='1' and province_code='1'
		group by
			province_code        ,
			province_name        ,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit
		union all
		-- 统计商品+频次
		SELECT
			province_code        ,
			province_name        ,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit                 ,
			0                                                                                sale_goods         ,
			count(DISTINCT sdt)over(PARTITION BY province_code, goods_code,customer_name) as sale_cust_goods    ,
			0                                                                             as cust_no            ,
			0                                                                             as sale
		FROM
			csx_dw.temp_goods_sale
			--where goods_code='1' and province_code='1'
		group by
			province_code        ,
			province_name        ,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit                 ,
			customer_name
		union all
		-- 销售数
		SELECT
			province_code        ,
			province_name        ,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit                 ,
			0                                                                         sale_goods         ,
			0                                                                         as sale_cust_goods    ,
			count(DISTINCT customer_name)over(PARTITION BY province_code, goods_code) as cust_no            ,
			sum(sale)                                                                 as sale
		FROM
			csx_dw.temp_goods_sale
			--where goods_code='1' and province_code='1'
		group by
			province_code        ,
			province_name        ,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit
	)
	a
	--where  province_code='1'
group by
	province_code        ,
	province_name        ,
	bd_id                ,
	bd_name              ,
	is_factory_goods_name,
	goods_code           ,
	goods_name           ,
	unit
;

;
--- 入库金额、SKU 90%占比、DC库存额、销售额、库存额
--select * FROM csx_dw.temp_goods_sale where goods_code='1' and province_code='1';
drop table csx_dw.temp_inv_data
;

create temporary table csx_dw.temp_inv_data as
select
	province_code,
	province_name,
	bd_id        ,
	bd_name      ,
	count(distinct
	case
		when (
				sale      !=0
				or inv_qty!=0
			)
			then goods_code
	end ) sku,
	count(distinct
	case
		when (
				sale!=0
			)
			then goods_code
	end )       sale_sku ,
	sum(inv_qty)inv_qty  ,
	sum(amt)    inv_amt
from
	(
		select
			dc_code               ,
			dc_name               ,
			bd_id                 ,
			bd_name               ,
			a.goods_code          ,
			sum(a.sales_value)sale,
			sum
				(
					case
						when sdt='20200331'
							then inventory_qty
					end
				)
			inv_qty,
			sum
				(
					case
						when sdt='20200331'
							then inventory_amt
					end
				)
			amt
		from
			csx_dw.dc_sale_inventory a
		where
			sdt    <='20200331'
			and sdt>='20200301'
		group by
			dc_code,
			dc_name,
			bd_id  ,
			bd_name,
			a.goods_code
	)
	a
	join
		(
			select
				case
					when location_code='W0H4'
						then location_code
						else province_code
				end province_code,
				case
					when location_code='W0H4'
						then shop_name
						else province_name
				end province_name,
				location_code    ,
				shop_name
			from
				csx_dw.csx_shop a
				join
					(
						select distinct
							dc_code
						from
							csx_dw.dws_sale_r_d_customer_sale
						where
							sdt       >='20200301'
							and sdt   <='20200331'
							and channel='1'
					)
					c
					on
						a.location_code=c.dc_code
			where
				sdt='current'
		)
		b
		on
			a.dc_code=b.location_code
group by
	province_code,
	province_name,
	bd_id        ,
	bd_name
;

-- 入库金额
drop table csx_dw.temp_entry_data
;

create temporary table csx_dw.temp_entry_data as
select
	province_code,
	province_name,
	bd_id        ,
	bd_name      ,
	entry_amt
from
	(
		select
			a.receive_location_code dc_code,
			case
				when a.division_code in ('10',
										 '11')
					then '11'
				when a.division_code in ('12',
										 '13',
										 '14')
					then '12'
					else a.division_code
			end bd_id,
			case
				when a.division_code in ('10',
										 '11')
					then '生鲜采购部'
				when a.division_code in ('12',
										 '13',
										 '14')
					then '食百采购部'
					else a.division_name
			end                       bd_name,
			sum(a.price*a.receive_qty)entry_amt
		from
			csx_dw.wms_entry_order a
		where
			sdt            <='20200331'
			and sdt        >='20200301'
			and a.entry_type='采购入库'
			--and business_type='供应商配送'
		group by
			a.receive_location_code,
			case
				when a.division_code in ('10',
										 '11')
					then '11'
				when a.division_code in ('12',
										 '13',
										 '14')
					then '12'
					else a.division_code
			end ,
			case
				when a.division_code in ('10',
										 '11')
					then '生鲜采购部'
				when a.division_code in ('12',
										 '13',
										 '14')
					then '食百采购部'
					else a.division_name
			end
	)
	a
	join
		(
			select
				case
					when location_code='W0H4'
						then location_code
						else province_code
				end province_code,
				case
					when location_code='W0H4'
						then shop_name
						else province_name
				end province_name,
				location_code    ,
				shop_name
			from
				csx_dw.csx_shop a
				join
					(
						select distinct
							dc_code
						from
							csx_dw.dws_sale_r_d_customer_sale
						where
							sdt       >='20200301'
							and sdt   <='20200331'
							and channel='1'
					)
					c
					on
						a.location_code=c.dc_code
			where
				sdt='current'
		)
		b
		on
			a.dc_code=b.location_code
;

-- 创建实体表，使用Impala 导出
create table csx_dw.temp_goods_sale_02 as
select *
from
	csx_dw.temp_goods_sale_01
;
create temporary table csx_dw.temp_goods_sale_03 as
select
	province_code        ,
	province_name        ,
	bd_id                ,
	bd_name              ,
	is_factory_goods_name,
	goods_code           ,
	goods_name           ,
	unit                 ,
	sale_goods           ,
	sale_cust_goods      ,
	cust_no              ,
	sale                 ,
	sum(sale)over(partition by province_code order by
				  province_code,sale desc rows between unbounded preceding and current row) as leijia ,
	sum(sale)over(partition by province_code order by
				  province_code rows between unbounded preceding and current row)/sum(sale)over(partition by province_code order by
																								province_code) as sale_ratio
from
	csx_dw.temp_goods_sale_02
where
	1=1
;

-- 统计数据
select
	province_name                           ,
	bd_id                                   ,
	bd_name                                 ,
	sum(sale)/10000             sale        ,
	sum(sku)                    as sku      ,
	sum(sale_sku)               as sale_sku ,
	sum(sale_sku)/sum(sku)*1.00 as sku_rate ,
	sum(top_goods)              as top_goods,
	sum(top_sale)               as top_sale ,
	sum(inv_amt)  /10000          as inv_amt  ,
	sum(entry_amt)/10000          as entry_amt
from
	(
		select
			province_code,
			province_name,
			bd_id        ,
			bd_name      ,
			count(distinct
			case
				when sale_ratio<=0.9
					then goods_code
			end ) as top_goods,
			sum
				(
					case
						when sale_ratio<=0.9
							then sale
					end
				)
			         as top_sale ,
			sum(sale)   sale     ,
			0        as entry_amt,
			0        as sku      ,
			0        as sale_sku ,
			0        as inv_amt
		from
			csx_dw.temp_goods_sale_03
		group by
			province_code,
			province_name,
			bd_id        ,
			bd_name
		union all
		select
			province_code ,
			province_name ,
			bd_id         ,
			bd_name       ,
			0 as top_goods,
			0 as top_sale ,
			0 as sale     ,
			entry_amt     ,
			0 as sku      ,
			0 as sale_sku ,
			0 as inv_amt
		from
			csx_dw.temp_entry_data
		union all
		select
			province_code,
			province_name,
			bd_id        ,
			case
				when bd_id='11'
					then '生鲜采购部'
				when bd_id='12'
					then '食百采购部'
					else bd_name
			end    bd_name  ,
			0   as top_goods,
			0   as top_sale ,
			0   as sale     ,
			0   as entry_amt,
			sku             ,
			sale_sku        ,
			inv_amt
		from
			csx_dw.temp_inv_data
	)
	a
group by
	province_name,
	bd_id        ,
	bd_name;
	
	
	





--使用Impala 查询 导出 dbeaver 数据量大
select province_code,
       province_name,
       dc_city_name ,
       bd_id,
       bd_name,
       is_factory_goods_name,
       goods_code,
       goods_name,
       unit,
       sale_goods,
       sale_cust_goods,
       cust_no,
       sale,
      sum(sale)over(partition by province_code,dc_city_name order by province_code,dc_city_name,sale desc rows between unbounded preceding and current row) as leijia
   ,sum(sale)over(partition by province_code,dc_city_name order by province_code,dc_city_name rows between unbounded preceding and current row)/sum(sale)over(partition by province_code,dc_city_name order by province_code,dc_city_name) as sale_ratio
      
       from  csx_dw.temp_goods_sale_02
       where 1=1
  ;
  
  
  
  
  
  
  
  --- 城市维度 
  
  set mapreduce.job.queuename                 =caishixian;
set edate='2020-04-26';
set sdate='2020-02-01';
drop table csx_dw.temp_goods_sale
;

create temporary table csx_dw.temp_goods_sale as
select
	province_code ,
	province_name ,
	case
		when division_code in ('11',
							   '10')
			then '11'
		when division_code in ('12',
							   '13',
							   '14')
			then '12'
			else division_code
	end bd_id,
	case
		when division_code in ('11',
							   '10')
			then '生鲜采购部'
		when division_code in ('12',
							   '13',
							   '14')
			then '食百采购部'
			else division_name
	end bd_name           ,
	goods_code            ,
	goods_name            ,
	dc_city_name,
	unit                  ,
	sdt                   ,
	is_factory_goods_name ,
	customer_name         ,
	sum(sales_qty)   as qty  ,
	sum(sales_value)    sale ,
	sum(profit)         profit
from
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt        >= regexp_replace(${hiveconf:sdate},'-','')
	and sdt    <= regexp_replace(${hiveconf:edate},'-','')
	and channel = '1'
	and attribute !='合伙人'
group by
	province_code ,
	province_name ,
	dc_city_name,
	case
		when division_code in ('11',
							   '10')
			then '11'
		when division_code in ('12',
							   '13',
							   '14')
			then '12'
			else division_code
	end ,
	case
		when division_code in ('11',
							   '10')
			then '生鲜采购部'
		when division_code in ('12',
							   '13',
							   '14')
			then '食百采购部'
			else division_name
	end          ,
	goods_code   ,
	goods_name   ,
	unit         ,
	sdt          ,
	customer_name,
	division_code,
	division_name,
	is_factory_goods_name
;


-- 计算商品频次、数、销售额
drop table csx_dw.temp_goods_sale_01
;

create temporary table csx_dw.temp_goods_sale_01 as
select
	province_code                          ,
	province_name                          ,
	dc_city_name,
	bd_id                                  ,
	bd_name                                ,
	is_factory_goods_name                  ,
	goods_code                             ,
	goods_name                             ,
	unit                                   ,
	sum(sale_goods)      as sale_goods     ,
	sum(sale_cust_goods) as sale_cust_goods,
	sum(cust_no)         as cust_no        ,
	sum(sale)            as sale
from
	(
		SELECT
			province_code                                                                                                    ,
			province_name  ,
			dc_city_name,
			bd_id                                                                                                            ,
			bd_name                                                                                                          ,
			is_factory_goods_name                                                                                            ,
			goods_code                                                                                                       ,
			goods_name                                                                                                       ,
			unit                                                                                                             ,
			count(DISTINCT sdt)over(PARTITION BY province_code, bd_id, dc_city_name, goods_code) as sale_goods     ,
			0                                                                                              as sale_cust_goods,
			0                                                                                              as cust_no        ,
			0                                                                                              as sale
		FROM
			csx_dw.temp_goods_sale
			--where goods_code='1' and province_code='1'
		group by
			province_code        ,
			province_name        ,
			dc_city_name,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit
		union all
		-- 统计商品+频次
		SELECT
			province_code        ,
			province_name        ,
			dc_city_name,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit                 ,
			0                                                                                sale_goods         ,
			count(DISTINCT sdt)over(PARTITION BY province_code, goods_code,customer_name,dc_city_name) as sale_cust_goods    ,
			0                                                                             as cust_no            ,
			0                                                                             as sale
		FROM
			csx_dw.temp_goods_sale
			--where goods_code='1' and province_code='1'
		group by
			province_code        ,
			province_name        ,
			dc_city_name,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit                 ,
			customer_name
		union all
		-- 销售数
		SELECT
			province_code        ,
			province_name        ,
			dc_city_name,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit                 ,
			0                                                                         sale_goods         ,
			0                                                                         as sale_cust_goods    ,
			count(DISTINCT customer_name)over(PARTITION BY province_code, goods_code,dc_city_name) as cust_no            ,
			sum(sale)                                                                 as sale
		FROM
			csx_dw.temp_goods_sale
			--where goods_code='1' and province_code='1'
		group by
			province_code        ,
			province_name        ,
		    dc_city_name	,
			bd_id                ,
			bd_name              ,
			is_factory_goods_name,
			goods_code           ,
			goods_name           ,
			unit
	)
	a
	--where  province_code='1'
group by
	province_code        ,
	province_name        ,
	dc_city_name,
	bd_id                ,
	bd_name              ,
	is_factory_goods_name,
	goods_code           ,
	goods_name           ,
	unit
;

;
--- 入库金额、SKU 90%占比、DC库存额、销售额、库存额
--select * FROM csx_dw.temp_goods_sale where goods_code='1' and province_code='1';
drop table csx_dw.temp_inv_data
;

create temporary table csx_dw.temp_inv_data as
select
	province_code,
	province_name,
	city_name,
	bd_id        ,
	bd_name      ,
	count(distinct
	case
		when (
				sale      !=0
				or inv_qty!=0
			)
			then goods_code
	end ) sku,
		count(distinct
	case
		when (
				sale      !=0
				or inv_qty>=1
			)
			then goods_code
	end ) inv_sku,
	count(distinct
	case
		when (
				sale!=0
			)
			then goods_code
	end )       sale_sku ,
	sum(inv_qty)inv_qty  ,
	sum(amt)    inv_amt
from
	(
		select
			dc_code               ,
			dc_name               ,
			bd_id                 ,
			bd_name               ,
			a.goods_code          ,
			sum(a.sales_value)sale,
			sum
				(
					case
						when sdt=regexp_replace(${hiveconf:edate},'-','')
							then inventory_qty
					end
				)
			inv_qty,
			sum
				(
					case
						when sdt=regexp_replace(${hiveconf:edate},'-','')
							then inventory_amt
					end
				)
			amt
		from
			csx_dw.dc_sale_inventory a
		where
			sdt    <=regexp_replace(${hiveconf:edate},'-','')
			and sdt>=regexp_replace(${hiveconf:sdate},'-','')
		group by
			dc_code,
			dc_name,
			bd_id  ,
			bd_name,
			a.goods_code
	)
	a
	join
		(
			select
				case
					when location_code='W0H4'
						then location_code
						else province_code
				end province_code,
				case
					when location_code='W0H4'
						then shop_name
						else province_name
				end province_name,
				location_code    ,
				shop_name,
				a.prefecture_city_name as city_name
			from
				csx_dw.csx_shop a
				join
					(select distinct dc_code from csx_dw.dws_sale_r_d_customer_sale where sdt>=regexp_replace(${hiveconf:sdate},'-','') and sdt<=regexp_replace(${hiveconf:edate},'-','') and channel='1' and `attribute` !='合伙人')
					c
					on
						a.location_code=c.dc_code
			where
				sdt='current'
		)
		b
		on
			a.dc_code=b.location_code
group by
	province_code,
	province_name,
	city_name,
	bd_id        ,
	bd_name
;

-- 入库金额
drop table csx_dw.temp_entry_data
;

create temporary table csx_dw.temp_entry_data as
select
	province_code,
	province_name,
	city_name,
	bd_id        ,
	bd_name      ,
	entry_amt
from
	(
		select
			a.receive_location_code dc_code,
			case
				when a.division_code in ('10',
										 '11')
					then '11'
				when a.division_code in ('12',
										 '13',
										 '14')
					then '12'
					else a.division_code
			end bd_id,
			case
				when a.division_code in ('10',
										 '11')
					then '生鲜采购部'
				when a.division_code in ('12',
										 '13',
										 '14')
					then '食百采购部'
					else a.division_name
			end                       bd_name,
			sum(a.price*a.receive_qty)entry_amt
		from
			csx_dw.wms_entry_order a
		where
			sdt            <=regexp_replace(${hiveconf:edate},'-','')
			and sdt        >=regexp_replace(${hiveconf:sdate},'-','')
			and a.entry_type='采购入库'
			--and business_type='供应商配送'
		group by
			a.receive_location_code,
			case
				when a.division_code in ('10',
										 '11')
					then '11'
				when a.division_code in ('12',
										 '13',
										 '14')
					then '12'
					else a.division_code
			end ,
			case
				when a.division_code in ('10',
										 '11')
					then '生鲜采购部'
				when a.division_code in ('12',
										 '13',
										 '14')
					then '食百采购部'
					else a.division_name
			end
	)
	a
	join
		(
			select
				case
					when location_code='W0H4'
						then location_code
						else province_code
				end province_code,
				case
					when location_code='W0H4'
						then shop_name
						else province_name
				end province_name,
				location_code    ,
				shop_name,
				a.prefecture_city_name city_name
			from
				csx_dw.csx_shop a
				join
					(select distinct dc_code from csx_dw.dws_sale_r_d_customer_sale where sdt>=regexp_replace(${hiveconf:sdate},'-','') and sdt<=regexp_replace(${hiveconf:edate},'-','') and channel='1' and `attribute` !='合伙人')
					c
					on
						a.location_code=c.dc_code
			where
				sdt='current'
		)
		b
		on
			a.dc_code=b.location_code
;

-- -- 创建实体表，使用Impala 导出
-- create table csx_dw.temp_goods_sale_02 as
-- select *
-- from
-- 	csx_dw.temp_goods_sale_01
-- ;

drop table csx_dw.temp_goods_sale_02;
 create  table csx_dw.temp_goods_sale_02 as
select
	province_code        ,
	province_name        ,
	dc_city_name,
	bd_id                ,
	bd_name              ,
	is_factory_goods_name,
	goods_code           ,
	goods_name           ,
	unit                 ,
	sale_goods           ,
	sale_cust_goods      ,
	cust_no              ,
	sale                 ,
   sum(sale)over(partition by province_code,dc_city_name order by province_code,dc_city_name,sale desc rows between unbounded preceding and current row) as leijia
   ,sum(sale)over(partition by province_code,dc_city_name order by province_code,dc_city_name,sale desc rows between unbounded preceding and current row)/sum(sale)over(partition by province_code,dc_city_name) as sale_ratio
      
from
	csx_dw.temp_goods_sale_01
where
	1=1
;

-- 统计数据
select
	province_name                           ,
	city_name,
	bd_id                                   ,
	bd_name                                 ,
	sum(sale)/10000             sale        ,
	sum(sku)                    as sku      ,
	sum(inv_sku)as inv_sku,
	sum(sale_sku)               as sale_sku ,
	sum(sale_sku)/sum(sku)*1.00 as sku_rate ,
	sum(low_sales) as low_sales,
	sum(top_goods)              as top_goods,
	sum(top_sale)/10000              as top_sale ,
	sum(inv_amt)  /10000          as inv_amt  ,
	sum(entry_amt)/10000          as entry_amt
from
	(
		select
			province_code,
			province_name,
			dc_city_name as city_name,
			bd_id        ,
			bd_name      ,
			count(distinct
			case
				when (sale_ratio<=0.9 and sale_ratio>0)
					then goods_code
			end ) as top_goods,
			sum
				(
					case
						when (sale_ratio<=0.9 and sale_ratio>0)
							then sale
					end
				)
			         as top_sale ,
			sum(sale)   sale     ,
			count(distinct case when sale<5000 and sale_goods<4 then goods_code end  ) as low_sales,
			0        as entry_amt,
			0        as sku      ,
			0 as inv_sku,
			count(distinct goods_code ) as sale_sku,
			0        as inv_amt
		from
			csx_dw.temp_goods_sale_02
		group by
			province_code,
			province_name,
			dc_city_name,
			bd_id        ,
			bd_name
		union all
		select
			province_code ,
			province_name ,
			city_name,
			bd_id         ,
			bd_name       ,
			0 as top_goods,
			0 as top_sale ,
			0 as sale     ,
			0 as low_sales,
			entry_amt     ,
			0 as sku      ,
			0 as inv_sku,
			0 as sale_sku ,
			0 as inv_amt
		from
			csx_dw.temp_entry_data
		union all
		select
			province_code,
			province_name,
			city_name,
			bd_id        ,
			case
				when bd_id='11'
					then '生鲜采购部'
				when bd_id='12'
					then '食百采购部'
					else bd_name
			end    bd_name  ,
			0   as top_goods,
			0   as top_sale ,
			0   as sale     ,
			0 as low_sales,
			0   as entry_amt,
			sku             ,
			inv_sku,
			0 as sale_sku        ,
			inv_amt
		from
			csx_dw.temp_inv_data
	)
	a
group by
	province_name,
	bd_id        ,
	bd_name,city_name;
	
	
	


