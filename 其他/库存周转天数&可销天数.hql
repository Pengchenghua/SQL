--hive
drop table if exists b2b_tmp.p_csx_ivn
;

drop table if exists b2b_tmp.p_csx_sale
;

drop table if exists b2b_tmp.p_csx_order
;

set sdate  =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','');
set sdt_yc =regexp_replace(to_date(date_sub(trunc(date_sub(current_timestamp(),1),'MM'),1)),'-','');
set sdt_30d=regexp_replace(to_date(date_sub(current_timestamp(),31)),'-','');
--取销售\库存\入库\
create temporary table if not exists b2b_tmp.p_csx_sale as
select
	a.shop_id                          ,
	a.goodsid                          ,
	a.catg_s_id                        ,
	sum(qty)              qty          ,
	sum(sale)             sale         ,
	sum(profit)           profit       ,
	sum(sale_30day)       sale_30day   ,
	sum(qty_30day)        qty_30day    ,
	sum(pur_qty_in)       pur_qty_in   ,
	sum(pur_val_in)       pur_val_in   ,
	sum(pur_qty_out)      pur_qty_out  ,
	sum(pur_val_out)      pur_val_out  ,
	sum(inv_amt)          inv_amt      ,
	sum(inv_qty)          inv_qty      ,
	sum(begin_inv_qty) AS begin_inv_qty,
	sum(begin_inv_amt) as begin_inv_amt,
	sum(end_inv_qty)   AS end_inv_qty  ,
	sum(end_inv_amt)   as end_inv_amt
from
	(
		select
			a.shop_id                ,
			a.goodsid                ,
			a.catg_s_id              ,
			sum(qty)             qty    ,
			sum(tax_salevalue)   sale   ,
			sum(a.tax_profit)    profit ,
			0                    sale_30day,
			0 qty_30day,
			0 pur_qty_in,
			0 pur_val_in,
			0 pur_qty_out,
			0 pur_val_out,
			0 inv_amt,
			0 inv_qty,
			0                 AS begin_inv_qty,
			0                 as begin_inv_amt,
			0                 AS end_inv_qty  ,
			0                 as end_inv_amt
		from
			csx_ods.sale_b2b_dtl_fct a
			left join
				(
					select
						concat('S',shop_id)cust_id
					from
						dim.dim_shop
					where
						edate             ='9999-12-31'
						and sales_dist_new='610000'
				)
				c
				on
					a.cust_id=c.cust_id
		where
			c.cust_id is null
			and a.sdt      <=${hiveconf:sdate}
			and a.sdt       >${hiveconf:sdt_yc}
			and a.sflag    !='md'
		group by
			a.shop_id  ,
			a.catg_s_id,
			a.goodsid
		union all
		select
			a.shop_id,
			a.goodsid,
			a.catg_s_id,0        qty,0 sale,0 profit,
			sum(tax_salevalue)   sale_30day,
			sum(a.qty)           qty_30day ,
			0                    pur_qty_in,0 pur_val_in,0 pur_qty_out,0 pur_val_out,
			0                    inv_amt,0 inv_qty,
			0                 AS begin_inv_qty,
			0                 as begin_inv_amt,
			0                 AS end_inv_qty  ,
			0                 as end_inv_amt
		from
			csx_ods.sale_b2b_dtl_fct a
		where
			a.sdt       <=${hiveconf:sdate}
			and a.sdt   >=${hiveconf:sdt_30d}
			and a.sflag !='md'
		group by
			a.shop_id  ,
			a.catg_s_id,
			a.goodsid
		union all
		--入库
		SELECT
			shop_id as shop_id,
			goodsid           ,
			a.goods_catg as catg_s_id ,0 qty,0 sale,0 profit,0 sale_30day,
			0               qty_30day,
			sum
				(
					case
						when a.ordertype in ('配送'  ,
											 '直送'  ,
											 '直通'  ,
											 '货到即配',
											 'UD'  ,
											 '返配申偿')
							then a.pur_qty_in
						when a.ordertype in ('退货')
							then ( a.pur_qty_in*-1.00)
					end
				)
			pur_qty_in,
			sum
				(
					case
						when a.ordertype in ('配送'  ,
											 '直送'  ,
											 '直通'  ,
											 '货到即配',
											 'UD'  ,
											 '返配申偿')
							then a.tax_pur_val_in
						when a.ordertype in ('退货')
							then ( a.tax_pur_val_in*-1.00)
					end
				)
			pur_val_in,
			sum
				(
					case
						when a.ordertype in ('退货',
											 '返配',
											 '配送申偿')
							then a.pur_qty_out
					end
				)
			pur_qty_out,
			sum
				(
					case
						when a.ordertype in ('退货',
											 '返配',
											 '配送申偿')
							then a.tax_pur_val_out
					end
				)
			     pur_val_out,
			0    inv_amt,0 inv_qty,
			0 AS begin_inv_qty,
			0 as begin_inv_amt,
			0 AS end_inv_qty  ,
			0 as end_inv_amt
		FROM
			b2b.ord_orderflow_t a
			join
				(
					select
						a.shop_id
					from
						dim.dim_shop a
					where
						a.sales_dist_new='610000'
						and edate       ='9999-12-31'
				)
				b
				on
					a.shop_id=b.shop_id
		where
			pur_order_qty            <>0
			and pur_order_total_value<>0
			and sdt                  <=${hiveconf:sdate}
			and sdt                   >${hiveconf:sdt_yc}
		group by
			a.shop_id,
			a.goodsid,
			goods_catg
		--库存
		union all
		SELECT
			a.shop_id,
			a.goodsid,
			catg_s_id,
			0 qty,0 sale,0 profit ,
			0 sale_30day,0 qty_30day,0 pur_qty_in,0 pur_val_in,0 pur_qty_out,0 pur_val_out,
			sum
				(
					case
						when a.sdt  <=${hiveconf:sdate}
							and a.sdt>${hiveconf:sdt_yc}
							then inv_amt
					end
				)
			inv_amt,
			sum
				(
					case
						when a.sdt  <=${hiveconf:sdate}
							and a.sdt>${hiveconf:sdt_yc}
							then inv_qty
					end
				)
			inv_qty,
			sum
				(
					case
						WHEN sdt=${hiveconf:sdt_yc}
							THEN inv_qty
					end
				)
			AS begin_inv_qty,
			sum
				(
					case
						WHEN sdt=${hiveconf:sdt_yc}
							THEN inv_amt
					END
				)
			as begin_inv_amt,
			sum
				(
					case
						WHEN sdt=${hiveconf:sdate}
							THEN inv_qty
					end
				)
			AS end_inv_qty ,
			sum
				(
					case
						WHEN sdt=${hiveconf:sdate}
							THEN inv_amt
					END
				)
			as end_inv_amt
		FROM
			csx_dw.inv_sap_setl_dly_fct a
		where
			a.sdt    <=${hiveconf:sdate}
			and a.sdt>=${hiveconf:sdt_yc}
			and inv_place not in ('B997',
								  'B999')
		GROUP BY
			a.shop_id,
			catg_s_id,
			a.goodsid
	)
	a
group by
	a.shop_id,
	a.goodsid,
	a.catg_s_id
;

--插入单品级
drop table if exists b2b_tmp.p_csx_goods
;

create temporary table if not exists b2b_tmp.p_csx_goods as
select
	${hiveconf:sdate} calday,
	channelsale             ,
	type                    ,
	prov_code               ,
	prov_name               ,
	a.shop_id               ,
	shop_name               ,
	a.goodsid               ,
	goodsname               ,
	bd_id                   ,
	bd_name                 ,
	firm_id                 ,
	firm_name               ,
	dept_id                 ,
	dept_name               ,
	catg_l_id               ,
	catg_l_name             ,
	catg_m_id               ,
	catg_m_name             ,
	a.catg_s_id             ,
	catg_s_name             ,
	sum(qty)                                       qty             ,
	sum(sale)*1.0                                  sale            ,
	sum(a.profit)                                  profit          ,
	(sum(sale)-sum(a.profit))*1.0                  as cost            ,
	sum(a.sale_30day)                                 sale_30day      ,
	sum(a.sale_30day)*1.0/30.00                       avg_sale_30day  ,
	sum(qty_30day)                                    qty_30day          ,
	sum(qty_30day)*1.0/30.00                          avg_qty_30day      ,
	sum(inv_amt)                                      inv_amt            ,
	sum(inv_qty)                                      inv_qty            ,
	sum(pur_qty_in)                                   pur_qty_in         ,
	sum(pur_val_in)                                   pur_val_in         ,
	sum(pur_qty_out)                                  pur_qty_out        ,
	sum(pur_val_out)                                  pur_val_out        ,
	sum(begin_inv_qty)                                begin_inv_qty      ,
	sum(begin_inv_amt)                                begin_inv_amt      ,
	sum(end_inv_qty)                                  end_inv_qty        ,
	sum(end_inv_amt)*1.0                              end_inv_amt        ,
	sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as turnover_days      ,
	sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       sale_days
from
	b2b_tmp.p_csx_sale a
	join
		(
			select
				shop_id  ,
				shop_name,
				prov_code,
				prov_name,
				case
					when a.shop_belong='27'
						then 'B端'
						else 'M端'
				end channelsale,
				case
					when region_id_new='WL01'
						AND shop_id  !='W0B6'
						THEN '物流'
					when shop_id='W0B6'
						then 'BBC'
						ELSE '门店端'
				end type
			from
				dim.dim_shop a
			where
				edate             ='9999-12-31'
				and sales_dist_new='610000'
		)
		c
		on
			a.shop_id=c.shop_id
	join
		(
			select
				goodsid    ,
				goodsname  ,
				div_id     ,
				div_name   ,
				catg_l_id  ,
				catg_l_name,
				catg_m_id  ,
				catg_m_name,
				catg_s_id  ,
				catg_s_name,
				case
					when bd_id=''
						then '13'
						else a.bd_id
				end bd_id,
				case
					when bd_name=''
						then '其他'
						else a.bd_name
				end bd_name,
				dept_id    ,
				dept_name  ,
				case
					when firm_g1_id =''
						then a.div_id
						else firm_g1_id
				end firm_id,
				case
					when firm_g1_name =''
						then a.div_name
						else firm_g1_name
				end firm_name
			from
				dim.dim_goods a
			where
				edate='9999-12-31'
		)
		d
		on
			a.goodsid      =d.goodsid
			and a.catg_s_id=d.catg_s_id
group by
	channelsale,
	type       ,
	prov_code  ,
	prov_name  ,
	a.shop_id  ,
	shop_name  ,
	a.goodsid  ,
	goodsname  ,
	catg_l_id  ,
	catg_l_name,
	catg_m_id  ,
	catg_m_name,
	a.catg_s_id,
	catg_s_name,
	bd_id      ,
	bd_name    ,
	dept_id    ,
	dept_name  ,
	firm_id    ,
	firm_name
;

set hive.exec.parallel              =true;
set hive.exec.dynamic.partition     =true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.csx_turn_days_goods partition
	(sdt
	)
select *
	,
	calday sdt
from
	b2b_tmp.p_csx_goods
;

--商行级
insert overwrite table csx_dw.csx_turn_days_firm partition
	(sdt
	)
select
	${hiveconf:sdate} calday,
	layer                   ,
	type                    ,
	prov_code               ,
	prov_name               ,
	firm_id                 ,
	firm_name               ,
	sum(qty)                                       qty             ,
	sum(sale)*1.0                                  sale            ,
	sum(a.profit)                                  profit          ,
	(sum(sale)-sum(a.profit))*1.0                  as cost            ,
	sum(a.sale_30day)                                 sale_30day      ,
	sum(a.sale_30day)*1.0/30.00                       avg_sale_30day  ,
	sum(qty_30day)                                    qty_30day       ,
	sum(qty_30day)*1.0/30.00                          avg_qty_30day   ,
	sum(inv_amt)                                      inv_amt         ,
	sum(inv_qty)                                      inv_qty         ,
	sum(pur_qty_in)                                   pur_qty_in      ,
	sum(pur_val_in)                                   pur_val_in      ,
	sum(pur_qty_out)                                  pur_qty_out     ,
	sum(pur_val_out)                                  pur_val_out     ,
	sum(begin_inv_qty)                                begin_inv_qty   ,
	sum(begin_inv_amt)                                begin_inv_amt   ,
	sum(end_inv_qty)                                  end_inv_qty     ,
	sum(end_inv_amt)*1.0                              end_inv_amt     ,
	sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as turnover_days   ,
	sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       sale_days       ,
	count(distinct
	case
		when sale<>0
			then goodsid
	end) action_sku,
	count(distinct
	case
		when end_inv_amt<>0
			then goodsid
	end)              inv_cn,
	${hiveconf:sdate} sdt
from
	(
		select
			'1'layer                                       ,
			type                                           ,
			prov_code                                      ,
			prov_name                                      ,
			firm_id                                        ,
			firm_name                                      ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			prov_code,
			prov_name,
			firm_id  ,
			goodsid  ,
			firm_name,
			type
		union all
		select
			'2'layer                                       ,
			type                                           ,
			prov_code                                      ,
			prov_name                                      ,
			''firm_id                                      ,
			''firm_name                                    ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			prov_code,
			prov_name,
			goodsid  ,
			type
		union all
		select
			'3'layer                                       ,
			type                                           ,
			''prov_code                                    ,
			''prov_name                                    ,
			firm_id                                        ,
			firm_name                                      ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			firm_id  ,
			firm_name,
			goodsid  ,
			type
		union all
		select
			'4'layer                                       ,
			type                                           ,
			'900000'prov_code                              ,
			'全国'    prov_name                              ,
			''      firm_id                                ,
			'合计'    firm_name                              ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			type,
			a.goodsid
		--合计
		union all
		select
			'1' layer                                      ,
			'全国'type                                       ,
			prov_code                                      ,
			prov_name                                      ,
			firm_id                                        ,
			firm_name                                      ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			prov_code,
			prov_name,
			firm_id  ,
			goodsid  ,
			firm_name
		union all
		select
			'2'  layer                                     ,
			'全国' type                                      ,
			prov_code                                      ,
			prov_name                                      ,
			''firm_id                                      ,
			''firm_name                                    ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			prov_code,
			prov_name,
			goodsid
		union all
		select
			'3' layer                                      ,
			'全国'type                                       ,
			''  prov_code                                  ,
			''  prov_name                                  ,
			firm_id                                        ,
			firm_name                                      ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			firm_id  ,
			firm_name,
			goodsid
		union all
		select
			'3' layer    ,
			'全国'type     ,
			''  prov_code,
			''  prov_name,
			case
				when a.bd_id='11'
					then 'A111'
				when a.bd_id='12'
					then 'B122'
				when a.bd_id='13'
					then 'C133'
					else a.bd_id
			end       firm_id                              ,
			a.bd_name firm_name                            ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			bd_id  ,
			bd_name,
			goodsid
		union all
		select
			'4'     layer                                  ,
			'全国'    type                                   ,
			'900000'prov_code                              ,
			'全国'    prov_name                              ,
			''      firm_id                                ,
			'合计'    firm_name                              ,
			a.goodsid                                      ,
			sum(qty)                                   qty           ,
			sum(sale)                                  sale          ,
			sum(a.profit)                              profit        ,
			(sum(sale)-sum(a.profit))*1.0              as cost          ,
			sum(a.sale_30day)                             sale_30day    ,
			sum(a.sale_30day)*1.0/30.00                   avg_sale_30day,
			sum(qty_30day)                                qty_30day     ,
			sum(qty_30day)*1.0/30.00                      avg_qty_30day ,
			sum(inv_amt)                                  inv_amt       ,
			sum(inv_qty)                                  inv_qty       ,
			sum(pur_qty_in)                               pur_qty_in    ,
			sum(pur_val_in)                               pur_val_in    ,
			sum(pur_qty_out)                              pur_qty_out   ,
			sum(pur_val_out)                              pur_val_out   ,
			sum(begin_inv_qty)                            begin_inv_qty ,
			sum(begin_inv_amt)                            begin_inv_amt ,
			sum(end_inv_qty)                              end_inv_qty   ,
			sum(end_inv_amt)                              end_inv_amt   ,
			sum(inv_amt)    /(sum(sale)-sum(a.profit))*1.0 as days_inv      ,
			sum(end_inv_qty)/(sum(qty_30day)*1.0/30.00)       days_sale
		from
			b2b_tmp.p_csx_goods a
		group by
			goodsid
	)
	a
group by
	layer    ,
	type     ,
	prov_code,
	prov_name,
	firm_id  ,
	firm_name
;
