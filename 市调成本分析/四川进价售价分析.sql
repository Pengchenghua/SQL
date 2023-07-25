
				

-- 临时表1：近7天销售数量、金额
drop table csx_analyse_tmp.tmp_price_pur_sale_trend_sale;
create temporary table csx_analyse_tmp.tmp_price_pur_sale_trend_sale 
as
select 
  'sale' as STYPE,
  a.province_code,
  a.province_name,
  a.city_code,
  a.city_name,
  a.location_code,
  a.location_name,
  a.goods_code,
  a.sdt,
  d.goods_name,
  d.division_name,
  d.classify_large_code,
  d.classify_large_name,
  d.classify_middle_code,
  d.classify_middle_name,
  d.purchase_group_code department_id,
  d.purchase_group_name department_name,
  sum(a.sale_qty) as sale_qty,
  sum(a.sale_qty*a.fact_price)/sum(case when a.fact_price is not null then a.sale_qty end) as fact_price, -- 原料价
  sum(a.sale_qty*a.cost_price)/sum(a.sale_qty) as cost_price, -- 成本价
  sum(a.sale_qty*a.purchase_price)/sum(a.sale_qty) as purchase_price, -- 采购价
  sum(a.sale_qty*a.sale_price)/sum(a.sale_qty) as sale_price, -- 销售价
  sum(sale_cost) as sale_cost,
  sum(profit) as profit,
  sum(a.sale_amt_no_tax) as sale_amt_no_tax,
  sum(a.sale_amt) sale_amt
from -- 销售表中大生鲜部类
(
  select 
    inventory_dc_province_code as province_code,
    inventory_dc_province_name as province_name,
	inventory_dc_city_code as city_code,
    inventory_dc_city_name as city_name,
	inventory_dc_code as location_code,
	inventory_dc_name as location_name,
	a.goods_code,
	customer_code customer_no,
	sdt,
	cast(sum(sale_qty) as decimal(20,6)) as sale_qty,
	sum(sale_qty*fact_price)/cast(sum(case when fact_price is not null then sale_qty end) as decimal(20,6)) as fact_price,  -- 原料价
	sum(sale_qty*cost_price)/cast(sum(sale_qty) as decimal(20,6)) as cost_price, -- 成本价
	sum(sale_qty*purchase_price)/cast(sum(sale_qty) as decimal(20,6)) as purchase_price, -- 采购价
	sum(sale_qty*sale_price)/cast(sum(sale_qty) as decimal(20,6)) as sale_price, -- 销售价
	sum(sale_cost) as sale_cost,
	sum(profit) as profit,
	sum(sale_amt_no_tax) as sale_amt_no_tax,
	sum(sale_amt) sale_amt	
  from 
  (select split(id, '&')[0] as credential_no,
		inventory_dc_province_code,inventory_dc_province_name,
		inventory_dc_city_code,inventory_dc_city_name,
		inventory_dc_code,inventory_dc_name,
		goods_code,customer_code,sdt,
		sale_qty,cost_price,purchase_price,sale_price,
		sale_cost,profit,sale_amt_no_tax,sale_amt
  from csx_dws.csx_dws_sale_detail_di
  where sdt >=regexp_replace(date_sub('${sdt_yes_date}',6),'-','')
     and sdt <=regexp_replace('${sdt_yes_date}','-','')
     and purchase_group_code like 'H%' 
	 and channel_code in('1','7','9') 
	 and order_channel_code not in( '4','6') -- 不含调价返利
	 and business_type_code <> '4' -- 不含城市服务商业绩
	 and refund_order_flag=0  -- 退货订单标识(0-正向单 1-逆向单)
	 and delivery_type_code<>'2'  -- 剔除直送单
	 and inventory_dc_code in('W0A6','W0A8')
	 and inventory_dc_province_name in('四川省','福建省')
  )a 
left join
  (
    select
    t2.goods_code,
    t2.credential_no,
    sum(t2.qty) as qty,
    cast(sum(t3.fact_price*t2.qty)/sum(case when t3.fact_price is not null then t2.qty end) as decimal(30,6)) fact_price
  from
  (
    select
      goods_code,
      credential_no,
      source_order_no,
      sum(qty) as qty
    from csx_dws.csx_dws_wms_batch_detail_di
    where sdt >= regexp_replace(date_sub('${sdt_yes_date}', 100), '-', '')
    and move_type_code in ('107A', '108A')
    group by goods_code, credential_no, source_order_no
    )t2
left join
    (
      select
        goods_code,
        order_code,
        cast(sum(fact_values)/sum(goods_reality_receive_qty) as decimal(30,6)) as fact_price
      from csx_dws.csx_dws_mms_factory_order_df
      where sdt >= regexp_replace(date_sub('${sdt_yes_date}', 100), '-', '') and mrp_prop_key in('3061','3010')
      group by goods_code, order_code
    )t3 on t2.source_order_no = t3.order_code and t2.goods_code = t3.goods_code
  group by t2.goods_code,t2.credential_no
  )b on a.goods_code = b.goods_code and a.credential_no = b.credential_no	
  
  group by 
    inventory_dc_province_code,inventory_dc_province_name,inventory_dc_city_code,inventory_dc_city_name,
	inventory_dc_code,inventory_dc_name,a.goods_code,customer_code,sdt
) a 
-- 商品信息
join
(
  select * from csx_dim.csx_dim_basic_goods where sdt = 'current' and classify_middle_code='B0202' -- 蔬菜
) d on d.goods_code = a.goods_code 
-- 销售表日配关联剔除直送的关联方式
join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag =0
    ) b on a.location_code = b.shop_code
	
group by a.province_code,a.province_name,a.city_code,a.city_name,
  a.location_code,a.location_name,a.goods_code,a.sdt,
  d.goods_name,d.division_name,d.classify_large_code,d.classify_large_name,d.classify_middle_code,
  d.classify_middle_name,d.purchase_group_code,d.purchase_group_name;


-- 临时表2：销售占比 各商品在该城市中类（蔬菜）销售额占比
drop table csx_analyse_tmp.tmp_price_sale_trend_sale_1;
create temporary table csx_analyse_tmp.tmp_price_sale_trend_sale_1 
as
select 
  a.goods_code,
  a.goods_name,
  a.province_code,
  a.province_name,
  -- a.city_code,
  -- a.city_name,
  a.classify_large_code,
  a.classify_large_name,
  a.department_id,
  a.department_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.sale_qty,
  a.sale_amt_no_tax,
  a.sale_amt,
  row_number() over(partition by a.province_code,a.classify_middle_code order by sale_amt desc) as sale_ranks,
  sum(sale_amt) over(partition by a.province_code,a.goods_code)/all_sale_amt as sale_amt_proportion --占比
  -- sum(sale_amt) over(partition by a.province_code,a.classify_middle_code order by sale_amt desc rows between UNBOUNDED PRECEDING and 0 PRECEDING)/all_sale_amt as sale_amt_proportion --累计占比
from
(
  select 
    goods_code,
    goods_name,
    province_code,
    province_name,
	-- city_code,
    -- city_name,
    classify_large_code,
    classify_large_name,
	classify_middle_code,
	classify_middle_name,
	department_id,
	department_name,
    sum(sale_qty) as sale_qty,
    sum(sale_amt_no_tax) as sale_amt_no_tax,
    sum(sale_amt) as sale_amt
  from csx_analyse_tmp.tmp_price_pur_sale_trend_sale
  group by goods_code,goods_name,province_code,province_name,-- city_code,city_name,
    classify_large_code,classify_large_name,classify_middle_code,
	classify_middle_name,department_id,department_name
  having sale_amt > 0
) a
join
(
  select 
    province_code,
    -- city_code,
    classify_middle_code,
    sum(sale_amt) as all_sale_amt
  from csx_analyse_tmp.tmp_price_pur_sale_trend_sale
  group by province_code,classify_middle_code 
) b on b.province_code = a.province_code and b.classify_middle_code = a.classify_middle_code
;


-- 临时表3：采购入库的数量、单价、金额
drop table csx_analyse_tmp.tmp_price_sale_trend_purchase;
create temporary table csx_analyse_tmp.tmp_price_sale_trend_purchase 
as
select 
  'purchase' as STYPE,
  c.province_code,
  c.province_name,
  c.city_code,
  c.city_name,
  location_code,
  c.shop_name,
  a.goods_code,
  order_code,
  a.receive_date as sdt,
  d.goods_name,
  d.division_name,
  d.classify_large_code,
  d.classify_large_name,
  d.classify_middle_code,
  d.classify_middle_name,
  d.purchase_group_code department_id,
  d.purchase_group_name department_name,
  receive_qty,
  receive_price,
  receive_amt
from
(
  select 
    order_code, 
	goods_code,
	receive_dc_code as location_code,
	regexp_replace(to_date(receive_time),'-','') as receive_date,
    sum(receive_qty) as receive_qty,
	sum(receive_qty*price)/sum(receive_qty) as receive_price,
	sum(receive_qty*price) as receive_amt
  from csx_dws.csx_dws_wms_entry_detail_di
  where sdt >=regexp_replace(date_sub('${sdt_yes_date}',6),'-','')
    and regexp_replace(to_date(receive_time),'-','') >=regexp_replace(date_sub('${sdt_yes_date}',6),'-','')
	and regexp_replace(to_date(receive_time),'-','') <=regexp_replace('${sdt_yes_date}','-','')
	and return_flag <> 'Y'   -- 不含退货
	and receive_status <> 0     -- 收货状态 0-待收货 1-收货中 2-已关单
	and entry_type like 'P%'  -- 订单类型
	and receive_qty > 0
	and purpose <> '09'  -- 不含城市服务商
	 and receive_dc_code in('W0A6','W0A8')
	 -- and business_type_code<>'03'  -- 直送
  group by order_code,goods_code,receive_dc_code,regexp_replace(to_date(receive_time),'-','')
) a
join
(
  select 
    shop_code,
    shop_name,
    province_code,
    province_name,
	city_code,
    city_name
  from csx_dim.csx_dim_shop
  where sdt='current'
  and province_name in('四川省','福建省')
) c on a.location_code = c.shop_code
join
(
  select * from csx_dim.csx_dim_basic_goods 
  where sdt='current' and classify_middle_code='B0202' -- 蔬菜
) d on d.goods_code = a.goods_code
-- 关联剔除直送的关联方式
join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag =0
    ) b on a.location_code = b.shop_code;




-- 临时表4：采购入库占比 各商品在省区中类入库金额占比
drop table csx_analyse_tmp.tmp_price_sale_trend_purchase_1;
create temporary table csx_analyse_tmp.tmp_price_sale_trend_purchase_1 
as
select 
  a.goods_code,
  a.goods_name,
  a.province_code,
  a.province_name,
  -- a.city_code,
  -- a.city_name,
  a.classify_large_code,
  a.classify_large_name,
  a.department_id,
  a.department_name,
  a.classify_middle_code,
  a.classify_middle_name,
  a.receive_qty,
  a.receive_amt,
  row_number() over(partition by a.province_code,a.classify_middle_code order by receive_amt desc) as receive_ranks,
  sum(receive_amt)over(partition by a.province_code,a.goods_code)/all_receive_amt receive_amt_proportion  -- 占比
  -- sum(receive_amt)over(partition by a.province_code,a.classify_middle_code order by receive_amt desc rows between UNBOUNDED PRECEDING and 0 PRECEDING)/all_receive_amt receive_amt_proportion  -- 累计占比
from
(
  select 
    province_code,
    province_name,
    goods_code,
    goods_name,
	-- city_code,
    -- city_name,
    classify_large_code,
    classify_large_name,
    department_id,
    department_name,	
    classify_middle_code,
    classify_middle_name,
    sum(receive_qty) as receive_qty,
    sum(receive_amt) as receive_amt
  from csx_analyse_tmp.tmp_price_sale_trend_purchase
  group by goods_code,goods_name,province_code,province_name,
    classify_large_code,classify_large_name,department_id,department_name,classify_middle_code,classify_middle_name 
) a
join
(
  select 
    province_code,
    -- city_code,
    classify_middle_code,
    sum(receive_amt) as all_receive_amt
   from csx_analyse_tmp.tmp_price_sale_trend_purchase
   group by province_code,classify_middle_code 
) c on c.province_code = a.province_code and c.classify_middle_code = a.classify_middle_code;






drop table csx_analyse_tmp.tmp_price_trend_purchase_sale_detail;
create temporary table csx_analyse_tmp.tmp_price_trend_purchase_sale_detail 
as
select 
	a.province_name,
	a.city_name,
	a.location_code,
	a.location_name,
	a.goods_code,
	a.goods_name,
	a.classify_middle_code,
	a.classify_middle_name,
	c.classify_small_code,
	c.classify_small_name,	
	a.sdt,
	a.receive_qty,
	a.receive_price,
	a.receive_amt,
	a.sale_qty,
	a.fact_price, -- 原料价
	a.cost_price, -- 成本价
	a.purchase_price, -- 采购价
	a.sale_price, -- 销售价
	a.sale_cost,
	a.profit,
	a.sale_amt,
	b.sale_amt as sale_amt_all,
	b.sale_ranks,
	b.sale_amt_proportion,
	d.receive_amt as receive_amt_all,
	d.receive_ranks,
	d.receive_amt_proportion
from
(
  select 
    a.province_code,
	a.province_name,
	-- a.city_code,
	a.city_name,
	a.location_code,
	a.location_name,
	a.goods_code,
	a.goods_name,
	-- a.classify_large_code,
	-- a.classify_large_name,
	-- a.department_id,
	-- a.department_name,
	a.classify_middle_code,
	a.classify_middle_name,
	a.sdt,
	sum(a.receive_qty) as receive_qty,
	sum(a.receive_amt)/sum(a.receive_qty) as receive_price,
	sum(a.receive_amt) as receive_amt,
	cast(sum(a.sale_qty) as decimal(20,6)) as sale_qty,
	sum(a.sale_qty*fact_price)/cast(sum(case when fact_price is not null then a.sale_qty end) as decimal(20,6)) as fact_price, -- 原料价
	sum(a.sale_qty*cost_price)/cast(sum(a.sale_qty) as decimal(20,6)) as cost_price, -- 成本价
	sum(a.sale_qty*purchase_price)/cast(sum(a.sale_qty) as decimal(20,6)) as purchase_price, -- 采购价
	sum(a.sale_qty*sale_price)/cast(sum(a.sale_qty) as decimal(20,6)) as sale_price, -- 销售价
	sum(a.sale_cost) sale_cost,
	sum(a.profit) as profit,
	sum(a.sale_amt) sale_amt
  from -- 销售价格、数量、金额
  (
    select 
      province_code,
	  province_name,
	  city_code,
	  city_name,
	  location_code,
	  location_name,
	  goods_code,
	  goods_name,
	  classify_large_code,
	  classify_large_name,
	  department_id,
	  department_name,
	  classify_middle_code,
	  classify_middle_name,
	  sdt,
	  cast('' as int) receive_qty,
	  cast('' as int) receive_price,
	  cast('' as int) receive_amt,
	  sale_qty,
	  fact_price, -- 原料价
	  cost_price,
	  purchase_price,
	  sale_price,
	  sale_cost,
	  profit,
	  sale_amt
    from csx_analyse_tmp.tmp_price_pur_sale_trend_sale
      
	union all -- 采购价格、数量、金额

    select 
      province_code,
	  province_name,
	  city_code,
	  city_name,
	  location_code,
	  shop_name as location_name,
	  goods_code,
	  goods_name,
	  classify_large_code,
	  classify_large_name,
	  department_id,
	  department_name,
	  classify_middle_code,
	  classify_middle_name,
      sdt,
      receive_qty,
      receive_price,
      receive_amt,
      cast('' as int) sale_qty,
	  cast('' as int) fact_price, -- 原料价
      cast('' as int) cost_price,
      cast('' as int) purchase_price,
      cast('' as int) sale_price,
	  cast('' as int) sale_cost,
	  cast('' as int) profit,
      cast('' as int) sale_amt
    from csx_analyse_tmp.tmp_price_sale_trend_purchase
  ) a 
group by
    a.province_code,
	a.province_name,
	-- a.city_code,
	a.city_name,
	a.location_code,
	a.location_name,
	a.goods_code,
	a.goods_name,
	-- a.classify_large_code,
	-- a.classify_large_name,
	-- a.department_id,
	-- a.department_name,
	a.classify_middle_code,
	a.classify_middle_name,
	a.sdt
) a 
-- 商品销售占比与排名
left join
(
  select 
    goods_code,
    goods_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    department_id,
    department_name,
    classify_middle_code,
    classify_middle_name,
    sale_qty,
    sale_amt_no_tax,
    sale_amt,
    sale_ranks,
    sale_amt_proportion
  from csx_analyse_tmp.tmp_price_sale_trend_sale_1
) b on a.province_code = b.province_code and a.goods_code = b.goods_code
-- 商品采购入库占比与排名
left join
(
  select 
    goods_code,
    goods_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    department_id,
    department_name,
    classify_middle_code,
    classify_middle_name,
    receive_qty,
    receive_amt,
    receive_ranks,
    receive_amt_proportion  
  from csx_analyse_tmp.tmp_price_sale_trend_purchase_1
) d on d.goods_code = a.goods_code and d.province_code = a.province_code
-- 商品信息
left join
(
  select * from csx_dim.csx_dim_basic_goods where sdt = 'current' and classify_middle_code='B0202' -- 蔬菜
) c on c.goods_code = a.goods_code ;




drop table csx_analyse_tmp.tmp_price_trend_purchase_sale_jh;
create temporary table csx_analyse_tmp.tmp_price_trend_purchase_sale_jh 
as
select a.*,
b.receive_amt_sc,
b.receive_amt_fj,
b.sale_amt_sc,
b.sale_amt_fj
from 
(
select 
	province_name,
	goods_code,
	goods_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(receive_qty) as receive_qty,
	sum(receive_amt)/sum(receive_qty) as receive_price,
	sum(receive_amt) as receive_amt,
	cast(sum(sale_qty) as decimal(20,6)) as sale_qty,
	sum(sale_qty*fact_price)/cast(sum(case when fact_price is not null then sale_qty end) as decimal(20,6)) as fact_price, -- 原料价
	sum(sale_qty*cost_price)/cast(sum(sale_qty) as decimal(20,6)) as cost_price, -- 成本价
	sum(sale_qty*purchase_price)/cast(sum(sale_qty) as decimal(20,6)) as purchase_price, -- 采购价
	sum(sale_qty*sale_price)/cast(sum(sale_qty) as decimal(20,6)) as sale_price, -- 销售价
	sum(sale_cost) sale_cost,
	sum(profit) profit,
	sum(sale_amt) sale_amt,

	max(sale_amt_all) sale_amt_all,
	max(sale_ranks) sale_ranks,
	max(sale_amt_proportion) sale_amt_proportion,
	max(receive_amt_all) receive_amt_all,
	max(receive_ranks) receive_ranks,
	max(receive_amt_proportion) receive_amt_proportion
from csx_analyse_tmp.tmp_price_trend_purchase_sale_detail
where province_name in('四川省','福建省')
group by 
	province_name,
	goods_code,
	goods_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
)a 
left join 
(
select goods_code, 
sum(case when province_name='四川省' then receive_amt end) receive_amt_sc,
sum(case when province_name='福建省' then receive_amt end) receive_amt_fj,
sum(case when province_name='四川省' then sale_amt end) sale_amt_sc,
sum(case when province_name='福建省' then sale_amt end) sale_amt_fj
from csx_analyse_tmp.tmp_price_trend_purchase_sale_detail
where province_name in('四川省','福建省')
group by goods_code
)b on a.goods_code=b.goods_code;



------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
-- 
select
  a.inventory_dc_province_name,
  a.inventory_dc_city_name,
  a.inventory_dc_code,
  a.inventory_dc_name,
  a.customer_code,a.customer_name,
  a.sdt,
  -- a.business_type_code,
  a.goods_code,
  d.goods_name,
  -- d.classify_large_code,
  -- d.classify_large_name,
  d.classify_middle_code,
  d.classify_middle_name,
  d.classify_small_code,
  d.classify_small_name,
  sale_qty,cost_price,purchase_price,sale_price,
  sale_cost,profit,sale_amt,
  case coalesce(b1.price_type,b2.price_type,b3.price_type,b4.price_type) 
	when 1 then '建议定价'
	when 2 then '对标对象'
	when 3 then '销售成本价'
	when 4 then '上一周价格'
	when 5 then '售价'
	when 6 then '采购/库存成本'
	else '其它' end as price_type_name,  -- 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
  
  case coalesce(b1.bmk_type,b2.bmk_type,b3.bmk_type,b4.bmk_type)
	when 0 then '永辉门店'
	when 1 then '网站'
	when 2 then '市场'
	when 3 then '终端' end as bmk_type,  -- 对标类型(0 - 永辉门店 1 - 网站 2 - 市场 3-终端)
  
  coalesce(b1.bmk_code,b2.bmk_code,b3.bmk_code,b4.bmk_code) as bmk_code,  -- 对标对象编码
  coalesce(b1.bmk_name,b2.bmk_name,b3.bmk_name,b4.bmk_name) as bmk_name,  -- 对标对象名称(描述)
  case c.price_period_code
    when 1 then '每天'
	when 2 then '每周'
	when 3 then '每半月'
	when 4 then '每月' 
    else '其它' end price_period_name
from
(
	select split(id, '&')[0] as credential_no,
		customer_code,customer_name,
		inventory_dc_province_code,inventory_dc_province_name,
		inventory_dc_city_code,inventory_dc_city_name,
		inventory_dc_code,inventory_dc_name,
		goods_code,
		classify_large_code,	-- 管理大类编号
		classify_middle_code,	-- 管理中类编号
		classify_small_code,	-- 管理小类编号
		sdt,business_type_code,
		sale_qty,cost_price,purchase_price,sale_price,
		sale_cost,profit,sale_amt_no_tax,sale_amt
  from csx_dws.csx_dws_sale_detail_di
  where sdt >=regexp_replace(date_sub('${sdt_yes_date}',6),'-','')
     and sdt <=regexp_replace('${sdt_yes_date}','-','')
     and purchase_group_code like 'H%' 
	 and channel_code in('1','7','9') 
	 and order_channel_code not in( '4','6') -- 不含调价返利
	 and business_type_code <> '4' -- 不含城市服务商业绩
	 and refund_order_flag=0  -- 退货订单标识(0-正向单 1-逆向单)
	 and delivery_type_code<>'2'  -- 剔除直送单
	 and inventory_dc_code in('W0A6','W0A8')
	 and inventory_dc_province_name in('四川省','福建省')
)a 
-- 报价策略 商品
left join 
(
  select 
    warehouse_code,
    customer_code,
    price_type,  -- 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
    addition_rate,  -- 加成系数
    bmk_type,  -- 对标类型(0 - 永辉门店 1 - 网站 2 - 市场 3-终端)
    bmk_code,  -- 对标对象编码
    bmk_name,  -- 对标对象名称(描述)
    dimension_value_code,  -- 商品或分类编码
    dimension_value_name,  -- 商品或分类名称
    dimension_type,  -- 商品 =0 小类 =1 中类 =2 大类 =3
    float_up_rate,  -- 售价类型:上浮点数
    float_down_rate,  -- 售价类型:下浮点数
    suggest_price_type,  -- 建议售价类型: 1-高;2:中;3:低
    is_fix_price  -- 是否固定价(1-固定价)	
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df  -- 报价策略配置表
  where sdt=regexp_replace(date_sub(current_date, 1),'-','') 
  and dimension_type = 0 --商品
) b1 on b1.customer_code = a.customer_code and b1.warehouse_code = a.inventory_dc_code 
	and b1.dimension_value_code = a.goods_code
-- 报价策略 小类
left join 
(
  select 
    warehouse_code,
    customer_code,
    price_type,  -- 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
    addition_rate,  -- 加成系数
    bmk_type,  -- 对标类型(0 - 永辉门店 1 - 网站 2 - 市场 3-终端)
    bmk_code,  -- 对标对象编码
    bmk_name,  -- 对标对象名称(描述)
    dimension_value_code,  -- 商品或分类编码
    dimension_value_name,  -- 商品或分类名称
	small_management_classify_code,  -- 管理品类-小类编码
    dimension_type,  -- 商品 =0 小类 =1 中类 =2 大类 =3
    float_up_rate,  -- 售价类型:上浮点数
    float_down_rate,  -- 售价类型:下浮点数
    suggest_price_type,  -- 建议售价类型: 1-高;2:中;3:低
    is_fix_price  -- 是否固定价(1-固定价)	
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df  -- 报价策略配置表
  where sdt=regexp_replace(date_sub(current_date, 1),'-','') 
  and dimension_type = 1 --小类
  and dimension_value_code like'B0202%'
) b2 on b2.customer_code = a.customer_code and b2.warehouse_code = a.inventory_dc_code 
	and b2.small_management_classify_code = a.classify_small_code
-- 报价策略 中类
left join 
(
  select 
    warehouse_code,
    customer_code,
    price_type,  -- 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
    addition_rate,  -- 加成系数
    bmk_type,  -- 对标类型(0 - 永辉门店 1 - 网站 2 - 市场 3-终端)
    bmk_code,  -- 对标对象编码
    bmk_name,  -- 对标对象名称(描述)
    dimension_value_code,  -- 商品或分类编码
    dimension_value_name,  -- 商品或分类名称
    dimension_type,  -- 商品 =0 小类 =1 中类 =2 大类 =3
    float_up_rate,  -- 售价类型:上浮点数
    float_down_rate,  -- 售价类型:下浮点数
    suggest_price_type,  -- 建议售价类型: 1-高;2:中;3:低
    is_fix_price  -- 是否固定价(1-固定价)	
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df  -- 报价策略配置表
  where sdt=regexp_replace(date_sub(current_date, 1),'-','') 
  and dimension_type = 2 --中类
  and dimension_value_code='B0202'	-- 蔬菜
) b3 on b3.customer_code = a.customer_code and b3.warehouse_code = a.inventory_dc_code 
-- 报价策略 大类
left join 
(
  select 
    warehouse_code,
    customer_code,
    price_type,  -- 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
    addition_rate,  -- 加成系数
    bmk_type,  -- 对标类型(0 - 永辉门店 1 - 网站 2 - 市场 3-终端)
    bmk_code,  -- 对标对象编码
    bmk_name,  -- 对标对象名称(描述)
    dimension_value_code,  -- 商品或分类编码
    dimension_value_name,  -- 商品或分类名称
    dimension_type,  -- 商品 =0 小类 =1 中类 =2 大类 =3
    float_up_rate,  -- 售价类型:上浮点数
    float_down_rate,  -- 售价类型:下浮点数
    suggest_price_type,  -- 建议售价类型: 1-高;2:中;3:低
    is_fix_price  -- 是否固定价(1-固定价)	
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df  -- 报价策略配置表
  where sdt=regexp_replace(date_sub(current_date, 1),'-','') 
  and dimension_type = 3 --大类
  and dimension_value_code='B02' -- 蔬菜水果
) b4 on b4.customer_code = a.customer_code and b4.warehouse_code = a.inventory_dc_code 
-- 报价周期
left join
(
  select 
    customer_id,
	customer_code,
    business_type_code,
    price_period_code
  from 
  (
    select
      customer_id,
	  customer_code,
      case when business_attribute_code = '1' then '1'
       when business_attribute_code = '2' then '2'
  	   when business_attribute_code = '3' then '5'
  	   when business_attribute_code = '4' then '9'
  	   when business_attribute_code = '5' then '6'
  	   when business_attribute_code = '6' then '3'
      end as business_type_code,
      price_period_code,
      row_number() over(partition by customer_id,business_attribute_code order by create_time desc) as ranks
    from csx_dim.csx_dim_crm_business_info
    where sdt = regexp_replace(date_sub(current_date, 1),'-','') 
	and business_stage = 5 
	and `status` = 1 
	-- 限制日配
	and business_attribute_code = '1'	
  )a 
  where ranks = 1 and business_type_code is not null
) c on a.customer_code = c.customer_code and a.business_type_code = c.business_type_code
-- 商品信息
join
(
  select * from csx_dim.csx_dim_basic_goods where sdt = 'current' and classify_middle_code='B0202' -- 蔬菜
) d on d.goods_code = a.goods_code 