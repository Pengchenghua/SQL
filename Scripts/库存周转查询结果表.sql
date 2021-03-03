
-- 商品级别  食百
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
order by 
	prov_code     ,
	dc_type,
prov_name     ,
shop_id       ,
catg_s_id      
--and  shop_id like 'E%'
;


-- 商品级别  生鲜
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
	bd_id = '11' 	
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
--and  shop_id like 'E%'
order by 
	prov_code     ,
	dc_type,
prov_name     ,
shop_id       ,
catg_s_id 
;

-- 商品级别  联营
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
	bd_id = '11' 	
	and sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
and  shop_id like 'E%'
order by 
	prov_code     ,
	dc_type,
prov_name     ,
shop_id       ,
catg_s_id 
;
-- 省区级别
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
	and type = '11'
	
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
	and type = 'E'
	
;
-- 门店级别
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

--报价清单
--select * from 
--csx_dw.dws_price_r_d_goods_prices_m;


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
profit/sales_value as profit_rate,
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
entry_sdt,
entry_days
FROM
	csx_dw.supply_turnover
WHERE
sdt='20200408'
and shop_id='W0A3'
;

refresh csx_dw.supply_turnover;
refresh csx_dw.supply_turnover_dc;
refresh csx_dw.supply_turnover_province;