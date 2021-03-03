select substring(sdt,1,6)mon ,dc_code,dc_name,sum(sales_value )sale,
sum(sales_cost )cost,sum(inventory_amt )inventory_amt ,sum(inventory_amt )/sum(sales_cost ) as trunc_days,
SUM(case when sdt=regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))),'-','') then inventory_amt end ) as end_amt
from csx_dw.dc_sale_inventory where sdt>='20200101' and sdt<'20200601'
and bd_id ='12'
and  dc_code ='W0A5'
group by dc_code,dc_name,substring(sdt,1,6)
;

select substring(sdt,1,6)mon ,dc_code,dc_name,sum(sales_value )sale,
sum(sales_cost )cost,sum(inventory_amt )inventory_amt ,sum(inventory_amt )/sum(sales_cost ) as trunc_days,
SUM(case when sdt='20200430' then inventory_amt end ) as end_amt
from csx_dw.dc_sale_inventory where sdt>='20200101' and sdt<'20200601'
and bd_id ='12'
and  dc_code ='W0A5'
group by dc_code,dc_name,substring(sdt,1,6);


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
days_trunover_30 ,
cost_30day ,
period_inv_amt_30day ,
days_sale      ,
max_sale_sdt,
no_sale_days,
entry_qty,
entry_sdt,
entry_days
FROM
    csx_dw.supply_turnover
WHERE
sdt=regexp_replace(to_date('${sdate}'),'-','') 
-- and prov_code ='${prov}'
${if(len(dc)==0  ,"","and shop_id in ('"+dc+"') ") }
${if(len(dept_c)==0,"","and dept_id in ('"+dept_c+"')")}
${if(len(text)==0,"","and goodsid in ('"+REPLACE(text,",","','")+"')")}
order by 
prov_code     ,
shop_id       ,
catg_s_id,
final_amt desc;
select COUNT(goodsid )  from csx_dw.supply_turnover where sdt='20200608';

