
SELECT
months ,
dist_code,
dist_name,
province_code     ,
province_name     ,
dc_code       ,
dc_name     ,
goods_id       ,
goods_name     ,
standard      ,
unit_name     ,
brand_name    ,
dept_id       ,
dept_name     ,
business_division_code,
business_division_name,
division_code        ,
division_name        ,
category_large_code  ,
category_large_name  ,
category_middle_code ,
category_middle_name ,
a.category_small_code  ,
category_small_name  ,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name,
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
sales_30day     ,
qty_30day      ,
days_turnover_30 ,
cost_30day ,
dms,
period_inv_amt_30day ,
inv_sales_days,
max_sale_sdt,
no_sale_days,
entry_qty,
entry_value,
entry_sdt,
entry_days
FROM
   csx_tmp.ads_wms_r_d_goods_turnover a 
 join 
(select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current'
	and classify_middle_code = 'B0304') as m on a.category_small_code =m.category_small_code 
WHERE
a.sdt='20201031'
-- in ('20201031','20201130','20201230')
;


select * from    csx_tmp.tmp_supervisor_day_detail
			where sdt='20201231';


SELECT
months ,
dist_code,
dist_name,
province_code     ,
province_name     ,
dc_code       ,
dc_name     ,
goods_id       ,
goods_name     ,
standard      ,
unit_name     ,
brand_name    ,
dept_id       ,
dept_name     ,
business_division_code,
business_division_name,
division_code        ,
division_name        ,
category_large_code  ,
category_large_name  ,
category_middle_code ,
category_middle_name ,
a.category_small_code  ,
category_small_name  ,
--classify_large_code,
--classify_large_name,
--classify_middle_code,
--classify_middle_name,
--classify_small_code,
--classify_small_name,
--valid_tag      ,
--valid_tag_name ,
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
sales_30day     ,
qty_30day      ,
days_turnover_30 ,
cost_30day ,
dms,
period_inv_amt_30day ,
inv_sales_days,
max_sale_sdt,
no_sale_days,
entry_qty,
entry_value,
entry_sdt,
entry_days
FROM
   csx_tmp.ads_wms_r_d_goods_turnover a 
 
WHERE
a.sdt='20201230'
-- in ('20201031','20201130','20201230')

;
