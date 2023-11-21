-- 供应链异常库存处理跟进-徐力

-- 供应链增加仓信息供筛选-徐力
-- 20231108采用新表 csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new
select
  performance_region_name,
  performance_province_name,
  performance_city_name,
  a.dc_code,
  dc_name,
  business_division_name,
--   classify_large_code,
--   classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  a.goods_code,
  goods_name,
  unit_name,
  stock_qty,
  stock_amt,
  a.stock_amt_no_tax	,   -- 不含税金额
 -- b.stock_amt_no_tax as last_stock_amt_no_tax,
 -- a.stock_turnover_days,
  a.nearly30days_transfer,-- 含税周转
  nearly30days_transfer_no_tax,  -- 未税周转
  last_receive_date,
  high_stock_flag,
  unsold_flag,
  unmoved_days,
  unsold_amt,
  qm_qty,
  qm_amt,
  qm_amt_no_tax,
  nearly30days_turnover_days,
  contain_transfer_receive_sdt
  ,no_sale_days
  ,c.shop_tags_name,c.purpose_name
from
(select
  performance_region_name,
  performance_province_name,
  performance_city_name,
  dc_area_code dc_code,
  dc_area_name dc_name,
  purchase_group_name,
  category_name business_division_name ,
--   classify_large_code,
--   classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  goods_code,
  goods_name,
  unit_name,
  stock_qty,
  stock_amt,
  stock_amt_no_tax,
 -- stock_turnover_days,
  nearly30days_transfer,
  nearly30days_transfer_no_tax,
  last_receive_date,
  high_stock_flag,
  unsold_flag,
  unsold_amt,
  unsold_amt_no_tax,
  unmoved_days,
  shop_tags_name
  from
          csx_report.csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new
where
  sdt = '20230930'
  and (unsold_flag=1 or high_stock_flag=1)
  ) a 
  
  left join 
  (select dc_area_code dc_code,a.goods_code,stock_qty qm_qty, stock_amt qm_amt,stock_amt_no_tax  as qm_amt_no_tax,nearly30days_transfer nearly30days_turnover_days,last_receive_date contain_transfer_receive_sdt,unmoved_days no_sale_days
  from    csx_report.csx_report_cas_accounting_turnover_stock_cost_goods_detail_df_new a 
 
 where sdt='20231107') b on a.dc_code=b.dc_code and a.goods_code=b.goods_code 
 left join
 (select shop_code,shop_tags_name,purpose_name from csx_dim.csx_dim_shop where sdt='current') c on a.dc_code=c.shop_code 
-- where c.dc_code is not null 

;
-- 按照管理仓
select
  performance_region_name,
  performance_province_name,
  performance_city_name,
  a.dc_code,
  dc_name,
  business_division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  a.goods_code,
  goods_name,
  unit_name,
  stock_qty,
  stock_amt,
  a.stock_amt_no_tax	,   -- 不含税金额
 -- b.stock_amt_no_tax as last_stock_amt_no_tax,
 -- a.stock_turnover_days,
  a.tax_stock_turnover_days,-- 含税周转
  last_receive_date,
  high_stock_flag,
  unsold_flag,
  unmoved_days,
  unsold_days,
  qm_qty,
  qm_amt,
  qm_amt_no_tax,
  nearly30days_turnover_days,
  contain_transfer_receive_sdt
  ,no_sale_days
  
from
(select
  performance_region_name,
  performance_province_name,
  performance_city_name,
  dc_code,
  dc_name,
  business_division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  goods_code,
  goods_name,
  unit_name,
  stock_qty,
  stock_amt,
  stock_amt_no_tax,
  stock_turnover_days,
  tax_stock_turnover_days,
  last_receive_date,
  high_stock_flag,
  unsold_flag,
  unmoved_days,
  unsold_days
from
         csx_report.csx_report_wms_turnover_high_unsold_stock_df
where
  sdt = '20230930'
  and (unsold_flag=1 or high_stock_flag=1)
  ) a 
  
  left join 
  (select dc_code,a.goods_code,qm_qty,qm_amt,qm_amt/(1+tax_rate/100) as qm_amt_no_tax,nearly30days_turnover_days,contain_transfer_receive_sdt,no_sale_days
  from     csx_ads.csx_ads_wms_goods_turnover_df a 
  left join 
  (select goods_code,tax_rate from csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code
 where sdt='20231031') b on a.dc_code=b.dc_code and a.goods_code=b.goods_code 
 left join
 (select * from csx_dim.csx_dim_csx_data_market_conf_supplychain_location) c on a.dc_code=c.dc_code 
 where c.dc_code is not null 