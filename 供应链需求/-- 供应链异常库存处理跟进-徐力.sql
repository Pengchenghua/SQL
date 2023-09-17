-- 供应链异常库存处理跟进-徐力
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
  stock_turnover_days,
  last_receive_date,
  high_stock_flag,
  unsold_flag,
  unmoved_days,
  unsold_days,
  qm_qty,
  qm_amt,
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
  stock_turnover_days,
  last_receive_date,
  high_stock_flag,
  unsold_flag,
  unmoved_days,
  unsold_days
from
     csx_report.csx_report_wms_turnover_high_unsold_stock_df
where
  sdt = '20230731'
  and (unsold_flag=1 or high_stock_flag=1)
  ) a 
  left join 
  (select dc_code,goods_code,qm_qty,	qm_amt,nearly30days_turnover_days,contain_transfer_receive_sdt,no_sale_days
  from csx_ads.csx_ads_wms_goods_turnover_df
 where sdt='20230820') b on a.dc_code=b.dc_code and a.goods_code=b.goods_code 
 left join 
 (select * from csx_dim.csx_dim_csx_data_market_conf_supplychain_location) c on a.dc_code=c.dc_code 
 where c.dc_code is not null 