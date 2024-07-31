-- 财务客户损益利润
select
  sales_channel_name,
  a.customer_code,
  customer_name,
  first_sign_date,
  first_sale_date,
  c.second_category_name,
  performance_region_name,
  performance_province_name,
  sum(a.sale_amt_no_tax) / 10000 sale_amt_no_tax,
  sum(a.profit_no_tax) / 10000 profit_no_tax,
  sum(net_profit) / 10000 net_profit,
  -- 净利润
  sum(capital_takes_up) as capital_takes_up,
  -- 资金占用费用
  sum(transport_amount) transport_amount,
  -- 未税费用
  sum(performance_profit) performance_profit -- 履约利润
from
  csx_analyse.csx_analyse_report_sss_customer_capital_takes_up_di a
  
  left join (
    select
      customer_code,
      first_sale_date,
      first_sign_date
    from
        csx_dws.csx_dws_crm_customer_active_di
    where
      sdt = 'current'
  ) b on a.customer_code = b.customer_code
  left join (
    select
      customer_code,
      first_category_name,
      second_category_name,
      third_category_name
    from
      csx_dim.csx_dim_crm_customer_info
    where
      sdt = 'current'
  ) c on a.customer_code = c.customer_code
where
  sdt >= '20230101'
  and sdt < '20231001'
  and channel_code in('1', '9')
  and sales_channel_name not in('项目供应商', '城市服务商')
group by
  a.customer_code,
  customer_name,
  performance_region_name,
  performance_province_name,
  sales_channel_name,
  first_sale_date,
  first_sign_date,
  c.second_category_name
having
  sum(coalesce(a.sale_amt_no_tax, 0)) <> 0
  or sum(coalesce(a.sale_amt_no_tax, 0)) <> 0