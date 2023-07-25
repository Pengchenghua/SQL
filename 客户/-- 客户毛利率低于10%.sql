-- 毛利率低于10%
with aa as (select
  performance_region_name,
  performance_province_name,
  performance_city_name,
  customer_code,
  customer_name,
  business_type_name,
  if(first_order_month='202305','新客','老客') is_new_cust ,
  sum(case when sdt>='20230401' and sdt<='20230409' then sale_amt end ) /10000 last_sale_amt,
  sum(case when sdt>='20230401' and sdt<='20230409' then profit end ) /10000 last_profit,
  sum(case when sdt>='20230501' and sdt<='20230509' then sale_amt end ) /10000 sale_amt,
  sum(case when sdt>='20230501' and sdt<='20230509' then profit end ) /10000  profit
from
    csx_dws.csx_dws_sale_detail_di a
 left  join (select shop_code,shop_low_profit_flag from   csx_dim.csx_dim_shop where sdt='current' and  shop_low_profit_flag=0)c on a.inventory_dc_code=c.shop_code
  left join (
    select
      customer_code as customer_no,
      first_business_sale_date as first_order_date,
      substr(first_business_sale_date, 1, 6) first_order_month,
      business_type_code
    from
      csx_dws.csx_dws_crm_customer_business_active_di
    where
      sdt = 'current'
     -- and business_type_code = 1
  ) b on a.customer_code = b.customer_no and a.business_type_code=b.business_type_code
where
  sdt >= '20230401'
  and sdt <= '20230510'
  and a.business_type_code not in (1,4)
 -- and a.business_type_code=1
  and channel_code in ('1','7','9')
 group by  performance_region_name,
  performance_province_name,
  performance_city_name,
  customer_code,
  customer_name,
  business_type_name,
  if(first_order_month='202305','新客','老客')
  )
  select performance_region_name,
  performance_province_name,
  performance_city_name,
  customer_code,
  customer_name,
  customer_large_level,
  business_type_name,
  is_new_cust ,
  last_sale_amt,
  last_profit,
  sale_amt,
  profit,
  if(sale_amt=0,0, profit/sale_amt ) as profit_rate
  from aa 
 LEFT   join
  (select province_name,customer_no,customer_level_tag,customer_large_level
  from   csx_analyse.csx_analyse_report_sale_customer_level_mf
  where month='202305' and customer_large_level='C' and tag=2) b 
  on aa.customer_code=b.customer_no and aa.performance_province_name=b.province_name
  where   if(sale_amt=0,0, profit/sale_amt ) <0.10