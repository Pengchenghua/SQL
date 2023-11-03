-- 部队销售占比-周芸

with aa as (select
--   customer_code,
--   customer_name,
--  new_second_category_name,
 -- inventory_dc_code,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
--  performance_city_code,
--  performance_city_name,
  second_category_name,
  case
    when inventory_dc_code in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71') then 'HJ'
    else second_category_name
  end new_second_category_name,
--  shop_low_profit_flag,
  sum(sale_amt ) sale_amt,
  sum(sale_cost) sale_cost,
  sum(profit) profit
from
  csx_dws.csx_dws_sale_detail_di a 
 left  join 
  (select shop_code,shop_low_profit_flag from
  csx_dim.csx_dim_shop a where sdt='current'   ) b on a.inventory_dc_code=b.shop_code
where
  sdt >= '20231001'
  and business_type_code = 1
  group by  
  --customer_code,
 -- customer_name,
--  new_second_category_name,
 -- inventory_dc_code,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
 -- performance_city_code,
--  performance_city_name,
  second_category_name,
  case
    when inventory_dc_code in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71')  then 'HJ'
    else second_category_name
  end  
--  shop_low_profit_flag
  )
  select  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
--  performance_city_code,
--  performance_city_name,
  sum(case when second_category_name='部队' then sale_amt end ) as troops_sale_amt,
  sum(case when second_category_name='部队' then profit end ) as troops_profit,
  sum(case when new_second_category_name='HJ' then sale_amt end ) as HJ_sale_amt,
  sum(case when new_second_category_name='HJ' then profit end ) as HJ_profit,
  sum(sale_amt) as sale_amt,
  sum(profit) profit 
 from aa 
 group by performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name
  ;
  
 
 -- 低毛利TOP3客户
with aa as (select
  customer_code,
  customer_name,
  -- inventory_dc_code,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
 performance_city_code,
 performance_city_name,
 second_category_name,
  case
    when inventory_dc_code in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71') then 'HJ'
    else second_category_name
  end new_second_category_name,
--  shop_low_profit_flag,
  sum(sale_amt ) sale_amt,
  sum(sale_cost) sale_cost,
  sum(profit) profit
from
  csx_dws.csx_dws_sale_detail_di a 
 left  join 
  (select shop_code,shop_low_profit_flag from
  csx_dim.csx_dim_shop a where sdt='current'   ) b on a.inventory_dc_code=b.shop_code
where
  sdt >= '20231001'
  and business_type_code = 1
  group by  
  customer_code,
  customer_name,
--  inventory_dc_code,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
 performance_city_name,
  second_category_name,
  case
    when inventory_dc_code in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71')  then 'HJ'
    else second_category_name
  end  
 -- shop_low_profit_flag
  ) 
  select performance_region_name,performance_province_name,
   concat_ws('\n',COLLECT_set(concat_ws(' ',full_name,bb ))) 
  from (
  select 
  concat_ws('-',customer_code,customer_name) as full_name,
  customer_code,
  customer_name,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  second_category_name,
  (sale_amt ) sale_amt,
  (sale_cost) sale_cost,
  (profit) profit,
  profit_rate,
  concat('(',round(profit_rate*100,2),'%)') as bb,
  aa 
 from (
  select customer_code,
  customer_name,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  second_category_name,
  (sale_amt ) sale_amt,
  (sale_cost) sale_cost,
  (profit) profit,
  profit_rate,
  dense_rank()over(partition by performance_province_code,second_category_name order by profit_rate asc ) aa 
 from (
  select customer_code,
  customer_name,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  second_category_name,
  (sale_amt ) sale_amt,
  (sale_cost) sale_cost,
  (profit) profit,
  profit/sale_amt as profit_rate
  from aa 
  ) a where coalesce(profit_rate,0)!=0
  )a where aa <4 and second_category_name='部队'
 )a 
  group by performance_region_name,performance_province_name
  ;


  --区分部队直送仓与非直送仓
  
with aa as (

create table csx_analyse_tmp.csx_analyse_tmp_troops_sale as 
select
   customer_code,
   customer_name,
--  new_second_category_name,
  inventory_dc_code,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
--  performance_city_code,
--  performance_city_name,
  second_category_name,
  case
    when inventory_dc_code in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71') then 'HJ'
    else second_category_name
  end new_second_category_name,
  shop_low_profit_flag,
  sum(sale_amt ) sale_amt,
  sum(sale_cost) sale_cost,
  sum(profit) profit
from
  csx_dws.csx_dws_sale_detail_di a 
 left  join 
  (select shop_code,shop_low_profit_flag from
  csx_dim.csx_dim_shop a where sdt='current'   ) b on a.inventory_dc_code=b.shop_code
where
  sdt >= '20231001'
  and business_type_code = 1
--  and second_category_name='部队'
--  and (shop_low_profit_flag=1
--   or  inventory_dc_code    in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71'))
  group by  
 customer_code,
 customer_name,
--  new_second_category_name,
  inventory_dc_code,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
 -- performance_city_code,
--  performance_city_name,
  second_category_name,
  case
    when inventory_dc_code in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71')  then 'HJ'
    else second_category_name
  end  
  ,shop_low_profit_flag
  )
  
  ;
  
  
  select  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
--  performance_city_code,
--  performance_city_name,
  sum(case when (second_category_name='部队' and shop_low_profit_flag=0) and inventory_dc_code not  in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71') then sale_amt end ) as troops_sale_amt,
  sum(case when (second_category_name='部队' and shop_low_profit_flag=0) and inventory_dc_code not  in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71') then profit end ) as troops_profit,
  sum(case when (second_category_name='部队' and shop_low_profit_flag=1) or  inventory_dc_code  in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71') then sale_amt end ) as zs_sale_amt,
  sum(case when (second_category_name='部队' and shop_low_profit_flag=1) or  inventory_dc_code  in( 'W0AX', 'W0BD', 'W0T0', 'W0AJ', 'W0G6', 'WB71') then profit end ) as zs_profit,
  sum(sale_amt) as sale_amt,()
  sum(profit) profit 
 from   csx_analyse_tmp.csx_analyse_tmp_troops_sale   
 group by performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name
  ;
  