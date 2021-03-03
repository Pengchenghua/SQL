select 
--    category_large_code                    ,
--    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name                   ,  
     category_small_code  ,
    category_small_name,
    sale    ,
    sale/ring_sale-1 as sale_rate,
    sale/sum(sale)over() as sale_ratio,
    profit,
    profit/sale as profit_rate,
    coalesce(profit/sale,0)-coalesce(ring_profit/ring_sale,0) as diff_profit_rate,
    ring_sale_sku,
    ring_sale,   
    ring_profit,
    sale_sku,
    ring_sale/sum(ring_sale)over() as ring_sale_ratio
from (select 
--    category_large_code                    ,
--    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name                   ,
 category_small_code                    ,
 category_small_name                    ,
    sum(sale_sku) as sale_sku,
    sum(sale)               sale    ,
    sum(profit )                   profit,
    sum(ring_sale_sku)as ring_sale_sku,
    sum(ring_sale )as ring_sale,   
    sum(ring_profit)as ring_profit
from 
(
select
    category_large_code                    ,
    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name                   ,
    category_small_code                    ,
    category_small_name                    ,
    COUNT(DISTINCT goods_code ) as sale_sku,
    sum(sales_value)               sale    ,
    sum(profit )                   profit,
    0 as ring_sale_sku,
    0 as ring_sale ,   
    0 as ring_profit
    from csx_dw.dws_sale_r_d_customer_sale
where
    sdt             >='20200601'
    and sdt         <='20200618'
    and is_self_sale =1
    and channel      ='1'
    and
    (
        category_large_code     ='1241'
        or category_middle_code ='110406'
    )
group by
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    category_small_code  ,
    category_small_name
union all
select
    category_large_code                         ,
    category_large_name                         ,
    category_middle_code                        ,
    category_middle_name                        ,
    category_small_code                         ,
    category_small_name                         ,
    0 as sale_sku,
    0 as sale    ,
    0 as profit,
    COUNT(DISTINCT goods_code ) as ring_sale_sku,
    sum(sales_value)               ring_sale    ,
    sum(profit )                   ring_profit
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt             >='20200501'
    and sdt         <='20200518'
    and is_self_sale =1
    and channel      ='1'
    and
    (
        category_large_code     ='1241'
        or category_middle_code ='110406'
    )
group by
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    category_small_code  ,
    category_small_name
    ) a 

    where category_middle_code ='110406'
    group by 
--    category_large_code                    ,
--    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name,
     category_small_code  ,
    category_small_name
)a 
order by category_small_code
;

select 
department_code ,department_name ,
    category_large_code                    ,
    category_large_name                    ,
--    category_middle_code                   ,
--    category_middle_name                   ,  
--     category_small_code  ,
--    category_small_name,
    sale    ,
    sale/ring_sale-1 as sale_rate,
    sale/sum(sale)over() as sale_ratio,
    profit,
    profit/sale as profit_rate,
    coalesce(profit/sale,0)-coalesce(ring_profit/ring_sale,0) as diff_profit_rate,
    ring_sale_sku,
    ring_sale,   
    ring_profit,
    sale_sku,
    ring_sale/sum(ring_sale)over() as ring_sale_ratio
from (select 
department_code ,department_name ,
    category_large_code                    ,
    category_large_name                    ,
--    category_middle_code                   ,
--    category_middle_name                   ,
-- category_small_code                    ,
-- category_small_name                    ,
    sum(sale_sku) as sale_sku,
    sum(sale)               sale    ,
    sum(profit )                   profit,
    sum(ring_sale_sku)as ring_sale_sku,
    sum(ring_sale )as ring_sale,   
    sum(ring_profit)as ring_profit
from 
(
select
department_code ,department_name ,
    category_large_code                    ,
    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name                   ,
    category_small_code                    ,
    category_small_name                    ,
    COUNT(DISTINCT goods_code ) as sale_sku,
    sum(sales_value)               sale    ,
    sum(profit )                   profit,
    0 as ring_sale_sku,
    0 as ring_sale ,   
    0 as ring_profit
    from csx_dw.dws_sale_r_d_customer_sale
where
    sdt             >='20200601'
    and sdt         <='20200618'
    and is_self_sale =1
    and channel      ='1'
    and
    (
        department_code     ='A03'
        or category_middle_code ='110406'
    )
group by
department_code ,department_name ,
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    category_small_code  ,
    category_small_name
union all
select
department_code ,department_name ,
    category_large_code                         ,
    category_large_name                         ,
    category_middle_code                        ,
    category_middle_name                        ,
    category_small_code                         ,
    category_small_name                         ,
    0 as sale_sku,
    0 as sale    ,
    0 as profit,
    COUNT(DISTINCT goods_code ) as ring_sale_sku,
    sum(sales_value)               ring_sale    ,
    sum(profit )                   ring_profit
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt             >='20200501'
    and sdt         <='20200518'
    and is_self_sale =1
    and channel      ='1'
    and
    (
       department_code     ='A03'
        or category_middle_code ='110406'
    )
group by
department_code ,department_name ,
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    category_small_code  ,
    category_small_name
    ) a 

    where 
    -- category_middle_code ='110406'
     department_code     ='A03'
    group by 
    department_code ,department_name ,
    category_large_code                    ,
    category_large_name                    
--    category_middle_code                   ,
--    category_middle_name
----     category_small_code  ,
--    category_small_name
)a 
order by category_large_code
;



select
    category_large_code                    ,
    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name                   ,
    category_small_code                    ,
    category_small_name                    ,
    COUNT(DISTINCT goods_code ) as sale_sku,
    sum(sales_value)               sale    ,
    sum(profit )                   profit,
    0 as ring_sale_sku,
    0 as ring_sale ,   
    0 as ring_profit
    from csx_dw.dws_sale_r_d_customer_sale
where
    sdt             >='20200601'
    and sdt         <='20200618'
    and is_self_sale =1
    and channel      ='1'
--    and
--    (
--        category_large_code     ='1241'
--        or category_middle_code ='110406'
--    )
group by
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    category_small_code  ,
    category_small_name;
    
select * from csx_dw.dws_basic_w_a_category_m  where sdt='current' and purchase_group_name like '干性%';


SELECT
province_code ,
province_name ,
goods_code ,
goods_name ,
unit ,
department_code ,department_name ,
    category_large_code                    ,
    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name                   ,
    category_small_code                    ,
    category_small_name                    ,
  --  COUNT(DISTINCT goods_code ) as sale_sku,
 qty,
          sale    ,
       profit,
       profit/sale profit_rate
    from (
SELECT
province_code ,
province_name ,
goods_code ,
goods_name ,
unit ,
department_code ,department_name ,
    category_large_code                    ,
    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name                   ,
    category_small_code                    ,
    category_small_name                    ,
  --  COUNT(DISTINCT goods_code ) as sale_sku,
  sum(sales_qty )qty,
    sum(sales_value)               sale    ,
    sum(profit )                   profit
    from csx_dw.dws_sale_r_d_customer_sale
where
    sdt             >='20200601'
    and sdt         <='20200618'
    and is_self_sale =1
    and channel      ='1'
    and
    (
        department_code     ='A03'
        or category_middle_code ='110406'
    )
group by
province_code ,
province_name ,
goods_code ,
goods_name ,
unit ,
department_code ,department_name ,
    category_large_code                    ,
    category_large_name                    ,
    category_middle_code                   ,
    category_middle_name                   ,
    category_small_code                    ,
    category_small_name        
) a 
-- where  profit <0
;
