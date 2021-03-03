
select order_code,sum(order_qty),sum(receive_qty ),receive_close_date,
from_unixtime(unix_timestamp(receive_close_date,'yyyyMMdd'),'yyyy-MM-dd'),last_delivery_date ,
datediff(from_unixtime(unix_timestamp(receive_close_date,'yyyyMMdd'),'yyyy-MM-dd'),last_delivery_date )
from csx_dw.ads_supply_order_flow where sdt>'20200501' and supplier_code='20041906' and regexp_replace(last_delivery_date,'-','')<='20200526'
group by order_code,receive_close_date,
from_unixtime(unix_timestamp(receive_close_date,'yyyyMMdd'),'yyyy-MM-dd'),last_delivery_date
;

SELECT
     mon ,
    channel_name,
    province_name,
    province_code,
    a.customer_no,
    customer_name,
 if(b.customer_no is null ,'否','是') is_par,
    first_category,
    second_category,
    vendor_code,
    vendor_name,
    goods_code,
    goods_name,
    unit,
    division_name ,
    brand_name,
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name,
    category_small_code,
    category_small_name,
    sales_qty,
    sale,
    sales_cost,
    profit
    from 
(SELECT
    SUBSTRING(sdt, 1, 6) mon ,
    channel_name,
    province_name,
    province_code,
    a.customer_no,
    customer_name,
--  if(a.customer_no is null ,'否','是') is_par,
    first_category,
    second_category,
    vendor_code,
    vendor_name,
    goods_code,
    goods_name,
    unit,
    brand_name,
    division_code ,
    division_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name,
    category_small_code,
    category_small_name,
    SUM(sales_qty)sales_qty,
    SUM(sales_value)sale,
    SUM(sales_cost )sales_cost,
    SUM(profit)profit
FROM
    csx_dw.dws_sale_r_d_customer_sale  a 
WHERE
    sdt >= '20200101'
    AND sdt <= '20200331'
    -- and is_copemate_order =1 
    -- AND division_code IN ('11',  '12','10','13','14')
GROUP BY
    SUBSTRING(sdt, 1, 6),
--  if(a.customer_no is null ,'否','是') ,
    channel_name,
    province_name,
    province_code,
    a.customer_no,
    customer_name,
    vendor_code,
    vendor_name,
    goods_code,
    goods_name,
    unit,
    division_code ,
    division_name ,
    brand_name,
    department_code ,
    department_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name,
    category_small_code,
    category_small_name,
    first_category,
    second_category) as a 
       left join 
    (select DISTINCT customer_no from csx_dw.csx_partner_list where sdt>'202001' )b on a.customer_no=b.customer_no ;
