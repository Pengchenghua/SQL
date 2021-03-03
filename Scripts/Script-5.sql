
select goods_code,sales_value,
       sales_value/sum(sales_value)over() as sale_ratio,
       sum(sales_value)over(partition by 1 order by sales_value ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)/sum(sales_value)over() as sum_zb
from (
SELECT goods_code,
       sum(sales_value)sales_value
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE sdt>='20200101'
  AND sdt<'20201001'
  AND dc_code='W0A3'
  and channel in ('1','7')
  and division_code in ('12','13')
 group by 
       goods_code 
 ) a 
 order by sales_value desc;
  

select * from csx_dw.dws_basic_w_a_csx_product_m