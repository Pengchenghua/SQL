
with sale_01 as 
(SELECT  
       brand_name,
       sale,
       row_number()over(order by sale desc) row_num
FROM
(
SELECT brand_name,
       sum(sales_value)sale
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20190101' and sdt<'20210101'
and classify_large_code in ('B06','B07')
and business_type_code!='4'
and channel_code in ('1','7','9')
GROUP BY 
     brand_name
      
) a 
)  ,
sale_02 as (

SELECT substr(sdt,1,4) years,
    business_type_code,
    business_type_name,
       brand_name,
       sum(sales_value)sale
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20190101'
and classify_large_code in ('B06','B07')
and business_type_code!='4'
and channel_code in ('1','7','9')
GROUP BY 
    substr(sdt,1,4) ,
     brand_name,
     business_type_code,
    business_type_name
       )
select a.years,
       a.brand_name,
       business_type_code,
    business_type_name,
       a.sale,
       row_num
from sale_02 a
 join sale_01 b on a.brand_name=b.brand_name 
where row_num<51
;



SELECT substr(sdt,1,4) years,
        classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_small_code,
       category_small_name,
       sum(sales_value)sale
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20190101' and sdt<'20210101'
and classify_large_code in ('B06','B07')
GROUP BY 
substr(sdt,1,4) ,
        classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       category_small_code,
       category_small_name;