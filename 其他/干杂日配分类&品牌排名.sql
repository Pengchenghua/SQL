
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






with sale_01 as 
(SELECT  
       brand_name,
       sale,
       profit,
       all_sale_num,
       row_number()over(order by sale desc) row_num
FROM
(
SELECT brand_name,
       sum(sales_value)sale,
       sum(profit) profit,
       count(distinct goods_code) as all_sale_num
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
    province_code,
    province_name,
    business_type_code,
    business_type_name,
    brand_name,
    sum(sales_value)sale,
    sum(profit) profit,
    count(distinct goods_code) as sale_num
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20190101'
and classify_large_code in ('B06','B07')
and business_type_code!='4'
and channel_code in ('1','7','9')
GROUP BY 
    substr(sdt,1,4) ,
     brand_name,
     business_type_code,
    business_type_name,
    province_code,
    province_name
       )
select a.years,
    province_code,
    province_name,
    a.brand_name,
    business_type_code,
    business_type_name,
    a.sale,
    a.profit,
    sale_num,
    b.sale,
    b.profit,
    b.all_sale_num,
    row_num
from sale_02 a
 join sale_01 b on a.brand_name=b.brand_name 
where row_num<51
;
REFRESH csx_dw.dws_sale_r_d_detail;




with sale_01 as 
(SELECT  
       brand_name,
       sale,
       profit,
       all_sale_num,
       row_number()over(order by sale desc) row_num
FROM
(
SELECT brand_name,
       sum(sales_value)sale,
       sum(profit) profit,
       count(distinct goods_code) as all_sale_num
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
    province_code,
    province_name,
    -- business_type_code,
    -- business_type_name,
    brand_name,
    sum(sales_value)sale,
    sum(profit) profit,
    count(distinct goods_code) as sale_num
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20190101'
and classify_large_code in ('B06','B07')
and business_type_code!='4'
and channel_code in ('1','7','9')
GROUP BY 
    substr(sdt,1,4) ,
     brand_name,
    --  business_type_code,
    -- business_type_name,
    province_code,
    province_name
       )
select a.years,
    province_code,
    province_name,
    a.brand_name,
    -- business_type_code,
    -- business_type_name,
    a.sale,
    a.profit,
    sale_num,
    b.sale,
    b.profit,
    b.all_sale_num,
    row_num
from sale_02 a
 join sale_01 b on a.brand_name=b.brand_name 
where row_num<51
;


drop table  csx_tmp.sale_01 ;
create temporary table  csx_tmp.sale_01 as 
SELECT  
       brand_name,
       sale,
       profit,
       all_sale_num,
       row_number()over(order by sale desc) row_num
FROM
(
SELECT brand_name,
       sum(sales_value)sale,
       sum(profit) profit,
       count(distinct goods_code) as all_sale_num
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20190101' and sdt<'20210101'
and classify_large_code in ('B06','B07')
and business_type_code!='4'
and channel_code in ('1','7','9')
GROUP BY 
     brand_name
) a 
 
  ;
  
create temporary table  csx_tmp.sale_02 as 
SELECT substr(sdt,1,4) years,
    province_code,
    province_name,
    business_type_code,
    business_type_name,
    brand_name,
    sum(sales_value)sale,
    sum(profit) profit,
    count(distinct goods_code) as sale_num
FROM csx_dw.dws_sale_r_d_detail
WHERE sdt>='20190101'
and classify_large_code in ('B06','B07')
and business_type_code!='4'
and channel_code in ('1','7','9')
GROUP BY 
    substr(sdt,1,4) ,
     brand_name,
     business_type_code,
    business_type_name,
    province_code,
    province_name
       
       
;
create temporary table csx_tmp.sale_1 as 
select a.years,
    province_code,
    province_name,
    a.brand_name,
    business_type_code,
    business_type_name,
    a.sale as a_sale,
    a.profit as a_profit,
    sale_num,
    b.sale as b_sale,
    b.profit,
    b.all_sale_num,
    row_num
from csx_tmp.sale_02 a
 join csx_tmp.sale_01 b on a.brand_name=b.brand_name 
where row_num<51
;

select * from csx_tmp.sale_1 ;