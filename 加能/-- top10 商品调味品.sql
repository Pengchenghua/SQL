-- top10 商品调味品 
select a.goods_code,
    goods_name,
    classify_small_code,
    classify_small_name,
    qty,
    sales_value,
    profit ,
    dense_rank()over(partition by classify_small_name order by sales_value desc) aa 
from 
(
select a.goods_code,
    b.goods_name,
    b.classify_small_code,
    b.classify_small_name,
    sum(sales_qty) qty,
    sum(sales_value) sales_value,
    sum(profit) profit 
from csx_dw.dws_sale_r_d_detail a 
join 
(SELECT goods_id,
       goods_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
  AND classify_middle_code ='B0602') b on a.goods_code=b.goods_id
where sdt>='20210101' and sdt<'20220101' 
and business_type_code!='4' 
and channel_code in ('7','1','9')
group by 
    a.goods_code,
    b.goods_name,
    b.classify_small_code,
    b.classify_small_name
)a;

-- top10 商品调味品 
select a.goods_code,
    goods_name,
    classify_small_code,
    classify_small_name,
    qty,
    sales_value,
    profit ,
    dense_rank()over(partition by classify_small_name order by sales_value desc) aa 
from 
(
select a.goods_code,
    b.goods_name,
    b.classify_small_code,
    b.classify_small_name,
    sum(sales_qty) qty,
    sum(sales_value) sales_value,
    sum(profit) profit 
from csx_dw.dws_sale_r_d_detail a 
join 
(SELECT goods_id,
       goods_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
  AND classify_middle_code ='B0602') b on a.goods_code=b.goods_id
where sdt>='20210101' and sdt<'20220101' 
and business_type_code!='4' 
and channel_code in ('7','1','9')
group by 
    a.goods_code,
    b.goods_name,
    b.classify_small_code,
    b.classify_small_name
)a
;



select 
    a.classify_small_code,
    a.classify_small_name,
    b.classify_small_code,
    b.classify_small_name,
    sum(sales_qty) qty,
    sum(sales_value) sales_value,
    sum(profit) profit ,
    sum(profit)/sum(a.sales_value) as profit_rate
from csx_dw.dws_sale_r_d_detail a 
join 
(SELECT goods_id,
       goods_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
  AND classify_middle_code ='B0602') b on a.goods_code=b.goods_id
where sdt>='20210101' and sdt<'20220101'
and goods_code='257257'
and business_type_code!='4' 
and channel_code in ('7','1','9')
group by 
   a.classify_small_code,
    a.classify_small_name,
    b.classify_small_code,
    b.classify_small_name
