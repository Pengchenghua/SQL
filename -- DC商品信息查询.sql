

insert overwrite directory '/tmp/pengchenghua/goods'
row format delimited
fields terminated by ';'
SELECT  province_code,province_name ,
        shop_code,
        shop_name,
        product_code,
       product_name,
       product_bar_code,
       brand_name,
       division_code,
       division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       department_id,
       department_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       b.product_level,
       b.product_level_name,
       a.des_specific_product_status,
       a.product_status_name,
       a.valid_tag,
       a.valid_tag_name
FROM csx_dw.dws_basic_w_a_csx_product_info a 
join 
(select goods_id, 
        division_code,
       division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       department_id,
       department_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       product_level,
       product_level_name
     from csx_dw.dws_basic_w_a_csx_product_m 
     where sdt='current') b on a.product_code=b.goods_id
join 
(select shop_id,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose='01') c on a.shop_code=c.shop_id
WHERE sdt='current'
and division_code in ('10','11','12','13','14')
and a.product_status_name not like '%退场%'
;
