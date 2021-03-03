insert overwrite directory '/tmp/pengchenghua/11' row format delimited fields terminated  by '\t'
SELECT year,
      week_of_year,
        csx_week,
       receive_location_code,
       shop_name,
       a.goods_code,
       g.goods_name,
       unit_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       amt/qty as price,
       qty, 
       amt,
       a.supplier_code,
       a.supplier_name
from 
(SELECT m.year,
        m.week_of_year,
       m.csx_week,
       a.receive_location_code,
       a.goods_code,
       a.goods_name,
       a.goods_bar_code,
       a.unit,
       sum(a.receive_qty)as qty,
       sum(price*a.receive_qty) amt,
       a.supplier_code,
       a.supplier_name
FROM csx_dw.dws_wms_r_d_entry_detail a
join csx_dw.dws_basic_w_a_date m on a.sdt=m.calday
where a.sdt>='20200101'
and a.order_type_code like 'P%'
group by 
    m.year,
        m.week_of_year,
        m.csx_week,
       a.receive_location_code,
       a.goods_code,
       a.goods_name,
       a.goods_bar_code,
       a.unit,
        a.supplier_code,
       a.supplier_name
)a 
JOIN
  (SELECT shop_id,
          shop_name,
          sales_region_code,
          sales_region_name,
          sales_province_code,
          sales_province_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND sales_region_code='1'
     and sales_province_code='2'
     )b ON a.receive_location_code=b.shop_id
join 
(SELECT goods_id,
       goods_name,
       unit_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') g on a.goods_code=g.goods_idinsert overwrite directory '/tmp/pengchenghua/11' row format delimited fields terminated  by '\t'
SELECT year,
      week_of_year,
        csx_week,
       receive_location_code,
       shop_name,
       a.goods_code,
       g.goods_name,
       unit_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       amt/qty as price,
       qty, 
       amt,
       a.supplier_code,
       a.supplier_name
from 
(SELECT m.year,
        m.week_of_year,
       m.csx_week,
       a.receive_location_code,
       a.goods_code,
       a.goods_name,
       a.goods_bar_code,
       a.unit,
       sum(a.receive_qty)as qty,
       sum(price*a.receive_qty) amt,
       a.supplier_code,
       a.supplier_name
FROM csx_dw.dws_wms_r_d_entry_detail a
join csx_dw.dws_basic_w_a_date m on a.sdt=m.calday
where a.sdt>='20200101'
and a.order_type_code like 'P%'
group by 
    m.year,
        m.week_of_year,
        m.csx_week,
       a.receive_location_code,
       a.goods_code,
       a.goods_name,
       a.goods_bar_code,
       a.unit,
        a.supplier_code,
       a.supplier_name
)a 
JOIN
  (SELECT shop_id,
          shop_name,
          sales_region_code,
          sales_region_name,
          sales_province_code,
          sales_province_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND sales_region_code='1'
     and sales_province_code='2'
     )b ON a.receive_location_code=b.shop_id
join 
(SELECT goods_id,
       goods_name,
       unit_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') g on a.goods_code=g.goods_id