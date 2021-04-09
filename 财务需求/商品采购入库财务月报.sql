--采购商品入库财务月报
SELECT  sales_region_code,
        sales_region_name ,
        sales_province_code,
        sales_province_name,
        receive_location_code,
       receive_location_name,
       source_type_name,
       super_class ,
       supplier_code,
       supplier_name,
       shipped_location_code,
       shipped_location_name,
       local_purchase_flag,
       receive_business_type,
       shipped_business_type,
       goods_code,
       b.goods_name,
        unit_name,
        standard,
        brand_name,
        division_code,
        division_name,
        department_id,
        department_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        b.category_large_code,
        b.category_large_name,
       sum(receive_qty)as receive_qty,
       sum(receive_amt)as receive_amt,
       sum(shipped_qty) as shipped_qty,
       sum(shipped_amt) as shipped_amt
FROM csx_dw.ads_supply_order_flow a 
join 
(select goods_id,
        goods_name,
        unit_name,
        standard,
        brand_name,
        division_code,
        division_name,
        department_id,
        department_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_large_code,
        category_large_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
left join 
(select sales_province_code,sales_province_name,shop_id,sales_region_code,sales_region_name 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') c on a.receive_location_code=c.shop_id
WHERE ( shipped_status in ('6','7','8') or a.receive_status='2') and ((a.receive_close_date>='20210101'
        AND receive_close_date<='20210131')
       OR (shipped_date >='20210101'
           AND shipped_date<='20210131'))
group by 
sales_region_code,
        sales_region_name ,
        sales_province_code,
        sales_province_name,
        receive_location_code,
       receive_location_name,
       source_type_name,
       super_class ,
       supplier_code,
       supplier_name,
       shipped_location_code,
       shipped_location_name,
       local_purchase_flag,
       receive_business_type,
       shipped_business_type,
       goods_code,
       b.goods_name,
        unit_name,
        standard,
        brand_name,
        division_code,
        division_name,
        department_id,
        department_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        b.category_large_code,
        b.category_large_name;


-- 增加单号

insert overwrite directory '/tmp/pengchenghua/entry' row format delimited fields terminated by '\t'
SELECT  substr(sdt,1,6) as mon,
        sales_region_code,
        sales_region_name ,
        a.order_code,
         sales_province_code,
        sales_province_name,
        source_type_name,
       CASE
			WHEN a.super_class='1'
				THEN '供应商订单'
			WHEN a.super_class='2'
				THEN '供应商退货订单'
			WHEN a.super_class='3'
				THEN '配送订单'
			WHEN a.super_class='4'
				THEN '返配订单'
				ELSE a.super_class
		END super_class_name  ,
       receive_location_code,
       receive_location_name,
       goods_code,
       b.goods_name,
        unit_name,
        standard,
        brand_name,
        -- division_code,
        -- division_name,
        department_id,
        department_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        b.category_large_code,
        b.category_large_name,
        supplier_code,
       supplier_name,
       shipped_location_code,
       shipped_location_name,
       local_purchase_flag,
       receive_business_type,
       shipped_business_type,
       receive_qty,
       receive_amt,
       shipped_qty,
       shipped_amt,
       receive_colse_date
FROM csx_dw.ads_supply_order_flow a 
join 
(select goods_id,
        goods_name,
        unit_name,
        standard,
        brand_name,
        division_code,
        division_name,
        department_id,
        department_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_large_code,
        category_large_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
left join 
(select sales_province_code,sales_province_name,shop_id,sales_region_code,sales_region_name 
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') c on a.receive_location_code=c.shop_id
WHERE ( shipped_status in ('6','7','8') or a.receive_status='2') and ((a.receive_close_date>='20210301'
        AND receive_close_date<='20210331')
       OR (shipped_date >='20210201'
           AND shipped_date<='20210331'))
           
;           
group by 
sales_region_code,
        sales_region_name ,
        sales_province_code,
        sales_province_name,
        receive_location_code,
       receive_location_name,
       source_type_name,
       super_class ,
       supplier_code,
       supplier_name,
       shipped_location_code,
       shipped_location_name,
       local_purchase_flag,
       receive_business_type,
       shipped_business_type,
       goods_code,
       b.goods_name,
        unit_name,
        standard,
        brand_name,
        division_code,
        division_name,
        department_id,
        department_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        b.category_large_code,
        b.category_large_name;