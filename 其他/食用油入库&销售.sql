SELECT *
FROM csx_dw.dws_basic_w_a_manage_classify_m
WHERE sdt='current'
and classify_middle_code like 'B0603'
  ;
  
  
  
  -- 食用油供应商入库
select 
substr(sdt,1,6) as mon,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    supplier_code,
    supplier_name,
    a.division_code,
    a.division_name,
    a.category_large_code,
    a.category_large_name,
    a.goods_code,
    c.goods_name,
    c.brand_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    c.department_id,
    c.department_name,
    sum(receive_qty) as qty,
    sum(price*receive_qty) as amt
from csx_dw.dws_wms_r_d_entry_detail a
join 
(SELECT goods_id,
       goods_name,
       brand_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
    and classify_middle_code ='B0603'
)c on a.goods_code=c.goods_id
 join 
 (SELECT shop_id,
       sales_region_code,
       sales_region_name,
    --   sales_province_code,
    --   sales_province_name,
     case when (purchase_org='P620'or shop_id='W0J8') then '620' else   province_code end province_code,
     case when (purchase_org='P620'or shop_id='W0J8') then '平台' else  province_name end province_name,
       purpose,
       purpose_name,
       shop_name,
       city_code,
       city_name
FROM csx_dw.dws_basic_w_a_csx_shop_m
WHERE sdt='current'
  AND table_type=1 
--   and purchase_org !='P620'
--   and shop_id not in ('W0J8') --,'W0K4'
 --  AND purpose IN ('01')
  ) b on a.receive_location_code=b.shop_id
where sdt>='20210101' 
    and sdt<'20210924'
    and receive_status in (1,2)
    and a.order_type_code LIKE 'P%' 
    and a.business_type='01'
    and classify_middle_code='B0603'
group by substr(sdt,1,6),
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    supplier_code,
    supplier_name,
    a.division_code,
    a.division_name,
    a.category_large_code,
    a.category_large_name,
    a.goods_code,
    c.goods_name,
    c.brand_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    c.department_id,
    c.department_name
    ;


     
  -- 食用油销售
select 
substr(sdt,1,6) as mon,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,

    a.division_code,
    a.division_name,
    a.category_large_code,
    a.category_large_name,
    a.goods_code,
    c.goods_name,
    c.brand_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    c.department_id,
    c.department_name,
    sum(a.sales_qty) as qty,
    sum(sales_cost) as cost,
    sum(a.sales_value) as amt,
    sum(a.profit) profit
from csx_dw.dws_sale_r_d_detail a
join 
(SELECT goods_id,
       goods_name,
       brand_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
    and classify_middle_code ='B0603'
)c on a.goods_code=c.goods_id
--  join 
--  (SELECT shop_id,
--       sales_region_code,
--       sales_region_name,
--     --   sales_province_code,
--     --   sales_province_name,
--      case when (purchase_org='P620'or shop_id='W0J8') then '620' else   province_code end province_code,
--      case when (purchase_org='P620'or shop_id='W0J8') then '平台' else  province_name end province_name,
--       purpose,
--       purpose_name,
--       shop_name,
--       city_code,
--       city_name
-- FROM csx_dw.dws_basic_w_a_csx_shop_m
-- WHERE sdt='current'
--   AND table_type=1 
-- --   and purchase_org !='P620'
-- --   and shop_id not in ('W0J8') --,'W0K4'
--  --  AND purpose IN ('01')
--   ) b on a.receive_location_code=b.shop_id
where sdt>='20210101' 
    and sdt<'20210924'
    -- and receive_status in (1,2)
    -- and a.order_type_code LIKE 'P%' 
    -- and a.business_type='01'
    and c.classify_middle_code='B0603'
group by substr(sdt,1,6),
   a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.division_code,
    a.division_name,
    a.category_large_code,
    a.category_large_name,
    a.goods_code,
    c.goods_name,
    c.brand_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    c.department_id,
    c.department_name
    ;