insert overwrite directory '/tmp/pengchenghua/bb' row format delimited fields terminated by '\t'
select sales_region_code,sales_region_name,
sales_province_code,sales_province_name,
    receive_location_code,
    receive_location_name,
    supplier_code,
    supplier_name,
    goods_code,
    goods_name,
    unit,
    sum(receive_qty)qty,
    sum(price*receive_qty) as receive_amt,
    category_large_code,
    category_large_name,
    category_middle_code,category_middle_name,
    category_small_code,
    category_small_name,
    department_code,
    department_name
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select 
    shop_id,shop_name,sales_province_code,sales_province_name,sales_region_code,sales_region_name
    from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose !='06') b on a.receive_location_code=b.shop_id
where sdt>='20210101' and sdt<'20210401' 
and order_type_code like 'P%'
and a.business_type='01'
group by sales_region_code,sales_region_name,
sales_province_code,sales_province_name,
    receive_location_code,
    receive_location_name,
    supplier_code,
    supplier_name,
    goods_code,
    goods_name,
    unit,
    category_large_code,
    category_large_name,
    category_middle_code,category_middle_name,
    category_small_code,
    category_small_name,
    department_code,
    department_name;


-- 供应商配送	01
-- 云超配送	02
-- 客户直送	03
-- 实物直通	04
-- 货到即配	54
-- 商超直送	67