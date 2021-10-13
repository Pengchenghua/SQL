--供应商入库明细 &供应商需求【徐力】
select sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    supplier_code,
    supplier_name,
    goods_code,
    c.goods_name,
    c.division_code,
    c.division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    c.department_id,
    c.department_name,
    sum(receive_qty) qty,
    sum(price*receive_qty) as amt
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select 
    sales_province_code,
    sales_province_name,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
        when province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市' 
    else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1  ) b on a.receive_location_code =b.shop_id
join 
(SELECT goods_id,
       goods_name,
       spu_goods_code,
       spu_goods_name,
       brand_name,
       division_code,
       division_name,
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
    -- and classify_middle_code ='B0302'
)c on a.goods_code=c.goods_id
where sdt>='20210101' 
and a.order_type_code LIKE 'P%' and business_type !='02'
group by sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    supplier_code,
    supplier_name,
    goods_code,
    c.goods_name,
    c.division_code,
    c.division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    c.department_id,
    c.department_name,
    sdt