--财务需求【单品入库】
--薯条+薯饼+红薯条

SELECT substr(sdt,1,6) as mon,
   -- receive_location_code,
    goods_code,
    goods_name,
    department_code,
    department_name,
    unit,
    spec,
    brand_name,
    sum(receive_qty) qty,
    sum(amount) amt,
    sum(amount)/ sum(receive_qty) as avg_price
FROM csx_dw.dws_wms_r_d_entry_detail a 
join 
(    select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' 
                WHEN province_name LIKE '%上海%'   then town_code
    else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then ''
            WHEN province_name LIKE '%上海%'   then town_name
    else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市'

    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1
    and purpose in ('01','03','07','08','02')) b on a.receive_location_code=shop_id
WHERE sdt>='20210101'
  AND sdt<'20211201'
  AND order_type_code LIKE 'P%'
  and business_type='01'
  and a.receive_status=2
  and (a.goods_name like '%鸡块%' or a.goods_name like '%翅尖%'  or a.goods_name like '%翅根%' )
 GROUP BY 
   substr(sdt,1,6),
   -- receive_location_code,
    goods_code,
    goods_name,
    department_code,
    department_name,
    unit,
    spec,
    brand_name
    ;
    
    
--薯条+薯饼+红薯条

SELECT substr(sdt,1,6) as mon,
   -- receive_location_code,
    goods_code,
    goods_name,
    department_code,
    department_name,
    unit,
    spec,
    brand_name,
    sum(receive_qty) qty,
    sum(amount) amt,
    sum(amount)/ sum(receive_qty) as avg_price
FROM csx_dw.dws_wms_r_d_entry_detail a 
join 
(    select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' 
                WHEN province_name LIKE '%上海%'   then town_code
    else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then ''
            WHEN province_name LIKE '%上海%'   then town_name
    else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市'

    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1
    and purpose in ('01','03','07','08','02')) b on a.receive_location_code=shop_id
WHERE sdt>='20210101'
  AND sdt<'20211201'
  AND order_type_code LIKE 'P%'
  and business_type='01'
  and a.receive_status=2
  and (a.goods_name like '%薯条%' or a.goods_name like '%薯饼%')
 GROUP BY 
   substr(sdt,1,6),
   -- receive_location_code,
    goods_code,
    goods_name,
    department_code,
    department_name,
    unit,
    spec,
    brand_name
    ;
    
    
 
    
--薯条+薯饼+红薯条

SELECT substr(sdt,1,6) as mon,
   -- receive_location_code,
    goods_code,
    goods_name,
    department_code,
    department_name,
    unit,
    spec,
    brand_name,
    sum(receive_qty) qty,
    sum(amount) amt,
    sum(amount)/ sum(receive_qty) as avg_price
FROM csx_dw.dws_wms_r_d_entry_detail a 
join 
(    select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' 
                WHEN province_name LIKE '%上海%'   then town_code
    else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then ''
            WHEN province_name LIKE '%上海%'   then town_name
    else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市'

    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1
    and purpose in ('01','03','07','08','02')) b on a.receive_location_code=shop_id
WHERE sdt>='20210101'
  AND sdt<'20211201'
  AND order_type_code LIKE 'P%'
  and business_type='01'
  and a.receive_status=2
  and (a.goods_name like '%奶酪%'  )
 GROUP BY 
   substr(sdt,1,6),
   -- receive_location_code,
    goods_code,
    goods_name,
    department_code,
    department_name,
    unit,
    spec,
    brand_name
    ;