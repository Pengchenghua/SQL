-- 管理中类供应商入库查询 classify_middle_code in('B0603','B0701') 徐力需求
-- 供应商货源
select d.*,qty,amt from 

(select a.product_code,
    product_name,
    location_code,
    location_name,
    supplier_code,
    supplier_name,
    def_flag,
    product_status_name,    
    joint_purchase
from csx_ods.source_basic_w_a_scm_source_dimension_relation a 
left join
(select product_code,shop_code,product_status_name from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') c on a.location_code=c.shop_code and a.product_code=c.product_code
left join 
(select vendor_id,joint_purchase from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id
left join 
(select goods_id,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') g on a.product_code=g.goods_id

where sdt='20211128'
and g.classify_middle_code in('B0603','B0701')
and a.location_code in ('WA93','W0A2','W080','W0K7','W0L4','W0AW','W0J8','W048','WB04','W0A3','WB11',
        'W0A8','WB03','W053','W0F4','W0G9','W0K6','W0AH','W0AJ','W0J2','W0F7','W0G6','WA96','W0K1','W0AU','W0L3',
        'W0BK','W0AL','W0S9','W0Q2','W0Q9','W0Q8','W0BS','W0BH','W0BR','W0R9','WB00','W0R8','W088','W0BZ','W0A5',
        'W0P8','WA94','W0AS','W0AR','WA99','W0N1','W079','W0A6','W0BD','W0N0','WB01','W0P3','W0W7','W0X1','W0X2',
        'W0Z8','W0Z9','W0AZ','W039','W0A7')
)d 
left join 
(select a.province_code,
    a.province_name,
    a.receive_location_code,
    a.receive_location_name,
    supplier_code,
    a.supplier_name,
    a.goods_code,
    a.goods_name,
    a.brand_name,
    a.unit,
    sum(a.receive_qty) qty,
    sum(a.amount) as amt
from csx_dw.dws_wms_r_d_entry_batch as a

where sdt>='20210101' and sdt<='20211125'
    and a.classify_middle_code in('B0603','B0701')
    and business_type_name like '供应商配送'
    and a.receive_status='2'
    
    and receive_status='2'
group by  a.province_code,
    a.province_name,
    a.receive_location_code,
   a.receive_location_name,
    supplier_code,
    a.supplier_name,
    a.goods_code,
    a.goods_name,
    a.brand_name,
    a.unit
) b on d.location_code=b.receive_location_code and d.product_code=b.goods_code and d.supplier_code=b.supplier_code;



-- 供应商货源入库

-- 管理中类供应商入库查询 classify_middle_code in('B0603','B0701') 徐力需求
drop table csx_tmp.temp_aa;
create temporary table csx_tmp.temp_aa as 
select d.*,qty,amt from 
(select s.sales_region_code,
    s.sales_region_name,
    s.province_code,
    s.province_name,
    a.product_code,
    product_name,
    unit_name,
    brand_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    location_code,
    location_name,
    supplier_code,
    supplier_name,
    def_flag,
    product_status_name,    
    joint_purchase
from csx_ods.source_basic_w_a_scm_source_dimension_relation a 
left join 
(select 
    sales_province_code,
    sales_province_name,
    purchase_org,
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
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市' 
    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    
) s on a.location_code=s.shop_id
left join
(select product_code,shop_code,product_status_name from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') c on a.location_code=c.shop_code and a.product_code=c.product_code
left join 
(select vendor_id,joint_purchase from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id
left join 
(select goods_id,
    unit_name,
    brand_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') g on a.product_code=g.goods_id

where sdt='20211128'
and g.classify_middle_code in('B0603','B0701','B0103','B0102')
and a.location_code in ('WA93','W0A2','W080','W0K7','W0L4','W0AW','W0J8','W048','WB04','W0A3','WB11',
        'W0A8','WB03','W053','W0F4','W0G9','W0K6','W0AH','W0AJ','W0J2','W0F7','W0G6','WA96','W0K1','W0AU','W0L3',
        'W0BK','W0AL','W0S9','W0Q2','W0Q9','W0Q8','W0BS','W0BH','W0BR','W0R9','WB00','W0R8','W088','W0BZ','W0A5',
        'W0P8','WA94','W0AS','W0AR','WA99','W0N1','W079','W0A6','W0BD','W0N0','WB01','W0P3','W0W7','W0X1','W0X2',
        'W0Z8','W0Z9','W0AZ','W039','W0A7')
)d 
left join 
(select a.province_code,
    a.province_name,
    a.receive_location_code,
    a.receive_location_name,
    supplier_code,
    a.supplier_name,
    a.goods_code,
    a.goods_name,
    a.brand_name,
    a.unit,
    sum(a.receive_qty) qty,
    sum(a.amount) as amt
from csx_dw.dws_wms_r_d_entry_batch as a
where sdt>='20210101' and sdt<='20211125'
    and a.classify_middle_code in('B0603','B0701','B0103','B0102')
    and business_type_name like '供应商配送'
    and a.receive_status='2'
    
    and receive_status='2'
group by  a.province_code,
    a.province_name,
    a.receive_location_code,
   a.receive_location_name,
    supplier_code,
    a.supplier_name,
    a.goods_code,
    a.goods_name,
    a.brand_name,
    a.unit
) b on d.location_code=b.receive_location_code and d.product_code=b.goods_code and d.supplier_code=b.supplier_code;


select * from csx_tmp.temp_aa;