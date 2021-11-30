--采购退货监控【内控需求】
-- 1.0月入库额
drop table  csx_tmp.temp_entry_01 ;
CREATE temporary table csx_tmp.temp_entry_01 as 
SELECT sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    count(DISTINCT order_code) receive_order_cn,
    sum(amount) receive_amt
FROM csx_dw.dws_wms_r_d_entry_detail a 
LEFT JOIN 
(select 
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
    and  table_type=1 ) b on a.receive_location_code=b.shop_id
WHERE order_type_code LIKE 'P%'
  AND business_type='01'
  and sdt>='20211101'
   and b.purpose!='06'
  and a.receive_status='2'
  GROUP BY sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    sdt
    
    ;
    
    
--2.0 退货统计
drop table  csx_tmp.temp_entry_02 ;
CREATE temporary table csx_tmp.temp_entry_02 as 
SELECT sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    count(DISTINCT order_no) receive_order_cn,
    sum(a.shipped_amount) shipped_amount
FROM csx_dw.dws_wms_r_d_ship_detail a 
LEFT JOIN 
(select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' WHEN province_name LIKE '%上海%'   then town_code else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' WHEN province_name LIKE '%上海%'   then town_name else city_name end  city_name,
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
    and  table_type=1 ) b on a.shipped_location_code=b.shop_id
WHERE a.order_type_code LIKE 'P%'
  AND a.business_type_code='05'
  and return_flag='Y'
  and sdt>='20211101'
  and b.purpose!='06'
  and a.status in ('6','8')
  GROUP BY sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    sdt
    ;

-- 无效订单处理    
select sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    count(DISTINCT case when a.header_status='5' then  a.order_code end ) invalid_order_cn,         --无效订单
    count (distinct case when to_date(a.items_close_time)>a.last_delivery_date then a.order_code end ) timeout_order_cn,
    sum(case when a.header_status='5' then  a.amount_free_tax end ) invalid_order_amount,
    sum(case when to_date(a.items_close_time)>a.last_delivery_date then a.amount_free_tax end ) timeout_order_amount
from csx_dw.dws_scm_r_d_order_detail a 
LEFT JOIN 
(select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' WHEN province_name LIKE '%上海%'   then town_code else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' WHEN province_name LIKE '%上海%'   then town_name else city_name end  city_name,
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
    and  table_type=1 ) b on a.target_location_code=b.shop_id
where (sdt>='20211101' or sdt='19990101') and header_status in ('4','5')
and super_class ='1'
AND a.source_type in ('1','9','10')  --采购导入 9 工厂采购  10 智能补货
group by  sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name
;

show create table csx_dw.dws_scm_r_d_order_detail;