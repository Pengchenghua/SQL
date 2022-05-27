-- 供应链指定仓商品入库&销售【徐力】drop table  csx_tmp.temp_goods ;

set shop= ('WA93','W0A2','W080','W0K7','W0L4','W0AW','W0J8','W048','WB04','W0A3','WB11','W0A8','WB03','W053','W0F4','W0G9','W0K6','W0AH','W0AJ','W0J2','W0F7','W0G6','WA96','W0K1','W0AU','W0L3','W0BK','W0AL','W0S9','W0Q2','W0Q9','W0Q8','W0BS','W0BH','W0BR','W0R9','WB00','W0R8','W088','W0BZ','W0A5','W0P8','WA94','W0AS','W0AR','WA99','W0N1','W079','W0A6','W0BD','W0N0','WB01','W0P3','W0W7','W0X1','W0X2','W0Z8','W0Z9','W0AZ','W039','W0A7','W0BT');
set sdt='20220101';
set edt='20220526';
create temporary table csx_tmp.temp_goods as 
select shop_code,
    product_code,
    bar_code,
    goods_name,
    des_specific_product_status,
    product_status_name,
    regionalized_trade_names,
    unit_name,
    b.brand_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name
from csx_dw.dws_basic_w_a_csx_product_info a 
left join
(select goods_id,
    bar_code,
    goods_name,
    unit_name,
    brand_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.product_code=b.goods_id
where sdt='current'
and shop_code in ${hiveconf:shop}
    
    ;
drop table  csx_tmp.temp_goods_01 ;
create temporary table csx_tmp.temp_goods_01 as 
select receive_location_code,
    goods_code,
    -- sum(case when sdt between '20220101' and '20220131'  then  receive_qty end ) mon3_qty,
    -- sum(case when sdt between '20220201' and '20220131'  then  amount end ) mon3_amt,
    -- sum(case when sdt between '20220301' and '20220131'  then  receive_qty end ) mon6_qty,
    -- sum(case when sdt between '20220401' and '20220131'  then  amount end ) mon6_amt,
    sum(receive_qty) year_qty,
    sum(amount) year_amt
from csx_dw.dws_wms_r_d_entry_detail
where sdt>=${hiveconf:sdt}
    and sdt<=${hiveconf:edt}
    and order_type_code like 'P%'
    and business_type='01'
    and receive_location_code in ${hiveconf:shop}
    and receive_status='2'
group by receive_location_code,
    goods_code;
    
    
drop table  csx_tmp.temp_goods_02 ;
create temporary table csx_tmp.temp_goods_02 as 
select dc_code,
    goods_code,
    -- sum(case when sdt between '20211101' and '20220131' then  sales_qty end ) mon3_qty,
    -- sum(case when sdt between '20211101' and '20220131' then  sales_value end ) mon3_amt,
    -- sum(case when sdt between '20210801' and '20220131' then  sales_qty end ) mon6_qty,
    -- sum(case when sdt between '20210801' and '20220131' then  sales_value end ) mon6_amt,
    sum(sales_qty) year_qty,
    sum(sales_value) year_amt
from csx_dw.dws_sale_r_d_detail
where sdt>=${hiveconf:sdt}
    and sdt<=${hiveconf:edt}
    and dc_code in ${hiveconf:shop}
group by dc_code,
    goods_code;
    

drop table  csx_tmp.temp_goods_03 ;
create temporary table csx_tmp.temp_goods_03 as 
select 
    province_code,
    province_name,
    city_code,
    city_name,
    a.shop_code,
    shop_name,
    product_code,
    bar_code,
    goods_name,
    des_specific_product_status,
    product_status_name,
    regionalized_trade_names,
    unit_name,
     division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    -- b.mon3_qty,
    -- b.mon3_amt,
    -- b.mon6_qty,
    -- b.mon6_amt,
    b.year_qty,
    b.year_amt,
    -- c.mon3_qty sales_mon3_qty,
    -- c.mon3_amt sales_mon3_amt,
    -- c.mon6_qty sales_mon6_qty,
    -- c.mon6_amt sales_mon6_amt,
    c.year_qty sales_year_qty,
    c.year_amt as sales_year_amt

from  csx_tmp.temp_goods a 
left join 
 csx_tmp.temp_goods_01 b on a.shop_code=b.receive_location_code and a.product_code=b.goods_code
 left join csx_tmp.temp_goods_02 c on a.shop_code=c.dc_code and a.product_code=c.goods_code
 left join 
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
    and  table_type=1 ) j on a.shop_code=j.shop_id
    ;
    
    select * from csx_tmp.temp_goods_03;