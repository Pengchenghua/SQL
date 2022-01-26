--采购供应商21年入库查询
set s_date='20210101';
set e_date='20211231';

drop table csx_tmp.temp_entry_00 ;
create table csx_tmp.temp_entry_00 as
select   order_no,
    dc_code,
    business_type,
    business_type_name,
    goods_code,
    goods_name,
    unit_name,
    brand_name,
    department_id,
    department_name,
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
    supplier_code,
    vendor_name,
    send_dc_code,
    (receive_qty) receive_qty,
    (receive_amt) receive_amt,
    (no_tax_receive_amt) as no_tax_receive_amt,
    (shipped_qty) shipped_qty,
    (shipped_amt) shipped_amt,
    (no_tax_shipped_amt) as no_tax_shipped_amt,
    receive_sdt
from 
(
select origin_order_code order_no,
    receive_location_code as dc_code,
    business_type,
    business_type_name,
    goods_code,
    supplier_code,
    send_location_code as send_dc_code,
    (receive_qty) receive_qty,
    (price/(1+tax_rate/100)*receive_qty) as no_tax_receive_amt,
    (price*receive_qty) as receive_amt,
    0 shipped_qty,
    0 shipped_amt,
    0 no_tax_shipped_amt,
    regexp_replace( to_date(receive_time ),'-','') receive_sdt
from csx_dw.dws_wms_r_d_entry_batch a 

where (( sdt<=${hiveconf:e_date}
    and sdt >=${hiveconf:s_date}) or sdt='19990101')
    -- and regexp_replace( to_date(receive_time ),'-','')<= ${hiveconf:e_date}
    -- and  regexp_replace( to_date(receive_time ),'-','')>=${hiveconf:s_date}
    and order_type_code like 'P%'
    and business_type !='02'
    and receive_status in ('1','2')
union all 
select origin_order_no order_no, 
    shipped_location_code  as dc_code,
    business_type_code as business_type,
    business_type_name,
    goods_code,
    supplier_code,
    shipped_location_code as send_dc_code,
    0 receive_qty,
    0 no_tax_receive_amt,
    0 receive_amt,
    (shipped_qty) shipped_qty,
    (price*shipped_qty) as shipped_amt,
    (price/(1+tax_rate/100)*shipped_qty) as no_tax_shipped_amt,
    sdt as receive_sdt
from csx_dw.dws_wms_r_d_ship_detail
where sdt >=   ${hiveconf:s_date}
   and sdt <=${hiveconf:e_date}
    and order_type_code like 'P%'
  --  and business_type_code in ('05')
    and status in ('6','7','8')
) a 
join 
(SELECT goods_id,
       goods_name,
       unit_name,
       brand_name,
       department_id,
       department_name,
       division_code,
       division_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       category_large_code,
       category_large_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') b on a.goods_code=b.goods_id
join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code=c.vendor_id
;



drop table  csx_tmp.temp_cg_01;
create  table csx_tmp.temp_cg_01 as 
select substr(receive_sdt,1,6) mon,
     purchase_org,
    purchase_org_name,
    -- sales_region_code,
    -- sales_region_name ,
    j.order_code,
    sales_province_code,
    sales_province_name,
    source_type_name,
    CASE
            WHEN j.super_class='1'
                THEN '供应商订单'
            WHEN super_class='2'
                THEN '供应商退货订单'
            WHEN super_class='3'
                THEN '配送订单'
            WHEN super_class='4'
                THEN '返配订单'
                ELSE super_class
        END super_class_name  ,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    unit_name,
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
    category_large_name,
    j.supplier_code,
    vendor_name,
    j.send_dc_code,
    send_dc_name,
    local_purchase_flag,
    business_type_name,
    receive_qty,
    receive_amt,
    shipped_qty,
    shipped_amt,

  --  no_tax_receive_amt,
  --  no_tax_shipped_amt,
    receive_sdt,
    yh_reuse_tag,
    case when purpose ='07' then '20' when yh_reuse_tag='是' then '21' when  division_code in ('11','10') then '11' when  division_code in ('12','13','14','15') then '12' end supplier_type_code ,
    case when purpose ='07' then 'BBC' when yh_reuse_tag='是' then '复用供应商' when  division_code in ('11','10') then '生鲜' when  division_code in ('12','13','14','15') then '食百' end  supplier_type_name,
    purpose,
    purpose_name
from 
(select sales_province_code,
    sales_province_name,
     purchase_org,
    purchase_org_name,
    sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    b.order_code,
    super_class,
    source_type,
    source_type_name,
    local_purchase_flag,
    a. order_no,
    dc_code,
    shop_name,
    business_type,
    business_type_name,
    goods_code,
    goods_name,
    unit_name,
    brand_name,
    department_id,
    department_name,
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
    supplier_code,
    vendor_name,
    send_dc_code,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    receive_sdt     --发货日期&收货日期
from csx_tmp.temp_entry_00 a 
join 
(select  order_code,
    super_class,
    source_type,
    source_type_name,
    local_purchase_flag
    from csx_dw.dws_scm_r_d_header_item_price a
    where super_class in ('2','1')  
    -- and source_type in ('1','10')
    group by  order_code,
        source_type,
        source_type_name,
        local_purchase_flag,
        super_class
)b on a.order_no=b.order_code
 join 
(select sales_province_code,
    sales_province_name,
    purchase_org,
    purchase_org_name,
    case when (purchase_org ='P620' and purpose!='07') or shop_id='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    company_code,
    company_name ,
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
    and  table_type=1) d on a.dc_code=d.shop_id
) j 
left join  
(select company_code,supplier_code,yh_reuse_tag 
from csx_tmp.ads_fr_r_m_supplier_reuse 
where months=substr(${hiveconf:e_date},1,6) ) s on j.company_code=s.company_code and j.supplier_code=s.supplier_code
left join
(select 
    shop_id as send_dc_code,
    shop_name as send_dc_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
) m on j.send_dc_code=m.send_dc_code
 

;


select supplier_code,vendor_name ,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    sum(receive_amt) receiva_amt ,
    sum(shipped_amt) shipped_amt
from csx_tmp.temp_cg_01 
where super_class_name like '供应商%'
    and business_type_name in ('供应商配送','退供出库')
group by supplier_code,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    vendor_name;
    

select distinct business_type_name
from csx_tmp.temp_cg_01 
where super_class_name like '供应商%'
    and business_type_name in ('供应商配送','退供出库');