--供应商入库明细 &供应商需求【徐力】
select sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    dc_code,
    shop_name,
    purchase_org,
    purpose,
    purpose_name,
    a.acct_grp,
    acct_grp_name,
    supplier_code,
    supplier_name,
    purchase_frozen,
    frozen,
    create_date,
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
    ) b on a.receive_location_code =b.shop_id
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
 left join 
(select a.supplier_code,a.purchase_org,a.pay_condition,dic_value from csx_ods.source_basic_w_a_md_purchasing_info a
 LEFT JOIN 
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20211019' and dic_type='ACCOUNTCYCLE')b on a.pay_condition=b.dic_key
 where sdt='20211019') d on a.supplier_code=d.supplier_code  and a.purchase_org=d.purchase_org
 LEFT JOIN 
 (select vendor_id,purchase_frozen,frozen,industry_sector,create_date,a.acct_grp,dic_value as acct_grp_name from   csx_dw.dws_basic_w_a_csx_supplier_m a 
 LEFT JOIN 
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20211019' and dic_type='VENDERAGROUP') b on a.acct_grp=b.dic_key
 where sdt='current')  c on a.supplier_code=c.vendor_id

where sdt>='20210101' 
and a.order_type_code LIKE 'P%' and business_type !='02'
and sales_region_code!='9'
and b.purpose in ('01','07')
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
    ;


    --供应商信息
    --供应商入库明细 &供应商需求【徐力】
drop table  csx_tmp.temp_supplier_a ;
create temporary table csx_tmp.temp_supplier_a as 
select 
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    a.receive_location_code,
    shop_name,
    purchase_org,
    b.purpose,
    b.purpose_name,
    acct_grp,
    acct_grp_name,
    supplier_code,
    supplier_name,
    purchase_frozen,
    frozen,
    create_date,
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
    ) b on a.receive_location_code =b.shop_id
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

 LEFT JOIN 
 (select vendor_id,purchase_frozen,frozen,industry_sector,create_date,a.acct_grp,dic_value as acct_grp_name from   csx_dw.dws_basic_w_a_csx_supplier_m a 
 LEFT JOIN 
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20211019' and dic_type='VENDERAGROUP') b on a.acct_grp=b.dic_key
 where sdt='current')  f on a.supplier_code=f.vendor_id
where sdt>='20210101' 
and ((a.order_type_code LIKE 'P%' and business_type !='02')
    or ( a.business_type in ('ZN01','ZN02') AND a.sys='old'))
--and sales_region_code!='9'
-- and b.purpose in ('01','07')
group by sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    a.receive_location_code,
    shop_name,
    purchase_org,
    b.purpose,
    b.purpose_name,
    acct_grp,
    acct_grp_name,
    supplier_code,
    supplier_name,
    purchase_frozen,
    frozen,
    create_date,
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
    c.department_name
    ;
    
    select * from csx_tmp.temp_supplier_a;
    
    
    show create table csx_dw.dws_basic_w_a_csx_supplier_m ;