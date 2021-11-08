-- 冻品联采调拨&销售W0AQ--财务谢艳艳
select shop_id  from csx_ods.source_basic_frozen_dc where sdt=regexp_replace(date_sub(current_date(),1),'-','')  and is_frozen_dc='1';
-- 联采冻品：经过W0AQ调拨或直接销售的冻品sku（冻品仓W0AQ+冻品联采sku）

select 
    c.province_code,
    c.province_name,
    settlement_dc,
    settlement_dc_name,
    receive_location_code,
    receive_location_name,
    goods_code,
    goods_name,
    unit,
    joint_purchase_flag,
    sum(receive_qty) shipped_qty,
    sum(amount)shipped_amount,
    sum(receive_qty*(price/(1+tax_rate/100))) as no_tax_shipped_amt
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') b on a.receive_location_code=b.shop_code and a.goods_code=b.product_code
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
    and  table_type=1 ) c on a.receive_location_code=c.shop_id
where sdt>='20211001' 
    and sdt<='20211031' 
and settlement_dc='W0AQ'
group by settlement_dc,
    settlement_dc_name,
    receive_location_code,
    receive_location_name,
    goods_code,
    goods_name,
    joint_purchase_flag,
    c.province_code,
    c.province_name,
    unit
    ;

select  province_code,
    province_name,
    a.dc_code,
    a.dc_name,
    goods_code,
    goods_name,
    unit,
    joint_purchase_flag,
    sum(a.sales_qty) sales_qty,
    sum(a.sales_value) sales_value,
    sum(a.excluding_tax_sales) as excluding_tax_sales,
    sum(a.excluding_tax_profit)excluding_tax_profit,
    sum(a.excluding_tax_cost)excluding_tax_cost
from csx_dw.dws_sale_r_d_detail a 
join 
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') b on a.goods_code=b.product_code and a.dc_code=b.shop_code
where sdt>='20211001' and sdt<='20211031'
and a.business_type_code!='4'
and a.channel_code in ('1','7','9')
and a.classify_middle_code='B0304'
and b.joint_purchase_flag='1'
group by province_code,
    province_name,
    a.dc_code,
    a.dc_name,
    goods_code,
    goods_name,
    unit,
    joint_purchase_flag;