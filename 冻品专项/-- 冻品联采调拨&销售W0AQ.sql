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


--冻品入库与联采商品占比
DROP TABLE csx_tmp.temp_order_entry_01;
create  table csx_tmp.temp_order_entry_01 as 
select  mon,
    province_code,
    province_name,
    city_code,
    city_name,
    purpose,
    purpose_name,
    origin_order_code,
    source_type_name,
    source_type,
    dc_code,
    shop_name,
    settlement_dc,
    goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
    brand_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    department_id,
    department_name,
    supplier_code,
    vendor_name,
    (qty) qty,
    (amt) amt,
    (shipp_qty )shipp_qty,
    (shipp_amt )shipp_amt,
    (qty-shipp_qty) net_qty,
    (amt-shipp_amt) net_amt
from 
(select substr(sdt,1,6) mon,
    a.origin_order_code,
    receive_location_code dc_code,
    a.settlement_dc,
    goods_code,
    supplier_code,
    (case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) qty,
    (price*case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) amt,
    0 shipp_qty,
    0 shipp_amt
from csx_dw.dws_wms_r_d_entry_detail a
where 1=1 
and sdt>='20210101' 
and sdt<'20211101'
and receive_status in (1,2)
and a.order_type_code LIKE 'P%' and business_type !='02'
union all 
select substr(sdt,1,6) mon,
    a.origin_order_no origin_order_code,
    shipped_location_code dc_code,
    a.settlement_dc,
    goods_code,
    supplier_code,
    0 qty,
    0 amt,
    (shipped_qty) shipp_qty,
    (shipped_qty*price) shipp_amt
from csx_dw.dws_wms_r_d_ship_detail a
where 1=1
-- supplier_code in ('20046634','20042204','20051662','20043882','20024248','20029976','20028053','20048472','20043203','20043203','20041365','20038251')
    and sdt>='20210101'
    and sdt<'20211101'
    and status in ('6','7','8')
    AND (( order_type_code LIKE 'P%'  and business_type_code ='05'))
    ) a 
 join 
 (SELECT shop_id,
       sales_region_code,
       sales_region_name,
       sales_province_code,
       sales_province_name,
       province_code,
       province_name,
       purpose,
       purpose_name,
       shop_name,
       city_code,
       city_name
FROM csx_dw.dws_basic_w_a_csx_shop_m
WHERE sdt='current'
  AND table_type=1 
  --and purchase_org !='P620'
  --and shop_id not in ('W0J8','W0K4')
  AND purpose IN ('01',
                  '02',
                  '03',
                  '08',
                  '07',
                -- '06', 合伙人仓
                  '05' --彩食鲜小店
                -- '04' 寄售小店
                  )) b on a.dc_code=b.shop_id
join 
(SELECT goods_id,
       goods_name,
       spu_goods_code,
       spu_goods_name,
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
    -- and classify_middle_code ='B0302'
)c on a.goods_code=c.goods_id
join 
(SELECT vendor_id,vendor_name
FROM csx_dw.dws_basic_w_a_csx_supplier_m
WHERE sdt='current'
)d  on a.supplier_code=d.vendor_id
left join
(select order_code,source_type_name,source_type 
from csx_dw.dws_scm_r_d_header_item_price 
group by order_code,source_type_name,source_type )f on a.origin_order_code=f.order_code
 where 1=1
 and (source_type !='15' or source_type is null)
    -- and classify_large_code in('B03','B02')
    -- and supplier_code not in ('20020295','B10008','20020588')
    -- group by 
    -- mon,
    -- province_code,
    -- province_name,
    -- purpose,
    -- purpose_name,
    -- dc_code,
    -- shop_name,
    -- goods_code,
    -- goods_name,
    -- spu_goods_code,
    -- spu_goods_name,
    -- brand_name,
    -- classify_middle_code,
    -- classify_middle_name,
    -- classify_small_code,
    -- classify_small_name,
    -- supplier_code,
    -- vendor_name,
    -- city_code,
    -- city_name,
    -- classify_large_code,
    -- classify_large_name,
    -- department_id,
    -- department_name
    ;
    
    
select mon,sum(net_amt)/10000  from csx_tmp.temp_order_entry_01 a 
join 
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on a.goods_code=b.product_code and a.dc_code=b.shop_code
where joint_purchase_flag='1' and classify_middle_code='B0304' group by mon;



select  
    substr(sdt,1,6) mon,
    province_code,
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
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on a.goods_code=b.product_code and a.dc_code=b.shop_code
where sdt>='20210101' and sdt<='20211031'
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
    joint_purchase_flag,
    substr(sdt,1,6) ;
    
    
    --冻品销售与联采商品销售占比
select  
    substr(sdt,1,6) mon,
    sum(a.sales_qty) sales_qty,
    sum(a.sales_value) sales_value,
    sum(a.excluding_tax_sales) as excluding_tax_sales,
    sum(a.excluding_tax_profit)excluding_tax_profit,
    sum(a.excluding_tax_profit)/sum(a.excluding_tax_sales) profit_rate,
    sum(a.excluding_tax_cost)excluding_tax_cost
from csx_dw.dws_sale_r_d_detail a 
left join 
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on a.goods_code=b.product_code and a.dc_code=b.shop_code
where sdt>='20210101' and sdt<='20211031'
and a.business_type_code!='4'
and a.channel_code in ('1','7','9')
and a.classify_middle_code='B0304'
-- and b.joint_purchase_flag='1'
group by 
    substr(sdt,1,6) ;
  
--  create temporary table   csx_tmp.temp_order_entry_02 as 
    select *  from csx_tmp.temp_order_entry_01 a 
    left join  
    (select distinct product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on a.goods_code=b.product_code 
        where  joint_purchase_flag='1' and classify_middle_code='B0304'
    ;
    
    show create table csx_dw.dws_basic_w_a_csx_product_m;