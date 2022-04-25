-- '20046634','20042204','20051662','20043882','20024248','20029976','20028053','20048472','20043203','20043203','20041365','20038251'
--益海供应商-油入库&销售
create temporary table csx_tmp.temp_supp_sale as 
select  mon,
    sales_region_code,
    sales_region_name,
    sales_province_code,
    sales_province_name,
    purpose,
    purpose_name,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    brand_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    supplier_code,
    vendor_name,
    sum(qty) qty,
    sum(amt) amt,
    sum(shipp_qty )shipp_qty,
    sum(shipp_amt )shipp_amt,
    sum(qty-shipp_qty) net_qty,
    sum(amt-shipp_amt) net_amt
from 
(select substr(sdt,1,6) mon,
    province_code,
    receive_location_code dc_code,
    goods_code,
    supplier_code,
    sum(receive_qty) qty,
    sum(price*receive_qty) amt,
    0 shipp_qty,
    0 shipp_amt
from csx_dw.dws_wms_r_d_entry_detail 
where supplier_code in ('20046634','20042204','20051662','20043882','20024248','20029976','20028053','20048472','20043203','20043203','20041365','20038251')
and sdt>='20210101' 
and sdt<'20210901'
and receive_status in ('1','2')
group by substr(sdt,1,6) ,
    province_code,
    province_name,
    receive_location_code  ,
    goods_code,
    supplier_code
union all 
select substr(regexp_replace(to_date(send_time),'-',''),1,6) mon,
    province_code,
    shipped_location_code dc_code,
    goods_code,
    supplier_code,
    0 qty,
    0 amt,
    sum(shipped_qty) shipp_qty,
    sum(shipped_qty*price) shipp_amt
from csx_dw.dws_wms_r_d_ship_detail 
where supplier_code in ('20046634','20042204','20051662','20043882','20024248','20029976','20028053','20048472','20043203','20043203','20041365','20038251')
    and send_time>='2021-01-01 00:00:00'
    and send_time<'2021-09-01 00:00:00'
    and status !='9'
group by substr(regexp_replace(to_date(send_time),'-',''),1,6) ,
    province_code,
    province_name,
    shipped_location_code  ,
    supplier_code,
    goods_code
    ) a 
 join 
 (select shop_id,sales_region_code,sales_region_name,sales_province_code,sales_province_name,purpose,purpose_name,shop_name from csx_dw.dws_basic_w_a_csx_shop_m 
where sdt='current' 
    and table_type=1 
   -- and purchase_org !='P620' 
    -- and dist_code='15'
    and purpose in ('01','02','03','08','07') ) b on a.dc_code=b.shop_id
join 
(SELECT goods_id,
       goods_name,
       brand_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current')c on a.goods_code=c.goods_id
join 
(SELECT vendor_id,vendor_name
FROM csx_dw.dws_basic_w_a_csx_supplier_m
WHERE sdt='current')d  on a.supplier_code=d.vendor_id
    group by 
    mon,
    sales_region_code,
    sales_region_name,
    sales_province_code,
    sales_province_name,
    purpose,purpose_name,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
    brand_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    supplier_code,
    vendor_name
    ;
    
    
    select 
    substr(sdt,1,6) mon,
    a.region_code,
    region_name ,
    a.province_code,
    a.province_name,
    a.channel_code,
    a.channel_name,
    a.business_type_code,
    a.business_type_name,
    a.goods_code,
    a.goods_name,
    b.brand_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name,
    sum(a.sales_qty) qty,
    sum(a.sales_value) sales,
    sum(profit)profit
    from csx_dw.dws_sale_r_d_detail a 
    join 
    (select distinct a.goods_code,a.brand_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    from  csx_tmp.temp_supp_sale a  ) b on a.goods_code=b.goods_code
    where sdt >='20210101'
        and sdt<'20210901'
    group by 
       substr(sdt,1,6) ,
    a.region_code,
    region_name ,
    a.province_code,
    a.province_name,
    a.channel_code,
    a.channel_name,
    a.business_type_code,
    a.business_type_name,
    a.goods_code,
    a.goods_name,
    b.brand_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name;