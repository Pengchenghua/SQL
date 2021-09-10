
--生鲜净入库情况20210910
select * from  csx_tmp.temp_supp_sale ;
create temporary table csx_tmp.temp_supp_sale as 
select  mon,
    sales_region_code,
    sales_region_name,
    sales_province_code,
    sales_province_name,
    city_code,
    city_name,
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
where 1=1 
-- and supplier_code in ('20046634','20042204','20051662','20043882','20024248','20029976','20028053','20048472','20043203','20043203','20041365','20038251')
and sdt>='20210101' 
and sdt<'20210901'
and receive_status in (1,2)
and order_type_code like 'P%'
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
where 1=1
-- supplier_code in ('20046634','20042204','20051662','20043882','20024248','20029976','20028053','20048472','20043203','20043203','20041365','20038251')
    and send_time>='2021-01-01 00:00:00'
    and send_time<'2021-09-01 00:00:00'
    and status in ('6','7','8')
    AND order_type_code LIKE 'P%'
    and business_type_code ='05'
group by substr(regexp_replace(to_date(send_time),'-',''),1,6) ,
    province_code,
    province_name,
    shipped_location_code  ,
    supplier_code,
    goods_code
    ) a 
 join 
 (select shop_id,sales_region_code,sales_region_name,sales_province_code,sales_province_name,purpose,purpose_name,shop_name,city_code,city_name from csx_dw.dws_basic_w_a_csx_shop_m 
where sdt='current' 
    and table_type=1 
   -- and purchase_org !='P620' 
    -- and dist_code='15'
    and purpose in ('01','02','03','08','07','06') ) b on a.dc_code=b.shop_id
join 
(SELECT goods_id,
       goods_name,
       brand_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current'
    and division_code ='11'
)c on a.goods_code=c.goods_id
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
    vendor_name,
    city_code,
    city_name
    ;
    
    