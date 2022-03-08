-- TOP供应商占比【财务】
DROP TABLE csx_tmp.temp_order_entry_t1;
create  table csx_tmp.temp_order_entry_t1 as
select  mon,
    source_type_name,
    source_type ,
    a.origin_order_code,
    dc_code,
    goods_code,
    supplier_code,
    qty,
    amt,
    shipp_qty,
    shipp_amt
from(
select  mon,
    a.origin_order_code,
    dc_code,
    goods_code,
    supplier_code,
    sum(qty) qty,
    sum(amt) amt,
    sum(shipp_qty) shipp_qty,
    sum(shipp_amt) shipp_amt
from(
select substr(sdt,1,6) mon,
    a.origin_order_code,
    receive_location_code dc_code,
    goods_code,
    supplier_code,
    (case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) qty,
    (price*case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) amt,
    0 shipp_qty,
    0 shipp_amt
from csx_dw.dws_wms_r_d_entry_detail a
where 1=1 
and sdt>='20210101' 
and sdt<'20220101'
and receive_status in (1,2)
and (a.business_type in ('ZN01','ZN02','ZNR1','ZNR2')
       OR (a.order_type_code LIKE 'P%' and business_type !='02')  )
union all 
select substr(sdt,1,6) mon,
    a.origin_order_no origin_order_code,
    shipped_location_code dc_code,
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
    and sdt<'20220101'
    and status in ('6','7','8')
    AND (( order_type_code LIKE 'P%'  and business_type_code ='05') or a.business_type_code in ('ZN01','ZN02','ZNR1','ZNR2'))
    
 ) a   
 group by mon,
    a.origin_order_code,
    dc_code,
    goods_code,
    supplier_code
) a 
left join 
(select order_code,    
case when source_type = 1 then '采购导入'
    when source_type = 2 then '直送客户'
    when source_type = 3 then '一键代发'
    when source_type = 4 then '项目合伙人'
    when source_type = 5 then '无单入库'
    when source_type = 6 then '寄售调拨'
    when source_type = 7 then '自营调拨'
    when source_type = 8 then '云超物流采购'
    when source_type = 9 then '工厂调拨'
    when source_type = 10 then '智能补货'
    when source_type = 11 then '商超直送'
    when source_type = 12 then 'WMS调拨'
    when source_type = 13 then '云超门店采购'
    when source_type = 14 then '临时地采'
    when source_type = 15 then '联营直送'
    when source_type = 16 then '永辉生活'
    when source_type = 17 then 'RDC调拨'
    when source_type = 18 then '城市服务商'
    else '其他' end as source_type_name,source_type 
from csx_dw.dws_scm_r_d_header_item_price 
group by order_code,source_type ) b on a.origin_order_code=b.order_code

;


DROP TABLE csx_tmp.temp_order_entry_t2;
create  table csx_tmp.temp_order_entry_t2 as 
select  mon,
    -- province_code,
    -- province_name,
    -- city_code,
    -- city_name,
    -- purpose,
    -- purpose_name,
    origin_order_code,
    source_type_name      ,
    source_type,
    dc_code,
    -- shop_name,
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
from csx_tmp.temp_order_entry_t1 a
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
)c on a.goods_code=c.goods_id
join 
(SELECT vendor_id,vendor_name
FROM csx_dw.dws_basic_w_a_csx_supplier_m
WHERE sdt='current'
)d  on a.supplier_code=d.vendor_id
 where 1=1
 and source_type !='15'
;

DROP TABLE csx_tmp.temp_order_entry_t3;
create  table csx_tmp.temp_order_entry_t3 as 
select  mon,
    province_code,
    province_name,
    city_code,
    city_name,
    purpose,
    purpose_name,
    origin_order_code,
    source_type_name      ,
    source_type,
    dc_code,
    shop_name,
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
from csx_tmp.temp_order_entry_t2 a 
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
  and purchase_org !='P620'
  and shop_id not in ('W0J8','W0K4')
  AND purpose IN ('01',
                  '02',
                  '03',
                  '08',
                  '07'
                -- '06', 合伙人仓
                --  '05' --彩食鲜小店
                -- '04' 寄售小店
                  )) b on a.dc_code=b.shop_id
    
    ;
select mon, province_name,sum(all_net_amt) from 
(select  province_name,supplier_code,vendor_name,all_net_amt,row_number()over (partition by province_name order by all_net_amt desc ) aa from
(
select '全国' province_name,a.supplier_code, vendor_name ,sum(amt)/10000 all_net_amt from  csx_tmp.temp_order_entry_t3 a 
where 1=1
-- classify_middle_code ='B0302' 
group by a.supplier_code,vendor_name
union all
select province_name,supplier_code,vendor_name,sum(amt)/10000 all_net_amt from  csx_tmp.temp_order_entry_t3 
where 1=1
-- classify_middle_code ='B0302' 
and province_name in ( '四川省', '安徽省' ,'北京市','重庆市')

group by province_name,supplier_code,vendor_name
) a where all_net_amt!=0
) a where aa<11
group by   province_name
;

select supplier_code,sum(amt) from csx_tmp.temp_order_entry_t1   where supplier_code='20031467'
group by supplier_code;


select province_name,supplier_code,sum(amount) from csx_dw.dws_wms_r_d_entry_batch  a
where supplier_code='20031467'
    and receive_location_code !='W0J8'
    and receive_status in (1,2)
    AND SDT>='20210101' AND SDT<'20220101'
    and a.order_type_code LIKE 'P%' and business_type !='02'
group by province_name,supplier_code;



select supplier_code,sum(amt) from csx_tmp.temp_order_entry_t3   where supplier_code='20031467'
group by supplier_code;