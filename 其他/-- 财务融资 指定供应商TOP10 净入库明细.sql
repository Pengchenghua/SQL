-- 财务融资 指定供应商TOP10 净入库明细
-- 财务融资 指定供应商TOP10 净入库明细

DROP TABLE csx_tmp.temp_supp_sale_a;
create  table csx_tmp.temp_supp_sale_a as 
select  mon,
    province_code,
    province_name,
    city_code,
    city_name,
    purpose,
    purpose_name,
    dc_code,
    shop_name,
    goods_code,
    goods_name,
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
    sum(qty) qty,
    sum(amt) amt,
    sum(shipp_qty )shipp_qty,
    sum(shipp_amt )shipp_amt,
    sum(qty-shipp_qty) net_qty,
    sum(amt-shipp_amt) net_amt
from 
(select substr(sdt,1,6) mon,
    receive_location_code dc_code,
    goods_code,
    supplier_code,
    sum(case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) qty,
    sum(price*case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) amt,
    0 shipp_qty,
    0 shipp_amt
from csx_dw.dws_wms_r_d_entry_detail a
where 1=1 
and sdt>='20200101' 
and sdt<'20210701'
and receive_status in (1,2)
and supplier_code in 
('20034399','20020295','20044680','20049806','20046766','20014608','20030080','20020588','20043536','20011716',
'20047386','20022481','20034005','QGC00029','C05013','20031467','20039700','20026290','20001274','20030175',
'20001274','20020295','75000021','20034005','QGC00029','20009929','20020588','20039700','20006168','20004752',
'B10008','200428','20044680','20034399','20011716','20008481','20034021','20007717','20030175','210879','20020295',
'20001274','20034399','75000021 ','20034005','QGC00029','20044680','20020588','20039700','20009929',
'20011716','20006168','20004752','B10008','200428','20030080','20034021','20030175','20049806','20034742')
and (a.business_type in ('ZN01','ZN02','ZNR1','ZNR2')
       OR (a.order_type_code LIKE 'P%' and business_type !='02')  )
group by substr(sdt,1,6) ,
    receive_location_code  ,
    goods_code,
    supplier_code
union all 
select substr(regexp_replace(to_date(send_time),'-',''),1,6) mon,
    shipped_location_code dc_code,
    goods_code,
    supplier_code,
    0 qty,
    0 amt,
    sum(shipped_qty) shipp_qty,
    sum(shipped_qty*price) shipp_amt
from csx_dw.dws_wms_r_d_ship_detail a
where 1=1
-- supplier_code in ('20046634','20042204','20051662','20043882','20024248','20029976','20028053','20048472','20043203','20043203','20041365','20038251')
    and sdt>='20200101'
    and sdt<'20210701'
    and status in ('6','7','8')
    and supplier_code in 
('20034399','20020295','20044680','20049806','20046766','20014608','20030080','20020588','20043536','20011716',
'20047386','20022481','20034005','QGC00029','C05013','20031467','20039700','20026290','20001274','20030175',
'20001274','20020295','75000021','20034005','QGC00029','20009929','20020588','20039700','20006168','20004752',
'B10008','200428','20044680','20034399','20011716','20008481','20034021','20007717','20030175','210879','20020295',
'20001274','20034399','75000021 ','20034005','QGC00029','20044680','20020588','20039700','20009929',
'20011716','20006168','20004752','B10008','200428','20030080','20034021','20030175','20049806','20034742')
    AND (( order_type_code LIKE 'P%'  and business_type_code ='05') or a.business_type_code in ('ZN01','ZN02','ZNR1','ZNR2'))
group by substr(regexp_replace(to_date(send_time),'-',''),1,6) ,
    shipped_location_code  ,
    supplier_code,
    goods_code
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
                  '06', -- 合伙人仓
                  '05', --彩食鲜小店
                  '04' --寄售小店
                  )) b on a.dc_code=b.shop_id
join 
(SELECT goods_id,
       goods_name,
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
 where 1=1
    -- and classify_large_code in('B03','B02')
    -- and supplier_code not in ('20020295','B10008','20020588')
    group by 
    mon,
    province_code,
    province_name,
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
    city_code,
    city_name,
     classify_large_code,
    classify_large_name,
    department_id,
       department_name
    ;
    

create temporary table csx_tmp.temp_entry_min as 
select receive_location_code as dc_code,
    supplier_code,
    min(case when sdt='19990101' then regexp_replace(to_date(receive_time),'-','') else sdt end) min_sdt
from csx_dw.dws_wms_r_d_entry_detail 
where 
supplier_code in 
('20034399','20020295','20044680','20049806','20046766','20014608','20030080','20020588','20043536','20011716',
'20047386','20022481','20034005','QGC00029','C05013','20031467','20039700','20026290','20001274','20030175',
'20001274','20020295','75000021','20034005','QGC00029','20009929','20020588','20039700','20006168','20004752',
'B10008','200428','20044680','20034399','20011716','20008481','20034021','20007717','20030175','210879','20020295',
'20001274','20034399','75000021 ','20034005','QGC00029','20044680','20020588','20039700','20009929',
'20011716','20006168','20004752','B10008','200428','20030080','20034021','20030175','20049806','20034742')
and receive_status in ('1','2')
group by receive_location_code,supplier_code
;


SELECT 
       a.supplier_code,
       vendor_name,
       min_sdt,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       qty,
       net_amt,
       company_code,
       company_name,
       a.dc_code,
       shop_name,
       purpose,
       purpose_name
from 
(SELECT dc_code,
       supplier_code,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       sum(net_qty) qty,
       sum(net_amt) net_amt
from csx_tmp.temp_supp_sale_a a 
where supplier_code in ('20034399','20020295','20044680','20049806','20046766','20014608','20030080','20020588','20043536','20011716',
'20047386','20022481','20034005','QGC00029','C05013','20031467','20039700','20026290','20001274','20030175',
'20001274','20020295','75000021','20034005','QGC00029','20009929','20020588','20039700','20006168','20004752',
'B10008','200428','20044680','20034399','20011716','20008481','20034021','20007717','20030175','210879','20020295',
'20001274','20034399','75000021 ','20034005','QGC00029','20044680','20020588','20039700','20009929',
'20011716','20006168','20004752','B10008','200428','20030080','20034021','20030175','20049806','20034742')
-- and mon>='202101' 
-- and mon<'202107'
GROUP BY 
       dc_code,
       supplier_code,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
)a 
LEFT JOIN 
    csx_tmp.temp_entry_min as   b on a.dc_code=b.dc_code and a.supplier_code=b.supplier_code
JOIN
(select shop_id,shop_name,company_code,company_name,purpose,
    purpose_name
    from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') c on a.dc_code=c.shop_id
JOIN
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current')d on a.supplier_code=d.vendor_id
;



SELECT 
       a.supplier_code,
       vendor_name,
       min_sdt,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       qty,
       net_amt,
       company_code,
       company_name,
       a.dc_code,
       shop_name
from 
(SELECT dc_code,
       supplier_code,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name, 
       sum(net_qty) qty,
       sum(net_amt) net_amt
from csx_tmp.temp_supp_sale a 
where supplier_code in ('20034399','20020295','20044680','20049806','20046766','20014608','20030080','20020588','20043536','20011716',
'20047386','20022481','20034005','QGC00029','C05013','20031467','20039700','20026290','20001274','20030175',
'20001274','20020295','75000021','20034005','QGC00029','20009929','20020588','20039700','20006168','20004752',
'B10008','200428','20044680','20034399','20011716','20008481','20034021','20007717','20030175','210879','20020295',
'20001274','20034399','75000021 ','20034005','QGC00029','20044680','20020588','20039700','20009929',
'20011716','20006168','20004752','B10008','200428','20030080','20034021','20030175','20049806','20034742')
and mon>='202101' 
and mon<'202107'
GROUP BY 
       dc_code,
       supplier_code,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
)a 
LEFT JOIN 
    csx_tmp.temp_entry_min as   b on a.dc_code=b.dc_code and a.supplier_code=b.supplier_code
JOIN
(select shop_id,shop_name,company_code,company_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') c on a.dc_code=c.shop_id
JOIN
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current')d on a.supplier_code=d.vendor_id;


SELECT sum(net_qty) qty,
       sum(net_amt) net_amt,
       sum(amt),
       sum(shipp_amt)
from csx_tmp.temp_supp_sale_a a 
where a.supplier_code='20030175'
and mon<'202101'
and a.purpose !='06'
;



SELECT 
       a.supplier_code,
       vendor_name,
       min_sdt,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       qty,
       net_amt,
       company_code,
       company_name,
       a.dc_code,
       shop_name
from 
(SELECT dc_code,
       supplier_code,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name, 
       sum(net_qty) qty,
       sum(net_amt) net_amt
from csx_tmp.temp_supp_sale a 
where supplier_code in ('20034399','20020295','20044680','20049806','20046766','20014608','20030080','20020588','20043536','20011716',
'20047386','20022481','20034005','QGC00029','C05013','20031467','20039700','20026290','20001274','20030175',
'20001274','20020295','75000021','20034005','QGC00029','20009929','20020588','20039700','20006168','20004752',
'B10008','200428','20044680','20034399','20011716','20008481','20034021','20007717','20030175','210879','20020295',
'20001274','20034399','75000021 ','20034005','QGC00029','20044680','20020588','20039700','20009929',
'20011716','20006168','20004752','B10008','200428','20030080','20034021','20030175','20049806','20034742')
and mon>='202101' 
and mon<'202107'
GROUP BY 
       dc_code,
       supplier_code,
       goods_code,
       goods_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
)a 
LEFT JOIN 
    csx_tmp.temp_entry_min as   b on a.dc_code=b.dc_code and a.supplier_code=b.supplier_code
JOIN
(select shop_id,shop_name,company_code,company_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') c on a.dc_code=c.shop_id
JOIN
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current')d on a.supplier_code=d.vendor_id