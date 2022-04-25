-- -- 融资需求--肉禽、蔬菜水果 净入库
-- 1、剔除转配供应商：2304 20020295 北京二商大红门五肉联食品有限公司
-- 2304 B10008 北京顺鑫农业股份有限公司鹏程食品分公司
-- 2304 20020588 中粮肉食（北京）有限公司大兴分公司
-- 2、剔除W0K4
DROP TABLE csx_tmp.temp_order_entry;
create  table csx_tmp.temp_order_entry as 
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
    goods_code,
    supplier_code,
    (case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) qty,
    (price*case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) amt,
    0 shipp_qty,
    0 shipp_amt
from csx_dw.dws_wms_r_d_entry_detail a
where 1=1 
and sdt>='20200101' 
and sdt<'20211001'
and receive_status in (1,2)
and (a.business_type in ('ZN01','ZN02','ZNR1','ZNR2')
       OR (a.order_type_code LIKE 'P%' and business_type !='02')  )
union all 
select substr(regexp_replace(to_date(send_time),'-',''),1,6) mon,
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
    and sdt>='20200101'
    and sdt<'20211001'
    and status in ('6','7','8')
    AND (( order_type_code LIKE 'P%'  and business_type_code ='05') or a.business_type_code in ('ZN01','ZN02','ZNR1','ZNR2'))
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
    
    

-- 全国入库金额
select mon,province_name ,sum(all_net_amt) from (
select mon,'全国' province_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where classify_middle_code ='B0302' 
group by mon
union all
select mon, province_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale
where classify_middle_code ='B0302' 
and (province_name in ( '四川省', '安徽省') or city_name='福州市')
group by mon,province_name
) a 
group by mon,province_name
;



-- 白条&片肉 入库额
select mon,province_name ,sum(all_net_amt) from (
select mon,'全国' province_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where classify_middle_code ='B0302' 
and ( goods_name like '%白条%' or goods_name like '片肉%')
group by mon
union all
select mon, province_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale 
where classify_middle_code ='B0302' 
and ( goods_name like '%白条%' or goods_name like '片肉%')
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by mon,province_name
) a 
group by mon,province_name
;

-- top10供应商

select mon, province_name,sum(all_net_amt) from 
(select mon, province_name,supplier_code,all_net_amt,row_number()over (partition by province_name,mon order by all_net_amt desc ) aa from
(
select mon,'全国' province_name,a.supplier_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where classify_middle_code ='B0302' 
group by mon,a.supplier_code
union all
select mon, province_name,supplier_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale 
where classify_middle_code ='B0302' 
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by mon,province_name,supplier_code
) a 
) a where aa<11
group by  mon, province_name
;

-- 蔬菜水果 入库额
select  concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) as  Q,province_name ,sum(all_net_amt) from (
select mon,'全国' province_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where classify_large_code ='B02' 
group by mon,substr(mon,1,4)
union all
select mon, province_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale
where classify_large_code ='B02' 
and (province_name in ( '四川省', '安徽省') or city_name='福州市')
group by mon,province_name
) a 
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) ,province_name
;

--蔬菜水果TOP10 供应商
select mon, province_name,sum(all_net_amt) from 
(select mon, province_name,supplier_code,all_net_amt,row_number()over (partition by province_name,mon order by all_net_amt desc ) aa from
(
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) mon,'全国' province_name,a.supplier_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where a.classify_large_code ='B02' 
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),a.supplier_code
union all
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) as mon, province_name,supplier_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale 
where classify_large_code ='B02' 
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),province_name,supplier_code
) a 
) a where aa<11
group by  mon, province_name
;



-- top10 食百商品


select mon, province_name,sum(all_net_amt) from 
(select mon, province_name,goods_code,all_net_amt,row_number()over (partition by province_name,mon order by all_net_amt desc ) aa from
(
select mon,'全国' province_name,a.goods_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where  ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
group by mon,a.goods_code
union all
select mon, province_name,goods_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale 
where  ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by mon,province_name,goods_code
) a 
) a where aa<11 and mon='202106'
group by  mon, province_name
;



-- top10 食百商品


select mon, province_name,sum(all_net_amt) from 
(select mon, province_name,goods_code,all_net_amt,row_number()over (partition by province_name,mon order by all_net_amt desc ) aa from
(
select mon,'全国' province_name,a.goods_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where  ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
group by mon,a.goods_code
union all
select mon, province_name,goods_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale 
where  ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by mon,province_name,goods_code
) a 
) a where aa<11 and mon='202106'
group by  mon, province_name
;

-- 食百入库额
select mon,province_name ,sum(all_net_amt) from (
select mon,'全国' province_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where ( substr(department_id,1,1) in ('A','P') OR department_id='105')  
group by mon
union all
select mon, province_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale 
where ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by mon,province_name
) a WHERE MON='202106'
group by mon,province_name
;



--四川 基地入库情况
select mon, province_name,goods_code,goods_name,sum(amt)/sum(qty) price from  csx_tmp.temp_supp_sale 
where  1=1
and province_name in ( '四川省' )
and goods_code in ('3695','1330713','262352','2230','1330712','153890',
    '1065513','1251396','576','263859','2112','562','538','883188','620',
    '1134244','1356734','317132','1374480')
group by mon, province_name,goods_code,goods_name
;


--省区供应商明细
select mon, province_name,supplier_code,vendor_name,all_net_amt,(all_net_amt),aa supplier_net_amt from 
(select mon, province_name,supplier_code,vendor_name,all_net_amt,row_number()over (partition by province_name,mon order by all_net_amt desc ) aa from
(
select mon,'全国' province_name,a.supplier_code,a.vendor_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where classify_middle_code ='B0302' 
group by mon,a.supplier_code,vendor_name
union all
select mon, province_name,supplier_code,vendor_name,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale 
where classify_middle_code ='B0302' 
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by mon,province_name,supplier_code,vendor_name
) a 
) a where aa<11;



-- 蔬菜水果 入库额
-- 蔬菜水果 入库额

SELECT concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) mon,
       province_name,
    --   goods_code,
    --   goods_name,
       supplier_code,
       vendor_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       sum(net_amt)/10000 all_net_amt
FROM csx_tmp.temp_supp_sale
WHERE classify_large_code ='B02'
  AND (province_name IN ('四川省',
                         '安徽省')
       OR city_name='福州市')
GROUP BY concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),
         province_name,
        --  goods_code,
        --  goods_name,
        supplier_code,
        vendor_name,
         classify_large_code,
         classify_large_name,
          classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name

union all 
-- 全国蔬菜三级分类
SELECT concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) mon,
      '全国' province_name,
    --   goods_code,
    --   goods_name,
        supplier_code,
        vendor_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       sum(net_amt)/10000 all_net_amt
FROM csx_tmp.temp_supp_sale
WHERE classify_large_code ='B02'
 
GROUP BY concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),
        --  goods_code,
        --  goods_name,
        supplier_code,
        vendor_name,
         classify_large_code,
         classify_large_name,
          classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name;



-- 猪肉入库明细

SELECT mon,
       province_name,
       purpose,
       purpose_name,
       dc_code,
       shop_name,
       supplier_code,
       vendor_name,
       goods_code,
       goods_name,
       brand_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       sum(qty) qty,
       sum(amt) amt,
       sum(shipp_qty)shipp_qty,
       sum(shipp_amt)shipp_amt,
       sum(net_qty) net_qty,
       sum(net_amt) net_amt
FROM csx_tmp.temp_supp_sale
WHERE classify_middle_code ='B0302'
-- and (province_name in ( '四川省', '安徽省') or city_name='福州市')
AND mon BETWEEN '202001' AND '202004'
GROUP BY  mon,
       province_name,
       purpose,
       purpose_name,
       dc_code,
       shop_name,
       supplier_code,
       vendor_name,
       goods_code,
       goods_name,
       brand_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name ;


-- 食百明细

SELECT mon,
       province_name,
       purpose,
       purpose_name,
       dc_code,
       shop_name,
       supplier_code,
       vendor_name,
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
       sum(qty) qty,
       sum(amt) amt,
       sum(shipp_qty)shipp_qty,
       sum(shipp_amt)shipp_amt,
       sum(net_qty) net_qty,
       sum(net_amt) net_amt
FROM csx_tmp.temp_supp_sale
WHERE ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
and mon='202106'
GROUP BY  mon,
       province_name,
       purpose,
       purpose_name,
       dc_code,
       shop_name,
       supplier_code,
       vendor_name,
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
       department_name ;
       
  --蔬菜水果TOP10 供应商明细
select  a.mon,
    a.province_name,
    a.supplier_code,
    vendor_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    all_net_amt,
    all_amt,
    aa

from
(
select  concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) as mon,
    '全国' province_name,
    a.supplier_code,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(net_amt)/10000 all_net_amt 
    from  csx_tmp.temp_supp_sale a 
where  a.classify_large_code ='B02' 
 and mon>='202104'
  AND dc_code!='W0K4'

group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),
    a.supplier_code,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
union all
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)  mon,
    province_name,
    supplier_code,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(net_amt)/10000 all_net_amt 
from  csx_tmp.temp_supp_sale a
where   a.classify_large_code ='B02' 
 and mon>='202104'
  AND dc_code!='W0K4'

and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),
    province_name,
    supplier_code,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
) a 
join 
(select mon, province_name,supplier_code,all_net_amt as all_amt,row_number()over (partition by province_name,mon order by all_net_amt desc ) aa from
(
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) mon,'全国' province_name,a.supplier_code,sum(net_amt)/10000 all_net_amt 
from  csx_tmp.temp_supp_sale a 
where  a.classify_large_code ='B02' 
 and mon>='202104' 
  AND dc_code!='W0K4'

group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),a.supplier_code
union all
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) as mon, province_name,supplier_code,sum(net_amt)/10000 all_net_amt 
from  csx_tmp.temp_supp_sale  a
where  a.classify_large_code ='B02' 
 and mon>='202104'
  AND dc_code!='W0K4'
 
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),province_name,supplier_code
) a 
) b on a.province_name=b.province_name and a.supplier_code=b.supplier_code and a.mon=b.mon

join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code=c.vendor_id
where aa<11
;


-- 猪肉TOP10 供应商明细

select  a.mon,
    a.province_name,
    a.supplier_code,
    vendor_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    all_net_amt,
    all_amt,
    aa

from
(
select  concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) as mon,
    '全国' province_name,
    a.supplier_code,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(net_amt)/10000 all_net_amt 
    from  csx_tmp.temp_supp_sale a 
where  a.classify_middle_code ='B0302' 
 and mon='202106'
  AND dc_code!='W0K4'

group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),
    a.supplier_code,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
union all
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)  mon,
    province_name,
    supplier_code,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(net_amt)/10000 all_net_amt 
from  csx_tmp.temp_supp_sale a
where  a.classify_middle_code ='B0302' 
 and mon='202106'
  AND dc_code!='W0K4'

and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),
    province_name,
    supplier_code,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
) a 
join 
(select mon, province_name,supplier_code,all_net_amt as all_amt,row_number()over (partition by province_name,mon order by all_net_amt desc ) aa from
(
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) mon,'全国' province_name,a.supplier_code,sum(net_amt)/10000 all_net_amt 
from  csx_tmp.temp_supp_sale a 
where a.classify_middle_code ='B0302' 
 and mon='202106' 
  AND dc_code!='W0K4'

group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),a.supplier_code
union all
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) as mon, province_name,supplier_code,sum(net_amt)/10000 all_net_amt 
from  csx_tmp.temp_supp_sale  a
where  a.classify_middle_code ='B0302' 
 and mon='202106'
  AND dc_code!='W0K4'

and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),province_name,supplier_code
) a 
) b on a.province_name=b.province_name and a.supplier_code=b.supplier_code and a.mon=b.mon

join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code=c.vendor_id
where aa<11
;

--食百供应商TOP 明细
select  a.mon,
    a.province_name,
    a.supplier_code,
    vendor_name,
    department_id,
    department_name,
     classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    all_net_amt,
    all_amt,
    aa

from
(
select  concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) as mon,
    '全国' province_name,
    a.supplier_code,
     department_id,
    department_name,
     classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(net_amt)/10000 all_net_amt 
    from  csx_tmp.temp_supp_sale a 
where  ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
 and mon>='202106'
  AND dc_code!='W0K4'

group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),
    a.supplier_code,
    department_id,
    department_name,
     classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
union all
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)  mon,
    province_name,
    supplier_code,
    department_id,
    department_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(net_amt)/10000 all_net_amt 
from  csx_tmp.temp_supp_sale 
where  ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
and mon>='202106'
 AND dc_code!='W0K4'

and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),
    province_name,
    supplier_code,
    department_id,
    department_name,
     classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
) a 
join 
(select mon, province_name,supplier_code,all_net_amt as all_amt,row_number()over (partition by province_name,mon order by all_net_amt desc ) aa from
(
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) mon,'全国' province_name,a.supplier_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale a 
where ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
 and mon>='202106'
  AND dc_code!='W0K4'

group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),a.supplier_code
union all
select concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) as mon, province_name,supplier_code,sum(net_amt)/10000 all_net_amt from  csx_tmp.temp_supp_sale 
where ( substr(department_id,1,1) in ('A','P') OR department_id='105') 
 and mon>='202106' 
 AND dc_code!='W0K4'
and (province_name in ( '四川省', '安徽省')or city_name='福州市')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),province_name,supplier_code
) a 
) b on a.province_name=b.province_name and a.supplier_code=b.supplier_code and a.mon=b.mon

join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code=c.vendor_id
where aa<11
;


--福建TOP商品入库
select mon,
a.dc_code,
a.shop_name,
a.goods_code,
a.goods_name,
sum(a.net_amt)net_amt,
sum(a.net_no_tax_amt)net_no_tax_amt 
from  csx_tmp.temp_supp_sale a 
where goods_code in ('8773','1285376','23748','1388920','59324','1168188','7632',
'651368','1285347','1295923','1296123','1295849','860131',
'1279391','634720','1277914','977399','1122756','8775','278956')
and city_name='福州市'
 AND dc_code!='W0K4'
 group by 
a.dc_code,a.shop_name,a.goods_code,a.goods_name,mon
;


-- 安徽TOP商品入库
select mon,
a.province_name,
a.dc_code,
a.shop_name,
a.goods_code,
a.goods_name,
sum(a.net_amt)net_amt,
sum(a.net_no_tax_amt)net_no_tax_amt 
from  csx_tmp.temp_supp_sale a 
where goods_code in ('1197634','612425','276132','834063','843623','650712','1286630','795397',
'8773','1404542','263871','8775',
'1410317','1057928','8172','771720','1371553','449672','1057910','1030016')
and province_name in (  '安徽省')
 group by 
a.dc_code,a.shop_name,a.goods_code,a.goods_name,mon,province_name
;
-- 四川TOP商品入库
select mon,
a.province_name,
a.dc_code,
a.shop_name,
a.goods_code,
a.goods_name,
sum(a.net_amt)net_amt,
sum(a.net_no_tax_amt)net_no_tax_amt 
from  csx_tmp.temp_supp_sale a 
where goods_code in ('1364628','1277967','1277914','1327013','1277744','1412840','874989','1099633',
'1276142','1277731','699304','1277959','1277964','1167806','1277753','1405516','834098','1277735','1277918','1277737')
and province_name in (  '四川省')
 group by 
a.dc_code,a.shop_name,a.goods_code,a.goods_name,mon,province_name
;