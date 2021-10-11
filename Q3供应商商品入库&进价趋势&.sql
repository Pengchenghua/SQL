
-- 供应商类别	0	空
-- 供应商类别	1	代理商
-- 供应商类别	2	生产厂商
-- 供应商类别	3	经销商(资产)
-- 供应商类别	4	集成商(资产)

--供应商层级入库&进价趋势
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
    supplier_type,
    supplier_type_name,
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
 -- 不含合伙人仓
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
(SELECT vendor_id,
    vendor_name,
    supplier_type,
    case when supplier_type='0' then '空'
        when supplier_type   ='1' then '代理商'
        when supplier_type ='2' then '生产厂商'
        when supplier_type ='3' then '经销商(资产)'
        when supplier_type ='4' then '集成商(资产)'
        else ''
    end supplier_type_name
FROM csx_dw.dws_basic_w_a_csx_supplier_m
WHERE sdt='current'
)d  on a.supplier_code=d.vendor_id
left join
(select order_code,source_type_name,source_type 
from csx_dw.dws_scm_r_d_header_item_price 
group by order_code,source_type_name,source_type )f on a.origin_order_code=f.order_code
 where 1=1
 and (source_type !='15' or source_type is null)    --剔除联营直送

;
    
    
select province_name, concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)as qq,
sum(net_amt)/10000,
sum(case when spu_goods_code is not null then net_amt end )/10000 as spu_amt
from csx_tmp.temp_order_entry 
where classify_middle_code='B0304' 
and dc_code not in ('W0K4','W0J8')
and source_type !='4'
-- and province_name in ( '四川省', '安徽省','福建省')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1),province_name
;


select  concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)as qq,
sum(net_amt)/10000,
sum(case when spu_goods_code is not null then net_amt end )/10000 as spu_amt
from csx_tmp.temp_order_entry 
where classify_middle_code='B0304' 
and dc_code not in ('W0K4','W0J8')
and source_type !='4'
-- and province_name in ( '四川省', '安徽省','福建省')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)
;



select  concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)as qq,
sum(net_amt)/10000,
sum(case when spu_goods_code is not null then net_amt end )/10000 as spu_amt
from csx_tmp.temp_order_entry 
where classify_middle_code='B0202' 
and dc_code not in ('W0K4','W0J8')
and source_type !='4'
 and province_name in ( '四川省')
group by concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)
;


drop table  csx_tmp.temp_entry_01;
create temporary table csx_tmp.temp_entry_01 as 
select  
concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) qq,
sales_region_code,
sales_region_name,
b.province_code,
b.province_name,
dc_code,
supplier_code,
vendor_name,
supplier_type,
supplier_type_name,
sum(amt)/10000 entry_amt,
sum(shipp_amt) /10000 as shipp_amt,
sum(net_amt)/10000 as net_amt
from csx_tmp.temp_order_entry  a 
join 
(select 
    sales_province_code,
    sales_province_name,
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
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1) b  on a.dc_code=b.shop_id
where 1=1
and dc_code not in ('W0K4')
and source_type !='4'
and mon>='202106'
-- and province_name in ( '四川省')
group by
sales_region_code,
sales_region_name,
b.province_code,
b.province_name,
dc_code,
supplier_code,
supplier_type,
supplier_type_name,
vendor_name,
concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) 
;


select  qq,
    sales_region_code,
  sales_region_name,
  province_code,
  province_name,
   supplier_type,
  supplier_type_name,
  count(supplier_code ) as supplier_cn,
  sum(net_amt)net_amt  ,
   aa  
from(
select  qq,
  sales_region_code,
  sales_region_name,
  province_code,
  province_name,
  supplier_code,
  supplier_type,
  supplier_type_name,
  net_amt  ,
  case when net_amt <10 then '0~10万'
    when net_amt>=10 and net_amt<100 then '10~100万'
    else '100万以上'
    end aa  
from
(
select  qq,
    sales_region_code,
  sales_region_name,
  province_code,
  province_name,
  supplier_code,
   supplier_type,
  supplier_type_name,
  sum(net_amt) net_amt
from  csx_tmp.temp_entry_01  a 
group by  sales_region_code,
  sales_region_name,qq,
  province_code,
  province_name,
  supplier_code,
   supplier_type,
  supplier_type_name
  ) a 
  ) b
  
  where 
  qq='2021Q2'

  group by qq,
    sales_region_code,
  sales_region_name,
  province_code,
  province_name,
   supplier_type,
  supplier_type_name,
   aa 
   ;
   
   select * from  csx_tmp.temp_entry_01;

show create table  csx_dw.dws_basic_w_a_csx_supplier_m ;

select a.*,b.classify_small_code,b.classify_small_name from  csx_tmp.temp_order_entry a
join 
(select goods_id,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and classify_middle_code='B0304')b on a.goods_code=b.goods_id
where  source_type !='4' ;


select Q,
      goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
    net_qty,
    net_amt,
    avg_price,
    dense_rank()over(partition by goods_code,Q order by net_amt desc) as aa
from (
select 
    concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) Q,
    goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
    sum(net_qty)net_qty,
    sum(net_amt) net_amt,
    sum(net_amt)/sum(net_qty) as avg_price
from csx_tmp.temp_order_entry
group by  goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
     concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)
    )a 
    ;
    
--冻品进价趋势
    
with  temp_01 as (
select Q,
      goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
    net_qty,
    net_amt,
    avg_price,
    dense_rank()over(partition by Q order by net_amt desc) as aa
from (
select 
    concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) Q,
    goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
    sum(net_qty)net_qty,
    sum(net_amt) net_amt,
    sum(net_amt)/sum(net_qty) as avg_price
from csx_tmp.temp_order_entry
where classify_middle_code='B0304'
AND MON between '202107' and '202109'
group by  goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
     concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)
    )a 
),
temp_02 as 
(select 
    goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
    sum(case when Q='2020Q1' then avg_price end) as price_2020Q1,
    sum(case when Q='2020Q2' then avg_price end) as price_2020Q2,
    sum(case when Q='2020Q3' then avg_price end) as price_2020Q3,
    sum(case when Q='2020Q4' then avg_price end) as price_2020Q4,
    sum(case when Q='2021Q1' then avg_price end) as price_2021Q1,
    sum(case when Q='2021Q2' then avg_price end) as price_2021Q2,
    sum(case when Q='2021Q3' then avg_price end) as price_2021Q3
from(

select 
    concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1) Q,
    goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
    sum(net_qty)net_qty,
    sum(net_amt) net_amt,
    sum(net_amt)/sum(net_qty) as avg_price
from csx_tmp.temp_order_entry
where classify_middle_code='B0304'
-- AND MON between '202107' and '202109'
group by  goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name,
     concat(substr(mon,1,4),'Q',floor(substr(mon,5,2)/3.1)+1)
    )a
    group by 
    goods_code,
    goods_name,
    spu_goods_code,
    spu_goods_name
    )
select a.*,
price_2020Q1,
price_2020Q2,
price_2020Q3,
price_2020Q4,
price_2021Q1,
price_2021Q2,
price_2021Q3 
from temp_01 a 
left join 
(select * from temp_02) b on a.goods_code=b.goods_code
where aa<51

    ;

-- 蔬菜四川毛利率
-- set hive.execution.engine=tez;
select a.province_code,a.province_name,
concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) mon,
dc_code,
classify_middle_name,
sum(a.sales_value) sale,
sum(a.profit) profit,
sum(case when spu_goods_code is not null then a.sales_value end) sale_spu,
sum(case when spu_goods_code is not null then a.profit end) profit_spu
from csx_dw.dws_sale_r_d_detail a 
join 
(select goods_id,spu_goods_code,spu_goods_name 
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt='current' 
-- and spu_goods_code is not null
and classify_middle_code in ('B0202')
)b on a.goods_code=b.goods_id
where sdt>='20200101' and sdt<'20211001'
  and province_name in ( '四川省' )
  and a.channel_code in ('1','7','9')
-- and channel_code !='2'
 and dc_code !='W0K4'
 and a.business_type_code!='4'
 group by a.province_code,a.province_name,
concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) ,
dc_code,
classify_middle_name
;

SELECT * FROM CSX_DW.dws_sale_r_d_detail where dc_code='W0J8';



-- 季度销售冻品
select a.province_code,a.province_name,
concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) mon,
dc_code,
sum(a.sales_value) sale,
sum(a.profit) profit,
sum(case when spu_goods_code is not null then a.sales_value end) sale_spu,
sum(case when spu_goods_code is not null then a.profit end) profit_spu
from csx_dw.dws_sale_r_d_detail a 
join 
(select goods_id,spu_goods_code,spu_goods_name 
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt='current' 
-- and spu_goods_code is not null
and classify_middle_code in ('B0202')
)b on a.goods_code=b.goods_id
where sdt>='20200101' and sdt<'20211001'
 and province_name in ( '四川省')
 and a.channel_code in ('1','7','9')
 and channel_code !='2'
 and dc_code !='W0K4'
 and a.business_type_code!='4'
 group by a.province_code,a.province_name,
concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) ,
dc_code
;
