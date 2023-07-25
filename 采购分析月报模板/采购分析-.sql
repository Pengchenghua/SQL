set sdt='20210101';
set edt='20210731';
drop table csx_tmp.temp_fin_sup ;
create table csx_tmp.temp_fin_sup as 
SELECT  substr(sdt,1,6) as mon,
        coalesce(j.sales_region_code,d.sales_region_code) as region_code,
        coalesce(j.sales_region_name ,d.sales_region_name) as region_name,
        a.order_code,
        coalesce(j.province_code,d.province_code) province_code,
        coalesce(j.province_name,d.province_name) province_name,
        source_type_name,
       CASE
            WHEN a.super_class='1'
                THEN '供应商订单'
            WHEN a.super_class='2'
                THEN '供应商退货订单'
            WHEN a.super_class='3'
                THEN '配送订单'
            WHEN a.super_class='4'
                THEN '返配订单'
                ELSE a.super_class
        END super_class_name  ,
       receive_location_code,
       receive_location_name,
       goods_code,
       b.goods_name,
       unit_name,
       standard,
       brand_name,
       department_id,
       department_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       b.category_large_code,
       b.category_large_name,
       supplier_code,
       supplier_name,
       shipped_location_code,
       shipped_location_name,
       local_purchase_flag,
       receive_business_type,
       shipped_business_type,
       receive_qty,
       receive_amt,
       shipped_qty,
       shipped_amt,
       a.receive_close_date,
       coalesce(j.purpose,d.purpose) as purpose
FROM csx_dw.ads_supply_order_flow a 
left join 
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    and purpose  in ('01','02','03','07','08') 
)j on a.receive_location_code=j.shop_id
LEFT JOIN 
(select goods_id,
        goods_name,
        unit_name,
        standard,
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
        category_large_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
LEFT JOIN
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    and purpose  in ('01','02','03','07','08') 
) d on a.shipped_location_code=d.shop_id
WHERE ( ( sdt>='20210101' or sdt='19990101')
    and a.super_class in ('1','2')
    and  ( shipped_status in ('6','7','8') or a.receive_status='2') )
    and ((a.receive_close_date>=${hiveconf:sdt} AND receive_close_date<=${hiveconf:edt})
     OR (shipped_date >=${hiveconf:sdt} AND shipped_date<=${hiveconf:edt})
     )
     ;
     
     

drop table csx_tmp.temp_entry_00 ;
create table csx_tmp.temp_entry_00 as
select   order_no,
    dc_code,
    goods_code,
    division_code,
    division_name,
    supplier_code,
    sum(receive_qty) receive_qty,
    sum(receive_amt) receive_amt,
    sum(no_tax_receive_amt) as no_tax_receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(shipped_amt) shipped_amt,
    sum(no_tax_shipped_amt) as no_tax_shipped_amt
from 
(
select origin_order_code order_no,
    receive_location_code as dc_code,
    goods_code,
    supplier_code,
    sum(receive_qty) receive_qty,
    sum(price/(1+tax_rate/100)*receive_qty) as no_tax_receive_amt,
    sum(price*receive_qty) as receive_amt,
    0 shipped_qty,
    0 shipped_amt,
    0 no_tax_shipped_amt
from csx_dw.dws_wms_r_d_entry_batch
where sdt>='20210101' 
    and regexp_replace( to_date(receive_time ),'-','')<='20210731'
    and  regexp_replace( to_date(receive_time ),'-','')>='20210701'
    and order_type_code like 'P%'
    and business_type !='02'
    and receive_status in ('1','2')
   group by receive_location_code,goods_code,origin_order_code,supplier_code
union all 
select origin_order_no order_no, 
    shipped_location_code as dc_code,
    goods_code,
    supplier_code,
    0 receive_qty,
    0 no_tax_receive_amt,
    0 receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(price*shipped_qty) as shipped_amt,
    sum(price/(1+tax_rate/100)*shipped_qty) as no_tax_shipped_amt
from csx_dw.dws_wms_r_d_ship_detail
where regexp_replace( to_date(send_time),'-','') >='20210701' 
    and  regexp_replace( to_date(send_time),'-','') <='20210731'
    and order_type_code like 'P%'
    and business_type_code in ('05')
    and status in ('6','7','8')
    group by shipped_location_code,goods_code,origin_order_no,supplier_code
) a 
join 
(select goods_id,division_code,division_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id

group by  
    order_no,
    dc_code,
    goods_code,
    supplier_code,
    division_code,
    division_name
;

-- 关联采购订单&DC类型&复用供应商

drop table  csx_tmp.temp_entry_01;
create temporary table csx_tmp.temp_entry_01 as 
select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    j.company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    order_no,
    dc_code,
    goods_code,
    case when purpose ='07' then '20' when yh_reuse_tag='是' then '21' when  division_code in ('11','10') then '11' when  division_code in ('12','13','14','15') then '12' end supplier_type_code ,
    case when purpose ='07' then 'BBC' when yh_reuse_tag='是' then '复用供应商' when  division_code in ('11','10') then '生鲜' when  division_code in ('12','13','14','15') then '食百' end  supplier_type_name,
    j.supplier_code,
    source_type,
    source_type_name,
    yh_reuse_tag,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt
from 
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    a.order_no,
    dc_code,
    goods_code,
    division_code,
    division_name,
    supplier_code,
    source_type,
    source_type_name ,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt
from csx_tmp.temp_entry_00 a 
join 
(select  order_code,source_type,source_type_name 
    from csx_dw.dws_scm_r_d_header_item_price 
    where super_class in ('2','1')  
    and source_type in ('1','10')
    group by  order_code,source_type,source_type_name
)b on a.order_no=b.order_code
join 
(select 
    sales_province_code,
    sales_province_name,
   case when purchase_org ='P620' then '9' else  sales_region_code end sales_region_code,
   case when purchase_org ='P620' then '大宗' else  sales_region_name end sales_region_name,
    shop_id,
    company_code,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    and purpose  in ('01','02','03','07','08') 
) d on a.dc_code=d.shop_id
) j 
left join  
(select company_code,supplier_code,yh_reuse_tag from csx_tmp.ads_fr_r_m_supplier_reuse where months='202107' ) s on j.company_code=s.company_code and j.supplier_code=s.supplier_code
;

drop table csx_tmp.temp_entry_02;
 create  temporary table csx_tmp.temp_entry_02 as 
select 
    sales_region_code,
    sales_region_name,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    section,
    count(supplier_code) as supplier_num,
    sum(receive_qty)receive_qty,
    sum(receive_amt)receive_amt,
    sum(no_tax_receive_amt)no_tax_receive_amt,
    sum(shipped_qty)shipped_qty,
    sum(shipped_amt)shipped_amt,
    sum(no_tax_shipped_amt)no_tax_shipped_amt,
    sum(net_receive_qty) as net_receive_qty,
    sum(net_receive_amt) as net_receive_amt,
    sum(no_tax_net_receive_amt) no_tax_net_receive_amt
from (
select 
    sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    supplier_code,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    net_receive_qty,
    net_receive_amt,
    no_tax_net_receive_amt,
    case when  no_tax_net_receive_amt/10000*1.00 between 0 and 10 then '0~10万'
        when   no_tax_net_receive_amt/10000*1.00 between 10 and 100 then '10~100万'
        when   no_tax_net_receive_amt/10000*1.00 > 100   then '100万以上'
        else '其他' end section 
    from (
select 
    sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    supplier_code,
    sum(receive_qty)receive_qty,
    sum(receive_amt)receive_amt,
    sum(no_tax_receive_amt)no_tax_receive_amt,
    sum(shipped_qty)shipped_qty,
    sum(shipped_amt)shipped_amt,
    sum(no_tax_shipped_amt)no_tax_shipped_amt,
    sum(receive_qty-shipped_qty ) as net_receive_qty,
    sum(receive_amt-shipped_amt) as net_receive_amt,
    sum(no_tax_receive_amt- no_tax_shipped_amt) no_tax_net_receive_amt
from csx_tmp.temp_entry_01 a 
group by sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    supplier_code
) a 
) a 
group by sales_region_code,
    sales_region_name,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    section
grouping sets 
((  section,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_type_code,
    supplier_type_name),
    (  section,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name),     --城市汇总
    (section,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name),        -- 省区层级 
    (
    section,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name),      --省区汇总
    (  section,
    sales_region_code,
    sales_region_name,
    supplier_type_code,
    supplier_type_name),
    (  section,
    sales_region_code,
    sales_region_name)
    )
;


insert into csx_tmp.temp_entry_02 
select 
    sales_region_code,
    sales_region_name,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    '合计'section,
    count(supplier_code) as supplier_num,
    sum(receive_qty)receive_qty,
    sum(receive_amt)receive_amt,
    sum(no_tax_receive_amt)no_tax_receive_amt,
    sum(shipped_qty)shipped_qty,
    sum(shipped_amt)shipped_amt,
    sum(no_tax_shipped_amt)no_tax_shipped_amt,
    sum(net_receive_qty) as net_receive_qty,
    sum(net_receive_amt) as net_receive_amt,
    sum(no_tax_net_receive_amt) no_tax_net_receive_amt
from (
select 
    sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    supplier_code,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    net_receive_qty,
    net_receive_amt,
    no_tax_net_receive_amt,
    case when  no_tax_net_receive_amt/10000*1.00 between 0 and 10 then '0~10万'
        when   no_tax_net_receive_amt/10000*1.00 between 10 and 100 then '10~100万'
        when   no_tax_net_receive_amt/10000*1.00 > 100   then '100万以上'
        else '其他' end section 
    from (
select 
    sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    supplier_code,
    sum(receive_qty)receive_qty,
    sum(receive_amt)receive_amt,
    sum(no_tax_receive_amt)no_tax_receive_amt,
    sum(shipped_qty)shipped_qty,
    sum(shipped_amt)shipped_amt,
    sum(no_tax_shipped_amt)no_tax_shipped_amt,
    sum(receive_qty-shipped_qty ) as net_receive_qty,
    sum(receive_amt-shipped_amt) as net_receive_amt,
    sum(no_tax_receive_amt- no_tax_shipped_amt) no_tax_net_receive_amt
from csx_tmp.temp_entry_01 a 
group by sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    supplier_code
) a 
) a 
group by sales_region_code,
    sales_region_name,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name
grouping sets 
((  
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_type_code,
    supplier_type_name),
    (  
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name),     --城市汇总
    (
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name),        -- 省区层级 
    (
    sales_region_code,
    sales_region_name,
    province_code,
    province_name),      --省区汇总
    ( 
    sales_region_code,
    sales_region_name,
    supplier_type_code,
    supplier_type_name),
    ( 
    sales_region_code,
    sales_region_name),
    ()
    )
;

select * from csx_tmp.temp_entry_02 ;
show create table csx_dw.dws_wms_r_d_entry_batch;

-- source_type,source_type_name  
--1 采购导入
--2 直送
--3 一键代发
--4 项目合伙人
--5 无单入库
--8 云超物流采购
--10    智能补货
--11    商超直送
--13    云超门店采购
--14    临时地采
--15    联营直送
--16    永辉生活





set sdt='20210101';
set edt='20210731';

drop table csx_tmp.temp_entry_00 ;
create table csx_tmp.temp_entry_00 as
select   order_no,
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    shop_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    dc_code,
    goods_code,
    supplier_code,
    order_type_code,
    order_type_name,
    business_type_code,
    business_type_name,
    goods_name,
    unit_name,
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
    department_name,
    category_large_code,
    category_large_name,
    sum(receive_qty) receive_qty,
    sum(receive_amt) receive_amt,
    sum(no_tax_receive_amt)no_tax_receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(shipped_amt) shipped_amt,
    sum(no_tax_shipped_amt) no_tax_shipped_amt,
    sdt
from 
(
select origin_order_code order_no,
    receive_location_code as dc_code,
    goods_code,
    supplier_code,
    order_type_code,
    order_type_name,
    business_type as business_type_code,
    business_type_name,
    regexp_replace( to_date(receive_time ),'-','') as sdt,
    sum(receive_qty) receive_qty,
    sum(price/(1+tax_rate/100)*receive_qty) as no_tax_receive_amt,
    sum(price *receive_qty) as receive_amt,
    0 shipped_qty,
    0 no_tax_shipped_amt,
    0 shipped_amt 
from csx_dw.dws_wms_r_d_entry_batch
where sdt>='20210101' 
    and regexp_replace( to_date(receive_time ),'-','')<='20210831'
    and  regexp_replace( to_date(receive_time ),'-','')>='20210801'
   -- and order_type_code like 'P%'
   -- and business_type !='02'
    and receive_status in ('1','2')
   group by receive_location_code,
    regexp_replace( to_date(receive_time ),'-','') ,
   goods_code,
   origin_order_code,
   supplier_code,
   order_type_code,
    order_type_name,
    business_type,
    business_type_name
union all 
select origin_order_no order_no, 
    shipped_location_code as dc_code,
    goods_code,
    supplier_code,
    order_type_code,
    wms_order_type_name as order_type_name,
    business_type_code,
    business_type_name,
    regexp_replace( to_date(send_time),'-','') as sdt,
    0 receive_qty,
    0 no_tax_receive_amt,
    0 receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(price/(1+tax_rate/100)*shipped_qty) as no_tax_shipped_amt,
     sum(price *shipped_qty) as shipped_amt
from csx_dw.dws_wms_r_d_ship_detail
where regexp_replace( to_date(send_time),'-','') >='20210801' 
    and  regexp_replace( to_date(send_time),'-','') <='20210831'
    and order_type_code like 'P%'
    and business_type_code in ('05')
    and status in ('6','7','8')
    group by shipped_location_code,goods_code,origin_order_no,supplier_code,
    order_type_code,
    wms_order_type_name,
    business_type_code,
    business_type_name,
    regexp_replace( to_date(send_time),'-','')
) a 
join 
(select goods_id,
    goods_name,
    unit_name,
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
    department_name,
    category_large_code,
    category_large_name
from csx_dw.dws_basic_w_a_csx_product_m 
    where sdt='current') b on a.goods_code=b.goods_id
join 
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    shop_name,
    company_code,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    and purpose  in ('01','02','03','07','08') 
) d on a.dc_code=d.shop_id
group by  
    order_no,
    dc_code,
    goods_code,
    supplier_code,
    division_code,
    division_name,
    goods_name,
    unit_name,
    brand_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    department_id,
    department_name,
    category_large_code,
    category_large_name,
     sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    shop_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    order_type_code,
    order_type_name,
    business_type_code,
    business_type_name,
    sdt
;

-- 关联采购订单&DC类型&复用供应商

-- create temporary table csx_tmp.temp_entry_01 as 
select      order_no,
    sales_region_code,
    sales_region_name,
    sales_province_code,
    sales_province_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    source_type,
    source_type_name,
    super_class_name ,
    dc_code,
    shop_name,
    j.company_code,
    goods_code,
    goods_name,
    unit_name,
    brand_name,
    department_id,
    department_name,
    -- division_code,
    -- division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    j.supplier_code,
    supplier_num as supplier_name,
    order_type_code,
    order_type_name,
    business_type_code,
    business_type_name,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
     no_tax_shipped_amt,
    if( joint_purchase_flag=0,'否','是') as joint_name ,
    local_purchase_flag,
    yh_reuse_tag,
    sdt,
    case when purpose ='07' then '20' when yh_reuse_tag='是' then '21' when  division_code in ('11','10') then '11' when  division_code in ('12','13','14','15') then '12' end division_group_code ,
    case when purpose ='07' then 'BBC' when yh_reuse_tag='是' then '复用供应商' when  division_code in ('11','10') then '生鲜' when  division_code in ('12','13','14','15') then '食百' end  division_group_name
from 
(select order_no,
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    shop_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    dc_code,
    goods_code,
    supplier_code,
    order_type_code,
    order_type_name,
    business_type_code,
    business_type_name,
    goods_name,
    unit_name,
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
    department_name,
    category_large_code,
    category_large_name,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
     no_tax_shipped_amt,
    a.sdt,
    source_type,
    source_type_name,
    super_class_name,
    local_purchase_flag
from csx_tmp.temp_entry_00 a 
join 
(select  order_code,
    source_type,
    source_type_name ,
       CASE
            WHEN a.super_class='1'
                THEN '供应商订单'
            WHEN a.super_class='2'
                THEN '供应商退货订单'
            WHEN a.super_class='3'
                THEN '配送订单'
            WHEN a.super_class='4'
                THEN '返配订单'
                ELSE a.super_class
        END super_class_name  ,
        a.local_purchase_flag
    from csx_dw.dws_scm_r_d_header_item_price a
    where 1=1
    -- super_class in ('2','1')  
   -- and source_type in ('1','10')
    group by  order_code,source_type,
        source_type_name,
         CASE
            WHEN a.super_class='1'
                THEN '供应商订单'
            WHEN a.super_class='2'
                THEN '供应商退货订单'
            WHEN a.super_class='3'
                THEN '配送订单'
            WHEN a.super_class='4'
                THEN '返配订单'
                ELSE a.super_class
        END ,
        super_class,
        a.local_purchase_flag
)b on a.order_no=b.order_code
) j 
left join  
(select company_code,supplier_code,
    supplier_num,
    yh_reuse_tag 
    from csx_tmp.ads_fr_r_m_supplier_reuse 
    where months='202107' ) s on j.company_code=s.company_code and j.supplier_code=s.supplier_code
LEFT JOIn 
(select product_code,shop_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') p on j.dc_code=p.shop_code and j.goods_code=p.product_code
;


select distinct business_type_code,business_type_name,order_type_code,order_type_name from csx_tmp.temp_entry_00 ;



show create table csx_dw.dws_wms_r_d_entry_batch;

-- source_type,source_type_name  
--1 采购导入
--2 直送
--3 一键代发
--4 项目合伙人
--5 无单入库
--8 云超物流采购
--10    智能补货
--11    商超直送
--13    云超门店采购
--14    临时地采
--15    联营直送
--16    永辉生活





set sdt='20210101';
set edt='20210731';

drop table csx_tmp.temp_entry_00 ;
create table csx_tmp.temp_entry_00 as
select   order_no,
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    shop_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    dc_code,
    goods_code,
    supplier_code,
    order_type_code,
    order_type_name,
    business_type_code,
    business_type_name,
    goods_name,
    unit_name,
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
    department_name,
    category_large_code,
    category_large_name,
    sum(receive_qty) receive_qty,
    sum(receive_amt) receive_amt,
    sum(no_tax_receive_amt)no_tax_receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(shipped_amt) shipped_amt,
    sum(no_tax_shipped_amt) no_tax_shipped_amt,
    sdt
from 
(
select origin_order_code order_no,
    receive_location_code as dc_code,
    goods_code,
    supplier_code,
    order_type_code,
    order_type_name,
    business_type as business_type_code,
    business_type_name,
    regexp_replace( to_date(receive_time ),'-','') as sdt,
    sum(receive_qty) receive_qty,
    sum(price/(1+tax_rate/100)*receive_qty) as no_tax_receive_amt,
    sum(price *receive_qty) as receive_amt,
    0 shipped_qty,
    0 no_tax_shipped_amt,
    0 shipped_amt 
from csx_dw.dws_wms_r_d_entry_batch
where sdt>='20210101' 
    and regexp_replace( to_date(receive_time ),'-','')<='20210831'
    and  regexp_replace( to_date(receive_time ),'-','')>='20210801'
   -- and order_type_code like 'P%'
   -- and business_type !='02'
    and receive_status in ('1','2')
   group by receive_location_code,
    regexp_replace( to_date(receive_time ),'-','') ,
   goods_code,
   origin_order_code,
   supplier_code,
   order_type_code,
    order_type_name,
    business_type,
    business_type_name
union all 
select origin_order_no order_no, 
    shipped_location_code as dc_code,
    goods_code,
    supplier_code,
    order_type_code,
    wms_order_type_name as order_type_name,
    business_type_code,
    business_type_name,
    regexp_replace( to_date(send_time),'-','') as sdt,
    0 receive_qty,
    0 no_tax_receive_amt,
    0 receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(price/(1+tax_rate/100)*shipped_qty) as no_tax_shipped_amt,
     sum(price *shipped_qty) as shipped_amt
from csx_dw.dws_wms_r_d_ship_detail
where regexp_replace( to_date(send_time),'-','') >='20210801' 
    and  regexp_replace( to_date(send_time),'-','') <='20210831'
    and order_type_code like 'P%'
    and business_type_code in ('05')
    and status in ('6','7','8')
    group by shipped_location_code,goods_code,origin_order_no,supplier_code,
    order_type_code,
    wms_order_type_name,
    business_type_code,
    business_type_name,
    regexp_replace( to_date(send_time),'-','')
) a 
join 
(select goods_id,
    goods_name,
    unit_name,
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
    department_name,
    category_large_code,
    category_large_name
from csx_dw.dws_basic_w_a_csx_product_m 
    where sdt='current') b on a.goods_code=b.goods_id
join 
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    shop_name,
    company_code,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    and purpose  in ('01','02','03','07','08') 
) d on a.dc_code=d.shop_id
group by  
    order_no,
    dc_code,
    goods_code,
    supplier_code,
    division_code,
    division_name,
    goods_name,
    unit_name,
    brand_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    department_id,
    department_name,
    category_large_code,
    category_large_name,
     sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    shop_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    order_type_code,
    order_type_name,
    business_type_code,
    business_type_name,
    sdt
;

-- 关联采购订单&DC类型&复用供应商

-- create temporary table csx_tmp.temp_entry_01 as 
select      order_no,
    sales_region_code,
    sales_region_name,
    sales_province_code,
    sales_province_name,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    source_type,
    source_type_name,
    super_class_name ,
    dc_code,
    shop_name,
    j.company_code,
    goods_code,
    goods_name,
    unit_name,
    brand_name,
    department_id,
    department_name,
    -- division_code,
    -- division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    category_large_code,
    category_large_name,
    j.supplier_code,
    supplier_num as supplier_name,
    order_type_code,
    order_type_name,
    business_type_code,
    business_type_name,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
     no_tax_shipped_amt,
    if( joint_purchase_flag=0,'否','是') as joint_name ,
    local_purchase_flag,
    yh_reuse_tag,
    sdt,
    case when purpose ='07' then '20' when yh_reuse_tag='是' then '21' when  division_code in ('11','10') then '11' when  division_code in ('12','13','14','15') then '12' end division_group_code ,
    case when purpose ='07' then 'BBC' when yh_reuse_tag='是' then '复用供应商' when  division_code in ('11','10') then '生鲜' when  division_code in ('12','13','14','15') then '食百' end  division_group_name
from 
(select order_no,
    sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    shop_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    purpose_name,
    dc_code,
    goods_code,
    supplier_code,
    order_type_code,
    order_type_name,
    business_type_code,
    business_type_name,
    goods_name,
    unit_name,
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
    department_name,
    category_large_code,
    category_large_name,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
     no_tax_shipped_amt,
    a.sdt,
    source_type,
    source_type_name,
    super_class_name,
    local_purchase_flag
from csx_tmp.temp_entry_00 a 
join 
(select  order_code,
    source_type,
    source_type_name ,
       CASE
            WHEN a.super_class='1'
                THEN '供应商订单'
            WHEN a.super_class='2'
                THEN '供应商退货订单'
            WHEN a.super_class='3'
                THEN '配送订单'
            WHEN a.super_class='4'
                THEN '返配订单'
                ELSE a.super_class
        END super_class_name  ,
        a.local_purchase_flag
    from csx_dw.dws_scm_r_d_header_item_price a
    where 1=1
    -- super_class in ('2','1')  
   -- and source_type in ('1','10')
    group by  order_code,source_type,
        source_type_name,
         CASE
            WHEN a.super_class='1'
                THEN '供应商订单'
            WHEN a.super_class='2'
                THEN '供应商退货订单'
            WHEN a.super_class='3'
                THEN '配送订单'
            WHEN a.super_class='4'
                THEN '返配订单'
                ELSE a.super_class
        END ,
        super_class,
        a.local_purchase_flag
)b on a.order_no=b.order_code
) j 
left join  
(select company_code,supplier_code,
    supplier_num,
    yh_reuse_tag 
    from csx_tmp.ads_fr_r_m_supplier_reuse 
    where months='202107' ) s on j.company_code=s.company_code and j.supplier_code=s.supplier_code
LEFT JOIn 
(select product_code,shop_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') p on j.dc_code=p.shop_code and j.goods_code=p.product_code
;


select distinct business_type_code,business_type_name,order_type_code,order_type_name from csx_tmp.temp_entry_00 ;



show create table csx_dw.dws_wms_r_d_entry_batch;

-- source_type,source_type_name  
--1 采购导入
--2 直送
--3 一键代发
--4 项目合伙人
--5 无单入库
--8 云超物流采购
--10    智能补货
--11    商超直送
--13    云超门店采购
--14    临时地采
--15    联营直送
--16    永辉生活


