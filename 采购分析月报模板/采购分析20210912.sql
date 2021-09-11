set sdt='20210101';
set edt='20210731';

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
-- select * from csx_tmp.temp_entry_01 where sales_region_name ='大宗' and province_name='福建省'; 
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
    case when purchase_org ='P620' and purpose!='07' then '9' else  sales_region_code end sales_region_code,
    case when purchase_org ='P620' and purpose!='07' then '大宗' else  sales_region_name end sales_region_name,
    shop_id,
    company_code,
    case when shop_id in ('W0H4','W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4','W0G1','W0J8')  then '' else city_name end  city_name,
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
(select company_code,supplier_code,yh_reuse_tag from csx_tmp.ads_fr_r_m_supplier_reuse where months=substr(${hiveconf:edt},1,6) ) s on j.company_code=s.company_code and j.supplier_code=s.supplier_code
;



drop table csx_tmp.temp_entry_02;
 create   table csx_tmp.temp_entry_02 as 
select 
    type,
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
    case when sales_region_code!='9' then '1' else '2' end type,
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
    section,
    type
grouping sets 
((  type,section,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_type_code,
    supplier_type_name),
    (  type,section,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name),     --城市汇总
    (type,section,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name),        -- 省区层级 
    (type,
    section,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name),      --省区汇总
    (  type,section,
    sales_region_code,
    sales_region_name,
    supplier_type_code,
    supplier_type_name),
    (  type,
    section,
    sales_region_code,
    sales_region_name),
    ( section,
    supplier_type_code,
    supplier_type_name),
    ( type,
    section,
    supplier_type_code,
    supplier_type_name),
     (type, 
     section),
    ( section)
    )
;

--区间汇总
drop table if exists csx_tmp.temp_entry_03;
 create   table csx_tmp.temp_entry_03 as 
select 
    type,
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
    type,
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
        case when sales_region_code!='9' then '1' else '2' end type,

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
where 1=1
group by sales_region_code,
    sales_region_name,
    city_code,
    city_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name,
    type
grouping sets 
((  type,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    supplier_type_code,
    supplier_type_name),
    (  type,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name),     --城市汇总
    (type,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    supplier_type_code,
    supplier_type_name),        -- 省区层级 
    (type,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name),      --省区汇总
    ( type,
    sales_region_code,
    sales_region_name,
    supplier_type_code,
    supplier_type_name),
    ( type,
    sales_region_code,
    sales_region_name),
    ( supplier_type_code,
    supplier_type_name),
    ( type,
    supplier_type_code,
    supplier_type_name),
    ( type),
    ()
    )
;


drop table  csx_tmp.temp_supplier_type_analysis;

create table csx_tmp.temp_supplier_type_analysis as
select case when sales_region_code is null and province_code is  null then '0' else type end as type ,
    case when sales_region_code is null and province_code is  null then '00' else sales_region_code end sales_region_code,
    case when sales_region_code is null and province_code is  null then '合计' else sales_region_name end sales_region_name,
    case when province_code is null and city_code is  null then '00'  else province_code end province_code,
    case when province_code is null and city_code is  null then '合计'else province_name end province_name,
    case when city_code is null then '00' else city_code end city_code,
    case when city_code is null then '合计' else city_name end city_name,
    case when supplier_type_code is null then '00' else    supplier_type_code end supplier_type_code,
    case when supplier_type_name is null then '合计' else  supplier_type_name end supplier_type_name,
    section,
    supplier_num,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    net_receive_qty,
    net_receive_amt,
    no_tax_net_receive_amt
from csx_tmp.temp_entry_02
union all 
select   case when sales_region_code is null and province_code is  null then '0' else type end as type ,
    case when sales_region_code is null and province_code is  null then '00' else sales_region_code end sales_region_code,
    case when sales_region_code is null and province_code is  null then '合计' else sales_region_name end sales_region_name,
    case when province_code is null and city_code is  null then '00'  else province_code end province_code,
    case when province_code is null and city_code is  null then '合计'else province_name end province_name,
    case when city_code is null then '00' else city_code end city_code,
    case when city_code is null then '合计' else city_name end city_name,
    case when supplier_type_code is null then '00' else    supplier_type_code end supplier_type_code,
    case when supplier_type_name is null then '合计' else  supplier_type_name end supplier_type_name,
    section,
    supplier_num,
    receive_qty,
    receive_amt,
    no_tax_receive_amt,
    shipped_qty,
    shipped_amt,
    no_tax_shipped_amt,
    net_receive_qty,
    net_receive_amt,
    no_tax_net_receive_amt
from csx_tmp.temp_entry_03;

show create table csx_dw.dws_wms_r_d_entry_batch;

select * from csx_tmp.temp_supplier_type_analysis where type='0';
-- source_type,source_type_name  
--1 采购导入
--2 直送客户
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


select * from csx_tmp.temp_entry_02 where type is null and sales_region_code is null    ;