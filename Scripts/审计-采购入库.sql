

--采购入库
select
    years,
    months,
    b.company_code ,
    b.company_name,
    receive_location_code ,
    shop_name,
    supplier_code ,
    vendor_name,
    f.company_code ,
    f.company_name ,
    goods_code ,
    goods_name ,
    department_id ,
    department_name,
    division_code ,
    division_name,
    qty,
    receive_amt,
    no_tax_amt,
    entry_type,
    business_type
from 
 (select
    entry_type,
    business_type,
    substr(sdt,1,4) as years,
    substr(sdt,1,6) as months,
    receive_location_code ,
    supplier_code ,
    goods_code ,
    sum(receive_qty) qty,
    sum(amount)receive_amt,
    sum(amount /((1+tax_rate/100)) )as no_tax_amt
from
    csx_dw.wms_entry_order
where
    sdt >= '20200701'
    and sdt<'20201001'
    and (entry_type like '采购入库%' or business_type like '调拨%' or business_type like '采购%')
group by 
    substr(sdt,1,4),
    substr(sdt,1,6),
    receive_location_code ,
    supplier_code ,
     goods_code ,
    entry_type,
    business_type
   )a 
left join 
(select location_code,
    shop_name ,
    company_code ,
    company_name
from csx_dw.csx_shop where sdt='current') b on a.receive_location_code=b.location_code
left join 
(select goods_id ,
    goods_name ,
    unit ,
    department_id ,
    department_name ,
    division_code ,
    division_name 
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_code=c.goods_id
left join 
(select vendor_id ,
    vendor_name
from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current' )d on a.supplier_code=d.vendor_id
left join 
(select concat('S',location_code) as shop_id,company_code ,company_name 
from csx_dw.csx_shop where sdt='current'
union all 
select DISTINCT concat('G',company_code) as shop_id,company_code ,company_name 
from csx_dw.csx_shop where sdt='current'
) f on a.supplier_code=f.shop_id
;


select sum(receive_qty) qty,
    sum(amount )receive_amt,
    sum(amount/((1+tax_rate/100)) )as no_tax_amt
from
    csx_dw.wms_entry_order
where
    sdt >= '20200101'
    and sdt<'20200701'
    and business_type_code ='02'
;
select sum(receive_qty) qty,
    sum(amount )receive_amt,
    sum(amount/((1+tax_rate/100)) )as no_tax_amt
from
    csx_dw.dws_wms_r_d_entry_order_all_detail 
where
    sdt >= '20200101'
    and sdt<'20200701'
    and business_type ='02'
;

select company_code,company_name ,supplier_code,sum(receive_qty) qty,
    sum(amount )receive_amt,
    sum(amount/((1+tax_rate/100)) )as no_tax_amt
from
    csx_dw.dws_wms_r_d_entry_order_all_detail a 
 join 
(select concat('S',location_code)as dc_code,company_code,company_name from csx_dw.csx_shop where sdt='current' and table_type =2) b 
    on a.supplier_code =b.dc_code
where
    sdt >= '20190101'
    and sdt<'20200101'
    and business_type ='ZC01'
    and sys ='old'
    group by company_code,company_name ,supplier_code;
;
--采购出库
select
    years,
    months,
    company_code ,
    company_name,
    shipped_location_code ,
    shop_name,
    supplier_code ,
    supplier_name ,
    department_id ,
    department_name,
    division_code ,
    division_name,
    qty,
    shipped_amt,
    no_tax_amt
from 
 (select
    substr(sdt,1,4) as years,
    substr(sdt,1,6) as months,
    shipped_location_code ,
    supplier_code ,
    supplier_name ,
    department_id ,
    department_name,
    division_code ,
    division_name,
    sum(coalesce(shipped_qty,0) ) qty,
    sum(amount ) shipped_amt,
    sum(amount/((1+tax_rate /100)) )as no_tax_amt
from
    csx_dw.wms_shipped_order 
where
    sdt >= '20190101'
    and sdt<'20200701'
    and (business_type IN ('申偿出库(old)','直送退供出库','返配出库(old)','退供出库') )
group by 
    substr(sdt,1,4),
    substr(sdt,1,6),
    shipped_location_code ,
    supplier_code ,
    supplier_name ,
    department_id ,
    department_name,
    division_code ,
    division_name
   )a 
left join 
(select location_code,
    shop_name ,
    company_code ,
    company_name
from csx_dw.csx_shop where sdt='current') b on a.shipped_location_code=b.location_code
;

-- 销售数据
 select 
    substr(month,1,4) as years,
    channel_name ,
    province_code ,
    province_name ,
    customer_no,
    customer_name,
    `attribute` ,
    first_category ,
    second_category ,
    third_category ,
    sales_name ,
    work_no ,
    division_code ,
    division_name ,
    department_code ,
    department_name ,
    sum(sales_value )sales_value ,
    sum(sales_cost )sales_cost ,
    sum(profit )profit,
    sum(excluding_tax_sales)as no_tax_sales,
    sum(excluding_tax_cost ) as no_tax_cost,
    sum(excluding_tax_profit ) as no_tax_profit
from
    csx_dw.ads_sale_r_m_customer_goods_sale
where
    month >= '201901'
group by 
channel_name ,
    province_code ,
    province_name ,
    customer_no,
    customer_name,
    `attribute` ,
    first_category ,
    second_category ,
    third_category ,
    sales_name ,
    work_no ,
    division_code ,
    division_name ,
    department_code ,
    department_name,
    substr(month,1,4) ;


--调拨入库 
select
    years,
    months,
company_code ,
company_name,
 receive_location_code ,
 shop_name,
    v_compan_code,
    v_compan_name ,
    supplier_code ,
    vendor_name ,
    department_id ,
    department_name,
    division_code ,
    division_name,
    qty,
    shipped_amt,
    no_tax_amt
from 
 (select
    substr(sdt,1,4) as years,
    substr(sdt,1,6) as months,
    receive_location_code ,
    supplier_code , 
    department_id ,
    department_name ,
    division_code ,
    division_name, 
    sum(coalesce(receive_qty ,0) ) qty,
    sum(amount) shipped_amt,
    sum(amount/((1+tax_rate /100)) )as no_tax_amt
from
    csx_dw.dws_wms_r_d_entry_order_all_detail a 
    join 
(select goods_id ,department_id ,department_name ,division_code ,division_name 
    from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b
on a.goods_code =b.goods_id
where
    a.sdt >= '20190101'
    and a.sdt<'20200701'
    and sys='old'
    and a.business_type ='ZC01'
 --   and business_type IN ('调拨入库(old)')
group by department_id ,
    department_name ,
    division_code ,
    division_name, 
    substr(sdt,1,4),
    substr(sdt,1,6),
    receive_location_code,
    supplier_code 
   )a 
join 
(select concat('S',shop_id)as vendor_id,shop_name as vendor_name,company_code as v_compan_code,company_name as v_compan_name
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt='current' and table_type=2) b on a.supplier_code=b.vendor_id
join 
(select location_code,
    shop_name ,
    company_code ,
    company_name
from csx_dw.csx_shop where sdt='current'and table_type=1) c on a.receive_location_code=c.location_code

;


--调拨入库 
select
    years,
    sum(qty),
    sum(shipped_amt),
    sum(no_tax_amt)
from 
 (select
    substr(sdt,1,4) as years,
    substr(sdt,1,6) as months,
    receive_location_code ,
    supplier_code ,
--    department_id ,
--    department_name,
--    division_code ,
--    division_name,
    sum(coalesce(receive_qty ,0) ) qty,
    sum(amount) shipped_amt,
    sum((price*receive_qty)/((1+tax_rate /100)) )as no_tax_amt
from
    csx_dw.dws_wms_r_d_entry_order_all_detail 
where
    sdt >= '20190101'
    and sdt<'20200701'
    and (business_type IN ('ZC01') )
group by 
    substr(sdt,1,4),
    substr(sdt,1,6),
    receive_location_code,
    supplier_code 
--    department_id ,
--    department_name,
--    division_code ,
--    division_name
   )a 
join 
(select concat('S',shop_id)as vendor_id,company_code as v_compan_code,company_name as v_compan_name
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt='current' and table_type=2 ) b on a.supplier_code=b.vendor_id
join 
(select location_code,
    shop_name ,
    company_code ,
    company_name
from csx_dw.csx_shop where sdt='current'and table_type=1) c on a.receive_location_code=c.location_code
group by years
;

select pur_doc_id order_code,sum(tax_pur_val_in)bb,0 aa from b2b.ord_orderflow_t 
where shop_id_in ='W039' and vendor_id ='SW0C8' and sdt >='20190301' and sdt<='20190331';
union all 
select order_code,0 bb ,sum(amount)aa from csx_dw.dws_wms_r_d_entry_order_all_detail 
where receive_location_code ='W039' and supplier_code ='SW0C8' and sdt >='20190301' and sdt<='20190331';



