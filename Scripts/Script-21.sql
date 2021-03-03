--采购入库
select
    years,
    months,
    company_code ,
    company_name,
    dist_code ,
    dist_name,
    business_type ,
    receive_location_code ,
    shop_name,
    supplier_code ,
    supplier_name ,
    department_id ,
    department_name,
    division_code ,
    division_name,
    qty,
    receive_amt,
    no_tax_amt
from 
 (select
    substr(sdt,1,4) as years,
    substr(sdt,1,6) as months,
    business_type ,
    receive_location_code ,
    order_code ,
    supplier_code ,
    supplier_name ,
    department_id ,
    department_name,
    division_code ,
    division_name,
    sum(receive_qty) qty,
    sum(amount )receive_amt,
    sum(amount/((1+tax_rate/100)) )as no_tax_amt
from
    csx_dw.wms_entry_order
where
    sdt >= '20200801'
    and sdt<'20200826'
    and (entry_type like '采购入库%' )
group by 
    substr(sdt,1,4),
    substr(sdt,1,6),
    receive_location_code ,
    supplier_code ,
    supplier_name ,
    department_id ,
    department_name,
    division_code ,
    division_name,
    business_type ,
    order_code 
   )a 
left join 
(select location_code,
    shop_name ,
    company_code ,
    company_name,
    dist_code ,
    dist_name
from csx_dw.csx_shop where sdt='current') b on a.receive_location_code=b.location_code
;