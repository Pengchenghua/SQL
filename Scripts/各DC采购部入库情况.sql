-- 7-9月各DC入库情况 
select
    years,
    months,
    dist_code,
    dist_name,
    b.company_code ,
    b.company_name,
    receive_location_code ,
    shop_name,
    a.supplier_code ,
    d.vendor_name,
    f.company_code ,
    f.company_name ,
    business_division_code ,
	business_division_name,
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
    business_division_code ,
	business_division_name,
    sum(receive_qty) qty,
    sum(amount)receive_amt,
    sum(amount /((1+tax_rate/100)) )as no_tax_amt
from
    csx_dw.wms_entry_order a 
join 
(select business_division_code ,
		business_division_name,
		category_small_code 
from csx_dw.dws_basic_w_a_category_m where sdt='current') c on a.category_small_code=c.category_small_code
where
    sdt >= '20200701'
    and sdt<'20201001'
    and (entry_type like '采购入库%' or business_type like '调拨%' or business_type like '采购%')
group by 
    substr(sdt,1,4),
    substr(sdt,1,6),
    receive_location_code ,
    supplier_code ,
    business_division_code ,
	business_division_name,
    entry_type,
    business_type
   )a 
left join 
(select location_code,
    shop_name ,
    dist_code,
    dist_name,
    company_code ,
    company_name
from csx_dw.csx_shop where sdt='current') b on a.receive_location_code=b.location_code
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