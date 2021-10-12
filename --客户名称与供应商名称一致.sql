--客户名称与供应商名称一致
with temp_entry as 
(select 
    b.company_code,
    b.company_name,
    b.city_code,
    b.city_name,
    customer_name,
    sum(sales_value) sales 
from csx_dw.dws_sale_r_d_detail a 
 join 
 (select shop_id,company_code,company_name,city_code,city_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current')b on a.dc_code=b.shop_id
where sdt>='20210601' 
and sdt<'20211001'
group by  b.company_code,
    b.company_name,
    b.city_code,
    b.city_name,
    customer_name),
temp_sale as 
(select company_code,
    company_name,
    b.city_code,
    b.city_name ,
    supplier_name,
    sum(price*receive_qty) as entry_amt
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select shop_id,company_code,company_name,city_code,city_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current')b on a.receive_location_code=b.shop_id
where sdt>='20210601'
and sdt<'20211001'
group by company_code,company_name,b.city_code,b.city_name,a.supplier_name
)
select a.*,b.supplier_name,entry_amt
from temp_entry a 
join 
temp_sale b on a.company_code=b.company_code and a.city_code=b.city_code and a.customer_name=b.supplier_name
;