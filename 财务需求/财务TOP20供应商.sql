
-- 财务TOP20供应商
select supplier_code,
    supplier_name,
    AMT,
    no_tax_amt,
      a 
from (
select supplier_code,
    supplier_name,
    AMT,
    no_tax_amt,
    row_number()over(order by no_tax_amt desc) as a 
from 
(select supplier_code,
    supplier_name,
    SUM(amt) AMT,
    SUM(no_tax_amt)no_tax_amt
from (
select supplier_code,supplier_name,
sum(price*receive_qty) amt,
sum(price/(1+tax_rate/100)*receive_qty) as no_tax_amt
from csx_dw.dws_wms_r_d_entry_detail 
where 
SDT>='20200101'
AND SDT<'20210701'
and ( ( order_type_code like 'P%' 
and business_type in ('01'))
OR business_type in ('ZN01') )
 group by  supplier_code,supplier_name
 union all 
 
select supplier_code,supplier_name,
sum(price*shipped_qty)*-1 amt,
sum(price/(1+tax_rate/100)*shipped_qty)*-1 as no_tax_amt
from csx_dw.dws_wms_r_d_ship_detail 
where 
SDT>='20200101'
AND SDT<'20210701'
and ( ( order_type_code like 'P%' 
and business_type_code in ('05'))
OR business_type_code in ('ZNR1') )
 group by  supplier_code,supplier_name
 ) a 
 group by supplier_code,
    supplier_name
    
)a 
)a     where  a<21
 order by no_tax_amt desc ;