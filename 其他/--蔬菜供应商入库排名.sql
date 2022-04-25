--蔬菜供应商入库排名
select   province_code,
       province_name,
       supplier_code,
    supplier_name,
    a.division_code,
    a.division_name,
    a.category_large_code,
    a.category_large_name,
    qty,
    amt,
     aa 
from (

select   province_code,
       province_name,
       supplier_code,
    supplier_name,
    a.division_code,
    a.division_name,
    a.category_large_code,
    a.category_large_name,
    qty,
    amt,
    row_number()over(partition by division_code order by amt desc) as aa 
from (
select   province_code,
       province_name,
       supplier_code,
    supplier_name,
    a.division_code,
    a.division_name,
    a.category_large_code,
    a.category_large_name,
    sum(receive_qty) as qty,
    sum(price*receive_qty) as amt
from csx_dw.dws_wms_r_d_entry_detail a
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
  and purchase_org !='P620'
  and shop_id not in ('W0J8') --,'W0K4'
  AND purpose IN ('01')
  ) b on a.receive_location_code=b.shop_id
where sdt>='20210101' 
    and sdt<'20210924'
    and receive_status in (1,2)
    and a.order_type_code LIKE 'P%' 
    and a.business_type='01'
    and a.category_large_code='1103'
group by supplier_code,
    supplier_name,
    a.division_code,
    a.division_name,
    a.category_large_code,
    a.category_large_name,
      province_code,
       province_name
    )a 
    )a where aa<101
    ;
    
    select  business_type_name,business_type_code from csx_dw.dws_wms_r_d_ship_detail where  sdt>='20210101' 
    and sdt<'20210924'
    and supplier_code='G1933'
   -- and receive_location_code in ('W0N1','W0K7' )
 --   group by receive_location_code,receive_location_name;
 ;
 
 
 select substr(sdt,1,6) mon,
    receive_location_code dc_code,
    goods_code,
    supplier_code,
    sum(case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) qty,
    sum(price*case when business_type like 'ZNR%' THEN receive_qty*-1 ELSE receive_qty END) amt,
    0 shipp_qty,
    0 shipp_amt
from csx_dw.dws_wms_r_d_entry_detail a
where 1=1 
and sdt>='20210101' 
and sdt<'20210701'
and receive_status in (1,2)
and (a.business_type in ('ZN01','ZN02','ZNR1','ZNR2')
       OR (a.order_type_code LIKE 'P%' and business_type !='02')  )
group by substr(sdt,1,6) ,
    receive_location_code  ,
    goods_code,
    supplier_code