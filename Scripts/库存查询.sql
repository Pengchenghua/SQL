select dc_code ,
    dc_name ,   
    goods_code ,
    goods_name ,
    reservoir_area_code ,
    reservoir_area_name ,
    sum(qty)qty ,
    sum(amt)amt
from csx_dw.dws_wms_r_d_accounting_stock_m 
where sdt='20200809'
    and goods_code in ('912549','912610','912549','912612','912549','1042144','912549','960029','947800','','930900','930899','912612','912611','912610','912548','912547','912546','912545','828917','1027316
')  and category_large_code ='1104'
and reservoir_area_code not in ('TS01','PD01','PD02')
   GROUP BY  dc_code ,reservoir_area_code ,
    reservoir_area_name ,
    dc_name ,   
    goods_code ,
    goods_name ;

select a.receive_location_code,a.goods_code,a.goods_name,qty,amt,b.sdt 
from 
 (select receive_location_code,goods_code ,goods_name ,sum(receive_qty )qty,sum(amount )amt,sdt from csx_dw.wms_entry_order where sdt>='20190101' 
    and goods_code in ('912549','912610','912549','912612','912549','1042144','912549','960029','947800','','930900','930899','912612','912611','912610','912548','912547','912546','912545','828917','1027316
')  and receive_location_code ='W0A7'
and entry_type like '%采购%'
group by receive_location_code,sdt,goods_code,goods_name 
)a
join 
(select receive_location_code,goods_code,goods_name ,max(sdt )sdt from csx_dw.wms_entry_order where sdt>='20190101' 
    and goods_code in ('912549','912610','912549','912612','912549','1042144','912549','960029','947800','','930900','930899','912612','912611','912610','912548','912547','912546','912545','828917','1027316
')  and receive_location_code ='W0A7'
and entry_type like '采购%'
group by receive_location_code,goods_code,goods_name
)b on a.sdt=b.sdt and a.goods_code=b.goods_code
;
