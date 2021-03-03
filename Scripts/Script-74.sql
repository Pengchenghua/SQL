select mon,sum(sale)sale,sum(sale1)sale1,sum(sale2)sale2 ,sum(sale-sale1),sum(sale1-sale2) from (
SELECT substr(sdt,1,6)mon,sum(sales_value)sale,0 sale1,0 sale2 from csx_dw.customer_sales  group by substr(sdt,1,6)
UNION all
SELECT substr(sdt,1,6)mon,0 sale ,sum(sales_value)sale1 ,0 sale2 from csx_dw.customer_sale_m where sales_type  in('qyg','gc','anhui','sc')  group by substr(sdt,1,6)
UNION all
SELECT substr(sdt,1,6)mon,0 sale ,0 sale1 ,sum(sales_value)sale2 from csx_dw.sale_goods_m1 where sales_type  in('qyg','gc','anhui','sc')  group by substr(sdt,1,6)
)a group by mon;

select kunnr,budat,belnr
,SUM(cast( dmbtr as decimal(26,3))),prctr from ods_ecc.ecc_ytbcustomer   where sdt='20191102'  and kunnr='0000105472' and budat>='20191001' AND budat <='20191031'
group by kunnr,budat,prctr,belnr;

select * from  ods_ecc.ecc_ytbcustomer a  where sdt='20191102'  and kunnr='0000105472' and budat>='20191001' AND budat <='20191031' and hkont LIKE '1122%'
  AND substr(a.belnr,1,1)<>'6'
  AND mandt='800' 
  AND (substr(kunnr,1,1) NOT IN ('G',
                                 'L',
                                 'V',
                                 'S')
       OR kunnr='S9961')