--供应商净入库指定
select yy,supplier_code ,supplier_name ,sum(receive)receive,sum(shipper )shipper,SUM(receive-shipper) 
from (
select substr(sdt,1,4) yy,supplier_code ,supplier_name,receive_location_code as dc_code,
sum(case when business_type like 'ZXR%' THEN ((price/(1+tax_rate/100))*receive_qty) /10000*-1 
	ELSE (price/(1+tax_rate/100))*receive_qty/10000 END) receive, 
0 shipper
from csx_dw.dws_wms_r_d_entry_detail  
where sdt<'20210701'
and supplier_code  in ('20044321',
'20032426',
'20020295',
'119920CQ',
'B10028',
'20020348',
'20030922',
'20020588',
'20041946',
'20042232',
'20048365',
'20045649',
'20044616',
'20041308',
'20044647')
GROUP  by substr(sdt,1,4),supplier_code ,supplier_name,receive_location_code
union all 
select substr(sdt,1,4),supplier_code ,supplier_name,shipped_location_code as dc_code,0 receive, sum((price/(1+tax_rate/100))*shipped_qty) /10000  as shipper
from csx_dw.dws_wms_r_d_ship_detail 
where sdt<'20210701'
and supplier_code  in ('20044321',
'20032426',
'20020295',
'119920CQ',
'B10028',
'20020348',
'20030922',
'20020588',
'20041946',
'20042232',
'20048365',
'20045649',
'20044616',
'20041308',
'20044647')
GROUP  by substr(sdt,1,4),supplier_code ,supplier_name,shipped_location_code
) a
join 
(select location_code,zone_id,zone_name,purpose_code,purpose from csx_dw.csx_shop 
where sdt='current' 
    and table_type=1 
   -- and purchase_org !='P620' 
    -- and dist_code='15'
    and purpose_code in ('01','02','03','08','07') ) b on a.dc_code=b.location_code
group by  yy,supplier_code ,supplier_name ;







--供应商净入库指定
select yy,sum(receive)receive,sum(shipper )shipper,SUM(receive-shipper) 
from (
select substr(sdt,1,4) yy,supplier_code ,supplier_name,a.receive_location_code as dc_code ,
sum(case when business_type like 'ZXR%' THEN ((price/(1+tax_rate/100))*receive_qty) /10000*-1 
	ELSE (price/(1+tax_rate/100))*receive_qty/10000 END) receive, 
0 shipper
from csx_dw.dws_wms_r_d_entry_detail a  where 
(a.business_type in ('ZN01','ZN02','ZC01','ZXR1','ZXR2','ZCR1')
       OR a.order_type_code LIKE 'P%'  )
--supplier_code  in ('20044321',
--'20032426',
--'20020295',
--'119920CQ',
--'B10028',
--'20020348',
--'20030922',
--'20020588',
--'20041946',
--'20042232',
--'20048365',
--'20045649',
--'20044616',
--'20041308',
--'20044647')
GROUP  by substr(sdt,1,4),supplier_code ,supplier_name,a.receive_location_code 
union all 
select substr(sdt,1,4),
supplier_code ,
supplier_name,
b.shipped_location_code as dc_code,
0 receive, 
sum((price/(1+tax_rate/100))*shipped_qty) /10000  as shipper
from csx_dw.dws_wms_r_d_ship_detail b
where (b.business_type_code in ('ZXR1','ZXR2','ZCR1')
       OR b.order_type_code LIKE 'P%'  )
--supplier_code  in ('20044321',
--'20032426',
--'20020295',
--'119920CQ',
--'B10028',
--'20020348',
--'20030922',
--'20020588',
--'20041946',
--'20042232',
--'20048365',
--'20045649',
--'20044616',
--'20041308',
--'20044647')
GROUP  by substr(sdt,1,4),supplier_code ,supplier_name,b.shipped_location_code 
) a
join 
(select location_code,zone_id,zone_name,purpose_code,purpose from csx_dw.csx_shop 
where sdt='current' 
    and table_type=1 
   -- and purchase_org !='P620' 
    -- and dist_code='15'
    and purpose_code in ('01','02','03','08','07') ) b on a.dc_code=b.location_code
group by  yy;