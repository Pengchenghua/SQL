
select mon,
    dist_code,
    dist_name,
    receive_location_code,
    shop_name,
    supplier_code,
    vendor_name,
    goods_code,
    goods_name,
    department_id,
    department_name,
    qty,
    amount,
    no_tax_amount,
    business_type ,
    entry_type 
from 
(select substr(sdt,1,6)mon, 
    receive_location_code,
    supplier_code ,
    goods_code,
    business_type ,
    entry_type ,
    sum(receive_qty)qty,
    sum(receive_qty*price)amount,
    sum(amount/(1+tax_rate/100)) as no_tax_amount
from csx_dw.wms_entry_order 
where sdt>='20200101' and sdt<'20200701'
    and (entry_type like '采购%' or  business_type like '采购入库%')
    AND department_id in ('A03','A05')
--   and receive_location_code in ('W0G1','W0D4','C052','W0J7','W0J8','W0K2','W0L1','W0L2','W0L9','C331','W0R0','W0V3')
 --  and supplier_code in ('20032701','20032974','20033844','20033845','20033895','20033974','20034399','20034620','20034665','20034669','20034764','20034805','20034836','20036145','20036152','20036158','20036159','20036317','20036377','20036382','20036530','20036697','20036855','20036934','20036957','20036961','20037039','20037041','20038135','20038384','20037939','20041669','20038797','20039835','20040804','20040803','20039498','20040682','20039831','20041125','20040043','20041185','20033998','20041308','20038130','20038801','20038780','20038783','20038890','20039258','20039365','20039531','20039470','20041298','20041559','20041552','20041672','20041946','20038849','20027362','20034081','20039257','20039368','20038982','20039708','20039845','20039882','20040055','20042128','20042217','20042213','20042175','20042232','20042293','20042323','20042386','20042410','20042413','20042508','20042541','20042626','20042792','20043334','20043468','20043514','20043944','20043991','20043940','20043943','20043795','20043987','20044261','20044256','20044580','20044613','20044612','20044598','20044838','20044974','20044993','20044995')
group by
    receive_location_code,
    goods_code,
    supplier_code,
    business_type ,
    entry_type ,
    substr(sdt,1,6)
) a 
left join 
(select location_code,shop_name,dist_code,dist_name
    from csx_dw.csx_shop where sdt='current') b on a.receive_location_code=location_code 
left join 
(select goods_id,
    goods_name,
    department_id,
    department_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_code=c.goods_id
left join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') d on a.supplier_code=d.vendor_id
;

