-- 全国 厂商入库占比
 select months,super_class_name, 
 business_type_name,
 sum(receive_amt) all_amt,
 sum(case when supplier_code in ('20054206',
'20054270',
'20055149',
'20054478',
'20052301',
'20052504',
'20054518',
'20054739',
'20054778',
'20054287',
'116816BJ',
'109436CQ',
'20053812',
'20053405',
'20054624',
'20054909',
'20054578',
'20054422',
'20054211',
'20051311',
'20051950' ) then receive_amt end )qg_amt
from csx_tmp.report_fr_r_m_financial_purchase_detail 
where months>='202201' and purpose in('01','02','03','07','08')
   and super_class_name ='供应商订单'
   and business_type_name='供应商配送'
   and division_code in ('10','11','12','13','14')
group by months,super_class_name,business_type_name
 ;
 
 
  select months,supplier_code,supplier_name, 
    goods_code,
    goods_name,
    unit_name,
    classify_middle_code,
    classify_middle_name,
 sum(receive_amt) all_amt
from csx_tmp.report_fr_r_m_financial_purchase_detail 
where months>='202201' and purpose in('01','02','03','07','08')
   and super_class_name ='供应商订单'
   and business_type_name='供应商配送'
   and supplier_code in ('20054206',
'20054270',
'20055149',
'20054478',
'20052301',
'20052504',
'20054518',
'20054739',
'20054778',
'20054287',
'116816BJ',
'109436CQ',
'20053812',
'20053405',
'20054624',
'20054909',
'20054578',
'20054422',
'20054211',
'20051311',
'20051950' ) 
group by  months,supplier_code,supplier_name,goods_code,
    goods_name,classify_middle_code,
    classify_middle_name,unit_name
 ;
  