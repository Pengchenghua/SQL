-- 月末库存金额&销售成本--指定DC
-- 月末库存金额
select province_code,province_name,substr(a.sdt,1,6)mon,
a.classify_large_code ,
classify_large_name,
SUM(amt)/10000 amt,
SUM(amt_no_tax)/10000 amt_no_tax 
from csx_dw.dws_wms_r_d_accounting_stock_m  a 

join
(
select DISTINCT  regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd'))),'-','') sdt
from csx_dw.dws_basic_w_a_date  
where calday >'20200101'  and calday <'20210701'
) b on a.sdt=b.sdt
join 
(select shop_id,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m
where sdt='current' 
and shop_id in  ('9905','9906','9910','9951','9952','9955','9957','9958','9961','9963',
    '9964','9966','9967','9968','9969','9973','9975','9976','9978','9981','9983','9991',
    '9992','9993','9994','9995','9996','99A0','99A1','99A2','99A3','99A4','99A5','99A6',
    '99A7','99A8','99A9','99B1','99B2','99B3','99B4','W048','W053','W079','W080','W081',
    'W0A2','W0A3','W0A4','W0A6','W0A8','W0B1','W0B6','W0C1','W0C2','W0D4','W0E6','W0E7',
    'W0E8','W0E9','W0G7','W0G8','W0H2','W0H3','W0H6','W0H7','W0J2','W0J5','W0J6','W0J9',
    'W0K0','W0K4','W0K7','W0K8','W0L4','W0M4','W0M5','W0M6','E098','E0M3','E109','E131',
    'E155','E156','E157','E167','E178','E202','E218','E220','E221','E222','E227','E230',
    'E245','E268','E295','E312','E322','E378','E383','E391','E397','E398','E399','E403',
    'E463','E478','E484','E513','E539','E540','E541','E545','E546','E653','E693','E827',
    'E841','E898','EI16','ENG3','E715','E0R7','99B6','W0R1','99C2','W0R0','W0T5','E407',
    'E793','W0V3','W0Z7','W0AD','E007','E009','E010','E011','E012','E013','E014','E015',
    'E017','E018','E019','E020','E023','E024','E025','E026','E029','E030','E033','E038',
    'E039','E045','E046','E047','E048','E049','E050','E058','E063','E064','E067','E068',
    'E074','E078','E079','E0D6','E0D8','E0K5','E0L1','E0M0','E0S7','E0S8','E0T4','E0U7',
    'E0V3','E105','E113','E116','E120','E124','E129','E134','E136','E139','E141','E144',
    'E163','E165','E166','E172','E192','E199','E225','E250','E252','E253','E277','E285',
    'E296','E330','E344','E350','E387','E417','E434','E438','E439','E440','E448','E459',
    'E464','E496','E502','E531','E549','E557','E559','E598','E5C8','E5EM','E629','E630',
    'E660','E700','E712','E839','E845','E848','E872','E893','E053','E127','E194','E224',
    'E599','E5DJ','W0AW','W0AK','W0BY','WA93')
) c on a.dc_code=c.shop_id
where reservoir_area_code  not in ('PD01','PD02','TS01')
group by province_code,province_name, substr(a.sdt,1,6),a.classify_large_code ,classify_large_name
 ;

 
select * from csx_dw.ads_supply_order_flow  where order_code ='POW0A8200916003422';



-- 每月销售成本
select a.dc_province_code,a.dc_province_name, substr(a.sdt,1,6)mon,a.classify_large_code ,classify_large_name,
SUM(excluding_tax_cost)/10000 no_tax_amt,
sum(sales_cost)/10000 cost_amt
from csx_dw.dws_sale_r_d_detail  a 
where business_type_code !='4'
and sdt >='20200101'  and sdt <'20210701'
 and dc_code  in ('9905','9906','9910','9951','9952','9955','9957','9958','9961','9963',
    '9964','9966','9967','9968','9969','9973','9975','9976','9978','9981','9983','9991',
    '9992','9993','9994','9995','9996','99A0','99A1','99A2','99A3','99A4','99A5','99A6',
    '99A7','99A8','99A9','99B1','99B2','99B3','99B4','W048','W053','W079','W080','W081',
    'W0A2','W0A3','W0A4','W0A6','W0A8','W0B1','W0B6','W0C1','W0C2','W0D4','W0E6','W0E7',
    'W0E8','W0E9','W0G7','W0G8','W0H2','W0H3','W0H6','W0H7','W0J2','W0J5','W0J6','W0J9',
    'W0K0','W0K4','W0K7','W0K8','W0L4','W0M4','W0M5','W0M6','E098','E0M3','E109','E131',
    'E155','E156','E157','E167','E178','E202','E218','E220','E221','E222','E227','E230',
    'E245','E268','E295','E312','E322','E378','E383','E391','E397','E398','E399','E403',
    'E463','E478','E484','E513','E539','E540','E541','E545','E546','E653','E693','E827',
    'E841','E898','EI16','ENG3','E715','E0R7','99B6','W0R1','99C2','W0R0','W0T5','E407',
    'E793','W0V3','W0Z7','W0AD','E007','E009','E010','E011','E012','E013','E014','E015',
    'E017','E018','E019','E020','E023','E024','E025','E026','E029','E030','E033','E038',
    'E039','E045','E046','E047','E048','E049','E050','E058','E063','E064','E067','E068',
    'E074','E078','E079','E0D6','E0D8','E0K5','E0L1','E0M0','E0S7','E0S8','E0T4','E0U7',
    'E0V3','E105','E113','E116','E120','E124','E129','E134','E136','E139','E141','E144',
    'E163','E165','E166','E172','E192','E199','E225','E250','E252','E253','E277','E285',
    'E296','E330','E344','E350','E387','E417','E434','E438','E439','E440','E448','E459',
    'E464','E496','E502','E531','E549','E557','E559','E598','E5C8','E5EM','E629','E630',
    'E660','E700','E712','E839','E845','E848','E872','E893','E053','E127','E194','E224',
    'E599','E5DJ','W0AW','W0AK','W0BY','WA93')
group by a.dc_province_code,a.dc_province_name,substr(a.sdt,1,6),a.classify_large_code ,classify_large_name
 ;
 