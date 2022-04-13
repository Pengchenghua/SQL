-- 财务库存成本&定价成本 计算周转
-- 月末库存金额&销售成本--指定DC
-- 月末库存金额-- 月末库存金额&销售成本--指定DC
-- 月末库存金额
select province_code,province_name,substr(a.sdt,1,6)mon,
a.classify_large_code ,
classify_large_name,
classify_middle_code,
classify_middle_name,
SUM(amt_no_tax*(1+a.tax_rate/100))/10000 amt,
SUM(amt_no_tax)/10000 amt_no_tax 
from csx_dw.dws_wms_r_d_accounting_stock_m  a 
join
(
select DISTINCT  regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd'))),'-','') sdt
from csx_dw.dws_basic_w_a_date  
where calday >='20220101'  and calday <'20220401'
) b on a.sdt=b.sdt
join 
(select shop_id,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m
where sdt='current' 
and purpose IN (  '01',
                  '02',
                  '03',
                  '08',
                  '07'
                -- '06', -- 合伙人仓
                --  '05' --彩食鲜小店
                --  '04' --寄售小店
                  )
and shop_id not in('W0G1','W0H4','W0H1','W0S1','W0AQ')
) c on a.dc_code=c.shop_id
where reservoir_area_code  not in ('PD01','PD02','TS01')
group by province_code,province_name, substr(a.sdt,1,6),a.classify_large_code ,classify_large_name,
classify_middle_code,
classify_middle_name
 ;

 
-- select * from csx_dw.ads_supply_order_flow  where order_code ='POW0A8200916003422';



-- 每月销售成本
select a.dc_province_code,a.dc_province_name, 
substr(a.sdt,1,6)mon,
a.classify_large_code ,
classify_large_name,
classify_middle_code,
classify_middle_name,
SUM(excluding_tax_cost)/10000 no_tax_amt,
sum(sales_cost)/10000 cost_amt
from csx_dw.dws_sale_r_d_detail  a 
where business_type_code !='4'
and sdt >='20210701'  and sdt <'20211001'
 and a.dc_code not in ('W0G1','W0H4','W0H1','W0S1','W0AQ')

group by a.dc_province_code,a.dc_province_name,substr(a.sdt,1,6),a.classify_large_code ,classify_large_name,classify_middle_code,
classify_middle_name
 ;
 


 -- 重庆DC
select province_code,province_name,substr(a.sdt,1,6)mon,
a.classify_large_code ,
classify_large_name,
classify_middle_code,
classify_middle_name,
SUM(amt)/10000 amt,
SUM(amt_no_tax)/10000 amt_no_tax 
from csx_dw.dws_wms_r_d_accounting_stock_m  a 
join
(
select DISTINCT  regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd'))),'-','') sdt
from csx_dw.dws_basic_w_a_date  
where calday >='20200101'  and calday <'20211001'
) b on a.sdt=b.sdt
join 
(select shop_id,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m
where sdt='current' 
and shop_id in  ('9956','9965','W039','W0A7','W0C3','W0M1','W0M9','E080','E081','E084','E085','E088','E090',
'E092','E093','E097','E099','E0A1','E0A2','E0B8','E0D4','E0N6','E100','E103','E104','E118','E119','E137','E147',
'E148','E159','E162','E168','E170','E171','E176','E177','E185','E186','E191','E290','E308','E316','E360','E369',
'E384','E404','E447','E451','E467','E474','E501','E575','E576','E577','E655','E656','E689','E690','E708','E721',
'E812','E861','E866','E892','E867','E257','E486','W0S5','W0T7','W0X0','W0X1','W0X2','W0X3','W0Z9','W0AB','W0Z8',
'E209','W0AZ','E247','E368')
) c on a.dc_code=c.shop_id
where reservoir_area_code  not in ('PD01','PD02','TS01')
group by province_code,province_name, substr(a.sdt,1,6),a.classify_large_code ,classify_large_name,
classify_middle_code,
classify_middle_name
 ;

 
-- select * from csx_dw.ads_supply_order_flow  where order_code ='POW0A8200916003422';



-- 每月销售成本 &不含城市服务商
select a.dc_province_code,a.dc_province_name, 
substr(a.sdt,1,6)mon,
a.classify_large_code ,
classify_large_name,
classify_middle_code,
classify_middle_name,
SUM(excluding_tax_cost)/10000 no_tax_amt,
sum(sales_cost)/10000 cost_amt
from csx_dw.dws_sale_r_d_detail  a 
where business_type_code !='4'
and sdt >='20200101'  and sdt <'20211001'
 and dc_code  in ('9956','9965','W039','W0A7','W0C3','W0M1','W0M9','E080','E081',
 'E084','E085','E088','E090','E092','E093','E097','E099','E0A1','E0A2','E0B8','E0D4',
 'E0N6','E100','E103','E104','E118','E119','E137','E147','E148','E159','E162','E168',
 'E170','E171','E176','E177','E185','E186','E191','E290','E308','E316','E360','E369',
 'E384','E404','E447','E451','E467','E474','E501','E575','E576','E577','E655','E656',
 'E689','E690','E708','E721','E812','E861','E866','E892','E867','E257','E486','W0S5',
 'W0T7','W0X0','W0X1','W0X2','W0X3','W0Z9','W0AB','W0Z8','E209','W0AZ','E247','E368')
group by a.dc_province_code,a.dc_province_name,substr(a.sdt,1,6),a.classify_large_code ,classify_large_name,classify_middle_code,
classify_middle_name
 ;
 