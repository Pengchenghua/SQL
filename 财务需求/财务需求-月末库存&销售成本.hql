
-- 月末库存金额
select substr(a.sdt,1,6)mon,
a.classify_large_code ,
classify_large_name,
SUM(amt)/10000 amt,
SUM(amt_no_tax)/10000 amt_no_tax 
from csx_dw.dws_wms_r_d_accounting_stock_m  a 
join 
(select DISTINCT shop_id, purpose_name,purpose
from csx_dw.dws_basic_w_a_csx_shop_m 
where sdt='current' and purpose in ('01','02','03','07','08')) c on a.dc_code =c.shop_id
join
(
select DISTINCT  regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd'))),'-','') sdt
from csx_dw.dws_basic_w_a_date  
where calday >'20200101'  and calday <'20210701'
) b on a.sdt=b.sdt
where reservoir_area_code  not in ('PD01','PD02','TS01')
and dc_code not in ('W0G1','W0H4','W0H1','W0S1','W0AQ')
group by substr(a.sdt,1,6),a.classify_large_code ,classify_large_name
 ;
 
select * from csx_dw.ads_supply_order_flow  where order_code ='POW0A8200916003422';



-- 每月销售成本
select substr(a.sdt,1,6)mon,a.classify_large_code ,classify_large_name,
SUM(excluding_tax_cost)/10000 no_tax_amt,
sum(sales_cost)/10000 cost_amt
from csx_dw.dws_sale_r_d_detail  a 
where business_type_code !='4'
and sdt >'20200101'  and sdt <'20210701'
 and dc_code not in ('W0G1','W0H4','W0H1','W0S1','W0AQ')
group by substr(a.sdt,1,6),a.classify_large_code ,classify_large_name
 ;
 