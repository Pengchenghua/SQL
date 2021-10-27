-- 财务数据需求融资库存成本20211027
--剔除（'W0G1','W0H4','W0H1','W0S1','W0AQ'）及合伙人仓、小店、寄售小店

select  substr(a.sdt,1,6)mon,
a.classify_large_code ,
classify_large_name,
SUM(amt)/10000 amt,
SUM(amt_no_tax)/10000 amt_no_tax 
from csx_dw.dws_wms_r_d_accounting_stock_m  a 
join
(
select DISTINCT  regexp_replace(to_date(last_day(from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'yyyy-MM-dd'))),'-','') sdt
from csx_dw.dws_basic_w_a_date  
where calday >='20210601'  and calday <'20211001'
) b on a.sdt=b.sdt
join 
(select shop_id,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m
where sdt='current' 
AND purpose IN ('01','02','03','08','07')
and shop_id not in ('W0G1','W0H4','W0H1','W0S1','W0AQ')
) c on a.dc_code=c.shop_id
where reservoir_area_code  not in ('PD01','PD02','TS01')
group by   substr(a.sdt,1,6),a.classify_large_code ,classify_large_name 
 ;

--定价成本
--剔除城市服务商及DC仓（'W0G1','W0H4','W0H1','W0S1','W0AQ'）
select  
substr(a.sdt,1,6)mon,
a.classify_large_code ,
classify_large_name, 
SUM(excluding_tax_cost)/10000 no_tax_amt,
sum(sales_cost)/10000 cost_amt
from csx_dw.dws_sale_r_d_detail  a 
where business_type_code !='4'
and sdt >='20210601'  and sdt <'20211001'
and a.dc_code not in ('W0G1','W0H4','W0H1','W0S1','W0AQ')
group by substr(a.sdt,1,6),
a.classify_large_code ,
classify_large_name
;