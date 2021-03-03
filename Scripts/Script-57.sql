
select reservoir_area_code ,reservoir_area_name ,max(id) from csx_b2b_accounting.accounting_stock_detail_view as asdv  where product_code ='1001270' and posting_time <='2019-09-30 23:57:27'
group by  reservoir_area_code ,reservoir_area_name ;
-- 1178670192234979329

select * from csx_b2b_accounting.accounting_stock_detail_view as asdv  where product_code ='1001270' and posting_time <='2019-09-30 23:57:27';

BZ01	默认标准区	1179604146752344065
BZ02	食百区	1179604146924310530
PD01	默认盘点区	1167831654266376194
TH01	默认退货区	1178670192209813506
TS01	默认途损区	1178574690277515266

select *from csx_b2b_sell.apply_order_item 