-- 高库存处理跟进，处理业务
select * from csx_dw.dws_wms_r_d_batch_detail where 1=1;

select * from csx_dw.dws_wms_w_a_business_type
-- 103A 退货出库
-- 118A 领用出库
-- 120A 原料转成品
-- 115A 过帐-盘盈
-- 116A 过帐-盘亏
-- 118A 领用出库
-- 117A 报损出库
-- 110A 盘亏
-- 111A 盘盈



-- 期初库存取4.18
create temporary table csx_tmp.temp_stock_a as 
select a.province_code,a.province_name,
a.dc_code,
a.dc_name,
a.goods_id,
a.goods_name,
b.qty,
b.amt,
a.final_qty,
a.final_amt
from csx_tmp.ads_wms_r_d_goods_turnover a 
join  csx_tmp.fanruan b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where sdt='20220507'

;


drop table csx_tmp.temp_stack_a ;
-- 库存操作
create temporary table csx_tmp.temp_stack_a as 
select  a.dc_code,a.goods_code,
sum( case when a.move_type='103A' then amt_no_tax*(1+tax_rate/100) end) return_amt,
sum( case when a.move_type='103A' then a.qty end ) return_qty,
sum( case when a.move_type='117A' then amt_no_tax*(1+tax_rate/100) end) loss_amt,
sum( case when a.move_type='117A' then a.qty end) loss_qty,
sum( case when a.move_type='115A' then amt_no_tax*(1+tax_rate/100) end) shortage_amt,       --盘亏
sum( case when a.move_type='115A' then a.qty end) shortage_qty,
sum( case when a.move_type='110A' then amt_no_tax*(1+tax_rate/100) end) NO_shortage_amt,       --盘亏
sum( case when a.move_type='110A' then a.qty end) NO_shortage_qty,
sum( case when a.move_type='118A' then amt_no_tax*(1+tax_rate/100) end) receive_amt,       -- 领用
sum( case when a.move_type='118A' then a.qty end) receive_qty,
sum( case when a.move_type='104A' then amt_no_tax*(1+tax_rate/100) end) transfer_amt,       -- 调拨出库
sum( case when a.move_type='104A' then a.qty end) transfer_qty
from  csx_dw.dws_wms_r_d_batch_detail a 
join 
 csx_tmp.fanruan  b on a.dc_code=b.dc_code and a.goods_code=b.goods_code
WHERE 1=1 
    and a.in_or_out='1'
    and sdt>='20220419'
    -- and a.dc_code='W0A6'
  --  and a.goods_code='967518'
    and sdt<'20220508'
    and a.move_type in ('103A','117A','115A','110A','118A','104A')
group by a.dc_code,a.goods_code;




create temporary table csx_tmp.temp_sale as 
select a.dc_code,a.goods_code,
sum(case when sdt>='20220401' and sdt<='20220418' then sales_value end ) sales_1,
sum(case when sdt>='20220401' and sdt<='20220418' then sales_qty end ) sales_qty_1,
sum(case when sdt>='20220401' and sdt<='20220418' then profit end ) sales_profit_1,
sum(case when sdt>='20220419' and sdt<='20220508' then sales_value end ) sales_2,
sum(case when sdt>='20220419' and sdt<='20220508' then a.sales_qty end ) sales_qty_2,
sum(case when sdt>='20220419' and sdt<='20220508' then a.profit end ) sales_profit_2
from csx_dw.dws_sale_r_d_detail a 
join 
 csx_tmp.fanruan  b on a.dc_code=b.dc_code and a.goods_code=b.goods_code
WHERE 1=1
 and sdt>'20220401'
 and sdt<'20220508'
 group by 
 a.dc_code,a.goods_code

;

create temporary table  csx_tmp.temp_stock_aa as 
select  a.province_code,a.province_name,
a.dc_code,
a.dc_name,
a.goods_id,
a.goods_name,
qty,
amt,
a.final_qty,
a.final_amt,
return_qty,
return_amt,
loss_qty,
loss_amt,
shortage_qty,
shortage_amt,
NO_shortage_qty,
NO_shortage_amt,
receive_qty,
receive_amt,
transfer_qty,
transfer_amt,
sales_qty_1,
sales_1,
sales_profit_1,
sales_profit_1/sales_1 sales_profit_rate1,
sales_qty_2,
sales_2,
sales_profit_2,
sales_profit_2/sales_2 sales_profit_rate2
from csx_tmp.temp_stock_a a
left join
csx_tmp.temp_stack_a b on a.dc_code=b.dc_code and a.goods_id=b.goods_code 
left join 
csx_tmp.temp_sale c on a.dc_code=c.dc_code and a.goods_id=c.goods_code 
;

select  sum(amt),sum(final_amt)from  csx_tmp.temp_stock_a
;

select * from csx_tmp.temp_stock_aa