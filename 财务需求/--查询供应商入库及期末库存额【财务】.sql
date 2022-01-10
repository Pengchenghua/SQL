--查询供应商入库及期末库存额【财务】

drop table  csx_tmp.temp_01;
create temporary table csx_tmp.temp_01 as 
select sdt,receive_location_code,receive_location_name,order_code,batch_code,goods_code,goods_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
supplier_code,
supplier_name,
receive_qty,
amount
from csx_dw.dws_wms_r_d_entry_batch where supplier_code in('20045813','20047386',
'20043536',
'20052361',
'20049806',
'20034399',
'20052380'
)
and sdt>='20210101'
and receive_status='2'
;

create temporary table csx_tmp.temp_02 as 
select  receive_location_code,a.goods_code,qty,amt from 
(select distinct receive_location_code,goods_code from  csx_tmp.temp_01 a ) a 

join 
(select dc_code,goods_code,sum(qty)qty,sum(amt)amt from csx_dw.dws_wms_r_d_accounting_stock_m where sdt='20211228' 
and reservoir_area_code not in ('PD01','PD02','TS01')
group by dc_code,goods_code) b ON A.receive_location_code=b.dc_code and a.goods_code=b.goods_code
;

select a.*,b.qty,b.amt from 
(select  receive_location_code,receive_location_name,goods_code,goods_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
supplier_code,
supplier_name,
sum(receive_qty) receive_qty,
sum(a.amount) amount
from  csx_tmp.temp_01 a
group by receive_location_code,receive_location_name,goods_code,goods_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
supplier_code,
supplier_name) a 
left join 
csx_tmp.temp_02  b on a.receive_location_code=b.receive_location_code and a.goods_code=b.goods_code