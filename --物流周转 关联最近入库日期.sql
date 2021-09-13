--物流周转 关联最近入库日期
create table csx_tmp.temp_turn_01 as 
select a.*,b.sdt as entry_max_sdt,receive_amt mc_entry_amt from csx_tmp.ads_wms_r_d_goods_turnover a 
left join 
(
select receive_location_code as dc_code,
    a.goods_code,
    sdt,
    sum(a.receive_qty) qty ,
    sum(price*a.receive_qty) receive_amt
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select receive_location_code as dc_code,
    goods_code,
    max(sdt) max_sdt
from csx_dw.dws_wms_r_d_entry_detail 
where sdt>='20190101' 
group by receive_location_code,
    goods_code
) b on a.receive_location_code =b.dc_code and a.goods_code=b.goods_code and a.sdt=b.max_sdt
group by receive_location_code, a.goods_code,sdt
) b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where a.sdt='20210912'
;


select * from csx_tmp.temp_turn_01
;