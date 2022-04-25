DROP TABLE  csx_tmp.temp_aa;
create temporary table csx_tmp.temp_aa
as 
select a.province_code,
a.province_name,
goods_code,
goods_name,category_large_code,category_large_name,
qty,
amt,
row_number()over(partition by province_code,category_large_code order by amt desc) as rank_a
from 
(select a.province_code,
a.province_name,
goods_code,
c.goods_name,c.category_large_code,c.category_large_name,
sum(receive_qty) as qty,
sum(price*receive_qty) as amt
from csx_dw.dws_wms_r_d_entry_detail  a 
join 
(select shop_id,shop_name,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.receive_location_code=b.shop_id
join
(select goods_id,goods_name,category_large_code,category_large_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_code=c.goods_id
where sdt>='20210509' and sdt<='20210523' and c.category_large_code in ('1103','1105','1250','1257')
and receive_location_code in ('W0A8','W053','W0F4','W0K1','W0F7','W0K6','W0G5','W0L3','W0G9','W0AH','W039','W0A7','W0Z9','W0X2','W048','W0A3','W080','W0A2','W079','W0A6','W088','W0A5','W0R8','W0R9','W0N0','W0P3','W0W8','W0W7','W0K3','W0N1','W0AR','W0AS','W0Q4','W0P6','W0P8','W0Q2','W0Q8','W0Q9')
and a.order_type_code like 'P%'
and a.receive_status=2
group by a.province_code,
a.province_name,
goods_code,
c.goods_name,c.category_large_code,c.category_large_name
)a

;

select a.province_code,a.province_name,a.receive_location_code,a.receive_location_name,a.goods_code,a.goods_name,a.department_code,a.department_name,a.category_middle_code,a.category_middle_name,sdt,
sum(a.receive_qty)qty,sum(a.receive_qty*a.price) as amt from csx_dw.dws_wms_r_d_entry_detail a 
join 
csx_tmp.temp_aa b on a.goods_code=b.goods_code and  a.province_code=b.province_code
where b.rank_a<21
and receive_location_code in ('W0A8','W053','W0F4','W0K1','W0F7','W0K6','W0G5','W0L3','W0G9','W0AH','W039','W0A7','W0Z9','W0X2','W048','W0A3','W080','W0A2','W079','W0A6','W088','W0A5','W0R8','W0R9','W0N0','W0P3','W0W8','W0W7','W0K3','W0N1','W0AR','W0AS','W0Q4','W0P6','W0P8','W0Q2','W0Q8','W0Q9')
and  a.sdt>='20210509' and sdt<='20210523' 
and a.receive_status=2
and a.order_type_code like 'P%'
group by a.province_code,a.province_name,a.receive_location_code,a.receive_location_name,a.goods_code,a.goods_name,a.department_code,a.department_name,a.category_middle_code,a.category_middle_name,sdt
;

select * from csx_tmp.temp_aa where rank_a<21;