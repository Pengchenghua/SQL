select substr(sdt,1,6) mon,
province_code,province_name,
count(distinct case when a.source_type='10' then a.goods_code end )as self_sku,
count(distinct  a.goods_code  )as all_sku
from csx_dw.dws_scm_r_d_order_received a 
join 
(select province_code,province_name,shop_id from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and purpose in ('01','03')) b on a.target_location_code=b.shop_id
where super_class='1' and order_type='0'
and header_status!='5'
and sdt>='20210101'
group by province_code,province_name,substr(sdt,1,6);