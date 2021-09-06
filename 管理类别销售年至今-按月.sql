select channel_name ,business_type_name ,substr(sdt,1,6) mon,
b.classify_large_code ,
b.classify_large_name ,
b.classify_middle_code,
b.classify_middle_name,
b.classify_small_code ,
b.classify_small_name ,
SUM(sales_value)/10000 as sales_value,
SUM(profit)/10000 as profit,
SUM(profit)/SUM(sales_value) profit_rate
from csx_dw.dws_sale_r_d_detail  a 
left join 
(select m.goods_id,
	classify_large_code ,
classify_large_name ,
classify_middle_code,
classify_middle_name,
classify_small_code ,
classify_small_name 
from csx_dw.dws_basic_w_a_csx_product_m m where sdt='current') b on a.goods_code =b.goods_id
where sdt>='20210101' and sdt<='20210731' 
and channel_code in ('1','7','9') 
-- and business_type_code !='4' 
group by b.classify_large_code ,
b.classify_large_name ,
b.classify_middle_code,
b.classify_middle_name,
b.classify_small_code ,
b.classify_small_name ,
 substr(sdt,1,6),
 channel_name ,
 business_type_name;