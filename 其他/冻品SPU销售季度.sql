
-- 冻品SPU销售季度
select a.province_code,a.province_name,
concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) mon,
dc_code,
sum(a.sales_value) sale,
sum(a.profit) profit,
sum(case when spu_goods_code is null then a.sales_value end) sale_spu,
sum(case when spu_goods_code is null then a.profit end) profit_spu
from csx_dw.dws_sale_r_d_detail a 
join 
(select goods_id,spu_goods_code,spu_goods_name 
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt='current' 
-- and spu_goods_code is not null
and classify_middle_code in ('B0304')
)b on a.goods_code=b.goods_id
where sdt>='20200101' and sdt<'20211001'
 and province_name in ( '四川省', '安徽省','福建省')
 and a.channel_code in ('1','7','9')
 and a.business_type_code!='4'
 group by a.province_code,a.province_name,
concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) ,
dc_code
