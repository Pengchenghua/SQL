-- 省区  城市  一级  二级  三级分类    商品编码    商品品名    规格  是否工厂商品  商品状态
refresh csx_dw.dws_basic_w_a_csx_product_info;
select dist_code,dist_name,prefecture_city,
    prefecture_city_name,
    a.goods_code ,
    c.goods_name,
    standard ,
    classify_large_code,
    classify_large_name,
classify_middle_code ,
category_middle_name ,
c.category_large_code ,
c.category_large_name,
c.category_small_code ,
c.category_small_name ,
    des_specific_product_status,
    product_status_name,
    is_factory_goods_code
from 
(
-- 日配与福利订单
select province_code,dc_code ,goods_code ,is_factory_goods_code 
from csx_dw.dws_sale_r_d_customer_sale  
where sdt>='20200601' 
and attribute_code in (1,2)
and order_kind !='WELFARE'
and channel ='1'
group by province_code,dc_code,goods_code,is_factory_goods_code
) a 
join 
(
select
    shop_code ,
    product_code,
    des_specific_product_status,
    product_status_name
from
    csx_dw.dws_basic_w_a_csx_product_info
where
    sdt = 'current'
    and des_specific_product_status in ('0', '2', '7'))b 
    on a.dc_code=b.shop_code and a.goods_code=b.product_code
join 
(select goods_id ,goods_name ,standard ,classify_large_code,classify_large_name,
classify_middle_code ,
category_middle_name ,
category_large_code ,
category_large_name ,
category_small_code,
category_small_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_code=c.goods_id 
join 
(select location_code ,dist_code,dist_name,prefecture_city ,prefecture_city_name  
    from csx_dw.csx_shop 
    where sdt='current')d on a.dc_code=d.location_code
;


    and department_code = 'H03'
