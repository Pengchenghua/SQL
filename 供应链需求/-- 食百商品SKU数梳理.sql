-- 食百商品SKU数梳理
set shopid=('W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0P8','W0Q9','W0A5','W0R9','W0AS','W0N0','W0W7','W0F4','W0A8','W0J2','W0K6','W0L3','W0AH','W0K1','WA96','W0BK','W0A2','W0BR','W0BH');
set sdate='20211201';
set edate='20220531';
-- 销售基础

drop table csx_tmp.temp_sale_t1;
create temporary table csx_tmp.temp_sale_t1 as 
select 
        dc_code,
        goods_code,
        sum(sales_qty) sales_qty,
        sum(sales_value) sales_value,
        sum(profit) profit,
        sum(case when sdt>='20220301' and sdt<'20220601' then sales_qty end )   sales_qty_03,
        sum(case when sdt>='20220301' and sdt<'20220601' then sales_value end ) sales_value_03,
        sum(case when sdt>='20220501' and sdt<'20220601' then sales_qty end )   sales_qty_04,
        sum(case when sdt>='20220501' and sdt<'20220601' then sales_value end ) sales_value_04
from csx_dw.dws_sale_r_d_detail
where sdt >= ${hiveconf:sdate}
    and sdt<= ${hiveconf:edate}
    and dc_code in ${hiveconf:shopid}
   -- and sales_type !='fanli'
   -- and business_type_code='1'
   -- and channel_code in ('1','7','9')
   -- and province_name='安徽省'
    group by  
        dc_code,
        goods_code
;
-- DC编码	DC名称	部类	管理一级分类编码	管理一级分类名称	管理二级分类编码	管理二级分类名称	管理三级分类编码	管理三级分类名称	商品编码	商品条码	商品名称	品牌	单位	商品状态	退货标识	库存属性

drop table csx_tmp.temp_goods_01;
create temporary table csx_tmp.temp_goods_01 as 
select 
    sales_region_code,
    sales_region_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
    shop_code,
    c.shop_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    product_code,
    bar_code,
    goods_name,
    b.brand_name,
    unit_name,
    product_status_name,
    sales_return_tag,
    stock_properties,
	stock_properties_name
from  csx_dw.dws_basic_w_a_csx_product_info a 
join
(SELECT goods_id,
bar_code,
       goods_name,
       unit_name,
       brand_name,
       division_code,
       division_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') b on a.product_code=b.goods_id
join 
(select sales_province_code,
    sales_province_name,
    purchase_org,
    purchase_org_name,
    case when performance_province_name like'平台%' then '00' else   sales_region_code end  sales_region_code,
    case when performance_province_name like'平台%' then '平台' else  sales_region_name end sales_region_name,
    shop_id ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    ) c on a.shop_code=c.shop_id
where sdt=${hiveconf:edate} 
  --  and des_specific_product_status='0'
    and shop_code in  ${hiveconf:shopid}
;

create temporary table csx_tmp.temp_goods_02 as 
select dc_code,
    goods_code,
    sum(qty) qty,
    sum(amt) amt,
    sum(amt_no_tax) amt_no_tax
from csx_dw.dws_wms_r_d_accounting_stock_m
where sdt=${hiveconf:edate} 
and dc_code in  ${hiveconf:shopid}
and reservoir_area_code not in ('PD01','PD02','TS01','CY01')
group by dc_code,
        goods_code
;


 
select 
    sales_region_code,
    sales_region_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
    shop_code,
    shop_name,
    division_code,
    division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    product_code,
    bar_code,
    goods_name,
    brand_name,
    unit_name,
    product_status_name,
    case when sales_return_tag=1 then '是' else '否' end sales_return_tag,
    stock_properties,
	stock_properties_name,
    coalesce(sales_qty,0) sales_qty,
    coalesce(sales_value,0) sales_value,
    coalesce(profit,0) profit,
    coalesce(sales_qty_03,0) sales_qty_03,
    coalesce(sales_value_03,0) sales_value_03,
    coalesce(sales_qty_04,0) sales_qty_04,
    coalesce(sales_value_04,0) sales_value_04,
    coalesce(qty,0) qty,
    coalesce(amt,0) amt
from csx_tmp.temp_goods_01 as a
left join  csx_tmp.temp_sale_t1 b on a.shop_code=b.dc_code and a.product_code=b.goods_code
left join   csx_tmp.temp_goods_02 c on a.shop_code=c.dc_code and a.product_code=c.goods_code
where a.division_code in ('12','13','14','15')
;