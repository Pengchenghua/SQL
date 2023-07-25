--模糊查找品名对应销售明细

select month,performance_region_name,performance_province_name,performance_city_name,
    business_type_name, a.goods_code,goods_bar_code,goods_name,unit_name,brand_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
    (sale_qty)sale_qty,(sale_amt)sale_amt ,(profit) profit
    from 

 ( select substr(sdt,1,6) month,performance_region_name,performance_province_name,performance_city_name,
    business_type_name, goods_code,
    sum(sale_qty)sale_qty,sum(sale_amt)sale_amt ,sum(profit) profit
    from      csx_dws.csx_dws_sale_detail_di
    where sdt >='20220101' and sdt <='20230630'
    group by performance_region_name,performance_province_name,performance_city_name,
    business_type_name, goods_code,substr(sdt,1,6) 
  )    a
 join       
(select goods_code,goods_bar_code,goods_name,unit_name,brand_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
from csx_dim.csx_dim_basic_goods
where sdt='current' 
and  goods_name rlike '大烹|德兰|东海明珠|福临门|福掌柜|福之泉|皇家粮仓|皇中皇|家佳康|酒鬼|孔乙己|梅林|蒙牛|内参|四海|天坛|屯河|万威客|五湖|香雪|湘泉|悦活|长城|中茶|中粮|中糖|珠江桥|仁怀'
) b on a.goods_code=b.goods_code