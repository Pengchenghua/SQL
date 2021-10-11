--指定分类进价趋势
-- 冻禽类：B030407、B030408、B030409、B030410；
-- 冻牛肉类：B030403
-- 白糖：266
-- 元宝大豆油系列：1345304、566012、1345303、1094161……如有遗漏编码，也帮忙算进去
-- 2020~2021年，月度价格趋势
select mon,
    goods_code,
    goods_name,
    unit_name,
    standard,
    brand_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(amt) amt,
    sum(qty) qty,
    sum(amt)/sum(qty) as cost
from csx_tmp.temp_order_entry a 
join 
(select goods_id,unit_name,standard from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' ) b on a.goods_code=b.goods_id
where 
(classify_small_code in ('B030407','B030408','B030409','B030410','B030403')
or goods_code ='266' or goods_name like'%元宝大豆%' )
and mon between '202001' and '202109'
group by mon,
    goods_code,
    goods_name,
    brand_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,unit_name,
    standard
;