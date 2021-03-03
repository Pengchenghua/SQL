
--- 管理分类冻品库存数据
select
    dist_code,
    dist_name,
    a.dc_code,
    a.dc_name,
    a.goods_code,
    d.goods_name,
    classify_middle_code,
    classify_middle_name,
    sum(a.qty)qty,
    sum(a.amt)amt
from
    csx_dw.dws_wms_r_d_accounting_stock_m a
join (
    select
        classify_middle_code, classify_middle_name, category_small_code
    from
        csx_dw.dws_basic_w_a_manage_classify_m
    where
        sdt = 'current'
        and classify_middle_code = 'B0304' )b on
    a.category_small_code = b.category_small_code
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current')c on a.dc_code =c.location_code
JOIN (select goods_id ,goods_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') d on a.goods_code =d.goods_id
where
    sdt = '20200927'
    and a.reservoir_area_code not in ('PD01',
    'PD02',
    'TS01')
    and qty!=0
GROUP BY
    a.dc_code,
    a.dc_name,
    a.goods_code,
    d.goods_name,
    classify_middle_code,
    classify_middle_name,
    dist_code,
    dist_name;
