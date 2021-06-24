select a.province_code,
    a.province_name,
    inv_amt,
    cost,
    sale,
    trunc_day,
    all_sku,
    sale_sku,
    all_sale,
    sale/all_sale as sale_ratio
    from 
(select b.province_code,
    b.province_name,
    sum(inv_amt) inv_amt,
    sum(cost) cost,
    sum(sale) sale,
    sum(inv_amt)/sum(cost) as trunc_day,
    count(distinct b.product_code) as all_sku,
    count(distinct case when a.sale>0 then goods_id end  )as sale_sku
    from 
(select province_code,province_name,shop_code,stock_properties,product_code,stock_properties_name from csx_dw.dws_basic_w_a_csx_product_info a 
    join 
    (select province_code,province_name,shop_id from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.shop_code=b.shop_id
    where sdt='current' 
    and stock_properties='1'
    and a.root_category_code in ('12','13','14','15')
    and  shop_code in ('W0A8','W0A7','W0A3','W0A2','W0A6','W0A5','W0R9','W0N0','W0W7','W0N1','W0AS','W0P8','W0Q2','W0Q9')
    ) b 
left join 
(
select a.province_code,
    a.province_name,
    a.dc_code,
    a.goods_id,
    sum(a.period_inv_amt_30day) inv_amt,
    sum(a.cost_30day) cost,
    sum(a.sales_30day) sale
from csx_tmp.ads_wms_r_d_goods_turnover a 
where sdt='20210601'
    and a.business_division_code='12'
group by a.province_code,
    a.province_name,
    a.dc_code,
    a.goods_id
    )a 
on a.dc_code=b.shop_code and a.goods_id=b.product_code
group by  b.province_code,
    b.province_name
)a
left join 
(select province_name,sum(sales_value) all_sale from csx_dw.dws_sale_r_d_detail where sdt>='20210601' and sdt<='20210503' group by province_name ) c on a.province_name=c.province_name
;



-- 干货
-- 干货
select a.province_code,
    a.province_name,
    inv_amt,
    cost,
    sale,
    trunc_day,
    all_sku,
    sale_sku,
    all_sale,
    sale_sku/all_sku as pin_rate,
    sale/all_sale as sale_ratio
    from 
(select b.province_code,
    b.province_name,
    sum(inv_amt) inv_amt,
    sum(cost) cost,
    sum(sale) sale,
    sum(inv_amt)/sum(cost) as trunc_day,
    count(distinct b.product_code) as all_sku,
    count(distinct case when a.sale>0 then goods_id end  )as sale_sku
    from 
(select province_code,province_name,shop_code,stock_properties,product_code,stock_properties_name from csx_dw.dws_basic_w_a_csx_product_info a 
    join 
    (select province_code,province_name,shop_id from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.shop_code=b.shop_id
    where sdt='current' 
    and stock_properties='1'
    and a.big_category_code in ('1101')
    and  shop_code in ('W0A8','W0A7','W0A3','W0A2','W0A6','W0A5','W0R9','W0N0','W0W7','W0N1','W0AS','W0P8','W0Q2','W0Q9')
    ) b 
left join 
(
select a.province_code,
    a.province_name,
    a.dc_code,
    a.goods_id,
    sum(a.period_inv_amt_30day) inv_amt,
    sum(a.cost_30day) cost,
    sum(a.sales_30day) sale
from csx_tmp.ads_wms_r_d_goods_turnover a 
where sdt='20210603'
    and a.category_large_code='1101'
group by a.province_code,
    a.province_name,
    a.dc_code,
    a.goods_id
    )a 
on a.dc_code=b.shop_code and a.goods_id=b.product_code
group by  b.province_code,
    b.province_name
)a
left join 
(select province_name,sum(sales_value) all_sale from csx_dw.dws_sale_r_d_detail where sdt>='20210505' and sdt<='20210603' and category_large_code='1101' group by province_name ) c on a.province_name=c.province_name
;