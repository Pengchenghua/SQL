--异常库存占比&高库存&滞销&动销率
select b.province_code,
    b.province_name,
    a.dc_code,
    a.dc_name,
    a.goods_id,
    a.goods_name,
    a.dept_id,
    a.dept_name,
    purpose_name,
    sum(a.final_amt) all_amt,
    sum(case when  a.days_turnover_30>45 and a.final_amt>5000 then a.final_amt end ) as hight_amt,
    count(distinct case when a.period_inv_qty_30day!=0 then a.goods_id end ) all_sku,
    count(distinct case when a.cost_30day!=0 then a.goods_id end ) goods_pin_sku,
    sum(case when a.division_code='12' and a.no_sale_days>30 and a.entry_days/qualitative_period >0.5 then a.final_amt 
        when  a.division_code='13' and a.no_sale_days>60 and a.entry_days/qualitative_period >0.5 then a.final_amt end ) dead_goods_amt --滞销品
    
 from csx_tmp.ads_wms_r_d_goods_turnover  a 
join
(select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市' 
    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    ) b on a.dc_code=b.shop_id
join
(select goods_id,qualitative_period from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_id=c.goods_id
where sdt='20210930'
and a.dept_id like 'A%'
and purchase_org!='P620'
and sales_region_code!='9'
and purpose in ('01','07')
group by b.province_code,
    b.province_name,
    a.dc_code,
    a.dc_name,
    a.goods_id,
    a.goods_name,
    a.dept_id,
    a.dept_name,
    purpose_name
;

show create table  csx_dw.dws_basic_w_a_csx_product_m ;



--临保商品
select b.province_code,
    b.province_name,
    a.dc_code,
    a.dc_name,
    a.goods_id,
    a.goods_name,
    a.dept_id,
    a.dept_name,
    purpose_name,
    qualitative_period,
    a.entry_sdt,
    a.entry_days,
    (a.final_amt) all_amt,
    (case when  a.days_turnover_30>45 and a.final_amt>5000 then a.final_amt end ) as hight_amt,
     case when a.period_inv_qty_30day!=0 then 1 end  all_sku,
    case when a.cost_30day!=0 then 1 end  goods_pin_sku,
    (case when a.division_code='12' and a.no_sale_days>30 and a.entry_days/qualitative_period >0.5 then a.final_amt 
        when  a.division_code='13' and a.no_sale_days>60 and a.entry_days/qualitative_period >0.5 then a.final_amt end ) dead_goods_amt --滞销品
    
 from csx_tmp.ads_wms_r_d_goods_turnover  a 
join
(select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市' 
    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    ) b on a.dc_code=b.shop_id
join
(select goods_id,qualitative_period from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_id=c.goods_id
where sdt='20210930'
and a.dept_id like 'A%'
and purchase_org!='P620'
and sales_region_code!='9'
and purpose in ('01','07')
and (a.period_inv_amt_30day!=0 or a.cost_30day!=0)
-- group by b.province_code,
--     b.province_name,
--     a.dc_code,
--     a.dc_name,
--     a.goods_id,
--     a.goods_name,
--     a.dept_id,
--     a.dept_name,
--     purpose_name
;

show create table  csx_dw.dws_basic_w_a_csx_product_m ;