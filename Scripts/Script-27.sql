
select
    a.dc_code,
    a.dc_name,
    a.goods_code,
    a.goods_name,
    category_small_code,
    category_small_name,
--    classify_middle_code,
--    classify_middle_name,
    sum(a.qty)qty,
    sum(a.amt)amt
from
    csx_dw.dws_wms_r_d_accounting_stock_m a
where
    sdt = '20200927'
    and a.reservoir_area_code not in ('PD01','PD02','TS01')
 --   and category_small_code in ('11040601','11040602','11040603','11040604','11040605','11040606','11040607','11040608')
    and category_large_code ='1104'
    and dc_code ='W0A8'
GROUP BY
    a.dc_code,
    a.dc_name,
    a.goods_code,
    a.goods_name,
category_small_code,
    category_small_name;

SELECT * FROM csx_tmp.ads_fr_account_receivables WHERE sdt='20200928' and province_code='-1';

select *　from csx_dw.dws_crm_r_d_customer_2
select * from csx_dw.dws_basic_w_a_csx_product_m  where sdt='current' 
and category_small_code in ('11040601','11040602','11040603','11040604','11040605','11040606','11040607','11040608');


select
    channel_name as sflag ,
    hkont ,
    account_name ,
    comp_code ,
    comp_name ,
    province_code,
    province_name as dist ,
    sales_city ,
   customer_no as kunnr ,
   customer_name as  name ,
    first_category ,
    second_category ,
    third_category ,
    work_no ,
    sales_name ,
    first_supervisor_name,
    credit_limit ,
    temp_credit_limit ,    
    zterm ,
    diff ,
    ac_all ,
    ac_wdq ,
    ac_15d ,
    ac_30d ,
    ac_60d ,
    ac_90d ,
    ac_120d ,
    ac_180d ,
    ac_365d ,
    ac_2y ,
    ac_3y ,
    ac_over3y,
    last_sales_date,
        last_to_now_days,
        customer_active_sts_code,
        customer_active_sts 
from
    csx_tmp.ads_fr_account_receivables          
where
    sdt='20200928'
    and province_code ='-1'
   -- and customer_no ='103827'   
order by
    comp_code,
    dist ,
    kunnr ;
    

select  mrp_prop_key ,mrp_prop_value 
        from csx_dw.dws_mms_w_a_factory_bom_m 
        where sdt='current'
        group by mrp_prop_key ,mrp_prop_value 
;

