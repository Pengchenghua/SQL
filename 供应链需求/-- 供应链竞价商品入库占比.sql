-- 供应链竞价商品入库占比

with entry as 
(select  
        a.sdt,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        a.dc_code,
        a.dc_name,
        a.receive_dc_code,
        a.settle_dc_code,
        purchase_order_code,
        a.order_code,
        a.supplier_code,
        supplier_name,
        supplier_classify_name,
        a.goods_code,
        goods_name,
        classify_middle_name,
        classify_small_name,
        stall_flag,
        case when classify_large_code in ('B01','B02','B03') then '生鲜'  else '食百' end div_name,
        if (order_price1 = 0,order_price2,order_price1) as cost,
        receive_qty,
        receive_amt,
        cycle_price_source
from csx_analyse.csx_analyse_scm_purchase_order_flow_di a
left join 
(select
  order_code,
  goods_code,
  price1_include_tax,
  amount1_include_tax,
  cycle_price_source
from
  csx_dwd.csx_dwd_scm_order_product_price_di
  where sdt>='20240301'
  and cycle_price_source=2
  )b on a.purchase_order_code=b.order_code and a.goods_code=b.goods_code
where  sdt>='20240401'
and is_supply_stock_tag = '1'
and super_class_code = '1'
-- and source_type_code in ('1', '19', '23') -- 1-采购导入、10-智能补货（实际上就是委外）、19-日采补货、23-手工创建
--and source_type_code not in ('4','15','18')
-- and goods_code in ('846778', '620')  
) 
select  substr(sdt,1,6) as month_sdt,
        sdt,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,      
        classify_middle_name,
        classify_small_name,
        div_name,
        stall_flag,
        cycle_price_source,
        if(stall_flag=1 or cycle_price_source=2,'是','否') stall_flag,
        sum(receive_qty) receive_qty,
        sum(receive_amt) receive_amt
        
from entry a 

group by substr(sdt,1,6) ,
        sdt  ,
        a.performance_region_code,
        a.performance_region_name,
        a.performance_province_code,
        a.performance_province_name,
        a.performance_city_code,
        a.performance_city_name,
        classify_small_name,
        classify_middle_name,
        div_name,
        stall_flag,
        cycle_price_source,
        if(stall_flag=1 or cycle_price_source=2,'是','否')

