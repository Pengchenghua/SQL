 select
    province_code,
    province_name,
    sum(plan_receive_qty)plan_receive_qty,
    sum(reality_receive_qty)reality_receive_qty,
    sum(reality_receive_qty)/ sum(plan_receive_qty) as product_satisfy_rate,
    sum(fact_qty)fact_qty,
    -- 出品率
    sum(reality_receive_qty)/ sum(fact_qty)as yield,
    sum(transfer_quantity)transfer_quantity,-- 出库量
    sum(f_transfer_values) as f_transfer_values ,-- 实际出库额成本 
    sum(p_transfer_values) as p_transfer_values,-- 计划出库成本
    sum(f_total)as f_total -- 成本

    from csx_dw.dws_mms_r_a_factory_order
where
    sdt = '20200701'
    and province_code = '110000'
group by
    province_code,
    province_name;
 
 select
    sdt,
    province_code,
    province_name,
    sum(plan_receive_qty)/1000 as plan_receive_qty,
    sum(reality_receive_qty)/1000 as reality_receive_qty,
    sum(reality_receive_qty)/ sum(plan_receive_qty) as product_satisfy_rate,
    sum(fact_qty)/1000 as fact_qty,
    -- 出品率
    sum(reality_receive_qty)/ sum(fact_qty)as yield,
    sum(transfer_quantity)/1000 as transfer_quantity,-- 出库量
    sum(f_transfer_values)/10000  as f_transfer_values ,-- 实际出库额成本 
    sum(p_transfer_values)/10000 as p_transfer_values,-- 计划出库成本
    sum(f_total)/10000 as f_total -- 成本

    from csx_dw.dws_mms_r_a_factory_order
where
    sdt >= '20200601'
    and sdt <= '20200701'
    and province_code = '110000'
group by
    sdt,
    province_code,
    province_name;
 
-- 车间产量

select
    
    province_code,
    province_name,
    workshop_code ,
    workshop_name ,
    sum(plan_receive_qty)/1000 as plan_receive_qty,
    sum(reality_receive_qty)/1000 as reality_receive_qty,
    sum(reality_receive_qty)/ sum(plan_receive_qty) as product_satisfy_rate,
    sum(fact_qty)/1000 as fact_qty,
    -- 出品率
    sum(reality_receive_qty)/ sum(fact_qty)as yield,
    sum(transfer_quantity)/1000 as transfer_quantity,-- 出库量
    sum(f_transfer_values)/10000  as f_transfer_values ,-- 实际出库额成本 
    sum(p_transfer_values)/10000 as p_transfer_values,-- 计划出库成本
    sum(f_total)/10000 as f_total -- 成本

    from csx_dw.dws_mms_r_a_factory_order
where
    sdt >= '20200601'
    and sdt <= '20200701'
    and province_code = '110000'
group by
     workshop_code ,
    workshop_name ,
    province_code,
    province_name;
    
select sales_belong_flag,workshop_name,province_code ,province_name,sales_value/10000 sale ,profit/10000 profit  
from csx_dw.ads_sale_r_d_sc_sale_month where smonth='202007' and province_code ='1' 
union all 
select '大客户' as sales_belong_flag,workshop_name,province_code ,province_name,sum(sales_value )/10000 as sale,sum(profit )/10000 profit  
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200701' and is_factory_goods_code =1 and channel in('1','7')
and province_code ='1'
group by workshop_name,province_code ,province_name
;
select * from csx_dw.ads_sale_r_d_sc_sale_day where sdt='20200702';
select * from csx_dw.ads_sale_r_d_sc_deptsale where sdt='20200702';
select small_workshop_code ,small_workshop_name from csx_dw.workshop_m ;
select * from csx_dw.csx_shop where sdt='current';