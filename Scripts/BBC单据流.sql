-- 订单数据
(select
    location_code ,order_code ,goods_code ,
    -- count(distinct goods_code )as order_sku,
    sum(order_qty) as order_qty
from
    csx_dw.ads_supply_order_flow a 
where
    location_code in('W0P9', 'W0G8', 'W0N9', 'W0N8', 'W0S4', 'W0M9', 'W0K8', 'W0R2', 'W0B6', 'W0Q6', 'W0S6', 'W0H2')
    and sdt >= '20200701'
group by location_code ,order_code ,goods_code
)
;
-- 入库数据
select origin_order_code,
goods_code,
sum(receive_qty )as receive_qty ,
sum(shelf_qty)as shelf_qty
from csx_dw.wms_entry_order 
where sdt>='20200701' and sdt<='20200710'
group by 
goods_code,
origin_order_code
;  
select * from  csx_dw.wms_entry_order 
where sdt>='20200701' and sdt<='20200710' 
and entry_type like '采购%'
and business_type_code in ('P01','P02','T06');
-- 库存数据
select  dc_code ,
    count(distinct goods_code )as inv_sku,
    sum(case when subtring(reservoir_area_code,1,2)in ('BZ','TH') then qty end )as qty,
    sum(case when subtring(reservoir_area_code,1,2)in ('BZ','TH') then amt end )as amt ,
    count(DISTINCT case when reservoir_area_code like 'TH%' 
                        when division_code in ('12','13','14')
                        then  goods_code end )as th_foods_sku,
    count(DISTINCT case when reservoir_area_code like 'TH%' 
                        when division_code in ('10','11')
                        then  goods_code end )as th_fresh_sku,
    sum( case when reservoir_area_code like 'TH%' 
                        when division_code in ('12','13','14')
                        then  qty end )as th_foods_qty,
    sum( case when reservoir_area_code like 'TH%' 
                        when division_code in ('10','11')
                        then  qty end )as th_fresh_qty, 
    sum( case when reservoir_area_code like 'TH%' 
                        when division_code in ('12','13','14')
                        then  amt end )as th_foods_amt,
    sum( case when reservoir_area_code like 'TH%' 
               when division_code in ('10','11')
               then  amt end )as th_fresh_amt   
from csx_dw.dws_wms_r_d_accounting_stock_m 
where sdt='20200709'
-- and is_bz_reservoir=1
and dc_code in ('W0P9', 'W0G8', 'W0N9', 'W0N8', 'W0S4', 'W0M9', 'W0K8', 'W0R2', 'W0B6', 'W0Q6', 'W0S6', 'W0H2')
group by dc_code
;
-- 报损/盘点
select
       location_code ,
       sum(case when (move_type ='117A' and category_code in ('10','11')) then amt_no_tax 
                when (move_type ='117B' and category_code in ('10','11')) then amt_no_tax*-1 end ) as fresh_loss_amt, -- 生鲜报损金额
       sum(case when (move_type ='117A' and category_code in ('12','13','14')) then amt_no_tax 
                when (move_type ='117B' and category_code in ('12','13','14')) then amt_no_tax*-1 end ) as foods_loss_amt, -- 食百报损金额
       sum(case when move_type ='117A'then amt_no_tax when  move_type ='117B' then amt_no_tax*-1 end ) as loss_amt, -- 生鲜报损金额       
       sum(case when move_type in('115A') and category_code in ('10','11')  then amt_no_tax end ) as fresh_inventory_profit, -- 生鲜盘盈（生鲜取过帐）
       sum(case when move_type in('111A') and category_code in ('12','13','14')  then amt_no_tax end ) as foods_inventory_profit, -- 盘盈金额(食百取未过帐)
       sum(case when move_type in('116A') and category_code in ('10','11')  then amt_no_tax end ) as fresh_inventory_loss, -- 盘亏金额(生鲜取过帐)
       sum(case when move_type in('110A') and category_code in ('12','13','14') then amt_no_tax end ) as foods_inventory_loss -- 食百盘亏金额(生鲜取过帐、食百取未过帐)
from
        csx_dw.dwd_cas_r_d_accounting_credential_item
    where
        sdt >= '20200701'
        and sdt <= '20200712'
    group by location_code;


-- 出库数据
select shipped_location_code,order_no ,goods_code ,
    count(distinct case when business_type_code !='73' then  order_no end )order_num,
    count(distinct case when business_type_code !='73' then goods_code end)as shipped_sku,
    sum(case when business_type_code !='73' then shipped_qty end)as shipped_qty ,
    sum(case when business_type_code !='73' then (plan_qty-shipped_qty) end ) as diff_shipped_qty,  --  缺货数据
    count(distinct  case when (plan_qty-shipped_qty !=0 and business_type_code !='73') then goods_code  end )as stock_out_sku, -- 缺货SKU
    sum(case when business_type_code !='73' then (plan_qty*price -amount )end ) as diff_shipped_amt, --缺货金额
    sum(case when business_type_code ='22' then amount end ) as bbc_express_amt, -- 快递配出库额
    sum(case when business_type_code ='21' then amount end ) as bbc_city_amt, -- 同城配
    sum(case when business_type_code ='20' then amount end ) as bbc_pick_amt, -- 自提
    sum(case when business_type_code ='73' then amount end ) as bbc_wholesale_amt -- 一件代发
from csx_dw.wms_shipped_order
where send_sdt >='20200701'and send_sdt<='20200712'
and source_system ='BBC'
group by shipped_location_code;

-- 退货入库
select receive_location_code,
       sum(case when business_type_code ='R21' then amount end )as return_city_amt ,
       sum(case when business_type_code ='R22' then amount end )as return_express_amt , 
       sum(case when business_type_code ='R20' then amount end )as return_pick_amt ,
       sum(case when business_type_code ='R73' then amount end )as return_wholesale_amt ,
       sum(case when business_type_code ='71' then amount end )as return_noorder_amt
from csx_dw.wms_entry_order 
where business_type_code in ('R21','R22','71','R20','R73') 
    and sdt>='20200701'
group by receive_location_code;

-- 拣货效率
refresh csx_ods.source_wms_w_d_wms_product_stock_log ;
select warehouse_code ,
        create_by,
       count(distinct case when task_type='04' then product_code end) as pick_sku, -- 拣货SKU
       sum(case when task_type='04' then adjustment_qty end) as pick_qty, -- 拣货数量
       count(distinct case when task_type='08' then product_code end) as pack_sku, -- 打包SKU
       sum(case when task_type='08' then adjustment_qty end) as pack_qty -- 打包数据
 from csx_ods.source_wms_w_d_wms_product_stock_log  
 where sdt='19990101' 
 and task_type ='04'
 and to_date(update_time )>='2020-07-01'
group by 
warehouse_code ,
create_by
;

select * from csx_dw.ads_bbc_r_d_order ;

-- bbc订单
select * from csx_dw.dws_bbc_r_d_wshop_order_m where sdt>='20200701' and sdt<='20200712' ;
select* from csx_dw.wms_entry_order where business_type_code in ('R21','R22','71','R20','R73')and sdt>='20200701' ;



select distinct business_type,business_type_code ,shipped_type ,shipped_type_code from csx_dw.wms_shipped_order where shipped_type_code like 'S%';
select * from csx_dw.dwd_cas_r_d_accounting_credential_item ;
select * from csx_dw.wms_product_stock_m ;
select * from csx_dw.dws_wms_r_d_frmloss_order_all_detail where sdt='20200709';
select * from csx_dw.dwd_cas_r_d_accounting_credential_item where sdt='20200709';

select a.*,b.* from csx_dw.dws_crm_w_a_customer_m_v1 a 
join 
(select customer_number,company_code,payment_name,credit_limit,temp_credit_limit
    from csx_dw.dws_crm_r_a_customer_account_day where sdt='current')as b 
    on a.customer_no =b.customer_number
    where sdt='20200708';
    SELECT *from csx_dw.csx_shop where location_code ='W0M6' and sdt='current';
select * from csx_dw.dws_basic_w_a_company_code where code='2800';



select shipped_location_code,send_sdt as sdt,
--    count(distinct case when business_type_code !='73' then  order_no end )order_num,
--    count(distinct case when business_type_code !='73' then goods_code end)as shipped_sku,
--    sum(case when business_type_code !='73' then order_shipped_qty end)as shipped_qty ,
    sum(case when (business_type_code !='73' and plan_qty!=order_shipped_qty) then (plan_qty-coalesce(shipped_qty,0) ) end ) as diff_shipped_qty,  --  缺货数据
    count(distinct  case when (plan_qty>order_shipped_qty and business_type_code !='73') then goods_code  end )as stock_out_sku -- 缺货SKU
--    sum(case when (business_type_code !='73'and plan_qty>order_shipped_qty) then (plan_qty*price -amount )end ) as diff_shipped_amt, --缺货金额
--    sum(case when business_type_code ='22' then amount end ) as bbc_express_amt, -- 快递配出库额
--    sum(case when business_type_code ='21' then amount end ) as bbc_city_amt, -- 同城配
--    sum(case when business_type_code ='20' then amount end ) as bbc_pick_amt, -- 自提
--    sum(case when business_type_code ='73' then amount end ) as bbc_wholesale_amt -- 一件代发
from csx_dw.wms_shipped_order
where   send_sdt>='20200714'
    and send_sdt<='20200714'
and source_system ='BBC'
and shipped_location_code='W0B6'
and 
group by shipped_location_code,send_sdt
;

select shipped_location_code,send_sdt as sdt,goods_code ,goods_name ,plan_qty ,order_shipped_qty ,shipped_qty 
from csx_dw.wms_shipped_order
where   send_sdt>='20200714'
    and send_sdt<='20200716'
and source_system ='BBC'
and shipped_location_code='W0B6'
and business_type_code !='73' and plan_qty!=order_shipped_qty
;


select receive_location_code,sdt,goods_code ,goods_name ,plan_qty ,receive_qty ,shelf_qty ,business_type 
from csx_dw.wms_entry_order 
where  sdt>='20200714'
    and sdt<='20200714'
    and receive_status=2
--and source_system ='BBC'
and receive_location_code ='W0B6'
    and business_type_code in ('01','02','12')

;



select receive_location_code,sdt,
count(distinct case when business_type_code in('01','02') then goods_code end ) as order_sku,
sum(shelf_qty)as shelf_qty
from csx_dw.wms_entry_order 
where sdt>='20200715'
   -- and sdt<=${hiveconf:edate} 
    and business_type_code in ('01','02','12')
    and receive_status=2
    and receive_location_code ='W0B6'
group by 
receive_location_code,sdt
;  
