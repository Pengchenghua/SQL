
-- 供应链看板SQL
 select
 bd_id ,bd_name ,
    sum(sales_value)
from
    csx_dw.dc_sale_inventory
where
    sdt >= '20200401'
GROUP  by  bd_id ,bd_name;

refresh csx_dw.ads_sale_r_m_dept_sale_mon_report;
select
    date_m,
    sale / 10000 as sale,
    profit / 10000 profit,
    profitrate,
    sale_ratio
from
    csx_dw.ads_sale_r_m_dept_sale_mon_report
where
    sdt = '20200429'
    and division_code = '11'
    and province_code = '00'
    and date_m = '本年'
    and department_code = '00'
    and channel_name = '全渠道' ;
select * from csx_dw.ads_sale_r_m_dept_sale_mon_report where sdt ='20200422';
select * from csx_dw.supply_turnover_province  where sdt='20200422';

-- 每日销售趋势
select date_m,sale_sdt,channel_name ,bd_id,bd_name,sale,profit,profit_rate
from (
select '本月' date_m,
from_unixtime(unix_timestamp(sdt ,'yyyyMMdd'),'MM-dd')as sale_sdt,
channel_name ,
case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end bd_id,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end bd_name,
    sum(sales_value)sale,
    sum(profit )profit ,
    sum(profit )/sum(sales_value) as profit_rate
from
    csx_dw.dws_sale_r_d_customer_sale 
where
    sdt >= '20200401' and sdt<='20200422'
    group by case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end ,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end ,
sdt,channel_name
union all 
select '本年' date_m,
from_unixtime(unix_timestamp(sdt ,'yyyyMMdd'),'yyyy-MM')as mon,
channel_name ,
case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end bd_id,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end bd_name,
    sum(sales_value)sale,
    sum(profit )profit ,
    sum(profit )/sum(sales_value) as profit_rate
from
    csx_dw.dws_sale_r_d_customer_sale 
where
    sdt >= '20200101' and sdt<='20200422'
    group by case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end ,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end ,
from_unixtime(unix_timestamp(sdt ,'yyyyMMdd'),'yyyy-MM'),channel_name
)a where 1=1
;


-- 销售整体数据
SELECT * from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200101' and sdt<='20200423' and department_name  like '熟食%'
;
select *, to_utc_timestamp(write_time ,'GMT +8'),
(write_time + interval 8 hour) from csx_dw.ads_sale_r_m_dept_sale_mon_report  where sdt='20200423' and division_code in ('12','13','14') and date_m ='本年';
SELECT * from csx_dw.account_age_dtl_fct_new where kunnr ='0000107182' and sdt='20200422';

select regexp_replace('0000009951','(^0*)','');

select * from b2b.csx_customer where cust_id ='G2121';

select *  from csx_dw.wms_entry_order where sdt>='20200401'and supplier_code like '75%';

select DISTINCT vendor_code from csx_dw.supple_goods_sale_dtl where sdt>='20200401';

select customer_no ,customer_name,second_category  ,sum(sales_qty )qty,SUM(sales_value )/10000 sale,SUM(profit )/10000 profit ,SUM(profit )/SUM(sales_value )profit_rate,count(distinct goods_code) as sale_sku
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200401' and division_code in('11','10')and channel='1'
group by customer_no ,customer_name ,second_category
order by SUM(sales_value ) desc
limit 10;

select
    supplier_code ,
    supplier_name ,
    sum(receive_qty) receive_qty,
    sum(receive_amt)/ 10000 receive_amt,
    sum(shipped_qty)shipped_qty,
    sum(shipped_amt)/ 10000 shipped_amt
from
    (
    select
        supplier_code ,
        supplier_name,
        receive_location_code as dc_code ,
        sum(receive_qty) receive_qty,
        sum(amount) receive_amt,
        0 shipped_qty,
        0 shipped_amt
    from
        csx_dw.wms_entry_order
    where
        sdt >= '20200401'
        and entry_type = '采购入库'
        and division_code in ('10',
        '11')
    group by
        supplier_code ,
        supplier_name,
        receive_location_code
union all
    select
        supplier_code ,
        supplier_name,
        shipped_location_code as dc_code,
        0 receive_qty,
        0 receive_amt,
        sum(shipped_qty) shipped_qty,
        sum(amount) amt
    from
        csx_dw.wms_shipped_order
    where
        sdt >= '20200401'
        and shipped_type = '采购出库'
        and division_code in ('10',
        '11')
    group by
        supplier_code ,
        supplier_name,
        shipped_location_code )a
JOIN (
    select
        *
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and location_type_code = '1') b on
    a.dc_code = b.location_code
group by
    supplier_code ,
    supplier_name
order by
    receive_amt desc
limit 10;
SELECT supplier_code ,supplier_name,sum(case when in_or_out ='in' then in_out_amount end )in_amt,
sum(case when in_or_out ='in' then price*in_out_qty end )in_amt_2,
sum(case when in_or_out ='out' then in_out_amount end )out_amt,
sum(case when in_or_out ='out' then price*in_out_qty end )out_amt_2
FROM csx_dw.dws_scm_r_d_scm_order_m  a 
(
    select
        *
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and location_type_code = '1') b on a.
where sdt>='20200401'
group by supplier_code ,supplier_name;

select supplier_code ,goods_code,goods_name,in_out_qty,price,amt,batch_price,price2_include_tax,price1_enable_type ,price*in_out_qty 
from csx_dw.dws_scm_r_d_scm_order_m  where supplier_code ='20024807' and sdt>='20200401' ;


select * from csx_dw.dws_scm_r_d_scm_order_m   where supplier_code ='20024807' and sdt>='20200401' and goods_code ='519' ;

select * from csx_dw.receivables_collection where channel like '%供应链(生鲜)%'and sdt='20200429' ;

select date_m,sale/10000 as sale,profit/10000 profit,profitrate,sale_ratio,sale_rate from csx_dw.ads_sale_r_m_dept_sale_mon_report where sdt='20200429' 
and division_code in ('11')
and province_code='00' and date_m='本年' and   department_code = '00' and channel_name='全渠道';


select date_m,   channel_code,channels,bd_id,bd_name,sum(sale)/10000 sale,sum(profit )/10000 as profit 
from (
select  '本月'date_m, channel  channel_code,
 regexp_replace( channel_name,'\\s','')  channels ,
 case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end bd_id,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end bd_name,
 SUM(sales_value )as sale,sum(profit)profit from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200401'
group by  channel  ,
  channel_name ,
  case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end ,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end 
  union all 
select  '本年'date_m, channel  channel_code,regexp_replace( channel_name,'\\s','')  channels ,
case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end bd_id,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end bd_name,
 SUM(sales_value )as sale,sum(profit)profit from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200401'
group by  channel  ,case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12' else division_code end ,
case when division_code in ('10','11') then '生鲜采购部' when division_code in ('12','13','14') then '食百采购部' else division_name end ,
  channel_name ) a where 1=1 
  group by date_m,   channel_code,channels,bd_id,bd_name;
  
  
  select supplier_code ,supplier_name ,count(distinct case when receive_qty>0 then  goods_code end ) as sku,sum(receive_qty )receive_qty,sum(receive_amt )/10000 receive_amt,sum(shipped_qty)shipped_qty,sum(shipped_amt)/10000 shipped_amt 
from (
select supplier_code ,supplier_name,receive_location_code as dc_code,goods_code,sum(receive_qty  )receive_qty,
sum( amount )receive_amt,
0 shipped_qty,0 shipped_amt
from csx_dw.wms_entry_order where sdt>='20200101' and entry_type ='采购入库' and division_code in ('10','11')
group by supplier_code ,supplier_name,receive_location_code,goods_code
union all 
select supplier_code ,supplier_name,shipped_location_code  as dc_code,goods_code,
0 receive_qty,0 receive_amt,
sum( shipped_qty  )shipped_qty,
sum(amount )shipped_amt
from csx_dw.wms_shipped_order where sdt>='20200101' and shipped_type ='采购出库'and division_code in ('10','11')
group by supplier_code ,supplier_name,shipped_location_code,goods_code
)a 
JOIN 
(select * from csx_dw.csx_shop where sdt='current' and location_type_code ='1') b on a.dc_code=b.location_code
group by supplier_code ,supplier_name
order by receive_amt desc
limit 10;

select *   from
        csx_dw.provinces_kanban_goods_lose  where sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','');
        
select * from     csx_dw.dws_basic_w_a_category_m;


select supplier_code ,supplier_name,business_division_code,business_division_name ,count(distinct case when receive_qty>0 then  goods_code end ) as sku,sum(receive_qty )receive_qty,sum(receive_amt )/10000 receive_amt,sum(shipped_qty)shipped_qty,sum(shipped_amt)/10000 shipped_amt 
from (
select supplier_code ,supplier_name,receive_location_code as dc_code,goods_code,category_small_code ,sum(receive_qty  )receive_qty,
sum( amount )receive_amt,
0 shipped_qty,0 shipped_amt
from csx_dw.wms_entry_order where sdt>='20200101' and entry_type ='采购入库' 
group by supplier_code ,supplier_name,receive_location_code,goods_code,category_small_code
union all 
select supplier_code ,supplier_name,shipped_location_code  as dc_code,goods_code,category_small_code ,
0 receive_qty,0 receive_amt,
sum( shipped_qty  )shipped_qty,
sum(amount )shipped_amt
from csx_dw.wms_shipped_order where sdt>='20200101' and shipped_type ='采购出库'
group by supplier_code ,supplier_name,shipped_location_code,goods_code,category_small_code
)a 
JOIN 
(select * from csx_dw.csx_shop where sdt='current' and location_type_code ='1') b on a.dc_code=b.location_code
join 
(select * from     csx_dw.dws_basic_w_a_category_m where sdt='current' ) c on a.category_small_code=c.category_small_code
group by supplier_code ,supplier_name,business_division_code,business_division_name
order by receive_amt desc
limit 10;

-- 客户TOP 10
select customer_no ,customer_name,second_category  ,(sales_qty )qty,(sales_value )/10000 sale,(profit )/10000 profit ,
(profit )/(sales_value )profit_rate,sales_sku,sales_days,rank_num
from csx_dw.ads_sale_customer_division_level_sales where sdt='20200507' and business_division_code='11' and purchase_group_code='00' and channel='1' and date_m='m' and sales_months='202005'
order by sales_value desc
LIMIT 10;
select customer_no ,customer_name,second_category  ,(sales_qty )qty,(sales_value )/10000 sale,(profit )/10000 profit ,
(profit )/(sales_value )profit_rate,sales_sku,sales_days,rank_num
from csx_dw.ads_sale_customer_division_level_sales where sdt='20200507' and business_division_code='11' and layer='2' and province_code='1'
and channel='1' and date_m='m' and sales_months='202005'
order by sales_value DESC;

refresh csx_dw.ads_supply_daily_sales_trends ;
-- 销售日趋势
SELECT * FROM csx_dw.ads_supply_daily_sales_trends WHERE sdt='20200507';

-- 供应商入库
refresh csx_dw.ads_supply_kanban_supplier_entry;
select supplier_code,supplier_name,sku,receive_qty,receive_amt,shipped_qty,shipped_amt
from csx_dw.ads_supply_kanban_supplier_entry
where sdt='20200510' and date_m='m' and province_code='00'
order by receive_amt desc;

refersh csx_dw.ads_supply_customer_division_level_sales;

select * from csx_dw.ads_supply_customer_division_level_sales;

select channel_name ,goods_code ,goods_name ,category_small_code,
COUNT(DISTINCT customer_no ) as sales_cust_num,
COUNT(DISTINCT sdt) as sales_days, 
sum(sales_qty )qty,SUM(sales_value )sales_value,sum(profit )profit ,
min(sales_price )min_price,
max(sales_price )max_price
from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200501'
group by channel_name ,goods_code ,goods_name ,category_small_code;


select date_m,sale_sdt,bd_id,bd_name,sale/10000 sale ,profit/10000 profit,profit/sale*1.00 as profit_rate
from csx_dw.ads_supply_daily_sales_trends where sdt ='20200508' and sale_sdt >=from_unixtime(unix_timestamp('20200501','yyyyMMdd'),'MM-dd')
limit 10;

SELECT province_code,province_name,business_division_code,supplier_code ,supplier_name ,sku ,receive_qty ,receive_amt ,shipped_qty ,shipped_amt 
FROM csx_dw.ads_supply_kanban_supplier_entry 
where sdt='20200508' and province_code ='00' ;

select* from csx_dw.ads_supply_kanban_goods_sales; 
group by brand_name,business_division_code ,business_division_name  ;


select * from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200501' and goods_name  like '彩食鲜金针菇300g';
select MIN(write_time)from 
(
select  '供应商入库'as note, max(write_time)write_time from csx_dw.ads_supply_kanban_supplier_entry where sdt>='20200511' 
union all 
select  '每日销售趋势'as note, max(write_time)write_time  from csx_dw.ads_supply_daily_sales_trends where sdt>='20200511' 
union all 
select  '商品销售表'as note, max(write_time)write_time  from csx_dw.ads_supply_kanban_goods_sales where sdt>='20200511' 
union all 
select  '客户销售'as note, max(write_time)write_time  from  csx_dw.ads_supply_customer_division_level_sales where sdt>='20200511' 
union all 
select  '课组销售'as note, max(write_time)write_time  from csx_dw.ads_sale_r_m_dept_sale_mon_report where sdt>='20200511')a ;

-- 1-4月TOP30 销售、毛利剔除合伙人商超
select * from csx_dw.provinces_kanban sdt='20200514';



select * from         csx_dw.dws_mms_r_a_factory_order 
    where
        sdt >= regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'MM')),'-','')  
        and sdt <= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')   ;
    select
        transfer_out_province_code province_code,
        transfer_out_province_name province_name,
        sum(plan_receive_qty)/1000 plan_qty,
        sum(reality_receive_qty)/1000 as 计划原料量,
        sum(user_qty)/1000 user_qty,
        sum(fact_qty)/1000 as 实际原料量,
        sum(goods_reality_receive_qty)/1000 as 成品产量,
        round(sum(transfer_quantity)/1000,2) as 成品调拨量
    from
        csx_dw.dws_mms_r_a_factory_order 
    where
        sdt >= regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'MM')),'-','')  
        and sdt <= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')  
        
    group by
        transfer_out_province_code,
        transfer_out_province_name;
        
    select
    *
from
    csx_dw.ads_supply_kanban_supplier_entry
where
    sdt = '20200515'
    and province_name like '河北%'
   and sales_months = ''
    and business_division_code = '11'
      and supplier_code = '20039625';
    
select province_code,province_name,customer_no ,customer_name,attribute ,(sales_qty )qty,(sales_value )/10000 sale,(profit )/10000 profit ,(profit )/(sales_value )profit_rate,sales_sku,sales_days,return_amt/10000 return_amt
from csx_dw.ads_supply_customer_division_level_sales where sdt='${edate}' 
and business_division_code='11' and channel='1' and layer='2'
and date_m='y'
order by sales_value desc
limit 10;

select *
from  csx_dw.ads_supply_customer_division_level_sales where sdt='20200515' 
and business_division_code='11' and channel='1' and layer='2'
and date_m='y' and customer_no ='102955'
order by sales_value desc;



    select
         province_code,
         province_name,
        sum(plan_receive_qty)/1000 plan_qty,
        sum(reality_receive_qty)/1000 as 计划原料量,
        sum(user_qty)/1000 user_qty,
        sum(fact_qty)/1000 as 实际原料量,
        sum(goods_reality_receive_qty)/1000 as 成品产量,
        round(sum(transfer_quantity)/1000,2) as 成品调拨量
    from
csx_dw.dws_mms_r_a_factory_order 
    where
        sdt >= regexp_replace(to_date(trunc(date_sub(CURRENT_TIMESTAMP(),1),'MM')),'-','')  
        and sdt <= regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')  
        and province_code = '${province_01}'
    group by
        province_code ,
        province_name ;
        
    
    
    select
        province_code,
        type,
        province_name,
        workshop_code,
        workshop_name,
        sale_sku,
        all_sku,
        pin_rate,
        day_sale/10000 day_sale,
        day_profit_rate,
        round(sale/10000,
        2) sale,
        sale_ring_ratio,
        sale_yoy_ratio,
        round(profit/10000,
        2)profit ,
        profit_ring_ratio,
         mom_gross_rate_diff,
         profit_yoy_ratio,
         yoy_gross_rate_diff,
        profit_rate,
        shop_dept_cust,
        big_cust,
        big_dept_cust,
        negative_sku ,
        round(negative_value/10000,2)negative_value,
        round(reality_receive_qty/1000,2)reality_receive_qty,
        round(fact_qty/1000,2)fact_qty,
        round(fact_values/10000,2) as fact_values,
        fact_rate,
        round(transfer_amt/10000,2)transfer_amt, 
        round(transfer_qty/1000,2) transfer_qty
    from
 select * from        csx_dw.provinces_kanban       
    where
        sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')          
        and province_code='110000'
        and workshop_code !='H00'
   order by
        province_code,
        type desc,       
        CASE WHEN workshop_code='00'  then 1 WHEN workshop_code LIKE 'H%' and type ='1' THEN 2   end asc, workshop_code;
        
    select * from csx_dw.workshop_m ;
    
SELECT DISTINCT ''workshop_code,
                  ''workshop_name,
                    workshop_code AS small_workshop_code,
                    workshop_name AS small_workshop_name
FROM csx_dw.dws_mms_w_a_factory_bom_m
WHERE sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),
                         '-',
                         '')
  AND workshop_code!='H09';