
-- 部类课组销售
create temporary table csx_tmp.zone_sale_01
as
select
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    sum(days_sale/10000)as days_sale,
    sum(days_profit/10000) as days_profit,
    sum(days_profit)/ sum(days_sale) as days_profit_rate,
    sum(sale/10000) sale,
    sum(ring_months_sale/10000) as ring_months_sale,
    sum(sale-ring_months_sale)/sum(ring_months_sale) as ring_sales_ratio,
    sum(profit/10000) profit,
    sum(profit)/sum(sale)as profit_rate,
    sum(sale_sku)as sale_sku,
    sum(sale_cust)as sale_cust,
    sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
    sum(all_sale_cust) as all_sale_cust
from
(
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    sum(case when sdt = '20200724' then sales_value end )as days_sale,
    sum(case when sdt = '20200724' then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
province_code ,
    province_name ,
    division_code ,
    division_name,
    a.department_code,
    a.department_name
union all 
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    0 as days_sale,
    0 as days_profit,
    0 sale,
    0 profit,
    0 sale_cust,
    0 sale_sku,
    sum(sales_value)as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt >= '20200601'
    and sdt <= '20200624'
    and  channel in ('1','7')
group by 
    province_code ,
    province_name ,
    division_code ,
    division_name,
    department_code,
    department_name
) a 
left join 
(
select
    province_code ,
    province_name ,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
    province_code ,
    province_name 
   ) b on a.province_code=b.province_code 
   left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    group by zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name;



-- 部类课组销售
drop table csx_tmp.zone_sale_02;
create temporary table csx_tmp.zone_sale_02
as
select
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    division_code ,
    division_name,
    sum(days_sale/10000)as days_sale,
    sum(days_profit/10000) as days_profit,
    sum(days_profit)/ sum(days_sale) as days_profit_rate,
    sum(sale/10000) sale,
    sum(ring_months_sale/10000) as ring_months_sale,
    sum(sale-ring_months_sale)/sum(ring_months_sale) as ring_sales_ratio,
    sum(profit/10000) profit,
    sum(profit)/sum(sale)as profit_rate,
    sum(sale_sku)as sale_sku,
    sum(sale_cust)as sale_cust,
    sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
    sum(all_sale_cust) as all_sale_cust
from
(
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
    sum(case when sdt = '20200724' then sales_value end )as days_sale,
    sum(case when sdt = '20200724' then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
province_code ,
    province_name ,
    division_code ,
    division_name
union all 
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
    0 as days_sale,
    0 as days_profit,
    0 sale,
    0 profit,
    0 sale_cust,
    0 sale_sku,
    sum(sales_value)as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt >= '20200601'
    and sdt <= '20200624'
    and  channel in ('1','7')
group by 
    province_code ,
    province_name ,
    division_code ,
    division_name
) a 
left join 
(
select
    province_code ,
    province_name ,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
    province_code ,
    province_name 
   ) b on a.province_code=b.province_code 
   left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    group by zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    division_code ,
    division_name
   ;
   

create temporary table csx_tmp.zone_sale_03
as 
-- 事业部类课组销售
select
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    business_division_code,business_division_name ,
    sum(days_sale/10000)as days_sale,
    sum(days_profit/10000) as days_profit,
    sum(days_profit)/ sum(days_sale) as days_profit_rate,
    sum(sale/10000) sale,
    sum(ring_months_sale/10000) as ring_months_sale,
    sum(sale-ring_months_sale)/sum(ring_months_sale) as ring_sales_ratio,
    sum(profit/10000) profit,
    sum(profit)/sum(sale)as profit_rate,
    sum(sale_sku)as sale_sku,
    sum(sale_cust)as sale_cust,
    sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
    sum(all_sale_cust) as all_sale_cust
from
(
select
    province_code ,
    province_name ,
    business_division_code,business_division_name ,
    sum(case when sdt = '20200724' then sales_value end )as days_sale,
    sum(case when sdt = '20200724' then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
left join
(select  category_small_code,business_division_code,business_division_name from csx_dw.dws_basic_w_a_category_m where sdt='current') d on a.category_small_code=d.category_small_code
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
province_code ,
    province_name ,
    business_division_code,business_division_name 
union all 
select
    province_code ,
    province_name ,
    business_division_code,
    business_division_name ,
    0 as days_sale,
    0 as days_profit,
    0 sale,
    0 profit,
    0 sale_cust,
    0 sale_sku,
    sum(sales_value)as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
    left join
(select  category_small_code,business_division_code,business_division_name from csx_dw.dws_basic_w_a_category_m where sdt='current') d on a.category_small_code=d.category_small_code
where
    sdt >= '20200601'
    and sdt <= '20200624'
    and  channel in ('1','7')
group by 
    province_code ,
    province_name ,
    business_division_code,business_division_name 
) a 
left join 
(
select
    province_code ,
    province_name ,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
    province_code ,
    province_name 
   ) b on a.province_code=b.province_code 
   left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
  group by zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    business_division_code,
    business_division_name
   ;

select 
id,
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    days_sale,
    days_profit,
    days_profit_rate,
    sale,
    ring_months_sale,
    ring_sales_ratio,
    profit,
    profit_rate,
    sale_sku,
    sale_cust,
    penetration_rate,  -- 渗透率
    all_sale_cust
    from 
    (
select 
    1 as id,
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    days_sale,
    days_profit,
    days_profit_rate,
    sale,
    ring_months_sale,
    ring_sales_ratio,
    profit,
    profit_rate,
    sale_sku,
    sale_cust,
    penetration_rate,  -- 渗透率
    all_sale_cust from  csx_tmp.zone_sale_01 a
   union all 
   select  
    2 as id,
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    division_code ,
    division_name,
    0 as  department_code ,
    0 as department_name,
    days_sale,
    days_profit,
    days_profit_rate,
    sale,
    ring_months_sale,
    ring_sales_ratio,
    profit,
    profit_rate,
    sale_sku,
    sale_cust,
    penetration_rate,  -- 渗透率
    all_sale_cust from  csx_tmp.zone_sale_02 a 
   union all 
   select  
    3 as id,
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    case when business_division_code='12' then '13' else business_division_code end  as  division_code , 
    business_division_name as  division_name ,
    0 as department_code ,
    0 as department_name,
    days_sale,
    days_profit,
    days_profit_rate,
    sale,
    ring_months_sale,
    ring_sales_ratio,
    profit,
    profit_rate,
    sale_sku,
    sale_cust,
    penetration_rate,  -- 渗透率
    all_sale_cust from  csx_tmp.zone_sale_03 a 
    ) a 
    where zone_id='3'
    order by province_code,division_code,id ,department_code    ;



-- -- 部类课组销售
-- drop table csx_tmp.zone_sale_02;
-- create temporary table csx_tmp.zone_sale_02
-- as
select
    a.zone_id,
    a.zone_name,
    division_code ,
    division_name,
    sum(days_sale/10000)as days_sale,
    sum(days_profit/10000) as days_profit,
    sum(days_profit)/ sum(days_sale) as days_profit_rate,
    sum(sale/10000) sale,
    sum(ring_months_sale/10000) as ring_months_sale,
    sum(sale-ring_months_sale)/sum(ring_months_sale) as ring_sales_ratio,
    sum(profit/10000) profit,
    sum(profit)/sum(sale)as profit_rate,
    sum(sale_sku)as sale_sku,
    sum(sale_cust)as sale_cust,
    sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
    sum(all_sale_cust) as all_sale_cust
from
(
select
    zone_id,zone_name  ,
    division_code ,
    division_name,
    sum(case when sdt = '20200724' then sales_value end )as days_sale,
    sum(case when sdt = '20200724' then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
 left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
zone_id,zone_name  ,
    division_code ,
    division_name
union all 
select
   zone_id,zone_name  ,
    division_code ,
    division_name,
    0 as days_sale,
    0 as days_profit,
    0 sale,
    0 profit,
    0 sale_cust,
    0 sale_sku,
    sum(sales_value)as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
 left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
where
    sdt >= '20200601'
    and sdt <= '20200624'
    and  channel in ('1','7')
group by 
    zone_id,zone_name  ,
    division_code ,
    division_name
) a 
left join 
(
select
   zone_id,zone_name  ,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_customer_sale a
 left join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
where
    sdt >= '20200701'
    and sdt <= '20200724'
    and  channel in ('1','7')
group by 
   zone_id,zone_name 
   ) b on a.zone_id=b.zone_id 
  
    group by a.zone_id,
   a.zone_name,
   
    division_code ,
    division_name
   ;
   

-- 库存周转
    
select dc_type,
zone_id,zone_name,
    prov_code ,
    prov_name,
    bd_id ,
    bd_name ,
    final_amt ,
    final_qty,
    days_turnover,
    goods_sku ,
    sale_sku,
    pin_rate,
  negative_inventory/goods_sku  negative_rate,
  highet_sku /goods_sku  as highet_sku_rate,
  no_sale_sku /goods_sku as no_sale_sku_rate, 
    negative_amt,
    negative_inventory,
    highet_amt,
    highet_sku,
    no_sale_amt ,
    no_sale_sku 
from
    csx_dw.supply_turnover_province a 
join 
(select distinct province_code ,zone_id,zone_name from csx_dw.csx_shop where sdt='current' and zone_id='3') b on a.prov_code =b.province_code
where
    sdt = '20200724'
    and (dept_id ='00' or bd_id='00')
    and dc_type=    '仓库';
