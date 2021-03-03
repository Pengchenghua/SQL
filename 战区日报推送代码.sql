-- set tez.queue.name= mr;
SET sdate=trunc(${hiveconf:edate},'MM');


SET edate= date_sub(CURRENT_DATE,1);


SET l_sdate= trunc(add_months(${hiveconf:edate},-1),'MM');


SET l_edate=add_months(${hiveconf:edate},-1);

-- select ${hiveconf:l_edate};
 -- 昨日\月销售数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale;


CREATE
TEMPORARY TABLE csx_tmp.temp_war_zone_sale AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    yesterday_sales_value, 
   -- last_day_sales,
    yesterday_profit,
   -- yesterday_negative_profit,
    yesterday_new_customer_sale,
    yesterday_often_customer_sale,
    yesterday_customer_num, 
    -- 月累计
    months_sales_value, 
    months_profit,
   -- negative_profit,
    months_new_customer_sale,
    months_often_customer_sale,
    months_customer_num
FROM 
(
SELECT CASE
           WHEN a.channel IN ('1','7') THEN '大客户'
           ELSE a.channel_name
       END channel_name,
       a.province_code,
       a.province_name,
       sum(CASE  WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value  END)AS yesterday_sales_value, 
     --  sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') and profit <0 then profit end ) as yesterday_negative_profit,
    -- sum(case when sdt=regexp_replace(date_sub(${hiveconf:edate},1),'-','') then sales_value end ) as last_day_sales,
    sum(CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN profit  END)AS yesterday_profit,
    sum(CASE WHEN substr(sdt,1,6)=first_sale_mon  AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value     END) AS yesterday_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon AND sdt=regexp_replace(${hiveconf:edate},'-','') THEN sales_value END) AS yesterday_often_customer_sale,
    count(DISTINCT CASE WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN a.customer_no END) AS yesterday_customer_num, 
    -- 月累计
    sum(sales_value)AS months_sales_value, 
    sum(profit)AS months_profit,
    -- sum(case when profit <0 then profit end ) as negative_profit,
    sum(CASE WHEN substr(sdt,1,6)=first_sale_mon THEN sales_value  END) AS months_new_customer_sale,
    sum(CASE WHEN substr(sdt,1,6) !=first_sale_mon THEN sales_value END) AS months_often_customer_sale,
    count(DISTINCT a.customer_no) AS months_customer_num
FROM csx_dw.dws_sale_r_d_customer_sale a
JOIN
  (SELECT customer_no,
          substr(first_sale_day,1,6) AS first_sale_mon
   FROM csx_dw.ads_sale_w_d_ads_customer_sales_q
   WHERE sdt=regexp_replace(${hiveconf:edate},'-','')) b ON a.customer_no=b.customer_no
WHERE sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
GROUP BY province_code,
         province_name,
         CASE
             WHEN a.channel IN ('1', '7') THEN '大客户'
             ELSE a.channel_name
         END
)a 

;

-- 关联上周环比数据

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_01;


CREATE
TEMPORARY TABLE csx_tmp.temp_war_zone_sale_01 AS

SELECT a.channel_name,
    a.province_code,
    a.province_name,
    yesterday_sales_value, 
    last_day_sales,
    yesterday_profit,
   -- yesterday_negative_profit,
    yesterday_new_customer_sale,
    yesterday_often_customer_sale,
    yesterday_customer_num, 
    -- 月累计
    months_sales_value, 
    months_profit,
   -- negative_profit,
    months_new_customer_sale,
    months_often_customer_sale,
    months_customer_num
FROM 
csx_tmp.temp_war_zone_sale  a 
join 
(select CASE
           WHEN a.channel IN ('1','7') THEN '大客户'
           ELSE a.channel_name
       END channel_name,
       province_code,
       sum(a.sales_value)as last_day_sales
    from csx_dw.dws_sale_r_d_customer_sale a
    where sdt=regexp_replace(date_sub(${hiveconf:edate},7),'-','')
    group by CASE
           WHEN a.channel IN ('1','7') THEN '大客户'
           ELSE a.channel_name
       END ,
       province_code
     ) as c on a.province_code=c.province_code and a.channel_name=c.channel_name;

-- select regexp_replace(date_sub(${hiveconf:edate},7),'-','');
-- show create table csx_dw.ads_sale_w_d_ads_customer_sales_q;
-- 上月环比数据

DROP TABLE IF EXISTS csx_tmp.temp_ring_war_zone_sale;
CREATE TEMPORARY TABLE csx_tmp.temp_ring_war_zone_sale AS
SELECT CASE
           WHEN a.channel IN ('1', '7') THEN '大客户'
           ELSE a.channel_name
       END channel_name,
       province_code,
       province_name,
       sum(CASE
               WHEN sdt=regexp_replace(${hiveconf:l_edate},'-','') THEN sales_value
           END)AS last_yesterday_sales_value,
       sum(sales_value)AS last_months_sales_value
FROM csx_dw.dws_sale_r_d_customer_sale a
WHERE sdt>=regexp_replace(${hiveconf:l_sdate},'-', '')
  AND sdt<=regexp_replace(${hiveconf:l_edate},'-','')
GROUP BY province_code,
         province_name,
         CASE
             WHEN a.channel IN ('1','7') THEN '大客户'
             ELSE a.channel_name
         END ;


-- 负毛利

DROP TABLE IF EXISTS csx_tmp.temp_war_zone_sale_02;


CREATE
TEMPORARY TABLE csx_tmp.temp_war_zone_sale_02 AS

select
    '大客户' as channel_name,
    province_code ,
    province_name  ,
    count(distinct  goods_code )as sale_sku,
    sum(sale/10000)sale,
    sum(profit/10000 )profit,
    sum(profit) /sum(sale) as profit_rate,
    sum(case when profit<0 and sdt=regexp_replace(${hiveconf:edate},'-','') then profit end ) as negative_days_profit,
    sum(case when profit<0 then profit end ) as negative_profit
 from (
select
    province_code ,
    province_name ,
    sdt,
    a.customer_no,
    goods_code ,
    goods_name,
    division_code ,division_name ,
    avg(cost_price )avg_cost,
    avg(sales_price )avg_sale,
    sum(sales_qty )qty,
    sum(sales_value) sale,
    sum(profit) profit
from
    csx_dw.dws_sale_r_d_customer_sale a 
where
    sdt>=regexp_replace(${hiveconf:sdate},'-','')
  AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    and channel in ('1','7')
group by 
   province_code ,
    province_name ,
    sdt,
    a.customer_no,
    goods_code ,
    goods_name,
    division_code ,division_name 
   ) a 
group by 
  
   province_code ,
   province_name ;
 
  


SELECT a.channel_name,
       a.province_code,
       a.province_name,
       zone_id,
       zone_name,
       sum(yesterday_sales_value/10000 )AS yesterday_sales_value,
       sum(last_day_sales/10000 ) as last_day_sales,
       (coalesce(sum(yesterday_sales_value),0)-coalesce(sum(last_day_sales),0))/coalesce(sum(last_day_sales),0) as daily_sale_rate,
       sum(yesterday_profit/10000 )AS yesterday_profit,
       coalesce(sum(yesterday_profit)/sum(yesterday_sales_value),0) as yesterday_profit_rate,
       (negative_days_profit/10000) as yesterday_negative_profit,
       sum(yesterday_often_customer_sale/10000 )AS yesterday_often_customer_sale,
       sum(yesterday_new_customer_sale/10000 )AS yesterday_new_customer_sale,
       sum(yesterday_customer_num)AS yesterday_customer_num,
       sum(months_sales_value/10000 )AS months_sales_value,
       sum(ring_months_sale/10000 )AS ring_months_sale,
       (coalesce(sum(months_sales_value),0)-coalesce(sum(ring_months_sale),0))/coalesce(sum(ring_months_sale),0) as months_sale_rate,
       sum(months_profit/10000 )AS months_profit,
       sum(months_profit)/sum(months_sales_value) as months_profit_rate,
       (negative_profit/10000) as negative_profit, 
       sum(months_often_customer_sale/10000 )AS months_often_customer_sale,
       sum(months_new_customer_sale/10000 )AS months_new_customer_sale,
       sum(months_customer_num)AS months_customer_num,
       sum(ring_date_sale/10000 ) AS ring_date_sale
FROM
  (SELECT channel_name,
          province_code,
          province_name,
          yesterday_sales_value,
          yesterday_profit,
          yesterday_often_customer_sale,
          yesterday_new_customer_sale,
          yesterday_customer_num,
          months_sales_value,
          months_profit,
          months_often_customer_sale,
          months_new_customer_sale,
          months_customer_num,
          last_day_sales,
          0 AS ring_date_sale,
          0 AS ring_months_sale
   FROM csx_tmp.temp_war_zone_sale_01
   UNION ALL SELECT channel_name,
                    province_code,
                    province_name,
                    0 AS yesterday_sales_value,
                    0 AS yesterday_profit,
                    0 AS yesterday_often_customer_sale,
                    0 AS yesterday_new_customer_sale,
                    0 AS yesterday_customer_num,
                    0 AS months_sales_value,
                    0 AS months_profit,
                    0 AS months_often_customer_sale,
                    0 AS months_new_customer_sale,
                    0 AS months_customer_num,
                    0 as last_day_sales,
                    last_yesterday_sales_value AS ring_date_sale,
                    last_months_sales_value AS ring_months_sale
   FROM csx_tmp.temp_ring_war_zone_sale) a
   left join 
   csx_tmp.temp_war_zone_sale_02 c on a.province_code=c.province_code and a.channel_name=c.channel_name
   left join 
   (select DISTINCT dist_code ,zone_id,zone_name from csx_dw.csx_shop where sdt='current') b on case when a.province_code in ('35','36') then '35' else a.province_code end =b.dist_code
GROUP BY a.channel_name,
         a.province_code,
         a.province_name,
         zone_id,
         zone_name,
         negative_days_profit,
         negative_profit
    ;
	

-- 大区课组销售

-- set sdate='20200701';
-- set edate='20200727';
-- set l_sdate='20200601';
-- set l_edate='20200627';

create temporary table csx_tmp.temp_zone_bd_sale as 
select
    c.zone_id,
    c.zone_name,
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
    sum(sale_cust)as sale_cust
   -- sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
   -- sum(all_sale_cust) as all_sale_cust
from
(
select
    province_code,a.province_name  ,
    case when department_code like 'U%' then 'U01' else department_code end     department_code ,
    case when department_code like 'U%' then '加工课' else department_name end department_name,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as days_sale,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
     sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
group by 
province_code,a.province_name ,
    case when department_code like 'U%' then 'U01' else department_code end  ,  
 case when department_code like 'U%' then '加工课' else department_name end 
union all 
select
   province_code,a.province_name ,
    case when department_code like 'U%' then 'U01' else department_code end     department_code ,
    case when department_code like 'U%' then '加工课' else department_name end  department_name,
    0 as days_sale,
    0 as days_profit,
    0 sale,
    0 profit,
    0 sale_cust,
    0 sale_sku,
    sum(sales_value)as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
where
    sdt >=  	regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  channel in ('1','7')
group by 
    province_code,a.province_name ,
     case when department_code like 'U%' then 'U01' else department_code end    ,
 case when department_code like 'U%' then '加工课' else department_name end 
) a 
  join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
     group by c.zone_id,
    c.zone_name,
    department_code ,
    department_name
    
    ;
 
 
create temporary table csx_tmp.temp_zone_bd_sale_02 as 
select
    c.zone_id,
    c.zone_name,
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
    sum(sale_cust)as sale_cust
   -- sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
   -- sum(all_sale_cust) as all_sale_cust
from
(
select
    province_code,a.province_name  ,
    a.division_code as     department_code ,
    a.division_name as  department_name,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as days_sale,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >=  	regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
group by 
province_code,a.province_name ,
    a.division_code  ,  
    a.division_name 
union all 
select
   province_code,a.province_name ,
    a.division_code  as  department_code ,
    a.division_name as  department_name,
    0 as days_sale,
    0 as days_profit,
    0 sale,
    0 profit,
    0 sale_cust,
    0 sale_sku,
    sum(sales_value)as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
where
   sdt >=  regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  channel in ('1','7')
group by 
    province_code,a.province_name ,
     a.division_code,  
a.division_name 
) a 
  join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
     group by c.zone_id,
    c.zone_name,
    department_code ,
    department_name
    ;
 
 
create temporary table csx_tmp.temp_zone_bd_sale_03 as 
select
    c.zone_id,
    c.zone_name,
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
    sum(sale_cust)as sale_cust
   -- sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
   -- sum(all_sale_cust) as all_sale_cust
from
(
select
    province_code,a.province_name  ,
    case when  a.division_code in('11','10') then '11' when a.division_code in('12','13','14') then '12' else  division_code end as     department_code ,
    case when  a.division_code in('11','10') then '生鲜采购部' when a.division_code in('12','13','14') then '食百采购部' else  division_name end  as  department_name,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then sales_value end )as days_sale,
    sum(case when sdt = regexp_replace(${hiveconf:edate},'-','') then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
group by 
province_code,a.province_name ,
    case when  a.division_code in('11','10') then '11' when a.division_code in('12','13','14') then '12' else  division_code end ,
case when  a.division_code in('11','10') then '生鲜采购部' when a.division_code in('12','13','14') then '食百采购部' else  division_name end  
union all 
select
   province_code,a.province_name ,
   case when  a.division_code in('11','10') then '11' when a.division_code in('12','13','14') then '12' else  division_code end as      department_code ,
   case when  a.division_code in('11','10') then '生鲜采购部' when a.division_code in('12','13','14') then '食百采购部' else  division_name end as  department_name,
    0 as days_sale,
    0 as days_profit,
    0 sale,
    0 profit,
    0 sale_cust,
    0 sale_sku,
    sum(sales_value)as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a 
where
   sdt >=  regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  channel in ('1','7')
group by 
    province_code,a.province_name ,
     case when  a.division_code in('11','10') then '11' when a.division_code in('12','13','14') then '12' else  division_code end ,
case when  a.division_code in('11','10') then '生鲜采购部' when a.division_code in('12','13','14') then '食百采购部' else  division_name end 
) a 
  join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
     group by c.zone_id,
    c.zone_name,
    department_code ,
    department_name
    ;
  
      select
    a.zone_id,
    a.zone_name,
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
    (sale_cust)/(all_sale_cust) as penetration_rate,  -- 渗透率
    row_num,
    all_sale_cust
    
from (
 select
    a.zone_id,
    a.zone_name,
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
    row_number()over(partition by a.zone_id order by sale desc) as row_num
from  csx_tmp.temp_zone_bd_sale a 

 union all 
 select
    a.zone_id,
    a.zone_name,
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
   row_number()over(partition by a.zone_id order by sale desc) as row_num
from  csx_tmp.temp_zone_bd_sale_02 a 
 union all 
  select
    a.zone_id,
    a.zone_name,
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
    row_number()over(partition by a.zone_id order by sale desc) as row_num
from  csx_tmp.temp_zone_bd_sale_03 a 
) a 
 join 
(
select
    zone_id,
    zone_name,
    count(distinct a.customer_no )as all_sale_cust
from
    csx_dw.dws_sale_r_d_customer_sale a
join 
   (select distinct dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) c on a.province_code=c.dist_code 
    where
 sdt >=  regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
    group by 
    zone_id,
    zone_name
 ) b on a.zone_id=b.zone_id
 ;
 
 
 
 -- 省区课组销售 课组销售占比、渗透率
 
 -- 课组销售
 -- set sdate='20200701';
-- set edate='20200726';
-- set l_sdate='20200601';
-- set l_edate='20200626';
-- 部类课组销售
-- select * from csx_tmp.zone_sale_01;

select
    zone_id,
    zone_name,
    a.province_code ,
    a.province_name ,
    division_code ,
    division_name,
    department_code ,
    department_name,
    -- sum(days_sale/10000)as days_sale,
    -- sum(days_profit/10000) as days_profit,
    -- sum(days_profit)/ sum(days_sale) as days_profit_rate,
    sum(sale/10000) sale,
    sum(ring_months_sale/10000) as ring_months_sale,
    sum(sale-ring_months_sale)/sum(ring_months_sale) as ring_sales_ratio,
    sum(profit/10000) profit,
    sum(profit)/sum(sale)as profit_rate,
    sum(sale_sku)as sale_sku,
    sum(sale_cust)as sale_cust,
    sum(sale_cust)/sum(all_sale_cust) as penetration_rate,  -- 渗透率
    (all_sale_cust) as all_sale_cust
from
(
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
 case when department_code like 'U%' then 'U01' else department_code end       department_code ,
  case when department_code like 'U%' then '加工课' else department_name end   department_name,
    sum(case when sdt = ${hiveconf:edate} then sales_value end )as days_sale,
    sum(case when sdt =  ${hiveconf:edate} then profit end) as days_profit,
    sum(sales_value) sale,
    sum(profit) profit,
    count(distinct a.customer_no )as sale_cust,
    count(distinct goods_code )as sale_sku,
    0 as ring_months_sale
from
    csx_dw.dws_sale_r_d_customer_sale a
where
    sdt >=   regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=   regexp_replace(${hiveconf:edate},'-','')
    and  channel in ('1','7')
group by 
province_code ,
    province_name ,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end   , 
 case when department_code like 'U%' then '加工课' else department_name end 
union all 
select
    province_code ,
    province_name ,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end  department_code  , 
 case when department_code like 'U%' then '加工课' else department_name end  department_name,
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
    sdt >=  regexp_replace(${hiveconf:l_sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:l_edate},'-','')
    and  channel in ('1','7')
group by 
    province_code ,
    province_name ,
    division_code ,
    division_name,
    case when department_code like 'U%' then 'U01' else department_code end   , 
 case when department_code like 'U%' then '加工课' else department_name end 
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
    sdt >=   regexp_replace(${hiveconf:sdate},'-','')
    and sdt <=  regexp_replace(${hiveconf:edate},'-','')
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
    department_name,
	all_sale_cust
	;


-- 商超销售查询
select
    province_code ,
    province_name,
    sales_belong_flag,
    sum(days_sale/10000 )as days_sale,
    sum(days_profit/10000) as days_profit,
    sum(days_profit)/sum(days_sale ) as days_profit_rate,
    sum(sale/10000) sale,
    sum(ring_sale/10000)  as ring_sale,
   (sum(sale)-sum(ring_sale))/sum(ring_sale) as ring_sale_ratio,
    sum(profit/10000 )profit ,
    sum(profit )/sum(sale )as profit_rate,    
    sum(ring_profit/10000)  as ring_profit
from
(
select
    province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        else sales_belong_flag
    end sales_belong_flag,
    sum(case when sdt=${hiveconf:edate} then sales_value end )as days_sale,
    sum(case when sdt=${hiveconf:edate} then profit end )as days_profit,
    sum(sales_value) sale,
    sum(profit )profit ,
    0 as ring_sale,
    0 as ring_profit
from
    csx_dw.dws_sale_r_d_customer_sale as a
left join (
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
where
    sdt >= ${hiveconf:sdate}
    and sdt <= ${hiveconf:edate}
    and channel = '2'
    and province_code in ('32','23','24')
  group by 
   province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        else sales_belong_flag
    end  
union all 
select 
 province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        else sales_belong_flag
    end sales_belong_flag,
    0 as days_sale,
    0 as days_profit,
    0 as sale,
    0 as profit ,
    sum(sales_value) ring_sale,
    sum(profit ) ring_profit 
from
    csx_dw.dws_sale_r_d_customer_sale as a
left join (
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
where
    sdt >= ${hiveconf:l_sdate}
    and sdt <= ${hiveconf:l_edate}
    and channel = '2'
    and province_code in ('32','23','24')
  group by 
   province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        else sales_belong_flag
    end  
) a 
group by 
    province_code ,
    province_name,
    sales_belong_flag;


-- 客户属性销售  
select  
       zone_id,zone_name ,
       province_code ,
       province_name ,
       attribute,
       attribute_code,
       sum(days_sale/10000 )as days_sale,
       sum(days_profit/10000) as days_profit,
       sum(days_profit)/sum(days_sale) as days_profit_rate,
       sum(sale/10000 )sale,
        sum(ring_sale/10000 ) as ring_sale,
       sum(profit/10000)profit,
       sum(profit)/sum(sale) as profit_rate,
       sum(sale_cust )as sale_cust,
       sum(sale_cust-ring_sale_cust) as diff_sale_cust,
       sum(ring_profit/10000) as ring_profit,
       sum(ring_sale_cust) as ring_sale_cust
from (
   SELECT 
       province_code ,
       province_name ,
       attribute,
       attribute_code,
       sum(case when sdt=${hiveconf:edate} then sales_value end )as days_sale,
       sum(case when sdt=${hiveconf:edate} then profit end) as days_profit,
       sum(sales_value )sale,
       sum(profit )profit,
       count(distinct a.customer_no )as sale_cust,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_cust
   FROM csx_dw.customer_sales a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate} and a.channel in('1','7')
   group by attribute,
       attribute_code,
       province_code,
     province_name
 union all 
   SELECT 
       province_code ,
       province_name ,
       attribute,
       attribute_code,
       0 as days_sale,
       0 as days_profit,
       0 as sale,
       0 as profit,
       0 as sale_cust,
       sum(sales_value)as ring_sale,
       sum(profit)as ring_profit,
       count(distinct a.customer_no)as ring_sale_cust       
   FROM csx_dw.customer_sales a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>=${hiveconf:l_sdate} and sdt<=${hiveconf:l_edate} and a.channel in('1','7')
   group by attribute,
       attribute_code,
       province_code,
     province_name
) a 
join 
(select distinct   dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) b on a.province_code=b.dist_code 
group by zone_id,zone_name ,
        province_code ,
       province_name ,
       attribute,
       attribute_code
;
  
  
 -- 客户属性销售  20200730

select  
       zone_id,zone_name ,
       province_code ,
       province_name ,
       attribute_code,
       attribute,       
       sum(days_sale/10000 )as days_sale,
       sum(days_profit/10000) as days_profit,
       sum(days_profit)/sum(days_sale) as days_profit_rate,
       sum(sale/10000 )sale,
       sum(ring_sale/10000 ) as ring_sale,
       (sum(sale)- coalesce(sum(ring_sale),0))/coalesce(sum(ring_sale),0) as mom_sale_rate,
       sum(profit/10000)profit,
       sum(profit)/sum(sale) as profit_rate,
       sum(sale_cust )as sale_cust,
       sum(sale_cust-ring_sale_cust) as diff_sale_cust,
       sum(ring_profit/10000) as ring_profit,
       sum(ring_sale_cust) as ring_sale_cust
from (
   SELECT 
       province_code ,
       province_name ,
       case when a.channel='7' then 'BBC'
            when b.attribute_code=3 then '贸易客户'
            when a.order_kind='WELFARE' then '福利客户'
            when b.attribute_code=5 then '合伙人客户'
            else '日配客户'
            end attribute,
       case when a.channel='7' then '7'
            when b.attribute_code=3 then '3'
            when a.order_kind='WELFARE' then '2'
            when b.attribute_code=5 then '5'
            else '1'
            end attribute_code,
       sum(case when sdt= '${edate}' then sales_value end )as days_sale,
       sum(case when sdt= '${edate}' then profit end) as days_profit,
       sum(sales_value )sale,
       sum(profit )profit,
       count(distinct a.customer_no )as sale_cust,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_cust
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code,
    first_category,
    first_category_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>='${sdate}' and sdt<= '${edate}' and a.channel in('1','7')
   group by case when a.channel='7' then 'BBC'
            when b.attribute_code=3 then '贸易客户'
            when a.order_kind='WELFARE' then '福利客户'
            when b.attribute_code=5 then '合伙人客户'
            else '日配客户'   end ,
       case when a.channel='7' then '7'
            when b.attribute_code=3 then '3'
            when a.order_kind='WELFARE' then '2'
            when b.attribute_code=5 then '5'
            else '1' end ,
            province_code,
            province_name
 union all 
   SELECT 
       province_code ,
       province_name ,
       case when a.channel='7' then 'BBC'
            when b.attribute_code=3 then '贸易客户'
            when a.order_kind='WELFARE' then '福利客户'
            when b.attribute_code=5 then '合伙人客户'
            else '日配客户'
            end attribute,
       case when a.channel='7' then '7'
            when b.attribute_code=3 then '3'
            when a.order_kind='WELFARE' then '2'
            when b.attribute_code=5 then '5'
            else '1'
            end attribute_code,
       0 as days_sale,
       0 as days_profit,
       0 as sale,
       0 as profit,
       0 as sale_cust,
       sum(sales_value)as ring_sale,
       sum(profit)as ring_profit,
       count(distinct a.customer_no)as ring_sale_cust       
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select
    customer_no ,
    attribute ,
    attribute_code,
    first_category,
    first_category_code
    from
    csx_dw.dws_crm_w_a_customer_m_v1
    where
    sdt = 'current') as b on a.customer_no =b.customer_no
   where sdt>= '${l_sdate}' and sdt<= '${l_edate}' and a.channel in('1','7')
   group by case when a.channel='7' then 'BBC'
            when b.attribute_code=3 then '贸易客户'
            when a.order_kind='WELFARE' then '福利客户'
            when b.attribute_code=5 then '合伙人客户'
            else '日配客户'
            end ,
       case when a.channel='7' then '7'
            when b.attribute_code=3 then '3'
            when a.order_kind='WELFARE' then '2'
            when b.attribute_code=5 then '5'
            else '1'
            end ,
       province_code,
       province_name
) a 
join 
(select distinct   dist_code,zone_id,zone_name 
from 
    csx_dw.csx_shop where sdt='current'
) b on a.province_code=b.dist_code 
group by zone_id,zone_name ,
        province_code ,
        province_name ,
       attribute,
       attribute_code
;

-- 商超查询数据 20200730
select
    province_code ,
    province_name,
    sales_belong_flag,
    sum(days_sale/10000 )as days_sale,
    sum(days_profit/10000) as days_profit,
    sum(days_profit)/sum(days_sale ) as days_profit_rate,
    sum(sale/10000) sale,
    sum(ring_sale/10000)  as ring_sale,
   (sum(sale)-sum(ring_sale))/sum(ring_sale) as ring_sale_ratio,
    sum(profit/10000 )profit ,
    sum(profit )/sum(sale )as profit_rate,
    
    sum(ring_profit/10000)  as ring_profit
from
(
select
    province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end sales_belong_flag,
    sum(case when sdt='${edate}' then sales_value end )as days_sale,
    sum(case when sdt='${edate}' then profit end )as days_profit,
    sum(sales_value) sale,
    sum(profit )profit ,
    0 as ring_sale,
    0 as ring_profit
from
    csx_dw.dws_sale_r_d_customer_sale as a
left join (
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
where
    sdt >= '${sdate}'
    and sdt <= '${edate}'
    and channel = '2'
    and province_code in ('32','23','24')
  group by 
   province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end  
union all 
select 
 province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end  sales_belong_flag,
    0 as days_sale,
    0 as days_profit,
    0 as sale,
    0 as profit ,
    sum(sales_value) ring_sale,
    sum(profit ) ring_profit 
from
    csx_dw.dws_sale_r_d_customer_sale as a
left join (
    select
        concat('S', shop_id)shop_id, sales_belong_flag
    from
        csx_dw.dws_basic_w_a_csx_shop_m a
    where
        sdt = 'current') b on
    a.customer_no = shop_id
where
    sdt >= '${l_sdate}'
    and sdt <= '${l_edate}'
    and channel = '2'
    and province_code in ('32','23','24')
  group by 
   province_code ,
    province_name,
    case
        when customer_no in ('103097', '103903','104842') then '红旗/中百'
        when sales_belong_flag in ('2_云创会员店','6_云创到家') then '2_云创永辉生活' else sales_belong_flag
    end 
) a 
group by 
    province_code ,
    province_name,
    sales_belong_flag;
	
 
-- 销售主管数据


select *
from
    (select region_name,
        coalesce(province_name,'合计') as province_name,
        -- coalesce(city_group_name,'合计') as city_group_name,
        -- coalesce(channel_name_1,'合计') as channel_name_1,
        -- if(sale_group is null,'合计',third_supervisor_name) as third_supervisor_name,
        coalesce(first_supervisor_name,'合计') as first_supervisor_name,
        -- coalesce(sale_group,'合计') sale_group,
        old_Md_sales_value,
        old_M_sales_value,
        old_M_profit,
        old_H_sales_value,
        old_M_profit/old_M_sales_value as old_M_prorate,
        (old_M_sales_value/old_H_sales_value-1) as old_H_sale_rate,
        new_cust_count,
        new_Md_sales_value,
        new_M_sales_value,
        new_M_profit,
        new_H_sales_value,
        new_M_profit/new_M_sales_value as new_M_prorate,
        (new_M_sales_value/new_H_sales_value-1) as new_H_sale_rate,
        ALL_Md_sales_value,
        ALL_M_sales_value,
        ALL_M_profit,
        ALL_H_sales_value,
        ALL_M_profit/ALL_M_sales_value as ALL_M_prorate,
        (ALL_M_sales_value/ALL_H_sales_value-1) as ALL_H_sale_rate,
       -- case when city_group_name='-' and channel_name_1 is null then '是' else '否' end is_delete,
        GROUPING__ID 
    from
        (select region_name,
            province_name,
            -- city_group_name,
            -- channel_name_1,
            -- third_supervisor_name,
            first_supervisor_name,
            -- sale_group,
            coalesce(sum(case when smonth='本月' and is_new_sale='否' then Md_sales_value end)/10000,0) as old_Md_sales_value, --老客-昨日销售额
            coalesce(sum(case when smonth='本月' and is_new_sale='否' then sales_value end)/10000,0) as old_M_sales_value,  --老客-累计销售额
            coalesce(sum(case when smonth='本月' and is_new_sale='否' then profit end)/10000,0) as old_M_profit,  --老客-累计毛利额
            coalesce(sum(case when smonth='环比月' and is_new_sale='否' then sales_value end)/10000,0) as old_H_sales_value,  --老客-环比累计销售额
            coalesce(count(distinct case when smonth='本月' and is_new_sale='是' then customer_no end),0)as new_cust_count,  --新客-累计客户数
            coalesce(sum(case when smonth='本月' and is_new_sale='是' then Md_sales_value end)/10000,0) as new_Md_sales_value, --新客-昨日销售额
            coalesce(sum(case when smonth='本月' and is_new_sale='是' then sales_value end)/10000,0) as new_M_sales_value,  --新客-累计销售额
            coalesce(sum(case when smonth='本月' and is_new_sale='是' then profit end)/10000,0) as new_M_profit,  --新客-累计毛利额
            coalesce(sum(case when smonth='环比月' and is_new_sale='是' then sales_value end)/10000,0) as new_H_sales_value,  --新客-环比累计销售额
            coalesce(sum(case when smonth='本月' then Md_sales_value end)/10000,0) as ALL_Md_sales_value, --汇总-昨日销售额
            coalesce(sum(case when smonth='本月' then sales_value end)/10000,0) as ALL_M_sales_value,  --汇总-累计销售额
            coalesce(sum(case when smonth='本月' then profit end)/10000,0) as ALL_M_profit,  --汇总-累计毛利额
            coalesce(sum(case when smonth='环比月' then sales_value end)/10000,0) as ALL_H_sales_value , --汇总-环比累计销售额
            GROUPING__ID 
        from (SELECT region_code,
                     region_name,
                     province_code,
                     province_name,
                     city_group_code,
                     city_group_name,
                     channel,
                     channel_name,
                     third_supervisor_name,
                     coalesce(first_supervisor_name,'')as first_supervisor_name,
                     customer_no,
                     customer_name,
                     province_manager_id,
                     province_manager_name,
                     city_group_manager_id,
                     city_group_manager_name,
                     order_kind,
                     sales_belong_flag,
                     is_partner,
                     attribute_0,
                     ascription_type_name,
                     sale_group,
                     is_new_sale,
                     coalesce(Md_sales_value,0)as Md_sales_value, --昨日销售额
                     coalesce(sales_value,0)as sales_value,
                     coalesce(profit,0) as profit,
                     smonth,
                     sdt,
                     case when channel_name='商超' then 'M端'
							when channel_name='大客户' or channel_name like '企业购%' then 'B端'
							else '其他' end channel_name_1
                FROM csx_tmp.tmp_supervisor_day_detail
                )a
            where channel_name_1 ='B端'
            group by region_name,
                 province_name,
                 city_group_name,
                 -- channel_name_1,
                 -- third_supervisor_name,
                 first_supervisor_name
                 -- sale_group
             grouping sets((region_name),
                      (region_name,province_name),
                      (region_name,province_name,first_supervisor_name))
        )a
    )a  
where 1=1
  -- is_delete='否'
;