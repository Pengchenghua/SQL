-- 第四季度销售达成
SET hive.execution.engine=spark; 
--set tez.queue.name=caishixian;
set edate='${edt}';
set sdt= to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')) ;
set last_edate = add_months(${hiveconf:edate},-3);
set last_sdt = add_months(to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')),-3);
 
-- 新客销售分析
-- channel_code in ('1','9','7')

SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       --sum(plan_sale)plan_sale,
       sales_value,
       --sum(sales_value/10000)/sum(plan_sale) as sale_fill_rate,
       last_sale,
       coalesce((sales_value-last_sale)/last_sale,0) sale_growth_rate,
       daily_cust_sale,
       daily_match_sale,
       coalesce(daily_cust_sale/sales_value,0) daily_cust_ratio,
       coalesce(daily_match_sale/sales_value,0)daily_match_ratio,
       0 new_cust_ratio,
       --sum(plan_profit)plan_profit,
       profit,
       profit_rate,
       --sum(last_profit/10000)last_profit,
       plan_cust_num,
       sale_cust_num,
       coalesce(sale_cust_num/plan_cust_num ,0) cust_fill_rate,
       last_sale_cust_num,
       (sale_cust_num)-(last_sale_cust_num)  as diff_cust_num,
       sale_daily_cust_num,
       daily_order_cust,
       (sale_daily_cust_num)/(sale_cust_num) as daily_cust_num_ratio,
       (daily_order_cust)/(sale_cust_num) as daily_order_cust_ratio
from
(
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       --sum(plan_sale)plan_sale,
       sum(sales_value/10000)sales_value,
       --sum(sales_value/10000)/sum(plan_sale) as sale_fill_rate,
       sum(last_sale/10000)last_sale,
       --(sum(sales_value/10000)-sum(last_sale/10000))/sum(last_sale/10000) as sale_growth_rate,
       sum(daily_cust_sale/10000) as daily_cust_sale,
       sum(daily_match_sale/10000) as daily_match_sale,
       --coalesce(sum(daily_cust_sale/sales_value),0) as daily_cust_ratio,
       --coalesce(sum(daily_match_sale/sales_value),0) as daily_match_ratio,
       0 new_cust_ratio,
       --sum(plan_profit)plan_profit,
       sum(profit/10000)profit,
       sum(profit)/sum(sales_value) as profit_rate,
       --sum(last_profit/10000)last_profit,
       sum(plan_cust_num)plan_cust_num,
       sum(sale_cust_num)sale_cust_num,
       --sum(sale_cust_num)/sum(plan_cust_num) as cust_fill_rate,
       sum(last_sale_cust_num)last_sale_cust_num,
       sum(sale_cust_num)-sum(last_sale_cust_num)  as diff_cust_num,
       sum(sale_daily_cust_num)sale_daily_cust_num,
       sum(daily_order_cust)daily_order_cust
      -- sum(sale_daily_cust_num)/sum(sale_cust_num) as daily_cust_num_ratio,
      -- sum(daily_order_cust)/sum(sale_cust_num) as daily_order_cust_ratio
from
    (SELECT 
          province_code,
          sum(sales_value) as sales_value,
          sum(profit) as profit,
          sum(case when attribute_code ='1' then a.sales_value end ) as daily_cust_sale,  --日配客户
          sum(case when a.business_type_code ='1' then a.sales_value end ) as daily_match_sale,  --日配单销售额
          0 last_sale,
          0 last_profit,
          count(DISTINCT a.customer_no) as sale_cust_num,
          count(distinct case when attribute_code ='1' then a.customer_no end ) as sale_daily_cust_num,  --日配客户数
          count(DISTINCT case when a.business_type_code='1' then a.customer_no end ) as daily_order_cust,    --日配单客户数
          0 last_sale_cust_num,
          0 plan_cust_num
   FROM csx_dw.dws_sale_r_d_detail a 
   join 
   (select customer_no,
           first_order_date,
           last_order_date 
    from csx_dw.dws_crm_w_a_customer_active 
        where  sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_order_date>=regexp_replace(${hiveconf:sdt},'-','')
            and first_order_date<=regexp_replace(${hiveconf:edate},'-','')
   ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and a.channel_code in ('7','1','9')
     and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
   GROUP BY  
            province_code
   UNION ALL SELECT province_code,
                    0 sales_value,
                    0 profit,
                    0 daily_cust_sale,
                    0 daily_match_sale,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    0 sale_daily_cust_num,
                    0 daily_order_cust,
                    count(DISTINCT a.customer_no) AS last_sale_cust_num,
                    0 plan_cust_num
   FROM csx_dw.dws_sale_r_d_detail a 
   join 
   (select customer_no,
           first_order_date,
           last_order_date 
    from csx_dw.dws_crm_w_a_customer_active 
        where  sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_order_date>=regexp_replace(${hiveconf:last_sdt},'-','')
            and first_order_date<=regexp_replace(${hiveconf:last_edate},'-','')
   ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and a.channel_code in ('7','1','9')
    and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
   GROUP BY  province_code
   union all 
   select dist_code as province_code,
        0 sales_value,
        0 profit,
        0 daily_cust_sale,
        0 daily_match_sale,
        0 last_sale,
        0 last_profit,
        0 sale_cust_num,
        0 sale_daily_cust_num,
        0 daily_order_cust,
        0 AS last_sale_cust_num,
        sum(plan_cust_num)plan_cust_num
    from csx_tmp.ads_sale_plan_national 
    where quarters='202004'and plan_type_code='5'
    GROUP BY dist_code
    )a
LEFT JOIN
  (SELECT province_code,
          region_code,
          region_name,
          province_name
   FROM csx_dw.dim_area
   WHERE area_rank =13) c ON a.province_code =c.province_code
GROUP BY 
         region_code,
         region_name,
         a.province_code,
         province_name
GROUPING
SETS ((region_code,
       region_name), (region_code,
                  region_name,
                  a.province_code,
                  province_name)
                  )
) a 
ORDER BY case when  region_code='2' then 1
             when region_code='4' then 2
             when region_code ='1' then 3
             when region_code ='3' then 4 end asc,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    ;
    

-- 渠道销售
-- and (a.customer_name not like '%内购%' or a.customer_name not like '%内%购%') and 
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       plan_sale_value,
       sales_value,
       coalesce(sales_value/plan_sale_value,0) as sale_fill_rate,
       last_sale,
       coalesce((sales_value-last_sale)/last_sale,0) as sale_growth_rate,
       plan_profit,
       profit,
       coalesce(profit/plan_profit,0) as profit_fill_rate,
       profit/sales_value as profit_rate,
       last_profit,
       last_profit/last_sale as last_profit_tate,
       coalesce(profit/sales_value-last_profit/last_sale,0) as diff_profit_rate,
       sale_cust_num,
       last_sale_cust_num,
       coalesce(sale_cust_num-last_sale_cust_num,0) diff_sale_cust_num
from (
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sales_value,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num,
       sum(plan_sale_value)plan_sale_value,
       sum(plan_profit)plan_profit
FROM
  (SELECT CASE
              WHEN a.channel_code IN ('1', '7','9') THEN '1'
              ELSE a.channel_code
          END channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT  customer_no  ) sale_cust_num,
          0 last_sale_cust_num,
          0 plan_sale_value,
          0 plan_profit
   FROM csx_dw.dws_sale_r_d_detail  a
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     -- and business_type_code !='3'  --剔除批发内购
      and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
   GROUP BY CASE
                WHEN a.channel_code IN ('1', '7', '9') THEN '1'
                ELSE a.channel_code
            END,
            province_code
   UNION ALL 
   SELECT case  when a.channel_code in ('1', '7', '9') THEN '1'
             else a.channel_code
            end channel,
            province_code,
            0 sales_value,
            0 profit,
            sum(sales_value ) last_sale,
            sum(profit ) last_profit,
            0 sale_cust_num,
            count(DISTINCT  customer_no ) AS last_sale_cust_num,
            0 plan_sale_value,
            0 plan_profit
   FROM csx_dw.dws_sale_r_d_detail a 
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
    --   and business_type_code !='3'  --剔除批发内购
   GROUP BY CASE
                WHEN a.channel_code IN ('1', '7','9') THEN '1'
                ELSE a.channel_code
            END,
            province_code
    union all 
   select channel_code as channel,
         province_code,
        0 sales_value,
        0 profit,
        0 last_sale,
        0 last_profit,
        0 sale_cust_num,
        0 AS last_sale_cust_num,
        sum(plan_sale_value)plan_sale_value,
        sum(plan_profit)    plan_profit
    from csx_tmp.dws_all_format_plan_sales 
    where month BETWEEN '202010' and '202012'
     --   and attribute_code not in ('99','3')
    GROUP BY province_code ,
    channel_code
  )a
LEFT JOIN
  (SELECT province_code,
          region_code,
          region_name,
          province_name
   FROM csx_dw.dim_area
   WHERE area_rank =13) c ON a.province_code =c.province_code
GROUP BY channel,
         region_code,
         region_name,
         a.province_code,
         province_name
GROUPING
SETS ((region_code,
       region_name,
       channel), (region_code,
                  region_name,
                  channel,
                  a.province_code,
                  province_name)) 
) a 
ORDER BY case when  region_code='2' then 1
             when region_code='4' then 2
             when region_code ='1' then 3
             when region_code ='3' then 4 end asc,
             channel,
    case when a.province_code in ('2','20') then '01' 
    when a.province_code='1' then '9' else a.province_code end desc 
      ;
    
                
--- BBC、 福利单
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       plan_sale_value,
       sales_value,
       coalesce(sales_value/plan_sale_value,0) as sale_fill_rate,
       last_sale,
       coalesce((sales_value-last_sale)/last_sale,0) as sale_growth_rate,
       plan_profit,
       profit,
       profit/plan_profit as profit_fill_rate,
       profit/sales_value as profit_rate,
       -- last_profit,
       sale_cust_num,
       last_sale_cust_num,
       sale_cust_num-last_sale_cust_num as diff_sale_cust_num
FROM
  (
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sales_value,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num,
       sum( plan_sale_value)plan_sale_value,
       sum(plan_profit)plan_profit
FROM
  (SELECT a.business_type_name channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT   a.customer_no   )AS sale_cust_num,
          0 last_sale_cust_num,
          0 plan_sale_value,
          0 plan_profit
   FROM csx_dw.dws_sale_r_d_detail a 
  WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
   GROUP BY  business_type_name  ,
            province_code
   UNION ALL 
   SELECT business_type_name channel,
            province_code,
            0 sales_value,
            0 profit,
            sum(sales_value ) last_sale,
            sum(profit  ) last_profit,
            0 sale_cust_num,
            count(DISTINCT    a.customer_no   ) AS last_sale_cust_num,
            0 plan_sale_value,
            0 plan_profit
   FROM  csx_dw.dws_sale_r_d_detail a
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
     and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
   GROUP BY business_type_name,
            province_code
    union all 
    select attribute_name channel,
        province_code,
        0 sales_value,
        0 profit,
        0 last_sale,
        0 last_profit,
        0 sale_cust_num,
        0 AS last_sale_cust_num,
        sum(plan_sale_value)plan_sale_value,
        sum(plan_profit)    plan_profit
    from csx_tmp.dws_all_format_plan_sales 
    where month BETWEEN '202010' and '202012' 
    and channel_code='1'
    and attribute_code in('7','2')
    GROUP BY attribute_name ,
    province_code
    )a
LEFT JOIN
  (SELECT province_code,
          region_code,
          region_name,
          province_name
   FROM csx_dw.dim_area
   WHERE area_rank =13) c ON a.province_code =c.province_code
GROUP BY channel,
         region_code,
         region_name,
         a.province_code,
         province_name
GROUPING
SETS ((region_code,
       region_name,
       channel), (region_code,
                  region_name,
                  channel,
                  a.province_code,
                  province_name)) 
)a
ORDER BY  case when  region_code='2' then 1
             when region_code='4' then 2
             when region_code ='1' then 3
             when region_code ='3' then 4 end asc,
             channel,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc 
;
      

-- 全渠道销售
SELECT region_code,
       region_name,
       a.province_code,
       province_name,
       plan_sale_value,
       sales_value,
       coalesce(sales_value/plan_sale_value,0) as sale_fill_rate,
       last_sale,
       coalesce((sales_value-last_sale)/last_sale,0) as sale_growth_rate,
       plan_profit,
       profit,
       coalesce(profit/plan_profit,0) as profit_fill_rate,
       coalesce(profit/sales_value,0) as profit_rate
       -- last_profit 
       
FROM
  (
SELECT region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sales_value,
       sum(profit/10000)profit,
       sum(last_sale/10000)last_sale,
       sum(last_profit/10000)last_profit,
       sum(plan_sale_value)plan_sale_value,
       sum(plan_profit)    plan_profit
FROM
  (SELECT  case when province_code in ('35','36') then '35' else province_code end province_code,
          sum(sales_value)sales_value,
          sum( profit) profit,
          0 last_sale,
          0 last_profit,
          0 plan_sale_value,
          0 plan_profit
   FROM  csx_dw.dws_sale_r_d_detail a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    -- and a.business_type_name !='批发内购'
     and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
    group by case when province_code in ('35','36') then '35' else province_code end 
   UNION ALL 
   SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value ) last_sale,
                    sum(profit ) last_profit,
                    0 plan_sale_value,
                    0 plan_profit
   FROM  csx_dw.dws_sale_r_d_detail a
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    -- and a.business_type_name !='批发内购'
     and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
				'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
    group by case when province_code in ('35','36') then '35' else province_code end
    union all 
    select
        province_code,
        0 sales_value,
        0 profit,
        0 last_sale,
        0 last_profit,
        sum(plan_sale_value)plan_sale_value,
        sum(plan_profit)    plan_profit
    from csx_tmp.dws_all_format_plan_sales 
    where month BETWEEN '202010' and '202012'  
    -- and attribute_code not in ('99','3')
    GROUP BY province_code
    )a
LEFT JOIN
  (SELECT province_code,
          region_code,
          region_name,
          province_name
   FROM csx_dw.dim_area
   WHERE area_rank =13) c ON a.province_code =c.province_code
GROUP BY region_code,
         region_name,
         a.province_code,
         province_name
GROUPING
SETS ((region_code,
       region_name), (region_code,
                      region_name,
                      a.province_code,
                      province_name)) 
)a
ORDER BY case when  region_code='2' then 1
             when region_code='4' then 2
             when region_code ='1' then 3
             when region_code ='3' then 4 
             when region_code ='5' then 5
             when region_code ='6' then 6
             end asc,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;


 
-- 亏损客户
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sales_value,
       last_sale,
       coalesce((sales_value-last_sale)/last_sale ,0) as sale_growth_rate,
       profit,
       last_profit,
       coalesce((profit-last_profit)/(last_profit) ,0) as sale_growth_rate,
       coalesce(profit/sales_value,0) as profit_rate,
       coalesce(last_profit/last_sale,0) as last_profit_rate,
       coalesce( profit/sales_value-last_profit/last_sale,0) as diff_profit_rate,
       sale_cust_num,
       last_sale_cust_num,
       sale_cust_num-last_sale_cust_num as diff_sale_cust_num,
       (sale_cust_num-last_sale_cust_num)/last_sale_cust_num as cust_growth_rate
FROM
  (
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sales_value,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (
  select province_code,
        sum(sales_value) sales_value,
        sum(profit) profit,
        0 last_sale,
        0 last_profit,
        count( DISTINCT a.customer_no) sale_cust_num,
        0 AS last_sale_cust_num
    from 
    (SELECT substr(sdt,1,6) mon,
          province_code,
          a.customer_no,
          sum(sales_value)sales_value,
          sum(profit) profit
   FROM  csx_dw.dws_sale_r_d_detail a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel_code in ('7','1','9')
    and a.business_type_name !='批发内购'
    -- and a.customer_no not in ('104444','102998')
   GROUP BY  
            substr(sdt,1,6) ,
          province_code,
          a.customer_no
    )a 
    where profit <-100
    and sales_value>0
    group by province_code
   UNION ALL 
   select province_code,
        0 sales_value,
        0 profit,
        sum(last_sale) last_sale,
        sum(last_profit) last_profit,
        0 sale_cust_num,
        count(DISTINCT a.customer_no) AS last_sale_cust_num
   from (
   SELECT substr(sdt,1,6) mon,
                    province_code,
                    a.customer_no,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit
   FROM  csx_dw.dws_sale_r_d_detail a 
    WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and a.channel_code in ('7','1','9')
     and a.business_type_name !='批发内购'
   --  and a.return_flag !='X'
  -- and a.customer_no not in ('104444','102998')
   GROUP BY  province_code,
            customer_no,
            substr(sdt,1,6)
    
   -- and a.customer_no not in ('104444','102998')
   
            )a 
    where last_profit<-100
    AND last_sale>0
     group by province_code
    )a
    
LEFT JOIN
  (SELECT province_code,
          region_code,
          region_name,
          province_name
   FROM csx_dw.dim_area
   WHERE area_rank =13) c ON a.province_code =c.province_code
GROUP BY 
         region_code,
         region_name,
         a.province_code,
         province_name
GROUPING
SETS ((region_code,
       region_name), (region_code,
                  region_name,
                  a.province_code,
                  province_name))
) a 
ORDER BY case when  region_code='2' then 1
             when region_code='4' then 2
             when region_code ='1' then 3
             when region_code ='3' then 4 
             when region_code ='5' then 5
             when region_code ='6' then 6
             end asc ,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    ;


-- B端自营销售
select sum(sales_value)sales_value,
    sum(profit)profit,
     sum(profit)/sum(sales_value) profit_rate 
from csx_tmp.sale_01 
where sale_group !='城市服务商' 
and channel in ('1','7','9')
and sdt>='20201001' and sdt<='20201130'
;


--较验总数
select sum(sales_value)sales_value,
    sum(profit)profit,
     sum(profit)/sum(sales_value) profit_rate 
from csx_tmp.sale_01 
where 1=1
and sdt>='20201001' and sdt<='20201130'
;