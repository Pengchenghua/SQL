SET hive.execution.engine=tez; 
set tez.queue.name=caishixian;
set edate='${edt}';
set sdt= to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')) ;
set last_edate = add_months(${hiveconf:edate},-3);
set last_sdt = add_months(to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')),-3);
   
-- select ${hiveconf:sdt},${hiveconf:last_edate},${hiveconf:last_sdt},${hiveconf:edate};


-- 新客销售分析

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
          sum(sales_value)sales_value,
          sum(profit) profit,
          sum(case when b.attribute_code ='1' then a.sales_value end )AS daily_cust_sale,
          sum(case when b.attribute_code in ('1','2') and order_kind !='WELFARE' then a.sales_value end )AS daily_match_sale,
          0 last_sale,
          0 last_profit,
          count(DISTINCT a.customer_no) AS sale_cust_num,
          count(distinct case when b.attribute_code ='1' then a.customer_no end )AS sale_daily_cust_num,
          count(DISTINCT case when b.attribute_code in ('1','2') and order_kind !='WELFARE' then a.customer_no end )AS daily_order_cust,
          0 last_sale_cust_num,
          0 plan_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select a.customer_no,first_sale_day,attribute,attribute_code 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q a 
    join 
    (select customer_no,attribute,attribute_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current')b on a.customer_no=b.customer_no
        where sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_sale_day>=regexp_replace(${hiveconf:sdt},'-','')
            AND first_sale_day<=regexp_replace(${hiveconf:edate},'-','')  ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('7','1')
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
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select a.customer_no,first_sale_day,attribute,attribute_code 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q a 
    join 
    (select customer_no,attribute,attribute_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current')b on a.customer_no=b.customer_no
        where sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_sale_day>=regexp_replace(${hiveconf:last_sdt},'-','')
            AND first_sale_day<=regexp_replace(${hiveconf:last_edate},'-','')  ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and channel in ('7','1')
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
    where quarters='202003'and plan_type_code='5'
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
              WHEN channel IN ('1',
                               '7',
                               '9') THEN '1'
              ELSE channel
          END channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT case when a.return_flag!='X'  then  customer_no end ) sale_cust_num,
          0 last_sale_cust_num,
          0 plan_sale_value,
          0 plan_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code
   UNION ALL 
   SELECT CASE
                        WHEN channel IN ('1',
                                         '7',
                                         '9') THEN '1'
                        ELSE channel
                    END channel,
                    province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value ) last_sale,
                    sum(profit ) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT case when  a.return_flag!='X' then  customer_no end ) AS last_sale_cust_num,
                    0 plan_sale_value,
                    0 plan_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
     and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046')or a.order_no is null)
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code
    union all 
   select channel_code channel,
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
    where month BETWEEN '202007' and '202009'
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
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    
                
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
  (SELECT case when channel='7' then 'BBC' 
                when (b.attribute_code in ('1','2') and  order_kind='WELFARE') then '福利单'
                end channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT case when a.return_flag !='X' then  a.customer_no end )AS sale_cust_num,
          0 last_sale_cust_num,
          0 plan_sale_value,
          0 plan_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a
   join (select customer_no,attribute_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current') b on a.customer_no=b.customer_no
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
   --  and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when (b.attribute_code in ('1','2') and  order_kind='WELFARE') then '福利单'
                end
            ,
            province_code
   UNION ALL 
   SELECT case when channel='7' then 'BBC' 
                when (b.attribute_code in ('1','2') and  order_kind='WELFARE') then '福利单'
                end channel,
            province_code,
            0 sales_value,
            0 profit,
            sum(sales_value ) last_sale,
            sum(profit  ) last_profit,
            0 sale_cust_num,
            count(DISTINCT case when a.return_flag !='X' then  a.customer_no end ) AS last_sale_cust_num,
            0 plan_sale_value,
            0 plan_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a
   join (select customer_no,attribute_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current') b on a.customer_no=b.customer_no
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046')or a.order_no is null )
   -- and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when (b.attribute_code in ('1','2') and  order_kind='WELFARE') then '福利单'
                end
            ,
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
    where month BETWEEN '202007' and '202009' 
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
  (SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
          sum(sales_value)sales_value,
          sum( profit) profit,
          0 last_sale,
          0 last_profit,
          0 plan_sale_value,
          0 plan_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    group by case when province_code in ('35','36') then '35' else province_code end
   UNION ALL 
   SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value ) last_sale,
                    sum(profit ) last_profit,
                    0 plan_sale_value,
                    0 plan_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
     and ( order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046')or a.order_no is null)
    -- and channel in ('1','7','9')
    group by 
    case when province_code in ('35','36') then '35' else province_code end
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
    where month BETWEEN '202007' and '202009'
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


 
-- 亏损
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
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('7','1','9')
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
   FROM csx_dw.dws_sale_r_d_customer_sale a 
    WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and channel in ('7','1','9')
   --  and a.return_flag !='X'
   and a.customer_no not in ('104444','102998')
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


--------------------------------------------------------------------------------------------分割线------------------------------------------------------------------------------



SET hive.execution.engine=tez; -- set hive.execution.engine=mr;

-- 全渠道销售

SELECT region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(profit/10000)profit,
       sum(last_sale/10000)last_sale,
       sum(last_profit/10000)last_profit
FROM
  (SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
          sales_value,
          profit,
          0 last_sale,
          0 last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200701'
     AND sdt<='20200820'
     and a.customer_no not in ('104444','102998')
   UNION ALL SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
                    0 sales_value,
                    0 profit,
                    sales_value last_sale,
                    profit last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>='20200401'
     AND sdt<='20200520' 
     and a.customer_no not in ('104444','102998') )a
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
                      province_name)) ;

-- 渠道数据

SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT CASE
              WHEN channel IN ('1',
                               '7',
                               '9') THEN '1'
              ELSE channel
          END channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.customer_sales
   WHERE sdt>='20200701'
     AND sdt<='20200820'
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code
   UNION ALL SELECT CASE
                        WHEN channel IN ('1',
                                         '7',
                                         '9') THEN '1'
                        ELSE channel
                    END channel,
                    province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT customer_no) AS last_sale_cust_num
   FROM csx_dw.customer_sales
   WHERE sdt>='20200401'
     AND sdt<='20200520'
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code)a
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
                  province_name)) ;
                  
                  
--- BBC、 福利单
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>='20200701'
     AND sdt<='20200820'
   --  and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end
            ,
            province_code
   UNION ALL SELECT case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end channel,
                    province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>='20200401'
     AND sdt<='20200520'
   -- and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end
            ,
            province_code)a
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
                  province_name)) ;

-- 新客销售分析
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT 
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT a.customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select customer_no,first_sale_day 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q 
        where sdt='20200820' and first_sale_day>='20200701'
            AND first_sale_day<='20200820'  ) b on a.customer_no=b.customer_no  
   WHERE sdt>='20200701'
     AND sdt<='20200820'
     and channel in ('7','1')
   GROUP BY  
            province_code
   UNION ALL SELECT province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT a.customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select customer_no,first_sale_day 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q 
        where sdt='20200820' and first_sale_day>='20200401'
            AND first_sale_day<='20200520'  ) b on a.customer_no=b.customer_no  
   WHERE sdt>='20200401'
     AND sdt<='20200520'
    and channel in ('7','1')
   GROUP BY  province_code
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
                  province_name)) ;


-- 亏损分析 剔除退货、'104444','102998' 两个大
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
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
        count( a.customer_no) sale_cust_num,
        0 AS last_sale_cust_num
    from 
    (SELECT substr(sdt,1,6) mon,
          province_code,
          a.customer_no,
          sum(sales_value)sales_value,
          sum(profit) profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200701'
     AND sdt<='20200820'
     and channel in ('7','1','9')
     and a.customer_no not in ('104444','102998')
   GROUP BY  
            substr(sdt,1,6) ,
          province_code,
          a.customer_no
    )a 
    where profit <-100
    group by province_code
   UNION ALL 
   select province_code,
        0 sales_value,
        0 profit,
        sum(last_sale) last_sale,
        sum(last_profit) last_profit,
        0 sale_cust_num,
        count( a.customer_no) AS last_sale_cust_num
   from (
   SELECT substr(sdt,1,6) mon,
                    province_code,
                    a.customer_no,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
    WHERE sdt>='20200401'
     AND sdt<='20200520'
    and channel in ('7','1','9')
   --  and a.return_flag !='X'
    and a.customer_no not in ('104444','102998')
   GROUP BY  province_code,
            customer_no,
            substr(sdt,1,6)
            )a 
    where last_profit<-100
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
                  province_name)) ;


-----------------------------------------------引参---------------------------------



SET hive.execution.engine=tez; 
-- set hive.execution.engine=mr;
set edate=to_date(date_sub(current_date,1));
set sdt= to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')) ;
set last_edate = add_months(${hiveconf:edate},-3);
set last_sdt = add_months(to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')),-3);

-- select ${hiveconf:edate},${hiveconf:sdt},${hiveconf:last_edate},${hiveconf:last_sdt};
-- 全渠道销售

SELECT region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(profit/10000)profit,
       sum(last_sale/10000)last_sale,
       sum(last_profit/10000)last_profit
FROM
  (SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
          sales_value,
          profit,
          0 last_sale,
          0 last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    -- and a.customer_no not in ('104444','102998')
   UNION ALL SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
                    0 sales_value,
                    0 profit,
                    sales_value last_sale,
                    profit last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    -- and a.customer_no not in ('104444','102998') )a
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
                      province_name)) ;

-- 渠道数据

SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT CASE
              WHEN channel IN ('1',
                               '7',
                               '9') THEN '1'
              ELSE channel
          END channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.customer_sales
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code
   UNION ALL SELECT CASE
                        WHEN channel IN ('1',
                                         '7',
                                         '9') THEN '1'
                        ELSE channel
                    END channel,
                    province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT customer_no) AS last_sale_cust_num
   FROM csx_dw.customer_sales
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code)a
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
                  province_name)) ;
                  
                  
--- BBC、 福利单
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
   --  and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end
            ,
            province_code
   UNION ALL SELECT case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end channel,
                    province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
   -- and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end
            ,
            province_code)a
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
                  province_name)) ;

-- 新客销售分析
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT 
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT a.customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select customer_no,first_sale_day 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q 
        where sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_sale_day>=regexp_replace(${hiveconf:sdt},'-','')
            AND first_sale_day<=regexp_replace(${hiveconf:edate},'-','')  ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('7','1')
   GROUP BY  
            province_code
   UNION ALL SELECT province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT a.customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select customer_no,first_sale_day 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q 
        where sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_sale_day>=regexp_replace(${hiveconf:last_sdt},'-','')
            AND first_sale_day<=regexp_replace(${hiveconf:last_edate},'-','') 
    ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and channel in ('7','1')
   GROUP BY  province_code
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
                  province_name)) ;


-- 亏损分析 剔除退货、'104444','102998' 两个大
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
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
        count( a.customer_no) sale_cust_num,
        0 AS last_sale_cust_num
    from 
    (SELECT substr(sdt,1,6) mon,
          province_code,
          a.customer_no,
          sum(sales_value)sales_value,
          sum(profit) profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('7','1','9')
     and a.customer_no not in ('104444','102998')
   GROUP BY  
            substr(sdt,1,6) ,
          province_code,
          a.customer_no
    )a 
    where profit <-100
    group by province_code
   UNION ALL 
   select province_code,
        0 sales_value,
        0 profit,
        sum(last_sale) last_sale,
        sum(last_profit) last_profit,
        0 sale_cust_num,
        count( a.customer_no) AS last_sale_cust_num
   from (
   SELECT substr(sdt,1,6) mon,
                    province_code,
                    a.customer_no,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
    WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and channel in ('7','1','9')
   --  and a.return_flag !='X'
    and a.customer_no not in ('104444','102998')
   GROUP BY  province_code,
            customer_no,
            substr(sdt,1,6)
            )a 
    where last_profit<-100
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
                  province_name)) ;



---------------------------- 分割，不剔除北京两个

SET hive.execution.engine=tez; 
set tez.queue.name=caishixian;
set edate=to_date(date_sub(current_date,1));
set sdt= to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')) ;
set last_edate = add_months(${hiveconf:edate},-3);
set last_sdt = add_months(to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')),-3);

-- select ${hiveconf:edate},${hiveconf:sdt},${hiveconf:last_edate},${hiveconf:last_sdt};
-- 全渠道销售

SELECT region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(profit/10000)profit,
       sum(last_sale/10000)last_sale,
       sum(last_profit/10000)last_profit
FROM
  (SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
          sales_value,
          profit,
          0 last_sale,
          0 last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    -- and a.customer_no not in ('104444','102998')
   UNION ALL SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
                    0 sales_value,
                    0 profit,
                    sales_value last_sale,
                    profit last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    -- and a.customer_no not in ('104444','102998') 
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
ORDER BY region_code asc,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;

-- 渠道数据

SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT CASE
              WHEN channel IN ('1',
                               '7',
                               '9') THEN '1'
              ELSE channel
          END channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code
   UNION ALL SELECT CASE
                        WHEN channel IN ('1',
                                         '7',
                                         '9') THEN '1'
                        ELSE channel
                    END channel,
                    province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code)a
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
ORDER BY region_code asc,channel,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
                  
                  
--- BBC、 福利单
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end channel,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
   --  and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end
            ,
            province_code
   UNION ALL SELECT case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end channel,
                    province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
   -- and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end
            ,
            province_code)a
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
ORDER BY region_code asc,channel,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    

-- 新客销售分析
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT 
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT a.customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select customer_no,first_sale_day 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q 
        where sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_sale_day>=regexp_replace(${hiveconf:sdt},'-','')
            AND first_sale_day<=regexp_replace(${hiveconf:edate},'-','')  ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('7','1')
   GROUP BY  
            province_code
   UNION ALL SELECT province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT a.customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select customer_no,first_sale_day 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q 
        where sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_sale_day>=regexp_replace(${hiveconf:last_sdt},'-','')
            AND first_sale_day<=regexp_replace(${hiveconf:last_edate},'-','') 
    ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and channel in ('7','1')
   GROUP BY  province_code
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
ORDER BY region_code asc,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    ;


-- 亏损分析 剔除退货、'104444','102998' 两个大 
-- 负毛利金额-100 ，月销售额>0

SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
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
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('7','1','9')
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
   FROM csx_dw.dws_sale_r_d_customer_sale a 
    WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and channel in ('7','1','9')
   --  and a.return_flag !='X'
   -- and a.customer_no not in ('104444','102998')
   GROUP BY  province_code,
            customer_no,
            substr(sdt,1,6)
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
ORDER BY region_code asc,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    ;

---------------------------------------------------计划导入临时表----------------------------------------
drop table csx_tmp.ads_sale_plan_national;
 create table csx_tmp.ads_sale_plan_national(
  years string  comment  '年份',
  sale_months string comment '计划月份',
  sale_quarter string  comment  '计划季度',
  zone_id string  comment  '大区编码',
  zone_name string  comment  '大区名称',
  dist_code    string  comment  '省区编码简称',
  dist_name    string  comment  '省区编码简称',
  plan_type_code string comment '指标类型,0 全渠道，1 大,2商超,3 BBC,4 福利单 WELFARE、5 新客',
  plan_type string comment '指标类型,0 全渠道，1 大,2商超,BBC,福利单(WELFARE)、新客',
  plan_sale_value decimal(38,6)  comment  '季度预算',
  plan_profit decimal(38,6)  comment  '毛利预算',
  plan_cust_num decimal(38,6)  comment  '预算',
  update_time timestamp comment '更新日期'
 ) comment  '全国季度计划指标含各省区计划' 
  partitioned by (quarters string comment '日期分区' )
   row format delimited 
   fields terminated by ','
  STORED AS textfile
;

select * from csx_tmp.ads_sale_plan_national;
alter table csx_tmp.ads_sale_plan_national add partition (quarters='202003');
alter table csx_tmp.ads_sale_plan_national drop partition (quarters>='202003');





-- SET hive.execution.engine=tez; 
SET hive.execution.engine=spark; 
-- set tez.queue.name=caishixian;
set edate=to_date(date_sub(current_date,1));
set sdt= to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')) ;
set last_edate = add_months(${hiveconf:edate},-3);
set last_sdt = add_months(to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')),-3);
                 
--- 渠道 、BBC、 福利单
drop table csx_tmp.temp_channel_sale;
create temporary table csx_tmp.temp_channel_sale
as 
SELECT channel_name,
       sales_type,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num
FROM
  (SELECT case when channel in ('1','7','9') then '大' else channel_name end channel_name,
        case when channel='7' then 'BBC' 
            when order_kind='WELFARE' then '福利单'
            end sales_type,
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
   --  and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end
            ,
             case when channel in ('1','7','9') then '大' else channel_name end ,
            province_code
   UNION ALL SELECT 
                case when channel in ('1','7','9') then '大' else channel_name end channel_name,
                case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end sales_type,
                    province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value) last_sale,
                    sum(profit) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
   -- and channel ='7'
   GROUP BY  case when channel='7' then 'BBC' 
                when order_kind='WELFARE' then '福利单'
                end
            , case when channel in ('1','7','9') then '大' else channel_name end ,
            province_code)a
LEFT JOIN
  (SELECT province_code,
          region_code,
          region_name,
          province_name
   FROM csx_dw.dim_area
   WHERE area_rank =13) c ON a.province_code =c.province_code
GROUP BY channel_name,
         sales_type,
         region_code,
         region_name,
         a.province_code,
         province_name

ORDER BY region_code asc,channel_name,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;

select region_code,a.region_name,a.province_code,a.province_name,channel_name,plan_sale_value,
    sum(a.sale) sale,
    coalesce(sum(a.sale)/plan_sale_value,0) as sale_fill_rate,
    sum(last_sale) as sale,
    sum(sale-last_sale)/abs(sum(a.last_sale)) as sale_growth_rate,
    plan_profit,
    sum(a.profit)profit,
    coalesce(sum(a.profit)/plan_profit,0) as profit_fill_rate,
    coalesce(sum(a.profit)/sum(a.sale),0) as profit_rate
from csx_tmp.temp_channel_sale a 
left join 
(select dist_code,plan_type,plan_sale_value,plan_profit,plan_cust_num 
    from csx_tmp.ads_sale_plan_national where plan_type in ('大','商超') and quarters='202003')b on a.province_code=b.dist_code and a.channel_name=b.plan_type
group by region_code,a.region_name,a.province_code,a.province_name,channel_name,plan_sale_value,plan_profit;




CREATE TEMPORARY TABLE `csx_tmp.ads_fr_channel_quarter_national`(
    level_id string COMMENT '层级 0 全渠道、1、大、2、商超',
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `channel_name` string comment '渠道名称', 
  `plan_sale_value` decimal(38,6) comment '计划销售额', 
  `sale` decimal(38,9) comment '毛利率', 
  `sale_fill_rate` decimal(38,20) comment '毛利率', 
  `last_sale` decimal(38,9) comment '毛利率', 
  `sale_growth_rate` decimal(38,18) comment '毛利率', 
  `plan_profit` decimal(38,6) comment '毛利率', 
  `profit` decimal(38,9) comment '毛利率', 
  `profit_fill_rate` decimal(38,20) comment '毛利率', 
  `profit_rate` decimal(38,18) comment '毛利率'
  )comment '全国季报渠道销售报表'
  partitioned by (sdt string COMMENT '日期分区')
STORED AS PARQUET 
;



---- 分割线，剔除北京两个---------------------------------------------------------

SET hive.execution.engine=tez; 
set tez.queue.name=caishixian;
set edate='${edt}';
set sdt= to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')) ;
set last_edate = add_months(${hiveconf:edate},-3);
set last_sdt = add_months(to_date(concat(year(${hiveconf:edate}),'-',lpad(ceil(month(${hiveconf:edate})/3) *3-2,2,0),'-01')),-3);

-- select ${hiveconf:edate},${hiveconf:sdt},${hiveconf:last_edate},${hiveconf:last_sdt};
-- 全渠道销售

SELECT region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(profit/10000)profit,
       sum(last_sale/10000)last_sale,
       sum(last_profit/10000)last_profit
FROM
  (SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
          sum(sales_value)sales_value,
         sum( profit) profit,
          0 last_sale,
          0 last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
    -- and channel in ('1','7','9')
    group by case when province_code in ('35','36') then '35' else province_code end
   UNION ALL SELECT case when province_code in ('35','36') then '35' else province_code end province_code,
                    0 sales_value,
                    0 profit,
                    sum(sales_value ) last_sale,
                    sum(profit ) last_profit
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
     and order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046')
    -- and channel in ('1','7','9')
    group by 
    case when province_code in ('35','36') then '35' else province_code end

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
ORDER BY region_code asc,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;


-- 渠道销售
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       count(DISTINCT case when sales_value!=0 then  customer_no end )sale_cust_num,
       count(DISTINCT case when last_sale!=0 then  customer_no end )last_sale_cust_num
FROM
  (SELECT CASE
              WHEN channel IN ('1',
                               '7',
                               '9') THEN '1'
              ELSE channel
          END channel,
          province_code,
          customer_no,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          0 AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            province_code,
            customer_no
   UNION ALL SELECT CASE
                        WHEN channel IN ('1',
                                         '7',
                                         '9') THEN '1'
                        ELSE channel
                    END channel,
                    province_code,
                    customer_no,
                    0 sales_value,
                    0 profit,
                    sum(sales_value ) last_sale,
                    sum(profit ) last_profit,
                    0 sale_cust_num,
                    0 AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
     and order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046')
   GROUP BY CASE
                WHEN channel IN ('1',
                                 '7',
                                 '9') THEN '1'
                ELSE channel
            END,
            customer_no,
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
ORDER BY region_code asc,channel,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    
    
       
    
                 
--- BBC、 福利单
SELECT channel,
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
        count(DISTINCT case when sales_value!=0 then  a.customer_no end )sale_cust_num,
       count(DISTINCT case when last_sale!=0 then  a.customer_no end )last_sale_cust_num
FROM
  (SELECT case when channel='7' then 'BBC'
               when b.attribute_code='3' then '贸易'
               when b.attribute_code='5' then '合伙人'
               when b.attribute_code in ('1','2') and  order_kind='WELFARE' then '福利单'
               when b.attribute_code in ('1','2') and  order_kind !='WELFARE' then '日配单'
               end channel,
          province_code,
          a.customer_no,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT a.customer_no) AS sale_cust_num,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a
   join 
   (select customer_no,attribute_code from csx_dw.dws_crm_w_a_customer_20200924 where sdt='current')b on a.customer_no=b.customer_no
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in('1','7')
   GROUP BY   case when channel='7' then 'BBC'
               when b.attribute_code='3' then '贸易'
               when b.attribute_code='5' then '合伙人'
               when b.attribute_code in ('1','2') and  order_kind='WELFARE' then '福利单'
               when b.attribute_code in ('1','2') and  order_kind !='WELFARE' then '日配单'
               end
            ,
            a.customer_no,
            province_code
   UNION ALL 
   SELECT case when channel='7' then 'BBC'
               when b.attribute_code='3' then '贸易'
               when b.attribute_code='5' then '合伙人'
               when b.attribute_code in ('1','2') and  order_kind='WELFARE' then '福利单'
               when b.attribute_code in ('1','2') and  order_kind !='WELFARE' then '日配单'
               end channel,
                    province_code,
                    a.customer_no,
                    0 sales_value,
                    0 profit,
                    sum(sales_value ) last_sale,
                    sum(profit  ) last_profit,
                    0 sale_cust_num,
                    count(DISTINCT a.customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a
   join 
   (select customer_no,attribute_code from csx_dw.dws_crm_w_a_customer_20200924 where sdt='current')b on a.customer_no=b.customer_no
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046')
     and channel in('1','7')
   GROUP BY  case when channel='7' then 'BBC'
               when b.attribute_code='3' then '贸易'
               when b.attribute_code='5' then '合伙人'
               when b.attribute_code in ('1','2') and  order_kind='WELFARE' then '福利单'
               when b.attribute_code in ('1','2') and  order_kind !='WELFARE' then '日配单'
               end
            ,
            a.customer_no,
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
ORDER BY region_code asc,channel,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc 
;
  
 
 

-- 新客销售分析


-- 新客销售分析
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
       sum(daily_cust_sale/10000) as daily_cust_sale,
       sum(daily_match_sale/10000) as daily_match_sale,
       sum(last_sale/10000)last_sale,
       sum(profit/10000)profit,
       sum(last_profit/10000)last_profit,
       sum(sale_cust_num)sale_cust_num,
       sum(last_sale_cust_num)last_sale_cust_num,
       sum(sale_daily_cust_num)sale_daily_cust_num,
       sum(daily_order_cust)daily_order_cust
FROM
  (SELECT 
          province_code,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 AS daily_cust_sale,
          0 AS daily_match_sale,
          0 last_sale,
          0 last_profit,
          count(DISTINCT a.customer_no) AS sale_cust_num,
          0 AS sale_daily_cust_num,
          0 AS daily_order_cust,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select a.customer_no,first_sale_day 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q a 
    where sdt=regexp_replace(${hiveconf:edate},'-','')
            and first_sale_day>=regexp_replace(${hiveconf:sdt},'-','')
            AND first_sale_day<=regexp_replace(${hiveconf:edate},'-','')  
    ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('1','7')
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
                    count(DISTINCT a.customer_no) AS last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select a.customer_no,first_sale_day
    from csx_dw.ads_sale_w_d_ads_customer_sales_q a 
    where sdt=regexp_replace(${hiveconf:edate},'-','')
        and first_sale_day>=regexp_replace(${hiveconf:last_sdt},'-','')
        AND first_sale_day<=regexp_replace(${hiveconf:last_edate},'-','')  ) b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and channel in ('1','7')
   GROUP BY  province_code
   UNION all 
  SELECT 
          province_code,
          0 sales_value,
          0 profit,
          sum(case when b.attribute_code='1'  then a.sales_value end  ) AS daily_cust_sale,
          sum(case when b.attribute_code in('1','2') and a.order_kind !='WELFARE'  then a.sales_value end  ) AS daily_match_sale,
          0 last_sale,
          0 last_profit,
          0 AS sale_cust_num,
          count(distinct case when b.attribute_code ='1' then a.customer_no end )AS sale_daily_cust_num,
          count(DISTINCT case when b.attribute_code in ('1','2') and order_kind !='WELFARE' then a.customer_no end )AS daily_order_cust,
          0 last_sale_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select customer_no,attribute,attribute_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='20200930') b on a.customer_no=b.customer_no  
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('1','7')
   GROUP BY  
            province_code
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
ORDER BY case when  region_code='2' then 1
             when region_code='4' then 2
             when region_code ='1' then 3
             when region_code ='3' then 4 end asc,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    

    

-- 亏损
SELECT 
       region_code,
       region_name,
       a.province_code,
       province_name,
       sum(sales_value/10000)sale,
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
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>=regexp_replace(${hiveconf:sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:edate},'-','')
     and channel in ('7','1','9')
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
   FROM csx_dw.dws_sale_r_d_customer_sale a 
    WHERE sdt>=regexp_replace(${hiveconf:last_sdt},'-','')
     AND sdt<=regexp_replace(${hiveconf:last_edate},'-','')
    and channel in ('7','1','9')
   --  and a.return_flag !='X'
   and a.customer_no not in ('104444','102998')
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
ORDER BY region_code asc,
    case when a.province_code in ('2','20') then '01' when a.province_code='1' then '9' else a.province_code end desc   ;
    ;

------------------------------------------------------------------
