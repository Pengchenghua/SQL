-- 客户属性销售  

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
       (sum(sale)- coalesce(sum(ring_sale),0))/abs(coalesce(sum(ring_sale),0) ) as mom_sale_rate,
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

-- 商超查询数据
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
        sdt = 'current' and distribution_area='17') b on
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
    sales_belong_flag
;

select sales_province ,sales_province_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current' and customer_no in ('103097', '103903','104842') ;
select * from csx_dw.csx_shop where sdt='current' and ascription_type ='17';
select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and shop_id ='9F03';


