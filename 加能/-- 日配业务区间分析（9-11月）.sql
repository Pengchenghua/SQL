-- 日配业务区间分析（9-11月）
create temporary table csx_tmp.temp_rp_sale as 
select    mon,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    second_category_code,
    second_category_name,
    customer_name,
    customer_no,
    (sales_value)/10000 sales_value,
    (profit)/10000 profit,
    profit/sales_value profit_rate,
    case when sales_value/10000>=12 and profit_rate>=0.15 then 1 end cust_sale_12,
    case when sales_value/10000>=50 and profit_rate>=0.05 then 1 end cust_sale_50
from 
(
select substr(sdt,1,6) mon,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    second_category_code,
    second_category_name,
    customer_name,
    customer_no,
    
    sum(sales_value) sales_value,
    sum(profit) profit ,
    sum(profit)/sum(sales_value) profit_rate
from csx_dw.dws_sale_r_d_detail 
where sdt>='20210901' and sdt<='20211130'
and channel_code='1'
and business_type_code='1'
group by  substr(sdt,1,6) ,
    province_code,
    province_name,
    customer_name,
    customer_no,
    city_group_code,
    city_group_name,
    second_category_code,
    second_category_name
    ) a 
    ;
    
    
    select province_code,
    province_name, 
    all_cust,
    (sales_value) sales_value,
    (profit) profit,
    (profit)/(sales_value) as profit_rate,
    cust_sale_12,
    sales_value_12,
    profit_12,
    profit_12/sales_value_12 profit_rate_12,
    cust_sale_12_50,
    sales_value_12_50,
    profit_12_50,
    profit_12_50/sales_value_12_50 profit_rate_12_50,
    cust_sale_50,
    sales_value_50,
    profit_50,
    profit_50/sales_value_50 as profit_rate_50
    from(
        select province_code,
            province_name, 
            count(distinct case when sales_value>0 then customer_no end ) all_cust,
            sum(sales_value) sales_value,
            sum(profit) profit,
            sum(profit)/sum(sales_value) as profit_rate,
            --销售额12万以上且毛利率15%
            count(distinct case when   sales_value>=10 and profit/sales_value>=0.15 then customer_no end  ) as cust_sale_12,
            sum(case when  sales_value>=10 and profit/sales_value>=0.15 then sales_value end) sales_value_12,
            sum(case when  sales_value>=10 and profit/sales_value>=0.15 then profit end ) profit_12,
            -- 销售额50W且毛利率15%以上
            count(distinct case when  sales_value>=50 and profit/sales_value>=0.15 then customer_no end  ) as cust_sale_12_50,
            sum(case when  sales_value>=50 and profit/sales_value>=0.15 then sales_value end) sales_value_12_50,
            sum(case when  sales_value>=50 and profit/sales_value>=0.15 then profit end ) profit_12_50,
            -- 销售额50W以上且毛利率5%
            count(distinct case when cust_sale_50=1 then customer_no end ) cust_sale_50,
            sum(case when  cust_sale_50=1 then sales_value end) sales_value_50,
            sum(case when  cust_sale_50=1 then profit end ) profit_50
        from csx_tmp.temp_rp_sale
        group by province_code,
                 province_name
    )a 
    ;
    
    select * from csx_tmp.temp_rp_sale;