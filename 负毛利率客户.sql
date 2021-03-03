select province_code,
          province_name ,
          a.customer_no,
          a.customer_name ,
        (case when mon='202004' then  sales_value end ) sales_value_04,
        (case when mon='202004' then profit end ) profit_04,
        (case when mon='202004' then profit/sales_value end ) profit_rate_04,
        (case when mon='202004' then sale_sdt end ) sale_sdt_04,
        (case when mon='202005' then  sales_value end ) sales_value_05,
        (case when mon='202005' then profit end ) profit_05,
        (case when mon='202004' then profit/sales_value end ) profit_rate_05,
        (case when mon='202004' then sale_sdt end ) sale_sdt_05,
        (case when mon='202006' then  sales_value end ) sales_value_06,
        (case when mon='202006' then profit end ) profit_06,
        (case when mon='202004' then profit/sales_value end ) profit_rate_06,
        (case when mon='202006' then sale_sdt end ) sale_sdt_06,
        (case when mon='202007' then  sales_value end ) sales_value_07,
        (case when mon='202007' then profit end ) profit_07,
        (case when mon='202004' then profit/sales_value end ) profit_rate_07,
        (case when mon='202007' then sale_sdt end ) sale_sdt_07
        (case when mon='202008' then  sales_value end ) sales_value_08,
        (case when mon='202008' then profit end ) profit_08,
        (case when mon='202004' then profit/sales_value end ) profit_rate_08,
        (case when mon='202008' then sale_sdt end ) sale_sdt_08
    from 
    (SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          a.customer_name ,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200401'
     AND sdt<='20200820'
     and channel in ('7','1','9')
     and a.customer_no not in ('104444','102998')
     and is_self_sale =1
   GROUP BY  
            substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no,
          a.customer_name
    )a 
    where profit_rate  BETWEEN 0 and 0.1
    group by province_code,
          province_name ,
          a.customer_no,
          a.customer_name;
