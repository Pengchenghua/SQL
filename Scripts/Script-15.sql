SELECT 
          province_code,
          a.customer_no ,
          sum(sales_value)sales_value,
          sum(profit) profit,
          0 last_sale,
          0 last_profit,
          count(DISTINCT a.customer_no) AS sale_cust_num,
          0 plan_cust_num
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   join 
   (select a.customer_no,first_sale_day,attribute,attribute_code 
    from csx_dw.ads_sale_w_d_ads_customer_sales_q a 
    join 
    (select customer_no,attribute,attribute_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current')b on a.customer_no=b.customer_no
        where sdt='20201206'
            and first_sale_day>='20201001'
            AND first_sale_day<='20201130'  ) b on a.customer_no=b.customer_no  
   WHERE sdt>='20201001'
     AND sdt<='20201130' 
     and channel in ('7','1')
   GROUP BY  
            province_code,
            a.customer_no 
            ;
            
           
 select * from     csx_dw.dws_sale_r_d_customer_sale a  WHERE sdt>='20201001'
     AND sdt<='20201130' and customer_no in ('114477','114235');
     
    
