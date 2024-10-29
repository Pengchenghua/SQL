 -- 客服管家红黑榜历史销售6-8月
 with 
sale as 
    (select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        -- sales_user_name,
        -- sales_user_number,
        -- sales_user_position,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
        where sdt >= '20240701'
        and sdt <= '20240930'   
        and (a.business_type_code in ('1','2','6')  -- 1-日配、2-福利、6-BBC
            or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209') and a.business_type_code =4)
        )
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name
        -- sales_user_name,
        -- sales_user_number,
        -- sales_user_position
    )
   
   select  a.sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.service_user_work_no,
        a.service_user_name,
        sum(avg_sale_amt) avg_sale_amt,
        sum(avg_profit)  avg_profit,
        if(sum(avg_sale_amt)=0,0,sum(avg_profit)/sum(avg_sale_amt)) as profit_rate
    from 
    (
   select  a.sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.business_type_code,
        a.customer_code,
        a.customer_name,
        b.service_user_work_no,
        b.service_user_name,
        sale_amt/c.cnt as avg_sale_amt,
        profit/c.cnt as avg_profit,
        c.cnt,
        sale_amt,
        profit
    from sale a 
    left join 
    (
    select substr(sdt,1,6) sale_month,
      customer_code as customer_no,
      service_manager_user_number service_user_work_no,
      service_manager_user_name service_user_name,
      -- service_manager_user_id service_user_id,
      business_attribute_code attribute_code,
      business_attribute_name attribute_name,
     case when  business_attribute_code=1 then 1 
        when business_attribute_code=2 then 2 
        when business_attribute_code=5 then 6 
        end business_type_code,
      service_manager_user_position,
      count()over(partition by customer_code,business_attribute_code,substr(sdt,1,6) ) as cnt
    from csx_dim.csx_dim_crm_customer_business_ownership
    where sdt in ( '20240930','20240731','20240630')
   
   -- and customer_code='237857'
    --  and service_manager_user_id <> 0 -- and customer_code='111207'
    --  and business_attribute_code='1'
    ) b on a.customer_code=b.customer_no and a.business_type_code=b.business_type_code and a.sale_month=b.sale_month
    left join 
    (
    select substr(sdt,1,6) sale_month,
      customer_code as customer_no,
      case when  business_attribute_code=1 then 1 
        when business_attribute_code=2 then 2 
        when business_attribute_code=5 then 6 
        end business_type_code,
      count(service_manager_user_id) cnt
    from csx_dim.csx_dim_crm_customer_business_ownership
    where sdt in ( '20240930','20240731','20240630')
     group by substr(sdt,1,6)  ,
        customer_code,
        case when  business_attribute_code=1 then 1 
            when business_attribute_code=2 then 2 
            when business_attribute_code=5 then 6 
        end 
    ) c on a.customer_code=c.customer_no and a.business_type_code=c.business_type_code and a.sale_month=c.sale_month
   ) a 
   group by  a.sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.service_user_work_no,
        a.service_user_name
  --  where leader_user_name='谢志晓'
  ;


   with 
sale as 
    (select substr(sdt, 1, 6) sale_month,
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position,
        sum(sale_amt) sale_amt,
        sum(profit) profit
    from csx_dws.csx_dws_sale_detail_di a   
        where sdt >= '20240701'
        and sdt <= '20240930'   
        and (a.business_type_code in ('1','2','6')  -- 1-日配、2-福利、6-BBC
            or (sales_user_number in ('81244592','81079752','80897025','81022821','81190209') and a.business_type_code =4)
        )
    group by substr(sdt, 1, 6),
        performance_province_name,
        performance_region_name,
        performance_city_name,
        a.business_type_code,
        a.customer_code,
        customer_name,
        sales_user_name,
        sales_user_number,
        sales_user_position
    )
    select  a.sale_month,
        a.performance_province_name,
        a.performance_region_name,
        a.performance_city_name,
        a.business_type_code,
        a.customer_code,
        a.customer_name,
        a.sales_user_name,
        a.sales_user_number,
        a.sales_user_position,
        b.service_user_work_no,
        b.service_user_name,
        b.service_manager_user_position,
        sale_amt/c.cnt as avg_sale_amt,
        profit/c.cnt as avg_profit,
        c.cnt,
        sale_amt,
        profit
    from sale a 
    left join 
    (
    select substr(sdt,1,6) sale_month,
      customer_code as customer_no,
      service_manager_user_number service_user_work_no,
      service_manager_user_name service_user_name,
      -- service_manager_user_id service_user_id,
      business_attribute_code attribute_code,
      business_attribute_name attribute_name,
     case when  business_attribute_code=1 then 1 
        when business_attribute_code=2 then 2 
        when business_attribute_code=5 then 6 
        end business_type_code,
      service_manager_user_position,
      sales_user_name,
      sales_user_number,
      sales_user_position,
      count()over(partition by customer_code,business_attribute_code,substr(sdt,1,6) ) as cnt
    from csx_dim.csx_dim_crm_customer_business_ownership
    where sdt in ( '20240930','20240731','20240630')
   -- and customer_code='237857'
    --  and service_manager_user_id <> 0 -- and customer_code='111207'
    --  and business_attribute_code='1'
    ) b on a.customer_code=b.customer_no and a.business_type_code=b.business_type_code and a.sale_month=b.sale_month
    left join 
    (
    select substr(sdt,1,6) sale_month,
      customer_code as customer_no,
      case when  business_attribute_code=1 then 1 
        when business_attribute_code=2 then 2 
        when business_attribute_code=5 then 6 
        end business_type_code,
      count(service_manager_user_id) cnt
    from csx_dim.csx_dim_crm_customer_business_ownership
    where sdt in ( '20240930','20240731','20240630')
     group by substr(sdt,1,6)  ,
        customer_code,
        case when  business_attribute_code=1 then 1 
            when business_attribute_code=2 then 2 
            when business_attribute_code=5 then 6 
        end 
    ) c on a.customer_code=c.customer_no and a.business_type_code=c.business_type_code and a.sale_month=c.sale_month
   
  --  where leader_user_name='谢志晓'
  ;