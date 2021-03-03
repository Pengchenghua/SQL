drop table  csx_tmp.temp_profit_low;
create temporary table csx_tmp.temp_profit_low
as 
select province_code,
          province_name ,
          a.customer_no,
        sum(case when mon='202004' then  sales_value end ) sales_value_04,
        sum(case when mon='202004' then profit end ) profit_04,
        sum(case when mon='202004' then profit/sales_value end ) profit_rate_04,
        sum(case when mon='202004' then sale_sdt end ) sale_sdt_04,
        sum(case when mon='202005' then  sales_value end ) sales_value_05,
        sum(case when mon='202005' then profit end ) profit_05,
        sum(case when mon='202005' then profit/sales_value end ) profit_rate_05,
        sum(case when mon='202005' then sale_sdt end ) sale_sdt_05
    from (
 SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200401'
     AND sdt<='20200522'
     and channel in ('7','1','9')
     and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no
 ) a 
 where profit_rate between 0 and 0.05 
    and sales_value>0
 
 group by 
          province_code,
          province_name ,
          a.customer_no
          
          ;
          

-- select * from csx_tmp.temp_profit_low;

drop table csx_tmp.temp_customer_low_01;
 create temporary table csx_tmp.temp_customer_low_01 as 
select         
        a.province_code,
        a.province_name ,
        a.customer_no,
        coalesce(sum(sales_value_07),0) sales_value_07,
        coalesce(sum(profit_07),0) profit_07,
        coalesce(sum(sale_sdt_07),0) sale_sdt_07,
        coalesce(sum(sales_value_08),0) sales_value_08,
        coalesce(sum(profit_08),0) profit_08,
        coalesce(sum(sale_sdt_08),0) sale_sdt_08
from 
(
select        
        a.province_code,
        a.province_name ,
        a.customer_no,
        coalesce(sum(case when mon='202007' then sales_value end ),0) sales_value_07,
        coalesce(sum(case when mon='202007' then profit end ),0) profit_07,
        coalesce(sum(case when mon='202007' then sale_sdt end ),0) sale_sdt_07,
        coalesce(sum(case when mon='202008' then sales_value end ),0) sales_value_08,
        coalesce(sum(case when mon='202008' then profit end ),0) profit_08,
        coalesce(sum(case when mon='202008' then sale_sdt end ),0) sale_sdt_08
from 
(SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
    WHERE sdt>='20200701'
     AND sdt<='20200822'
     and channel in ('7','1','9')
      and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no
) a 
group by 
        a.province_code,
        a.province_name ,
        a.customer_no
) a 
group by 
        a.province_code,
        a.province_name ,
        a.customer_no 
        ;
          
 
select * from  csx_tmp.temp_profit_low ;
         
select  region_code,
        region_name,
        a.province_code,
        province_name ,
        a.customer_no,
        a.customer_name ,
        sum(sales_value_04) sales_value_04,
        sum(profit_04) profit_04,
        sum(profit_rate_04) profit_rate_04,
        sum(sale_sdt_04) sale_sdt_04,
        sum(sales_value_05) sales_value_05,
        sum(profit_05) profit_05,
        sum(profit_rate_05) profit_rate_05,
        sum(sale_sdt_05) sale_sdt_05,
        sum(sales_value_06) sales_value_06,
        sum(profit_06) profit_06,
        sum(profit_rate_06) profit_rate_06,
        sum(sale_sdt_06) sale_sdt_06,
        sum(sales_value_07) sales_value_07,
        sum(profit_07) profit_07,
        sum(profit_rate_07) profit_rate_07,
        sum(sale_sdt_07) sale_sdt_07,
        sum(sales_value_08) sales_value_08,
        sum(profit_08) profit_08,
        sum(profit_rate_08) profit_rate_08,
        sum(sale_sdt_08) sale_sdt_08
from (
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
        (case when mon='202005' then profit/sales_value end ) profit_rate_05,
        (case when mon='202005' then sale_sdt end ) sale_sdt_05,
        (case when mon='202006' then  sales_value end ) sales_value_06,
        (case when mon='202006' then profit end ) profit_06,
        (case when mon='202006' then profit/sales_value end ) profit_rate_06,
        (case when mon='202006' then sale_sdt end ) sale_sdt_06,
        (case when mon='202007' then  sales_value end ) sales_value_07,
        (case when mon='202007' then profit end ) profit_07,
        (case when mon='202007' then profit/sales_value end ) profit_rate_07,
        (case when mon='202007' then sale_sdt end ) sale_sdt_07,
        (case when mon='202008' then  sales_value end ) sales_value_08,
        (case when mon='202008' then profit end ) profit_08,
        (case when mon='202008' then profit/sales_value end ) profit_rate_08,
        (case when mon='202008' then sale_sdt end ) sale_sdt_08
    from 
    (SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200401'
     AND sdt<='20200520'
     and channel in ('7','1','9')
     and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no,
          trim(a.customer_name)
    
    )a 
    where profit_rate  BETWEEN 0 and 0.05
    and sales_value >10000
) a 
left join 
(select region_code,region_name,province_code from csx_dw.dim_area where area_rank =13) b on a.province_code =b.province_code
group by  a.province_code,
          province_name ,
          a.customer_no,
          a.customer_name,
          region_code,region_name;
          


select region_code,region_name,
        a.province_code,
        a.province_name ,
        a.customer_no,
        customer_name ,
        sum(sales_value_04) as sales_value_04,
        sum(profit_04) as profit_04,
        sum(sale_sdt_04) as sale_sdt_04,
        sum(sales_value_05)sales_value_05,
        sum(profit_05)profit_05,
        sum(sale_sdt_05)sale_sdt_05,
        coalesce(sum(sales_value_07),0) sales_value_07,
        coalesce(sum(profit_07),0) profit_07,
        coalesce(sum(sale_sdt_07),0) sale_sdt_07,
        coalesce(sum(sales_value_08),0) sales_value_08,
        coalesce(sum(profit_08),0) profit_08,
        coalesce(sum(sale_sdt_08),0) sale_sdt_08,
        c.first_sale_day from csx_tmp.temp_profit_low a 
left join 
 csx_tmp.temp_customer_low_01 b on a.customer_no=b.customer_no
left join 
(select customer_no,first_sale_day from csx_dw.ads_sale_w_d_ads_customer_sales_q where sdt='20200820') c on a.customer_no=c.customer_no
left join 
(select region_code,region_name,province_code from csx_dw.dim_area where area_rank =13) d on a.province_code =d.province_code
left join
(select customer_no,customer_name from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current') e on a.customer_no=e.customer_no
group by 
region_code,region_name,
        a.province_code,
        a.province_name ,
        a.customer_no,
        customer_name ,
        c.first_sale_day;
