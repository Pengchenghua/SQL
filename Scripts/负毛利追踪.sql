select * from csx_ods.source_basic_w_a_md_dic where sdt='20200812';
select * from csx_ods.source_wms_r_d_bills_config where sdt='20200811';

select * from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20190101' and origin_order_no ='';


select substr(sdt,1,6) as mon,customer_no ,sum(sales_value )
    from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200101' and dc_code ='W0G7'
    group by substr(sdt,1,6),customer_no ;
    
select substr('1912260600508344',7);
select substr('OM200819009364',7);
select
    level_id,
    sales_month,
    zone_id,
    zone_name,
    province_code,
    province_name,
    attribute_code,
    attribute_name,
    business_division_code,
    business_division_name,
    division_code,
    division_name,
    department_code,
    department_name,
    dail_plan_sale,
    daily_sale_value/10000 daily_sale_value,
    daily_sale_fill_rate,
    daily_profit/10000 daily_profit,
    daily_profit_rate,
    month_plan_sale,
    month_sale/10000 month_sale,
    month_sale_fill_rate,
    last_month_sale/10000 last_month_sale,
    mom_sale_growth_rate,
    month_sale_ratio,
    month_avg_cust_sale/10000 month_avg_cust_sale,
    month_plan_profit,
    month_profit/10000 month_profit,
    month_profit_fill_rate,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    cust_penetration_rate,
    all_sale_cust_num,
    row_num,
    update_time,
    months
from
    csx_tmp.ads_sale_r_d_zone_province_dept_fr
where
    months = '202008';
refresh csx_tmp.ads_sale_r_d_zone_super_type_fr;
select
    level_id,
    sales_month,
    zone_id,
    zone_name,
    channel_code,
    channel,
    division_code,
    division_name,
    department_code,
    department_name,
    daily_plan_sale,
    daily_sale_value/10000 as daily_sale_value,
    daily_sale_fill_rate,
    daily_profit/10000 as daily_profit,
    daily_profit_rate,
    month_plan_sale,
    month_sale/10000 month_sale,
    month_sale_fill_rate,
    last_month_sale/10000 last_month_sale,
    mom_sale_growth_rate,
    month_plan_profit,
    month_profit/10000 month_profit,
    month_profit_fill_rate,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    cust_penetration_rate,
    all_sale_cust_num,
    row_num,
    update_time,
    months
from
    csx_tmp.ads_sale_r_d_zone_catg_sales_fr
where
    months = '202008'
    and zone_id = '3'
    and level_id in ('2', '3');
select * from csx_tmp.ads_sale_r_d_zone_super_type_fr where months='202008';
select * from csx_tmp.ads_sale_r_d_zone_cust_attribute_fr  where months='202008';
select * from csx_tmp.ads_sale_r_d_zone_sales_fr where months='202008';

select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and shop_id ='99A7';

select * from csx_dw.dws_sale_r_d_customer_sale a where sdt>='20200801'
    and channel = '2'
    and province_code in ('1','11','15')
    AND a.customer_no IN ('S9961','S99A0','S9996','SW098');

select distinct format_type ,format_type_code from csx_tmp.ads_sale_r_d_zone_super_type_fr where format_type not like '%小计';
select distinct channel, customer_attribute_code ,customer_attribute_name from csx_tmp.dws_csms_manager_month_sale_plan_tmp ;
select level_id,
    sales_month,
    zone_id,
    zone_name,
    channel_code,
    channel,
    division_code,
    division_name,
    department_code,
    department_name,
    daily_plan_sale,
    daily_sale_value/10000 as daily_sale_value,
    daily_sale_fill_rate,
    daily_profit/10000 as daily_profit,
    daily_profit_rate,
    month_plan_sale,
    month_sale/10000 month_sale,
    month_sale_fill_rate,
    last_month_sale/10000 last_month_sale,
    mom_sale_growth_rate,
    month_plan_profit,
    month_profit/10000 month_profit,
    month_profit_fill_rate,
    month_profit_rate,
    month_sales_sku,
    month_sale_cust_num,
    cust_penetration_rate,
    all_sale_cust_num,
    row_num,
    update_time,
    months
from csx_tmp.ads_sale_r_d_zone_catg_sales_fr 
    where months='202008' and zone_id ='3'
order by zone_id,case when division_name ='食百采购部' then '13' else division_code end asc,level_id,row_num;

SELECT * FROM csx_dw.dws_basic_w_a_csx_shop_m where customer_no='104576' and sdt='current';

SELECT * FROM  csx_dw.dws_bbc_r_d_wshop_order_m;


select distinct business_type ,business_type_code 
from csx_dw.wms_shipped_order
where 
--    regexp_replace(to_date(create_time),'-','')>='20200801' 
--    and regexp_replace(to_date(create_time),'-','')<='20200831'
     send_sdt >= '20200801'
   -- and business_type_code !='73'
  --  and source_system ='BBC'
  --  and shipped_location_code ='W0B6'
 
;

select * from csx_ods.source_crm_w_a_intent_customer limit 10; 
refresh csx_tmp.ads_sale_r_d_zone_super_type_fr;
select * from csx_tmp.temp_turnover;


select  origin_order_no 
from csx_dw.wms_shipped_order 
where send_sdt >= '20200801'
and send_sdt <= '20200817'
and business_type_code  in ('18','19','73')
and shipped_location_code ='W0B6'
and goods_code ='1126788'
group by origin_order_no
;
select a.*from 
(select origin_order_no ,SUM(sales_value ) from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200801'and goods_code ='1126788' and dc_code ='W0B6'
group by origin_order_no) a 
left join 
(select  origin_order_no 
from csx_dw.wms_shipped_order 
where send_sdt >= '20200801'
and send_sdt <= '20200817'
and business_type_code  in ('18','19','73')
and shipped_location_code ='W0B6'
and goods_code ='1126788'
group by origin_order_no)b on substr(a.origin_order_no,7)=b.origin_order_no 
where b.origin_order_no is null ;

;

select
        DISTINCT entry_type ,entry_type_code ,business_type ,business_type_code 
    from
        csx_dw.wms_entry_order
    where
        sdt              >'20200801'
       
  ;
  select distinct shop_belong,shop_belong_desc from dim.dim_shop ;
  
  
  
  
-- 负毛利客户追踪 2-2--822  
select  region_code,
          region_name,
          a.province_code,
          province_name ,
          a.customer_no,
          c.customer_name ,
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
        sum(sale_sdt_08) sale_sdt_08,
        first_sale_day
from (
select province_code,
          province_name ,
          a.customer_no,
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
     AND sdt<='20200522'
     and channel in ('7','1','9')
    -- and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no
    ) a where sales_value>0 
    union all 
     SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
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
    
    )a 
    where 1=1
--    profit_rate  BETWEEN 0 and 0.05
  and sales_value >0
) a 
left join 
(select region_code,region_name,province_code from csx_dw.dim_area where area_rank =13) b on a.province_code =b.province_code
left join 
(select customer_no,customer_name from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current')c  on a.customer_no =c.customer_no
left join 
(select customer_no ,first_sale_day from csx_dw.ads_sale_w_d_ads_customer_sales_q where sdt='20200821') d on a.customer_no=d.customer_no
group by  a.province_code,
          province_name ,
          a.customer_no,
          c.customer_name,
          region_code,region_name,
      first_sale_day;

select province_code,
        province_name ,
        a.customer_no,
        a.customer_name ,
        coalesce(sum(sales_value_04),0) as sales_value_04,
        coalesce(sum(profit_04),0)  as profit_04,
        coalesce(sum(sale_sdt_04),0)  as sale_sdt_04,
        coalesce(sum(sales_value_05),0) sales_value_05,
        coalesce(sum(profit_05),0) profit_05,
        coalesce(sum(sale_sdt_05),0) sale_sdt_05
from 
 (select province_code,
        province_name ,
        a.customer_no,
        a.customer_name ,
        sum(coalesce(sales_value_04,0)) as sales_value_04,
        sum(coalesce(profit_04,0))  as profit_04,
        sum(coalesce(sale_sdt_04,0))  as sale_sdt_04,
        sum(coalesce(sales_value_05,0)) sales_value_05,
        sum(coalesce(profit_05,0)) profit_05,
        sum(coalesce(sale_sdt_05,0)) sale_sdt_05
from (
 select province_code,
        province_name ,
        a.customer_no,
        a.customer_name ,
        sum(case when mon='202004' then coalesce(sales_value ,0)end) sales_value_04,
        sum(case when mon='202004' then coalesce(profit  ,0)end) profit_04,
        sum(case when mon='202004' then coalesce(sale_sdt ,0)end) sale_sdt_04,
        sum(case when mon='202005' then coalesce(sales_value  ,0)end) sales_value_05,
        sum(case when mon='202005' then coalesce(profit ,0)end) profit_05,
        sum(case when mon='202005' then coalesce(sale_sdt,0)end) sale_sdt_05
        -- sum(case when mon='202006' then  sales_value end ) sales_value_06,
        -- sum(case when mon='202006' then profit end ) profit_06,
        -- sum(case when mon='202006' then sale_sdt end ) sale_sdt_06
    from 
    (SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          a.customer_name ,
          coalesce(sum(sales_value),0) sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200401'
     AND sdt<='20200520'
     and channel in ('7','1','9')
   --  and a.customer_no not in ('104444','102998')
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no,
          a.customer_name
    )a 
    where profit <-100
    and sales_value >0
    group by province_code,
          province_name ,
          a.customer_no,
          a.customer_name
) a 
group by 
        province_code,
        province_name ,
        a.customer_no,
        a.customer_name
) a 
WHERE customer_no='104601'
group by
        province_code,
        province_name ,
        a.customer_no,
        a.customer_name;
select * from csx_dw.ads_fixation_report_account_province_supervisor_month where sdt='20200821';
select
    distinct sales_province_code ,
    sales_province ,
    first_supervisor_work_no ,
    first_supervisor_name
from
    csx_dw.dws_crm_w_a_customer_m_v1
where
    sdt = 'current'
    and sales_province_code in ('1', '26', '6');
    
select sum(sales_value )as aa from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200701' and sdt<='20200822' 
group by province_code ;

select sum(sales_value )as bb from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200701' and sdt<='20200820' 
and channel !='2' 
and province_code ='1' group by province_code ;

select   mon,
          province_code,
          province_name ,
          a.customer_no,
          sales_value,
          profit,
          sale_sdt
from (
SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          coalesce(sum(sales_value),0) sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200401'
     AND sdt<='20200520'
     and channel in ('7','1','9')
   --  and a.customer_no not in ('104444','102998')
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no
 ) a where sales_value >0 
 and profit <-100
 ;
 
 
 select * from  csx_dw.ads_supply_order_flow a 
where 
 sdt>='20200801'
and regexp_replace(last_delivery_date,'-','') <='20200824'
and super_class='1'
and category_code in ('12','13','14')
and location_code ='W0A3';

select distinct zone_id ,zone_name ,dist_code,dist_name from csx_dw.csx_shop where sdt='current';

SELECT * from csx_dw.dws_sale_r_d_sale_item_simple where goods_code ='874186' and dc_code ='W0Q2' and sdt>='20200701';



-- 负毛利客户追踪 2-2--822  
select  region_code,
          region_name,
          a.province_code,
          province_name ,
          a.customer_no,
          c.customer_name ,
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
        sum(sale_sdt_08) sale_sdt_08,
        sum(sales_value_09) sales_value_09,
        sum(profit_09) profit_09,
        sum(profit_rate_09) profit_rate_09,
        sum(sale_sdt_09) sale_sdt_09,
        first_sale_day
from (
select province_code,
          province_name ,
          a.customer_no,
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
        (case when mon='202008' then sale_sdt end ) sale_sdt_08,
        (case when mon='202009' then  sales_value end ) sales_value_09,
        (case when mon='202009' then profit end ) profit_09,
        (case when mon='202009' then profit/sales_value end ) profit_rate_09,
        (case when mon='202009' then sale_sdt end ) sale_sdt_09
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
     AND sdt<='20200614'
     and channel in ('7','1','9')
    -- and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no
    union all 
    
    SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200701'
     AND sdt<='20200914'
     and channel in ('7','1','9')
     --and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
     
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no  
         
    )a
where 1=1
   and profit_rate  BETWEEN 0 and 0.05
  and sales_value >0
)a
left join 
(select region_code,region_name,province_code from csx_dw.dim_area where area_rank =13) b on a.province_code =b.province_code
left join 
(select customer_no,customer_name from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current')c  on a.customer_no =c.customer_no
left join 
(select customer_no ,first_sale_day from csx_dw.ads_sale_w_d_ads_customer_sales_q where sdt='20200914') d 
    on a.customer_no=d.customer_no
     
group by  a.province_code,
          province_name ,
          a.customer_no,
          c.customer_name,
          region_code,region_name,
      first_sale_day;





drop table  csx_tmp.temp_negative_profit_01;
create  table csx_tmp.temp_negative_profit_01
as 
 SELECT   mon,
          region_code,
          region_name,
          a.province_code,
          province_name ,
          a.customer_no,
          customer_name,
          attribute,
          first_sale_day,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
          sum(sale_sdt) as sale_num ,
          if(substr(first_sale_day,1,6)=mon,'是','否') as age_note
    
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
     AND sdt<='20200614'
     and channel in ('7','1','9')
    -- and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
     and a.return_flag!='X'
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no
    union all 
    SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200701'
     AND sdt<='20200914'
     and channel in ('7','1','9')
     --and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
     and a.return_flag!='X'
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no  
         
    )a
left join 
(select region_code,region_name,province_code from csx_dw.dim_area where area_rank =13) b on a.province_code =b.province_code
left join 
(select customer_no,customer_name,attribute,attribute_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current')c  on a.customer_no =c.customer_no
left join 
(select customer_no ,first_sale_day from csx_dw.ads_sale_w_d_ads_customer_sales_q where sdt='20200914') d 
    on a.customer_no=d.customer_no
group by mon,
          region_code,
          region_name,
          a.province_code,
          province_name ,
          a.customer_no,
          customer_name,
          first_sale_day,
          attribute,
          if(substr(first_sale_day,1,6)=mon,'是','否');





-- 负毛利客户追踪 2-2--822  
drop table  csx_tmp.temp_negative_profit;
create  table csx_tmp.temp_negative_profit
as 
select  region_code,
          region_name,
          a.province_code,
          province_name ,
          a.customer_no,
          c.customer_name ,
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
        sum(sale_sdt_08) sale_sdt_08,
        sum(sales_value_09) sales_value_09,
        sum(profit_09) profit_09,
        sum(profit_rate_09) profit_rate_09,
        sum(sale_sdt_09) sale_sdt_09,
        first_sale_day
from (
select province_code,
          province_name ,
          a.customer_no,
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
        (case when mon='202008' then sale_sdt end ) sale_sdt_08,
        (case when mon='202009' then  sales_value end ) sales_value_09,
        (case when mon='202009' then profit end ) profit_09,
        (case when mon='202009' then profit/sales_value end ) profit_rate_09,
        (case when mon='202009' then sale_sdt end ) sale_sdt_09
    
 ;

 ------------------------ 分割线 20200916----------------------------
 drop table  csx_tmp.temp_negative_profit_01;
create  table csx_tmp.temp_negative_profit_01
as 
 SELECT   mon,
          region_code,
          region_name,
          a.province_code,
          province_name ,
          a.customer_no,
          customer_name,
          attribute,
          first_sale_day,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
          sum(sale_sdt) as sale_num ,
          if(substr(first_sale_day,1,6)=mon,'是','否') as age_note
    
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
     AND sdt<='20200615'
     and channel in ('7','1','9')
    -- and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
    -- and a.return_flag!='X'
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no
    union all 
    SELECT substr(sdt,1,6) mon,
          province_code,
          province_name ,
          a.customer_no,
          coalesce(sum(sales_value),0)sales_value,
          coalesce(sum(profit),0) profit,
          coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
          coalesce(count(distinct sdt),0) as sale_sdt
   FROM csx_dw.dws_sale_r_d_customer_sale a 
   WHERE sdt>='20200701'
     AND sdt<='20200915'
     and channel in ('7','1','9')
     --and a.customer_no not in ('104444','102998')
     and is_self_sale =1
     and attribute_code =1
    -- and a.return_flag!='X'
   GROUP BY  
          substr(sdt,1,6) ,
          province_code,
          province_name ,
          a.customer_no  
         
    )a
left join 
(select region_code,region_name,province_code from csx_dw.dim_area where area_rank =13) b on a.province_code =b.province_code
left join 
(select customer_no,customer_name,attribute,attribute_code from csx_dw.dws_crm_w_a_customer_m_v1 where sdt='current')c  on a.customer_no =c.customer_no
left join 
(select customer_no ,first_sale_day from csx_dw.ads_sale_w_d_ads_customer_sales_q where sdt='20200914') d 
    on a.customer_no=d.customer_no
group by mon,
          region_code,
          region_name,
          a.province_code,
          province_name ,
          a.customer_no,
          customer_name,
          first_sale_day,
          attribute,
          if(substr(first_sale_day,1,6)=mon,'是','否');

-----------------------------------使用Impala 查询
---------负毛利客户 
select region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          `attribute`,
          age_note,
        (sales_value_04) sales_value_04,
        (profit_04) profit_04,
        (profit_rate_04) profit_rate_04,
        (sale_num_04) sale_num_04,
        (sales_value_05) sales_value_05,
        (profit_05) profit_05,
        (profit_rate_05) profit_rate_05,
        (sale_num_05) sale_num_05,
        (sales_value_06) sales_value_06,
        (profit_06) profit_06,
        (profit_rate_06) profit_rate_06,
        (sale_num_06) sale_num_06,
        (sales_value_07) sales_value_07,
        (profit_07) profit_07,
        (profit_rate_07) profit_rate_07,
        (sale_num_07) sale_num_07,
        (sales_value_08) sales_value_08,
        (profit_08) profit_08,
        (profit_rate_08) profit_rate_08,
        (sale_num_08) sale_num_08,
        (sales_value_09) sales_value_09,
        (profit_09) profit_09,
        (profit_rate_09) profit_rate_09,
        (sale_num_09) sale_num_09
from 
(select   region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          age_note,
          `attribute`,
          
        sum(case when mon='202004' then  sales_value end ) sales_value_04,
        sum(case when mon='202004' then profit end ) profit_04,
        sum(case when mon='202004' then profit/sales_value end ) profit_rate_04,
        sum(case when mon='202004' then sale_num end ) sale_num_04,
        sum(case when mon='202005' then  sales_value end ) sales_value_05,
        sum(case when mon='202005' then profit end ) profit_05,
        sum(case when mon='202005' then profit/sales_value end ) profit_rate_05,
        sum(case when mon='202005' then sale_num end ) sale_num_05,
        sum(case when mon='202006' then  sales_value end ) sales_value_06,
        sum(case when mon='202006' then profit end ) profit_06,
        sum(case when mon='202006' then profit/sales_value end ) profit_rate_06,
        sum(case when mon='202006' then sale_num end ) sale_num_06,
        sum(case when mon='202007' then  sales_value end ) sales_value_07,
        sum(case when mon='202007' then profit end ) profit_07,
        sum(case when mon='202007' then profit/sales_value end ) profit_rate_07,
        sum(case when mon='202007' then sale_num end ) sale_num_07,
        sum(case when mon='202008' then  sales_value end ) sales_value_08,
        sum(case when mon='202008' then profit end ) profit_08,
        sum(case when mon='202008' then profit/sales_value end ) profit_rate_08,
        sum(case when mon='202008' then sale_num end ) sale_num_08,
        sum(case when mon='202009' then  sales_value end ) sales_value_09,
        sum(case when mon='202009' then profit end ) profit_09,
        sum(case when mon='202009' then profit/sales_value end ) profit_rate_09,
        sum(case when mon='202009' then sale_num end ) sale_num_09
from  csx_tmp.temp_negative_profit_01
where  1=1
group by region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          `attribute`,
          age_note
) a where 1=1
and (round(profit_04,0)<0 or round(profit_05,0)<0 or profit_06<0 or profit_07<0 or profit_08<0 or profit_09<0) 
and (round(sales_value_04,0)>0 or round(sales_value_05,0)>0 or sales_value_06>0 or sales_value_07>0 or sales_value_08>0 or sales_value_09>0) 

-- ;
;

-----------------低毛利客户 ---------------------------

select region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          `attribute`,
          age_note,
          all_num,
        (sales_value_04) sales_value_04,
        (profit_04) profit_04,
        (profit_rate_04) profit_rate_04,
        (sale_num_04) sale_num_04,
        (sales_value_05) sales_value_05,
        (profit_05) profit_05,
        (profit_rate_05) profit_rate_05,
        (sale_num_05) sale_num_05,
        (sales_value_06) sales_value_06,
        (profit_06) profit_06,
        (profit_rate_06) profit_rate_06,
        (sale_num_06) sale_num_06,
        (sales_value_07) sales_value_07,
        (profit_07) profit_07,
        (profit_rate_07) profit_rate_07,
        (sale_num_07) sale_num_07,
        (sales_value_08) sales_value_08,
        (profit_08) profit_08,
        (profit_rate_08) profit_rate_08,
        (sale_num_08) sale_num_08,
        (sales_value_09) sales_value_09,
        (profit_09) profit_09,
        (profit_rate_09) profit_rate_09,
        (sale_num_09) sale_num_09,
        min_profit_rate,
         max_profit_rate
from 
(select   region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          age_note,
          `attribute`,
          sum(sale_num) as all_num,
          sum(sales_value) as sales_value ,
          min(profit_rate) min_profit_rate,
          max(profit_rate) max_profit_rate,
        sum(case when mon='202004' then  sales_value end ) sales_value_04,
        sum(case when mon='202004' then profit end ) profit_04,
        sum(case when mon='202004' then profit/sales_value end ) profit_rate_04,
        sum(case when mon='202004' then sale_num end ) sale_num_04,
        sum(case when mon='202005' then  sales_value end ) sales_value_05,
        sum(case when mon='202005' then profit end ) profit_05,
        sum(case when mon='202005' then profit/sales_value end ) profit_rate_05,
        sum(case when mon='202005' then sale_num end ) sale_num_05,
        sum(case when mon='202006' then  sales_value end ) sales_value_06,
        sum(case when mon='202006' then profit end ) profit_06,
        sum(case when mon='202006' then profit/sales_value end ) profit_rate_06,
        sum(case when mon='202006' then sale_num end ) sale_num_06,
        sum(case when mon='202007' then  sales_value end ) sales_value_07,
        sum(case when mon='202007' then profit end ) profit_07,
        sum(case when mon='202007' then profit/sales_value end ) profit_rate_07,
        sum(case when mon='202007' then sale_num end ) sale_num_07,
        sum(case when mon='202008' then  sales_value end ) sales_value_08,
        sum(case when mon='202008' then profit end ) profit_08,
        sum(case when mon='202008' then profit/sales_value end ) profit_rate_08,
        sum(case when mon='202008' then sale_num end ) sale_num_08,
        sum(case when mon='202009' then  sales_value end ) sales_value_09,
        sum(case when mon='202009' then profit end ) profit_09,
        sum(case when mon='202009' then profit/sales_value end ) profit_rate_09,
        sum(case when mon='202009' then sale_num end ) sale_num_09
from  csx_tmp.temp_negative_profit_01
where  1=1
-- AND MON BETWEEN '202004' AND '202006'
--and sales_value >0
--and round(profit_rate*100,2)BETWEEN 0 and 5
--and province_code='32'
--and customer_no='104703'
group by region_code,
          region_name,
          province_code,
          province_name ,
          customer_no,
          customer_name,
          first_sale_day,
          `attribute`,
          age_note
) a where 1=1
and sales_value >0
and (round(profit_rate_04,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_05,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_06,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_07,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_08,2) BETWEEN 0.00 and 0.05 
     or round(profit_rate_09,2) BETWEEN 0.00 and 0.05 
     ) 
--and (round(sales_value_04,0)>0 or round(sales_value_05,0)>0 or sales_value_06>0 ) 

-- ;
;   
