select * from csx_dw.dws_sale_r_d_customer_sale ;

refresh  csx_tmp.temp_peng_goods_sale;

select
    province_code,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    goods_code,
    goods_name,
    unit,
    sale_mon,
    avg_sale,
    sales_value,
    sales_value/sum()over(partition by 
                            case when division_code in ('11','10')then '11' 
                                 when division_code in ('12','13','14')then '12'
                                 else division_code end order by sales_value ) as sale_ratio,
    sale_cust,
    sale_sdt,
    max_sale,
    min_sale,
    b_sale_cust,
    max_sale_cust,
    min_sale_cust,
    b_sale_sdt,
    b_avg_sale_cust
from
    csx_tmp.temp_peng_goods_sale;
    
refresh csx_dw.ads_sale_province_daily_sales_report;
select * from csx_dw.csx_shop where sdt='current';

select * from csx_dw.dws_bbc_r_d_wshop_order_m;
select * from csx_ods.source_bbc_r_d_wshop_order;
select * from csx_dw.dwd_bbc_r_d_wshop_order_detail;

select
    province_code ,
    province_name,
    a.customer_no,
    customer_name,    
    attribute_name,
    first_category ,
    second_category ,
    case when second_category in ('事业单位','政府机关') then '政府/事业单位' 
         when second_category in ('部队','监狱') then '部队/监狱'
         when second_category in ('电力燃气水供应','金融业') then '电力/金融'
         when second_category in ('教育','医疗卫生') then '教育/医疗'
         else '制造业/其他'
         end as second_category_new ,
    sales_name,
    supervisor_name,
    city_manager_name,
    sale,
    profit,
    profit_rate,
    sales_sdt ,
    min_sdt,
    sign_date
from (
select
    province_code ,
    province_name,
    a.customer_no,
    customer_name,
   attribute as  attribute_name,
    first_category ,
    second_category ,
    sales_name,
    supervisor_name,
    city_manager_name,
    min_sdt,
    sign_date,
    sum(sales_value )sale,
    sum(profit )profit,
    round(sum(profit )/sum(sales_value ),4) as profit_rate,
    COUNT(DISTINCT sdt)sales_sdt 
from
    csx_dw.dws_sale_r_d_customer_sale  a 
    left join 
    (select customer_no ,min(sdt)min_sdt from csx_dw.customer_sales group by customer_no ) b on a.customer_no =b.customer_no
where
    sdt >= '20200601'
    and sdt <= '20200630'
    and attribute like '%日配%'
    and province_name not like '平台%'
group by 
    province_code ,
    province_name,
    min_sdt,
    sign_date,
    a.customer_no,
    customer_name,
    attribute ,
    first_category ,
    second_category ,
    sales_name,
    supervisor_name,
    city_manager_name
    ) a    
  left  JOIN 
   (select DISTINCT  customer_no from csx_dw.csx_partner_list where sdt='202006') b on a.customer_no=b.customer_no
    where b.customer_no is null 
    and profit_rate*100 <10
    AND profit_rate*100>=5;

select
    province_code ,
    province_name,
--    second_category ,
--    case when second_category in ('事业单位','政府机关') then '政府/事业单位' 
--         when second_category in ('部队','监狱') then '部队/监狱'
--         when second_category in ('电力燃气水供应','金融业') then '电力/金融'
--         when second_category in ('教育','医疗卫生') then '教育/医疗'
--         else '制造业/其他'
--         end as second_category_new ,
   count( a.customer_no) sale_cust
from (
select
    province_code ,
    province_name,
    customer_no,
    customer_name,
  attribute as   attribute_name,
    first_category ,
    second_category ,
    sales_name,
    supervisor_name,
    city_manager_name,
    min(sdt)min_sdt,
    sum(sales_value )sale,
    sum(profit )profit,
    ROUND( sum(profit )/sum(sales_value ),4) as profit_rate,
    COUNT(DISTINCT sdt)sales_sdt 
from
    csx_dw.dws_sale_r_d_customer_sale 
where
    sdt >= '20200601'
    and sdt <= '20200630'
   and `attribute`   like '%日配%'
   -- and channel in('1','7')
group by 
    province_code ,
    province_name,
    customer_no,
    customer_name,
    attribute,
    first_category ,
    second_category ,
    sales_name,
    supervisor_name,
    city_manager_name
    ) a
  left  JOIN 
   (select DISTINCT  customer_no from csx_dw.csx_partner_list where sdt='202006') b on a.customer_no=b.customer_no
    where b.customer_no is null 
--and profit_rate*100 >=5
and profit_rate*100>10
group by province_code ,
    province_name;
--    second_category ,
--    case when second_category in ('事业单位','政府机关') then '政府/事业单位' 
--         when second_category in ('部队','监狱') then '部队/监狱'
--         when second_category in ('电力燃气水供应','金融业') then '电力/金融'
--         when second_category in ('教育','医疗卫生') then '教育/医疗'
--         else '制造业/其他'
--         end;
select mon ,sum(aa),sum(bb),sum(aa-bb) from (
   select SUBSTRING(sdt,1,6) mon ,sum(sales_value+profit) aa,0 bb from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200101'and sdt<='20200630'  group by SUBSTRING(sdt,1,6) 
   UNION all
   select SUBSTRING(sdt,1,6)mon , 0 aa, sum(sales_value+profit) bb from csx_dw.customer_sales where sdt>='20200101' and sdt<='20200630' group by SUBSTRING(sdt,1,6) 
   ) a 
   group by mon ;