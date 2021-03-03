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