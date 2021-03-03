select channel_name ,
   province_code,
   province_name,
   a.customer_no ,
   customer_name ,
   first_category_code ,
   first_category_name ,
   second_category_code ,
   second_category_name ,
    sum(sales_qty)sales_qty,
    sum(sales_value )sale,
    sum(profit )profit , sum(profit )/sum(sales_value ) as profit_rate,
    count(DISTINCT goods_code ) as sale_sku
from
    csx_dw.dws_sale_r_d_detail a 
        
 where 1=1
 and sdt >= '${sdate}'
    and sdt <= '${edate}'
    and a.business_type_code ='4'
${if (len(prov)==0 ,"","and province_code in ('"+prov+"') " )}   
--  ${if(len(dept)==0,""," and department_code in ('"+dept+"')" )} 
  ${if(len(dept)==0,"","and department_code in ('"+SUBSTITUTE(dept,",","','")+"')")}
  ${if(len(cust)==0,""," and customer_no ='"+cust+"' ")}
    group by  
    channel_name ,
   province_code,
   province_name,
   a.customer_no ,
   customer_name ,
   first_category_code ,
   first_category_name ,
   second_category_code ,
   second_category_name 
   order by  province_code,a.customer_no ,
   customer_name ,
  first_category_code ,
   first_category_name ,
   second_category_code ,
   second_category_name ;