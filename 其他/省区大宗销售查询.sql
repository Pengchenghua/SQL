--省区
select 
 a.mon,
 a.prov_code,
 a.prov_name,
 sales,
 
 profit,
 profit_rate,
 sales_cust,
 sales/b_sale as b_sale_ration,
 sales/all_sales as sale_ration,
 all_sales
from (
select 
substr(sdt,1,6) as mon,
case when dc_code ='W0K4' then 'W0K4' else province_code end prov_code,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end prov_name,
sum(sales_value)sales,
sum(profit)profit,
sum(profit)/sum(sales_value) as profit_rate,
count(distinct customer_no) as sales_cust
from csx_dw.dws_sale_r_d_detail 
where sdt>='20201001' and sdt<'20210401' 
and (business_type_code='5' or dc_code='W0K4')
group by case when dc_code ='W0K4' then 'W0K4' else province_code end ,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end,
substr(sdt,1,6)
)a 
left join
(
select 
substr(sdt,1,6) as mon,
case when dc_code ='W0K4' then 'W0K4' else province_code end prov_code,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end prov_name,
sum(sales_value) as all_sales,
sum(case when channel_code!='2' then sales_value end ) as b_sale,
sum(profit)/sum(sales_value) as all_profit_rate,
count(distinct customer_no) as all_sales_cust
from csx_dw.dws_sale_r_d_detail 
where sdt>='20201001' and sdt<'20210401' 
-- and channel_code !='2'
group by case when dc_code ='W0K4' then 'W0K4' else province_code end ,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end,
substr(sdt,1,6)
) b on a.prov_code=b.prov_code and a.mon=b.mon

;SET hive.execution.engine=spark; 

select 
substr(sdt,1,6) as mon,
case when dc_code ='W0K4' then 'W0K4' else province_code end prov_code,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end prov_name,
goods_code,
goods_name,
sum(sales_value)sales,
sum(profit)profit,
sum(profit)/sum(sales_value) as profit_rate,
count(distinct customer_no) as sales_cust
from csx_dw.dws_sale_r_d_detail 
where sdt>='20201001' and sdt<'20210401' 
and (business_type_code='5' or dc_code='W0K4')
group by case when dc_code ='W0K4' then 'W0K4' else province_code end ,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end,
substr(sdt,1,6) ;



--省区

select 
 a.mon,
 sales,
 profit,
 profit_rate,
 sales_cust,
 sales/b_sale as b_sale_ration,
 sales/all_sales as sale_ration,
 all_sales
from (
select 
substr(sdt,1,6) as mon,
-- case when dc_code ='W0K4' then 'W0K4' else province_code end prov_code,
-- case when dc_code ='W0K4' then 'W0K4_联营' else province_name end prov_name,
sum(sales_value)sales,
sum(profit)profit,
sum(profit)/sum(sales_value) as profit_rate,
count(distinct customer_no) as sales_cust
from csx_dw.dws_sale_r_d_detail 
where sdt>='20201001' and sdt<'20210401' 
and (business_type_code='5' or dc_code='W0K4')
group by 
-- case when dc_code ='W0K4' then 'W0K4' else province_code end ,
-- case when dc_code ='W0K4' then 'W0K4_联营' else province_name end,
substr(sdt,1,6)
)a 
left join
(
select 
substr(sdt,1,6) as mon,
-- case when dc_code ='W0K4' then 'W0K4' else province_code end prov_code,
-- case when dc_code ='W0K4' then 'W0K4_联营' else province_name end prov_name,
sum(sales_value) as all_sales,
sum(case when channel_code!='2' then sales_value end ) as b_sale,
sum(profit)/sum(sales_value) as all_profit_rate,
count(distinct customer_no) as all_sales_cust
from csx_dw.dws_sale_r_d_detail 
where sdt>='20201001' and sdt<'20210401' 
-- and channel_code !='2'
group by 
substr(sdt,1,6)
) b on a.mon=b.mon

;


select case when dc_code ='W0K4' then 'W0K4' else province_code end prov_code,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end prov_name,
goods_code,
goods_name,
unit,
department_code,
department_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name,
sum(sales_value)sales,
sum(profit) profit,
sum(profit)/sum(sales_value)as  profit_rate,
count(distinct case when sales_value>0 then  customer_no end ) as sale_cust,
count(distinct case when sales_value>0 then  sdt end) as sale_sdt
from csx_dw.dws_sale_r_d_detail 
where sdt>='20210301' 
and sdt<'20210401' 
-- and (business_type_code='5'or 
and dc_code='W0K4'
group by 
case when dc_code ='W0K4' then 'W0K4' else province_code end ,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end ,
goods_code,
goods_name,
unit,
department_code,
department_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name;



select case when dc_code ='W0K4' then 'W0K4' else province_code end prov_code,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end prov_name,
customer_no,
customer_name,
second_category_name,
sum(sales_value)sales,
sum(profit) profit,
sum(profit)/sum(sales_value)as  profit_rate,
count(distinct goods_code) as sale_sku,
count(distinct sdt) as sale_sdt
from csx_dw.dws_sale_r_d_detail where sdt>='20210301' and sdt<'20210401' AND (business_type_code='5'or dc_code='W0K4')
group by 
case when dc_code ='W0K4' then 'W0K4' else province_code end ,
case when dc_code ='W0K4' then 'W0K4_联营' else province_name end ,
second_category_name,
customer_no,
customer_name;