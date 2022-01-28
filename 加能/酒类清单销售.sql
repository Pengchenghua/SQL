    
--酒类销售清单
drop table     csx_tmp.temp_sale_01;
create temporary table csx_tmp.temp_sale_01 as    
select province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    customer_name,
    goods_code,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(sales_qty) qty
 from csx_dw.dws_sale_r_d_detail 
 where sdt>='20220101'
 and goods_code in (
 '1386902','8718','8708','800682','909970','8649','1316197','1316198',
 '1316196','1017653','1128274','232454','6939','1288902','1418636',
 '715129','13523','1092359','1092910','825041','1422934','1422935',
 '451849','1474830','1474769','1131104','1275847','1131103','1339644',
 '1474814','8619','1479155','1479313','8623','48985','1160045','8621',
 '14631','8613','8625','8624','227048','226574')
 group by customer_no,
    customer_name,
    province_code,
    province_name,
    city_group_code,
    goods_code,
    city_group_name
;
drop table     csx_tmp.temp_sale_02;
create temporary table csx_tmp.temp_sale_02 as  
select a.province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.business_type_name,
    a.customer_no,
    customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_large_name,
    a.classify_middle_name,
    a.classify_small_name,
    sales_value,
    profit,
    qty,
    if(b.goods_code is null ,0,1)as aa
from (
select a.province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.business_type_name,
    a.customer_no,
    customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_large_name,
    a.classify_middle_name,
    a.classify_small_name,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(sales_qty) qty
 from csx_dw.dws_sale_r_d_detail a 
 join 
 (select distinct customer_no,province_code from  csx_tmp.temp_sale_01) b on a.customer_no=b.customer_no and a.province_code = b.province_code
 where sdt>='20220101'
 group by a.province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.customer_no,
    customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_large_name,
    a.classify_middle_name,
    a.classify_small_name,
    a.business_type_name
)a 
left join
 (select distinct goods_code from  csx_tmp.temp_sale_01) b on a.goods_code=b.goods_code

 ;
 
 select a.province_code,
    province_name,
    a.customer_name,
    a.customer_no,
    count(distinct customer_no)sale_cust,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(profit)/sum(sales_value) profit_rate,
    sum(qty) qty
   
from csx_tmp.temp_sale_02 a
group by a.province_code,
    province_name,
     a.customer_name,
    a.customer_no
;


 
 select a.province_code,
    province_name,

    count(distinct customer_no)sale_cust,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(profit)/sum(sales_value) profit_rate,
    sum(qty) qty
   
from csx_tmp.temp_sale_02 a
group by a.province_code,
    province_name
;


    
--酒类销售清单
drop table     csx_tmp.temp_sale_01;
create temporary table csx_tmp.temp_sale_01 as    
select province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    customer_name,
    goods_code,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(sales_qty) qty
 from csx_dw.dws_sale_r_d_detail 
 where sdt>='20220101'
 and goods_code in ('8649','8708','8718','800682','909970','1017653','1316198',
 '1316196','1316197','1386902','1128274 ','232454 ','1288902 ','13532','6939','13523','451849')
 group by customer_no,
    customer_name,
    province_code,
    province_name,
    city_group_code,
    goods_code,
    city_group_name
;


drop table     csx_tmp.temp_sale_02;
create temporary table csx_tmp.temp_sale_02 as  
select a.province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.business_type_name,
    a.customer_no,
    customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_large_name,
    a.classify_middle_name,
    a.classify_small_name,
    sales_value,
    profit,
    qty,
    if(b.goods_code is null ,0,1)as aa
from (
select a.province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.business_type_name,
    a.customer_no,
    customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_large_name,
    a.classify_middle_name,
    a.classify_small_name,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(sales_qty) qty
 from csx_dw.dws_sale_r_d_detail a 
 join 
 (select distinct customer_no,province_code from  csx_tmp.temp_sale_01) b on a.customer_no=b.customer_no and a.province_code = b.province_code
 where sdt>='20220101'
 group by a.province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.customer_no,
    customer_name,
    a.goods_code,
    a.goods_name,
    a.classify_large_name,
    a.classify_middle_name,
    a.classify_small_name,
    a.business_type_name
)a 
left join
 (select distinct goods_code from  csx_tmp.temp_sale_01) b on a.goods_code=b.goods_code

 ;
 
 
 
 select a.province_code,
    province_name,
    a.customer_name,
    a.customer_no,
    aa,
    count(distinct customer_no)sale_cust,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(profit)/sum(sales_value) profit_rate,
    sum(qty) qty
   
from csx_tmp.temp_sale_02 a
group by a.province_code,
    province_name,
     a.customer_name,
    a.customer_no,
    aa
;


 
 select a.province_code,
    province_name,
   
    count(distinct customer_no)sale_cust,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(profit)/sum(sales_value) profit_rate,
    sum(qty) qty
   
from csx_tmp.temp_sale_02 a
group by a.province_code,
    province_name
;

select province_code,
    province_name,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(sales_qty) qty
 from csx_dw.dws_sale_r_d_detail 
 where sdt>='20210101'
    and sdt<'20220101'
 and goods_code in ('8649','8708','8718','800682','909970','1017653','1316198',
 '1316196','1316197','1386902','1128274 ','232454 ','1288902 ','13532','6939','13523','451849')
 group by 
    province_code,
    province_name
;


select * from csx_tmp.temp_sale_02 a 