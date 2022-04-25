-- 供应链绩效（蔬菜、猪肉、大米）
-- 1、涉及:蔬菜、猪肉、大米
-- 2、日配业务剔除（W0K4、W0Z7、WB26、WB38）
-- 1、涉及:蔬菜、猪肉、大米
-- 2、日配业务剔除（W0K4、W0Z7、WB26、WB38）

set edt ='${enddate}';
set edate=regexp_replace(${hiveconf:edt},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set l_edate=regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','');
set l_sdate=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');


-- select ${hiveconf:l_sdate} , ${hiveconf:l_edate};

-- 日配销售额
drop table  csx_tmp.temp_sale_cl_01;
create temporary table  csx_tmp.temp_sale_cl_01 as 
select 
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    customer_no,
    sum(sales_cost) sales_cost,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(case when product_purchase_level_name='OEM商品' then sales_value end ) oem_sales,
    sum(case when product_purchase_level_name='OEM商品' then profit end ) oem_profit
from csx_dw.dws_sale_r_d_detail a 
left join
(select goods_id, product_purchase_level_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and product_purchase_level_name='OEM商品') b on a.goods_code=b.goods_id
where sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate}
    and channel_code in ('1','7','9')
    and business_type_code='1'
    and dc_code not in ('W0K4','W0Z7','WB26','WB38')
    and province_code !='34'
group by 
        classify_middle_code,
        province_code,province_name,city_group_code,city_group_name,
        classify_middle_name,
        customer_no
        ;
        
-- 环比        
drop table  csx_tmp.temp_sale_cl_02;
create temporary table csx_tmp.temp_sale_cl_02 as 
select 
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    customer_no,
    sum(sales_cost) sales_cost,
    sum(sales_qty) sales_qty,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(case when product_purchase_level_name='OEM商品' then sales_value end ) oem_sales,
    sum(case when product_purchase_level_name='OEM商品' then profit end ) oem_profit
from csx_dw.dws_sale_r_d_detail a 
left join
(select goods_id, product_purchase_level_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and product_purchase_level_name='OEM商品') b on a.goods_code=b.goods_id
where sdt>=${hiveconf:l_sdate} and sdt<=${hiveconf:l_edate}
    and channel_code in ('1','7','9')
    and business_type_code='1'
    and dc_code not in ('W0K4','W0Z7','WB26','WB38')
    and province_code !='34'
group by 
        classify_middle_code,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        classify_middle_name,
        customer_no
        ;


 -- 本期—+环期
 drop table  csx_tmp.temp_sale_cl_03;
 create temporary table csx_tmp.temp_sale_cl_03
 as 
 select 
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    customer_no,
    sum(sales_cost) sales_cost,
    sum(sales_qty) sales_qty,
    sum(sales_value)sales_value,
    sum(profit)profit,
    sum(oem_sales)oem_sales,
    sum(oem_profit)oem_profit,
    sum(last_sales_cost) last_sales_cost,
    sum(last_sales_qty)  last_sales_qty,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    sum(last_oem_sales) as last_oem_sales,
    sum(last_oem_profit) as last_oem_profit
from (
  select 
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    customer_no,
    sales_cost,
    sales_qty,
    sales_value,
    profit,
    oem_sales,
    oem_profit,
    0 as last_sales_cost,
    0 as last_sales_qty,
    0 as  last_sales_value,
    0 as  last_profit,
    0 as  last_oem_sales,
    0 as  last_oem_profit
from csx_tmp.temp_sale_cl_01
union all 
     select 
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    customer_no,
    0 as sales_cost,
    0 as sales_qty,
    0 as  sales_value,
    0 as  profit,
    0 as  oem_sales,
    0 as  oem_profit,
   sales_cost as last_sales_cost,
   sales_qty as last_sales_qty,
    sales_value  as  last_sales_value,
    profit       as  last_profit,
    oem_sales    as  last_oem_sales,
    oem_profit as    last_oem_profit
from csx_tmp.temp_sale_cl_02
)a 
group by province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    customer_no
    ;
    
    
-- 品类成交客户数

drop table  csx_tmp.temp_cust_01;
create temporary table csx_tmp.temp_cust_01 as 
select 
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.class_cust_num,
    last_class_cust_num,
    all_cust_num,
    last_all_cust_num
    from 
(select 
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    count(distinct case when sales_value>0 then customer_no end ) class_cust_num,
    count(distinct case when last_sales_value>0 then customer_no end ) last_class_cust_num
from csx_tmp.temp_sale_cl_03
    group by 
        city_group_code,
        city_group_name,
        province_code,
        province_name,
        classify_middle_code,
        classify_middle_name
)a 
left join 
(select 
    province_code,
    province_name,
    city_group_code,
    city_group_name, 
    count(distinct case when sales_value>0 then customer_no end ) all_cust_num,
    count(distinct case when last_sales_value>0 then customer_no end ) last_all_cust_num

from csx_tmp.temp_sale_cl_03
    group by 
        city_group_code,
        city_group_name,
        province_code,
        province_name 
) b on a.province_code=b.province_code and a.city_group_code=b.city_group_code ;

-- 猪肉销售成本

select a.* ,
    b.class_cust_num,
    last_class_cust_num,
    all_cust_num,
    last_all_cust_num,
    (a.sales_cost-last_sales_cost)/(last_sales_cost) as ring_cost_rate
from 
(select a.province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    sum(sales_cost) sales_cost,
    sum(sales_qty) sales_qty,
    sum(sales_value)sales_value,
    sum(profit)profit,
    sum(oem_sales)oem_sales,
    sum(oem_profit)oem_profit,
    sum(last_sales_cost) last_sales_cost,
    sum(last_sales_qty) last_sales_qty,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    sum(last_oem_sales) as last_oem_sales,
    sum(last_oem_profit) as last_oem_profit
    
 from csx_tmp.temp_sale_cl_03 a 
 group by 
    a.province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_middle_code,
    classify_middle_name
    )a
left join 
csx_tmp.temp_cust_01 b on a.province_code=b.province_code and a.city_group_code=b.city_group_code and a.classify_middle_code=b.classify_middle_code ;

