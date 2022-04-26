-- 供应链绩效（蔬菜、猪肉、大米）
-- 1、涉及:蔬菜、猪肉、大米
-- 2、日配业务剔除（W0K4、W0Z7、WB26、WB38）
-- 1、涉及:蔬菜、猪肉、大米
-- 2、日配业务剔除（W0K4、W0Z7、WB26、WB38）
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
) b on a.province_code=b.province_code and a.city_group_code=b.city_group_code 

;


drop table  csx_tmp.temp_cust_02;
create temporary table csx_tmp.temp_cust_02 as 
select 
   
    a.classify_middle_code,
    a.classify_middle_name,
     all_cust_num,
     qg_cust_num
    from 
(select 
   
    a.classify_middle_code,
    a.classify_middle_name,
    count(distinct case when sales_value>0 then customer_no end )  all_cust_num
    from 
(select 
    classify_middle_code,
    classify_middle_name,
    customer_no,
    sum(sales_value) sales_value,
    sum(last_sales_value) last_sales_value
from csx_tmp.temp_sale_cl_03
    group by 
        customer_no,
        classify_middle_code,
        classify_middle_name
)a 
group by   a.classify_middle_code,
    a.classify_middle_name
)a 
left join 
(select 
    count(distinct case when sales_value>0 then customer_no end )  qg_cust_num
    from 
(select 
    customer_no,
    sum(sales_value) sales_value,
    sum(last_sales_value) last_sales_value
from csx_tmp.temp_sale_cl_03
    group by 
        customer_no
)a
) b on 1=1

;


create temporary table csx_tmp.temp_all_a as 
select a.province_code,
    a.province_name,
    case when a.city_group_name in ('福州市','北京市','成都市','重庆主城','合肥市') then '一组'
        when  a.city_group_name in  ('贵阳市','苏州市','松江区','南京市','杭州市') then '二组'
        when  a.city_group_name in  ('深圳市','西安市','郑州市','武汉市','石家庄市') then '三组'
        else '其他' end group_aa,
    a.city_group_code,
    a.city_group_name,
    a.classify_middle_code,
    a.classify_middle_name,
    sales_cost,
    sales_qty,
    sales_value,
    profit,
    profit/sales_value as profit_rate,                                          -- 毛利率
    oem_sales,
    oem_sales/sales_value oem_sale_rate,                                        -- OEM占比
    oem_profit,
   ( sales_value-last_sales_value)/(last_sales_value) as sales_growth_rate,     -- 环比增长率
   (sum(sales_value)over(partition by a.classify_middle_code)-sum(last_sales_value)over(partition by a.classify_middle_code))/sum(last_sales_value)over(partition by a.classify_middle_code) as all_sales_growth_rate,  -- 全国环比增长率
    (sum(profit)over(partition by a.classify_middle_code)/sum(sales_value)over(partition by a.classify_middle_code)) as all_profit_rate , -- 全国毛利率
    sum(oem_sales)over(partition by a.classify_middle_code)/sum(sales_value)over(partition by a.classify_middle_code) as all_oem_sale_ratio ,   -- 全国OEM销售占比
    class_cust_num/b.all_cust_num as cust_p_rate,   --渗透率
    c.all_cust_num/qg_cust_num as all_qg_p_rate ,   -- 全国渗透率
    c.all_cust_num as qg_class_num,
    qg_cust_num,
    last_sales_cost,
    last_sales_qty,
    last_sales_value,
    last_profit,
    last_oem_sales,
    last_oem_profit ,
    b.class_cust_num,
    last_class_cust_num,
    b.all_cust_num,
    last_all_cust_num,
    (a.sales_cost/sales_qty-last_sales_cost/last_sales_qty)/(last_sales_cost/last_sales_qty) as ring_cost_rate
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
csx_tmp.temp_cust_01 b on a.province_code=b.province_code and a.city_group_code=b.city_group_code and a.classify_middle_code=b.classify_middle_code
left join
  csx_tmp.temp_cust_02 c on a.classify_middle_code=c.classify_middle_code ;


SELECT a.province_code,
    a.province_name,
    group_aa,
    a.city_group_code,
    a.city_group_name,
    a.classify_middle_code,
    a.classify_middle_name,
    sales_cost,
    sales_qty,
    sales_value,
    profit,
    profit_rate,                                          -- 毛利率
    oem_sales,
    oem_sale_rate,                                        -- OEM占比
    oem_profit,
    sales_growth_rate,     -- 销售环比增长率
    all_sales_growth_rate,  -- 全国环比增长率
    all_profit_rate , -- 全国毛利率
    all_oem_sale_ratio ,   -- 全国OEM销售占比
    cust_p_rate,   --渗透率
    all_qg_p_rate ,   -- 全国渗透率
    dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY sales_growth_rate desc) as sales_growth_rate_rank, --销售环比增长率排名
    dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY diff_profit_rate desc) as diff_profit_rate_rank,   --毛利率环比增长率排名
    dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY cust_p_rate desc) as cust_p_rate_rank,   --渗透率比增长率排名
    dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY oem_sale_rate desc) as oem_sale_rate_rank,   --渗透率比增长率排名
    dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY ring_cost_rate desc) as ring_cost_rate_rank,   --渗透率比增长率排名
     -- 毛利率高于全国值
    diff_qg_profit_rate,
    dense_rank()over(PARTITION BY classify_middle_code ORDER BY diff_qg_profit_rate desc) as diff_qg_profit_rate_rank,
     -- 全国销售增长
    diff_qg_sale_rate,
     dense_rank()over(PARTITION BY classify_middle_code ORDER BY diff_qg_sale_rate desc) as diff_qg_sale_rate_rank,
     --客户渗透率高于全国
     diff_cust_r_rate,
      dense_rank()over(PARTITION BY classify_middle_code ORDER BY diff_cust_r_rate desc) as diff_qg_cust_rate_rank,
     --OEM全国占比
     diff_oem_sale_rate,
     dense_rank()over(PARTITION BY classify_middle_code ORDER BY diff_oem_sale_rate desc) as diff_qg_oem_rate_rank,
    qg_class_num,
    qg_cust_num,
    last_sales_cost,
    last_sales_qty,
    last_sales_value,
    last_profit,
    last_oem_sales,
    last_oem_profit ,
    class_cust_num,
    last_class_cust_num,
    all_cust_num,
    last_all_cust_num,
     ring_cost_rate
FROM(

SELECT a.province_code,
    a.province_name,
    group_aa,
    a.city_group_code,
    a.city_group_name,
    a.classify_middle_code,
    a.classify_middle_name,
    sales_cost,
    sales_qty,
    sales_value,
    profit,
    profit_rate,                                          -- 毛利率
    oem_sales,
    oem_sale_rate,                                        -- OEM占比
    oem_profit,
    sales_growth_rate,     -- 环比增长率
    (profit_rate-(last_profit/last_sales_value))/abs(last_profit/last_sales_value) as diff_profit_rate,            -- 毛利率环比
    all_sales_growth_rate,  -- 全国环比增长率
     all_profit_rate , -- 全国毛利率
     all_oem_sale_ratio ,   -- 全国OEM销售占比
    cust_p_rate,   --渗透率
     all_qg_p_rate ,   -- 全国渗透率
     -- 毛利率高于全国值
     profit_rate-all_profit_rate  as diff_qg_profit_rate,
     -- 全国销售增长
     sales_growth_rate-all_sales_growth_rate as diff_qg_sale_rate,
     --客户渗透率高于全国
     cust_p_rate-all_qg_p_rate as diff_cust_r_rate,
     --OEM全国占比
    oem_sale_rate-all_oem_sale_ratio as diff_oem_sale_rate,
    qg_class_num,
    qg_cust_num,
    last_sales_cost,
    last_sales_qty,
    last_sales_value,
    last_profit,
    last_oem_sales,
    last_oem_profit ,
    class_cust_num,
    last_class_cust_num,
    all_cust_num,
    last_all_cust_num,
    ring_cost_rate
FROM csx_tmp.temp_all_a a
WHERE city_group_name in ('福州市','北京市','成都市','重庆主城','合肥市','贵阳市','苏州市','松江区','南京市','杭州市','深圳市','西安市','郑州市','武汉市','石家庄市')
    and a.classify_middle_name in ('米','蔬菜','猪肉')

) a ;