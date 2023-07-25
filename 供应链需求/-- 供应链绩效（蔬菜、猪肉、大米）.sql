-- 供应链绩效（蔬菜、猪肉、大米）
-- 1、涉及:蔬菜、猪肉、大米
-- 2、日配业务剔除（W0K4、W0Z7、WB26、WB38）
-- 猪肉仓 'W0A3','W0A2','W0N0','W0A8','W0BH','W0BK','W0Q2','W0A6','W0P8','W0A7','W0Q9','W0A5','W0R9','W0AS','W0BR'

set edt ='${enddate}';
set edate=regexp_replace(${hiveconf:edt},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set l_edate=regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','');
set l_sdate=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
set pig_shop=('W0A3','W0A2','W0N0','W0A8','W0BH','W0BK','W0Q2','W0A6','W0P8','W0A7','W0Q9','W0A5','W0R9','W0AS','W0BR');
set no_shop=('W0K4','W0Z7','WB26','WB38');


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
    and dc_code not in ${hiveconf:no_shop}
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
    coalesce(sum(case when product_purchase_level_name='OEM商品' then sales_value end ),0) oem_sales,
    coalesce(sum(case when product_purchase_level_name='OEM商品' then profit end ),0) oem_profit
from csx_dw.dws_sale_r_d_detail a 
left join
(select goods_id, product_purchase_level_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and product_purchase_level_name='OEM商品') b on a.goods_code=b.goods_id
where sdt>=${hiveconf:l_sdate} and sdt<=${hiveconf:l_edate}
    and channel_code in ('1','7','9')
    and business_type_code='1'
    and dc_code not in  ${hiveconf:no_shop}
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
    
    
-- 品类成交数

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

-- 全国成交数
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

--猪肉入库平均成本
drop table csx_tmp.pig_cost ;
create temporary table csx_tmp.pig_cost as 
select city_group_code,
       city_group_name,
       sales_province_code,
       sales_province_name,
       classify_middle_code,
    sum(last_receive_qty) last_receive_qty,
    sum(last_receive_amt) last_receive_amt,
    sum(last_receive_amt)/sum(last_receive_qty) last_avg_cost,
    sum(receive_qty)      receive_qty,
    sum(receive_amt)      receive_amt,
    sum(receive_amt) /sum(receive_qty) avg_cost
from (
select a.receive_location_code dc_code,
    a.classify_middle_code,
    sum(case when sdt>=${hiveconf:l_sdate} and sdt<=${hiveconf:l_edate} then   a.receive_qty end ) as last_receive_qty,
    sum(case when sdt>=${hiveconf:l_sdate} and sdt<=${hiveconf:l_edate} then a.amount end) last_receive_amt,
    sum(case when sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate} then   a.receive_qty end ) as receive_qty,
    sum(case when sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate} then a.amount end) receive_amt
from csx_dw.dws_wms_r_d_entry_batch a 
where sdt>=${hiveconf:l_sdate}  
    and sdt<=${hiveconf:edate} 
    and a.receive_location_code in ${hiveconf:pig_shop}
    and a.classify_middle_code='B0302'
group by a.receive_location_code,
        classify_middle_code
) a 
left join
(SELECT shop_id,
       city_group_code,
       city_group_name,
       sales_province_code,
       sales_province_name
FROM csx_dw.dws_basic_w_a_csx_shop_m
WHERE sdt='current') b on a.dc_code=b.shop_id
group by city_group_code,
       city_group_name,
       sales_province_code,
       sales_province_name,
       classify_middle_code
;



drop table  csx_tmp.temp_all_a;
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
    avg_cost as pig_avg_cost,
    last_avg_cost as last_pig_avg_cost,
    (avg_cost-last_avg_cost)/last_avg_cost as ring_cost_rate
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
  csx_tmp.temp_cust_02 c on a.classify_middle_code=c.classify_middle_code 
left join 
  csx_tmp.pig_cost d on a.province_code=d.sales_province_code and a.city_group_code=d.city_group_code and a.classify_middle_code=d.classify_middle_code
  ;

drop table csx_tmp.temp_rank_01;
create temporary table csx_tmp.temp_rank_01 as 
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
    diff_profit_rate,   --毛利率环比差
    case when sales_growth_rate<0 then 0 else  dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY sales_growth_rate desc) end  as sales_growth_rate_rank, --销售环比增长率排名
    case when diff_profit_rate<0 then 0 else dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY diff_profit_rate desc) end  as diff_profit_rate_rank,   --毛利率环比增长率排名
    dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY cust_p_rate desc) as cust_p_rate_rank,             --渗透率比增长率排名
    case when oem_sale_rate <=0 then 0  else dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY oem_sale_rate desc) end as oem_sale_rate_rank,         --OEM占比排名
    dense_rank()over(PARTITION BY group_aa,classify_middle_code ORDER BY ring_cost_rate asc) as ring_cost_rate_rank,       --猪肉平均成本环比排名
     -- 毛利率高于全国值
    diff_qg_profit_rate,
    dense_rank()over(PARTITION BY classify_middle_code ORDER BY diff_qg_profit_rate desc) as diff_qg_profit_rate_rank,
     -- 全国销售增长
    diff_qg_sale_rate,
    dense_rank()over(PARTITION BY classify_middle_code ORDER BY diff_qg_sale_rate desc) as diff_qg_sale_rate_rank,
     --渗透率高于全国
     diff_cust_r_rate,
      dense_rank()over(PARTITION BY classify_middle_code ORDER BY diff_cust_r_rate desc) as diff_qg_cust_rate_rank,
     --OEM全国占比
     diff_oem_sale_rate,
    case when oem_sales<=0 then 0 else  dense_rank()over(PARTITION BY classify_middle_code ORDER BY diff_oem_sale_rate desc) end  as diff_qg_oem_rate_rank,
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
    pig_avg_cost,
    last_pig_avg_cost,
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
    (profit_rate-(last_profit/last_sales_value)) as diff_profit_rate,            -- 毛利率环比
    all_sales_growth_rate,  -- 全国环比增长率
    all_profit_rate , -- 全国毛利率
    all_oem_sale_ratio ,   -- 全国OEM销售占比
    cust_p_rate,   --渗透率
    all_qg_p_rate ,   -- 全国渗透率
     -- 毛利率高于全国值
     profit_rate-all_profit_rate  as diff_qg_profit_rate,
     -- 全国销售增长
     sales_growth_rate-all_sales_growth_rate as diff_qg_sale_rate,
     --渗透率高于全国
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
    ring_cost_rate,
    pig_avg_cost,
    last_pig_avg_cost
FROM csx_tmp.temp_all_a a
WHERE city_group_name in ('福州市','北京市','成都市','重庆主城','合肥市','贵阳市','苏州市','松江区','南京市','杭州市','深圳市','西安市','郑州市','武汉市','石家庄市')
    and a.classify_middle_name in ('米','蔬菜','猪肉')
) a ;



set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.report_scm_r_d_category_rating partition(sdt)
SELECT a.province_code,
    a.province_name,
    group_aa,
    a.city_group_code,
    a.city_group_name,
    a.classify_middle_code,
    a.classify_middle_name,
    (diff_qg_sale_rate_integral+sales_growth_rate_rank_intrgral+diff_qg_profit_rate_rank_integral+diff_profit_rate_rank_intrgral+diff_cust_r_rate_integral+ cust_p_rate_rank_intrgral+diff_oem_sale_rate_integral+ oem_sale_rate_rank_integral+  ring_cost_rate_rank_integral ) as total_integral,
    diff_qg_sale_rate_integral,         -- 销售增长率高于全国值 5
    sales_growth_rate_rank_intrgral,    --销售环比增长率排名积分15、10、5
    diff_qg_profit_rate_rank_integral , -- 毛利率高于全国毛利率得分
    diff_profit_rate_rank_intrgral,     --毛利率环比增长率排名积分35、20、10
    diff_cust_r_rate_integral,         --渗透率高于全国得5分
    cust_p_rate_rank_intrgral,          --组内渗透率得分 5、3、2
    diff_oem_sale_rate_integral,        -- 销售占比高于或等于全国
    oem_sale_rate_rank_integral,        -- 组内OEM排名10、7、5
    ring_cost_rate_rank_integral,            --猪肉平均成本比增长率排名
    sales_cost/10000 sales_cost,
    sales_qty,
    sales_value/10000 sales_value,
    profit/10000 profit,
    profit_rate,                                          -- 毛利率
    oem_sales/10000 oem_sales,
    oem_sale_rate,                                        -- OEM占比
    oem_profit/10000 oem_profit,
    last_sales_cost/10000 last_sales_cost,
    last_sales_qty,
    last_sales_value/10000 last_sales_value,
    last_profit/10000 last_profit,
    (last_profit/last_sales_value) as last_profit_rate,
    last_oem_sales/10000 last_oem_sales,
    last_oem_profit/10000 last_oem_profit ,
    class_cust_num,
    last_class_cust_num,
    all_cust_num,
    last_all_cust_num,
    sales_growth_rate,           -- 销售环比增长率
    all_sales_growth_rate,       -- 全国环比增长率
    (profit_rate- (last_profit/last_sales_value)) diff_profit_rate,  -- 环比毛利率差
    all_profit_rate ,           -- 全国毛利率
    all_oem_sale_ratio ,         -- 全国OEM销售占比
    cust_p_rate,                 --渗透率
    all_qg_p_rate ,              -- 全国渗透率
    sales_growth_rate_rank,      --销售环比增长率排名
    diff_profit_rate_rank,       --毛利率环比增长率排名
    cust_p_rate_rank,            --渗透率比增长率排名
    oem_sale_rate_rank,            --渗透率比增长率排名
    ring_cost_rate_rank,
     -- 毛利率高于全国值
    diff_qg_profit_rate,
    diff_qg_profit_rate_rank,
     -- 销售环比高于或等于全国销售增长
     diff_qg_sale_rate,
     diff_qg_sale_rate_rank,
     --渗透率高于全国
     diff_cust_r_rate,
      diff_qg_cust_rate_rank,
     --OEM全国占比
    diff_oem_sale_rate,
    diff_qg_oem_rate_rank,
    qg_class_num,
    qg_cust_num,
    ring_cost_rate,
    pig_avg_cost,
    last_pig_avg_cost,
    current_timestamp(),
    ${hiveconf:edate}

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
    sales_growth_rate,           -- 销售环比增长率
    all_sales_growth_rate,       -- 全国环比增长率
    all_profit_rate ,           -- 全国毛利率
    all_oem_sale_ratio ,         -- 全国OEM销售占比
    cust_p_rate,                 --渗透率
    all_qg_p_rate ,              -- 全国渗透率
      -- 销售环比高于或等于全国销售增长
     diff_qg_sale_rate,
    if(diff_qg_sale_rate>=0,5,0) diff_qg_sale_rate_integral,  
    sales_growth_rate_rank,      --销售环比增长率排名
    case when sales_growth_rate_rank=1 then 15
         when sales_growth_rate_rank=2 then 10
         when sales_growth_rate_rank=3 then 5 
         else 0 end sales_growth_rate_rank_intrgral,    --销售环比增长率排名积分15、10、5
     -- 毛利率高于全国值
    diff_qg_profit_rate,
    diff_qg_profit_rate_rank,
    case when diff_qg_profit_rate_rank=1 then 15
         when diff_qg_profit_rate_rank=2 then 10
         when diff_qg_profit_rate_rank=3 then 5 
         else 0 end diff_qg_profit_rate_rank_integral , -- 毛利率高于全国毛利率得分
    diff_profit_rate_rank,       --毛利率环比增长率排名
    case when diff_profit_rate_rank=1 then 35
         when diff_profit_rate_rank=2 then 20
         when diff_profit_rate_rank=3 then 10 
         else 0 end diff_profit_rate_rank_intrgral,    --毛利率环比增长率排名积分35、20、10
      --渗透率高于全国
     diff_cust_r_rate,
     case when diff_cust_r_rate>=0 then 5 else 0 end diff_cust_r_rate_integral,         --渗透率高于全国得5分 
    cust_p_rate_rank,                                   --渗透率比增长率排名
    case when cust_p_rate_rank=1 then 5
         when cust_p_rate_rank=2 then 3
         else 2 end cust_p_rate_rank_intrgral,      --组内渗透率得分 5、3、2
    oem_sale_rate_rank,                             --渗透率比增长率排名
         --OEM全国占比
    diff_oem_sale_rate,
    case when diff_oem_sale_rate>=0 and oem_sales >0 then 10 else 0 end as diff_oem_sale_rate_integral, -- OEM占比高于全国
    case when oem_sale_rate_rank=1 then 10
         when oem_sale_rate_rank=2 then 7
         else 5 end oem_sale_rate_rank_integral,    -- 组内OEM排名10、7、5
    ring_cost_rate_rank,
    case when ring_cost_rate_rank=1 and a.classify_middle_code='B0302' then 20
         when ring_cost_rate_rank=2 and a.classify_middle_code='B0302' then 10    
         else 5 end ring_cost_rate_rank_integral,            --猪肉平均成本比增长率排名
     diff_qg_sale_rate_rank,
     diff_qg_cust_rate_rank,
    diff_qg_oem_rate_rank,
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
    ring_cost_rate,
    pig_avg_cost,
    last_pig_avg_cost
FROM csx_tmp.temp_rank_01 a 
) a ;
select * from csx_tmp.temp_rank_01 ;



create table csx_tmp.report_scm_r_d_category_rating 
(
province_code	string comment '省区',
province_name	string comment '省区名称',
group_aa	string comment '考核组',
city_group_code	string comment '城市组编码',
city_group_name	string comment '城市组名称',
classify_middle_code	string comment '管理二级编码',
classify_middle_name	string comment '管理二级名称',
total_integral	int comment '总分',
diff_qg_sale_rate_integral	int comment '销售环比高于或等于全国环比',
sales_growth_rate_rank_intrgral	int comment '销售环比增长率',
diff_qg_profit_rate_rank_integral	int comment '毛利率高于或等于全国环比',
diff_profit_rate_rank_intrgral	int comment '毛利率环比增长率',
diff_cust_r_rate_integral	int comment '渗透率高于全国',
cust_p_rate_rank_intrgral	int comment '组内渗透率',
diff_oem_sale_rate_integral	int comment 'OEM高于或等于全国',
oem_sale_rate_rank_integral	int comment 'OEM组内',
ring_cost_rate_rank_integral	int comment '猪肉平均成本环比',
sales_cost	decimal(26,6) comment '销售成本',
sales_qty	decimal(26,6) comment '销量',
sales_value	decimal(26,6) comment '销售额',
profit	decimal(26,6) comment '毛利额',
profit_rate	decimal(26,6) comment '毛利率',
oem_sales	decimal(26,6) comment 'OEM销售额',
oem_sale_rate	decimal(26,6) comment 'OEM销售占比',
oem_profit	decimal(26,6) comment 'OEM销售毛利',
last_sales_cost	decimal(26,6) comment '环比销售成本',
last_sales_qty	decimal(26,6) comment '环期销量',
last_sales_value	decimal(26,6) comment '环期销售额',
last_profit	decimal(26,6) comment '环期毛利额',
last_profit_rate	decimal(26,6) comment '环期毛利率',
last_oem_sales	decimal(26,6) comment '环期OEM销售额',
last_oem_profit	decimal(26,6) comment '环期OEM毛利额',
class_cust_num	decimal(26,6) comment '成交数',
last_class_cust_num	decimal(26,6) comment '环期成交数',
all_cust_num	decimal(26,6) comment 'B端成交数',
last_all_cust_num	decimal(26,6) comment '环期B端成交数',
sales_growth_rate	decimal(26,6) comment '销售额环比增长率',
all_sales_growth_rate	decimal(26,6) comment '全国销售增长率',
diff_profit_rate	decimal(26,6) comment '毛利率环比',
all_profit_rate	decimal(26,6) comment '全国毛利率',
all_oem_sale_ratio	decimal(26,6) comment '全国OEM占比',
cust_p_rate	decimal(26,6) comment '渗透率',
all_qg_p_rate	decimal(26,6) comment '全国渗透率',
sales_growth_rate_rank	int comment '销售环比排名',
diff_profit_rate_rank	int comment '毛利率排名',
cust_p_rate_rank	int comment '渗透率排名',
oem_sale_rate_rank	int comment 'OEM销售占比排名',
ring_cost_rate_rank	int comment '猪肉平均成本排名',
diff_qg_profit_rate	int comment '全国毛利率差',
diff_qg_profit_rate_rank	int comment '全国毛利率差排名',
diff_qg_sale_rate	int comment '全国销售环比率差',
diff_qg_sale_rate_rank	int comment '全国销售环比率差排名',
diff_cust_r_rate	int comment '全国渗透率差',
diff_qg_cust_rate_rank	int comment '全国渗透率差排名',
diff_oem_sale_rate	int comment '全国OEM占比差',
diff_qg_oem_rate_rank	int comment '全国OEM占比差排名',
qg_class_num	int comment '	全国管理二级成交数',
qg_cust_num	int comment '全国数',
ring_cost_rate	decimal(26,6) comment '猪肉入库平均成本环比增长率',
pig_avg_cost	decimal(26,6) comment '猪肉入库平均成本',
last_pig_avg_cost	decimal(26,6) comment '环期猪肉入库平均成本',
update_time	timestamp comment '更新时间'
) comment '供应链品类评比'
partitioned by (sdt string comment '日期分区')
stored as parquet
;