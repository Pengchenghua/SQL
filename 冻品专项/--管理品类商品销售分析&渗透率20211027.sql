--管理品类商品销售分析&渗透率20211027

set  mapreduce.job.reduces = 100;
set hive.exec.reducers.max=299;  -- reduce 个数，默认 299 不建议
-- set  hive.map.aggr = true;
-- set  hive.groupby.skewindata = false;
set hive.exec.parallel.thread.number = 16;
set  hive.exec.parallel = true;
set  hive.exec.dynamic.partition = true;
set hive.merge.size.per.task=128000000;  --合并后的文件大小 默认 128M
set hive.merge.smallfiles.avgsize=6400000; --平均最小文件大小合并 默认 64M
--启动态分区
set  hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
set  hive.exec.max.dynamic.partitions = 10000;
--在所有执行mr的节点上，最大一共可以创建多少个动态分区。
set  hive.exec.max.dynamic.partitions.pernode = 100000;
--源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错

--每个Map最大输入大小(这个值决定了合并后文件的数量)  
set mapred.max.split.size=256000000;    
--一个节点上split的至少的大小(这个值决定了多个DataNode上的文件是否需要合并)  
set mapred.min.split.size.per.node=64000000;  
--一个交换机下split的至少的大小(这个值决定了多个交换机上的文件是否需要合并)    
set mapred.min.split.size.per.rack=64000000;  
--执行Map前进行小文件合并  
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;   


set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set s_dt=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set last_sdt=regexp_replace(add_months(trunc(${hiveconf:edt},'MM'),-1),'-','');
--上月结束日期，当前日期不等于月末取当前日期，等于月末取上月最后一天
set last_edt=regexp_replace(if(${hiveconf:edt}=last_day(${hiveconf:edt}),last_day(add_months(${hiveconf:edt},-1)),add_months(${hiveconf:edt},-1)),'-','');
set s_dt_30 =regexp_replace(date_sub(${hiveconf:edt},30),'-','');
-- set parquet.compression=snappy;
-- set hive.exec.dynamic.partition=true; 
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;


-- 本期数据 (不含城市服务商)

drop table if exists csx_tmp.tmp_dp_goods_sale;
create temporary table csx_tmp.tmp_dp_goods_sale
as 
select 
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.customer_no,
    a.goods_code,
    joint_purchase_flag,
    business_type_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sum(case when sdt>=${hiveconf:s_dt} and sdt<=${hiveconf:e_dt}  then  sales_qty end ) as  sales_qty,
    sum(case when sdt>=${hiveconf:s_dt} and sdt<=${hiveconf:e_dt}  then a.sales_value end ) as  sales_value,
    sum(case when sdt>=${hiveconf:s_dt} and sdt<=${hiveconf:e_dt}  then a.profit  end) as  profit,
    sum(case when sdt>=${hiveconf:last_sdt} and sdt<=${hiveconf:last_edt}  then  sales_qty end ) as last_sales_qty,
    sum(case when sdt>=${hiveconf:last_sdt} and sdt<=${hiveconf:last_edt}  then a.sales_value end ) as last_sales_value,
    sum(case when sdt>=${hiveconf:last_sdt} and sdt<=${hiveconf:last_edt}  then a.profit  end) as last_profit
from csx_dw.dws_sale_r_d_detail a 
join 
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' )b on a.dc_code=b.shop_code and a.goods_code=b.product_code
where sdt<=${hiveconf:e_dt} and sdt>=${hiveconf:last_sdt} 
    and a.business_type_code !='4'
    and a.channel_code  in ('1','7','9')
   -- and a.classify_middle_code='B0304'
group by 
    case when channel_code in ('1','9','7') then 'B端' end,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    joint_purchase_flag,
    business_type_code ,
    a.customer_no,
    a.goods_code,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.province_code,
    a.province_name,
    a.customer_no
;



-- 商品层级销售额/成交客户数
drop table if exists csx_tmp.temp_sale_all_01;
create temporary table csx_tmp.temp_sale_all_01 as 
select
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_qty) as goods_sales_qty,
    sum(sales_value) as goods_sales,
    sum(profit) as goods_profit,
    sum(last_sales_qty) as last_goods_sales_qty,
    sum(last_sales_value) as last_goods_sales,
    sum(last_profit) as last_goods_profit,
    count(distinct case when sales_value>0 then customer_no end ) as goods_cust_number, --商品成交客户数
    count(distinct case when last_sales_value>0 then customer_no end )as last_goods_cust_number,  --商品环比冻品成交客户数
    grouping__id
from csx_tmp.tmp_dp_goods_sale a
where 1=1
-- and classify_middle_code  in('B0304','B0305')
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name
grouping sets
((channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),  --8191 省区商品明细    
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name),   -- 31 省区汇总
    (channel_name,
    region_code,
    region_name,
    goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),  --8167 大区商品统计
    (channel_name,
    region_code,
    region_name),  --7 大区汇总
    (channel_name,
    goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name), --8161 全国商品汇总
    ()   --0 
)
;

   
-- 总客户数
drop table if exists  csx_tmp.temp_sale_cust;
create  temporary table csx_tmp.temp_sale_cust as 
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    count(  case when b_sales_value>0 then customer_no end) as b_cust_number,
    count(  case when last_b_sales_value>0 then customer_no end ) as last_b_cust_number,
    -- sum(frozen_sales_qty) as frozen_sales_qty,
    -- sum(frozen_sales) as frozen_sales,
    -- sum(frozen_profit) as frozen_profit,
    sum(b_sales_value) as  b_sales_value,
    sum(b_profit) as b_profit,
    sum(last_b_sales_value) as last_b_sales_value,
    sum(last_b_profit) as last_b_profit,
    grouping__id
from
(
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    customer_no,
    -- sum(case when classify_middle_code in('B0304','B0305') then sales_qty end) as frozen_sales_qty,
    -- sum(case when classify_middle_code in('B0304','B0305') then sales_value end) as frozen_sales,
    -- sum(case when classify_middle_code in('B0304','B0305') then profit end) as frozen_profit,
    sum(sales_value) as b_sales_value,
    sum(profit) as b_profit,
    sum(last_sales_value) as last_b_sales_value,
    sum(last_profit) as last_b_profit
from csx_tmp.tmp_dp_goods_sale
where 1=1
--AND region_code='7'
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    customer_no
) a
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name
grouping sets
    ((channel_name,
     region_code,
    region_name,
    province_code,
    province_name),
    (channel_name,
    region_code,
    region_name),
    ())
;


--- 计算30日冻品销售额
drop table if exists csx_tmp.temp_sale_30day;
create temporary table csx_tmp.temp_sale_30day as 
select 
    region_code,
    region_name,
    province_code,
    province_name,
    a.goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(a.sales_qty  ) as frozen_sales_qty_30day,
    sum(a.sales_value ) as frozen_sales_30day,
    sum(a.profit) as frozen_profit_day,
    grouping__id
from csx_dw.dws_sale_r_d_detail a 
join 
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' )b on a.dc_code=b.shop_code and a.goods_code=b.product_code
where sdt>${hiveconf:s_dt_30} 
    and sdt<=${hiveconf:e_dt}
    and a.business_type_code !='4'
    and a.channel_code  in ('1','7','9')
  --  and classify_middle_code  in('B0304','B0305')
group by province_code,
    province_name,
    region_code,
    region_name,
    goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
grouping sets
    (
    (region_code,
    region_name,
    province_code,
    province_name,
    goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
     (
     region_code,
     region_name,
     province_code,
     province_name),
     (region_code,
    region_name,
    goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name),
     (
     region_code,
     region_name ),
    (goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name), ());


-- 统计期末库存
drop table if exists csx_tmp.temp_sale_02;
create temporary table csx_tmp.temp_sale_02 as 
select 
zone_id  sales_region_code,
dist_code,
a.goods_code,
joint_purchase_flag,
classify_large_code,
classify_middle_code,
classify_small_code,
sum(a.qty) final_qty,
sum(a.amt)final_amt ,
grouping__id
from csx_dw.dws_wms_r_d_accounting_stock_m a  
join 
(select shop_code,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' )s on a.dc_code=s.shop_code and a.goods_code=s.product_code
join 
(select 
    sales_province_code as dist_code,
    sales_province_name,
    city_group_code,
    city_group_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end zone_id,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '900002' 
        when shop_id in ('WB09') then '900003'
        WHEN province_name LIKE '%江苏%' and city_name='南京市' then '320100'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '320500' 
    else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' 
        when shop_id in ('W0G1','W0J8','W0H1')  then '大宗一' 
        when shop_id in ('WB09') then '平台酒水'
       WHEN province_name LIKE '%江苏%' and city_name='南京市' then '南京市'
        when province_name LIKE '%江苏%' and city_name !='南京市' then '昆山市' 
    else  province_name  end province_name,
    purpose,
    purpose_name
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current'    
    and  table_type=1 
    ) b on a.dc_code = b.shop_id 
where sdt=${hiveconf:e_dt}
and  reservoir_area_code  not in ('PD01','PD02','TS01')
and zone_id!='9'
    and b.shop_id not in ('W0J8','W0K4')
    and purpose in ('01', '07')
    and a.sys='new'
-- and classify_middle_code  in('B0304','B0305')
group by 
zone_id,
dist_code,
a.goods_code,
joint_purchase_flag,
classify_large_code,
classify_middle_code,
classify_small_code
grouping sets
((zone_id,
dist_code,
a.goods_code,
joint_purchase_flag,
classify_large_code,
classify_middle_code,
classify_small_code),
(zone_id,
dist_code),
(zone_id,
a.goods_code,
joint_purchase_flag,
classify_large_code,
classify_middle_code,
classify_small_code),
(zone_id),
(
classify_large_code,
classify_middle_code,
classify_small_code,
a.goods_code,
joint_purchase_flag),());

--写入明细层

drop table if exists csx_tmp.temp_all_sale;
create temporary table csx_tmp.temp_all_sale as 
select a.channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.goods_code,
    joint_purchase_flag,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    goods_sales_qty,
    goods_sales,
    goods_profit,
    last_goods_sales_qty,
    last_goods_sales,
    last_goods_profit,
    goods_cust_number,                                  --商品成交客户数
    last_goods_cust_number,                             --环期商品成交客户数
    b.b_sales_value,                                     --B端销售额
    b.b_profit,                                         --B端销售毛利
    b.b_cust_number,                                    --B端成交客户
    b.last_b_sales_value,                               --环期B端销售额
    b.last_b_profit,                                    --环期B端毛利额
    b.last_b_cust_number,                               --环期B端成交客户
    sum(goods_sales_qty)over(partition by classify_middle_code,a.province_code,a.region_name ) as classify_sales_qty,--管理分类销量
    sum(goods_sales)over(partition by classify_middle_code,a.province_code,a.region_name ) as classify_sales_value,                                     --管理分类销售额
    a.goods_sales/sum(goods_sales)over(partition by classify_middle_code,a.province_code,a.region_name ) as goods_sales_ratio,    -- 商品销售/管理二级分类销售占比
    a.goods_sales_qty/sum(goods_sales_qty)over(partition by classify_middle_code,a.province_code,a.region_name ) as goods_sales_qty_ratio,    -- 商品销售量/管理二级分类销售量占比
    coalesce((goods_sales-last_goods_sales)/last_goods_sales,0) as goods_ring_sales_ratio , --销售环比增长率
    goods_profit/goods_sales as frozen_profit_rate,  --商品定价毛利率
    goods_profit/goods_sales-last_goods_profit/last_goods_sales as goods_diff_profit_rate,     --毛利率差
    goods_cust_number/b_cust_number as cust_penetration_rate ,   ---商品渗透率
    last_goods_cust_number/last_b_cust_number as last_cust_penetration_rate ,   ---环期商品渗透率
    coalesce(goods_cust_number/b_cust_number - last_goods_cust_number/last_b_cust_number ,0) as diff_cust_penetration_rate ,   --- 商品渗透率环比
    goods_sales/b_sales_value as b_sales_ratio,   -- B端销售额/省区日配占比
    last_goods_sales/last_b_sales_value as last_b_sales_ratio,    -- B端销售额/省区日配占比
    a.grouping__id
from csx_tmp.temp_sale_all_01 a 
left join 
(select * from csx_tmp.temp_sale_cust ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'') and coalesce(a.region_code,'')=coalesce(b.region_code ,'')

where 1=1
-- and (classify_middle_code  in('B0304','B0305') or a.grouping__id in ('0','7','31') ) 
;


-- insert overwrite table csx_tmp.report_sale_r_d_frozen_goods_fr partition(months)
drop table csx_tmp.temp_report_scm_r_d_goods_sale_fr ;
create table csx_tmp.temp_report_scm_r_d_goods_sale_fr as 
select
    case when a.grouping__id = '0' then '0'
     when a.grouping__id = '8161' then '1' 
    when a.grouping__id = '7' then '2' 
    when a.grouping__id='8167' then '3' 
    when a.grouping__id='31' then '4' 
    else '5' end level_id,  --分组：0 全国，1 全国管理分类，2 大区，3大区管理分类 4省区，5省区管分类
    substr(${hiveconf:e_dt} ,1,4) as years,
    substr(${hiveconf:e_dt} ,1,6) as smonth,
    coalesce(a.channel_name,'B端')as channel_name,
    coalesce(a.region_code,'00')as region_code,
    coalesce(a.region_name,'全国')as region_name,
    coalesce(a.province_code,'00') as  province_code,
    coalesce(a.province_name,'小计') as province_name,
    coalesce(a.goods_code,'') as goods_code,
    coalesce(goods_name,'')as goods_name,
    coalesce(unit_name,'')as unit_name,
    coalesce(a.joint_purchase_flag,'')  as joint_purchase_flag,
    coalesce(a.classify_large_code,'') as  classify_large_code,
    coalesce(a.classify_large_name,'') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'') as classify_small_code,
    coalesce(a.classify_small_name,'') as classify_small_name,
    coalesce(goods_sales_qty,0) as goods_sales_qty,
    coalesce(goods_sales,0) as goods_sales,
    coalesce(goods_profit,0) as goods_profit,
    coalesce(last_goods_sales_qty,0) as last_goods_sales_qty,
    coalesce(last_goods_sales,0) as last_goods_sales,
    coalesce(last_goods_profit,0) as last_goods_profit,
    coalesce(goods_cust_number,    0) as goods_cust_number,                                  --商品成交客户数
    coalesce(last_goods_cust_number,0) as last_goods_cust_number,                             --环期商品成交客户数
    coalesce(b_sales_value, 0) as b_sales_value,                                     --B端销售额
    coalesce(b_profit, 0) as b_profit,                                         --B端销售毛利
    coalesce(b_cust_number,0) as b_cust_number,                                    --B端成交客户
    coalesce(last_b_sales_value, 0) as last_b_sales_value,                               --环期B端销售额
    coalesce(last_b_profit,0) as last_b_profit,                                    --环期B端毛利额
    coalesce(last_b_cust_number, 0) as last_b_cust_number,                               --环期B端成交客户
    coalesce(classify_sales_qty,  0) as classify_sales_qty,                                 --冻品销量
    coalesce(classify_sales_value, 0) as classify_sales_value,                                     --冻品销售额
    coalesce(goods_sales/classify_sales_value,0) as goods_sales_ratio,    -- 商品销售/冻品销售占比
    coalesce(goods_sales_qty/classify_sales_qty,0) as goods_sales_qty_ratio,    -- 商品销售量/冻品销售量占比
    case when coalesce(last_goods_sales,0)=0 and coalesce (goods_sales,0)>0  then 1 
        else coalesce((goods_sales-coalesce(last_goods_sales,0))/last_goods_sales,0) 
    end as goods_ring_sales_rate , --销售环比增长率
    case when coalesce(last_goods_sales_qty,0)=0 and  coalesce(goods_sales_qty,0)>0 then 1 
        else  coalesce((goods_sales_qty-coalesce(last_goods_sales_qty,0))/last_goods_sales_qty,0)
    end as goods_ring_sales_qty_rate , --销售量环比增长率
    goods_profit/goods_sales as goods_profit_rate,  --商品定价毛利率
    coalesce(goods_profit/goods_sales,0)- coalesce(last_goods_profit/last_goods_sales,0) as goods_diff_profit_rate,     --毛利率差
    coalesce(goods_cust_number/b_cust_number,0) as cust_penetration_rate ,   ---商品渗透率
    coalesce(last_goods_cust_number/last_b_cust_number,0) as last_cust_penetration_rate ,   ---环期商品渗透率
    coalesce(goods_cust_number/b_cust_number,0) - coalesce(last_goods_cust_number/last_b_cust_number ,0)  as diff_cust_penetration_rate ,   --- 商品渗透率环比
    coalesce( goods_sales/b_sales_value,0) as b_sales_ratio,   -- B端销售额/省区日配占比
    coalesce( last_goods_sales/last_b_sales_value,0) as last_b_sales_ratio,    -- B端销售额/省区占比
    coalesce( goods_sales/b_sales_value,0) - coalesce(last_goods_sales/last_b_sales_value,0)  as b_diff_sale_ratio,  --商品占省区占比环比差
    b.frozen_sales_qty_30day as goods_sales_qty_30day,  --滚动30天销量
    b.frozen_sales_30day as goods_sales_30day,      --滚动30天销售额    
    final_qty,      --期末库存量
    final_amt ,     --期末库存额
    a.grouping__id,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from csx_tmp.temp_all_sale  a
left join csx_tmp.temp_sale_30day b on coalesce(a.province_code,'')= coalesce(b.province_code,'')  and coalesce(a.goods_code,'')=coalesce(b.goods_code,'')
    and coalesce(a.joint_purchase_flag,'')=coalesce(b.joint_purchase_flag,'')
    and coalesce(a.classify_small_code,'')=coalesce(b.classify_small_code,'') and  coalesce(a.region_code,'')=coalesce(b.region_code ,'')
left join csx_tmp.temp_sale_02 d on coalesce(a.province_code,'')=coalesce(dist_code ,'')
    and coalesce(a.goods_code,'')=coalesce(d.goods_code,'') 
    and coalesce(a.joint_purchase_flag,'')=coalesce(d.joint_purchase_flag,'')
    and coalesce(a.classify_small_code,'')=coalesce(d.classify_small_code,'')
    and  coalesce(a.region_code,'')=coalesce(d.sales_region_code,'')
left join 
(select goods_id,goods_name,unit_name from  csx_dw.dws_basic_w_a_csx_product_m where sdt='current') m on a.goods_code=m.goods_id

;

select * from  csx_tmp.temp_report_scm_r_d_goods_sale_fr where region_code ='00' ;
CREATE TABLE `csx_tmp.report_scm_r_d_goods_sale_B_fr`(
  `level_id` string COMMENT '层级0 全国,1 全国商品,2大区,3大区商品,4 省区,5省区商品', 
  `years` string COMMENT '销售年', 
  `smonth` string COMMENT '销售月', 
  `channel_name` string COMMENT '渠道', 
  `region_code` string COMMENT '大区编码', 
  `region_name` string COMMENT '大区名称', 
  `province_code` string COMMENT '省区编码', 
  `province_name` string COMMENT '省区名称', 
  `goods_code` string COMMENT '商品编码', 
  `goods_name` string COMMENT '商品名称', 
  `unit_name` string COMMENT '单位', 
  `joint_purchase_flag` string COMMENT '是否联采 0 否 1是', 
  `classify_large_code` string COMMENT '管理一级分类', 
  `classify_large_name` string COMMENT '管理一级分类名称', 
  `classify_middle_code` string COMMENT '管理二级分类', 
  `classify_middle_name` string COMMENT '管理二级分类', 
  `classify_small_code` string COMMENT '管理三级分类', 
  `classify_small_name` string COMMENT '管理三级分类', 
  `goods_sales_qty` decimal(38,6) COMMENT '商品销售量', 
  `goods_sales` decimal(38,6) COMMENT '商品销售额', 
  `goods_profit` decimal(38,6) COMMENT '商品毛利额', 
  `last_goods_sales_qty` decimal(38,6) COMMENT '环期商品销量', 
  `last_goods_sales` decimal(38,6) COMMENT '环期商品销售额', 
  `last_goods_profit` decimal(38,6) COMMENT '环期商品毛利额', 
  `goods_cust_number` bigint COMMENT '商品成交客户数', 
  `last_goods_cust_number` bigint COMMENT '商品成交客户数环期', 
  `b_sales_value` decimal(38,6) COMMENT 'B端总销售额（剔除城市服务商）', 
  `b_profit` decimal(38,6) COMMENT 'B端总毛利额（剔除城市服务商）', 
  `b_cust_number` bigint COMMENT 'B端成交客户数（剔除城市服务商）', 
  `last_b_sales_value` decimal(38,6) COMMENT '环期B端总销售额（剔除城市服务商）', 
  `last_b_profit` decimal(38,6) COMMENT '环期B端总毛利额（剔除城市服务商）', 
  `last_b_cust_number` bigint COMMENT '环期B端成交客户数（剔除城市服务商）', 
  `frozen_sales_qty` decimal(38,6) COMMENT '冻品总销售量', 
  `frozen_sales` decimal(38,6) COMMENT '冻品总销售额', 
  `goods_sales_ratio` decimal(38,6) COMMENT '商品销售额占比=商品销售额/冻品销售额', 
  `goods_sales_qty_ratio` decimal(38,6) COMMENT '商品销售量占比=商品销售额/冻品销售额', 
  `goods_ring_sales_rate` decimal(38,6) COMMENT '商品销售额环比增长率', 
  `goods_ring_sales_qty_rate` decimal(38,6) COMMENT '商品销量环比增长率', 
  `goods_profit_rate` decimal(38,6) COMMENT '商品毛利率', 
  `goods_diff_profit_rate` decimal(38,6) COMMENT '商品环比毛利率差', 
  `cust_penetration_rate` decimal(38,6) COMMENT '商品客户渗透率', 
  `last_cust_penetration_rate` decimal(38,6) COMMENT '环期客户渗透率', 
  `diff_cust_penetration_rate` decimal(38,6) COMMENT '环期渗透率差', 
  `b_sales_ratio` decimal(38,6) COMMENT '商品销售占比=商品销售额/B端销售额', 
  `last_b_sales_ratio` decimal(38,6) COMMENT '环期商品销售占比=商品销售额/B端销售额', 
  `b_diff_sale_ratio` decimal(38,6) COMMENT '商品占省区占比环比差', 
  `goods_sales_qty_30day` decimal(30,6) COMMENT '商品30天销售量', 
  `goods_sales_30day` decimal(30,6) COMMENT '商品30天销售额', 
  `final_qty` decimal(38,6) COMMENT '', 
  `final_amt` decimal(38,6) COMMENT '', 
  `grouping__id` string, 
  `update_time` timestamp COMMENT '更新日期')
COMMENT '冻品商品销售环比与渗透率'
PARTITIONED BY ( 
  `months` string COMMENT '月分区')
 
STORED AS parquet 
 
