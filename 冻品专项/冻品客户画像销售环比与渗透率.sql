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
set last_edt=regexp_replace(add_months(${hiveconf:edt} ,-1),'-','');
set s_dt_30 =regexp_replace(date_sub(${hiveconf:edt},30),'-','');
-- set parquet.compression=snappy;
-- set hive.exec.dynamic.partition=true; 
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- select  ${hiveconf:last_sdt},${hiveconf:s_dt},${hiveconf:last_edt},${hiveconf:e_dt} ;
-- 本期数据 (不含合伙人 purpose!='06')

drop table if exists csx_tmp.tmp_sale_frozen_01;
create temporary table csx_tmp.tmp_sale_frozen_01
as 
select 
    case when channel_code in ('1','9','7') then 'B端' end channel_name,
    a.region_code,
    a.region_name,
    a.province_code,
    a.province_name,
    a.customer_no,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
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
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.province_code,
    a.province_name,
    first_category_code,
    a.first_category_name,
    a.second_category_code,
    a.second_category_name,
    a.customer_no
;



-- 行业销售额/成交数
drop table if exists csx_tmp.tmp_sale_frozen_02;
create temporary table csx_tmp.tmp_sale_frozen_02 as 
select
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_qty) as sales_qty,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(last_sales_qty) as last_sales_qty,
    sum(last_sales_value) as last_sales_value,
    sum(last_profit) as last_profit,
    count(distinct case when sales_value>0 then customer_no end ) as sales_cust_number, --商品成交数
    count(distinct case when last_sales_value>0 then customer_no end )as last_sales_cust_number,  --商品环比冻品成交数
    grouping__id
from csx_tmp.tmp_sale_frozen_01 a
where classify_middle_code='B0304'
group by 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
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
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),  --32767 省区行业明细  
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),  --	32287 省区小类汇总
    (channel_name,
    region_code,
    region_name,
    province_code,
    province_name),   -- 31 省区汇总
    (channel_name,
    region_code,
    region_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),  --32743 大区行业统计
    (channel_name,
    region_code,
    region_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),  --32263 大区小类汇总
    (channel_name,
    region_code,
    region_name),  --7 大区汇总
    (channel_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name), -- 32737 全国行业小类 汇总
    (channel_name,
    classify_large_code,
    classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name),  --32257 全国小类汇总
    ()   --0 
)
;


   
-- 总数
drop table if exists  csx_tmp.tmp_sale_frozen_03;
create  temporary table csx_tmp.tmp_sale_frozen_03 as 
select 
    channel_name,
    region_code,
    region_name,
    province_code,
    province_name,
    second_category_code,
    count(  case when b_sales_value>0 then customer_no end) as b_cust_number,
    count(  case when last_b_sales_value>0 then customer_no end ) as last_b_cust_number,
    sum(frozen_sales_qty) as frozen_sales_qty,
    sum(frozen_sales) as frozen_sales,
    sum(frozen_profit) as frozen_profit,
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
    second_category_code,
    sum(case when classify_middle_code='B0304' then sales_qty end) as frozen_sales_qty,
    sum(case when classify_middle_code='B0304' then sales_value end) as frozen_sales,
    sum(case when classify_middle_code='B0304' then profit end) as frozen_profit,
    sum(sales_value) as b_sales_value,
    sum(profit) as b_profit,
    sum(last_sales_value) as last_b_sales_value,
    sum(last_profit) as last_b_profit
from csx_tmp.tmp_sale_frozen_01
where 1=1
--AND region_code='7'
group by 
    channel_name,
    region_code,
    second_category_code,
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
    province_name,
    second_category_code
grouping sets
    ((channel_name,
     region_code,
    region_name,
    province_code,
    province_name,
    second_category_code),
    (channel_name,
    region_code,
    region_name,
    second_category_code),
    (second_category_code),
    ())
;

-- select * from csx_tmp.tmp_sale_frozen_02 where grouping__id in ('32257','32263','32287');
insert overwrite table  csx_tmp.report_sale_r_d_frozen_industry_fr partition(months) 
select
    case when a.grouping__id = '0' then '0'
     when a.grouping__id = '32737' then '1' 
    when a.grouping__id = '7' then '2' 
    when a.grouping__id='32743' then '3' 
    when a.grouping__id='31' then '4' 
    else '5' end level_id,  --分组：0 全国，1 全国管理分类，2 大区，3大区管理分类 4省区，5省区管分类
    substr(${hiveconf:e_dt} ,1,4) as years,
    substr(${hiveconf:e_dt} ,1,6) as smonth,
    coalesce(a.channel_name,'B端')as channel_name,
    coalesce(a.region_code,'00')as region_code,
    coalesce(a.region_name,'全国')as region_name,
    coalesce(a.province_code,'00') as  province_code,
    coalesce(a.province_name,'小计') as province_name,
    coalesce(a.first_category_code,'') as first_category_code,
    coalesce(a.first_category_name,'') as first_category_name,
    coalesce(a.second_category_code,'') as second_category_code,
    coalesce(a.second_category_name,'') as second_category_name,
    coalesce(a.classify_large_code,'') as  classify_large_code,
    coalesce(a.classify_large_name,'') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'') as classify_small_code,
    coalesce(a.classify_small_name,'') as classify_small_name,
    coalesce(sales_qty,0) as sales_qty,
    coalesce(a.sales_value,0) as sales_value,
    coalesce(profit,0) as profit,
    coalesce(last_sales_qty,0) as last_sales_qty,
    coalesce(a.last_sales_value,0) as last_sales_value,
    coalesce(last_profit,0) as last_profit,
    coalesce(sales_cust_number,    0) as sales_cust_number,                                  --成交数
    coalesce(last_sales_cust_number,0) as last_sales_cust_number,                             --环期成交数
    coalesce(b_sales_value, 0) as b_sales_value,                                     --B端行业销售额
    coalesce(b_profit, 0) as b_profit,                                         --B端行业销售毛利
    coalesce(b_cust_number,0) as b_cust_number,                                         --B端行业成交
    coalesce(last_b_sales_value, 0) as last_b_sales_value,                               --环期B端销售额
    coalesce(last_b_profit,0) as last_b_profit,                                         --环期B端行业毛利额
    coalesce(last_b_cust_number, 0) as last_b_cust_number,                               --环期B端行业成交
    coalesce(small_sales_qty,  0) as small_sales_qty,                                   --小类销量
    coalesce(small_sales_value, 0) as small_sales,                                      --小类销售额
    small_profit ,                                                  --小类毛利额
    small_profit/small_sales_value as small_profit_rate,
    coalesce(sales_value/small_sales_value,0) as sales_ratio,    -- 行业销售/小类销售占比
    coalesce(sales_qty/small_sales_qty,0) as sales_qty_ratio,    -- 商品销售量/冻品销售量占比
    case when coalesce(a.last_sales_value,0)=0 and coalesce(a.sales_value)>0 then 1 
        else coalesce((sales_value -coalesce(last_sales_value,0))/last_sales_value,0) 
    end as ring_sales_rate ,                                                                 --销售环比增长率
    case when coalesce(last_sales_qty,0)=0 and coalesce(a.sales_qty)>0 then 1 
        else  coalesce((sales_qty-coalesce(last_sales_qty,0))/last_sales_qty,0)
    end as ring_sales_qty_rate ,                                                             --销售量环比增长率
    coalesce(profit/a.sales_value,0) as profit_rate,                                          --商品定价毛利率
    coalesce(profit/sales_value,0)- coalesce(last_profit/last_sales_value,0) as goods_diff_profit_rate,     --毛利率差
    coalesce(sales_cust_number/b_cust_number,0) as cust_penetration_rate ,                                              ---商品渗透率
    coalesce(last_sales_cust_number/last_b_cust_number,0) as last_cust_penetration_rate ,                   ---环期商品渗透率
    coalesce(sales_cust_number/b_cust_number,0) - coalesce(last_sales_cust_number/last_b_cust_number ,0)  as diff_cust_penetration_rate ,   --- 商品渗透率环比
    coalesce( sales_value/b_sales_value,0) as b_sales_ratio,                                                                                -- B端销售额/省区占比
    coalesce( last_sales_value/last_b_sales_value,0) as last_b_sales_ratio,                                                                 -- B端销售额/省区占比
    coalesce( sales_value/b_sales_value,0) - coalesce(last_sales_value/last_b_sales_value,0)  as b_diff_sale_ratio,                         --商品占省区占比环比差
    a.grouping__id,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from csx_tmp.tmp_sale_frozen_02 a 
left join 
(select * from csx_tmp.tmp_sale_frozen_03 ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'') 
and coalesce(a.region_code,'')=coalesce(b.region_code ,'') 
and  coalesce(a.second_category_code,'')=coalesce(b.second_category_code ,'')
left join 
(select  coalesce(a.channel_name,'')as channel_name,
    coalesce(a.region_code,'')as region_code,
    coalesce(a.region_name,'')as region_name,
    coalesce(a.province_code,'') as  province_code,
    coalesce(a.province_name,'') as province_name,
    coalesce(a.first_category_code,'') as first_category_code,
    coalesce(a.first_category_name,'') as first_category_name,
    coalesce(a.second_category_code,'') as second_category_code,
    coalesce(a.second_category_name,'') as second_category_name,
    coalesce(a.classify_large_code,'') as  classify_large_code,
    coalesce(a.classify_large_name,'') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'') as classify_small_code,
    coalesce(a.classify_small_name,'') as classify_small_name,
    coalesce(sales_qty,0) as small_sales_qty,
    coalesce(a.sales_value,0) as small_sales_value,
    coalesce(profit,0) as small_profit
from csx_tmp.tmp_sale_frozen_02 a
    where grouping__id in ('32257','32263','32287')) c  on coalesce(a.region_code,'')=c.region_code 
 and coalesce(a.province_code,'') = c.province_code
 -- and coalesce(a.second_category_code,'')=c.second_category_code
    and coalesce(a.classify_small_code,'')=c.classify_small_code
    where a.grouping__id not in ('32257','32263','32287')
;






drop table csx_tmp.report_sale_r_d_frozen_industry_fr ;
CREATE TABLE csx_tmp.report_sale_r_d_frozen_industry_fr (
  level_id string COMMENT '层级0 全国,1 全国商品,2大区,3大区商品,4 省区,5省区商品', 
  years string COMMENT '销售年', 
  smonth string COMMENT '销售月', 
  channel_name string COMMENT '渠道', 
  region_code string COMMENT '大区编码', 
  region_name string COMMENT '大区名称', 
  province_code string COMMENT '省区编码', 
  province_name string COMMENT '省区名称', 
  first_category_code string COMMENT '一级行业', 
  first_category_name string COMMENT '一级行业名称', 
  second_category_code string COMMENT '二级行业', 
  second_category_name string COMMENT '二级行业名称', 
  classify_large_code string COMMENT '管理一级分类', 
  classify_large_name string COMMENT '管理一级分类名称', 
  classify_middle_code string COMMENT '管理二级分类', 
  classify_middle_name string COMMENT '管理二级分类', 
  classify_small_code string COMMENT '管理三级分类', 
  classify_small_name string COMMENT '管理三级分类', 
  sales_qty decimal(38,6) COMMENT '销售量', 
  sales_value decimal(38,6) COMMENT '销售额', 
  profit decimal(38,6) COMMENT '毛利额', 
  last_sales_qty decimal(38,6) COMMENT '环期销量', 
  last_sales_value decimal(38,6) COMMENT '环期销售额', 
  last_profit decimal(38,6) COMMENT '环期毛利额', 
  sales_cust_number bigint COMMENT '行业成交数', 
  last_sales_cust_number bigint COMMENT '行业成交数环期', 
  b_sales_value decimal(38,6) COMMENT 'B端总销售额（剔除城市服务商）', 
  b_profit decimal(38,6) COMMENT 'B端总毛利额（剔除城市服务商）', 
  b_cust_number bigint COMMENT 'B端成交数（剔除城市服务商）', 
  last_b_sales_value decimal(38,6) COMMENT '环期B端总销售额（剔除城市服务商）', 
  last_b_profit decimal(38,6) COMMENT '环期B端总毛利额（剔除城市服务商）', 
  last_b_cust_number bigint COMMENT '环期B端成交数（剔除城市服务商）', 
  small_sales_qty decimal(38,6) COMMENT '小类销售量', 
  small_sales_value decimal(38,6) COMMENT '小类销售额', 
  small_profit decimal(38,6) COMMENT '小类毛利额', 
  small_profit_rate decimal(38,6) COMMENT '小类毛利率', 
  sales_ratio decimal(38,6) COMMENT '行业销售额占比=行业销售额/小类销售额', 
  sales_qty_ratio decimal(38,6) COMMENT '行业销售量占比=行业销售额/小类销售额', 
  ring_sales_rate decimal(38,6) COMMENT '行业销售额环比增长率', 
  ring_sales_qty_rate decimal(38,6) COMMENT '行业销量环比增长率', 
  profit_rate decimal(38,6) COMMENT '行业毛利率', 
  diff_profit_rate decimal(38,6) COMMENT '行业环比毛利率差', 
  cust_penetration_rate decimal(38,6) COMMENT '行业渗透率', 
  last_cust_penetration_rate decimal(38,6) COMMENT '环期渗透率', 
  diff_cust_penetration_rate decimal(38,6) COMMENT '环期渗透率差', 
  b_sales_ratio decimal(38,6) COMMENT '行业销售占比=行业销售额/B端销售额', 
  last_b_sales_ratio decimal(38,6) COMMENT '环期行业销售占比=行业销售额/B端销售额', 
  b_diff_sale_ratio decimal(38,6) COMMENT '行业占省区占比环比差', 
  grouping__id string, 
  update_time timestamp COMMENT '更新日期')
COMMENT '冻品行业销售环比与渗透率'
PARTITIONED BY ( 
  months string COMMENT '月分区')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS PARQUET
;
 
-- select * from csx_tmp.tmp_sale_frozen_02 where grouping__id in ('32257','32263','32287');

--insert overwrite 
create table csx_tmp.report_sale_r_d_frozen_industry_fr as 
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
    coalesce(a.first_category_code,'') as first_category_code,
    coalesce(a.first_category_name,'') as first_category_name,
    coalesce(a.second_category_code,'') as second_category_code,
    coalesce(a.second_category_name,'') as second_category_name,
    coalesce(a.classify_large_code,'') as  classify_large_code,
    coalesce(a.classify_large_name,'') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'') as classify_small_code,
    coalesce(a.classify_small_name,'') as classify_small_name,
    coalesce(sales_qty,0) as sales_qty,
    coalesce(a.sales_value,0) as sales_value,
    coalesce(profit,0) as profit,
    coalesce(last_sales_qty,0) as last_sales_qty,
    coalesce(a.last_sales_value,0) as last_sales_value,
    coalesce(last_profit,0) as last_profit,
    coalesce(sales_cust_number,    0) as sales_cust_number,                                  --成交数
    coalesce(last_sales_cust_number,0) as last_sales_cust_number,                             --环期成交数
    coalesce(b_sales_value, 0) as b_sales_value,                                     --B端行业销售额
    coalesce(b_profit, 0) as b_profit,                                         --B端行业销售毛利
    coalesce(b_cust_number,0) as b_cust_number,                                         --B端行业成交
    coalesce(last_b_sales_value, 0) as last_b_sales_value,                               --环期B端销售额
    coalesce(last_b_profit,0) as last_b_profit,                                         --环期B端行业毛利额
    coalesce(last_b_cust_number, 0) as last_b_cust_number,                               --环期B端行业成交
    coalesce(small_sales_qty,  0) as small_sales_qty,                                   --小类销量
    coalesce(small_sales_value, 0) as small_sales,                                      --小类销售额
    small_profit ,                                                  --小类毛利额
    small_profit/small_sales_value as small_profit_rate,
    coalesce(sales_value/small_sales_value,0) as sales_ratio,    -- 行业销售/小类销售占比
    coalesce(sales_qty/small_sales_qty,0) as sales_qty_ratio,    -- 商品销售量/冻品销售量占比
    case when coalesce(a.last_sales_value,0)=0 then 1 
        else coalesce((sales_value -coalesce(last_sales_value,0))/last_sales_value,0) 
    end as ring_sales_rate ,                                                                 --销售环比增长率
    case when coalesce(last_sales_qty,0)=0 then 1 
        else  coalesce((sales_qty-coalesce(last_sales_qty,0))/last_sales_qty,0)
    end as ring_sales_qty_rate ,                                                             --销售量环比增长率
    coalesce(profit/a.sales_value,0) as profit_rate,                                          --商品定价毛利率
    coalesce(profit/sales_value,0)- coalesce(last_profit/last_sales_value,0) as diff_profit_rate,     --毛利率差
    coalesce(sales_cust_number/b_cust_number,0) as cust_penetration_rate ,                                              --商品渗透率
    coalesce(last_sales_cust_number/last_b_cust_number,0) as last_cust_penetration_rate ,                   --环期商品渗透率
    coalesce(sales_cust_number/b_cust_number,0) - coalesce(last_sales_cust_number/last_b_cust_number ,0)  as diff_cust_penetration_rate ,   -- 商品渗透率环比
    coalesce( sales_value/b_sales_value,0) as b_sales_ratio,                                                                                -- B端销售额/省区占比
    coalesce( last_sales_value/last_b_sales_value,0) as last_b_sales_ratio,                                                                 -- B端销售额/省区占比
    coalesce( sales_value/b_sales_value,0) - coalesce(last_sales_value/last_b_sales_value,0)  as b_diff_sale_ratio,                         --商品占省区占比环比差
    a.grouping__id,
    current_timestamp(),
    substr(${hiveconf:e_dt},1,6)
from csx_tmp.tmp_sale_frozen_02 a 
left join 
(select * from csx_tmp.tmp_sale_frozen_03 ) b on coalesce(a.province_code,'')=coalesce(b.province_code ,'') 
and coalesce(a.region_code,'')=coalesce(b.region_code ,'') 
and  coalesce(a.second_category_code,'')=coalesce(b.second_category_code ,'')
left join 
(select  coalesce(a.channel_name,'')as channel_name,
    coalesce(a.region_code,'')as region_code,
    coalesce(a.region_name,'')as region_name,
    coalesce(a.province_code,'') as  province_code,
    coalesce(a.province_name,'') as province_name,
    coalesce(a.first_category_code,'') as first_category_code,
    coalesce(a.first_category_name,'') as first_category_name,
    coalesce(a.second_category_code,'') as second_category_code,
    coalesce(a.second_category_name,'') as second_category_name,
    coalesce(a.classify_large_code,'') as  classify_large_code,
    coalesce(a.classify_large_name,'') as  classify_large_name,
    coalesce(a.classify_middle_code,'') as  classify_middle_code,
    coalesce(a.classify_middle_name,'') as  classify_middle_name,
    coalesce(a.classify_small_code,'') as classify_small_code,
    coalesce(a.classify_small_name,'') as classify_small_name,
    coalesce(sales_qty,0) as small_sales_qty,
    coalesce(a.sales_value,0) as small_sales_value,
    coalesce(profit,0) as small_profit
from csx_tmp.tmp_sale_frozen_02 a
    where grouping__id in ('32257','32263','32287')) c  on coalesce(a.region_code,'')=c.region_code 
 and coalesce(a.province_code,'') = c.province_code
 -- and coalesce(a.second_category_code,'')=c.second_category_code
    and coalesce(a.classify_small_code,'')=c.classify_small_code
    where a.grouping__id not in ('32257','32263','32287')
;







CREATE TABLE csx_tmp.report_sale_r_d_frozen_industry_fr(
  level_id string COMMENT '层级0 全国,1 全国商品,2大区,3大区商品,4 省区,5省区商品', 
  years string COMMENT '销售年', 
  smonth string COMMENT '销售月', 
  channel_name string COMMENT '渠道', 
  region_code string COMMENT '大区编码', 
  region_name string COMMENT '大区名称', 
  province_code string COMMENT '省区编码', 
  province_name string COMMENT '省区名称', 
  first_category_code string COMMENT '一级行业', 
  first_category_name string COMMENT '一级行业名称', 
  second_category_code string COMMENT '二级行业', 
  second_category_name string COMMENT '二级行业名称', 
  classify_large_code string COMMENT '管理一级分类', 
  classify_large_name string COMMENT '管理一级分类名称', 
  classify_middle_code string COMMENT '管理二级分类', 
  classify_middle_name string COMMENT '管理二级分类', 
  classify_small_code string COMMENT '管理三级分类', 
  classify_small_name string COMMENT '管理三级分类', 
  sales_qty decimal(38,6) COMMENT '销售量', 
  sales_value decimal(38,6) COMMENT '销售额', 
  profit decimal(38,6) COMMENT '毛利额', 
  last_sales_qty decimal(38,6) COMMENT '环期销量', 
  last_sales_value decimal(38,6) COMMENT '环期销售额', 
  last_profit decimal(38,6) COMMENT '环期毛利额', 
  sales_cust_number bigint COMMENT '行业成交数', 
  last_sales_cust_number bigint COMMENT '行业成交数环期', 
  b_sales_value decimal(38,6) COMMENT 'B端总销售额（剔除城市服务商）', 
  b_profit decimal(38,6) COMMENT 'B端总毛利额（剔除城市服务商）', 
  b_cust_number bigint COMMENT 'B端成交数（剔除城市服务商）', 
  last_b_sales_value decimal(38,6) COMMENT '环期B端总销售额（剔除城市服务商）', 
  last_b_profit decimal(38,6) COMMENT '环期B端总毛利额（剔除城市服务商）', 
  last_b_cust_number bigint COMMENT '环期B端成交数（剔除城市服务商）', 
  small_sales_qty decimal(38,6) COMMENT '小类销售量', 
  small_sales_value decimal(38,6) COMMENT '小类销售额', 
  small_profit decimal(38,6) COMMENT '小类毛利额', 
  small_profit_rate decimal(38,6) COMMENT '小类毛利率', 
  sales_ratio decimal(38,6) COMMENT '行业销售额占比=行业销售额/小类销售额', 
  sales_qty_ratio decimal(38,6) COMMENT '行业销售量占比=行业销售额/小类销售额', 
  ring_sales_rate decimal(38,6) COMMENT '行业销售额环比增长率', 
  ring_sales_qty_rate decimal(38,6) COMMENT '行业销量环比增长率', 
  profit_rate decimal(38,6) COMMENT '行业毛利率', 
  diff_profit_rate decimal(38,6) COMMENT '行业环比毛利率差', 
  cust_penetration_rate decimal(38,6) COMMENT '行业渗透率', 
  last_cust_penetration_rate decimal(38,6) COMMENT '环期渗透率', 
  diff_cust_penetration_rate decimal(38,6) COMMENT '环期渗透率差', 
  b_sales_ratio decimal(38,6) COMMENT '行业销售占比=行业销售额/B端销售额', 
  last_b_sales_ratio decimal(38,6) COMMENT '环期行业销售占比=行业销售额/B端销售额', 
  b_diff_sale_ratio decimal(38,6) COMMENT '行业占省区占比环比差', 
  grouping__id string, 
  update_time timestamp COMMENT '更新日期')
COMMENT '冻品行业销售环比与渗透率'
PARTITIONED BY ( 
  months string COMMENT '月分区')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/report_sale_r_d_frozen_goods_fr'
TBLPROPERTIES (
  'transient_lastDdlTime'='1614243974')