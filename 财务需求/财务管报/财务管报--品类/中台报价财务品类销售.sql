-- 中台报价 限制联采商品
-- 关联原单号
set  mapreduce.job.reduces = 100;
-- set  hive.map.aggr = true;
-- set  hive.groupby.skewindata = false;
set  hive.exec.parallel = true;
set  hive.exec.dynamic.partition = true;
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
set mapred.min.split.size.per.node=100000000;  
--一个交换机下split的至少的大小(这个值决定了多个交换机上的文件是否需要合并)    
set mapred.min.split.size.per.rack=100000000;  
--执行Map前进行小文件合并  
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;   

set edate = '${enddate}';
set edt =regexp_replace(${hiveconf:edate},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');

drop table if exists csx_tmp.temp_sale_01 ;
create temporary table if not exists csx_tmp.temp_sale_01 as 
  select
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    a.origin_order_no, 
    a.order_no, 
    a.dc_code, 
    a.goods_code, 
    a.channel_code,
    a.channel_name,
    a.order_category_name, 
    coalesce(c.delivery_date, a.shipped_date) as shipped_date,
    coalesce(c.delivery_time, a.shipped_time) as shipped_time,
    purchase_price_flag,            --采购报价标识
    sales_qty,                      --销量
    sales_value,                    --销售额
    sales_cost,                     --销售成本
    profit,                         --毛利额
    excluding_tax_sales,            --未税销售额    
    excluding_tax_cost,             --未税成本
    excluding_tax_profit,           --未税毛利额
    purchase_price,                 --采购报价
    middle_office_price             --中台报价
    no_tax_purchase_price_cost,     --采购报价成本未税
    purchase_price_cost,            --采购成本
    no_tax_middle_office_cost,       -- 中台报价成本未税
    middle_office_cost ,                 --中台成本
    joint_purchase_flag         --联采标识
  from
  (
    select  
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        b.classify_large_code,
        b.classify_large_name,
        b.classify_middle_code,
        b.classify_middle_name,
        b.classify_small_code,
        b.classify_small_name,
        origin_order_no, 
        order_no, 
        dc_code, 
        goods_code, 
        channel_code, 
        a.channel_name,
        order_category_name, 
        shipped_time, 
        regexp_replace(substr(shipped_time, 1, 10), '-', '') as shipped_date,
        purchase_price_flag,
        sales_qty,
        sales_value,
        sales_cost,
        profit,
        excluding_tax_sales,
        excluding_tax_cost,
        excluding_tax_profit,
        purchase_price,
        middle_office_price,
        (a.purchase_price*a.sales_qty) as  purchase_price_cost,
        middle_office_cost,
        (a.purchase_price/(1+a.tax_rate/100))*a.sales_qty as no_tax_purchase_price_cost,
        (a.middle_office_price/(1+a.tax_rate/100))*sales_qty as no_tax_middle_office_cost,
        joint_purchase_flag
    from csx_dw.dws_sale_r_d_detail a
    join
    (select shop_code,
        product_code,
        joint_purchase_flag,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_info a 
    left  join 
    (select 
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_small_code
    from csx_dw.dws_basic_w_a_manage_classify_m 
        where sdt='current' 
    --    and classify_middle_code in ('B0304','B0305')
    ) b 
    where sdt='current'  
        and a.small_category_code=b.category_small_code
    )  b on a.goods_code=b.product_code and a.dc_code=b.shop_code
    where sdt >=${hiveconf:sdate}
      and sdt<= ${hiveconf:edt}
      and sales_type in ('qyg','bbc') 
      and purchase_price_flag = 1
   --  and dc_code = 'W0A2' 
     ) a left join
     (
    SELECT distinct order_no, 
        delivery_time, 
        regexp_replace(substr(delivery_time, 1, 10), '-', '') as delivery_date
    FROM csx_dw.dws_csms_r_d_yszx_order_m_new
    WHERE sdt >= regexp_replace(date_add(trunc(${hiveconf:edate},'MM'),-30),'-','')
        AND return_flag = ''
     ) c on a.origin_order_no = c.order_no
     where  classify_middle_code in ('B0304','B0305')
 ;
 
 
insert overwrite table     csx_tmp.ads_fr_r_d_frozen_financial_middle partition(months)
select  
    substr(${hiveconf:edt},1,6),
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sales_qty,
    sales_cost,
    profit,
    middle_office_cost,
    purchase_price_cost,
    sales_value,
    warehouse_fee_amt,
    deliver_fee_amt,
    credit_fee_amt,
    run_fee_amt,
    joint_venture_fee_amt,
    no_tax_cost,
    no_tax_profit,
    no_tax_sales,
    no_tax_middle_office_cost,
    no_tax_purchase_price_cost,
    no_tax_warehouse_fee_amt,
    no_tax_deliver_fee_amt,
    no_tax_credit_fee_amt,
    no_tax_run_fee_amt,
    no_tax_joint_venture_fee_amt,
    current_timestamp(),
    substr(${hiveconf:edt},1,6)
from 
    (select  
        a.province_code,
        a.province_name,
        a.city_group_code,
        a.city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        sum(sales_qty)sales_qty,  
        sum(sales_cost)sales_cost, 
        sum(profit)profit,
        sum(middle_office_cost) as middle_office_cost,
        sum(purchase_price_cost) as purchase_price_cost,
        sum(a.sales_value) as sales_value,
        sum(warehouse_rate*a.sales_value) as warehouse_fee_amt,
        sum(delivery_rate*a.sales_value) as deliver_fee_amt,
        sum(credit_rate*a.sales_value) as credit_fee_amt,
        sum(run_rate*a.sales_value) as run_fee_amt,
        sum(joint_venture_rate*a.sales_value) as joint_venture_fee_amt, 
        sum(excluding_tax_cost)as no_tax_cost,   
        sum(excluding_tax_profit)as no_tax_profit, 
        sum(no_tax_middle_office_cost) as no_tax_middle_office_cost,
        sum(a.excluding_tax_sales) as no_tax_sales,
        sum(no_tax_purchase_price_cost) as no_tax_purchase_price_cost,
        sum(warehouse_rate*a.excluding_tax_sales) as no_tax_warehouse_fee_amt,
        sum(delivery_rate*a.excluding_tax_sales) as no_tax_deliver_fee_amt,
        sum(credit_rate*a.excluding_tax_sales) as no_tax_credit_fee_amt,
        sum(run_rate*a.excluding_tax_sales) as no_tax_run_fee_amt,
        sum(joint_venture_rate*a.excluding_tax_sales) as no_tax_joint_venture_fee_amt    
    from
    csx_tmp.temp_sale_01 a 
left join
    (
    select
         warehouse_code,
         goods_code,
         warehouse_rate,         --仓储率
         delivery_rate,          --配送率
         credit_rate,            --信控率       
         run_rate,               --运营率
         joint_venture_rate,     --联营率
         price_begin_time,
         price_end_time,
         channel,
         type,
         sdt
     from csx_dw.dws_price_r_d_goods_prices_m
        where 1=1
        and sdt >= regexp_replace(date_add(trunc(${hiveconf:edate},'MM'),-30),'-','')
    ) b on a.dc_code = b.warehouse_code and a.goods_code = b.goods_code 
        and a.channel_code = cast(b.channel as string) and a.order_category_name = b.type
        and a.shipped_date = b.sdt
    where a.shipped_time >= b.price_begin_time 
        and a.shipped_time <= b.price_end_time
        and joint_purchase_flag=1                --联采商品条件
    group by a.province_code,
        a.province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
)a
;

-- 中台报价所有未限制联采 
-- 关联原单号

set edate = '${enddate}';
set edt =regexp_replace(${hiveconf:edate},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');

drop table if exists csx_tmp.temp_sale_01 ;
create temporary table if not exists csx_tmp.temp_sale_01 as 
  select
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    a.origin_order_no, 
    a.order_no, 
    a.dc_code, 
    a.goods_code, 
    a.channel_code,
    a.channel_name,
    a.order_category_name, 
    coalesce(c.delivery_date, a.shipped_date) as shipped_date,
    coalesce(c.delivery_time, a.shipped_time) as shipped_time,
    purchase_price_flag,            --采购报价标识
    sales_qty,                      --销量
    sales_value,                    --销售额
    sales_cost,                     --销售成本
    profit,                         --毛利额
    excluding_tax_sales,            --未税销售额    
    excluding_tax_cost,             --未税成本
    excluding_tax_profit,           --未税毛利额
    purchase_price,                 --采购报价
    middle_office_price             --中台报价
    no_tax_purchase_price_cost,     --采购报价成本未税
    purchase_price_cost,            --采购成本
    no_tax_middle_office_cost,       -- 中台报价成本未税
    middle_office_cost                  --中台成本
  from
  (
    select  
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        b.classify_large_code,
        b.classify_large_name,
        b.classify_middle_code,
        b.classify_middle_name,
        b.classify_small_code,
        b.classify_small_name,
        origin_order_no, 
        order_no, 
        dc_code, 
        goods_code, 
        channel_code, 
        a.channel_name,
        order_category_name, 
        shipped_time, 
        regexp_replace(substr(shipped_time, 1, 10), '-', '') as shipped_date,
        purchase_price_flag,
        sales_qty,
        sales_value,
        sales_cost,
        profit,
        excluding_tax_sales,
        excluding_tax_cost,
        excluding_tax_profit,
        purchase_price,
        middle_office_price,
        (a.purchase_price*a.sales_qty) as  purchase_price_cost,
        middle_office_cost,
        (a.purchase_price/(1+a.tax_rate/100))*a.sales_qty as no_tax_purchase_price_cost,
        (a.middle_office_price/(1+a.tax_rate/100))*sales_qty as no_tax_middle_office_cost
    from csx_dw.dws_sale_r_d_detail a
    join 
    (select goods_id,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_m 
        where sdt='current' 
        and classify_middle_code in ('B0304','B0305')
      --  and classify_small_code='B030401'
    ) b on a.goods_code=b.goods_id
    where sdt >=${hiveconf:sdate}
      and sdt<= ${hiveconf:edt}
      and sales_type in ('qyg','bbc') 
      and purchase_price_flag = 1
   --  and dc_code = 'W0A2' 
     ) a left join
     (
    SELECT distinct order_no, 
        delivery_time, 
        regexp_replace(substr(delivery_time, 1, 10), '-', '') as delivery_date
    FROM csx_dw.dws_csms_r_d_yszx_order_m_new
    WHERE sdt >= regexp_replace(date_add(trunc(${hiveconf:edate},'MM'),-30),'-','')
        AND return_flag = ''
     ) c on a.origin_order_no = c.order_no
 ;
 
 
insert overwrite table     csx_tmp.ads_fr_r_d_frozen_financial_middle partition(months)
select  
    substr(${hiveconf:edt},1,6),
    a.province_code,
    a.province_name,
    a.city_group_code,
    a.city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sales_qty,
    sales_cost,
    profit,
    middle_office_cost,
    purchase_price_cost,
    sales_value,
    warehouse_fee_amt,
    deliver_fee_amt,
    credit_fee_amt,
    run_fee_amt,
    joint_venture_fee_amt,
    no_tax_cost,
    no_tax_profit,
    no_tax_sales,
    no_tax_middle_office_cost,
    no_tax_purchase_price_cost,
    no_tax_warehouse_fee_amt,
    no_tax_deliver_fee_amt,
    no_tax_credit_fee_amt,
    no_tax_run_fee_amt,
    no_tax_joint_venture_fee_amt,
    current_timestamp(),
    substr(${hiveconf:edt},1,6)
from 
    (select  
        a.province_code,
        a.province_name,
        a.city_group_code,
        a.city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        sum(sales_qty)sales_qty,  
        sum(sales_cost)sales_cost, 
        sum(profit)profit,
        sum(middle_office_cost) as middle_office_cost,
        sum(purchase_price_cost) as purchase_price_cost,
        sum(a.sales_value) as sales_value,
        sum(warehouse_rate*a.sales_value) as warehouse_fee_amt,
        sum(delivery_rate*a.sales_value) as deliver_fee_amt,
        sum(credit_rate*a.sales_value) as credit_fee_amt,
        sum(run_rate*a.sales_value) as run_fee_amt,
        sum(joint_venture_rate*a.sales_value) as joint_venture_fee_amt, 
        sum(excluding_tax_cost)as no_tax_cost,   
        sum(excluding_tax_profit)as no_tax_profit, 
        sum(no_tax_middle_office_cost) as no_tax_middle_office_cost,
        sum(a.excluding_tax_sales) as no_tax_sales,
        sum(no_tax_purchase_price_cost) as no_tax_purchase_price_cost,
        sum(warehouse_rate*a.excluding_tax_sales) as no_tax_warehouse_fee_amt,
        sum(delivery_rate*a.excluding_tax_sales) as no_tax_deliver_fee_amt,
        sum(credit_rate*a.excluding_tax_sales) as no_tax_credit_fee_amt,
        sum(run_rate*a.excluding_tax_sales) as no_tax_run_fee_amt,
        sum(joint_venture_rate*a.excluding_tax_sales) as no_tax_joint_venture_fee_amt    
    from
    csx_tmp.temp_sale_01 a 
left join
    (
    select
         warehouse_code,
         goods_code,
         warehouse_rate,         --仓储率
         delivery_rate,          --配送率
         credit_rate,            --信控率       
         run_rate,               --运营率
         joint_venture_rate,     --联营率
         price_begin_time,
         price_end_time,
         channel,
         type,
         sdt
     from csx_dw.dws_price_r_d_goods_prices_m
        where 1=1
        and sdt >= regexp_replace(date_add(trunc(${hiveconf:edate},'MM'),-30),'-','')
    ) b on a.dc_code = b.warehouse_code and a.goods_code = b.goods_code 
        and a.channel_code = cast(b.channel as string) and a.order_category_name = b.type
        and a.shipped_date = b.sdt
    where a.shipped_time >= b.price_begin_time 
        and a.shipped_time <= b.price_end_time
    group by a.province_code,
        a.province_name,
        city_group_code,
        city_group_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
)a
;

drop table csx_tmp.ads_fr_r_d_frozen_financial_middle;
CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_financial_middle`(
      sales_months string comment '销售月份',
      `province_code` string comment '省区', 
      `province_name` string comment '省区', 
      `city_group_code` string comment '城市组', 
      `city_group_name` string comment '城市组', 
      `classify_large_code` string comment '管理一级', 
      `classify_large_name` string comment '管理一级', 
      `classify_middle_code` string comment '管理二级', 
      `classify_middle_name` string comment '管理二级', 
      `classify_small_code` string comment'管理三级', 
      `classify_small_name` string comment'管理三级', 
      `sales_qty` decimal(30,6) comment '销售量', 
      `sales_cost` decimal(30,6) comment '销售成本', 
      `profit` decimal(30,6) comment '毛利额', 
      `middle_office_cost` decimal(30,6) comment '中台报价成本', 
      `purchase_price_cost` decimal(30,6) comment '采购中台报价成本', 
      `sales_value` decimal(30,6) comment '销售额',  
      `warehouse_fee_amt` decimal(38,10) comment '含税仓配成本', 
      `deliver_fee_amt` decimal(38,10) comment '含税配送金额', 
      `credit_fee_amt` decimal(38,10) comment '含税信控金额', 
      `run_fee_amt` decimal(38,10) comment '含税运营成本', 
      `joint_venture_fee_amt` decimal(38,10) comment '含税联营成本',
      `no_tax_cost` decimal(30,6) comment '未税销售成本',       
      `no_tax_profit` decimal(30,6) comment '未税毛利额',
      `no_tax_sales` decimal(30,6) comment '未税销售额', 
      `no_tax_middle_office_cost` decimal(38,22) comment '未税中台报价成本',  
      `no_tax_purchase_price_cost` decimal(30,6) comment '未税采购中台报价成本', 
      `no_tax_warehouse_fee_amt` decimal(38,10) comment '未税仓配成本', 
      `no_tax_deliver_fee_amt` decimal(38,10) comment '未税配送金额', 
      `no_tax_credit_fee_amt` decimal(38,10) comment '未税信控金额', 
      `no_tax_run_fee_amt` decimal(38,10) comment '未税运营成本', 
      `no_tax_joint_venture_fee_amt` decimal(38,10) comment '未税联营成本',
       update_time timestamp comment '插入时间'
 )comment '中台报价运营成本'
    partitioned by(months string comment '月分区')
    STORED AS parquet ;

show create table csx_tmp.ads_fr_r_d_frozen_financial_middle;