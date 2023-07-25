-- 城市业绩销售日统计
DROP TABLE csx_dw.ads_sale_r_d_city_channel_performance;
CREATE TABLE `csx_dw.ads_sale_r_d_city_channel_performance`(
  `biz_id` STRING COMMENT '主键ID(sdt销售日期&city_group_code城市组编码&channel_code渠道编码&change_channel_code转化后渠道编码)',
  `region_code` STRING COMMENT '销售大区编码',
  `region_name` STRING COMMENT '销售大区名称',
  `province_code` STRING COMMENT '省区编码',
  `province_name` STRING COMMENT '省区名称',
  `city_group_code` STRING COMMENT '城市组编码',
  `city_group_name` STRING COMMENT '城市组名称',
  `channel_code` STRING COMMENT '渠道编码(1:大 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)',
  `channel_name` STRING COMMENT '渠道名称',
  `change_channel_code` STRING COMMENT '转化后渠道编码(1:大 2:商超)',
  `change_channel_name` STRING COMMENT '转化后渠道名称',
  `sales_value` decimal(20,6) COMMENT '含税销售金额',
  `profit` decimal(20,6) COMMENT '含税定价毛利额',
  `excluding_tax_sales` decimal(20,6) COMMENT '不含税销售金额',
  `excluding_tax_profit` decimal(20,6) COMMENT '不含税定价毛利额',
  `reach_sales_value` decimal(20,6) COMMENT '达成含税销售金额(剔除批发内购)',
  `reach_profit` decimal(20,6) COMMENT '达成含税定价毛利额(剔除批发内购)',
  `self_sales_value` decimal(20,6) COMMENT '自营含税销售金额(大:剔除城市服务商 商超:出库地点非E开头 其他渠道:0)',
  `self_profit` decimal(20,6) COMMENT '自营含税定价毛利额(大:剔除城市服务商 商超:出库地点非E开头 其他渠道:0)',
  `joint_sales_value` decimal(20,6) COMMENT '联营含税销售金额(大:城市服务商 商超:出库地点E开头 其他渠道:0)',
  `joint_profit` decimal(20,6) COMMENT '联营含税定价毛利额(大:城市服务商 商超:出库地点E开头 其他渠道:0)',
  `normal_sales_value` decimal(20,6) COMMENT '日配含税销售金额',
  `normal_profit` decimal(20,6) COMMENT '日配含税定价毛利额'
) COMMENT '城市业绩销售日统计'
PARTITIONED BY (
  `sdt` STRING COMMENT '销售日期')
STORED AS PARQUET
TBLPROPERTIES('parquet.compression'='SNAPPY');


-- job名称
set mapred.job.name=ads_sale_r_d_city_channel_performance;
-- 切换tez计算引擎
set hive.execution.engine=tez;
set tez.queue.name=caishixian;
-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.snappycodec;
set mapred.output.compression.type=block;
set parquet.compression=snappy;
set hive.optimize.sort.dynamic.partition=true;
-- 来源表
set source_table_name = csx_dw.dws_sale_r_d_detail;
-- 目标表
set target_table_name = csx_dw.ads_sale_r_d_city_channel_performance;
-- 当天月1号
set s_today_month = trunc(current_date, 'MM');
-- 月份跑数初始日期
set s_start_date = regexp_replace(add_months(${hiveconf:s_today_month}, -1), '-', '');

-- 插入“城市业绩销售日统计”
insert overwrite table ${hiveconf:target_table_name} partition(sdt) 
select
  concat_ws('&', sdt, city_group_code, channel_code, change_channel_code) as biz_id,
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,
  channel_code,
  channel_name,
  change_channel_code,
  change_channel_name,
  sum(sales_value) as sales_value,
  sum(profit) as profit,
  sum(excluding_tax_sales) as excluding_tax_sales,
  sum(excluding_tax_profit) as excluding_tax_profit,
  sum( if(business_type_code <> '3', sales_value, 0) ) as reach_sales_value, -- 月达成业绩-剔除批发内购
  sum( if(business_type_code <> '3', profit, 0) ) as reach_profit,
  sum( if( (channel_code in ('1','7','9') and business_type_code <> '4') or (channel_code = '2' and operation_mode = 0),
    sales_value, 0) ) as self_sales_value, -- 自营业绩
  sum( if( (channel_code in ('1','7','9') and business_type_code <> '4') or (channel_code = '2' and operation_mode = 0),
    profit, 0) ) as self_profit,
  sum( if( (channel_code in ('1','7','9') and business_type_code = '4') or (channel_code = '2' and operation_mode = 1),
    sales_value, 0) ) as joint_sales_value, -- 非自营业绩
  sum( if( (channel_code in ('1','7','9') and business_type_code = '4') or (channel_code = '2' and operation_mode = 1),
    profit, 0) ) as joint_profit,
  sum( if(business_type_code = '1' and c.dc_code is null, sales_value, 0) ) as normal_sales_value,
  sum( if(business_type_code = '1' and c.dc_code is null, profit, 0) ) as normal_profit,
  sdt
from
(
  select
    region_code, region_name, province_code, province_name, city_group_code, city_group_name,
    channel_code, channel_name, business_type_code, operation_mode, dc_code, goods_code,
    if(channel_code <> '2' and substr(customer_no, 1, 1) <> 'S', '1', '2') as change_channel_code,
    if(channel_code <> '2' and substr(customer_no, 1, 1) <> 'S', '大', '商超') as change_channel_name,
    customer_no, regexp_replace(substr(sign_time, 1, 10), '-', '') as sign_date, sales_value, sales_cost,
    profit, excluding_tax_sales, excluding_tax_profit, sales_qty, sdt
  from ${hiveconf:source_table_name}
  where sdt >= ${hiveconf:s_start_date}
) a left join
(
  select
    customer_no, first_order_date
  from csx_dw.dws_crm_w_a_customer_active
  where sdt = 'current'
) b on a.customer_no = b.customer_no
left join csx_dw.dws_basic_w_a_normal_default_reject_warehouse c
  on a.dc_code = c.dc_code
group by region_code, region_name, province_code, province_name, city_group_code, city_group_name, 
  channel_code, channel_name, change_channel_code, change_channel_name, sdt;


