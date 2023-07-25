
SET hive.map.aggr = TRUE; --顶端聚合


SET hive.groupby.skewindata=FALSE; --数据倾斜


SET hive.exec.parallel=TRUE; --可以控制一个sql中多个可并行执行的job
--控制对于同一个sql来说同时可以运行的job的最大值,该参数默认为8


SET hive.exec.parallel.thread.number=8;


SET hive.exec.dynamic.partition.mode=nonstrict; --动态分区模式：strict至少要有个静态分区，nostrict不限制


SET hive.exec.dynamic.partition=TRUE; --开户动态分区


SET hive.exec.max.dynamic.partitions.pernode=10000000;--每个mapper节点最多创建1000个分区


SET mapreduce.job.queuename=caishixian; --使用队列


SET hive.support.quoted.identifiers=NONE; -- 查出剩余所有字段


SET i_edate = '${START_DATE}';


SET s_date=regexp_replace(to_date(trunc(${hiveconf:i_edate},'YY')),'-','');


DROP TABLE IF EXISTS csx_dw.temp_ads_supply_kanban_goods_sales_01;


CREATE
TEMPORARY TABLE IF NOT EXISTS csx_dw.temp_ads_supply_kanban_goods_sales_01 AS -- 各渠道商品销售

SELECT '本月'AS date_m,
       '00' AS province_code,
       '全国'AS province_name,
       channel,
       channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
       COUNT(DISTINCT customer_no) AS sales_cust_num,
       COUNT(DISTINCT sdt) AS sales_days,
       COUNT(DISTINCT sdt,
                      customer_no) AS sales_cust_days,
       sum(sales_qty)AS qty,
       SUM(sales_value)AS sales_value,
       sum(profit)AS profit,
       sum(front_profit)AS front_profit,
       min(sales_price)AS min_price,
       max(sales_price)AS max_price
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE sdt>=regexp_replace(trunc(${hiveconf:i_date},'MM'),'-','')
and sdt<=regexp_replace(${hiveconf:i_date},'-','')
GROUP BY channel_name,
         channel,
         goods_code,
         goods_name,
         category_small_code,
         unit
UNION ALL -- 全渠道数据

SELECT '本月'AS date_m,
       '00' AS province_code,
       '全国'AS province_name,
       '00'AS channel,
       '全渠道'AS channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
       COUNT(DISTINCT customer_no) AS sales_cust_num,
       COUNT(DISTINCT sdt) AS sales_days,
       COUNT(DISTINCT sdt,
                      customer_no) AS sales_cust_days,
       sum(sales_qty)AS qty,
       SUM(sales_value)AS sales_value,
       sum(profit)AS profit,
       sum(front_profit)AS front_profit,
       min(sales_price)AS min_price,
       max(sales_price)AS max_price
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE sdt>=regexp_replace(trunc(${hiveconf:i_date},'MM'),'-','')
and sdt<=regexp_replace(${hiveconf:i_date},'-','')
GROUP BY goods_code,
         goods_name,
         category_small_code,
         unit
UNION ALL --省区全渠道

SELECT '本月'AS date_m,
       province_code,
       province_name,
       '00'AS channel,
       '全渠道'AS channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
       COUNT(DISTINCT customer_no) AS sales_cust_num,
       COUNT(DISTINCT sdt) AS sales_days,
       COUNT(DISTINCT sdt,
                      customer_no) AS sales_cust_days,
       sum(sales_qty)AS qty,
       SUM(sales_value)AS sales_value,
       sum(profit)AS profit,
       sum(front_profit)AS front_profit,
       min(sales_price)AS min_price,
       max(sales_price)AS max_price
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE sdt>=regexp_replace(trunc(${hiveconf:i_date},'MM'),'-','')
and sdt<=regexp_replace(${hiveconf:i_date},'-','')
GROUP BY goods_code,
         goods_name,
         category_small_code,
         unit,
         province_code,
         province_name
UNION ALL -- 省区各渠道

SELECT '本月'AS date_m,
       province_code,
       province_name,
       channel,
       channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
       COUNT(DISTINCT customer_no) AS sales_cust_num,
       COUNT(DISTINCT sdt) AS sales_days,
       COUNT(DISTINCT sdt,
                      customer_no) AS sales_cust_days,
       sum(sales_qty)AS qty,
       SUM(sales_value)AS sales_value,
       sum(profit)AS profit,
       sum(front_profit)AS front_profit,
       min(sales_price)AS min_price,
       max(sales_price)AS max_price
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE  sdt>=regexp_replace(trunc(${hiveconf:i_date},'MM'),'-','')
and sdt<=regexp_replace(${hiveconf:i_date},'-','')
GROUP BY channel_name,
         channel,
         goods_code,
         goods_name,
         category_small_code,
         unit,
         province_code,
         province_name ;

-- 插入年数据

DROP TABLE IF EXISTS csx_dw.temp_ads_supply_kanban_goods_sales_02;


CREATE
TEMPORARY TABLE IF NOT EXISTS csx_dw.temp_ads_supply_kanban_goods_sales_02 AS
SELECT '本年'AS date_m,
       '00' AS province_code,
       '全国'AS province_name,
       channel,
       channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
       COUNT(DISTINCT customer_no) AS sales_cust_num,
       COUNT(DISTINCT sdt) AS sales_days,
       COUNT(DISTINCT sdt,
                      customer_no) AS sales_cust_days,
       sum(sales_qty)AS qty,
       SUM(sales_value)AS sales_value,
       sum(profit)AS profit,
       sum(front_profit)AS front_profit,
       min(sales_price)AS min_price,
       max(sales_price)AS max_price
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE sdt>=${hiveconf:s_date}
and sdt<=regexp_replace(${hiveconf:i_date},'-','')
GROUP BY channel_name,
         channel,
         goods_code,
         goods_name,
         category_small_code,
         unit
UNION ALL -- 全渠道数据

SELECT '本年'AS date_m,
       '00' AS province_code,
       '全国'AS province_name,
       '00'AS channel,
       '全渠道'AS channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
       COUNT(DISTINCT customer_no) AS sales_cust_num,
       COUNT(DISTINCT sdt) AS sales_days,
       COUNT(DISTINCT sdt,
                      customer_no) AS sales_cust_days,
       sum(sales_qty)AS qty,
       SUM(sales_value)AS sales_value,
       sum(profit)AS profit,
       sum(front_profit)AS front_profit,
       min(sales_price)AS min_price,
       max(sales_price)AS max_price
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE  sdt>=${hiveconf:s_date}
and sdt<=regexp_replace(${hiveconf:i_date},'-','')
GROUP BY goods_code,
         goods_name,
         category_small_code,
         unit
UNION ALL --省区全渠道

SELECT '本年'AS date_m,
       province_code,
       province_name,
       '00'AS channel,
       '全渠道'AS channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
       COUNT(DISTINCT customer_no) AS sales_cust_num,
       COUNT(DISTINCT sdt) AS sales_days,
       COUNT(DISTINCT sdt,
                      customer_no) AS sales_cust_days,
       sum(sales_qty)AS qty,
       SUM(sales_value)AS sales_value,
       sum(profit)AS profit,
       sum(front_profit)AS front_profit,
       min(sales_price)AS min_price,
       max(sales_price)AS max_price
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE  sdt>=${hiveconf:s_date}
and sdt<=regexp_replace(${hiveconf:i_date},'-','')
GROUP BY goods_code,
         goods_name,
         category_small_code,
         unit,
         province_code,
         province_name
UNION ALL -- 省区各渠道

SELECT '本年'AS date_m,
       province_code,
       province_name,
       channel,
       channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
       COUNT(DISTINCT customer_no) AS sales_cust_num,
       COUNT(DISTINCT sdt) AS sales_days,
       COUNT(DISTINCT sdt,
                      customer_no) AS sales_cust_days,
       sum(sales_qty)AS qty,
       SUM(sales_value)AS sales_value,
       sum(profit)AS profit,
       sum(front_profit)AS front_profit,
       min(sales_price)AS min_price,
       max(sales_price)AS max_price
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE  sdt>=${hiveconf:s_date}
and sdt<=regexp_replace(${hiveconf:i_date},'-','')
GROUP BY channel_name,
         channel,
         goods_code,
         goods_name,
         category_small_code,
         unit,
         province_code,
         province_name ;

INSERT overwrite TABLE `csx_dw.ads_supply_kanban_goods_sales` partition(sdt,date_m)
SELECT 
     province_code,
    province_name,
        channel,
       channel_name,
      a.goods_code,
      bar_code,
       b.goods_name,
       b.brand_name,
       a.unit,
        c.business_division_code,
       c.business_division_name,
        purchase_group_code,
        purchase_group_name,
       c.division_code,
       c.division_name,
       c.category_large_code,
       c.category_large_name,
       c.category_middle_code,
       c.category_middle_name,
       a.category_small_code,
       c.category_small_name,
      sales_cust_num,
       sales_days,
       sales_cust_days,
       qty,
       sales_value,
       profit,
       coalesce(profit/sales_value,0) as profit_rate,
       front_profit,
       min_price,
       max_price,
       regexp_replace(${hiveconf:i_edate},'-',''),
       date_m
from 
(SELECT date_m,
     province_code,
    province_name,
        channel,
       channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
      sales_cust_num,
       sales_days,
       sales_cust_days,
       qty,
       sales_value,
       profit,
       front_profit,
       min_price,
       max_price
from csx_dw.temp_ads_supply_kanban_goods_sales_01
union all 
SELECT date_m,
     province_code,
    province_name,
        channel,
       channel_name,
       goods_code,
       goods_name,
       unit,
       category_small_code,
      sales_cust_num,
       sales_days,
       sales_cust_days,
       qty,
       sales_value,
       profit,
       front_profit,
       min_price,
       max_price
       
from csx_dw.temp_ads_supply_kanban_goods_sales_02
) a 
join 
(select goods_id,goods_name,bar_code,brand_name from csx_dw.goods_m a where sdt='current') b on a.goods_code=b.goods_id
JOIN
(select * from csx_dw.dws_basic_w_a_category_m where sdt='current') c on a.category_small_code=c.category_small_code;

drop table `csx_dw.ads_supply_kanban_goods_sales`;
CREATE TABLE `csx_dw.ads_supply_kanban_goods_sales`
	(province_code STRING COMMENT '省区编码，00 全国',
    province_name STRING COMMENT '省区名称 全国 全国',
        channel STRING COMMENT '渠道编码 00 全渠道',
       channel_name STRING COMMENT '渠道名称 00 全渠道',
		`goods_code` string comment '商品编码'          ,
		`bar_code` string comment '商品条码'            ,
		`goods_name` string comment '商品名称'          ,
		`unit` string comment '销售单位'                ,
		`brand_name` STRING COMMENT '品牌名称',
		`business_division_code` string COMMENT '采购部编码'      ,
		`business_division_name` string COMMENT '采购部名称'      ,
		`purchase_group_code` string COMMENT '课组编码'          ,
		`purchase_group_name` string COMMENT '课组名称'          ,
		division_code               string comment '部类编码'                 ,
		division_name             string comment '部类名称'                   ,
		category_large_code        string comment '大类编码'                  ,
		category_large_name    string comment '大类名称'                      ,
		category_middle_code   string comment '中类编码'                      ,
		category_middle_name  string comment '中类名称'                       ,
		`category_small_code` string comment '小类编码' ,
		category_small_name    string comment '小类名称'                       ,
		`sales_cust_num` bigint comment '销售数'          ,
		`sales_days`     bigint comment '销售天数'          ,
		sales_cust_days BIGINT COMMENT '销售销售天数',
		`qty`            decimal(36,6) comment '销量'   ,
		`sales_value`    decimal(36,6) comment '销售额'   ,
		`profit`         decimal(36,6) comment '毛利额'   ,
		`profit_rate`         decimal(36,6) comment '毛利率'   ,
		`front_profit`	decimal(36,6) comment '前端毛利额'   ,
		`min_price`      decimal(26,6) comment '最低售价'   ,
		`max_price`      decimal(26,6) comment '最高售价'
	)comment '供应链看板商品销售表'
	partitioned by (sdt string COMMENT '日期分区',`date_m` string comment '日期维度，m 本月，y 本年'   )
 STORED AS PARQUET
	
	