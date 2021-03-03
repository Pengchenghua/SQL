set hive.map.aggr = true; --顶端聚合
set hive.groupby.skewindata=false; --数据倾斜
set hive.exec.parallel=true; --可以控制一个sql中多个可并行执行的job
--控制对于同一个sql来说同时可以运行的job的最大值,该参数默认为8
set hive.exec.parallel.thread.number=8;
set hive.exec.dynamic.partition.mode=nonstrict; --动态分区模式：strict至少要有个静态分区，nostrict不限制
set hive.exec.dynamic.partition=true; --开户动态分区
set hive.exec.max.dynamic.partitions.pernode=1000000;--每个mapper节点最多创建1000个分区
set mapreduce.job.queuename=caishixian;  --使用队列
set hive.support.quoted.identifiers=none;  -- 查出剩余所有字段

SET i_edate = '${START_DATE}';


SET s_date=regexp_replace(to_date(trunc(${hiveconf:i_edate},'YY')),'-','');

-- 供应商入库金额 针对DC location_type_code ='1' 仓库

INSERT overwrite table csx_dw.ads_supply_kanban_supplier_entry partition (sdt,date_m)
SELECT substr(sales_months,1,4) as sales_year,sales_months,
coalesce(dist_code,province_code)as province_code,
       coalesce( dist_name,province_name)as province_name,
        supplier_code,
       supplier_name,
       `business_division_code`  ,
		`business_division_name`    ,
       count(DISTINCT CASE
                          WHEN receive_qty>0 THEN goods_code
                      END) AS sku,
       sum(receive_qty)receive_qty,
       sum(receive_amt) receive_amt,
       sum(shipped_qty)shipped_qty,
       sum(shipped_amt) shipped_amt,
        regexp_replace(${hiveconf:i_edate},'-',''),
        'm' as date_m
FROM
  (SELECT substr(sdt,1,6) as sales_months,
        supplier_code,
          supplier_name,
          receive_location_code AS dc_code,
          goods_code,
          category_small_code,
          sum(receive_qty)receive_qty,
          sum(receive_qty*price)receive_amt,
          0 shipped_qty,
          0 shipped_amt
   FROM csx_dw.wms_entry_order
   WHERE sdt>= ${hiveconf:s_date} and sdt<= regexp_replace(${hiveconf:i_edate},'-','')
     AND entry_type ='采购入库'
   GROUP BY supplier_code,
            supplier_name,
            receive_location_code,
            goods_code,
            category_small_code,substr(sdt,1,6) 
   UNION ALL SELECT substr(sdt,1,6) as sales_months,supplier_code,
                    supplier_name,
                    shipped_location_code AS dc_code,
                    goods_code,
                    category_small_code,
                    0 receive_qty,
                    0 receive_amt,
                    sum(shipped_qty)shipped_qty,
                    sum(shipped_qty*price)shipped_amt
   FROM csx_dw.wms_shipped_order
   WHERE sdt>= ${hiveconf:s_date} and sdt<= regexp_replace(${hiveconf:i_edate},'-','')
     AND shipped_type ='采购出库'
   GROUP BY supplier_code,
            supplier_name,
            shipped_location_code,
            goods_code,substr(sdt,1,6),
            category_small_code)a
JOIN
  (SELECT *
   FROM csx_dw.csx_shop
   WHERE sdt='current'
     AND location_type_code ='1') b ON a.dc_code=b.location_code
JOIN
  (SELECT *
   FROM csx_dw.dws_basic_w_a_category_m
   WHERE sdt='current'
    ) c ON a.category_small_code=c.category_small_code
GROUP BY supplier_code,
         supplier_name,
        coalesce(dist_code,province_code),
coalesce( dist_name,province_name),substr(sales_months,1,4),sales_months, `business_division_code`  ,
		`business_division_name`    
;

--月度插入全国
INSERT into  table csx_dw.ads_supply_kanban_supplier_entry partition (sdt,date_m)
SELECT substr(sales_months,1,4) as sales_year,sales_months,
'00'as province_code,
 '全国'as province_name,
        supplier_code,
       supplier_name,
        `business_division_code`  ,
		`business_division_name`    ,
       count(DISTINCT CASE
                          WHEN receive_qty>0 THEN goods_code
                      END) AS sku,
       sum(receive_qty)receive_qty,
       sum(receive_amt) receive_amt,
       sum(shipped_qty)shipped_qty,
       sum(shipped_amt) shipped_amt,
        regexp_replace(${hiveconf:i_edate},'-',''),
        'm' as date_m
FROM
  (SELECT substr(sdt,1,6) as sales_months,
        supplier_code,
          supplier_name,
          receive_location_code AS dc_code,
          goods_code,
          category_small_code,
          sum(receive_qty)receive_qty,
          sum(receive_qty*price)receive_amt,
          0 shipped_qty,
          0 shipped_amt
   FROM csx_dw.wms_entry_order
   WHERE sdt>= ${hiveconf:s_date} and sdt<= regexp_replace(${hiveconf:i_edate},'-','')
     AND entry_type ='采购入库'
   GROUP BY supplier_code,
            supplier_name,
            receive_location_code,
            goods_code,
            category_small_code,substr(sdt,1,6) 
   UNION ALL SELECT substr(sdt,1,6) as sales_months,supplier_code,
                    supplier_name,
                    shipped_location_code AS dc_code,
                    goods_code,
                    category_small_code,
                    0 receive_qty,
                    0 receive_amt,
                    sum(shipped_qty)shipped_qty,
                    sum(shipped_qty*price)shipped_amt
   FROM csx_dw.wms_shipped_order
   WHERE sdt>= ${hiveconf:s_date} and sdt<= regexp_replace(${hiveconf:i_edate},'-','')
     AND shipped_type ='采购出库'
   GROUP BY supplier_code,
            supplier_name,
            shipped_location_code,
            goods_code,substr(sdt,1,6),
            category_small_code)a
JOIN
  (SELECT *
   FROM csx_dw.csx_shop
   WHERE sdt='current'
     AND location_type_code ='1') b ON a.dc_code=b.location_code
JOIN
  (SELECT *
   FROM csx_dw.dws_basic_w_a_category_m
   WHERE sdt='current'
    ) c ON a.category_small_code=c.category_small_code
GROUP BY supplier_code,
         supplier_name,
       substr(sales_months,1,4),sales_months, `business_division_code`  ,
		`business_division_name`    
;

INSERT overwrite table csx_dw.ads_supply_kanban_supplier_entry partition (sdt,date_m)
SELECT sales_year,'' as sales_months,
coalesce(dist_code,province_code)as province_code,
       coalesce( dist_name,province_name)as province_name,
        supplier_code,
       supplier_name,
        `business_division_code`  ,
		`business_division_name`    ,
       count(DISTINCT CASE
                          WHEN receive_qty>0 THEN goods_code
                      END) AS sku,
       sum(receive_qty)receive_qty,
       sum(receive_amt) receive_amt,
       sum(shipped_qty)shipped_qty,
       sum(shipped_amt) shipped_amt,
        regexp_replace(${hiveconf:i_edate},'-',''),
        'y' as date_m
FROM
  (SELECT substr(sdt,1,4) as sales_year,
        supplier_code,
          supplier_name,
          receive_location_code AS dc_code,
          goods_code,
          category_small_code,
          sum(receive_qty)receive_qty,
          sum(receive_qty*price)receive_amt,
          0 shipped_qty,
          0 shipped_amt
   FROM csx_dw.wms_entry_order
   WHERE sdt>= ${hiveconf:s_date} and sdt<= regexp_replace(${hiveconf:i_edate},'-','')
     AND entry_type ='采购入库'
   GROUP BY supplier_code,
            supplier_name,
            receive_location_code,
            goods_code,
            category_small_code,substr(sdt,1,4) 
   UNION ALL SELECT substr(sdt,1,4) as sales_year,supplier_code,
                    supplier_name,
                    shipped_location_code AS dc_code,
                    goods_code,
                    category_small_code,
                    0 receive_qty,
                    0 receive_amt,
                    sum(shipped_qty)shipped_qty,
                    sum(shipped_qty*price)shipped_amt
   FROM csx_dw.wms_shipped_order
   WHERE sdt>= ${hiveconf:s_date} and sdt<= regexp_replace(${hiveconf:i_edate},'-','')
     AND shipped_type ='采购出库'
   GROUP BY supplier_code,
            supplier_name,
            shipped_location_code,
            goods_code,substr(sdt,1,4),
            category_small_code)a
JOIN
  (SELECT *
   FROM csx_dw.csx_shop
   WHERE sdt='current'
     AND location_type_code ='1') b ON a.dc_code=b.location_code
JOIN
  (SELECT *
   FROM csx_dw.dws_basic_w_a_category_m
   WHERE sdt='current'
    ) c ON a.category_small_code=c.category_small_code
GROUP BY supplier_code,
         supplier_name,
       sales_year, `business_division_code`  ,
		`business_division_name`  ,coalesce(dist_code,province_code),
       coalesce( dist_name,province_name)  
;
--插入全国
INSERT into table csx_dw.ads_supply_kanban_supplier_entry partition (sdt,date_m)
SELECT sales_year,'' as sales_months,
'00'as province_code,
'全国'as province_name,
        supplier_code,
       supplier_name,
       `business_division_code` ,
		`business_division_name` ,
       count(DISTINCT CASE
                          WHEN receive_qty>0 THEN goods_code
                      END) AS sku,
       sum(receive_qty)receive_qty,
       sum(receive_amt) receive_amt,
       sum(shipped_qty)shipped_qty,
       sum(shipped_amt) shipped_amt,
        regexp_replace(${hiveconf:i_edate},'-',''),
        'y' as date_m
FROM
  (SELECT substr(sdt,1,4) as sales_year,
        supplier_code,
          supplier_name,
          receive_location_code AS dc_code,
          goods_code,
          category_small_code,
          sum(receive_qty)receive_qty,
          sum(receive_qty*price)receive_amt,
          0 shipped_qty,
          0 shipped_amt
   FROM csx_dw.wms_entry_order
   WHERE sdt>= ${hiveconf:s_date} and sdt<= regexp_replace(${hiveconf:i_edate},'-','')
     AND entry_type ='采购入库'
   GROUP BY supplier_code,
            supplier_name,
            receive_location_code,
            goods_code,
            category_small_code,substr(sdt,1,4) 
   UNION ALL SELECT substr(sdt,1,4) as sales_year,supplier_code,
                    supplier_name,
                    shipped_location_code AS dc_code,
                    goods_code,
                    category_small_code,
                    0 receive_qty,
                    0 receive_amt,
                    sum(shipped_qty)shipped_qty,
                    sum(shipped_qty*price)shipped_amt
   FROM csx_dw.wms_shipped_order
   WHERE sdt>= ${hiveconf:s_date} and sdt<= regexp_replace(${hiveconf:i_edate},'-','')
     AND shipped_type ='采购出库'
   GROUP BY supplier_code,
            supplier_name,
            shipped_location_code,
            goods_code,substr(sdt,1,4),
            category_small_code)a
JOIN
  (SELECT *
   FROM csx_dw.csx_shop
   WHERE sdt='current'
     AND location_type_code ='1') b ON a.dc_code=b.location_code
JOIN
  (SELECT *
   FROM csx_dw.dws_basic_w_a_category_m
   WHERE sdt='current'
    ) c ON a.category_small_code=c.category_small_code
GROUP BY supplier_code,
         supplier_name, `business_division_code`  ,
		`business_division_name`    ,
        sales_year
;
select * from csx_dw.ads_supply_kanban_supplier_entry where date_m='m';
drop table  `csx_dw.ads_supply_kanban_supplier_entry`;
CREATE  TABLE `csx_dw.ads_supply_kanban_supplier_entry`(
  `sales_year` STRING COMMENT '入库年',
  `sales_months` STRING COMMENT '入库月',
  `province_code` string comment  'DC省区', 
  `province_name` string comment  'DC省区名称', 
  `business_division_code` string COMMENT '采购部编码'      ,
  `business_division_name` string COMMENT '采购部名称'      ,
  `supplier_code` string comment  '供应商编码', 
  `supplier_name` string comment  '供应商名称', 
  `sku` bigint comment'入库sku' , 
  `receive_qty` decimal(30,6) comment '入库量', 
  `receive_amt` decimal(38,6) comment '入库额', 
  `shipped_qty` decimal(38,6) comment '退货量', 
  `shipped_amt` decimal(38,6) comment '退货额')
comment '供应链看板，供应商入库'
partitioned by (sdt string comment '日期分区\全量',date_m string comment '日期维度、m 月 ，y 年' )
STORED AS parquet 
;
 