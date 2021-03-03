--DROP table csx_dw.wms_shipped_day_report;
CREATE TABLE `csx_dw.wms_shipped_day_report`
	(
		shipped_type string comment '业务名称'          ,
		send_date string comment '发货日期'             ,
		shipped_location_code string comment '发货DC' ,
		shipped_location_name string comment '发货名称' ,
		source_system string comment'渠道'            ,
		cust_num        int comment '客户数'                  ,
		order_num       int comment '订单数'                  ,
		goods_num       int comment 'SKU数'                 ,
		qty             decimal(26,6) comment '发货数量'       ,
		shipped_amt     decimal(26,6) comment '发货金额'       ,
		food_goods_num  int comment '食百SKU数'               , --食百SKU数
		food_order_num  int comment '食百订单数'                , --食百订单数
		food_qty        decimal(26,6) comment '食百出库数量'     , --食百出库数量
		food_amt        decimal(26,6) comment '食百出库额'        --食百出库额 	
		fresh_goods_num int comment '生鲜SKU数'           ,
		fresh_order_num int comment '生鲜订单数'            ,
		fresh_qty       decimal(26,6) comment '生鲜出库数量' ,
		fresh_amt       decimal(26,6) comment '生鲜出库额'
	)
	comment '物流发货日报' partitioned by
	(
		sdt string comment '日期分区'
	)
	STORED AS parquet
;

SELECT
	shipped_type                       ,
	send_date                          ,
	shipped_location_code              ,
	shipped_location_name              ,
	source_system                      ,
	SUM(cust_num )       cust_num      ,
	SUM(order_num )      order_num     ,
	SUM(goods_num )      goods_num     ,
	SUM(qty )            qty           ,
	SUM(shipped_amt )    shipped_amt   ,
	SUM(food_goods_num ) food_goods_num,
	SUM(food_order_num ) food_order_num,
	SUM(food_qty )       food_qty      ,
	SUM(food_amt )       food_amt      ,
	sdt
FROM
	csx_dw.wms_shipped_day_report
;

set mapreduce.job.queuename                 =caishixian;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.dynamic.partition.mode        =nonstrict;
set hive.exec.dynamic.partition             =true;
SET hive.exec.parallel                      =TRUE; --（默认false）打开并发，多job无依赖时，并发执行
SET hive.exec.parallel.thread.number        =8;    --（默认8）设置job的并发数
set mapreduce.job.reduces                   =80;
INSERT OVERWRITE table csx_dw.wms_shipped_day_report partition
	(sdt
	)
SELECT
	shipped_type                  ,
	to_date(send_time)AS send_date,
	shipped_location_code         ,
	shipped_location_name         ,
	CASE
		WHEN source_system='BBC'
			THEN 'BBC'
		WHEN source_system='CSMS'
			THEN '大客户'
		WHEN source_system='SSMS'
			THEN '商超'
		WHEN source_system='SCM'
			THEN '供应链'
			ELSE source_system
	END as source_system,
	COUNT(DISTINCT
	CASE
		WHEN source_system IN ('BBC',
							   'CSMS')
			THEN customer_code
		WHEN source_system IN ('SSMS')
			THEN shop_code
		WHEN source_system IN ('SCM')
			THEN supplier_code
	END)                      AS cust_num   ,
	COUNT(DISTINCT order_no)  AS order_num  ,
	COUNT(DISTINCT goods_code)AS goods_num  ,
	sum(shipped_qty)          AS qty        ,
	sum(shipped_qty*price)    AS shipped_amt,
	COUNT(DISTINCT
	CASE
		WHEN division_code IN ('12',
							   '13',
							   '14')
			THEN goods_code
	END) AS food_goods_num, --食百SKU数
	COUNT(DISTINCT
	CASE
		WHEN division_code IN ('12',
							   '13',
							   '14')
			THEN order_no
	END) AS food_order_num, --食百订单数
	sum
		(
			CASE
				WHEN division_code IN ('12',
									   '13',
									   '14')
					THEN shipped_qty
			END
		)
	AS food_qty, --食百出库数量
	sum
		(
			CASE
				WHEN division_code IN ('12',
									   '13',
									   '14')
					THEN shipped_qty*price
			END
		)
	AS food_amt--食百出库额
	,
	COUNT(DISTINCT
	CASE
		WHEN division_code IN ('10',
							   '11')
			THEN goods_code
	END) AS fresh_goods_num, --生鲜SKU数
	COUNT(DISTINCT
	CASE
		WHEN division_code IN ('10',
							   '11')
			THEN order_no
	END) AS fresh_order_num, --生鲜订单数
	sum
		(
			CASE
				WHEN division_code IN ('10',
									   '11')
					THEN shipped_qty
			END
		)
	AS fresh_qty, --生鲜出库数量
	sum
		(
			CASE
				WHEN division_code IN ('10',
									   '11')
					THEN shipped_qty*price
			END
		)
	AS fresh_amt--生鲜出库额
	,
	REGEXP_replace(to_date(send_time),'-','')
FROM
	csx_dw.wms_shipped_order
WHERE
	send_sdt   >=regexp_replace(to_date(date_sub(current_timestamp(),60)),'-','')
	and send_sdt<regexp_replace(to_date(current_timestamp()),'-','')
	AND status IN ('8',
				   '7')
GROUP BY
	to_date(send_time)   ,
	shipped_type         ,
	shipped_location_code,
	shipped_location_name,
	CASE
		WHEN source_system='BBC'
			THEN 'BBC'
		WHEN source_system='CSMS'
			THEN '大客户'
		WHEN source_system='SSMS'
			THEN '商超'
		WHEN source_system='SCM'
			THEN '供应链'
			ELSE source_system
	END
;