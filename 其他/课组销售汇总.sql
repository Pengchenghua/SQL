
-- set hive.execution.engine=mr;
--set mapreduce.job.queuename                 =caishixian;
set mapreduce.job.reduces                   =80;
set hive.map.aggr                           =true;
--set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
set s_date ='2020-04-30';
set yesterday     = regexp_replace(${hiveconf:s_date},'-','');
set last_yesterday= regexp_replace(to_date(add_months(${hiveconf:s_date},-1)),'-','');
--本月
set mon     = regexp_replace(trunc(to_date(${hiveconf:s_date}),'MM'),'-','');
set last_mon= regexp_replace(trunc(to_date(add_months(${hiveconf:s_date},-1)),'MM'),'-','');
--本年
set year     = regexp_replace(trunc(to_date(${hiveconf:s_date}),'YY'),'-','');
set last_year= regexp_replace(trunc(to_date(add_months(${hiveconf:s_date},-1)),'YY'),'-','');

--本月
drop table if exists csx_tmp.temp_days_report02
;

CREATE table
	if not exists csx_tmp.temp_days_report02 as
SELECT
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end 	department_code,
	case when division_code='10' then '加工课' else department_name end department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt    >= ${hiveconf:mon}
			AND sdt <= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale a 
WHERE
	sdt    >= ${hiveconf:last_mon}
	AND sdt<= ${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end 	,
	case when division_code='10' then '加工课' else department_name end,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code          ,
	province_name          ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00' AS department_code,
	'小计' AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code         ,
	province_name         ,
	'00'AS division_code  ,
	'合计'AS division_name  ,
	''  AS department_code,
	''  AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code          ,
	province_name          ,
	'00'  AS division_code  ,
	'合计'    AS division_name  ,
	''    AS department_code,
	''    AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name
UNION ALL
SELECT
	province_code        ,
	province_name        ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name   ,
	case when division_code='10' then 'U00' else department_code end 	 department_code   ,
	case when division_code='10' then '加工课' else department_name end department_name
	'全渠道' AS channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
case when division_code='10' then 'U00' else department_code end 	,
case when division_code='10' then '加工课' else department_name end
UNION ALL
SELECT
	province_code          ,
	province_name          ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00'  AS department_code,
	'小计'  AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  
;

insert into table csx_tmp.temp_days_report02
-- 插入课组各渠道统计
SELECT
	'00' as province_code  ,
	'全国'as province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end department_code,	
case when division_code='10' then '加工课' else department_name end	department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt    >= ${hiveconf:mon}
			AND sdt <= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >= ${hiveconf:last_mon}
	AND sdt<= ${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end 	,
case when division_code='10' then '加工课' else department_name end,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 部类各渠道统计
SELECT
	'00' as province_code          ,
	'全国' as province_name          ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00' AS department_code,
	'小计' AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 全国各渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	'00'AS division_code  ,
	'合计'AS division_name  ,
	''  AS department_code,
	''  AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY

	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 插入全国全渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	'00'  AS division_code  ,
	'合计'    AS division_name  ,
	''    AS department_code,
	''    AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'

UNION ALL
-- 插入课组全渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name   ,
	case when division_code='10' then 'U00' else department_code end as	department_code      ,
case when division_code='10' then '加工课' else department_name end  as 	department_name      ,
	'全渠道' AS channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
		count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY

	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end 	,
case when division_code='10' then '加工课' else department_name end

UNION ALL
-- 插入部类统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00'  AS department_code,
	'小计'  AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:mon}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:mon}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_mon}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_mon}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end 
;

--本年
drop table if exists csx_tmp.temp_days_report03
;

CREATE table
	if not exists csx_tmp.temp_days_report03 as
--INSERT overwrite TABLE csx_dw.supply_dispay_report
SELECT
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end as	department_code,
case when division_code='10' then '加工课' else department_name end as department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt    >= ${hiveconf:year}
			AND sdt <= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >= ${hiveconf:last_year}
	AND sdt<= ${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end ,
	case when division_code='10' then 'U00' else department_code end 	,
case when division_code='10' then '加工课' else department_name end,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code          ,
	province_name          ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00' AS department_code,
	'小计' AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code         ,
	province_name         ,
	'00'AS division_code  ,
	'合计'AS division_name  ,
	''  AS department_code,
	''  AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code          ,
	province_name          ,
	'00'  AS division_code  ,
	'合计'    AS division_name  ,
	''    AS department_code,
	''    AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name
UNION ALL
SELECT
	province_code        ,
	province_name        ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end 	department_code      ,
case when division_code='10' then '加工课' else department_name end department_name,
	'全渠道' AS channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end 	,
case when division_code='10' then '加工课' else department_name end

UNION ALL
SELECT
	province_code          ,
	province_name          ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00'  AS department_code,
	'小计'  AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  
;

insert into table csx_tmp.temp_days_report03
--插入课组各渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code  ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end 	department_code      ,
case when division_code='10' then '加工课' else department_name end department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt    >= ${hiveconf:year}
			AND sdt <= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >= ${hiveconf:last_year}
	AND sdt<= ${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY

	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end 	,
case when division_code='10' then '加工课' else department_name end,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 插入部类各渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00' AS department_code,
	'小计' AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 插入全国各渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	'00'AS division_code  ,
	'合计'AS division_name  ,
	''  AS department_code,
	''  AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 插入全国全渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	'00'  AS division_code  ,
	'合计'    AS division_name  ,
	''    AS department_code,
	''    AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
UNION ALL
-- 全国课组全渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end 	department_code      ,
case when division_code='10' then '加工课' else department_name end department_name     ,
	'全渠道' AS channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end 	,
case when division_code='10' then '加工课' else department_name end

UNION ALL
-- 全国部类全渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00'  AS department_code,
	'小计'  AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<= ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt   >= ${hiveconf:year}
					AND sdt<=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt   >= ${hiveconf:year}
			AND sdt<=${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE when
		sdt   >= ${hiveconf:last_year}
					AND sdt<=${hiveconf:last_yesterday}
then 					customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_year}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  
;

-- 昨日
drop table if exists csx_tmp.temp_days_report01;

create temporary table  if not exists  csx_tmp.temp_days_report01
as 
SELECT
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end 	department_code      ,
case when division_code='10' then '加工课' else department_name end department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >= ${hiveconf:last_yesterday}
	AND sdt<= ${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end ,	
case when division_code='10' then '加工课' else department_name end,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code          ,
	province_name          ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00' AS department_code,
	'小计' AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code         ,
	province_name         ,
	'00'AS division_code  ,
	'合计'AS division_name  ,
	''  AS department_code,
	''  AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
SELECT
	province_code          ,
	province_name          ,
	'00'  AS division_code  ,
	'合计'    AS division_name  ,
	''    AS department_code,
	''    AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name
UNION ALL
SELECT
	province_code        ,
	province_name        ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end 	department_code      ,
case when division_code='10' then '加工课' else department_name end department_name,
	'全渠道' AS channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust ,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code  ,
	province_name  ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end ,	
case when division_code='10' then '加工课' else department_name end

UNION ALL
SELECT
	province_code          ,
	province_name          ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name  ,
	'00'  AS department_code,
	'小计'  AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	province_code,
	province_name,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end 
;

insert into table csx_tmp.temp_days_report01
-- 插入课组各渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	case when division_code='10' then 'U00' else department_code end 	department_code      ,
case when division_code='10' then '加工课' else department_name end department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >= ${hiveconf:last_yesterday}
	AND sdt<= ${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end   ,
	case when division_code='10' then 'U00' else department_code end, 	
case when division_code='10' then '加工课' else department_name end,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 插入部类各渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name   ,
	'00' AS department_code,
	'小计' AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	
case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end , 
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 插入全国各渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	'00'AS division_code  ,
	'合计'AS division_name  ,
	''  AS department_code,
	''  AS department_name,
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	
	CASE
		WHEN dc_code='W0H4'
			THEN item_channel
			ELSE channel_name
	END
UNION ALL
-- 插入全国全渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	'00'  AS division_code  ,
	'合计'    AS division_name  ,
	''    AS department_code,
	''    AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
UNION ALL
-- 插入课组全渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name  ,
	case when division_code='10' then 'U00' else department_code end 	department_code      ,
case when division_code='10' then '加工课' else department_name end department_name  ,
	'全渠道' AS channel_name,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust ,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY

	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	case when division_code='10' then 'U00' else department_code end ,	
case when division_code='10' then '加工课' else department_name end

UNION ALL
-- 插入部全渠道统计
SELECT
	'00'as province_code         ,
	'全国'as province_name         ,
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	'00'  AS department_code,
	'小计'  AS department_name,
	'全渠道' AS channel_name   ,
	count(DISTINCT
	CASE
		WHEN sdt   = ${hiveconf:yesterday}
			THEN goods_code
	END) AS sale_sku,
	sum
		(
			CASE
				WHEN sdt   =${hiveconf:yesterday}
					THEN sales_value
			END
		)
	sale,
	sum
		(
			CASE
				WHEN sdt =${hiveconf:last_yesterday}
					THEN sales_value
			END
		)
	last_sale,
	sum
		(
			CASE
				WHEN  sdt =${hiveconf:yesterday}
					THEN profit
			END
		)
	profit,
	sum
		(
			CASE
				WHEN sdt=${hiveconf:yesterday}
					THEN front_profit
			END
		)
	front_profit,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:yesterday}
			THEN customer_no
	END) AS sale_cust,
	count(DISTINCT
	CASE
		WHEN sdt =${hiveconf:last_yesterday}
			THEN customer_no
	END) AS last_sale_cust
FROM
	csx_dw.dws_sale_r_d_customer_sale
WHERE
	sdt    >=${hiveconf:last_yesterday}
	AND sdt<=${hiveconf:yesterday}
	--  AND province_code='1'
GROUP BY
	case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end      ,
	case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  
;

INSERT overwrite  table csx_tmp.ads_sale_r_m_dept_sale_mon_report  partition(sdt)
 
SELECT
	date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
	regexp_replace(channel_name,'\\s','')as channel_name   ,
	sum(coalesce(sale_sku,0)) as sale_sku       ,
	sum(coalesce(sale,0)) as sale           ,
    case when division_code='00' then  
            (sum(coalesce(sale,0))/sum(sum(coalesce(sale,0)))over(partition BY date_m,channel_name
                                                           ORDER BY date_m))*6.00
    else  (sum(coalesce(sale,0)) /sum(sum(coalesce(sale,0)) )over(partition by date_m,province_code,channel_name order by date_m ))*3.00 end  as sale_ratio,
	sum(coalesce(last_sale,0)) as last_sale      ,
	coalesce(sum(coalesce(sale,0))/sum(coalesce(last_sale,0))-1 ,0) as sale_rate,
	sum(coalesce(profit,0)) as profit         ,
	sum(coalesce(profit,0))/sum(coalesce(sale,0)) as profitrate,
	sum(coalesce(front_profit,0)) as front_profit   ,
	sum(coalesce(front_profit,0))/sum(coalesce(sale,0)) as front_profitrate,
	sum(coalesce(sale_cust,0))as sale_cust,
	sum(coalesce(last_sale_cust,0))as last_sale_cust,
	sum(coalesce(sale_cust,0))-sum(coalesce(last_sale_cust,0)) diff_cust,
	current_timestamp() as write_time,
	${hiveconf:yesterday}
from(
SELECT
	'昨日' AS date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
	channel_name   ,
	sale_sku       ,
	sale           ,
	last_sale      ,
	profit         ,
	front_profit   ,
	sale_cust,
	last_sale_cust
FROM
	csx_tmp.temp_days_report01
UNION ALL
SELECT
	'本月' AS date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
	channel_name   ,
	sale_sku       ,
	sale           ,
	last_sale      ,
	profit         ,
	front_profit   ,
	sale_cust,
	last_sale_cust
FROM
	csx_tmp.temp_days_report02
UNION ALL
SELECT
	'本年' AS date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
	channel_name   ,
	sale_sku       ,
	sale           ,
	last_sale      ,
	profit         ,
	front_profit   ,
	sale_cust,
	last_sale_cust
FROM
	csx_tmp.temp_days_report03
) a 
group by date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
	channel_name ;

--	drop table csx_dw.supply_dispay_report;

--   date_add(day,1-casewhen dayofweek(day) =1 then 7 else dayofweek(day) -1end) as week_first_day
--   -- 本周第一天_周一    ,
 -- date_add(day,7-casewhen dayofweek(day) =1 then 7 else dayofweek(day) -1end);


-- CREATE TABLE `csx_dw.dept_sale_mon_report`
	-- (
		-- `date_m` string          comment '日期维度：昨日、本月、本周'  ,
		-- `province_code` string   comment '省区编码，全国 00'  ,
		-- `province_name` string   comment '省区名称，全国'  ,
		-- `division_code` string   comment '部类编码'  ,
		-- `division_name` string   comment '部类名称'  ,
		-- `department_code` string comment '课组编码'  ,
		-- `department_name` string comment '课组名称'  ,
		-- `channel_name` string    comment '渠道，增加全渠道'  ,
		-- `sale_sku`         bigint         comment '动销SKU',
		-- `sale`             decimal(38,6)  comment '销售额',
		-- `last_sale`        decimal(38,6)  comment '环比销售额',
		-- `sale_rate`        decimal(38,18) comment '销售环比率',
		-- `profit`           decimal(38,6)  comment '毛利额',
		-- `profitrate`       decimal(38,18) comment '毛利率',
		-- `front_profit`     decimal(38,6)  comment '前端毛利额',
		-- `front_profitrate` decimal(38,18) comment '前端毛利率',
		-- `sale_cust`        bigint         comment '动销数',
		-- `last_sale_cust`   bigint         comment '环比数',
		-- `diff_cust`        bigint         comment '差'
	-- )comment '省区课组销售日、月、年环比销售表'
	-- partitioned by (sdt string comment '日期分区')
	-- STORED AS parquet
	-- LOCATION 'hdfs://nameservice1/user/hive/warehouse/csx_dw.db/dept_sale_mon_report' 