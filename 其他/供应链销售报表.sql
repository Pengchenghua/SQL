set sdate = trunc(date_sub(current_timestamp(),1),'MM');
set edate = to_date(date_sub(current_timestamp(),1));



-- 省区课组
CREATE  table csx_dw.temp_supp_sale_class
as
SELECT province_code,
       province_name,
       case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end as purchas_code,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end as purchas_name,
       department_code,
       department_name,
       sum(CASE when sdt=regexp_replace(${hiveconf:edate},'-','') then sales_value end) as day_sale,
       sum(CASE when sdt=regexp_replace(${hiveconf:edate},'-','') then profit end) as day_profit,
       sum(sales_value)sale,
       sum(profit)profit,
       count(DISTINCT goods_code)sale_sku,
       count(DISTINCT customer_no) sale_cust,
       count(DISTINCT order_no) sale_no,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_sku,
       0 as ring_sale_cust,
       0 as ring_sale_no,
       0 as same_sale,
       0 as same_profit,
       0 as same_sale_sku,
       0 as same_sale_cust,
       0 as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(${hiveconf:edate},'-','')
and sdt>=regexp_replace(${hiveconf:sdate},'-','')
GROUP BY province_code,
         province_name,
          department_code,
       department_name,
       case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end ,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end 
union all  
SELECT province_code,
       province_name,
       case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end as purchas_code,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end as purchas_name,
       department_code,
       department_name,
	   0 as day_sale,
	   0 as day_profit,
       0 as sale,
       0 as profit,
       0 as sale_sku,
       0 as sale_cust,
       0 as sale_no,
       sum(sales_value) as ring_sale,
       sum(profit)as ring_profit,
       count(DISTINCT goods_code)as ring_sale_sku,
       count(DISTINCT customer_no) as ring_sale_cust,
       count(DISTINCT order_no)as  ring_sale_no,
       0 as same_sale,
       0 as same_profit,
       0 as same_sale_sku,
       0 as same_sale_cust,
       0 as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(add_months(${hiveconf:edate},-1),'-','')
and sdt>=regexp_replace(add_months(${hiveconf:sdate},-1),'-','')
GROUP BY province_code,
         province_name,
          department_code,
       department_name,
       case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end ,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end 
union all  
SELECT province_code,
       province_name,
       case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end as purchas_code,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end as purchas_name,
       department_code,
       department_name,
	   0 as day_sale,
	   0 as day_profit,
       0 as sale,
       0 as profit,
       0 as sale_sku,
       0 as sale_cust,
       0 as sale_no,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_sku,
       0 as ring_sale_cust,
       0 as ring_sale_no,
       sum(sales_value)  as same_sale,
       sum(profit) as same_profit,
       count(DISTINCT goods_code) as same_sale_sku,
       count(DISTINCT customer_no)  as same_sale_cust,
       count(DISTINCT order_no) as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(add_months(${hiveconf:edate},-12),'-','')
and sdt>=regexp_replace(add_months(${hiveconf:sdate},-12),'-','')
GROUP BY province_code,
         province_name,
          department_code,
       department_name,
       case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end
;


--省区采购部
CREATE table csx_dw.temp_supp_sale_purchas
as
SELECT province_code,
       province_name,
       case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end as purchas_code,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end as purchas_name,
       '00'department_code,
       '小计'department_name,
       sum(CASE when sdt=regexp_replace(${hiveconf:edate},'-','') then sales_value end) as day_sale,
       sum(CASE when sdt=regexp_replace(${hiveconf:edate},'-','')  then profit end) as day_profit,
       sum(sales_value)sale,
       sum(profit)profit,
       count(DISTINCT goods_code)sale_sku,
       count(DISTINCT customer_no) sale_cust,
       count(DISTINCT order_no) sale_no,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_sku,
       0 as ring_sale_cust,
       0 as ring_sale_no,
       0 as same_sale,
       0 as same_profit,
       0 as same_sale_sku,
       0 as same_sale_cust,
       0 as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(${hiveconf:edate},'-','')
and sdt>=regexp_replace(${hiveconf:sdate},'-','')
GROUP BY province_code,
         province_name,
           case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end ,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end 
union all  
SELECT province_code,
       province_name,
        case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end as purchas_code,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end as purchas_name,
       '00'department_code,
       '小计'department_name,
	   0 as day_sale,
	   0 as day_profit,
       0 as sale,
       0 as profit,
       0 as sale_sku,
       0 as sale_cust,
       0 as sale_no,
       sum(sales_value) as ring_sale,
       sum(profit)as ring_profit,
       count(DISTINCT goods_code)as ring_sale_sku,
       count(DISTINCT customer_no) as ring_sale_cust,
       count(DISTINCT order_no)as  ring_sale_no,
       0 as same_sale,
       0 as same_profit,
       0 as same_sale_sku,
       0 as same_sale_cust,
       0 as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(add_months(${hiveconf:edate},-1),'-','')
and sdt>=regexp_replace(add_months(${hiveconf:sdate},-1),'-','')
GROUP BY province_code,
         province_name,
            case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end ,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end 
union all  
SELECT province_code,
       province_name,
      case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end as purchas_code,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end as purchas_name,
       '00'department_code,
       '小计'department_name,
	   0 as day_sale,
	   0 as day_profit,
       0 as sale,
       0 as profit,
       0 as sale_sku,
       0 as sale_cust,
       0 as sale_no,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_sku,
       0 as ring_sale_cust,
       0 as  ring_sale_no,
       sum(sales_value)  as same_sale,
       sum(profit) as same_profit,
       count(DISTINCT goods_code) as same_sale_sku,
       count(DISTINCT customer_no)  as same_sale_cust,
       count(DISTINCT order_no) as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(add_months(${hiveconf:edate},-12),'-','')
and sdt>=regexp_replace(add_months(${hiveconf:sdate},-12),'-','')
GROUP BY province_code,
         province_name,
           case when division_code in ('11','10') then '11'
            when division_code in ('12','13','14') then '12'
            else division_code 
        end ,
        case when division_code in ('11','10') then '生鲜采购部'
            when division_code in ('12','13','14') then '食百采购部'
            else division_name
        end 
;

--省区汇总
CREATE table csx_dw.temp_supp_sale_prov
as
SELECT province_code,
       province_name,
       '00' purchas_code,
       '合计'purchas_name,
       '00'department_code,
       '小计'department_name,
       sum(CASE when sdt=regexp_replace(${hiveconf:edate},'-','')  then sales_value end) as day_sale,
       sum(CASE when sdt==regexp_replace(${hiveconf:edate},'-','')  then profit end) as day_profit,
       sum(sales_value)sale,
       sum(profit)profit,
       count(DISTINCT goods_code)sale_sku,
       count(DISTINCT customer_no) sale_cust,
       count(DISTINCT order_no) sale_no,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_sku,
       0 as ring_sale_cust,
       0 as ring_sale_no,
       0 as same_sale,
       0 as same_profit,
       0 as same_sale_sku,
       0 as same_sale_cust,
       0 as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(${hiveconf:edate},'-','')
and sdt>=regexp_replace(${hiveconf:sdate},'-','')
GROUP BY province_code,
         province_name
union all  
SELECT province_code,
       province_name,
        '00' purchas_code,
       '合计'purchas_name,
       '00'department_code,
       '小计'department_name,
	   0 as day_sale,
	   0 as day_profit,
       0 as sale,
       0 as profit,
       0 as sale_sku,
       0 as sale_cust,
       0 as sale_no,
       sum(sales_value) as ring_sale,
       sum(profit)as ring_profit,
       count(DISTINCT goods_code)as ring_sale_sku,
       count(DISTINCT customer_no) as ring_sale_cust,
       count(DISTINCT order_no)as  ring_sale_no,
       0 as same_sale,
       0 as same_profit,
       0 as same_sale_sku,
       0 as same_sale_cust,
       0 as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(add_months(${hiveconf:edate},-1),'-','')
and sdt>=regexp_replace(add_months(${hiveconf:sdate},-1),'-','')
GROUP BY province_code,
         province_name
union all  
SELECT province_code,
       province_name,
       '00' purchas_code,
       '合计'purchas_name,
       '00'department_code,
       '小计'department_name,
	   0 as day_sale,
	   0 as day_profit,
       0 as sale,
       0 as profit,
       0 as sale_sku,
       0 as sale_cust,
       0 as sale_no,
       0 as ring_sale,
       0 as ring_profit,
       0 as ring_sale_sku,
       0 as ring_sale_cust,
       0 as  ring_sale_no,
       sum(sales_value)  as same_sale,
       sum(profit) as same_profit,
       count(DISTINCT goods_code) as same_sale_sku,
       count(DISTINCT customer_no)  as same_sale_cust,
       count(DISTINCT order_no) as same_sale_no
FROM csx_dw.customer_sale_m
WHERE sdt<=regexp_replace(add_months(${hiveconf:edate},-12),'-','')
and sdt>=regexp_replace(add_months(${hiveconf:sdate},-12),'-','')
GROUP BY province_code,
         province_name
;

 CREATE TABLE `csx_dw.supply_sale_firm`
 	(
 		`province_code` string comment '省区编码',
 		`province_name` string comment '省区名称'        ,
 		`purchas_code` string  comment '采购部'        ,
 		`purchas_name` string  comment '采购名称'        ,
 		`firm_id` string       comment '商行编码'        ,
 		`firm_name` string     comment '商行名称'        ,
 		`department_code` string  comment '课组编码'           ,
 		`department_name` string   comment '课组名称'          ,
 		`day_sale`       decimal(38,6) comment '昨日额'    ,
 		`day_profit`     decimal(38,6) comment '昨日毛利额'    ,
		`sale`           decimal(38,6) comment '月累计额'    ,
 		`profit`         decimal(38,6) comment '月累计毛利额'    ,
 		`sale_sku`       bigint        comment 'SKU'    ,
 		`sale_cust`      bigint        comment '成交客户数'    ,
 		`sale_no`        bigint        comment '成交笔数'    ,
		sale_ratio       decimal(38,6) comment '占比',
 		`ring_sale`      decimal(38,6) comment '上月额'    ,
 		`ring_profit`    decimal(38,6) comment '上月毛利额'    ,
 		`ring_sale_sku`  bigint        comment '上月SKU'    ,
 		`ring_sale_cust` bigint        comment '上月成交客户'    ,
 		`ring_sale_no`   bigint        comment '上月成交笔数'    ,
 		`same_sale`      decimal(38,6) comment '同期额'    ,
 		`same_profit`    decimal(38,6) comment '同期毛利额'    ,
 		`same_sale_sku`  bigint        comment '同期SKU'    ,
 		`same_sale_cust` bigint        comment '同期成交客户'    ,
 		`same_sale_no`   bigint        comment '同期成交笔数'
 	)comment '供应链商行报表'
	STORED AS parquet
	;