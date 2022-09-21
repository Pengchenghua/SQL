
-- 1.0月入库额

drop table  csx_analyse_tmp.csx_analyse_tmp_monitor_entry_01 ;
CREATE  table csx_analyse_tmp.csx_analyse_tmp_monitor_entry_01 as 
SELECT sdt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    a.receive_dc_code,
    shop_name,
    count(DISTINCT order_code) receive_order_cn,
    sum(receive_amt) receive_amt
FROM csx_dws.csx_dws_wms_entry_detail_di a 
LEFT JOIN 
(select 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    shop_code,
    shop_name,
    warehouse_purpose_code
from csx_dim.csx_dim_shop
 where sdt='current'    
   ) b on a.receive_dc_code=b.shop_code
WHERE  1=1
    and entry_type LIKE 'P%'
  AND business_type_code='01'
  and sdt>=regexp_replace(trunc('${edate}','MM'),'-','')
  and sdt <=regexp_replace('${edate}','-','')
 -- and b.warehouse_purpose_code in ('01','03','07','08','02')
  and a.receive_status='2'
  GROUP BY sdt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    a.receive_dc_code,
    shop_name
    ;
    
    
   
--2.0 退货统计
drop table  csx_analyse_tmp.csx_analyse_tmp_monitor_entry_02 ;
CREATE  table csx_analyse_tmp.csx_analyse_tmp_monitor_entry_02 as 
SELECT if(sdt='20000101',regexp_replace(to_date(a.finish_time),'-',''),sdt) as sdt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    a.send_dc_code,
    b.shop_name,
    count(DISTINCT order_code) shipped_order_cn,
    sum(a.shipped_amt) shipped_amount
FROM csx_dws.csx_dws_wms_shipped_detail_di a 
LEFT JOIN 
(select 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    shop_code,
    shop_name,
    warehouse_purpose_code
from csx_dim.csx_dim_shop
 where sdt='current'    ) b on a.send_dc_code=b.shop_code
WHERE a.shipped_type LIKE 'P%'
  AND a.business_type_code='05'
  and return_flag='Y'
  and ((a.sdt >= regexp_replace(date_sub('${edate}',90),'-','')
  AND  a.sdt <=regexp_replace('${edate}','-','')) 
        or (a.sdt='20000101' and to_date(a.finish_time) between date_sub('${edate}',90) and '${edate}'))
  and b.warehouse_purpose_code  in ('01','03','07','08','02')
  and a.status in ('6','8')
  GROUP BY if(sdt='20000101',regexp_replace(to_date(a.finish_time),'-',''),sdt),
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    a.send_dc_code,
    b.shop_name
    ;



-- 无效订单、超时订单 处理 订单日期   
drop table  csx_analyse_tmp.csx_analyse_tmp_monitor_entry_03 ;
create  table csx_analyse_tmp.csx_analyse_tmp_monitor_entry_03 as 

select
    if(sdt='19990101',regexp_replace(to_date(a.create_time),'-',''),sdt) as sdt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    target_location_code,
    shop_name,
    count(DISTINCT case when a.header_status='5' then  a.order_code end ) invalid_order_cn,         --无效取消订单
    sum(case when a.header_status='5' then  a.amount_free_tax end ) invalid_order_amount,           --无效取消订单金额
    count(distinct case when a.last_delivery_date< date_add(to_date(create_time),datediff(a.last_delivery_date,to_date(a.create_time))+1) and a.header_status !='5' then a.order_code end ) timeout_order_cn,  --超时订单订单数
    sum(case when a.last_delivery_date< date_add(to_date(create_time),datediff(a.last_delivery_date,to_date(a.create_time))+1) and a.header_status !='5'  then a.amount_free_tax end ) timeout_order_amount ,       --超时订单金额
    count(distinct case when to_date(a.items_close_time)>a.last_delivery_date and a.header_status !='5' then a.order_code end ) timeout_entry_cn,  --超时订单入库订单数
    sum(case when to_date(a.items_close_time)>a.last_delivery_date and a.header_status !='5'  then a.amount_free_tax end ) timeout_entry_amount ,       --超时订单入库金额
    count(distinct case when  a.last_delivery_date< date_add(to_date(create_time),datediff(a.last_delivery_date,to_date(a.create_time))+1) and a.header_status !='5' AND SDT='19990101' then a.order_code end ) timeout_no_order_cn,  --超时未入库订单
    sum(case when a.last_delivery_date< date_add(to_date(create_time),datediff(a.last_delivery_date,to_date(a.create_time))+1) and a.header_status !='5'AND SDT='19990101'  then a.amount_free_tax end ) timeout_no_order_amount        --超时未入库订单金额
from csx_dws.csx_dws_scm_order_detail_di a 
LEFT JOIN 
(select 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    shop_code,
    shop_name,
    warehouse_purpose_code
from csx_dim.csx_dim_shop
 where sdt='current'   ) b on a.target_location_code=b.shop_code
where  ( sdt>=regexp_replace(trunc('${edate}','MM'),'-','')
  and sdt <=regexp_replace('${edate}','-','')
  )
and super_class ='1'
and warehouse_purpose_code in  ('01','03','07','08','02')
AND a.source_type in ('1','9','10')  -- 采购导入 9 工厂采购  10 智能补货
group by  sdt,
    regexp_replace(to_date(a.create_time),'-',''),
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
     a.target_location_code,
    shop_name
    
    
    
    
;

-- CREATE table csx_analyse_tmp.csx_analyse_tmp_monitor_ads_inter_r_d_purchase_return_monitor  as 
 INSERT overwrite table csx_analyse.csx_analyse_fr_inter_purchase_return_monitor_di partition(sdt)



SELECT 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    dc_code,
    shop_name,
    sum(receive_order_cn)receive_order_cn,
    sum(receive_amt)receive_amt,
    sum(shipped_order_cn) shipped_order_cn,
    sum(shipped_amount) shipped_amount,
    sum(coalesce(invalid_order_cn,0)  ) invalid_order_cn,         --无效取消订单
     sum(coalesce(invalid_order_amount,0)) invalid_order_amount,           --无效取消订单金额
    sum(coalesce(timeout_order_cn,0) ) timeout_order_cn,
    sum(coalesce(timeout_order_amount,0)) timeout_order_amount  ,         --超时订单
    sum(coalesce(timeout_entry_cn,0)) timeout_entry_cn,
    sum(coalesce(timeout_entry_amount,0)) timeout_entry_amount ,
    sum(timeout_no_order_cn) timeout_no_order_cn,
    sum(timeout_no_order_amount) timeout_no_order_amount,
    current_timestamp(),
    sdt
from
(

SELECT sdt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    receive_dc_code dc_code,
    shop_name,
     receive_order_cn,
     receive_amt,
    0 shipped_order_cn,
    0 shipped_amount,
    0  invalid_order_cn,         --无效取消订单
    0 timeout_order_cn,  --超时订单
    0 timeout_entry_cn,
    0 timeout_entry_amount,
    0 invalid_order_amount,           --无效取消订单金额
    0 timeout_order_amount,
    0 timeout_no_order_cn,
    0 timeout_no_order_amount
from csx_analyse_tmp.csx_analyse_tmp_monitor_entry_01
union all 
SELECT sdt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    send_dc_code as dc_code,
    shop_name,
    0 receive_order_cn,
    0 receive_amt,
    shipped_order_cn,
    shipped_amount,
    0  invalid_order_cn,         --无效取消订单
    0 timeout_order_cn,  --超时订单
    0 timeout_entry_cn,
    0 timeout_entry_amount,
    0 invalid_order_amount,           --无效取消订单金额
    0 timeout_order_amount,
    0 timeout_no_order_cn,
    0 timeout_no_order_amount
from csx_analyse_tmp.csx_analyse_tmp_monitor_entry_02
union all 
select sdt,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    target_location_code as dc_code,
    shop_name,
    0 receive_order_cn,
    0 receive_amt,
    0 shipped_order_cn,
    0 shipped_amount,
    invalid_order_cn,         --无效取消订单
    timeout_order_cn,  --超时订单
    timeout_entry_cn,
    timeout_entry_amount,
    invalid_order_amount,           --无效取消订单金额
    timeout_order_amount,        --超时订单金额
    timeout_no_order_cn,
    timeout_no_order_amount
from  csx_analyse_tmp.csx_analyse_tmp_monitor_entry_03
where 1=1
) a 
GROUP BY  sdt,
     performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    dc_code,
    shop_name
;




CREATE TABLE `csx_analyse.csx_analyse_fr_inter_purchase_return_monitor_di`(
  `performance_region_code` string COMMENT '大区', 
  `performance_region_name` string COMMENT '大区', 
  `performance_province_code` string COMMENT '省区', 
  `performance_province_name` string COMMENT '省区', 
  `performance_city_code` string COMMENT '城市', 
  `performance_city_name` string COMMENT '城市', 
  `shop_code` string COMMENT 'DC编码', 
  `shop_name` string COMMENT 'dc名称', 
  `receive_order_cn` bigint COMMENT '入库单数', 
  `receive_amt` decimal(32,4) COMMENT '入库金额', 
  `shipped_order_cn` bigint COMMENT '退货单数', 
  `shipped_amount` decimal(38,6) COMMENT '退货金额', 
  `invalid_order_cn` bigint COMMENT '订单取消数', 
  `invalid_order_amount` decimal(38,4) COMMENT '订单取消订单金额', 
  `timeout_order_cn` bigint COMMENT '超时订单数', 
  `timeout_order_amount` decimal(38,4) COMMENT '超时订单订单金额', 
  `timeout_entry_cn` bigint COMMENT '超时订单入库单数', 
  `timeout_entry_amount` decimal(38,4) COMMENT '超时订单入库金额', 
  `timeout_no_entry_cn` bigint COMMENT '超时订单未入库单数', 
  `timeout_no_entry_amount` decimal(38,4) COMMENT '超时订单未入库金额', 
  `update_time` timestamp COMMENT '更新日期')
COMMENT '内控-采购退货监控'
PARTITIONED BY ( `sdt` string COMMENT '日期分区，入库、退货取关单，订单取创建日期')
 
STORED AS parquet 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
