--采购退货监控【内控需求】
set edate='${enddate}';
set sdate= date_sub(${hiveconf:edate},60) ;
-- 1.0月入库额
drop table  csx_tmp.temp_entry_01 ;
CREATE temporary table csx_tmp.temp_entry_01 as 
SELECT sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    a.receive_location_code,
    shop_name,
    count(DISTINCT order_code) receive_order_cn,
    sum(amount) receive_amt
FROM csx_dw.dws_wms_r_d_entry_detail a 
LEFT JOIN 
(select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' 
                WHEN province_name LIKE '%上海%'   then town_code
    else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then ''
            WHEN province_name LIKE '%上海%'   then town_name
    else city_name end  city_name,
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
    and  table_type=1 ) b on a.receive_location_code=b.shop_id
WHERE order_type_code LIKE 'P%'
  AND business_type='01'
  and sdt>=regexp_replace(${hiveconf:sdate},'-','')
  and sdt<=regexp_replace(${hiveconf:edate},'-','')
   and b.purpose in ('01','03','07','08','02')
  and a.receive_status='2'
  GROUP BY sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    sdt,
    a.receive_location_code,
    shop_name
    
    ;
    
    
--2.0 退货统计
drop table  csx_tmp.temp_entry_02 ;
CREATE temporary table csx_tmp.temp_entry_02 as 
SELECT sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    a.shipped_location_code,
    b.shop_name,
    count(DISTINCT order_no) shipped_order_cn,
    sum(a.shipped_amount) shipped_amount
FROM csx_dw.dws_wms_r_d_ship_detail a 
LEFT JOIN 
(select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' WHEN province_name LIKE '%上海%'   then town_code else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' WHEN province_name LIKE '%上海%'   then town_name else city_name end  city_name,
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
    and  table_type=1 ) b on a.shipped_location_code=b.shop_id
WHERE a.order_type_code LIKE 'P%'
  AND a.business_type_code='05'
  and return_flag='Y'
  and sdt>=regexp_replace(${hiveconf:sdate},'-','')
  and sdt<=regexp_replace(${hiveconf:edate},'-','')
  and b.purpose  in ('01','03','07','08','02')
  and a.status in ('6','8')
  GROUP BY sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    sdt,
     a.shipped_location_code,
    b.shop_name
    ;



-- 无效订单、超时订单 处理 订单日期   
drop table  csx_tmp.temp_entry_03 ;
create temporary table csx_tmp.temp_entry_03 as 
select
    if(sdt='19990101',regexp_replace(to_date(a.create_time),'-',''),sdt) as sdt,
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
    a.target_location_code,
    shop_name,
    count(DISTINCT case when a.header_status='5' then  a.order_code end ) invalid_order_cn,         --无效取消订单
    sum(case when a.header_status='5' then  a.amount_free_tax end ) invalid_order_amount,           --无效取消订单金额
    count(distinct case when a.last_delivery_date< date_add(to_date(create_time),datediff(a.last_delivery_date,to_date(a.create_time))+1) and a.header_status !='5' then a.order_code end ) timeout_order_cn,  --超时订单订单数
    sum(case when a.last_delivery_date< date_add(to_date(create_time),datediff(a.last_delivery_date,to_date(a.create_time))+1) and a.header_status !='5'  then a.amount_free_tax end ) timeout_order_amount ,       --超时订单金额
    count(distinct case when to_date(a.items_close_time)>a.last_delivery_date and a.header_status !='5' then a.order_code end ) timeout_entry_cn,  --超时订单入库订单数
    sum(case when to_date(a.items_close_time)>a.last_delivery_date and a.header_status !='5'  then a.amount_free_tax end ) timeout_entry_amount ,       --超时订单入库金额
    count(distinct case when  a.last_delivery_date< date_add(to_date(create_time),datediff(a.last_delivery_date,to_date(a.create_time))+1) and a.header_status !='5' AND SDT='19990101' then a.order_code end ) timeout_no_order_cn,  --超时未入库订单
    sum(case when a.last_delivery_date< date_add(to_date(create_time),datediff(a.last_delivery_date,to_date(a.create_time))+1) and a.header_status !='5'AND SDT='19990101'  then a.amount_free_tax end ) timeout_no_order_amount        --超时未入库订单金额
from csx_dw.dws_scm_r_d_order_detail a 
LEFT JOIN 
(select 
    sales_province_code,
    sales_province_name,
    purchase_org,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '9' else  sales_region_code end sales_region_code,
    case when (purchase_org ='P620' and purpose!='07') or shop_id ='W0J8' then '平台' else  sales_region_name end sales_region_name,
    shop_id,
    shop_name,
    case when purchase_org ='P620' and purpose!='07'  then '' WHEN province_name LIKE '%上海%'   then town_code else city_code end  city_code,
    case when purchase_org ='P620' and purpose!='07'  then '' WHEN province_name LIKE '%上海%'   then town_name else city_name end  city_name,
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
    and  table_type=1 ) b on a.target_location_code=b.shop_id
where ( ( sdt>=regexp_replace(${hiveconf:sdate},'-','')
  and sdt<=regexp_replace(${hiveconf:edate},'-','')) or sdt='19990101') and header_status in ('4','5')
and super_class ='1'
and a.target_location_code in  ('01','03','07','08','02')
AND a.source_type in ('1','9','10')  --采购导入 9 工厂采购  10 智能补货
group by  sdt,
    regexp_replace(to_date(a.create_time),'-',''),
    sales_region_code,
    sales_region_name,
    b.province_code,
    b.province_name,
    b.city_code,
    b.city_name,
     a.target_location_code,
    shop_name
    
;
set hive.exec.dynamic.partition.mode=nonstrict;
-- CREATE table csx_tmp.temp_ads_inter_r_d_purchase_return_monitor  as 
INSERT overwrite table csx_tmp.ads_inter_r_d_purchase_return_monitor partition(sdt)
SELECT 
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
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
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    receive_location_code dc_code,
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
from csx_tmp.temp_entry_01
union all 
SELECT sdt,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    shipped_location_code as dc_code,
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
from csx_tmp.temp_entry_02
union all 
select sdt,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
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
from  csx_tmp.temp_entry_03
where 1=1
) a 
GROUP BY  sdt,
    sales_region_code,
    sales_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    dc_code,
    shop_name
;




drop table csx_tmp.ads_inter_r_d_purchase_return_monitor;
CREATE TABLE `csx_tmp.ads_inter_r_d_purchase_return_monitor`(
  
  `sales_region_code` string comment '大区', 
  `sales_region_name` string comment '大区', 
  `province_code` string comment '省区', 
  `province_name` string comment '省区', 
  `city_code` string comment '城市', 
  `city_name` string comment '城市', 
  `dc_code` string comment 'DC编码', 
  `shop_name` string comment 'dc名称', 
  `receive_order_cn` bigint comment'入库单数', 
  `receive_amt` decimal(32,4) comment '入库金额', 
  `shipped_order_cn` bigint comment'退货单数', 
  `shipped_amount` decimal(38,6) comment '退货金额', 
  `invalid_order_cn` bigint comment'订单取消数', 
  `invalid_order_amount` decimal(38,4) comment '订单取消订单金额', 
  `timeout_order_cn` bigint comment'超时订单数', 
  `timeout_order_amount` decimal(38,4) comment '超时订单订单金额',
  `timeout_entry_cn` bigint comment'超时订单入库单数', 
  `timeout_entry_amount` decimal(38,4) comment '超时订单入库金额' ,
  `timeout_no_entry_cn` bigint comment'超时订单未入库单数', 
  `timeout_no_entry_amount` decimal(38,4) comment '超时订单未入库金额' ,
  update_time TIMESTAMP comment '更新日期'
  )
COMMENT '内控-采购退货监控'
PARTITIONed by(sdt string COMMENT '日期分区，入库、退货取关单，订单取创建日期')
STORED AS parquet ;
