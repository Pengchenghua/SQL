-- 物流销售出库统计
INSERT OVERWRITE table csx_tmp.report_r_d_wms_sale_out_fr partition(sdt)
SELECT s.province_code,
       s.province_name,
       s.city_code,
       s.city_name,
       b.wms_order_type_name,
       order_type_code,
       if(to_date(send_time) ='2020-01-01',to_date(a.finish_time),to_date(a.send_time)) AS send_date,
       shipped_location_code,
       shipped_location_name,
       -- BBC SSMS CSMS OMS SCM
        CASE
           WHEN source_system='BBC' THEN 'BBC'
           WHEN source_system in ('CSMS','OMS')  THEN '大'
           WHEN source_system='SSMS' THEN '商超'
           WHEN source_system='SCM' THEN '供应链'
           WHEN source_system='SAP' THEN 'SAP'
           ELSE source_system
       END AS source_system,
       sys_logistics_mode_code, --B端系统：10-配送，11-直送，12-自提，13-直通；BBC系统：21-站点自提，22-同城配送，23-快递配送，29-永辉生活单；商超系统：31-直送，32-直通，33-配送(寄售、自营订单)，34-供应商直送
       sys_logistics_mode_name,
       order_business_type_code, --1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超
       order_business_type_name,
       COUNT(DISTINCT CASE WHEN source_system IN ('BBC','CSMS','OMS') THEN sub_customer_code -- 更改为子 B端
                          WHEN source_system IN ('SSMS') THEN shop_code      -- 商超
                          WHEN source_system IN ('SCM') THEN supplier_code   -- 供应链
                      END)AS cust_num,
       COUNT(DISTINCT order_no)AS order_num,
       COUNT(DISTINCT goods_code)AS goods_num,
       sum(shipped_qty) AS qty,
       sum(shipped_qty*price) AS shipped_amt,
       COUNT(DISTINCT CASE  WHEN division_code IN ('12','13','14') THEN goods_code END) AS food_goods_num, --食百SKU数
       COUNT(DISTINCT CASE WHEN division_code IN ('12','13','14') THEN order_no  END) AS food_order_num, --食百订单数
       sum(CASE  WHEN division_code IN ('12','13','14') THEN shipped_qty  END) AS food_qty, --食百出库数量
       sum(CASE WHEN division_code IN ('12','13','14') THEN shipped_qty*price  END) AS food_amt ,--食百出库额 
       COUNT(DISTINCT CASE  WHEN division_code IN ('10','11') THEN goods_code  END) AS fresh_goods_num, --生鲜SKU数
       COUNT(DISTINCT CASE WHEN division_code IN ('10','11') THEN order_no END) AS fresh_order_num, --生鲜订单数
       sum(CASE WHEN division_code IN ('10','11') THEN shipped_qty END) AS fresh_qty, --生鲜出库数量
       sum(CASE WHEN division_code IN ('10','11') THEN shipped_qty*price END) AS fresh_amt , --生鲜出库额
       sdt as close_date,
       current_timestamp(),
       regexp_replace(if(to_date(send_time) ='2020-01-01',to_date(a.finish_time),to_date(a.send_time)),'-','')
 FROM csx_dw.dws_wms_r_d_ship_detail AS a 
   LEFT JOIN
   (    select 
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
    and  table_type=1 ) s on a.shipped_location_code=s.shop_id
    LEFT JOIN 
    (select * from csx_dw.dws_wms_w_a_business_type where direction='-') as b on a.order_type_code=b.in_out_business_type_code
 WHERE sdt >= regexp_replace(date_sub(${hiveconf:edt},90),'-','')
  AND sdt <=regexp_replace(${hiveconf:edt},'-','')
  AND a.order_type_code like 'S%'
  AND status IN ('8','7', '6')
  AND a.business_type_code not in ('73','56','58','72','05','70','19','17')
  AND a.shipped_location_code like 'W%'
 GROUP BY if(to_date(send_time) ='2020-01-01',to_date(a.finish_time),to_date(a.send_time)),
         b.wms_order_type_name,
         order_type_code,
         shipped_location_code,
         shipped_location_name,
         sys_logistics_mode_code, 
         sys_logistics_mode_name,
         order_business_type_code,
         order_business_type_name,
         s.province_code,
         s.province_name,
         s.city_code,
         s.city_name,
         sdt,
         CASE
           WHEN source_system='BBC' THEN 'BBC'
           WHEN source_system in ('CSMS','OMS')  THEN '大'
           WHEN source_system='SSMS' THEN '商超'
           WHEN source_system='SCM' THEN '供应链'
           WHEN source_system='SAP' THEN 'SAP'
           ELSE source_system
       END;
       
    drop table csx_tmp.report_r_d_wms_sale_out_fr;
	CREATE  TABLE `csx_tmp.report_r_d_wms_sale_out_fr`(
	  `province_code` string comment '省区', 
	  `province_name` string comment '省区', 
	  `city_code` string comment '城市', 
	  `city_name` string comment '城市', 
	  `wms_order_type_name` string comment '单据类型', 
	  `order_type_code` string comment '单据类型', 
	  `send_date` string comment '发货日期当发货日期为2000-01-01 取 finish_time 字段', 
	  `shipped_location_code` string comment '发货DC', 
	  `shipped_location_name` string comment '发货DC', 
	  `source_system` string comment '来源系统', 
	  `sys_logistics_mode_code` int comment '物流配送模式 B端系统：10-配送，11-直送，12-自提，13-直通；BBC系统：21-站点自提，22-同城配送，23-快递配送，29-永辉生活单；商超系统：31-直送，32-直通，33-配送(寄售、自营订单)，34-供应商直送', 
	  `sys_logistics_mode_name` string comment '物流配送模式名称', 
	  `order_business_type_code` string comment '销售订单业务类型', 
	  `order_business_type_name` string comment '销售订单业务类型', 
	  `cust_num` bigint  comment '子数',
	  `order_num` bigint comment'单据数', 
	  `goods_num` bigint comment'SKU数', 
	  `qty` decimal(30,6) comment '出库数量', 
	  `shipped_amt` decimal(38,6) comment '出库金额', 
	  `food_goods_num` bigint comment '食百SKU', 
	  `food_order_num` bigint comment '食百单据量', 
	  `food_qty` decimal(30,6) comment'食百出库量', 
	  `food_amt` decimal(38,6) comment '食百出库额', 
	  `fresh_goods_num` bigint comment '生鲜SKU', 
	  `fresh_order_num` bigint comment '生鲜单据数', 
	  `fresh_qty` decimal(30,6) comment '生鲜出库量', 
	  `fresh_amt` decimal(38,6) comment '生鲜出库额', 
	  `close_date` string comment '关单日期', 
	  `update_time` timestamp comment '更新日期'
      )comment 'WMS销售出库单据统计'
	partitioned by(sdt string comment '日期分区，按照发货日期分区')
	STORED AS parquet 
;