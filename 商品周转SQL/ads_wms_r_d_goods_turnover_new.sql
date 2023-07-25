 create table csx_tmp.ads_wms_r_d_goods_turnover
 (
  years string COMMENT '年份', 
  months string COMMENT '月份', 
  province_code string COMMENT '标准省区编码', 
  province_name string COMMENT '标准省区名称', 
  dist_code string COMMENT '销售省区编码简称', 
  dist_name string COMMENT '销售省区编码简称', 
  city_code string comment '城市编码地级',
  city_name string comment '城市名称地级名称',
  dc_code string COMMENT 'DC编码', 
  dc_name string COMMENT 'DC名称', 
  goods_id string COMMENT '商品编码', 
  goods_name string COMMENT '商品名称', 
  standard string COMMENT '规格', 
  unit_name string COMMENT '单位', 
  brand_name string COMMENT '品牌', 
  dept_id string COMMENT '课组编码', 
  dept_name string COMMENT '课组名称', 
  business_division_code string COMMENT '采购部编码', 
  business_division_name string COMMENT '采购部名称', 
  division_code string COMMENT '部类编码', 
  division_name string COMMENT '部类名称', 
  category_large_code string COMMENT '大类编码', 
  category_large_name string COMMENT '大类名称', 
  category_middle_code string COMMENT '中类编码', 
  category_middle_name string COMMENT '中类名称', 
  category_small_code string COMMENT '小类编码', 
  category_small_name string COMMENT '小类名称', 
  classify_large_code string COMMENT '管理大类编码', 
  classify_large_name string COMMENT '管理大类名称', 
  classify_middle_code string COMMENT '管理中类编码', 
  classify_middle_name string COMMENT '管理中类名称', 
  classify_small_code string COMMENT '管理小类编码', 
  classify_small_name string COMMENT '管理小类名称', 
  joint_purchase_flag int COMMENT '是否联采商品 1 是 0 否',
  valid_tag string COMMENT '有效标识', 
  valid_tag_name string COMMENT '有效标识名称', 
  goods_status_id string COMMENT '商品状态编码', 
  goods_status_name string COMMENT '商品状态名称', 
  sales_qty decimal(38,6) COMMENT '月累计销售数量', 
  sales_value decimal(38,6) COMMENT '月累计销售额', 
  profit decimal(38,6) COMMENT '月累计毛利额', 
  sales_cost decimal(38,6) COMMENT '月累计销售成本', 
  period_inv_qty decimal(38,6) COMMENT '月累计库存量', 
  period_inv_amt decimal(38,6) COMMENT '月累计库存额', 
  final_qty decimal(38,6) COMMENT '期末库存量', 
  final_amt decimal(38,6) COMMENT '期末库存额', 
  days_turnover decimal(38,6) COMMENT '月周转天数', 
  cost_30day decimal(38,6) COMMENT '近30天成本', 
  sales_30day decimal(38,6) COMMENT '30天日均销售额', 
  qty_30day decimal(38,6) COMMENT '30天销售量', 
  dms decimal(38,6) COMMENT '30天日均销量', 
  inv_sales_days decimal(38,6) COMMENT '库存可销天数', 
  period_inv_qty_30day decimal(38,6) COMMENT '近30天累计库存量', 
  period_inv_amt_30day decimal(38,6) COMMENT '近30天累计库存额', 
  days_turnover_30 decimal(38,6) COMMENT '近30天周转', 
  max_sale_sdt string COMMENT '最近一次销售日期', 
  no_sale_days int COMMENT '未销售天数', 
  dc_type string COMMENT 'DC类型', 
  entry_qty decimal(38,6) COMMENT '最近入库数量', 
  entry_value decimal(38,6) COMMENT '最近入库额', 
  entry_sdt string COMMENT '最近入库日期', 
  entry_days int COMMENT '最近入库日期天数', 
  receipt_amt decimal(38,6) COMMENT '领用金额',
  receipt_qty decimal(38,6) comment '领用数量',
  material_take_amt decimal(38,6) comment '原料消耗金额',
  material_take_qty decimal(38,6) COMMENT '原料消耗数量',
  dc_uses string COMMENT 'DC用途', 
  update_time timestamp COMMENT '更新日期'
  )
COMMENT '物流库存周转剔除直送、一件代发业务'
PARTITIONED BY ( 
  sdt string COMMENT '日期分区')
STORED AS parquet 
;




 set edt='2021-07-18';
 set hive.exec.dynamic.partition.mode=nonstrict;
-- drop table if exists csx_tmp.temp_turn_goods;
-- create temporary table csx_tmp.temp_turn_goods as 
INSERT OVERWRITE TABLE csx_tmp.ads_wms_r_d_goods_turnover_new partition(sdt)
SELECT
    substr(regexp_replace(${hiveconf:edt},'-',''),1,4)years,
    substr(regexp_replace(${hiveconf:edt},'-',''),1,6)months,
    f.dist_code,
    f.dist_name,
    province_code     ,
    province_name     ,
    prefecture_city,
    prefecture_city_name,
    a.dc_code       ,
    dc_name     ,
    a.goods_id    ,
    goods_name    ,
    standard      ,
    unit_name     ,
    brand_name    ,
    dept_id       ,
    dept_name     ,
    business_division_code,
    business_division_name,
    division_code        ,
    division_name        ,
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    a.category_small_code  ,
    category_small_name  ,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    joint_purchase_flag,
    valid_tag      ,
    valid_tag_name ,
    goods_status_id,
    goods_status_name,
    sales_qty      ,
    sales_value    ,
    profit         ,
    profit/sales_value as profit_rate,
    sales_cost     ,
    period_inv_qty ,
    period_inv_amt ,
    final_qty      ,
    final_amt      ,
    days_turnover  ,
    sales_30day     ,
    qty_30day      ,
    days_turnover_30 ,
    cost_30day ,
    dms,
    period_inv_amt_30day ,
    inv_sales_days,
    max_sale_sdt,
    no_sale_days,
    dc_type,
    entry_qty,
    entry_value,
    entry_sdt,
    entry_days,
    receipt_amt,            --领用金额
    receipt_qty,            --领用数量
    material_take_amt,      --原料使用金额
    material_take_qty,      --原料使用数量
    a.dc_uses,
    current_timestamp(),
    regexp_replace(${hiveconf:edt},'-','')
FROM
   csx_tmp.ads_wms_r_d_goods_turnover_backup_20210719 a 
join 
(select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current' 
	) as m on a.category_small_code =m.category_small_code 
LEFT	join 
	(
select location_code as dc_code,
    product_code as goods_code,
    sum(case when move_type = '118A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '118B' then amt_no_tax*(1+tax_rate/100 )*-1  end) receipt_amt,
    sum(case when move_type = '118A' then txn_qty  when  move_type = '118B' then txn_qty*-1 end) receipt_qty,
    sum(case when move_type = '119A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '119B' then amt_no_tax*(1+tax_rate/100 )*-1  end) material_take_amt,
    sum(case when move_type = '119A' then txn_qty  when  move_type = '119B' then txn_qty*-1 end) material_take_qty
from csx_dw.dwd_cas_r_d_accounting_stock_detail a
where sdt>regexp_replace(to_date(date_add(${hiveconf:edt},-30)),'-','')  
    and sdt<=regexp_replace(to_date(${hiveconf:edt}),'-','') 
    group by location_code,
    product_code
) c on a.goods_id=c.goods_code and a.dc_code=c.dc_code
left join 
(select shop_code,shop_id,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' ) d on a.dc_code=d.shop_code and a.goods_id=d.product_code

left join 
(select a.location_code,zone_id,zone_name,dist_code,dist_name,prefecture_city,prefecture_city_name,b.city_group_code,b.city_group_name 
from csx_dw.csx_shop a 
    left join csx_dw.dws_sale_w_a_area_belong  b on a.county_city=b.city_code
    where sdt='current' and table_type=1
    ) f on a.dc_code=f.location_code
--and a.business_division_code='11'
WHERE
    sdt=regexp_replace(to_date(${hiveconf:edt}),'-','') 

;

select * from  csx_tmp.ads_wms_r_d_goods_turnover_new ;
ALTER table  csx_tmp.ads_wms_r_d_goods_turnover_new  DROP PARTITION(sdt='2021-07-18');



--补充历史数据
set hive.exec.dynamic.partition.mode=nonstrict;
-- drop table if exists csx_tmp.temp_turn_goods;
-- create temporary table csx_tmp.temp_turn_goods as 
-- select * from  csx_tmp.temp_rece_01 WHERE GOODS_CODE='10' AND DC_CODE='W053';
drop table if exists csx_tmp.temp_rece_02;
create temporary table csx_tmp.temp_rece_02 as 
SELECT dc_code,
       goods_code,
       sdt,
       sum(receipt_amt) over(partition BY dc_code,goods_code
                             ORDER BY unix_timestamp(sdt,'yyyyMMdd') range BETWEEN 2505600  preceding AND CURRENT ROW) AS receipt_amt,
       sum(receipt_qty) over(partition BY dc_code,goods_code
                                    ORDER BY unix_timestamp(sdt,'yyyyMMdd') range BETWEEN 2505600  preceding AND CURRENT ROW) AS receipt_qty,
       sum(material_take_amt) over(partition BY dc_code,goods_code
                                          ORDER BY unix_timestamp(sdt,'yyyyMMdd') range BETWEEN 2505600  preceding AND CURRENT ROW) AS material_take_amt,
       sum(material_take_qty) over(partition BY dc_code,goods_code
                                          ORDER BY unix_timestamp(sdt,'yyyyMMdd') range BETWEEN 2505600 preceding AND CURRENT ROW) AS material_take_qty
FROM
  (SELECT location_code AS dc_code,
          product_code AS goods_code,
          coalesce(sum(CASE
                           WHEN move_type = '118A' THEN amt_no_tax*(1+tax_rate/100)
                           WHEN move_type = '118B' THEN amt_no_tax*(1+tax_rate/100)*-1
                           ELSE 0
                       END),0) AS receipt_amt,
          coalesce(sum(CASE
                           WHEN move_type = '118A' THEN txn_qty
                           WHEN move_type = '118B' THEN txn_qty*-1
                       END),0) AS receipt_qty,
          coalesce(sum(CASE
                           WHEN move_type = '119A' THEN amt_no_tax*(1+tax_rate/100)
                           WHEN move_type = '119B' THEN amt_no_tax*(1+tax_rate/100)*-1
                           ELSE 0
                       END),0) AS material_take_amt,
          coalesce(sum(CASE
                           WHEN move_type = '119A' THEN txn_qty
                           WHEN move_type = '119B' THEN txn_qty*-1
                           ELSE 0
                       END),0) AS material_take_qty,
           sdt
   FROM csx_dw.dwd_cas_r_d_accounting_stock_detail a
   WHERE sdt > '20190101' -- 历史开始时间前30天(20210601)
     AND sdt <='20210721'  
        --and location_code = 'W0M6'
        --and product_code = '815756'--历史结束时间  (20210719)
 -- and move_type in ('118A','118B','119A','119B')
GROUP BY location_code,
         product_code,
         sdt) a
;



-- select * from  csx_tmp.temp_rece_02 WHERE GOODS_CODE='214747' AND DC_CODE='W053';  里面一层是有数据

INSERT OVERWRITE TABLE csx_tmp.ads_wms_r_d_goods_turnover_new partition(sdt)
SELECT
    substr(a.sdt,1,4)years,
    substr(a.sdt,1,6)months,
    f.dist_code,
    f.dist_name,
    province_code     ,
    province_name     ,
    prefecture_city,
    prefecture_city_name,
    a.dc_code       ,
    dc_name     ,
    a.goods_id    ,
    goods_name    ,
    standard      ,
    unit_name     ,
    brand_name    ,
    dept_id       ,
    dept_name     ,
    business_division_code,
    business_division_name,
    division_code        ,
    division_name        ,
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    a.category_small_code  ,
    category_small_name  ,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    joint_purchase_flag,
    valid_tag      ,
    valid_tag_name ,
    goods_status_id,
    goods_status_name,
    sales_qty      ,
    sales_value    ,
    profit         ,
    sales_cost     ,
    period_inv_qty ,
    period_inv_amt ,
    final_qty      ,
    final_amt      ,
    days_turnover  ,
    a.cost_30day,
    sales_30day     ,
    qty_30day      ,
    dms,
    inv_sales_days,
    a.period_inv_qty_30day,
    period_inv_amt_30day ,
    days_turnover_30 ,
    max_sale_sdt,
    no_sale_days,
    dc_type,
    entry_qty,
    entry_value,
    entry_sdt,
    entry_days,
    receipt_amt,            --领用金额
    receipt_qty,            --领用数量
    material_take_amt,      --原料使用金额
    material_take_qty,      --原料使用数量
    a.dc_uses,
    current_timestamp(),
    a.sdt
FROM
   csx_tmp.ads_wms_r_d_goods_turnover_backup_20210720 a 
join 
(select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current' 
	) as m on a.category_small_code =m.category_small_code 
LEFT join 
 csx_tmp.temp_rece_02 c on a.goods_id=c.goods_code and a.dc_code=c.dc_code and a.sdt=c.sdt
left join 
(select shop_code,shop_id,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' ) d on a.dc_code=d.shop_code and a.goods_id=d.product_code
left join 
(select a.location_code,zone_id,zone_name,dist_code,dist_name,prefecture_city,prefecture_city_name,b.city_group_code,b.city_group_name 
from csx_dw.csx_shop a 
    left join csx_dw.dws_sale_w_a_area_belong  b on a.county_city=b.city_code
    where sdt='current' and table_type=1
    ) f on a.dc_code=f.location_code
-- and a.business_division_code='11'
WHERE
    a.sdt <'20201231' 
  --  and a.dc_code='W053'
    and a.sdt>='20201031'
    ;
    
    
    select * from csx_tmp.ads_wms_r_d_goods_turnover_new where  goods_id='10' AND DC_CODE='W053';