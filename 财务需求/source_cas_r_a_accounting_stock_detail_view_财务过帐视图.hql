
select * from csx_tmp.source_cas_r_a_accounting_stock_detail_view;


drop table if exists csx_tmp.temp_post_goods;
create temporary table csx_tmp.temp_post_goods as 
select 
    shipper_code,
    location_code,
    shop_name,
    a.company_code,
    b.company_name,
    a.reservoir_area_code,
    c.reservoir_area_name,
    a.product_code,
    product_name,
    purchase_group_code,
    purchase_group_name,
    end_qty,
    end_amt_no_tax,
    end_amt_tax
from 
(SELECT
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 sum(IF( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) ) AS end_qty,
  sum( IF (a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS end_amt_no_tax,
 sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_tax 
FROM
	csx_tmp.source_cas_r_a_accounting_stock_detail_view a
where posting_time < '2021-05-01 00:00:00'
 and sdt= '19990101'
GROUP BY
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code 
 ) a 
 left join 
 (select * from csx_dw.dws_basic_w_a_csx_product_info cpi where sdt='current' ) as b 
     on a.product_code=b.product_code and a.location_code=shop_code
LEFT JOIN 
(select warehouse_code,reservoir_area_code,reservoir_area_name from csx_ods.source_wms_w_a_wms_reservoir_area wra)c 
    on a.reservoir_area_code=c.reservoir_area_code and c.warehouse_code=location_code 
;


--select sum(end_amt_no_tax) from csx_tmp.temp_post_goods;
;
select shipper_code,
    dist_code,
    dist_name,
    a.location_code as dc_code,
    c.shop_name as dc_name,
    a.company_code,
    c.company_name,
    reservoir_area_code,
    reservoir_area_name,
    product_code as goods_code,
    bar_code,
    goods_name,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    purchase_group_code,
    purchase_group_name,
    end_qty as  qty,
    end_amt_no_tax as  amt_no_tax,
    end_amt_tax as  amt_tax
from csx_tmp.temp_post_goods a
    left join (
        select goods_id,
            bar_code,
            goods_name,
            division_code,
            division_name,
            category_large_code,
            category_large_name,
            category_middle_code,
            category_middle_name,
            category_small_code,
            category_small_name,
            classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name,
            classify_small_code,
            classify_small_name
        from csx_dw.dws_basic_w_a_csx_product_m
        where sdt = 'current'
    ) b on a.product_code = b.goods_id
    left join (
        select location_code,
            shop_name,
            company_code,
            company_name,
            dist_code,
            dist_name
        from csx_dw.csx_shop
        where sdt = 'current'
    ) c on a.location_code = c.location_code;
    



CREATE TABLE `csx_tmp.source_cas_r_a_accounting_stock_detail_view` (
  `id` bigint  COMMENT 'id',
  `stock_period` string  COMMENT '会计期间',
  `product_code` string  COMMENT '商品编码',
  `product_name` string  COMMENT '商品名称',
  `purchase_group_code` string COMMENT '采购组代码',
  `unit` string  COMMENT '计量单位',
  `location_code` string COMMENT '地点编码',
  `location_name` string COMMENT '地点名称',
  `shipper_code` string COMMENT '货主代码',
  `shipper_name` STRING COMMENT '货主名称',
  `ref_biz_type` string COMMENT '参照业务类型',
  `ref_biz_order_no` string COMMENT '参照业务单号',
  `change_source` string COMMENT '库存变动来源',
  `biz_time` TIMESTAMP COMMENT '业务时间',
  `posting_time` TIMESTAMP COMMENT '过账时间',
  `before_price` decimal(26,6) COMMENT '调整前单价',
  `before_qty` decimal(26,6) COMMENT '调整前数量',
  `before_amt` decimal(26,6) COMMENT '调整前金额',
  `after_price` decimal(26,6) COMMENT '调整后单价',
  `after_qty` decimal(26,6) COMMENT '调整后数量',
  `after_amt` decimal(26,6) COMMENT '调整后金额',
  `tax_rate` decimal(26,6) COMMENT '税率（百分）',
  `txn_qty` decimal(26,6) COMMENT '数量',
  `txn_price` decimal(26,6) COMMENT '记账单价',
  `txn_amt` decimal(26,6) COMMENT '记账金额',
  `remain_qty` decimal(26,6) COMMENT '本批次剩余数量',
  `in_or_out` int COMMENT '1出库或0入库2调整单',
  `batch_no` string COMMENT '批次号 ',
  `reservoir_area_code` string COMMENT '库区代码',
  `reservoir_area_name` string COMMENT '库区名称',
  `create_time` timestamp COMMENT '创建时间',
  `create_by` string COMMENT '创建人',
  `update_time` timestamp  COMMENT '更新时间',
  `update_by` string COMMENT '创建人',
  `frozen` int COMMENT '冻结',
  `adjustment_no` string COMMENT '调整单号',
  `adjustment_value` decimal(26,6) COMMENT '调整值',
  `wms_order_no` string COMMENT 'wms库存移动单号',
  `move_type` string COMMENT '移动类型编码',
  `in_out_type` string COMMENT '出入库类型',
  `credential_no` string COMMENT '凭证编号',
  `supplier_code` string COMMENT '供应商编码',
  `supplier_type` string COMMENT '供应商类型',
  `supplier_name` string COMMENT '供应商名称',
  `move_name` string COMMENT '移动类型名称',
  `wms_batch_no` string COMMENT 'wms批次号',
  `purchase_group_name` string COMMENT '采购组名称',
  `wms_order_type` string COMMENT 'WMS订单类型',
  `valuation_category_code` string COMMENT '评估类编码',
  `valuation_category_name` string COMMENT '评估类名称',
  `purchase_org_code` string COMMENT '采购组织编码',
  `company_code` string COMMENT '公司编码',
  `tax_code` string COMMENT '税码',
  `credential_item_id` bigint COMMENT '凭证明细id',
  `amt_no_tax` decimal(26,6) COMMENT '不含税金额',
  `price_no_tax` decimal(26,6)   COMMENT '不含税单价',
  `after_amt_no_tax` decimal(26,6) COMMENT '调整后金额',
  `after_price_no_tax` decimal(26,6) COMMENT '调整后金额',
  `before_amt_no_tax` decimal(26,6) COMMENT '调整后金额',
  `before_price_no_tax` decimal(26,6) COMMENT '调整后金额',
  `adjustment_value_no_tax` decimal(26,6) COMMENT '不含税调整值',
  `purchase_org_name`string COMMENT '采购组织名称'
)  COMMENT '库存明细表-前端接口使用'
PARTITIONed by (sdt string COMMENT '日期分区')
;


columns='id	,stock_period,product_code,product_name,purchase_group_code,unit,location_code,location_name,shipper_code,shipper_name,ref_biz_type,ref_biz_order_no,change_source,biz_time,posting_time,before_price,before_qty,before_amt	,after_price,after_qty,after_amt,tax_rate,txn_qty,txn_price	,txn_amt,remain_qty	,in_or_out,batch_no,reservoir_area_code,reservoir_area_name,create_time,create_by,update_time,update_by,frozen,adjustment_no,adjustment_value,wms_order_no,move_type,in_out_type,credential_no,supplier_code,supplier_type	,supplier_name,move_name,wms_batch_no,purchase_group_name,wms_order_type,valuation_category_code,valuation_category_name,purchase_org_code,company_code,tax_code,credential_item_id	,amt_no_tax	,price_no_tax,after_amt_no_tax,after_price_no_tax,before_amt_no_tax	,before_price_no_tax,adjustment_value_no_tax,purchase_org_name'
username="all_select"
password="I&^lshoejfj02934"
sqoop import \
 --connect jdbc:mysql://10.0.74.154:3306/csx_b2b_accounting?tinyInt1isBit=false \
 --username "$username" \
 --password "$password" \
 --table accounting_stock_detail_view \
 --fields-terminated-by '\001' \
 --columns "${columns}" \
 --hive-drop-import-delims \
 --null-string '\\N'  \
 --null-non-string '\\N' \
 --hive-overwrite \
 --hive-import \
 --hive-database csx_tmp \
 --hive-table  source_cas_r_a_accounting_stock_detail_view \
 --hive-partition-key sdt \
 --hive-partition-value "19990101"