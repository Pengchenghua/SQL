set edt='${enddate}';
set t_edt=regexp_replace(to_date(${hiveconf:edt}),'-','');
set sdate=trunc(${hiveconf:edt},'MM');
set l_edt=substr(regexp_replace(date_sub(${hiveconf:sdate},1),'-',''),1,6);

-- select ${hiveconf:sdate}; 
 
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
 sum( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS end_qty ,
 sum( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS end_amt_no_tax ,
 sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_tax 
FROM
	csx_dw.dwd_cas_r_d_accounting_stock_detail
where to_date(posting_time) < ${hiveconf:sdate}
 and sdt<= ${hiveconf:t_edt}
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
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.ads_fr_r_m_end_post_inventory partition(months)
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
    end_amt_tax as  amt_tax,
    current_timestamp(),
    ${hiveconf:l_edt} 
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
    
 

CREATE TABLE `csx_tmp.ads_fr_r_m_end_post_inventory`(
  `shipper_code` string comment '货主', 
  `dist_code` string comment '省区编码', 
  `dist_name` string comment '省区名称', 
  `dc_code` string comment 'DC编码', 
  `dc_name` string comment 'DC名称', 
  `company_code` string comment '公司代码', 
  `company_name` string comment '公司代码', 
  `reservoir_area_code` string comment '库区编码', 
  `reservoir_area_name` string comment '库区编码', 
  `goods_code` string comment '商品名称',
  `bar_code` string comment '商品条码',
  `goods_name` string comment '商品名称',
  `division_cdoe` string comment '部类编码',
  `division_name` string comment '部类名称',
  `category_large_code` string comment '大类', 
  `category_large_name` string comment '大类', 
  `category_middle_code` string comment '中类', 
  `category_middle_name` string comment '中类', 
  `category_small_code` string comment '小类', 
  `category_small_name` string comment '小类', 
  `classify_large_code` string comment '管理一级分类', 
  `classify_large_name` string comment '管理一级分类', 
  `classify_middle_code` string comment '管理二级分类', 
  `classify_middle_name` string comment '管理二级分类', 
  `classify_small_code` string comment '管理三级分类', 
  `classify_small_name` string comment '管理三级分类', 
  `purchase_group_code` string comment '采购课组', 
  `purchase_group_name` string comment '采购课组', 
  `qty` decimal(24,6) comment '期末库存量', 
  `amt_no_tax` decimal(26,6) comment '未税期末库存额', 
  `amt_tax` decimal(26,6) comment '含税期末库存额', 
  `update_time` timestamp comment '数据插入日期'
    )comment '财务过帐期末库存'
 partitioned by (`months` string comment '月分区')
STORED AS parquet
;

csx_tmp_ads_fr_r_m_end_post_inventory