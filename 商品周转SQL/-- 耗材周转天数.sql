-- 耗材周转天数 
--周转天数=期末库存量/30天领用量
set edt='${enddate}';
set e_date=regexp_replace(${hiveconf:edt},'-','');
set s_date=regexp_replace(trunc(${hiveconf:edt},'MM'),'-','');
set l_edate=regexp_replace(date_add(trunc(${hiveconf:edt},'MM'),-1),'-','');
--set s_date=regexp_replace(add_months(${hiveconf:edt},-30),'-','');
--select ${hiveconf:l_edate};
--期末库存
drop table if exists csx_analyse_tmp.csx_analyse_tmp_consumables_turnover;
create  table csx_analyse_tmp.csx_analyse_tmp_consumables_turnover as 
select 
    belong_region_code,
    belong_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    dc_code,
    shop_name,
    a.goods_code,
    goods_name,
    goods_bar_code,
    brand_name,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    purchase_group_code,
    purchase_group_name,
    begin_inventoty_qty,
    begin_inventoty_amt,
    end_inventoty_qty,
    end_inventoty_amt,
    receipt_amt,
    receipt_qty,
    material_take_amt,
    material_take_qty,
    purpose
from 
(select 
    belong_region_code,
    belong_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    dc_code,
    shop_name,
    goods_code,
    purpose,
    sum(begin_inventoty_qty) as begin_inventoty_qty,
    sum(begin_inventoty_amt) as begin_inventoty_amt,
    sum(end_inventoty_qty) as end_inventoty_qty,
    sum(end_inventoty_amt) as end_inventoty_amt,
    sum(receipt_amt) as receipt_amt,
    sum(receipt_qty) as receipt_qty,
    sum(material_take_amt) as material_take_amt,
    sum(material_take_qty) as material_take_qty
from (
select dc_code,
    goods_code,
    case when sdt=regexp_replace(date_add(trunc('${edate}','MM'),-1),'-','') then qty  end as begin_inventoty_qty,
    case when sdt=regexp_replace(date_add(trunc('${edate}','MM'),-1),'-','') then amt end  as begin_inventoty_amt,
    case when sdt=regexp_replace('${edate}','-','') then qty  end as end_inventoty_qty,
    case when sdt=regexp_replace('${edate}','-','') then amt end  as end_inventoty_amt,
    0 as receipt_amt,
    0 as receipt_qty,
    0 as  material_take_amt,
    0 as material_take_qty
from csx_dws.csx_dws_cas_accounting_stock_m_df 
where (sdt=regexp_replace('${edate}','-','') or sdt=regexp_replace(date_add(trunc('${edate}','MM'),-1),'-',''))
    and division_code='15'
    and reservoir_area_code not in ('PD01','PD02','TS01','CY01')
union all 
select location_code as dc_code,
    goods_code,
    0 begin_inventoty_qty,
    0 begin_inventoty_amt,
    0 as end_inventoty_qty,
    0 as end_inventoty_amt,
    sum(case when move_type_code = '118A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '118B' then amt_no_tax*(1+tax_rate/100 )*-1  end) receipt_amt,
    sum(case when move_type_code = '118A' then txn_qty  when  move_type = '118B' then txn_qty*-1 end) receipt_qty,
    sum(case when move_type_code = '119A'  then amt_no_tax*(1+tax_rate/100 ) when  move_type = '119B' then amt_no_tax*(1+tax_rate/100 )*-1  end) material_take_amt,
    sum(case when move_type_code = '119A' then txn_qty  when  move_type = '119B' then txn_qty*-1 end) material_take_qty
from csx_dwd.csx_dwd_cas_accounting_stock_detail_di
where sdt> regexp_replace(date_add(trunc('${edate}','MM'),-1),'-','')
    and sdt<= regexp_replace('${edate}','-','')
    group by location_code,
    goods_code
) a 
join 
(select  belong_region_code,
        belong_region_name,
        basic_performance_province_code province_code,
        basic_performance_province_name province_name,
        basic_performance_city_name city_code,
        basic_performance_city_code city_name,
        shop_code,
        shop_name ,
        purpose
from csx_dim.csx_dim_shop a 
csx_dim.csx_dim_basic_performance_attribution  b on basic_performance_city_code=b.performance_city_code
    where sdt='current' 
) b on a.dc_code=b.shop_code
group by belong_region_code,
        belong_region_name,
        province_code,
        province_name,
        city_code,
        city_name,
        shop_code,
        shop_name ,
        purpose
) a 
join 
(SELECT goods_code,
       goods_name,
       goods_bar_code,
       brand_name,
       division_code,
       division_name,
       category_large_code,
       category_large_name,
       category_middle_code,
       category_middle_name,
       category_small_code,
       category_small_name,
       purchase_group_code,
       purchase_group_name
FROM csx_dim.csx_dim_basic_goods
WHERE sdt='current'
   -- and division_code='15'
and category_middle_code in ('150602','150105','150102','150101','150601','150601','150118','150704','150131','150133',
                            '150702','150106','150108','150701','150122','150120','150107')
 ) b on a.goods_code=b.goods_id;


--insert overwrite directory '/tmp/pengchenghua/ii' row format delimited fields terminated by '\t'
insert overwrite table  csx_analyse.csx_analyse_fr_consumables_turnover_report_mi partition(months)
select   belong_region_code,
    belong_region_name,
    province_code,
    province_name,
    city_code,
    city_name,
    dc_code,
    shop_name,
    a.goods_code,
    goods_name,
    goods_bar_code,
    brand_name,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    purchase_group_code,
    purchase_group_name,
    begin_inventoty_qty,
    begin_inventoty_amt,
    end_inventoty_qty,
    end_inventoty_amt,
    receipt_amt,        --领用金额
    receipt_qty,        --领用数量
    material_take_amt,  --原料消耗金额
    material_take_qty,  --原料消耗数量
    coalesce(receipt_qty+material_take_qty,0) as use_qty,       --合计使用数量=领用+原料消耗
coalesce(receipt_amt+material_take_amt,0) as use_amt,
coalesce(case when coalesce(receipt_qty+material_take_qty,0)<=0 and end_inventoty_qty<>0 then 9999 
else (((begin_inventoty_qty+end_inventoty_qty)/2)/(receipt_qty+material_take_qty))*30 end,0) as turnover_days  ,    
current_timestamp(),
substr(regexp_replace('${edate}','-',''),1,6)
from csx_analyse_tmp.csx_analyse_tmp_consumables_turnover 

;



	CREATE TABLE ` csx_analyse.csx_analyse_fr_consumables_turnover_report_mi`(
        sale_month string comment '销售月份',
	  `region_code` string COMMENT '大区编码', 
	  `region_name` string COMMENT '大区名称', 
	  `province_code` string COMMENT '省区', 
	  `province_name` string COMMENT '省区', 
      `city_code` string COMMENT '城市', 
	  `city_name` string COMMENT '城市名称', 
	  `dc_code` string COMMENT 'DC编码', 
	  `dc_name` string COMMENT 'DC名称', 
	  `goods_code` string COMMENT '商品编码', 
	  `goods_name` string COMMENT '商品名称', 
	  `bar_code` string COMMENT '条码', 
	  `brand_name` string COMMENT '品牌名称', 
	  `division_code` string COMMENT '部类', 
	  `division_name` string COMMENT '部类', 
	  `category_large_code` string COMMENT '大类编码', 
	  `category_large_name` string COMMENT '大类编码', 
	  `category_middle_code` string COMMENT '中类编码', 
	  `category_middle_name` string COMMENT '中类名称', 
	  `category_small_code` string COMMENT '小类编码', 
	  `category_small_name` string COMMENT '小类名称', 
	  `department_id` string COMMENT '课组编码', 
	  `department_name` string COMMENT '课组名称', 
	  `begin_inventoty_qty` decimal(30,6) COMMENT '期初库存', 
	  `begin_inventoty_amt` decimal(30,6) COMMENT '期初库存', 
	  `end_inventoty_qty` decimal(30,6) COMMENT '期末库存', 
	  `end_inventoty_amt` decimal(30,6) COMMENT '期末库存', 
	  `receipt_amt` decimal(38,6) COMMENT '领用', 
	  `receipt_qty` decimal(36,6) COMMENT '领用', 
	  `material_take_amt` decimal(38,6) COMMENT '原料消耗', 
	  `material_take_qty` decimal(36,6) COMMENT '原料消耗', 
	  `use_qty` decimal(38,6) COMMENT '累计消耗=领用+原料消耗', 
	  `use_amt` decimal(38,6) COMMENT '累计消耗=领用+原料消耗', 
	  `turnover_days` decimal(38,24) COMMENT '周转天数=累计消耗/库存量', 
	  `update_time` timestamp COMMENT '插入时间')
	COMMENT '耗材库存周转月'
	PARTITIONED BY ( 
	  `months` string COMMENT '月分区')
	STORED AS parquet 
    ;

    
	CREATE TABLE ` report_csx_analyse_fr_consumables_turnover_report_mi`(
        id bigint NOT NULL auto_
        sale_month varchar(64) comment '销售月份',
	  `region_code` varchar(64) COMMENT '大区编码', 
	  `region_name` varchar(64) COMMENT '大区名称', 
	  `province_code` varchar(64) COMMENT '省区', 
	  `province_name` varchar(64) COMMENT '省区', 
      `city_code` varchar(64) COMMENT '城市', 
	  `city_name` varchar(64) COMMENT '城市名称', 
	  `dc_code` varchar(64) COMMENT 'DC编码', 
	  `dc_name` varchar(64) COMMENT 'DC名称', 
	  `goods_code` varchar(64) COMMENT '商品编码', 
	  `goods_name` varchar(64) COMMENT '商品名称', 
	  `bar_code` varchar(64) COMMENT '条码', 
	  `brand_name` varchar(64) COMMENT '品牌名称', 
	  `division_code` varchar(64) COMMENT '部类', 
	  `division_name` varchar(64) COMMENT '部类', 
	  `category_large_code` varchar(64) COMMENT '大类编码', 
	  `category_large_name` varchar(64) COMMENT '大类编码', 
	  `category_middle_code` varchar(64) COMMENT '中类编码', 
	  `category_middle_name` varchar(64) COMMENT '中类名称', 
	  `category_small_code` varchar(64) COMMENT '小类编码', 
	  `category_small_name` varchar(64) COMMENT '小类名称', 
	  `department_id` varchar(64) COMMENT '课组编码', 
	  `department_name` varchar(64) COMMENT '课组名称', 
	  `begin_inventoty_qty` decimal(30,6) COMMENT '期初库存', 
	  `begin_inventoty_amt` decimal(30,6) COMMENT '期初库存', 
	  `end_inventoty_qty` decimal(30,6) COMMENT '期末库存', 
	  `end_inventoty_amt` decimal(30,6) COMMENT '期末库存', 
	  `receipt_amt` decimal(38,6) COMMENT '领用', 
	  `receipt_qty` decimal(36,6) COMMENT '领用', 
	  `material_take_amt` decimal(38,6) COMMENT '原料消耗', 
	  `material_take_qty` decimal(36,6) COMMENT '原料消耗', 
	  `use_qty` decimal(38,6) COMMENT '累计消耗=领用+原料消耗', 
	  `use_amt` decimal(38,6) COMMENT '累计消耗=领用+原料消耗', 
	  `turnover_days` decimal(38,24) COMMENT '周转天数=累计消耗/库存量', 
	  `update_time` timestamp COMMENT '插入时间',
      p
      )
	COMMENT '耗材库存周转月'

