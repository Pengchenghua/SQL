-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.snappycodec;
set mapred.output.compression.type=block;
set parquet.compression=snappy;


set edt='${enddate}';  --参数
set t_edt=regexp_replace(to_date(${hiveconf:edt}),'-','');  --维表日期
set sdate=trunc(${hiveconf:edt},'MM');  --月初
set l_emon=substr(regexp_replace(date_sub(${hiveconf:sdate},1),'-',''),1,6); --上月  期末库存日期
set l_smon=substr(regexp_replace(add_months(${hiveconf:sdate},-2),'-',''),1,6); --上上月 期初库存日期

--select substr(regexp_replace(add_months(${hiveconf:sdate},-2),'-',''),1,6);
-- 城市组处理
--
drop table if exists csx_tmp.temp_shop;

create temporary table if not exists csx_tmp.temp_shop as 
select case when a.shop_id='W0H4' then '100000'
        else sales_province_code end province_code,
    case when a.shop_id='W0H4' then '供应链'
        else a.sales_province_name end as province_name,
    case when a.shop_id='W0H4' then '100000'
        when a.city_group_code='' and a.sales_province_code='32' then '12'
        else city_group_code end city_group_code ,
    case when a.shop_id='W0H4' then '供应链'
        when a.city_group_code='' and a.sales_province_code='32' then '重庆主城'
        else city_group_name end city_group_name,
    shop_id   
from csx_dw.dws_basic_w_a_csx_shop_m a  
   where sdt='current' and table_type=1  

  ;
  

-- 取库存金额平均值 三级分类占比 剔除PD01,PD02，TS01
drop table if  exists csx_tmp.temp_inv_01;
create temporary table if not exists csx_tmp.temp_inv_01 as 
select 
        pur_class_code,
        pur_class_name,
        wms_storage_type_code,
        wms_storage_type_name,
        city_group_code,
        city_group_name,
        a.classify_large_code,
        a.classify_large_name,
        a.classify_middle_code,
        a.classify_middle_name,
        a.classify_small_name,
        a.classify_small_code,
        avg_inv_amt,
        sum(avg_inv_amt)over(partition by city_group_code,wms_storage_type_code) as sum_amt,  --仓储类型库存金额
        avg_inv_amt/sum(avg_inv_amt)over(partition by city_group_code,wms_storage_type_code) as storage_ratio   --库存占比
from (
select  pur_class_code,
        pur_class_name,
        wms_storage_type_code,
        wms_storage_type_name,
        city_group_code,
        city_group_name,
        a.classify_large_code,
        a.classify_large_name,
        a.classify_middle_code,
        a.classify_middle_name,
        a.classify_small_name,
        a.classify_small_code,
        sum(a.amt_tax/10000)/2.00 as avg_inv_amt     --求平均库存，（期末+期初）/2 转换万元
from csx_tmp.ads_fr_r_m_end_post_inventory a
left join 
csx_tmp.temp_shop b on a.dc_code=b.shop_id
left join 
csx_tmp.ads_fr_w_a_wms_storage_type c on a.classify_small_code=c.classify_small_code
where months between ${hiveconf:l_smon} and ${hiveconf:l_emon}
and a.reservoir_area_code not in ('PD01','PD02','TS01')
group by 
       pur_class_code,
        pur_class_name,
        wms_storage_type_code,
        wms_storage_type_name,
        city_group_code,
        city_group_name,
        a.classify_large_code,
        a.classify_large_name,
        a.classify_middle_code,
        a.classify_middle_name,
        a.classify_small_name,
        a.classify_small_code
)a 
        ;
        
--库存利用率
drop table if  exists csx_tmp.temp_inv_02;
create temporary table if not exists csx_tmp.temp_inv_02 as 
select  a.wms_storage_type_code,
        a.wms_storage_type_name,
        a.city_group_code,
        a.city_group_name,
        sum_inv_amt,
        full_amt,
        coalesce(if(sum_inv_amt/full_amt>1.00,1.00,sum_inv_amt/full_amt),1.00) as wms_use_ratio
from 
(select  a.wms_storage_type_code,
        a.wms_storage_type_name,
        a.city_group_code,
        a.city_group_name,
        sum(avg_inv_amt) as sum_inv_amt 
    from  csx_tmp.temp_inv_01 a 
    group by 
    wms_storage_type_code,
        wms_storage_type_name,
        city_group_code,
        city_group_name
) a 
left join
    (select city_group_code,wms_storage_type_code,full_amt
     from csx_tmp.dws_wms_r_m_inherent_cost
        where months='current'
    )b on a.city_group_code=b.city_group_code and a.wms_storage_type_code=b.wms_storage_type_code;

--select * from csx_tmp.temp_inv_02 where city_group_code='1';
--报损数据
-- 报损统计 frmloss_type_code（报损类型为：37 商品变质、78商品报损、64无bom生产损耗、40自然灾害）
drop table if exists csx_tmp.temp_loss_01;
create temporary table if not exists csx_tmp.temp_loss_01 as 
  select
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(amt)/10000 loss_amt      --转换万元
  from 	csx_dw.ads_wms_r_d_bs_detail_days a 
  left join 
  csx_tmp.temp_shop b on a.dc_code=b.shop_id
  where frmloss_type_code in ('37','78','64','40')
    and sdt>=concat(${hiveconf:l_emon},'01')
    and sdt< regexp_replace(${hiveconf:sdate},'-','')
    group by 
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    ;
-- select concat(substr(regexp_replace(date_sub(trunc('2021-04-24','MM'),1),'-',''),1,6),'01');
--计算在库费用 
drop table if exists csx_tmp.temp_wms_02 ;
create temporary table if not exists csx_tmp.temp_wms_02 as 
select  pur_class_code,
        pur_class_name,
        b.province_code,
        b.province_name,
        a.wms_storage_type_code,
        a.wms_storage_type_name,
        a.city_group_code,
        a.city_group_name,
        a.classify_large_code,
        a.classify_large_name,
        a.classify_middle_code,
        a.classify_middle_name,
        a.classify_small_name,
        a.classify_small_code,
        coalesce(rental_amt,0.00)rental_amt,
        coalesce(fold_stand_amt,0.00)fold_stand_amt,
        coalesce(water_electricity_amt,0.00)water_electricity_amt,
        coalesce(personnel_amt,0.00)personnel_amt,
        coalesce(cost_funds,0.00)cost_funds,
        coalesce(loss_amt,0.00)loss_amt,
        (coalesce(rental_amt,0.00)+ coalesce(fold_stand_amt,0.00)+ coalesce(water_electricity_amt,0.00)+coalesce( personnel_amt,0.00) + coalesce(cost_funds+loss_amt,0.00)) as sum_wms_cost
from 
(
select  pur_class_code,
        pur_class_name,
        a.wms_storage_type_code,
        a.wms_storage_type_name,
        a.city_group_code,
        a.city_group_name,
        a.classify_large_code,
        a.classify_large_name,
        a.classify_middle_code,
        a.classify_middle_name,
        a.classify_small_name,
        a.classify_small_code,
        coalesce(wms_use_ratio,0.00)*coalesce(storage_ratio,0.00)*coalesce(rental,0.00) as rental_amt,
        coalesce(wms_use_ratio,0.00)* coalesce(a.storage_ratio,0.00) * coalesce(fold_stand_amt,0.00.00) as fold_stand_amt,  
        coalesce(wms_use_ratio,0.00)* coalesce(a.storage_ratio,0.00) * coalesce(b.water_electricity_amt,0.00) as water_electricity_amt, --水电费用
        (coalesce(wms_use_ratio,0.00)* coalesce(a.storage_ratio,0.00)* coalesce(personnel_amt,0.00) ) as personnel_amt,
        coalesce(avg_inv_amt,0.00)*0.06/12 as cost_funds,
        coalesce(loss_amt,0.00)*-1.00 as loss_amt,
        coalesce(wms_use_ratio,0.00)* coalesce(a.storage_ratio,0.00) as wms_use_,
        storage_ratio,
        wms_use_ratio
from  csx_tmp.temp_inv_01 a
left join
(select * from csx_tmp.dws_wms_r_m_inherent_cost where months='current') b on a.wms_storage_type_code=b.wms_storage_type_code and a.city_group_code=b.city_group_code
left join csx_tmp.temp_loss_01 c on a.classify_small_code=c.classify_small_code and a.city_group_code=c.city_group_code
left join csx_tmp.temp_inv_02 d on a.city_group_code=d.city_group_code and a.wms_storage_type_code=d.wms_storage_type_code
) a 
left join 
(select distinct city_group_code,city_group_name,province_code,province_name from  csx_tmp.temp_shop) b on a.city_group_code=b.city_group_code

;



insert overwrite table csx_tmp.report_fr_r_d_wms_stock_cost partition(months)
-- show create table csx_tmp.temp_wms_02;
select pur_class_code,
        pur_class_name,
        wms_storage_type_code,
        wms_storage_type_name,
        province_code,
        province_name,
        city_group_code,
        city_group_name,
        a.classify_large_code,
        a.classify_large_name,
        a.classify_middle_code,
        a.classify_middle_name,
        a.classify_small_code,
        classify_small_name,
        rental_amt,
        fold_stand_amt,
        water_electricity_amt,
        personnel_amt,
        cost_funds,
        loss_amt,
        sum_wms_cost,
        current_timestamp(),
        ${hiveconf:l_emon}
from csx_tmp.temp_wms_02 a;



CSX_TMP_REPORT_FR_R_D_WMS_STOCK_COST
CREATE  TABLE `csx_tmp.report_fr_r_d_wms_stock_cost`(

  `pur_class_code` string comment '采购 1、生鲜、2食百', 
  `pur_class_name` string comment '采购 1、生鲜、2食百', 
  `province_code` string comment '省区', 
  `province_name` string comment '省区', 
  `wms_storage_type_code` string comment '仓储类型 1、生鲜、3、冻品、2、食百', 
  `wms_storage_type_name` string comment '仓储类型 1、生鲜、3、冻品、2、食百', 
  `city_group_code` string comment '城市', 
  `city_group_name` string comment '城市', 
  `classify_large_code`string comment '管理一级分类',
  `classify_large_name`string comment '管理一级分类',
  `classify_middle_code`string comment '管理二级分类',
  `classify_middle_name`string comment '管理二级分类',
  `classify_small_code` string comment '管理三级编码', 
  `classify_small_name` string comment '管理三级名称', 
  `rental_amt` decimal(38,6) comment '租金费用', 
  `fold_stand_amt` decimal(38,6) comment '折摊费用', 
  `water_electricity_amt` decimal(38,6) comment '水电费用', 
  `personnel_amt` decimal(38,6) comment '人力费用', 
  `cost_funds` decimal(38,6) comment '资金费用', 
  `loss_amt` decimal(38,6) comment '报损金额', 
  `sum_wms_cost` decimal(38,6) comment '合计费用'
  )comment '商品在库费用'
  partitioned by (months string comment '月分区')
STORED as parquet 
;
