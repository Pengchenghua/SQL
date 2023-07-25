drop  table csx_tmp.report_wms_r_d_fresh_goods_turnover;
CREATE TABLE csx_tmp.report_wms_r_d_fresh_goods_turnover(
  level_id string comment '层次0 全国、1 大区、2 省区、3 城市、4、DC',
  years string COMMENT '年份', 
  months string COMMENT '月份', 
   region_code string COMMENT '大区编码', 
  region_name string COMMENT '大区名称', 
  province_code string COMMENT '标准省区编码', 
  province_name string COMMENT '标准省区名称', 
  dist_code string COMMENT '销售省区编码简称', 
  dist_name string COMMENT '销售省区编码简称', 
  city_code string COMMENT '城市编码地级', 
  city_name string COMMENT '城市名称地级名称', 
  dc_code string COMMENT 'DC编码', 
  dc_name string COMMENT 'DC名称', 
  goods_id string COMMENT '商品编码', 
  goods_name string COMMENT '商品名称', 
   spu_goods_code string COMMENT 'SPU商品名称',  
   spu_goods_name string COMMENT 'SPU商品名称', 
  standard string COMMENT '规格', 
  unit_name string COMMENT '单位', 
  brand_name string COMMENT '品牌', 
  dept_id string COMMENT '课组编码', 
  dept_name string COMMENT '课组名称', 
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
  joint_purchase_flag int comment  '是否联采商品 1 是 0 否', 
  final_qty decimal(38,6) COMMENT '期末库存量', 
  final_amt decimal(38,6) COMMENT '期末库存额', 
  period_inv_qty_30day decimal(38,6) COMMENT '近30天累计库存量', 
  period_inv_amt_30day decimal(38,6) COMMENT '近30天累计库存额', 
  cost_30day decimal(38,6) COMMENT '近30天成本', 
  qty_30day decimal(38,6) COMMENT '30天销售量', 
  receipt_amt decimal(38,6) COMMENT '领用金额', 
  receipt_qty decimal(38,6) COMMENT '领用数量', 
  material_take_amt decimal(38,6) COMMENT '原料消耗金额', 
  material_take_qty decimal(38,6) COMMENT '原料消耗数量', 
   total_cost DECIMAL(38,6) comment '合计成本:原料+领用+30天销售成本',
  days_turnover_30 decimal(38,6) COMMENT '近30天周转', 
  purpose_code string COMMENT 'DC用途,空为全部', 
   purpose string COMMENT 'DC用途,空为全部', 
  update_time timestamp COMMENT '更新日期')
COMMENT '生鲜库存周转按照城市汇总-剔除直送、一件代发业务'
PARTITIONED BY ( sdt string COMMENT '日期分区')
STORED AS parquet 
 
 ;
 set dt='20210727';
 
 drop table  csx_tmp.temp_turn_fresh_goods_01;
 CREATE temporary table csx_tmp.temp_turn_fresh_goods_01 as 
 SELECT purpose_code,
        purpose,
       zone_id,
       zone_name,
       dist_code,
       dist_name,
       city_code,
       city_name,
       dc_code,
       dc_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       a.goods_id,
       joint_purchase_flag,
       sum(final_qty) final_qty,
       sum(final_amt) as final_amt,
       sum(period_inv_qty_30day) as period_inv_qty_30day,       --期间库存量
       sum(period_inv_amt_30day) as period_inv_amt_30day,       --期间库存额
       sum(cost_30day) as cost_30day ,
       sum(qty_30day) as qty_30day,
       sum(coalesce(receipt_amt,0)) as receipt_amt,             --领用库存额
       sum(coalesce(a.receipt_qty,0)) as receipt_qty,             --领用库存额
       sum(coalesce(material_take_amt,0)) as material_take_amt, -- 原料消耗额
       sum(coalesce(a.material_take_qty,0)) as material_take_qty, -- 原料消耗额
       sum(coalesce(receipt_amt,0)+coalesce(material_take_amt,0)+coalesce(cost_30day,0)) as cost_sum,           --总成本
       CASE WHEN coalesce(sum(coalesce(period_inv_amt_30day,0))/sum(coalesce(receipt_amt,0)+coalesce(material_take_amt,0)+coalesce(cost_30day,0)),0)<0 THEN 0 
        ELSE
        coalesce(sum(coalesce(period_inv_amt_30day,0))/sum(coalesce(receipt_amt,0)+coalesce(material_take_amt,0)+coalesce(cost_30day,0)),0) 
        END   as turnover_days                --周转天数
    FROM csx_tmp.ads_wms_r_d_goods_turnover a 
join
(select location_code,zone_id,zone_name,purpose_code,purpose from csx_dw.csx_shop 
where sdt='current' 
    and table_type=1 
    and purchase_org !='P620' 
    -- and dist_code='15'
    and purpose_code in ('01','02','03','08','07') ) b on a.dc_code=b.location_code
WHERE sdt=${hiveconf:dt}
  AND business_division_code='11'
  and (final_qty!=0 and  a.period_inv_qty_30day !=0 and a.cost_30day!=0 )
GROUP BY zone_id,
       zone_name,
       dist_code,
       dist_name,
       city_code,
       city_name,
       dc_code,
       dc_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag,
       purpose_code,
       purpose
 GROUPING SETS
 ((zone_id,
       zone_name,
       dist_code,
       dist_name,
       city_code,
       city_name,
       dc_code,
       dc_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag,
       purpose_code,
       purpose),
   
       -- 城市汇总
      ( zone_id,
        zone_name,
        dist_code,
       dist_name,
       city_code,
       city_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag,
       purpose_code,
       purpose),
        -- 城市汇总
      ( zone_id,
        zone_name,
        dist_code,
       dist_name,
       city_code,
       city_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag),
    -- 省区汇总
 
       ( zone_id,
        zone_name,
        dist_code,
       dist_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag,
       purpose_code,
       purpose),
        (zone_id,
        zone_name,
        dist_code,
       dist_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag),
      -- 大区汇总
       ( zone_id,
        zone_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag,
       purpose_code,
       purpose),
         ( zone_id,
        zone_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag),
       -- 全国汇总
       (classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag,
       purpose_code,
       purpose),
        (classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_id,
       joint_purchase_flag )
       ) 
      
;



create temporary table csx_tmp.temp_fresh_goods_02 as 
    select case when purpose_code is null and a.zone_id is null and a.dist_code is null then '0'
            when a.dist_code is null and a.city_code is null then '1'
            when a.city_code is null and a.dc_code is null then '2'
            when a.dc_code is null then '3'
            else '4' 
            end level_id,
        substr(${hiveconf:dt},1,4) as years,
        substr(${hiveconf:dt},1,6) as months,
      
       coalesce(zone_id,'00')zone_id,
       coalesce(zone_name,'全国')zone_name,
       coalesce(province_code,'00')province_code,
       coalesce(province_name,'-')province_name,
       coalesce(dist_code,'00')dist_code ,
       coalesce(dist_name,'-')dist_name ,
       coalesce(a.city_code,'00') city_code,
       coalesce(city_name,'-')city_name,
       coalesce(dc_code,'00')dc_code,
       coalesce(dc_name,'-')dc_name,
       a.goods_id,
       goods_name,
       spu_goods_code,
       spu_goods_name,
       standard,
       unit_name,
       brand_name,
       department_id,
       department_name,
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
       joint_purchase_flag,
       coalesce(final_qty ,0)final_qty,
       coalesce(final_amt ,0)final_amt,
       coalesce( period_inv_qty_30day,0)period_inv_qty_30day,       --期间库存量
       coalesce(period_inv_amt_30day ,0)period_inv_amt_30day,       --期间库存额
       coalesce(cost_30day ,0)cost_30day ,
       coalesce(qty_30day ,0)qty_30day,
       coalesce(receipt_amt ,0)receipt_amt,             --领用库存额
       coalesce(receipt_qty ,0)receipt_qty,             --领用库存额
       coalesce( material_take_amt,0)material_take_amt,       -- 原料消耗额
       coalesce(material_take_qty ,0)material_take_qty,       -- 原料消耗额
       coalesce(cost_sum ,0) as total_cost,           --总成本
       coalesce( turnover_days,0)turnover_days,       --近30天周转
       coalesce(purpose_code,'00') purpose_code,
       coalesce(purpose,'-')  purpose,
       current_timestamp(),
       ${hiveconf:dt}

from  csx_tmp.temp_turn_fresh_goods_01 a 
join 
(select goods_id,
    goods_name,
    spu_goods_code,
    spu_goods_name,
    standard,
    unit_name,
    brand_name,
    department_id,
    department_name,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name
from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_id=b.goods_id
left join
(select distinct city_code ,location_code as province_code,area_province_name as province_name from csx_dw.dws_sale_w_a_area_belong ) c on a.city_code=c.province_code

order by purpose_code
;

insert overwrite table csx_tmp.report_wms_r_d_fresh_goods_turnover partition(sdt)
 
 
select * from csx_tmp.temp_fresh_goods_02 ;