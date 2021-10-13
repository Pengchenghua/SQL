数据需求：
1、高库存又订商品:现有高库存商品，在末次入库的时候，库存周转已经是超过300天的
2、滞销又订商品：末次订货的时候，无销售天数已经达到180天的商品
-- 滞销又订商品
--期间有入库

-- 取第一次高库存商品明细
drop table if exists csx_tmp.tmp_hight_turn_goods_01 ;
create temporary table  csx_tmp.tmp_hight_turn_goods_01 as 
SELECT a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
       a.division_code,
       a.division_name,
       a.goods_id,
       b.bar_code,
       b.goods_name,
       b.unit_name,
       b.standard,
       b.classify_middle_code,
       b.classify_middle_name,
       a.dept_id,
       a.dept_name,
       coalesce(final_amt/final_qty) as cost,
       a.final_qty,
       a.final_amt,
       a.days_turnover_30,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt,
       a.dc_uses
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          unit_name,
          standard,
          bar_code
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
     AND purpose IN ('01')
    -- and shop_id in ${hiveconf:dc}
    -- AND sales_region_code='3'
    --and sales_province_code='24'   --稽核省区编码
    ) c ON a.dc_code=c.shop_id
WHERE    
    sdt='20210901'             --更改查询日期
  AND a.days_turnover_30>300
  ;
  
 -- 高库存最后一次 
drop table if exists csx_tmp.tmp_hight_turn_goods_02 ;
create temporary table  csx_tmp.tmp_hight_turn_goods_02 as 
SELECT a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
       a.division_code,
       a.division_name,
       a.goods_id,
       b.bar_code,
       b.goods_name,
       b.unit_name,
       b.standard,
       b.classify_middle_code,
       b.classify_middle_name,
       a.dept_id,
       a.dept_name,
       coalesce(final_amt/final_qty) as cost,
       a.final_qty,
       a.final_amt,
       a.days_turnover_30,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt,
       a.dc_uses
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          unit_name,
          standard,
          bar_code
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
     AND purpose IN ('01')
    -- and shop_id in ${hiveconf:dc}
    -- AND sales_region_code='3'
    --and sales_province_code='24'   --稽核省区编码
    ) c ON a.dc_code=c.shop_id
WHERE    
    sdt='20210930'             --更改查询日期
  AND a.days_turnover_30>300
  ;




--期间入库数据
create temporary table csx_tmp.tmp_hight_turn_goods_03 as 
select order_code,
    origin_order_code,
    receive_location_code as dc_code,
    goods_code,
    sum(receive_qty) receive_qty,
    sum(price*receive_qty) receive_amt
from csx_dw.dws_wms_r_d_entry_detail
where sdt>='20210902'
    and sdt<='20210929'
    and order_type_code like 'P%'
    and receive_status='2'
group by
    order_code,
    origin_order_code,
    receive_location_code,
    goods_code;


--关联最后一次&第一次 两次同时高库存&关联期间入库数据
create temporary table csx_tmp.tmp_hight_turn_goods_04 as 
select a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
       a.division_code,
       a.division_name,
       a.goods_id,
       a.bar_code,
       a.goods_name,
       a.unit_name,
       a.standard,
       a.classify_middle_code,
       a.classify_middle_name,
       a.dept_id,
       a.dept_name,
       a.cost,
       a.final_qty,
       a.final_amt,
       a.days_turnover_30,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt,
       a.dc_uses,
       b.final_amt as final_amt_b,
       b.days_turnover_30 days_turnover_30_b,
       c.receive_amt,
       c.receive_qty,
       order_array,
       order_cn from csx_tmp.tmp_hight_turn_goods_02 a 
join 
csx_tmp.tmp_hight_turn_goods_01 b on a.dc_code=b.dc_code and a.goods_id=b.goods_id
left join
(select 
    concat_ws(',',collect_set(origin_order_code)) as order_array,
    dc_code,
    goods_code,
    count(distinct origin_order_code) as order_cn,
    sum(receive_qty) receive_qty,
    sum(receive_amt) receive_amt
from csx_tmp.tmp_hight_turn_goods_03
group by  dc_code,
    goods_code
)c on a.dc_code=c.dc_code and a.goods_id=c.goods_code
;
select * from  csx_tmp.tmp_hight_turn_goods_04 ;