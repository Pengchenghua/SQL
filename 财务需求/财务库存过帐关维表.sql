--财务库存期末过帐数据关联维表【20220601】
-- 导入数据时，注意将商品名称清空及中间有空白行
-- csx_tmp.temp_inv_sale_01 ;-- 操作记录
-- csx_tmp.temp_inv_cost_05; --结存库存


--计算期末库存额，库存余额-期间操作日志
drop table if exists csx_tmp.temp_inv_01 ;
create temporary table csx_tmp.temp_inv_01 as 
select a.dc_code,
    a.receive_are_code,
    a.goods_code,
    inv_qty ,
    no_tax_inv_amt,
    inv_amt
from 
(select a.dc_code,
    a.receive_are_code,
    a.goods_code,
    coalesce(a.qty,0)-coalesce(b.qty,0) as inv_qty ,
    coalesce( a.no_tax_amt,0)-coalesce(b.no_tax_amt,0) as no_tax_inv_amt,
    coalesce(a.amt,0)-coalesce(b.amt,0) as inv_amt
from csx_tmp.temp_inv_cost_05 a 
left join 
(select a.dc_code,
    a.receive_are_code,
    a.goods_code,
    sum(a.qty) qty  ,
    sum(a.no_tax_amt) as no_tax_amt,
    sum(a.amt) amt
from
csx_tmp.temp_inv_sale_01 a
-- where operating_time>='2021-08-01 00:00:00.0'
group by 
    a.dc_code,
    a.receive_are_code,
    a.goods_code
)b on a.dc_code=b.dc_code and a.goods_code=b.goods_code and a.receive_are_code=b.receive_are_code
)a ;



-- select sum(no_tax_inv_amt) from csx_tmp.temp_inv_01 ;


-- 关联维度
drop table if exists  csx_tmp.temp_inve_01;

create temporary table csx_tmp.temp_inve_01 as 
select a.dc_code,b.shop_name,company_code,company_name,a.receive_are_code,
    d.name,
    a.goods_code,
    c.bar_code,
    goods_name,
    c.division_code,
    c.division_name,
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
    department_id,
    department_name,
    a.inv_qty,
    a.no_tax_inv_amt,
    a.inv_amt,
    tax_rate,
    valuation_category_code,
    valuation_category_name
from csx_tmp.temp_inv_01 a 
left join 
(select shop_id,shop_name,company_code,company_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.dc_code=b.shop_id
left join
(SELECT goods_id,
       goods_name,
       bar_code,
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
       department_id,
       department_name,
       tax_rate,
       valuation_category_code,
       valuation_category_name
FROM   csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') c on a.goods_code=c.goods_id
left join
(select code,name,parent_code from csx_dw.dws_wms_w_a_basic_warehouse_reservoir where level='3') d on a.dc_code=d.parent_code and a.receive_are_code=d.code
where (inv_qty !=0 or inv_amt!=0);

  set hive.exec.dynamic.partition.mode=nonstrict;
  INSERT OVERWRITE table csx_tmp.report_wms_r_m_post_inventory PARTITION(months)
  select * ,current_timestamp(),'202205' from  csx_tmp.temp_inve_01
  ;
  
  
--导出数据
select * from  csx_tmp.temp_inve_01;

select sum(inv_amt)from  csx_tmp.temp_inve_01;


CREATE TABLE `csx_tmp.report_wms_r_m_post_inventory` (
  `dc_code` string, 
  `shop_name` string, 
  `company_code` string, 
  `company_name` string, 
  `receive_are_code` string, 
  `receive_are_name` string, 
  `goods_code` string, 
  `bar_code`   string, 
  `goods_name` string, 
  `division_code` string, 
  `division_name` string, 
  `category_large_code`  string, 
  `category_large_name`  string, 
  `category_middle_code` string, 
  `category_middle_name` string, 
  `category_small_code`  string, 
  `category_small_name`  string, 
  `classify_large_code`  string, 
  `classify_large_name`  string, 
  `classify_middle_code` string, 
  `classify_middle_name` string, 
  `classify_small_code` string, 
  `classify_small_name` string, 
  `department_id` string, 
  `department_name` string, 
  `inv_qty` decimal(38,6), 
  `no_tax_inv_amt` decimal(38,6), 
  `inv_amt` decimal(38,6), 
  `tax_rate` bigint, 
  `valuation_category_code` string, 
  `valuation_category_name` string)
  comment'财务过帐库存-手工处理'
  partitioned by (months string comment '月分区')
