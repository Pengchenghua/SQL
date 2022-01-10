--弃用
--导入库存
--导入数据时，20210901
-- 1、将excel 转换csv 文件后，上传 /tmp/pengchenghua/ 目录中，通过以下代码导入表中
LOAD data inpath '/tmp/pengchenghua/库存余额3.csv' OVERWRITE INTO TABLE csx_tmp.temp_inv_cost_05 ; 

LOAD data inpath '/tmp/pengchenghua/库存余额4.csv' OVERWRITE INTO TABLE csx_tmp.temp_inv_cost_05 ; 
select * from  csx_tmp.temp_inv_cost_05;

-- 出现表大分两批导入销售
-- 操作日志表导入，20210901
LOAD data inpath '/tmp/pengchenghua/明细01.csv'  INTO TABLE csx_tmp.temp_inv_sale_01 ; 
LOAD data inpath '/tmp/pengchenghua/明细0.csv'  INTO TABLE csx_tmp.temp_inv_sale_01 ; 

CREATE TABLE `csx_tmp.temp_inv_sale_01`(
 `certi_id` string, 
 `shipped` string, 
 `dc_code` string, 
 `receive_are_code` string, 
 `goods_code` string, 
 `qty` decimal(38,6), 
 `amt` decimal(38,6), 
 `no_tax_amt` decimal(38,6), 
 `operating_time` timestamp)
;

LOAD data inpath '/tmp/pengchenghua/库存明细.csv'  INTO TABLE csx_tmp.temp_inv_cost_04 ; 

show  create table csx_tmp.temp_inv_cost_05;




-- csx_tmp.temp_inv_cost_04 ;-- 操作记录
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
    tax_rate
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
       tax_rate
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') c on a.goods_code=c.goods_id
left join
(select code,name,parent_code from csx_dw.dws_wms_w_a_basic_warehouse_reservoir where level='3') d on a.dc_code=d.parent_code and a.receive_are_code=d.code
where (inv_qty !=0 or inv_amt!=0);


--导出数据
select * from  csx_tmp.temp_inve_01;

select sum(no_tax_inv_amt) from  csx_tmp.temp_inve_01;

select * from  csx_tmp.temp_inve_01;
select sum(no_tax_inv_amt) from  csx_tmp.temp_inve_01;

select sum(coalesce(no_tax_amt,0)) from  csx_tmp.temp_inv_cost_04;

select * from csx_tmp.temp_inv_cost_05 where no_tax_amt  is null;


--DC编码	DC名称	公司代码	公司代码名称	库存编码	库存名称	商品编码	商品条码	商品名称	部类编码	部类名称	大类编码	大类名称	中类编码	中类名称	小类编码	小类名称	管理一级分类编码	管理一级分类名称	管理二级编码	管理二级名称	管理三级编码	管理三级名称	课组编码	课组名称	期末库存量	未税期末库存额	含税库存额


select a.`?shipped_code`,a.dc_code,b.shop_name,company_code,company_name,a.receive_are_code,
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
    a.qty,
    a.no_tax_amt,
    a.amt
from csx_tmp.temp_inv_cost_01 a 
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
       department_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') c on a.goods_code=c.goods_id
left join
(select code,name,parent_code from csx_dw.dws_wms_w_a_basic_warehouse_reservoir where level='3') d on a.dc_code=d.parent_code and a.receive_are_code=d.code
where (qty !=0 or amt!=0);



-- 关联维度
drop table if exists  csx_tmp.temp_inve_01;

create temporary table csx_tmp.temp_inve_01 as 

select a.dc_code,b.shop_name,a.company_code,company_name,a.reservoir_area_code,
    d.name,
    a.product_code,
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
    a.qty,
    a.amt_no_tax,
    a.amt,
    a.tax_rate
from csx_tmp.temp_inv_cost a 
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
       tax_rate
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') c on a.product_code=c.goods_id
left join
(select code,name,parent_code from csx_dw.dws_wms_w_a_basic_warehouse_reservoir where level='3') d on a.dc_code=d.parent_code and a.reservoir_area_code=d.code
where (qty !=0 or a.amt!=0);


select * from  csx_tmp.temp_inve_01;




drop table  csx_tmp.temp_inv_cost_05
 ;
 create table csx_tmp.temp_inv_cost_05
 (
      `shipped` string, 
      `dc_code` string, 
      `receive_are_code` string, 
      `goods_code` string, 
      `qty` decimal(32,6), 
      `amt` decimal(32), 
      `no_tax_amt` decimal(32,6), 
      `cost` decimal(32,6), 
      `no_tax_cost` decimal(32,6) comment '商品单价成本', 
      `tax_rate` decimal(32,6) comment'税率'
    )comment '库存导入'
     ROW FORMAT DELIMITED  fields terminated by ',' 
--  WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
    STORED AS TEXTFILE
 ;


 --开发的结果表导入
 create table csx_tmp.temp_inv_cost 
 (
      `product_code` string, 
      `dc_code` string, 
      `tax_rate` decimal(32,6) comment '税率', 
      `amt_no_tax` decimal(32), 
      `amt` decimal(32,6), 
      `qty` decimal(32,6), 
      `reservoir_area_code`string comment '库区', 
      `company_code` string comment'公司代码'
    )comment '库存导入'
     ROW FORMAT DELIMITED  fields terminated by ',' 
--  WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
    STORED AS TEXTFILE
 ;


 
 --- ===========================
 -- 开发给结果导入，关联维表
 create table csx_tmp.temp_inv_cost 
 (
      `product_code` string, 
      `dc_code` string, 
      `tax_rate` decimal(32,6) comment '税率', 
      `amt_no_tax` decimal(32), 
      `amt` decimal(32,6), 
      `qty` decimal(32,6), 
      `reservoir_area_code`string comment '库区', 
      `company_code` string comment'公司代码'
    )comment '库存导入'
     ROW FORMAT DELIMITED  fields terminated by ',' 
--  WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
    STORED AS TEXTFILE
 ;


LOAD data inpath '/tmp/pengchenghua/7月份期末库存过帐源表2.csv'  INTO TABLE csx_tmp.temp_inv_cost ; 

select sum(amt_no_tax) from  csx_tmp.temp_inv_cost ;
