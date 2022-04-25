
select
    a.shop_code ,
    shop_name ,
   b.product_code ,
   b.product_bar_code,
    a.product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name
from(    
select
    shop_code ,
    shop_name ,
   -- product_code,
    product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name ,
    COUNT(DISTINCT product_bar_code ) as aa 
from
    csx_dw.dws_basic_w_a_csx_product_info
where
    shop_code in ('W0A2',
'W0A3',
'W0A5',
'W0A6',
'W0A7',
'W0A8',
'W0F4',
'W0K1',
'W0K5',
'W0R9',
'W0K6',
'W0L3',
'W0N0',
'W0N1',
'W0P5',
'W0P8',
'W0Q2',
'W0Q9')
  and  des_specific_product_status in ('0','2'，'9')
  and sdt='20200823'
group by shop_code ,
    shop_name ,
   -- product_code,
    product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name )a
    left join 
    (select shop_code ,product_code ,product_name ,product_bar_code from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on trim(a.product_name)=trim(b.product_name) and a.shop_code=b.shop_code
where aa >1;





-- 一品多码

create temporary table csx_tmp.temp_goods_01 as 
select
    a.shop_code ,
    shop_name ,
   b.product_code ,
   b.product_bar_code,
    a.product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name
from(    
select
    shop_code ,
    shop_name ,
   -- product_code,
    product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name ,
    COUNT(DISTINCT product_bar_code ) as aa 
from
    csx_dw.dws_basic_w_a_csx_product_info
where
     des_specific_product_status in ('0','2','9')
  and sdt='current'
group by shop_code ,
    shop_name ,
   -- product_code,
    product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name )a
    left join 
    (select shop_code ,product_code ,product_name ,product_bar_code from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on trim(a.product_name)=trim(b.product_name) and a.shop_code=b.shop_code
where aa >1;

select count(distinct a.product_name) as aa  from   csx_tmp.temp_goods_01 a 
join 
(select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b
 on a.shop_code=b.location_code
 group by dist_code,dist_name;







-- 商品动销SKU
select dist_code,dist_name,note, bd,count(distinct goods_code) as aa from 
(select
    dc_code,
    goods_code,
    bd,
    note
from
    (
    select
        dc_code, 
        goods_code,
        case when division_code in ('10','11') then '11' when division_code in ('12','13','14') then '12'
        else division_code
    end bd
from
    csx_dw.dws_sale_r_d_customer_sale
where
    sdt >= '20200801'
    and sdt<'20200901'
   -- and division_code in ('12', '13', '14')
group by
    dc_code,
    goods_code,
    case
        when division_code in ('10','11') then '11'    
        when division_code in ('12','13','14') then '12'
        else division_code
    end ) a
left join (
    SELECT
        product_code,
        CASE
            WHEN cast(review_flow_node_id AS string) LIKE '10%' THEN '非加工'
            WHEN cast(review_flow_node_id AS string) LIKE '20%' THEN '加工'
        END note
    FROM
        csx_ods.source_master_w_a_md_product_apply_review_view
    WHERE
        sdt = '20200831'
        AND review_status = '40' ) b on
    a.goods_code = b.product_code 
)a 
JOIN 
  (select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b 
  on a.dc_code=b.location_code
 and bd='12'
group by  dist_code,dist_name,note,bd
;



-- 一品多码
drop table csx_tmp.temp_goods_03 ;
create temporary table csx_tmp.temp_goods_03 as 
select
    a.shop_code ,
    shop_name ,
   b.product_code ,
    b.product_bar_code,
    a.product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name
from(    
select
    shop_code ,
    shop_name ,
   -- product_code,
    product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name ,
    COUNT(DISTINCT product_bar_code ) as aa 
from
    csx_dw.dws_basic_w_a_csx_product_info
where
     des_specific_product_status in ('0','2','9')
  and sdt='current'
group by shop_code ,
    shop_name ,
   -- product_code,
    product_name,
    root_category_code,
    root_category_name,
    big_category_code,
    big_category_name,
    middle_category_code,
    middle_category_name,
    small_category_code,
    small_category_name,
    purchase_group_code,
    purchase_group_name )a
    left join 
    (select shop_code ,product_code ,product_name ,product_bar_code from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on trim(a.product_name)=trim(b.product_name) and a.shop_code=b.shop_code
where aa >1;

-- 省区
select dist_code,dist_name,note,bd,count(distinct a.product_name) as aa  from  

(select shop_code,
    a.product_name ,
    case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13','14') then '12' else root_category_code end   as bd,
    note
from   csx_tmp.temp_goods_03 a 
left join 
(SELECT a.product_name,
        CASE
              WHEN cast(review_flow_node_id AS string) LIKE '10%' THEN '非加工'
              WHEN cast(review_flow_node_id AS string) LIKE '20%' THEN '加工'
        END note
   FROM csx_ods.source_master_w_a_md_product_apply_review_view a 
   WHERE sdt='20200831'
    AND review_status='40' 
)d  on a.product_name=d.product_name
)a
 join  
(select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b
 on a.shop_code=b.location_code
 -- where location_code is not null
 group by  dist_code,dist_name,note,bd;
 
 
-- 汇总
select bd,count(distinct a.product_name) as aa  from  

(select shop_code,
    a.product_name ,
    case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13','14') then '12' else root_category_code end   as bd,
    note
from   csx_tmp.temp_goods_03 a 
left join 
(SELECT a.product_name,
        CASE
              WHEN cast(review_flow_node_id AS string) LIKE '10%' THEN '非加工'
              WHEN cast(review_flow_node_id AS string) LIKE '20%' THEN '加工'
        END note
   FROM csx_ods.source_master_w_a_md_product_apply_review_view a 
   WHERE sdt='20200831'
    AND review_status='40' 
)d  on a.product_name=d.product_name
)a
 join  
(select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b
 on a.shop_code=b.location_code
 and  bd ='12'
 group by  bd;


 --
 -- 关联商品级别、商品池表
drop table csx_tmp.temp_goods_01
;
CREATE temporary table csx_tmp.temp_goods_01
as 
SELECT a.shop_code,
       a.product_code,
       a.root_category_code,
       a.root_category_name,
       product_level,
       product_level_name,
       goods_spc,
       create_date
from 
(SELECT a.shop_code,
       a.product_code,
       a.root_category_code,
       a.root_category_name,
       regexp_replace(to_date(a.create_time),'-','')create_date
FROM csx_dw.dws_basic_w_a_csx_product_info a
LEFT JOIN 
-- 商品级别

  (SELECT goods_id,
          product_level,
          product_level_name
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='20200831') d ON a.product_code=d.goods_id
where a.sdt='current' and a.des_specific_product_status in ('2','0','9')
)a 

LEFT JOIN 
-- 商品池
  (SELECT location_code,
          product_code,
          'spc' AS goods_spc
   FROM csx_ods.source_scm_w_a_scm_product_pool
   WHERE sdt='20200831') b ON a.product_code=b.product_code
AND a.shop_code=b.location_code 
;

-- 关联加工与非加工表 10 非加工、20 加工
CREATE temporary table csx_tmp.temp_goods_02
as 
SELECT a.shop_code,
       a.product_code,
       a.root_category_code,
       a.root_category_name,
       product_level,
       product_level_name,
       goods_spc,
       note,
       create_date
from  csx_tmp.temp_goods_01 a 
-- 加工与非加工
LEFT JOIN
  (SELECT product_code,
          CASE
              WHEN cast(review_flow_node_id AS string) LIKE '10%' THEN '非加工'
              WHEN cast(review_flow_node_id AS string) LIKE '20%' THEN '加工'
          END note
   FROM csx_ods.source_master_w_a_md_product_apply_review_view
   WHERE sdt='20200831'
    AND review_status='40' )c ON a.product_code=c.product_code

;



-- 销售SKU
create temporary table csx_tmp.temp_goods_04 as 
select dc_code,goods_code,bd,note from 
(select  dc_code,goods_code,case when division_code in ('10','11') then '11' when  division_code in ('12','13','14') then '12' else division_code end bd
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200801' and sdt<'20200901'
and division_code in ('12','13','14')
group by dc_code,
    goods_code,
    case when division_code in ('10','11') then '11' when  division_code in ('12','13','14') then '12' else division_code end 
) a 
left join 
(SELECT product_code,
          CASE
              WHEN cast(review_flow_node_id AS string) LIKE '10%' THEN '非加工'
              WHEN cast(review_flow_node_id AS string) LIKE '20%' THEN '加工'
          END note
   FROM csx_ods.source_master_w_a_md_product_apply_review_view
   WHERE sdt='20200831'
    AND review_status='40' ) b on a.goods_code=b.product_code
;


-- 新建SKU

select dist_code,dist_name,count(distinct a.product_code) as all_sku,
count(distinct case when a.create_date>='20200801' then a.product_code end )as new_goods ,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc
from  csx_tmp.temp_goods_02 a 
join 
(select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b on a.shop_code=b.location_code
  and a.root_category_code in ('12','13','14') 
 --  AND a.create_date>='20200801'
group by dist_code,dist_name
;

-- 生鲜 统计数据 加工与非加工
select note,dist_code,dist_name,count(distinct a.product_code)all_sku,
count(distinct case when a.create_date>='20200801' then a.product_code end )as new_goods ,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc
from  csx_tmp.temp_goods_02 a 
join 
(select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b on a.shop_code=b.location_code
  and a.root_category_code in ('10','11') 
group by note,dist_code,dist_name
;
--食百 统计数据 加工与非加工
select note,dist_code,dist_name,count(distinct a.product_code)all_sku,
count(distinct case when a.create_date>='20200801' then a.product_code end )as new_goods ,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc
from  csx_tmp.temp_goods_02 a 
join 
(select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b on a.shop_code=b.location_code
  and a.root_category_code in ('12','13','14') 
group by note,dist_code,dist_name
;


--  统计数据 加工与非加工
select dist_code,dist_name,count(distinct a.product_code)all_sku,
count(distinct case when a.create_date>='20200801' then a.product_code end )as new_goods ,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc,
count(distinct case when a.product_level!='3' then  a.product_code end)as level_sku
from  csx_tmp.temp_goods_02 a 
join 
(select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b on a.shop_code=b.location_code
--  and a.root_category_code in ('10','11') 
group by dist_code,dist_name
;

-- 汇总 统计数据 加工与非加工
select count(distinct a.product_code)all_sku,
count(distinct case when a.create_date>='20200801' then a.product_code end )as new_goods ,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc,
count(distinct case when a.product_level!='3' then  a.product_code end)as level_sku
from  csx_tmp.temp_goods_02 a 
join 
(select dist_code,dist_name,location_code 
    from csx_dw.csx_shop where sdt='current' and location_type_code='1' and dist_code not in ('34','35')) b on a.shop_code=b.location_code
--  and a.root_category_code in ('10','11') 
--  group by dist_code,dist_name
;


--------------------------------- 分割线-------------------------------------------
-- 各省区商品
drop table csx_tmp.temp_goods_01;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_01 AS
SELECT dist_code,
       dist_name,
       a.product_code,
       a.root_category_code,
       a.root_category_name,
       regexp_replace(to_date(max(a.create_time)), '-','')create_date
FROM csx_dw.dws_basic_w_a_csx_product_info a
JOIN
  (SELECT dist_code,
          dist_name,
          location_code
   FROM csx_dw.csx_shop
   WHERE sdt='current'
     AND location_type_code='1'
     AND dist_code NOT IN ('34','35')) b ON a.shop_code=b.location_code
AND a.shop_code IN ('W0A2','W0A3', 'W0A5', 'W0A6', 'W0A7', 'W0A8',  'W0F4', 'W0K1', 'W0R9', 'W0K6', 'W0L3','W0N0', 'W0N1',  'W0P8', 'W0Q2', 'W0Q9')
and a.sdt='20201108'
and a.des_specific_product_status in ('0','2')
GROUP BY dist_code,
         dist_name,
         a.product_code,
         a.root_category_code,
         a.root_category_name;

-- 省区BOM
drop table csx_tmp.temp_goods_02;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_02 AS 
select b.dist_code,a.province_code,goods_code,goods_name,'1' as is_fc from csx_dw.dws_mms_w_a_factory_bom_m  a 
join
(select distinct province_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current') b on a.province_code=b.province_code
where sdt='20201108'
group by b.dist_code,a.province_code,goods_code,goods_name;

-- 关联商品是否加工
drop table   if exists  csx_tmp.temp_goods_03 ;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_03 AS
SELECT a.dist_code,
       a.dist_name,
       a.product_code,
       case when root_category_code in ('10','11') then '11' when  root_category_code in ('12','13','14') then '12' else root_category_code end bd_id,
       case when root_category_code in ('10','11') then '生鲜' when  root_category_code in ('12','13','14') then '食百' else root_category_name end bd_name,
       create_date,
       if(is_fc='1','1','0') as is_fc_no
FROM csx_tmp.temp_goods_01 a 
left join 
csx_tmp.temp_goods_02 b on a.dist_code=b.dist_code and a.product_code=b.goods_code
;

-- 商品级别与商品池
drop table if exists csx_tmp.temp_goods_04 ;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_04 AS
SELECT a.dist_code,
       a.dist_name,
       a.product_code,
        bd_id,
        bd_name,
       create_date,
        is_fc_no,
         product_level,
          product_level_name
FROM csx_tmp.temp_goods_03 a 
 left join 
 (SELECT  goods_id,
          product_level,
          product_level_name
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='20201108') d ON a.product_code=d.goods_id
;

-- 商品池关联省区
drop table if exists csx_tmp.temp_goods_05 ;

CREATE TEMPORARY TABLE csx_tmp.temp_goods_05 AS
select 
dist_code,product_code,goods_spc from 
(
SELECT location_code,
        product_code,
        'spc' AS goods_spc
   FROM csx_ods.source_scm_w_a_scm_product_pool a 
   WHERE sdt='20201108'
   and a.location_code in ('W0A2','W0A3', 'W0A5', 'W0A6', 'W0A7', 'W0A8',  'W0F4', 'W0K1','W0R9', 'W0K6', 'W0L3','W0N0', 'W0N1', 'W0P8', 'W0Q2', 'W0Q9')
)a 
join 
(select location_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current')b on a.location_code=b.location_code
group by dist_code,product_code,goods_spc
;

-- 级联数据
drop table if exists csx_tmp.temp_goods_06 ;
CREATE TABLE csx_tmp.temp_goods_06 AS
SELECT a.dist_code,
    a.dist_name,
    a.product_code,
    bd_id,
    bd_name,
    create_date,
    is_fc_no,
    product_level,
    product_level_name,
    goods_spc
FROM csx_tmp.temp_goods_04 a
    left join csx_tmp.temp_goods_05 b on a.dist_code = b.dist_code
    and a.product_code = b.product_code;
    
-- 采购部 销售SKU

select province_code,bd,is_factory_goods_code,count(goods_code) from (
select  province_code,goods_code,is_factory_goods_code,
case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end bd
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20201101' and sdt<'20201109'
and division_code in ('12','13','11','10')
and province_code not in ('34','35')
group by province_code,
    goods_code,is_factory_goods_code,
    case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end 
) a
group by province_code,is_factory_goods_code,bd

;

--采购部汇总 销售SKU

select bd,is_factory_goods_code,count(distinct goods_code) from (
select  province_code,goods_code,is_factory_goods_code,
case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end bd
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20201101' and sdt<'20201109'
and division_code in ('12','13','11','10')
group by province_code,
    goods_code,is_factory_goods_code,
    case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end 
) a
group by is_factory_goods_code,bd

;

-- 全国采购部 销售SKU

select province_code,count(distinct goods_code) from (
select  province_code,province_name,goods_code,is_factory_goods_code,
case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end bd
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20201101' and sdt<'20201109'
and division_code in ('12','13','11','10')
and province_code not in ('34','35')
group by province_code,province_name,
    goods_code,is_factory_goods_code,
    case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end 
) a
group by province_code
;

-- 全国采购部 销售SKU

select count(distinct goods_code) from (
select  province_code,province_name,goods_code,is_factory_goods_code,
case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end bd
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20201101' and sdt<'20201109'
and division_code in ('12','13','11','10')
and province_code not in ('34','35')
group by province_code,province_name,
    goods_code,is_factory_goods_code,
    case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end 
) a

;

-- 生鲜加工与非加工
select dist_code,dist_name,is_fc_no,count(distinct product_code) as all_sku,
count(distinct case when create_date>='20201101' and create_date<'20201109' then product_code end ) as new_sku,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc
from  csx_tmp.temp_goods_06 a
where a.bd_id='11'
group by dist_code,dist_name,is_fc_no

;

-- 生鲜加工与非加工汇总
select is_fc_no,count(distinct product_code) as all_sku,
count(distinct case when create_date>='20201101' and create_date<'20201109' then product_code end ) as new_sku,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc
from  csx_tmp.temp_goods_06 a
where a.bd_id='11'
group by is_fc_no

;
--食百
select dist_code,dist_name,count(distinct product_code) as all_sku,
count(distinct case when create_date>='20201101' and create_date<'20201109' then product_code end ) as new_sku,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc
from  csx_tmp.temp_goods_06 a
where a.bd_id='12'
group by dist_code,dist_name


;

-- 食百 汇总
select count(distinct product_code) as all_sku,
count(distinct case when create_date>='20201101' and create_date<'20201109' then product_code end ) as new_sku,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc
from  csx_tmp.temp_goods_06 a
where a.bd_id='12'

;

--合计
select dist_code,dist_name,count(distinct product_code) as all_sku,
count(distinct case when create_date>='20201101' and create_date<'20201109' then product_code end ) as new_sku,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc,
count(distinct case when product_level  in  ('1','2','4','5') then  a.product_code end)as goods_level
from  csx_tmp.temp_goods_06 a
-- where a.bd_id='12'
group by dist_code,dist_name

;

--合计全国
select count(distinct product_code) as all_sku,
count(distinct case when create_date>='20201101' and create_date<'20201109' then product_code end ) as new_sku,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc,
count(distinct case when product_level  in  ('1','2','4','5') then  a.product_code end)as goods_level
from  csx_tmp.temp_goods_06 a
-- where a.bd_id='12'


;

--每月新建SKU
select dist_code,dist_name,a.is_fc_no,substr(a.create_date,1,6)mon,a.bd_id,count(distinct product_code) as all_sku,
count(distinct product_code  ) as new_sku
from  csx_tmp.temp_goods_06 a
 where a.create_date>='20200701' and a.create_date<'20200901'
group by dist_code,dist_name,substr(a.create_date,1,6),a.bd_id,a.is_fc_no

;
--每月新建SKU
select dist_code,dist_name,substr(a.create_date,1,6)mon,count(distinct product_code) as all_sku,
count(distinct product_code  ) as new_sku
from  csx_tmp.temp_goods_06 a
 where a.create_date>='20200701' and a.create_date<'20200901'
group by dist_code,dist_name,substr(a.create_date,1,6)

;

---------------------------------- 一品多码
---------------------------------- 一品多码

---------------------------------- 一品多码
set hive.execution.engine=spark;
set spark.master=yarn-cluster;
set mapreduce.job.queuename=ada.spark;

drop table csx_tmp.temp_goods_more_01 ;
create temporary table csx_tmp.temp_goods_more_01 as 

select
    a.shop_code ,
    b.product_code ,
    b.product_bar_code,
    region_goods_name,
    root_category_code 
from(    
select
    shop_code ,
    case when  (regionalized_trade_names ='' or  regionalized_trade_names is null ) then product_name else regionalized_trade_names end  region_goods_name,
    COUNT(DISTINCT product_bar_code ) as aa 
from
    csx_dw.dws_basic_w_a_csx_product_info a
where
    sdt='current'
  and  des_specific_product_status in ('0','2')
  and  root_category_code in ('10','11','12','13')
  and a.product_name!=''
  and shop_code in ('W0A2','W0A3', 'W0A5', 'W0A6', 'W0A7', 'W0A8',  'W0F4', 'W0K1', 'W0R9', 'W0K6', 'W0L3','W0N0', 'W0N1',  'W0P5', 'W0P8', 'W0Q2', 'W0Q9')
group by shop_code ,
   -- product_code,
    case when  (regionalized_trade_names ='' or  regionalized_trade_names is null ) then product_name else regionalized_trade_names end 
)a
left join 
    (select shop_code ,
    	product_code ,
    case when (regionalized_trade_names ='' or  regionalized_trade_names is null ) then product_name else regionalized_trade_names  end regionalized_trade_names ,
    product_bar_code,
    root_category_code
    from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on trim(a.region_goods_name)=trim(b.regionalized_trade_names) and a.shop_code=b.shop_code
where aa >1
;
-- select * from  csx_tmp.temp_goods_more_01 where root_category_code in ('10','11','12','13') and shop_code in ('W0A2','W0A3', 'W0A5', 'W0A6', 'W0A7', 'W0A8',  'W0F4', 'W0K1');
-- select * from  csx_tmp.temp_goods_more_01 where root_category_code in ('10','11','12','13') and shop_code in ('W0L3','W0K5');
-- select count(1) from  csx_tmp.temp_goods_more_01  ;
-- 插入
INSERT OVERWRITE DIRECTORY '/tmp/pengchenghua/data/aaa' row FORMAT DELIMITED fields TERMINATED BY '\t'
select a.shop_code ,
        shop_name,
        a.product_code ,
        a.product_bar_code,
        region_goods_name,
        a.region_goods_name ,
        b.big_category_code,
        big_category_name,
        purchase_group_code,
        purchase_group_name,
        a.root_category_code,
        root_category_name,
        purchase_org_code,
        purchase_org_name
from  csx_tmp.temp_goods_more_01 a
LEFT join 
( SELECT shop_code,
        shop_name,
        product_code,
        product_name,
        big_category_code,
        big_category_name,
        purchase_group_code,
        purchase_group_name,
        root_category_code,
        root_category_name,
        purchase_org_code,
        purchase_org_name
    FROM csx_dw.dws_basic_w_a_csx_product_info
    WHERE sdt='current'
 ) b on  a.product_code=b.product_code   and a.shop_code=b.shop_code 
WHERE a.root_category_code in ('10','11','12','13')
 and a.shop_code in ('W0A2','W0A3', 'W0A5', 'W0A6', 'W0A7', 'W0A8',  'W0F4', 'W0K1', 'W0R9', 'W0K6', 'W0L3','W0N0', 'W0N1',  'W0P5', 'W0P8', 'W0Q2', 'W0Q9')

;
-- 省区
select dist_code,dist_name,
    if(is_fc_no='1','1','0') as is_fc,bd,
    count(distinct a.region_goods_name) as aa 
from  
(select dist_code,
    dist_name,
    province_code,
    a.region_goods_name ,
    case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13') then '12' else root_category_code end   as bd
from   csx_tmp.temp_goods_more_01 a 
left join 
(select location_code,dist_code,dist_name,province_code from csx_dw.csx_shop where sdt='current')d  on a.shop_code=d.location_code
group by  dist_code,
    dist_name,
    province_code,
    a.region_goods_name ,
     case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13') then '12' else root_category_code end 
)a
left  join  
(select goods_name,province_code ,'1' as is_fc_no
    from csx_dw.dws_mms_w_a_factory_bom_m where sdt='current' ) b
 on a.province_code=b.province_code and a.region_goods_name=b.goods_name
 group by  dist_code,dist_name,if(is_fc_no='1','1','0') ,bd
 ;
 
 -- 加工与非加工  全国
select
    if(is_fc_no='1','1','0') as is_fc,bd,
    count(distinct a.region_goods_name) as aa 
from  
(select dist_code,
    dist_name,
    province_code,
    a.region_goods_name ,
    case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13','14') then '12' else root_category_code end   as bd
from   csx_tmp.temp_goods_more_01 a 
left join 
(select location_code,dist_code,dist_name,province_code from csx_dw.csx_shop where sdt='current')d  on a.shop_code=d.location_code
group by  dist_code,
    dist_name,
    province_code,
    a.region_goods_name ,
     case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13','14') then '12' else root_category_code end 
)a
left  join  
(select goods_name,province_code ,'1' as is_fc_no
    from csx_dw.dws_mms_w_a_factory_bom_m where sdt='current' ) b
 on a.province_code=b.province_code and a.region_goods_name=b.goods_name
 group by if(is_fc_no='1','1','0') ,bd;
 
 
  -- 加工与非加工  全国
select
   dist_code,dist_name,
    count(distinct a.region_goods_name) as aa 
from  
(select dist_code,
    dist_name,
    province_code,
    a.region_goods_name ,
    case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13','14') then '12' else root_category_code end   as bd
from   csx_tmp.temp_goods_more_01 a 
left join 
(select location_code,dist_code,dist_name,province_code from csx_dw.csx_shop where sdt='current')d  on a.shop_code=d.location_code
group by  dist_code,
    dist_name,
    province_code,
    a.region_goods_name ,
     case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13','14') then '12' else root_category_code end 
)a
left  join  
(select goods_name,province_code ,'1' as is_fc_no
    from csx_dw.dws_mms_w_a_factory_bom_m where sdt='current' ) b
 on a.province_code=b.province_code and a.region_goods_name=b.goods_name
 group by dist_code,dist_name
;
 
  
  -- 加工与非加工  全国
select

    count(distinct a.region_goods_name) as aa 
from  
(select dist_code,
    dist_name,
    province_code,
    a.region_goods_name ,
    case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13','14') then '12' else root_category_code end   as bd
from   csx_tmp.temp_goods_more_01 a 
left join 
(select location_code,dist_code,dist_name,province_code from csx_dw.csx_shop where sdt='current')d  on a.shop_code=d.location_code
group by  dist_code,
    dist_name,
    province_code,
    a.region_goods_name ,
     case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13','14') then '12' else root_category_code end 
)a
left  join  
(select goods_name,province_code ,'1' as is_fc_no
    from csx_dw.dws_mms_w_a_factory_bom_m where sdt='current' ) b
 on a.province_code=b.province_code and a.region_goods_name=b.goods_name

;