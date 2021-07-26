

set shop = ('W0A3','W0Q9','W0N1','W0R9','W0A5','W0N0','W0W7','W0A2','W0F4','W0A8','W0K1','W0K6','W0L3','W0A7','W0A6','W0Q2','W0P8','W0F7');

-- select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and shop_id in ${hiveconf:shop};
---------------------------------- 一品多码
set hive.execution.engine=spark;
set spark.master=yarn-cluster;
set mapreduce.job.queuename=ada.spark;

drop table csx_tmp.temp_goods_more_01 ;
create temporary table csx_tmp.temp_goods_more_01 as 
select
    province_code,
    province_name,
    case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city end prefecture_city,
    case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city_name end prefecture_city_name,
    a.shop_code ,
    shop_name,
    b.product_code ,
    b.product_bar_code,
    region_goods_name,
    root_category_code 
from(    
select
    shop_code ,
    case when (regionalized_trade_names ='' or  regionalized_trade_names is null ) then product_name else regionalized_trade_names end  region_goods_name,
    COUNT(DISTINCT product_bar_code ) as aa 
from
    csx_dw.dws_basic_w_a_csx_product_info a
where
    sdt='current'
  and  des_specific_product_status in ('0','2')
  and  root_category_code in ('10','11','12','13')
  and a.product_name!=''
  and shop_code in  ${hiveconf:shop}
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
left join 
 (select location_code,
     shop_name,
     province_code,
     province_name,
     prefecture_city,
     prefecture_city_name 
 from csx_dw.csx_shop where sdt='current') c on a.shop_code=c.location_code
where aa >1
;
-- select * from  csx_tmp.temp_goods_more_01 where root_category_code in ('10','11','12','13') and shop_code in ('W0A2','W0A3', 'W0A5', 'W0A6', 'W0A7', 'W0A8',  'W0F4', 'W0K1');
-- select * from  csx_tmp.temp_goods_more_01 where root_category_code in ('10','11','12','13') and shop_code in ('W0L3','W0K5');
-- select count(1) from  csx_tmp.temp_goods_more_01  ;
-- 插入
INSERT OVERWRITE DIRECTORY '/tmp/pengchenghua/data/aaa' row FORMAT DELIMITED fields TERMINATED BY '\t'
select  province_code,
        province_name,
        prefecture_city,
        prefecture_city_name ,
        a.shop_code ,
        shop_name,
        a.product_code ,
        a.product_bar_code,
        goods_name,
        a.region_goods_name ,
        category_large_code,
        category_large_name,
        department_id,
        department_name,
        division_code,
        division_name
from  csx_tmp.temp_goods_more_01 a
join 
( SELECT 
        goods_id,
        goods_name,
        category_large_code,
        category_large_name,
        department_id,
        department_name,
        division_code,
        division_name
    FROM csx_dw.dws_basic_w_a_csx_product_m
    WHERE sdt='current'
 ) b on  a.product_code=b.goods_id 
WHERE a.root_category_code in ('10','11','12','13')
 and a.shop_code in  ${hiveconf:shop}

;


 select coalesce(province_code,'00') as province_code,
       coalesce(province_name,'全国') as province_name,
       coalesce(prefecture_city,'00')as prefecture_city,
       coalesce(prefecture_city_name,'')prefecture_city_name ,
       coalesce(is_fc,'00') as is_factory_goods_code,
       coalesce(bd,'00') as bd,
        aa 
from (
select a.province_code,
    province_name,
    prefecture_city,
    prefecture_city_name,
    is_fc,
    bd,
    count(distinct a.region_goods_name) as aa 
from  
(
select a.province_code,
    province_name,
    prefecture_city,
    prefecture_city_name,
    if(is_fc_no='1','1','0') as  is_fc,
    bd,
    a.region_goods_name 
from 
(
select province_code,
    province_name,
    prefecture_city,
    prefecture_city_name,
    a.region_goods_name ,
    case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13') then '12' else root_category_code end   as bd
from   csx_tmp.temp_goods_more_01 a 
group by  province_code,
    province_name,
    a.region_goods_name ,
    prefecture_city_name,
    prefecture_city,
    case when a.root_category_code in ('10','11') then '11' when a.root_category_code in ('12','13') then '12' else root_category_code end 
)a
left  join  
(select goods_name,province_code ,'1' as is_fc_no
    from csx_dw.dws_mms_w_a_factory_bom_m where sdt='current' ) b
 on a.province_code=b.province_code and a.region_goods_name=b.goods_name
) a 
 group by  province_code,
           province_name,
          prefecture_city,
          prefecture_city_name ,
           is_fc ,
           bd
 grouping sets (
    (province_code,
           province_name,
          prefecture_city,
          prefecture_city_name ,
           is_fc ,
           bd),
    (province_code,
           province_name,
          prefecture_city,
          prefecture_city_name ),
    (is_fc ,bd),
    ()
    )
)a  ;





set edate='20210603';
set sdate='20210505';
--------------------------------- 分割线-------------------------------------------
-- 各省区商品
-- select * from csx_tmp.temp_goods_01;
drop table csx_tmp.temp_goods_01;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_01 AS
SELECT b.province_code,
       b.province_name,
       case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city end prefecture_city,
       case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city_name end prefecture_city_name,
       a.product_code,
       a.root_category_code,
       a.root_category_name,
       j.create_date
FROM csx_dw.dws_basic_w_a_csx_product_info a
JOIN
  (SELECT a.province_code,
          a.province_name,
          a.prefecture_city,
          a.prefecture_city_name,
          location_code
   FROM csx_dw.csx_shop a
   WHERE sdt='current'
     AND location_type_code='1'
     AND a.location_code IN  ${hiveconf:shop}
) b ON a.shop_code=b.location_code
left join 
(SELECT product_code,
       shop_code,
       regexp_replace(to_date(create_time),'-','')create_date
FROM csx_dw.dws_basic_w_a_csx_product_info a 
join 
(select goods_id,create_date from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.product_code=b.goods_id and  regexp_replace(to_date(create_time),'-','')=create_date
WHERE sdt= ${hiveconf:edate}
) j on a.shop_code=j.shop_code and a.product_code=j.product_code
where a.sdt= ${hiveconf:edate}
and a.des_specific_product_status in ('0','2')
;

-- 省区BOM
drop table csx_tmp.temp_goods_02;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_02 AS 
select b.dist_code,a.province_code,prefecture_city,prefecture_city_name,goods_code,goods_name,'1' as is_fc from csx_dw.dws_mms_w_a_factory_bom_m  a 
join
(select distinct province_code,dist_code,dist_name,case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city end prefecture_city,
       case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city_name end prefecture_city_name
 from csx_dw.csx_shop where sdt='current') b on a.province_code=b.province_code
where sdt=${hiveconf:edate}
group by b.dist_code,a.province_code,goods_code,goods_name,prefecture_city,prefecture_city_name;

-- 关联商品是否加工
drop table   if exists  csx_tmp.temp_goods_03 ;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_03 AS
SELECT a.province_code,
       a.province_name,
       a.prefecture_city,
       a.prefecture_city_name,
       a.product_code,
       case when root_category_code in ('10','11') then '11' when  root_category_code in ('12','13','14') then '12' else root_category_code end bd_id,
       case when root_category_code in ('10','11') then '生鲜' when  root_category_code in ('12','13','14') then '食百' else root_category_name end bd_name,
       create_date,
       if(is_fc='1','1','0') as is_fc_no
FROM csx_tmp.temp_goods_01 a 
left join 
csx_tmp.temp_goods_02 b on a.province_code=b.province_code and a.product_code=b.goods_code
;

-- 商品级别与商品池
drop table if exists csx_tmp.temp_goods_04 ;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_04 AS
SELECT a.province_code,
       a.province_name,
       prefecture_city,
       prefecture_city_name,
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
   WHERE sdt=${hiveconf:edate}) d ON a.product_code=d.goods_id
;

-- 商品池关联省区
drop table if exists csx_tmp.temp_goods_05 ;

CREATE TEMPORARY TABLE csx_tmp.temp_goods_05 AS
select 
province_code,prefecture_city,prefecture_city_name,product_code,goods_spc 
from 
(
SELECT location_code,
        product_code,
        'spc' AS goods_spc
   FROM csx_ods.source_scm_w_a_scm_product_pool a 
   WHERE sdt=${hiveconf:edate}
   and a.location_code in  ${hiveconf:shop}

)a 
join 
(select distinct province_code,
    dist_code,
    dist_name,
    location_code,
    case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city end prefecture_city,
    case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city_name end prefecture_city_name
 from csx_dw.csx_shop where sdt='current')b on a.location_code=b.location_code
group by province_code,prefecture_city,prefecture_city_name,product_code,goods_spc 
;


-- 级联数据
drop table if exists csx_tmp.temp_goods_06 ;
CREATE TABLE csx_tmp.temp_goods_06 AS
SELECT  a.province_code,
       a.province_name,
       a.prefecture_city,
       a.prefecture_city_name,
    a.product_code,
    bd_id,
    bd_name,
    create_date,
    is_fc_no,
    product_level,
    product_level_name,
    goods_spc
FROM csx_tmp.temp_goods_04 a
    left join csx_tmp.temp_goods_05 b on a.province_code = b.province_code
    and a.product_code = b.product_code;
    
    
    
       
-- 销售SKU
--create temporary table csx_tmp.temp_sale_sku as 
select coalesce(province_code,'00') as province_code,
dc_city_code,dc_city_name,
       coalesce(is_factory_goods,'00') as is_factory_goods,
       coalesce(bd,'00') as bd,
       sale_sku
from (
select province_code,is_factory_goods,dc_city_code,dc_city_name,bd,count(distinct goods_code) as sale_sku
from (
select  dc_province_code as  province_code,dc_city_code,dc_city_name, goods_code,is_factory_goods,
case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end bd
from csx_dw.dws_sale_r_d_detail
where sdt>=${hiveconf:sdate}and sdt<=${hiveconf:edate}
    and division_code in ('12','13','11','10')
    and dc_code in  ${hiveconf:shop}

group by dc_province_code,dc_city_code,dc_city_name,
    goods_code,is_factory_goods,
    case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end 
) a 
group by province_code,is_factory_goods,bd,dc_city_code,dc_city_name
grouping sets ((province_code,is_factory_goods,bd,dc_city_code,dc_city_name),
(province_code,dc_city_code,dc_city_name,is_factory_goods),
(is_factory_goods,bd),
(province_code,dc_city_code,dc_city_name),
()) 
) a ;
 
select coalesce(province_code,'00') as province_code,
       coalesce(province_name,'全国') as province_name,
       prefecture_city,
       prefecture_city_name,
       coalesce(is_fc_no,'00') as is_factory_goods_code,
       coalesce(bd_id,'00') as bd,
       all_sku,
       new_sku,
       goods_level,
       goods_spc
from (   
select a.province_code,
       a.province_name,
       a.prefecture_city,
       a.prefecture_city_name,
       is_fc_no,bd_id,
       count(distinct product_code) as all_sku,
       count(distinct case when create_date>=${hiveconf:sdate}and create_date<=${hiveconf:edate} then product_code end ) as new_sku,
       count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc,
       count(distinct case when product_level  in  ('1','2','4','5') then  a.product_code end)as goods_level
from  csx_tmp.temp_goods_06 a
-- where a.bd_id='11'
where bd_id in ('12','11')
group by province_code,province_name,is_fc_no,bd_id,prefecture_city,
prefecture_city_name
grouping sets (( province_code,province_name,prefecture_city,prefecture_city_name,is_fc_no,bd_id),
                ( province_code,province_name,prefecture_city,prefecture_city_name,is_fc_no),
                ( is_fc_no,bd_id),
                ( province_code,province_name,prefecture_city,prefecture_city_name),
                ()
                )
)a 

;




set edate='20210602';
set sdate='20210504';

set shop = ('W0A8','W0A7','W0A3','W0A2','W0A6','W0A5','W0R9','W0N0','W0W7','W0N1','W0AS','W0P8','W0Q2','W0Q9');


--------------------------------- 分割线-------------------------------------------
-- 各省区商品
-- select * from csx_tmp.temp_goods_01;
drop table csx_tmp.temp_goods_01;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_01 AS
SELECT dist_code,
       dist_name,
       a.product_code,
       a.root_category_code,
       a.root_category_name,
       j.create_date
FROM csx_dw.dws_basic_w_a_csx_product_info a
JOIN
  (SELECT dist_code,
          dist_name,
          location_code
   FROM csx_dw.csx_shop a
   WHERE sdt='current'
     AND location_type_code='1'
     AND a.location_code IN  ${hiveconf:shop}
      ) b ON a.shop_code=b.location_code
left join 
(SELECT product_code,
       shop_code,
       regexp_replace(to_date(create_time),'-','')create_date
FROM csx_dw.dws_basic_w_a_csx_product_info a 
join 
(select goods_id,create_date from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.product_code=b.goods_id and  regexp_replace(to_date(create_time),'-','')=create_date
WHERE sdt= ${hiveconf:edate}
) j on a.shop_code=j.shop_code and a.product_code=j.product_code
where a.sdt= ${hiveconf:edate}
and a.des_specific_product_status in ('0','2')
;

-- 省区BOM
drop table csx_tmp.temp_goods_02;
CREATE TEMPORARY TABLE csx_tmp.temp_goods_02 AS 
select b.dist_code,a.province_code,goods_code,goods_name,'1' as is_fc from csx_dw.dws_mms_w_a_factory_bom_m  a 
join
(select distinct province_code,dist_code,dist_name from csx_dw.csx_shop where sdt='current') b on a.province_code=b.province_code
where sdt=${hiveconf:edate}
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
   WHERE sdt=${hiveconf:edate}) d ON a.product_code=d.goods_id
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
   WHERE sdt=${hiveconf:edate}
   and a.location_code in  ${hiveconf:shop}
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
    
    
    
       
-- 销售SKU
--create temporary table csx_tmp.temp_sale_sku as 
select coalesce(province_code,'00') as province_code,
       coalesce(is_factory_goods_code,'00') as is_factory_goods_code,
       coalesce(bd,'00') as bd,
       sale_sku
from (
select province_code,is_factory_goods_code,bd,count(distinct goods_code) as sale_sku
from (
select  province_code,goods_code,is_factory_goods_code,
case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end bd
from csx_dw.dws_sale_r_d_customer_sale where sdt>=${hiveconf:sdate}and sdt<=${hiveconf:edate}
and division_code in ('12','13','11','10')
and dc_code in  ${hiveconf:shop}
group by province_code,
    goods_code,is_factory_goods_code,
    case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end 
) a 
group by province_code,is_factory_goods_code,bd
grouping sets ((province_code,is_factory_goods_code,bd),
(province_code,is_factory_goods_code),
(is_factory_goods_code,bd),
(province_code),
()) 
) a ;
 
select coalesce(dist_code,'00') as province_code,
       coalesce(dist_name,'全国') as province_name,
       coalesce(is_fc_no,'00') as is_factory_goods_code,
       coalesce(bd_id,'00') as bd,
       all_sku,
       new_sku,
       goods_level,
       goods_spc
from (   
select dist_code,dist_name,is_fc_no,bd_id,count(distinct product_code) as all_sku,
count(distinct case when create_date>=${hiveconf:sdate}and create_date<=${hiveconf:edate} then product_code end ) as new_sku,
count(distinct case when a.goods_spc='spc' then a.product_code end)as goods_spc,
count(distinct case when product_level  in  ('1','2','4','5') then  a.product_code end)as goods_level
from  csx_tmp.temp_goods_06 a
-- where a.bd_id='11'
where bd_id in ('12','11')
group by dist_code,dist_name,is_fc_no,bd_id
grouping sets (( dist_code,dist_name,is_fc_no,bd_id),
                ( dist_code,dist_name,is_fc_no),
                ( is_fc_no,bd_id),
                ( dist_code,dist_name),
                ()
                )
)a 

;





---- 一品多码



---------------------------------- 一品多码
set hive.execution.engine=spark;
set spark.master=yarn-cluster;
set mapreduce.job.queuename=ada.spark;

set shop = ('W0A8','W0A7','W0A3','W0A2','W0A6','W0A5','W0R9','W0N0','W0W7','W0N1','W0AS','W0P8','W0Q2','W0Q9');

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
  and shop_code in ${hiveconf:shop}
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
select  dist_code,
        dist_name,
        a.shop_code ,
        shop_name,
        a.product_code ,
        a.product_bar_code,
        goods_name,
        a.region_goods_name ,
        category_large_code,
        category_large_name,
        department_id,
        department_name,
        division_code,
        division_name
from  csx_tmp.temp_goods_more_01 a
join 
( SELECT 
        goods_id,
        goods_name,
        category_large_code,
        category_large_name,
        department_id,
        department_name,
        division_code,
        division_name
    FROM csx_dw.dws_basic_w_a_csx_product_m
    WHERE sdt='current'
 ) b on  a.product_code=b.goods_id 
 join 
 (select location_code,shop_name,dist_code,dist_name from csx_dw.csx_shop where sdt='current') c on a.shop_code=c.location_code
WHERE a.root_category_code in ('10','11','12','13')
 and a.shop_code in  ${hiveconf:shop}

;


 select coalesce(dist_code,'00') as province_code,
       coalesce(dist_name,'全国') as province_name,
       coalesce(is_fc,'00') as is_factory_goods_code,
       coalesce(bd,'00') as bd,
        aa 
from (
select dist_code,
    dist_name,
    is_fc,
    bd,
    count(distinct a.region_goods_name) as aa 
from  
(
select dist_code,
    dist_name,
    if(is_fc_no='1','1','0') as  is_fc,
    bd,
    a.region_goods_name 
from 
(
select dist_code,
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
) a 
 group by  dist_code,
           dist_name,
           is_fc ,
            bd
 grouping sets (
    (dist_code,dist_name,is_fc ,bd),
    (dist_code,dist_name,is_fc),
    (is_fc ,bd),
    (dist_code,dist_name),
    ()
    )
)a  ;







-- 城市组
