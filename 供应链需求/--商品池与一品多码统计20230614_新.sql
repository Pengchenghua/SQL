--商品池与一品多码统计20230614_新

-- select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and shop_id in ${hiveconf:shop};
---------------------------------- 一品多码
-- set hive.execution.engine=spark;
-- set spark.master=yarn-cluster;
-- set mapreduce.job.queuename=ada.spark;
drop table csx_analyse_tmp.csx_analyse_tmp_goods_more_code_01;
create table csx_analyse_tmp.csx_analyse_tmp_goods_more_code_01 as
select
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  a.dc_code,
  shop_name,
  b.goods_code,
  b.goods_bar_code,
  region_goods_name,
  division_code
from
(
    select
      dc_code,
      case
        when (
          regionalized_goods_name = ''
          or regionalized_goods_name is null
        ) then goods_name
        else regionalized_goods_name
      end region_goods_name,
      COUNT(DISTINCT goods_bar_code) as aa
    from
      csx_dim.csx_dim_basic_dc_goods a
    where
      sdt = 'current'
      and shop_special_goods_status in ('0', '2')
      and division_code in ('10', '11', '12', '13')
      and a.goods_name != ''
    group by
      dc_code,
      -- product_code,
      case
        when (
          regionalized_goods_name = ''
          or regionalized_goods_name is null
        ) then goods_name
        else regionalized_goods_name
      end
  ) a
  left join (
    select
      dc_code,
      goods_code,
      case
        when (
          regionalized_goods_name = ''
          or regionalized_goods_name is null
        ) then goods_name
        else regionalized_goods_name
      end regionalized_goods_name,
      goods_bar_code,
      division_code
    from
      csx_dim.csx_dim_basic_dc_goods
    where
      sdt = 'current'
  ) b on trim(a.region_goods_name) = trim(b.regionalized_goods_name)
  and a.dc_code = b.dc_code
  join (
    select
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      shop_code,
      shop_name,
      is_dc_tag
    from
      csx_dim.csx_dim_shop a
      join (
        select
          dc_code,
          regexp_replace(to_date(enable_time), '-', '') enable_date,
          '1' is_dc_tag
        from
          csx_dim.csx_dim_csx_data_market_conf_supplychain_location
        where
          sdt = 'current'
      ) c on a.shop_code = c.dc_code
    where
      sdt = 'current'
  ) c on a.dc_code = c.shop_code
where
  aa > 1;
select
  *
from
  csx_analyse_tmp.csx_analyse_tmp_goods_more_code_01;
-- select * from  csx_tmp.temp_goods_more_01 where division_code in ('10','11','12','13') and shop_code in ('W0A2','W0A3', 'W0A5', 'W0A6', 'W0A7', 'W0A8',  'W0F4', 'W0K1');
-- select * from  csx_tmp.temp_goods_more_01 where division_code in ('10','11','12','13') and shop_code in ('W0L3','W0K5');
-- select count(1) from  csx_tmp.temp_goods_more_01  ;
-- 插入
-- INSERT OVERWRITE DIRECTORY '/tmp/pengchenghua/data/aaa' row FORMAT DELIMITED fields TERMINATED BY '\t'
select
  basic_performance_province_code,
  basic_performance_province_name,
  basic_performance_city_code,
  basic_performance_city_name,
  a.dc_code,
  shop_name,
  a.goods_code,
  a.goods_bar_code,
  goods_name,
  a.region_goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  b.division_code,
  b.division_name
from
  csx_analyse_tmp.csx_analyse_tmp_goods_more_code_01 a
  join (
    SELECT
      goods_code,
      goods_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      division_code,
      division_name
    FROM
      csx_dim.csx_dim_basic_goods
    WHERE
      sdt = 'current'
  ) b on a.goods_code = b.goods_code
WHERE
  1 = 1;
select
  coalesce(basic_performance_province_code, '00') as basic_performance_province_code,
  coalesce(basic_performance_province_name, '全国') as basic_performance_province_name,
  coalesce(basic_performance_city_code, '00') as basic_performance_city_code,
  coalesce(basic_performance_city_name, '') basic_performance_city_name,
  coalesce(is_fc, '00') as is_factory_goods_code,
  coalesce(bd, '00') as bd,
  aa
from
  (
    select
      a.basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      is_fc,
      bd,
      count(distinct a.region_goods_name) as aa
    from
      (
        select
          a.basic_performance_province_code,
          a.basic_performance_province_name,
          a.basic_performance_city_code,
          a.basic_performance_city_name,
          if(is_fc = '1', '1', '0') as is_fc,
          bd,
          a.region_goods_name
        from
          (
            select
              basic_performance_province_code,
              basic_performance_province_name,
              basic_performance_city_code,
              basic_performance_city_name,
              a.region_goods_name,
              case
                when a.division_code in ('10', '11') then '11'
                when a.division_code in ('12', '13') then '12'
                else division_code
              end as bd
            from
              csx_analyse_tmp.csx_analyse_tmp_goods_more_code_01 a
            group by
              basic_performance_province_code,
              basic_performance_province_name,
              a.region_goods_name,
              basic_performance_city_name,
              basic_performance_city_code,
              case
                when a.division_code in ('10', '11') then '11'
                when a.division_code in ('12', '13') then '12'
                else division_code
              end
          ) a
          left join csx_analyse_tmp.csx_analyse_tmp_goods_more_02 b on a.basic_performance_province_code = b.basic_performance_province_code
          and a.region_goods_name = b.goods_name
      ) a
    group by
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      is_fc,
      bd grouping sets (
        (
          basic_performance_province_code,
          basic_performance_province_name,
          basic_performance_city_code,
          basic_performance_city_name,
          is_fc,
          bd
        ),
        (
          basic_performance_province_code,
          basic_performance_province_name,
          basic_performance_city_code,
          basic_performance_city_name
        ),
        (is_fc, bd),
        ()
      )
  ) a;






  
-- 各省区商品
-- select * from csx_analyse_tmp.csx_analyse_tmp_goods_more_01;
drop table  csx_analyse_tmp.csx_analyse_tmp_goods_more_01 ;
create   table csx_analyse_tmp.csx_analyse_tmp_goods_more_01 as 
SELECT b.basic_performance_province_code,
       b.basic_performance_province_name,
       basic_performance_city_code,
       basic_performance_city_name,
       a.goods_code,
       a.division_code,
       a.division_name,
       j.create_date,
       a.shop_special_goods_status,
       a.goods_status_name
FROM csx_dim.csx_dim_basic_dc_goods a
JOIN
  ( select 
   basic_performance_province_code,
   basic_performance_province_name,
   basic_performance_city_code,
   basic_performance_city_name,
    shop_code ,
    shop_name  ,
    is_dc_tag
from  csx_dim.csx_dim_shop a 
join  (select dc_code,
    regexp_replace(to_date(enable_time),'-','') enable_date ,
    '1' is_dc_tag
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current'
 ) c on a.shop_code=c.dc_code
 where sdt='current'    ) b ON a.dc_code=b.shop_code
left join 
(SELECT a.goods_code,
       dc_code,
       regexp_replace(to_date(create_time),'-','')create_date
FROM csx_dim.csx_dim_basic_dc_goods a 
join 
(select goods_code,create_date from csx_dim.csx_dim_basic_goods where sdt='current') b on a.goods_code=b.goods_code and  regexp_replace(to_date(create_time),'-','')=create_date
WHERE sdt= 'current'
) j on a.dc_code=j.dc_code and a.goods_code=j.goods_code
where a.sdt= 'current'
and a.shop_special_goods_status in ('0','2')
;



-- 省区BOM 总SKU数、商品池统计
drop table  csx_analyse_tmp.csx_analyse_tmp_goods_more_02 ;
create   table csx_analyse_tmp.csx_analyse_tmp_goods_more_02 as 
select 
   basic_performance_province_code,
   basic_performance_province_name,
   basic_performance_city_code,
   basic_performance_city_name,
    goods_code,
    goods_name,
    '1' as is_fc 
from    csx_dws.csx_dws_mms_factory_bom_m_df  a 
join
( select 
   basic_performance_province_code,
   basic_performance_province_name,
   basic_performance_city_code,
   basic_performance_city_name,
    shop_code ,
    shop_name  ,
    is_dc_tag
from  csx_dim.csx_dim_shop a 
join  (select dc_code,
    regexp_replace(to_date(enable_time),'-','') enable_date ,
    '1' is_dc_tag
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current'
 ) c on a.shop_code=c.dc_code
 where sdt='current'
    ) b on a.factory_location_code=b.shop_code
    where sdt='20230613'
group by basic_performance_province_code,
   basic_performance_province_name,
   basic_performance_city_code,
   basic_performance_city_name,
    goods_code,
    goods_name;


-- 关联商品是否加工
drop table   if exists csx_analyse_tmp.csx_analyse_tmp_goods_more_03 ;
CREATE   TABLE csx_analyse_tmp.csx_analyse_tmp_goods_more_03 AS
SELECT a.basic_performance_province_code,
       a.basic_performance_province_name,
       a.basic_performance_city_code,
       a.basic_performance_city_name,
       a.goods_code,
       case when division_code in ('10','11') then '11' when  division_code in ('12','13','14') then '12' else division_code end bd_id,
       case when division_code in ('10','11') then '生鲜' when  division_code in ('12','13','14') then '食百' else division_name end bd_name,
       create_date,
       a.shop_special_goods_status,
       a.goods_status_name,
       if(is_fc='1','1','0') as is_fc_no
FROM csx_analyse_tmp.csx_analyse_tmp_goods_more_01  a 
left join 
csx_analyse_tmp.csx_analyse_tmp_goods_more_02 b on a.basic_performance_province_code=b.basic_performance_province_code and a.goods_code=b.goods_code
;

-- 商品级别与商品池
drop table if exists csx_analyse_tmp.csx_analyse_tmp_goods_more_04 ;
CREATE  TABLE csx_analyse_tmp.csx_analyse_tmp_goods_more_04 AS
SELECT a.basic_performance_province_code,
       a.basic_performance_province_name,
       basic_performance_city_code,
       basic_performance_city_name,
       a.goods_code,
       bd_id,
       bd_name,
       create_date,
       is_fc_no,
       csx_purchase_level_code,
       csx_purchase_level_name	,
       a.shop_special_goods_status,
       a.goods_status_name
FROM csx_analyse_tmp.csx_analyse_tmp_goods_more_03 a 
 left join 
 ( SELECT 
        goods_code,
        goods_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        division_code,
        division_name,
        csx_purchase_level_code,
        csx_purchase_level_name	
    FROM    csx_dim.csx_dim_basic_goods
    WHERE sdt='current'
 ) d ON a.goods_code=d.goods_code
;

-- 商品池关联省区
drop table if exists csx_analyse_tmp.csx_analyse_tmp_goods_more_05 ;

CREATE   TABLE csx_analyse_tmp.csx_analyse_tmp_goods_more_05 AS
select 
basic_performance_province_code,basic_performance_city_code,basic_performance_city_name,goods_code,goods_spc 
from 
(
SELECT location_code,
        goods_code,
        'spc' AS goods_spc
   FROM csx_dws.csx_dws_scm_product_pool_df a 
   WHERE sdt='20230613'
  

)a 
join 
( select 
   basic_performance_province_code,
   basic_performance_province_name,
   basic_performance_city_code,
   basic_performance_city_name,
    shop_code ,
    shop_name  ,
    is_dc_tag
from  csx_dim.csx_dim_shop a 
join  (select dc_code,
    regexp_replace(to_date(enable_time),'-','') enable_date ,
    '1' is_dc_tag
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current'
 ) c on a.shop_code=c.dc_code
 where sdt='current'    )b on a.location_code=b.shop_code
group by basic_performance_province_code,basic_performance_city_code,basic_performance_city_name,goods_code,goods_spc 
;


-- 级联数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_goods_more_06 ;
CREATE TABLE csx_analyse_tmp.csx_analyse_tmp_goods_more_06 AS
SELECT  a.basic_performance_province_code,
    a.basic_performance_province_name,
    a.basic_performance_city_code,
    a.basic_performance_city_name,
    a.goods_code,
    bd_id,
    bd_name,
    create_date,
    is_fc_no,
    csx_purchase_level_code,
    csx_purchase_level_name,
    goods_spc,
    a.shop_special_goods_status,
    a.goods_status_name
FROM csx_analyse_tmp.csx_analyse_tmp_goods_more_04 a
    left join csx_analyse_tmp.csx_analyse_tmp_goods_more_05 b on a.basic_performance_province_code = b.basic_performance_province_code
    and a.goods_code = b.goods_code;
    
    
    
       
-- 销售SKU
--create temporary table csx_tmp.temp_sale_sku as 
select coalesce(basic_performance_province_code,'00') as basic_performance_province_code,
dc_city_code,dc_city_name,
       coalesce(is_factory_goods,'00') as is_factory_goods,
       coalesce(bd,'00') as bd,
       sale_sku
from (
select basic_performance_province_code,is_factory_goods,dc_city_code,dc_city_name,bd,count(distinct goods_code) as sale_sku
from (
select  dc_basic_performance_province_code as  basic_performance_province_code,dc_city_code,dc_city_name, goods_code,is_factory_goods,
case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end bd
from csx_dw.dws_sale_r_d_detail
where sdt>=${hiveconf:sdate}and sdt<=${hiveconf:edate}
    and division_code in ('12','13','11','10')
    and dc_code in  ${hiveconf:shop}

group by dc_basic_performance_province_code,dc_city_code,dc_city_name,
    goods_code,is_factory_goods,
    case when division_code in ('10','11') then '11' when  division_code in ('12','13') then '12' else division_code end 
) a 
group by basic_performance_province_code,is_factory_goods,bd,dc_city_code,dc_city_name
grouping sets ((basic_performance_province_code,is_factory_goods,bd,dc_city_code,dc_city_name),
(basic_performance_province_code,dc_city_code,dc_city_name,is_factory_goods),
(is_factory_goods,bd),
(basic_performance_province_code,dc_city_code,dc_city_name),
()) 
) a ;
 
select coalesce(basic_performance_province_code,'00') as basic_performance_province_code,
       coalesce(basic_performance_province_name,'全国') as basic_performance_province_name,
       basic_performance_city_code,
       basic_performance_city_name,
       coalesce(is_fc_no,'00') as is_factory_goods_code,
       coalesce(bd_id,'00') as bd,
       all_sku,
       new_sku,
       goods_level,
       goods_spc,
       error_status_sku
from (   
select a.basic_performance_province_code,
       a.basic_performance_province_name,
       a.basic_performance_city_code,
       a.basic_performance_city_name,
       is_fc_no,
       bd_id,
       count(distinct goods_code) as all_sku,
       count(distinct case when create_date>='20230601' and create_date<='20230613' then goods_code end ) as new_sku,
       count(distinct case when a.goods_spc='spc' then a.goods_code end)as goods_spc,
       count(distinct case when csx_purchase_level_code  in  ('1','2','4','5') then  a.goods_code end)as goods_level,
       count(distinct case when shop_special_goods_status not in ('0','7') then goods_code end  ) error_status_sku
from  csx_analyse_tmp.csx_analyse_tmp_goods_more_06 a
-- where a.bd_id='11'
where bd_id in ('12','11')
group by basic_performance_province_code,basic_performance_province_name,is_fc_no,bd_id,basic_performance_city_code,
basic_performance_city_name
grouping sets (( basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name,is_fc_no,bd_id),
                ( basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name,is_fc_no),
                ( is_fc_no,bd_id),
                ( basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name),
                ()
                )
)a 

;


select distinct shop_special_goods_status,shop_special_goods_status from csx_dw.dws_basic_w_a_csx_product_info where sdt='current'
;

--  3	H 停售
-- 	6	L 退场
-- 	7	K 永久停购
-- 	0	B 正常商品