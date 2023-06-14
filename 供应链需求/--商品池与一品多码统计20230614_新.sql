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