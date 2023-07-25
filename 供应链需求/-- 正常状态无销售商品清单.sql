-- 正常状态无销售商品清单
with aa as (  select
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      shop_code,
      shop_name,
      is_dc_tag,
      shop_tags_name,
      a.goods_code,
      goods_bar_code,
      region_goods_name,
      c.goods_name,
      unit_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      division_code,
      division_name
     from (
    select
      dc_code,
      goods_code,
      goods_name,
      case
        when (
          regionalized_goods_name = ''
          or regionalized_goods_name is null
        ) then goods_name
        else regionalized_goods_name
      end region_goods_name,
       goods_bar_code
    from
      csx_dim.csx_dim_basic_dc_goods a
    where
      sdt = 'current'
      and shop_special_goods_status in ('0', '2')
      and division_code in ('10', '11', '12', '13')
      and a.goods_name != ''
    
  ) a
  join (
    select
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      shop_code,
      shop_name,
      is_dc_tag,
      shop_tags_name
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
  )b on a.dc_code=b.shop_code
  join 
  (
    SELECT
      goods_code,
      goods_name,
      unit_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      division_code,
      division_name
    FROM
      csx_dim.csx_dim_basic_goods
    WHERE
      sdt = 'current') c on a.goods_code=c.goods_code
  ),
  bb as (select inventory_dc_code,goods_code,sum(sale_amt) sale_amt from csx_dws.csx_dws_sale_detail_di where sdt>='20220601' group by inventory_dc_code,goods_code),
  cc as (select goods_code,dc_code,sum(amt) stock_amt,sum(qty) stock_qty from csx_dws.csx_dws_cas_accounting_stock_m_df where sdt='20230615' and is_bz_reservoir=1 
  group by goods_code,dc_code)
  
  
 select  
      basic_performance_province_code,
      basic_performance_province_name,
      basic_performance_city_code,
      basic_performance_city_name,
      shop_code,
      shop_name,
      is_dc_tag,
      shop_tags_name,
      aa.goods_code,
      goods_bar_code,
      region_goods_name,
      goods_name,
      unit_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      division_code,
      division_name,
      stock_amt,
      stock_qty
      from aa 
      left join  bb on aa.shop_code=bb.inventory_dc_code and aa.goods_code =bb.goods_code
      left join  cc on  aa.shop_code=cc.dc_code and aa.goods_code=cc.goods_code
      where bb.goods_code is null 
      