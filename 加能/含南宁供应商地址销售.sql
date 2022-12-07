select
  substr(sdt,1,4) year,
  performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name  ,
  a.goods_code,
  goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  sum(sale_amt)/10000 sale_amt,
  sum(sale_qty) sale_qty,
  sum(profit)/10000 profit,
  b.supplier_code,
  b.supplier_name,
  street_name
from
  csx_dws.csx_dws_sale_detail_di a
   join (
    select
      goods_code,
      a.supplier_code,
      dc_code,
      a.supplier_name,
      street_name
    from
      csx_dim.csx_dim_basic_dc_goods a
      join (
        select
          supplier_code,
          supplier_name,
          street_name
        from
          csx_dim.csx_dim_basic_supplier
        where
          sdt = 'current'
          and (
            street_name like '%南宁%'
            or city_name like '%南宁%'
          )
      ) b on a.supplier_code = b.supplier_code
    where
      sdt = 'current'
  ) b on a.goods_code = b.goods_code
  and a.inventory_dc_code = b.dc_code
where
  sdt >= '20210101'
  and sdt <= '20221022'
 group by  
  performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name  ,
  a.goods_code,
  goods_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  b.supplier_code,
  b.supplier_name,
   substr(sdt,1,4),
   street_name