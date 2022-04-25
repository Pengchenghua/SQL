SET
  mapreduce.job.queuename = caishixian;
SET
  hive.exec.dynamic.partition = true;
--开启动态分区
SET
  hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
SET
  hive.exec.max.dynamic.partitions = 100000;
--在所有执行MR的节点上，最大一共可以创建多少个动态分区。
SET
  hive.exec.max.dynamic.partitions.pernode = 100000;
--源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
INSERT overwrite table csx_dw.supple_goods_sale_dtl partition(sdt)
SELECT
  substr(sales_date, 1, 4) years,
  lpad(
    ceil(
      month(
        from_unixtime(
          unix_timestamp(sales_date, 'yyyymmdd'),
          'yyyy-mm-dd'
        )
      ) / 3
    ),
    2,
    0
  ) as season,
  substr(sales_date, 1, 6) as months,
  a.sales_date,
  a.channel,
  a.channel_name,
  a.dc_code,
  a.dc_name,
  a.dc_province_code,
  a.dc_province_name,
  a.dc_company_code,
  a.dc_company_name,
  a.customer_no,
  a.customer_name,
  a.province_code,
  a.province_name,
  a.goods_code,
  a.bar_code,
  a.goods_name,
  a.brand_name,
  a.unit,
  case
    when division_code in ('10', '11') then '11'
    when division_code in ('12', '13', '14') then '12'
    when division_code in ('15') then '15'
    else division_code
  end bd_id,
  case
    when division_code in ('10', '11') then '生鲜供应链'
    when division_code in ('12', '13', '14') then '食百供应链'
    when division_code in ('15') then '自用品'
    else division_name
  end bd_name,
  a.division_code,
  division_name,
  department_code,
  department_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  vendor_code,
  vendor_name,
  b.des_specific_product_status,
  product_status_name,
  coalesce(valid_tag, '') as valid_tag,
  coalesce(valid_tag_name, '') as valid_tag_name,
  coalesce(sales_cost / sales_qty, 0) as avg_cost,
  coalesce(sales_value / sales_qty, 0) as avg_price,
  coalesce(sales_qty) as sales_qty,
  coalesce(sales_value) as sales_value,
  coalesce(sales_cost) as sales_cost,
  coalesce(profit) as profit,
  coalesce(front_profit) as front_profit,
  coalesce(promotion_deduction) as promotion_deduction,
  coalesce(excluding_tax_sales) as excluding_tax_sales,
  coalesce(excluding_tax_cost) as excluding_tax_cost,
  coalesce(excluding_tax_profit) as excluding_tax_profit,
  coalesce(excluding_tax_deduction) as excluding_tax_deduction,
  order_kind,
  sales_date as sdt
FROM (
    SELECT
      sales_date,
      channel,
      channel_name,
      dc_code,
      dc_name,
      dc_province_code,
      dc_province_name,
      dc_company_code,
      dc_company_name,
      customer_no,
      customer_name,
      province_code,
      province_name,
      goods_code,
      bar_code,
      goods_name,
      brand_name,
      unit,
      division_code,
      division_name,
      department_code,
      department_name,
      category_large_code,
      category_large_name,
      category_middle_code,
      category_middle_name,
      category_small_code,
      category_small_name,
      sum(sales_qty) AS sales_qty,
      sum(sales_value) AS sales_value,
      sum(sales_cost) AS sales_cost,
      sum(profit) AS profit,
      sum(front_profit) AS front_profit,
      sum(promotion_deduction) AS promotion_deduction,
      sum(excluding_tax_sales) AS excluding_tax_sales,
      sum(excluding_tax_cost) AS excluding_tax_cost,
      sum(excluding_tax_profit) AS excluding_tax_profit,
      sum(excluding_tax_deduction) AS excluding_tax_deduction,
      order_kind,
      regexp_replace(vendor_code, '(^0*)', '') AS vendor_code,
      vendor_name
    FROM csx_dw.customer_sale_m
    WHERE
      sdt < '20191211'
      and sdt >= '20191201'
    GROUP BY
      sales_date,
      channel,
      channel_name,
      dc_code,
      dc_name,
      dc_province_code,
      dc_province_name,
      customer_no,
      customer_name,
      province_code,
      province_name,
      goods_code,
      bar_code,
      goods_name,
      brand_name,
      unit,
      division_code,
      division_name,
      department_code,
      department_name,
      category_large_code,
      category_large_name,
      category_middle_code,
      category_middle_name,
      category_small_code,
      category_small_name,
      order_kind,
      vendor_code,
      vendor_name,
      dc_company_code,
      dc_company_name
  ) a
LEFT OUTER JOIN (
    SELECT
      shop_code,
      product_code,
      des_specific_product_status,
      product_status_name,
      valid_tag,
      valid_tag_name,
      location_name
    FROM csx_dw.csx_product_info
    WHERE
      sdt = regexp_replace(
        to_date(date_sub(current_timestamp(), 1)),
        '-',
        ''
      )
  ) b ON a.dc_code = regexp_replace(shop_code, '(^E)', '9')
  AND a.goods_code = b.product_code;