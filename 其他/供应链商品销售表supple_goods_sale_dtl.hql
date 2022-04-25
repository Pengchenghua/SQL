SET
  mapreduce.job.queuename = caishixian;
set
  hive.exec.dynamic.partition = true;
--开启动态分区
set
  hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
set
  hive.exec.max.dynamic.partitions = 100000;
--在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set
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
  valid_tag,
  valid_tag_name,
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
      case
        when channel in('1', '7') then '0'
        when province_code = '33' then '33'
        when province_code = '-100' then '-100'
        else channel
      end channel,
      case
        when channel in('1', '7') then '大客户'
        when province_code = '33' then '大客户平台'
        when province_code = '-100' then '商超平台'
        else channel_name
      end channel_name,
      dc_code,
      dc_name,
      case
        when dc_code = 'W0H4' then dc_code
        else dc_province_code
      end dc_province_code,
      case
        when dc_code = 'W0H4' then dc_name
        else dc_province_name
      end dc_province_name,
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
    FROM csx_dw.customer_sale_m a 
    WHERE
      sdt >= '20190901' --and sdt >='20190101'
    GROUP BY
      sales_date,
      channel,
      channel_name,
      dc_code,
      dc_name,
      case
        when dc_code = 'W0H4' then dc_code
        else dc_province_code
      end,
      case
        when dc_code = 'W0H4' then dc_name
        else dc_province_name
      end,
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



-- 创建表
  create table csx_dw.supple_goods_sale_dtl (
    years string comment '销售年份',
    season string comment '销售季度',
    months string comment '销售月份',
    sales_date string comment '销售日期',
    channel string comment '销售渠道编码',
    channel_name string comment '销售渠道编码',
    dc_code string comment 'DC编码',
    dc_name string comment 'DC名称',
    dc_province_code string comment 'DC省区编码',
    dc_province_name string comment 'DC省区名称',
    dc_company_code string comment 'DC公司代码',
    dc_company_name string comment 'DC公司代码名称',
    customer_no string comment '客户编码',
    customer_name string comment '客户名称',
    province_code string comment '客户省区编码',
    province_name string comment '客户省区名称',
    goods_code string comment '商品编码',
    bar_code string comment '商品条码',
    goods_name string comment '商品名称',
    brand_name string comment '品牌名称',
    unit string comment '销售单位',
    bd_id string comment '供应链编码',
    bd_name string comment '供应链名称',
    division_code string comment '部类编码',
    division_name string comment '部类名称',
    department_code string comment '课组编码',
    department_name string comment '课组名称',
    category_large_code string comment '大类编码',
    category_large_name string comment '大类名称',
    category_middle_code string comment '中类编码',
    category_middle_name string comment '中类名称',
    category_small_code string comment '小类编码',
    category_small_name string comment '小类名称',
    vendor_code string COMMENT '供应商编码(业务主键)',
    vendor_name string COMMENT '供应商名称',
    goods_status string COMMENT '门店商品状态',
    goods_status_name string COMMENT '门店商品状态名称',
    valid_tag string COMMENT '有效标识',
    valid_tag_name string COMMENT '有效标识名称',
    avg_cost DECIMAL(26, 6) comment '平均成本',
    avg_price DECIMAL(26, 6) comment '平均售价',
    sales_qty DECIMAL(26, 6) comment '销售量',
    sales_value DECIMAL(26, 6) comment '销售金额',
    sales_cost DECIMAL(26, 6) comment '销售成本',
    profit DECIMAL(26, 6) comment '销售毛利额',
    front_profit DECIMAL(26, 6) comment '前端毛利额',
    promotion_deduction DECIMAL(26, 6) comment '促销扣款',
    excluding_tax_sales DECIMAL(26, 6) comment '未税销售额',
    excluding_tax_cost DECIMAL(26, 6) comment '未税成本',
    excluding_tax_profit DECIMAL(26, 6) comment '未税毛利额',
    excluding_tax_deduction DECIMAL(26, 6) comment '未税促销扣款',
    order_kind string comment '订单类型：NORMAL-普通单，WELFARE-福利单'
  ) comment '供应链商品销售明细报表' partitioned by (sdt string comment '日期分区') stored as parquet;