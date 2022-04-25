set
  mapreduce.job.queuename = caishixian;
set
  mapreduce.job.reduces = 80;
set
  hive.map.aggr = true;
set
  hive.groupby.skewindata = true;
set
  hive.exec.parallel = true;
set
  hive.exec.dynamic.partition = true;
--开启动态分区
set
  hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
set
  hive.exec.max.dynamic.partitions = 10000;
--在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set
  hive.exec.max.dynamic.partitions.pernode = 100000;
--源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
  drop table if exists csx_dw.supply_inve_11;
create temporary table if not exists csx_dw.supply_inve_11 as
SELECT
  dc_type,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  a.goodsid,
  SUM(a.sales_qty) sales_qty,
  SUM(a.sales_value) sales_value,
  SUM(a.sales_cost) sales_cost,
  SUM(a.profit) profit,
  SUM(a.period_inv_amt) period_inv_amt,
  SUM(a.final_amt) final_amt,
  SUM(final_qty) final_qty,
  round(
    coalesce(SUM(period_inv_amt) / SUM(sales_cost), 0),
    2
  ) AS days_turnover,
  nvl(max(a.max_sale_sdt), '') as max_sale_sdt,
  coalesce(
    datediff(
      to_date(date_sub(current_timestamp(), 1)),
      from_unixtime(
        unix_timestamp(max(a.max_sale_sdt), 'yyyyMMdd'),
        'yyyy-MM-dd'
      )
    ),
    ''
  ) as no_sale_days,
  nvl(max(a.entry_sdt), '') as entry_sdt,
  coalesce(
    datediff(
      to_date(date_sub(current_timestamp(), 1)),
      from_unixtime(
        unix_timestamp(max(a.entry_sdt), 'yyyyMMdd'),
        'yyyy-MM-dd'
      )
    ),
    ''
  ) as entry_days
FROM csx_dw.supply_turnover a
WHERE
  sdt = regexp_replace(
    to_date(date_sub(CURRENT_TIMESTAMP(), 1)),
    '-',
    ''
  )
group by
  a.goodsid,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  dc_type;
drop table if exists csx_dw.supply_inve_12;
create temporary table if not exists csx_dw.supply_inve_12 as
SELECT
  dc_type,
  case
    when a.shop_id = 'W0H4' then 'W0H4'
    else prov_code
  end prov_code,
  case
    when a.shop_id = 'W0H4' then '供应链平台'
    else prov_name
  end prov_name,
  a.shop_id,
  a.shop_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  a.goodsid,
  SUM(a.sales_qty) sales_qty,
  SUM(a.sales_value) sales_value,
  SUM(a.sales_cost) sales_cost,
  SUM(a.profit) profit,
  SUM(a.period_inv_amt) period_inv_amt,
  SUM(a.final_amt) final_amt,
  SUM(final_qty) final_qty,
  round(
    coalesce(SUM(period_inv_amt) / SUM(sales_cost), 0),
    2
  ) AS days_turnover,
  nvl(max(a.max_sale_sdt), '') as max_sale_sdt,
  coalesce(
    datediff(
      to_date(date_sub(current_timestamp(), 1)),
      from_unixtime(
        unix_timestamp(max(a.max_sale_sdt), 'yyyyMMdd'),
        'yyyy-MM-dd'
      )
    ),
    ''
  ) as no_sale_days,
  nvl(max(a.entry_sdt), '') as entry_sdt,
  coalesce(
    datediff(
      to_date(date_sub(current_timestamp(), 1)),
      from_unixtime(
        unix_timestamp(max(a.entry_sdt), 'yyyyMMdd'),
        'yyyy-MM-dd'
      )
    ),
    ''
  ) as entry_days
FROM csx_dw.supply_turnover a
WHERE
  sdt = regexp_replace(
    to_date(date_sub(CURRENT_TIMESTAMP(), 1)),
    '-',
    ''
  )
group by
  a.goodsid,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  case
    when a.shop_id = 'W0H4' then 'W0H4'
    else prov_code
  end,
  case
    when a.shop_id = 'W0H4' then '供应链平台'
    else prov_name
  end,
  dc_type,
  a.shop_id,
  a.shop_name;
--drop table  csx_dw.supply_turnover_dc;
insert overwrite table csx_dw.supply_turnover_dc partition(sdt)
select
  dc_type,
  prov_code,
  prov_name,
  shop_id,
  shop_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  sum(sales_qty) sales_qty,
  sum(sales_value) sales_value,
  sum(profit) profit,
  COALESCE(sum(profit) / sum(sales_value), 0) * 1.00 AS profit_rate,
  sum(sales_cost) sales_cost,
  sum(period_inv_amt) period_inv_amt,
  sum(final_amt) final_amt,
  sum(final_qty) final_qty,
  sum(days_turnover) days_turnover,
  sum(goods_sku) goods_sku,
  sum(sale_sku) sale_sku,
  round(sum(sale_sku) / sum(goods_sku), 4) * 1.00 pin_rate,
  sum(negative_inventory) negative_inventory,
  sum(negative_amt) negative_amt,
  sum(highet_sku) highet_sku,
  sum(highet_amt) highet_amt,
  sum(no_sale_sku) no_sale_sku,
  sum(no_sale_amt) no_sale_amt,
  regexp_replace(
    to_date(date_sub(CURRENT_TIMESTAMP(), 1)),
    '-',
    ''
  )
from (
    SELECT
      dc_type,
      prov_code,
      prov_name,
      shop_id,
      shop_name,
      '00' bd_id,
      '合计' bd_name,
      '00' dept_id,
      '小计' dept_name,
      SUM(a.sales_qty) sales_qty,
      SUM(a.sales_value) sales_value,
      SUM(a.profit) profit,
      sum(sales_cost) sales_cost,
      SUM(a.period_inv_amt) period_inv_amt,
      SUM(a.final_amt) final_amt,
      SUM(final_qty) final_qty,
      round(SUM(period_inv_amt) / SUM(sales_cost), 2) AS days_turnover,
      COUNT(
        case
          when a.sales_value <> 0
          or period_inv_amt != 0 then goodsid
        end
      ) AS goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value != 0 THEN goodsid
        END
      ) AS sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) AS negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) AS negative_amt,
      COUNT (
        CASE
          WHEN (
            days_turnover > 15
            AND final_amt > 500
            AND bd_id = '11'
            and entry_days > 3
          ) THEN goodsid
          WHEN (
            days_turnover > 30
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A01',
              'A02',
              'A03',
              'A04',
              'A10'
            )
          ) THEN goodsid
          WHEN (
            days_turnover > 45
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A05',
              'A06',
              'A07',
              'A08',
              'A09',
              'P01',
              'P10'
            )
          ) THEN goodsid
        END
      ) AS highet_sku,
      SUM (
        CASE
          WHEN (
            days_turnover > 15
            AND final_amt > 500
            and entry_days > 3
            AND bd_id = '11'
          ) THEN final_amt
          WHEN (
            days_turnover > 30
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A01',
              'A02',
              'A03',
              'A04',
              'A10'
            )
          ) THEN final_amt
          WHEN (
            days_turnover > 45
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A05',
              'A06',
              'A07',
              'A08',
              'A09',
              'P01',
              'P10'
            )
          ) THEN final_amt
        END
      ) highet_amt,
      COUNT (
        CASE
          WHEN no_sale_days > 30
          AND final_amt > 0 THEN goodsid
        END
      ) AS no_sale_sku,
      SUM (
        CASE
          WHEN no_sale_days > 30
          AND final_amt > 0 THEN final_amt
        END
      ) AS no_sale_amt
    FROM csx_dw.supply_inve_12 a
    GROUP BY
      dc_type,
      prov_code,
      prov_name,
      shop_id,
      shop_name
    union all
    SELECT
      dc_type,
      prov_code,
      prov_name,
      shop_id,
      shop_name,
      bd_id,
      bd_name,
      '00' dept_id,
      '小计' dept_name,
      SUM(a.sales_qty) sales_qty,
      SUM(a.sales_value) sales_value,
      SUM(a.profit) profit,
      sum(sales_cost) sales_cost,
      SUM(a.period_inv_amt) period_inv_amt,
      SUM(a.final_amt) final_amt,
      SUM(final_qty) final_qty,
      round(SUM(period_inv_amt) / SUM(sales_cost), 2) AS days_turnover,
      COUNT(
        case
          when a.sales_value <> 0
          or period_inv_amt != 0 then goodsid
        end
      ) AS goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value != 0 THEN goodsid
        END
      ) AS sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) AS negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) AS negative_amt,
      COUNT (
        CASE
          WHEN (
            days_turnover > 15
            AND final_amt > 500
            and entry_days > 3
            AND bd_id = '11'
          ) THEN goodsid
          WHEN (
            days_turnover > 30
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A01',
              'A02',
              'A03',
              'A04',
              'A10'
            )
          ) THEN goodsid
          WHEN (
            days_turnover > 45
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A05',
              'A06',
              'A07',
              'A08',
              'A09',
              'P01',
              'P10'
            )
          ) THEN goodsid
        END
      ) AS highet_sku,
      SUM (
        CASE
          WHEN (
            days_turnover > 15
            AND final_amt > 500
            and entry_days > 3
            AND bd_id = '11'
          ) THEN final_amt
          WHEN (
            days_turnover > 30
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A01',
              'A02',
              'A03',
              'A04',
              'A10'
            )
          ) THEN final_amt
          WHEN (
            days_turnover > 45
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A05',
              'A06',
              'A07',
              'A08',
              'A09',
              'P01',
              'P10'
            )
          ) THEN final_amt
        END
      ) highet_amt,
      COUNT (
        CASE
          WHEN no_sale_days > 30
          AND final_amt > 0 THEN goodsid
        END
      ) AS no_sale_sku,
      SUM (
        CASE
          WHEN no_sale_days > 30
          AND final_amt > 0 THEN final_amt
        END
      ) AS no_sale_amt
    FROM csx_dw.supply_inve_12 a
    GROUP BY
      dc_type,
      prov_code,
      prov_name,
      shop_id,
      shop_name,
      bd_id,
      bd_name
    union all
    SELECT
      dc_type,
      prov_code,
      prov_name,
      shop_id,
      shop_name,
      bd_id,
      bd_name,
      dept_id,
      dept_name,
      SUM(a.sales_qty) sales_qty,
      SUM(a.sales_value) sales_value,
      SUM(a.profit) profit,
      sum(sales_cost) sales_cost,
      SUM(a.period_inv_amt) period_inv_amt,
      SUM(a.final_amt) final_amt,
      SUM(final_qty) final_qty,
      round(SUM(period_inv_amt) / SUM(sales_cost), 2) AS days_turnover,
      COUNT(
        case
          when a.sales_value <> 0
          or period_inv_amt != 0 then goodsid
        end
      ) AS goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value != 0 THEN goodsid
        END
      ) AS sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) AS negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) AS negative_amt,
      -- 生鲜高库存 周转天数大于15天，入库天数大于3，库存额大于500 ；食品 周转大于30天，库存额大于2000，入库天数大于7；用品 周转>45,库存额>2000 ，入库天数大于7;
      COUNT (
        CASE
          WHEN (
            days_turnover > 15
            AND final_amt > 500
            and entry_days > 3
            AND bd_id = '11'
          ) THEN goodsid
          WHEN (
            days_turnover > 30
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A01',
              'A02',
              'A03',
              'A04',
              'A10'
            )
          ) THEN goodsid
          WHEN (
            days_turnover > 45
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A05',
              'A06',
              'A07',
              'A08',
              'A09',
              'P01',
              'P10'
            )
          ) THEN goodsid
        END
      ) AS highet_sku,
      SUM (
        CASE
          WHEN (
            days_turnover > 15
            AND final_amt > 500
            AND bd_id = '11'
            and entry_days > 3
          ) THEN final_amt
          WHEN (
            days_turnover > 30
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A01',
              'A02',
              'A03',
              'A04',
              'A10'
            )
          ) THEN final_amt
          WHEN (
            days_turnover > 45
            AND final_amt > 2000
            and entry_days > 7
            AND a.dept_id IN (
              'A05',
              'A06',
              'A07',
              'A08',
              'A09',
              'P01',
              'P10'
            )
          ) THEN final_amt
        END
      ) highet_amt,
      -- 未销售 最近日期>30天，库存额>0
      COUNT (
        CASE
          WHEN no_sale_days > 30
          AND final_amt > 0 THEN goodsid
        END
      ) AS no_sale_sku,
      SUM (
        CASE
          WHEN no_sale_days > 30
          AND final_amt > 0 THEN final_amt
        END
      ) AS no_sale_amt
    FROM csx_dw.supply_inve_12 a
    GROUP BY
      dc_type,
      prov_code,
      prov_name,
      shop_id,
      shop_name,
      bd_id,
      bd_name,
      dept_id,
      dept_name
  ) a
group by
  prov_code,
  prov_name,
  shop_id,
  shop_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  dc_type;
