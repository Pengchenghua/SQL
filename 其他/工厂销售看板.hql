-- -- 建立库存/SKU/库存额/当天入库额;
-- 品类	动销SKU	"动销率（动销SKU/有库存SKU)"	SKU数	日销售	日毛利	累计销售	累计毛利	商超客户数	大客户客户数	"渗透率
-- （大客户）"	负毛利品项	库存额	周转天数	当天入库额
set
  mapreduce.job.queuename = caishixian;
SET
  sdate = '2019-12-01';
SET
  edate = '2019-12-19';
DROP TABLE IF EXISTS temp.factory_stock_00;
--select * from temp.factory_stock_01;
  CREATE TEMPORARY TABLE temp.factory_stock_00 AS
select
  a.*,
  b.*,
  c.province_code,
  c.province_name
from (
    select
      *
    from csx_dw.csx_product_info
    where
      sdt = regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '')
  ) a
left join (
    select
      distinct factory_location_code,
      workshop_code,
      goods_code
    from csx_dw.factory_bom
    where
      sdt = regexp_replace($ { hiveconf :edate }, '-', '')
  ) b on a.product_code = b.goods_code
  and a.shop_code = b.factory_location_code
LEFT JOIN (
    select
      shop_id,
      province_code,
      province_name
    from csx_dw.shop_m
    where
      sdt = 'current'
  ) c on regexp_replace(a.shop_code, '(^E)', '9') = c.shop_id;
DROP TABLE IF EXISTS temp.factory_stock_01;
CREATE TEMPORARY TABLE temp.factory_stock_01 AS
SELECT
  dc_code,
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  a.goods_code,
  division_code,
  final_qty,
  final_amt,
  period_qty,
  period_amt,
  receive_amt,
  if(c.goods_code is not null, '是', '否') as label
from (
    SELECT
      dc_code,
      a.goods_code,
      division_code,
      sum(
        CASE
          WHEN sdt = regexp_replace($ { hiveconf :edate }, '-', '') THEN qty
        END
      ) AS final_qty,
      sum(
        CASE
          WHEN sdt = regexp_replace($ { hiveconf :edate }, '-', '') THEN amt
        END
      ) AS final_amt,
      sum(qty) AS period_qty,
      sum(amt) AS period_amt
    FROM csx_dw.wms_accounting_stock_m a
    WHERE
      sdt >= regexp_replace(
        $ { hiveconf :sdate },
        '-',
        ''
      )
      AND sdt <= regexp_replace(
        $ { hiveconf :edate },
        '-',
        ''
      ) --and a.dc_code='W048'
      AND reservoir_area_code NOT IN (
        'B999',
        'B997',
        'PD01',
        'PD02',
        'TS01'
      )
    GROUP BY
      dc_code,
      a.goods_code,
      division_code
  ) a
LEFT JOIN (
    select
      shop_id,
      province_code,
      province_name
    from csx_dw.shop_m
    where
      sdt = 'current'
  ) b on regexp_replace(a.dc_code, '(^E)', '9') = b.shop_id
LEFT OUTER JOIN (
    select
      DISTINCT factory_location_code,
      workshop_code,
      workshop_name,
      goods_code
    from csx_dw.factory_bom
    where
      sdt = regexp_replace($ { hiveconf :edate }, '-', '')
  ) c on a.goods_code = c.goods_code
  and a.dc_code = c.factory_location_code
LEFT JOIN (
    select
      receive_location_code,
      goods_code,
      sum(receive_qty * price) as receive_amt
    from csx_dw.wms_entry_order_m
    where
      sdt = regexp_replace($ { hiveconf :edate }, '-', '')
    GROUP BY
      receive_location_code,
      goods_code
  ) d on a.dc_code = d.receive_location_code
  and a.goods_code = d.goods_code;
-- 2.0 创建销售表
  --select * from temp.stock_01 ;
  DROP TABLE IF EXISTS temp.factory_stock_02;
CREATE TEMPORARY TABLE temp.factory_stock_02 AS
select
  a.shop_id,
  a.province_code,
  a.province_name,
  workshop_code,
  workshop_name,
  channel_name,
  customer_no,
  a.goods_code,
  category_code,
  day_sale,
  day_profit,
  sales_value,
  sales_cost,
  profit,
  if(c.goods_code is NOT NULL, '是', '否') as label
from (
    select
      a.shop_id,
      b.province_code,
      a.province_name,
      -- workshop_code,
      -- workshop_name,
      channel_name,
      customer_no,
      a.goods_code,
      category_code,
      day_sale,
      day_profit,
      sales_value,
      sales_cost,
      profit -- if(c.goods_code is NOT NULL,'是','否') as label
    from (
        SELECT
          a.shop_id,
          a.province_code,
          a.province_name,
          CASE
            WHEN channel IN ('1', '7') THEN '大客户'
            WHEN channel IN ('2', '3') THEN '商超'
            else a.channel_name
          END channel_name,
          customer_no,
          goods_code,
          category_code,
          sum(
            CASE
              WHEN sdt = regexp_replace($ { hiveconf :edate }, '-', '') THEN sales_value
            END
          ) AS day_sale,
          sum(
            CASE
              WHEN sdt = regexp_replace($ { hiveconf :edate }, '-', '') THEN profit
            END
          ) AS day_profit,
          sum(coalesce(sales_value, 0)) AS sales_value,
          sum(sales_cost) as sales_cost,
          sum(coalesce(profit, 0)) AS profit
        FROM csx_dw.sale_goods_m1 a
        WHERE
          sdt >= regexp_replace(
            $ { hiveconf :sdate },
            '-',
            ''
          )
          AND sdt <= regexp_replace(
            $ { hiveconf :edate },
            '-',
            ''
          ) -- AND shop_id='W0A3'
        GROUP BY
          a.shop_id,
          CASE
            WHEN channel IN ('1', '7') THEN '大客户'
            WHEN channel IN ('2', '3') THEN '商超'
            else a.channel_name
          END,
          goods_code,
          customer_no,
          a.province_code,
          a.province_name,
          category_code
      ) a
    LEFT JOIN (
        select
          `limit` province_code,
          province
        from csx_ods.sys_province_ods
      ) b on a.province_name = b.province
  ) a
LEFT OUTER JOIN (
    select
      province_code,
      workshop_code,
      workshop_name,
      goods_code
    from csx_dw.factory_bom
    where
      sdt = regexp_replace($ { hiveconf :edate }, '-', '')
    GROUP BY
      province_code,
      workshop_code,
      workshop_name,
      goods_code
  ) c on a.goods_code = c.goods_code
  and a.province_code = c.province_code
where
  c.goods_code is NOT NULL;
--select * from  temp.factory_stock_02;
  -- 销售数/高库存/负毛利
  --set hive.groupby.skewindata=true;
  --set hive.map.aggr=true;
  drop table if exists temp.factory_stock_03;
CREATE temporary table temp.factory_stock_03 as
SELECT
  province_code,
  province_name,
  goods_code,
  workshop_code,
  workshop_name,
  sum(sale_sku) sale_sku,
  --  sum(all_sku) as all_sku,
  sum(day_sale) AS day_sale,
  sum(day_profit) AS day_profit,
  sum(sales_value) AS sales_value,
  sum(profit) AS profit,
  sum(final_qty) AS final_qty,
  sum(final_amt) AS final_amt,
  sum(period_qty) AS period_qty,
  sum(period_amt) AS period_amt,
  sum(sales_cost) as sales_cost,
  coalesce(sum(period_amt) / sum(sales_cost), 0) as day_turnover,
  sum(negative_sku) as negative_sku,
  sum(receive_amt) as receive_amt
FROM (
    SELECT
      a.province_code,
      province_name,
      a.goods_code,
      workshop_code,
      workshop_name,
      count(
        DISTINCT CASE
          WHEN coalesce(sales_value, 0) != 0 THEN a.goods_code
        END
      ) AS sale_sku,
      0 AS negative_sku,
      -- count(DISTINCT CASE
      --                      WHEN channel_name='大客户' THEN customer_no
      --                  END) AS big_cust_data,
      --   count(DISTINCT CASE
      --                      WHEN channel_name='商超' THEN customer_no
      --                  END) AS shop_cust_data,
      sum(day_sale) AS day_sale,
      sum(day_profit) AS day_profit,
      sum(sales_value) AS sales_value,
      sum(sales_cost) as sales_cost,
      sum(profit) AS profit,
      0 AS final_qty,
      0 AS final_amt,
      0 AS period_qty,
      0 AS period_amt,
      0 as receive_amt
    FROM temp.factory_stock_02 A
    GROUP BY
      a.province_code,
      province_name,
      a.goods_code,
      workshop_code,
      workshop_name
    UNION ALL
    SELECT
      a.province_code,
      province_name,
      a.goods_code,
      workshop_code,
      workshop_name,
      0 AS sale_sku,
      0 as negative_sku,
      -- count(DISTINCT goods_code) AS all_sku,
      -- 0 AS big_cust_data,
      -- 0 AS shop_cust_data,
      0 AS day_sale,
      0 AS day_profit,
      0 AS sales_value,
      0 as sales_cost,
      0 AS profit,
      sum(final_qty) AS final_qty,
      sum(final_amt) AS final_amt,
      sum(period_qty) AS period_qty,
      sum(period_amt) AS period_amt,
      sum(receive_amt) as receive_amt
    FROM temp.factory_stock_01 a
    GROUP BY
      a.province_code,
      province_name,
      a.goods_code,
      workshop_code,
      workshop_name
    union all
    SELECT
      province_code,
      province_name,
      goods_code,
      workshop_code,
      workshop_name,
      0 AS sale_sku,
      count(
        DISTINCT case
          when profit < 0 then goods_code
        end
      ) AS negative_sku,
      -- count(DISTINCT CASE
      --                      WHEN channel_name='大客户' THEN customer_no
      --                  END) AS big_cust_data,
      --   count(DISTINCT CASE
      --                      WHEN channel_name='商超' THEN customer_no
      --                  END) AS shop_cust_data,
      0 AS day_sale,
      0 AS day_profit,
      0 AS sales_value,
      0 as sales_cost,
      0 AS profit,
      0 AS final_qty,
      0 AS final_amt,
      0 AS period_qty,
      0 AS period_amt,
      0 as receive_amt
    FROM (
        select
          a.province_code,
          province_name,
          a.goods_code,
          workshop_code,
          workshop_name,
          sum(profit) profit
        from temp.factory_stock_02 a
        GROUP BY
          a.province_code,
          province_name,
          a.goods_code,
          workshop_code,
          workshop_name
      ) a
    GROUP BY
      province_code,
      province_name,
      goods_code,
      workshop_code,
      workshop_name
  ) a
GROUP BY
  province_code,
  province_name,
  goods_code,
  workshop_code,
  workshop_name;
drop table if exists temp.factory_stock_04;
CREATE temporary table temp.factory_stock_04 as
SELECT
  province_code,
  province_name,
  workshop_code,
  workshop_name,
  sum(sale_sku) AS sale_sku,
  count(DISTINCT goods_code) all_sku,
  sum(day_sale) AS day_sale,
  sum(day_profit) AS day_profit,
  sum(sales_value) AS sales_value,
  sum(profit) AS profit,
  sum(final_qty) AS final_qty,
  sum(final_amt) AS final_amt,
  sum(period_qty) AS period_qty,
  sum(period_amt) AS period_amt,
  coalesce(sum(period_amt) / sum(sales_cost), 0) as day_turnover,
  sum(negative_sku) as negative_sku,
  sum(receive_amt) as receive_amt
FROM temp.factory_stock_03
GROUP BY
  province_code,
  province_name,
  workshop_code,
  workshop_name;
--客户数
  --set hive.groupby.skewindata=false;
  drop table if exists temp.factory_stock_05;
CREATE temporary table temp.factory_stock_05 as
select
  a.province_code,
  workshop_code,
  workshop_name,
  shop_dept_cust,
  big_dept_cust,
  round(big_dept_cust / big_cust, 4) sale_cust_ratio,
  big_cust
from (
    select
      province_code,
      workshop_code,
      workshop_name,
      count(
        distinct case
          when channel_name = '商超' then customer_no
        end
      ) as shop_dept_cust,
      count(
        distinct case
          when channel_name = '大客户' then customer_no
        end
      ) as big_dept_cust
    from temp.factory_stock_02
    group by
      province_code,
      workshop_code,
      workshop_name
  ) a
left join (
    select
      province_code,
      count(
        distinct case
          when channel_name = '大客户' then customer_no
        end
      ) as big_cust
    from temp.factory_stock_02
    group by
      province_code
  ) b on a.province_code = b.province_code;
-- 产值数据
  drop table temp.temp_fact_01;
create temporary table temp.temp_fact_01 as
select
  a.province_code,
  a.workshop_code,
  a.workshop_name,
  user_qty,
  fact_qty,
  product_rate,
  precision_rate,
  b.fact_amt,
  plan_qty
from (
    SELECT
      province_code,
      workshop_code,
      workshop_name,
      sum(user_qty) user_qty,
      sum(fact_qty) fact_qty,
      sum(fact_qty) / sum(user_qty) AS product_rate,
      sum(plan_user) plan_qty,
      sum(fact_qty) / sum(plan_user) AS precision_rate
    FROM csx_dw.factory_out_rate
    WHERE
      sdt >= regexp_replace($ { hiveconf :sdate }, '-', '')
      and sdt <= regexp_replace($ { hiveconf :edate }, '-', '')
    GROUP BY
      province_code,
      workshop_code,
      location_code,
      workshop_name
  ) a
left join (
    select
      province_code,
      workshop_code,
      sum(p_cost_subtotal) fact_amt
    from csx_dw.factory_order_cost_materials
    where
      sdt <= regexp_replace($ { hiveconf :edate }, '-', '')
      and sdt >= regexp_replace($ { hiveconf :sdate }, '-', '')
    group by
      province_code,
      workshop_code
  ) b on a.province_code = b.province_code
  and a.workshop_code = b.workshop_code;
SELECT
  no,
  a.province_code,
  a.province_name,
  a.workshop_code,
  a.workshop_name,
  a.day_sale / 10000 day_sale,
  a.day_profit / 10000 day_profit,
  day_profit_rate,
  a.sales_value / 10000 sales_value,
  a.profit / 10000 profit,
  profit_rate,
  a.negative_sku,
  a.sale_sku,
  a.all_sku,
  pin_rate,
  a.final_amt / 10000 final_amt,
  a.day_turnover,
  a.receive_amt / 10000 receive_amt,
  a.shop_dept_cust,
  a.big_dept_cust,
  a.sale_cust_ratio,
  fact_qty / 10000 fact_qty,
  fact_amt / 10000 fact_amt,
  product_rate,
  precision_rate
from (
    SELECT
      '1' no,
      a.province_code,
      a.province_name,
      a.workshop_code,
      a.workshop_name,
      a.day_sale,
      a.day_profit,
      a.day_profit / a.day_sale day_profit_rate,
      a.sales_value,
      a.profit,
      a.profit / a.sales_value profit_rate,
      a.negative_sku,
      a.sale_sku,
      c.all_sku,
      a.sale_sku / c.all_sku pin_rate,
      a.final_amt,
      a.day_turnover,
      a.receive_amt,
      b.shop_dept_cust,
      b.big_dept_cust,
      b.sale_cust_ratio,
      fact_qty,
      fact_amt,
      product_rate,
      precision_rate
    FROM temp.factory_stock_04 a
    LEFT JOIN temp.factory_stock_05 b ON a.province_code = b.province_code
      AND a.workshop_code = b.workshop_code
    LEFT JOIN (
        SELECT
          province_code,
          workshop_code,
          count(DISTINCT goods_code) all_sku
        FROM temp.factory_stock_00
        WHERE
          des_specific_product_status = '0'
        GROUP BY
          province_code,
          workshop_code
      ) c ON a.province_code = c.province_code
      AND a.workshop_code = c.workshop_code
    left join temp.temp_fact_01 d on a.province_code = d.province_code
      and a.workshop_code = d.workshop_code
    union all
    SELECT
      '2' no,
      a.province_code,
      province_name,
      '' workshop_code,
      '' workshop_name,
      day_sale,
      day_profit,
      day_profit / day_sale day_profit_rate,
      sales_value,
      profit,
      profit / sales_value as profit_rate,
      negative_sku,
      sale_sku,
      b.all_sku,
      sale_sku / b.all_sku as pin_rate,
      final_amt,
      day_turnover,
      receive_amt,
      shop_cust as shop_dept_cust,
      big_cust as big_dept_cust,
      '' sale_cust_ratio,
      fact_qty,
      fact_amt,
      product_rate,
      precision_rate
    FROM (
        SELECT
          a.province_code,
          province_name,
          sum(sale_sku) AS sale_sku,
          --count(DISTINCT goods_code) all_sku,
          sum(day_sale) AS day_sale,
          sum(day_profit) AS day_profit,
          sum(sales_value) AS sales_value,
          sum(profit) AS profit,
          sum(final_qty) AS final_qty,
          sum(final_amt) AS final_amt,
          sum(period_qty) AS period_qty,
          sum(period_amt) AS period_amt,
          coalesce(sum(period_amt) / sum(sales_cost), 0) as day_turnover,
          sum(negative_sku) as negative_sku,
          sum(receive_amt) as receive_amt
        FROM temp.factory_stock_03 a
        where
          a.workshop_code is not null
        GROUP BY
          a.province_code,
          province_name
      ) a
    LEFT JOIN (
        SELECT
          province_code,
          count(DISTINCT goods_code) all_sku
        FROM temp.factory_stock_00
        WHERE
          des_specific_product_status = '0'
        GROUP BY
          province_code
      ) b on a.province_code = b.province_code
    LEFT JOIN (
        select
          province_code,
          count(
            distinct case
              when channel_name = '商超' then customer_no
            end
          ) as shop_cust,
          count(
            distinct case
              when channel_name = '大客户' then customer_no
            end
          ) as big_cust
        from temp.factory_stock_02
        group by
          province_code
      ) c on a.province_code = c.province_code
    left join (
        SELECT
          province_code,
          sum(user_qty) user_qty,
          sum(fact_qty) fact_qty,
          sum(fact_amt) fact_amt,
          sum(fact_qty) / sum(user_qty) AS product_rate,
          sum(plan_qty) plan_qty,
          sum(fact_qty) / sum(plan_qty) AS precision_rate
        FROM temp.temp_fact_01
        GROUP BY
          province_code
      ) d on a.province_code = d.province_code
  ) a
order by
  province_code,
  no,
  workshop_code;