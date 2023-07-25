-- -- 建立库存/SKU/库存额/当天入库额;
-- 品类	动销SKU	"动销率（动销SKU/有库存SKU)"	SKU数	日销售	日毛利	累计销售	累计毛利	商超数	大数	"渗透率
-- （大）"	负毛利品项	库存额	周转天数	当天入库额
set
  mapreduce.job.queuename = caishixian;
SET
  sdate = '2019-12-01';
SET
  edate = '2019-12-19';
DROP TABLE IF EXISTS temp.stock_01;
CREATE TEMPORARY TABLE temp.stock_01 AS
SELECT
  dc_code,
  province_code,
  province_name,
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
      )
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
join (
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
      goods_code
    from csx_dw.factory_bom
    where
      sdt = regexp_replace($ { hiveconf :edate }, '-', '')
  ) c on a.goods_code = c.goods_code
  AND A.dc_code = factory_location_code
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
--select * from temp.stock_01 ;
  -- 2.0 创建销售表
  DROP TABLE IF EXISTS temp.stock_02;
CREATE TEMPORARY TABLE temp.stock_02 AS
select
  a.shop_id,
  c.province_code,
  a.province_name,
  channel_name,
  customer_no,
  a.goods_code,
  category_code,
  day_sale,
  day_profit,
  sales_value,
  sales_cost,
  profit,
  if(b.goods_code is NOT NULL, '是', '否') as label
from (
    -- select
    -- 	a.shop_id,
    -- 	b.province_code,
    -- 	b.province_name,
    -- 	channel_name,
    -- 	customer_no,
    -- 	a.goods_code,
    -- 	category_code,
    -- 	day_sale,
    -- 	day_profit,
    -- 	sales_value,
    -- 	sales_cost,
    -- 	profit
    -- 	--  if(c.goods_code is NOT NULL,'是','否') as label
    -- 	from (
    SELECT
      a.shop_id,
      a.province_code,
      a.province_name,
      CASE
        WHEN channel IN ('1', '7') THEN '大'
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
      sdt >= regexp_replace($ { hiveconf :sdate }, '-', '')
      AND sdt <= regexp_replace($ { hiveconf :edate }, '-', '')
    GROUP BY
      a.shop_id,
      CASE
        WHEN channel IN ('1', '7') THEN '大'
        WHEN channel IN ('2', '3') THEN '商超'
        else a.channel_name
      END,
      goods_code,
      customer_no,
      a.province_code,
      a.province_name,
      category_code
  ) a -- LEFT JOIN (
  -- 	select
  -- 		shop_id,
  -- 		province_code,
  -- 		province_name
  -- 	from
  -- 		csx_dw.shop_m
  -- 	where
  -- 		sdt = 'current')b on
  -- 	a.shop_id = b.shop_id ) a
left join (
    select
      province_name,
      workshop_code,
      workshop_name,
      goods_code
    from csx_dw.factory_bom
    where
      sdt = regexp_replace($ { hiveconf :edate }, '-', '') -- and province_code='110000'
    GROUP BY
      province_name,
      workshop_code,
      workshop_name,
      goods_code
  ) as b on a.province_name = b.province_name
  and a.goods_code = b.goods_code
LEFT JOIN (
    select
      province,
      `limit` as province_code
    from csx_ods.sys_province_ods
  ) c on a.province_name = c.province;
--select * from temp.stock_02 where category_code='11';
  --select province_code, customer_no,sum(sales_value) from temp.stock_02 group by customer_noprovince_code;
  -- 销售数/高库存/负毛利
  --set hive.groupby.skewindata=true;
set
  hive.map.aggr = true;
drop table if exists temp.stock_03;
CREATE temporary table temp.stock_03 as
SELECT
  province_code,
  province_name,
  goods_code,
  label,
  category_code,
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
      province_code,
      province_name,
      goods_code,
      label,
      category_code,
      count(
        DISTINCT CASE
          WHEN coalesce(sales_value, 0) != 0 THEN goods_code
        END
      ) AS sale_sku,
      0 AS negative_sku,
      -- count(DISTINCT CASE
      --                      WHEN channel_name='大' THEN customer_no
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
    FROM temp.stock_02 --WHERE province_code='110000'
    GROUP BY
      province_code,
      province_name,
      goods_code,
      label,
      category_code
    UNION ALL
    SELECT
      province_code,
      province_name,
      goods_code,
      label,
      division_code AS category_code,
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
    FROM temp.stock_01 --WHERE province_code='110000'
    GROUP BY
      province_code,
      province_name,
      goods_code,
      label,
      division_code
    union all
    SELECT
      province_code,
      province_name,
      goods_code,
      label,
      category_code,
      0 AS sale_sku,
      count(
        DISTINCT case
          when profit < 0 then goods_code
        end
      ) AS negative_sku,
      -- count(DISTINCT CASE
      --                      WHEN channel_name='大' THEN customer_no
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
          province_code,
          province_name,
          goods_code,
          label,
          category_code,
          sum(profit) profit
        from temp.stock_02
        GROUP BY
          province_code,
          province_name,
          goods_code,
          label,
          category_code
      ) a
    GROUP BY
      province_code,
      province_name,
      goods_code,
      category_code,
      label
  ) a
GROUP BY
  province_code,
  province_name,
  goods_code,
  label,
  category_code;
drop table if exists temp.stock_04;
CREATE temporary table temp.stock_04 as
SELECT
  province_code,
  province_name,
  category_code,
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
FROM temp.stock_03
GROUP BY
  province_code,
  province_name,
  category_code;
-- select * from temp.stock_03 where province_code='500000' ;
  --数
  --set hive.groupby.skewindata=false;
  drop table if exists temp.stock_05;
CREATE temporary table temp.stock_05 as
select
  a.province_code,
  category_code,
  shop_dept_cust,
  big_dept_cust,
  round(big_dept_cust / big_cust, 4) sale_cust_ratio,
  big_cust
from (
    select
      province_code,
      category_code,
      count(
        distinct case
          when channel_name = '商超' then customer_no
        end
      ) as shop_dept_cust,
      count(
        distinct case
          when channel_name = '大' then customer_no
        end
      ) as big_dept_cust
    from temp.stock_02
    group by
      province_code,
      category_code
  ) a
left join (
    select
      province_code,
      count(
        distinct case
          when channel_name = '大' then customer_no
        end
      ) as big_cust
    from temp.stock_02
    group by
      province_code
  ) b on a.province_code = b.province_code;
-- 加工/非加工数据
  drop table if exists temp.stock_06;
CREATE temporary table temp.stock_06 as
SELECT
  province_code,
  province_name,
  label,
  sum(sale_sku) AS sale_sku,
  count(DISTINCT goods_code) all_sku,
  --   sum(big_cust_data) AS big_cust_data,
  --   sum(shop_cust_data) AS shop_cust_data,
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
FROM temp.stock_03 a
GROUP BY
  province_code,
  province_name,
  label;
drop table if exists temp.stock_07;
CREATE temporary table temp.stock_07 as
select
  a.province_code,
  a.label,
  shop_dept_cust,
  big_dept_cust,
  round(big_dept_cust / big_cust, 4) sale_cust_ratio,
  big_cust
from (
    select
      province_code,
      label,
      count(
        distinct case
          when channel_name = '商超' then customer_no
        end
      ) as shop_dept_cust,
      count(
        distinct case
          when channel_name = '大' then customer_no
        end
      ) as big_dept_cust
    from temp.stock_02
    group by
      province_code,
      label
  ) a
left join (
    select
      province_code,
      count(
        distinct case
          when channel_name = '大' then customer_no
        end
      ) as big_cust
    from temp.stock_02
    group by
      province_code
  ) b on a.province_code = b.province_code;
-- 商超与大
  drop table if exists temp.stock_08;
CREATE temporary table temp.stock_08 as
SELECT
  province_code,
  province_name,
  channel_name,
  count(
    DISTINCT case
      when sales_value != 0 then goods_code
    end
  ) AS sale_sku,
  count(DISTINCT goods_code) all_sku,
  sum(day_sale) AS day_sale,
  sum(day_profit) AS day_profit,
  sum(sales_value) AS sales_value,
  sum(profit) AS profit,
  '' AS final_qty,
  '' AS final_amt,
  '' AS period_qty,
  '' AS period_amt,
  '' day_turnover,
  sum(negative_sku) as negative_sku,
  '' as receive_amt
FROM (
    SELECT
      province_code,
      province_name,
      goods_code,
      channel_name,
      0 AS negative_sku,
      sum(day_sale) AS day_sale,
      sum(day_profit) AS day_profit,
      sum(sales_value) AS sales_value,
      sum(sales_cost) as sales_cost,
      sum(profit) AS profit
    FROM temp.stock_02
    GROUP BY
      province_code,
      province_name,
      goods_code,
      channel_name
    union all
    SELECT
      province_code,
      province_name,
      goods_code,
      channel_name,
      count(
        DISTINCT case
          when profit < 0 then goods_code
        end
      ) AS negative_sku,
      0 AS day_sale,
      0 AS day_profit,
      0 AS sales_value,
      0 as sales_cost,
      0 AS profit
    FROM (
        select
          province_code,
          province_name,
          goods_code,
          channel_name,
          sum(profit) profit
        from temp.stock_02
        GROUP BY
          province_code,
          province_name,
          goods_code,
          channel_name
      ) a
    GROUP BY
      province_code,
      province_name,
      goods_code,
      channel_name
  ) a
GROUP BY
  province_code,
  province_name,
  a.channel_name;
drop table if exists temp.stock_09;
CREATE temporary table temp.stock_09 as
select
  a.province_code,
  a.channel_name,
  shop_dept_cust,
  big_dept_cust,
  round(big_dept_cust / big_cust, 4) sale_cust_ratio,
  big_cust
from (
    select
      province_code,
      channel_name,
      count(
        distinct case
          when channel_name = '商超' then customer_no
        end
      ) as shop_dept_cust,
      count(
        distinct case
          when channel_name = '大' then customer_no
        end
      ) as big_dept_cust
    from temp.stock_02
    group by
      province_code,
      channel_name
  ) a
left join (
    select
      province_code,
      count(
        distinct case
          when channel_name = '大' then customer_no
        end
      ) as big_cust
    from temp.stock_02
    group by
      province_code
  ) b on a.province_code = b.province_code;
-- 汇总
  --类别销售
select
  no,
  province_code,
  province_name,
  category_code as category_code,
  day_sale / 10000 day_sale,
  day_profit / 10000 day_profit,
  day_profit_rate,
  sales_value / 10000 sales_value,
  profit / 10000 profit,
  profit_rate,
  negative_sku,
  sale_sku,
  all_sku,
  pin_rate,
  final_amt / 10000 final_amt,
  day_turnover,
  receive_amt / 10000 receive_amt,
  shop_dept_cust,
  big_dept_cust,
  sale_cust_ratio
from (
    SELECT
      '1' as no,
      a.province_code,
      a.province_name,
      a.category_code as category_code,
      a.day_sale,
      a.day_profit,
      a.day_profit / a.day_sale day_profit_rate,
      a.sales_value,
      a.profit,
      coalesce(a.profit / a.sales_value, 0) profit_rate,
      a.negative_sku,
      a.sale_sku,
      a.all_sku,
      coalesce(a.sale_sku / a.all_sku, 0) pin_rate,
      a.final_amt,
      a.day_turnover,
      a.receive_amt,
      shop_dept_cust,
      b.big_dept_cust,
      b.sale_cust_ratio
    from temp.stock_04 a
    LEFT JOIN temp.stock_05 b on a.province_code = b.province_code
      and a.category_code = b.category_code
    UNION all
      -- 加工/非加工数据
    SELECT
      '2' as no,
      a.province_code,
      a.province_name,
      a.label as category_code,
      a.day_sale,
      a.day_profit,
      a.day_profit / a.day_sale day_profit_rate,
      a.sales_value,
      a.profit,
      a.profit / a.sales_value profit_rate,
      a.negative_sku,
      a.sale_sku,
      a.all_sku,
      a.sale_sku / a.all_sku pin_rate,
      a.final_amt,
      a.day_turnover,
      a.receive_amt,
      b.shop_dept_cust,
      b.big_dept_cust,
      b.sale_cust_ratio
    from temp.stock_06 a
    LEFT JOIN temp.stock_07 b on a.province_code = b.province_code
      and a.label = b.label
    UNION all
      -- 汇总层
    SELECT
      '3' as no,
      a.province_code,
      province_name,
      '' category_code,
      day_sale,
      day_profit,
      day_profit / day_sale day_profit_rate,
      sales_value,
      profit,
      profit / sales_value as profit_rate,
      negative_sku,
      sale_sku,
      all_sku,
      sale_sku / all_sku as pin_rate,
      final_amt,
      day_turnover,
      receive_amt,
      shop_cust as shop_dept_cust,
      big_cust as big_dept_cust,
      '' sale_cust_ratio
    FROM (
        SELECT
          a.province_code,
          province_name,
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
        FROM temp.stock_03 a
        GROUP BY
          a.province_code,
          province_name
      ) a
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
              when channel_name = '大' then customer_no
            end
          ) as big_cust
        from temp.stock_02
        group by
          province_code
      ) c on a.province_code = c.province_code
    UNION all
      -- 商超/大销售
    SELECT
      '4' as no,
      a.province_code,
      a.province_name,
      a.channel_name as category_code,
      a.day_sale,
      a.day_profit,
      a.day_profit / a.day_sale day_profit_rate,
      a.sales_value,
      a.profit,
      a.profit / a.sales_value profit_rate,
      a.negative_sku,
      a.sale_sku,
      a.all_sku,
      a.sale_sku / a.all_sku pin_rate,
      a.final_amt,
      a.day_turnover,
      a.receive_amt,
      b.shop_dept_cust,
      b.big_dept_cust,
      b.sale_cust_ratio
    from temp.stock_08 a
    LEFT JOIN temp.stock_09 b on a.province_code = b.province_code
      and a.channel_name = b.channel_name
  ) a
order by
  province_code,
  no,
  category_code asc;