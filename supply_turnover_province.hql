CREATE TABLE IF NOT EXISTS csx_dw.supply_turnover_province
(
years string comment '年份',
months string comment '月份',
prov_code string comment '省区编码' ,
prov_name string comment '省区名称' ,
bd_id string comment '采购部编码' ,
bd_name string comment '采购部名称' ,
dept_id string comment '课组编码' ,
dept_name string comment '省区编码' ,
sales_qty decimal(26,6) comment '销量' ,
sales_value decimal(26,6) comment '销额',
profit decimal(26,6) comment '毛利额',
profit_rate decimal(26,6) comment '毛利率',
sales_cost decimal(26,6) comment '销售成本',
period_inv_amt decimal(26,6) comment '期间库存额',
final_amt decimal(26,6) comment '期末额',
final_qty decimal(26,6) comment '期末量',
days_turnover decimal(26,6) comment '周转天数',
goods_sku decimal(26,6) comment 'SKU数',
sale_sku decimal(26,6) comment '动销数',
pin_rate decimal(26,6) comment '动销率',
negative_inventory decimal(26,6) comment '负库存数',
negative_amt decimal(26,6) comment '负库存额',
highet_sku decimal(26,6) comment '高库存数',
highet_amt decimal(26,6) comment '高库存额',
no_sale_sku decimal(26,6) comment '未销售SKU',      
no_sale_amt decimal(26,6) comment '未销售SKU库存额'
)comment '供应链省区汇总'
partitioned by (sdt string comment '日期分区')


;


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
--   drop table if exists csx_dw.supply_inve_00;
create temporary table if not exists csx_dw.supply_inve_00 as
SELECT
dc_type runtype,
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
  max(a.max_sale_sdt) as max_sale_sdt,
 coalesce( datediff(
    to_date(date_sub(current_timestamp(), 1)),
    from_unixtime(
      unix_timestamp(max(a.max_sale_sdt), 'yyyyMMdd'),
      'yyyy-MM-dd'
    )
  ),'') as no_sale_days,
  nvl( max(a.entry_sdt),'') as entry_sdt,
 coalesce( datediff(
    to_date(date_sub(current_timestamp(), 1)),
    from_unixtime(
      unix_timestamp(max(a.entry_sdt), 'yyyyMMdd'),
      'yyyy-MM-dd'
    )
  ),'') as entry_days
FROM csx_dw.supply_turnover a
WHERE
  sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 1)), '-', '')
group by
  a.goodsid,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  dc_type;
  
  
  
drop table if exists csx_dw.supply_inve_01;
create temporary table if not exists csx_dw.supply_inve_01 as
SELECT
  dc_type runtype,
  case
    when a.shop_id = 'W0H4' then 'W0H4'
    else prov_code
  end prov_code,
  case
    when a.shop_id = 'W0H4' then '供应链平台'
    else prov_name
  end prov_name,
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
  round(case when SUM(sales_cost)=0 then 999 else 
    coalesce(SUM(period_inv_amt) / SUM(sales_cost),0) end ,
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
    nvl( max(a.entry_sdt),'') as entry_sdt,
 coalesce( datediff(
    to_date(date_sub(current_timestamp(), 1)),
    from_unixtime(
      unix_timestamp(max(a.entry_sdt), 'yyyyMMdd'),
      'yyyy-MM-dd'
    )
  ),'') as entry_days
FROM csx_dw.supply_turnover a
WHERE
  sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 1)), '-', '')
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
 dc_type;
 
  -- 食百全国数据
 insert overwrite table csx_dw.supply_turnover_province partition (sdt)
select
  bd_id as type,
  substr(
    regexp_replace(date_sub(current_date(), 1), '-', ''),
    1,
    4
  ) years,
  substr(
    regexp_replace(date_sub(current_date(), 1), '-', ''),
    1,
    6
  ) months,
  prov_code,
  prov_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  sales_qty / 10000 as sales_qty,
  sales_value / 10000 as sales_value,
  profit / 10000 as profit,
  COALESCE(profit / sales_value, 0) * 1.00 AS profit_rate,
  sales_cost / 10000 as sales_cost,
  period_inv_amt / 10000 as period_inv_amt,
  final_amt / 10000 as final_amt,
  final_qty / 10000 final_qty,
  days_turnover,
  goods_sku,
  sale_sku,
  round(sale_sku / goods_sku, 4) * 1.00 pin_rate,
  negative_inventory,
  negative_amt / 10000 as negative_amt,
  highet_sku,
  highet_amt / 10000 as highet_amt,
  no_sale_sku,
  no_sale_amt / 10000 as no_sale_amt,
  regexp_replace(date_sub(current_date(), 1), '-', '') sdt
from (
    select
      '00' prov_code,
      '全国' prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end , 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or final_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN (
            days_turnover > 30
            AND final_amt > 2000
            AND a.dept_id IN (
              'A01',
              'A02',
              'A03',
              'A04',
              'A10'
            )
			and entry_days>7
          ) THEN goodsid
          WHEN (
            days_turnover > 45
            AND final_amt > 2000
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
		  WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN (
            days_turnover > 30
            AND final_amt > 2000
            AND a.dept_id IN (
              'A01',
              'A02',
              'A03',
              'A04',
              'A10'
            )
			and entry_days>7
          ) THEN final_amt
          WHEN (
            days_turnover > 45
            AND final_amt > 2000
            AND a.dept_id IN (
              'A05',
              'A06',
              'A07',
              'A08',
              'A09',
              'P01',
              'P10'
            )
			and entry_days>7
          ) THEN final_amt
		  WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when no_sale_days > 30 entry_days>3
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days> 30 entry_days>3
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_00 a
    where
     1=1
group by  bd_id,
      bd_name
    union all
      -- 全国课组情况
    select
      '00' AS prov_code,
      '全国' AS prov_name,
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
  round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
  COUNT(case when a.sales_value <> 0 or final_amt !=0 then  goodsid end) goods_sku,
  COUNT (
    CASE
      WHEN a.sales_value <> 0 THEN goodsid
    END
  ) sale_sku,
  COUNT (
    CASE
      WHEN a.final_amt < 0 THEN goodsid
    END
  ) negative_inventory,
  SUM (
    CASE
      WHEN final_amt < 0 THEN final_amt
    END
  ) negative_amt,
  COUNT (
    CASE
      WHEN (
        days_turnover > 30
        AND final_amt > 2000
        AND a.dept_id IN (
          'A01',
          'A02',
          'A03',
          'A04',
          'A10'
        )
		and entry_days>7
      ) THEN goodsid
      WHEN (
        days_turnover > 45
        AND final_amt > 2000
        AND a.dept_id IN (
          'A05',
          'A06',
          'A07',
          'A08',
          'A09',
          'P01',
          'P10'
        )
		and entry_days>7
      ) THEN goodsid
	  WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN final_amt
    END
  ) highet_sku,
  SUM (
    CASE
      WHEN (
        days_turnover > 30
        AND final_amt > 2000
        AND a.dept_id IN (
          'A01',
          'A02',
          'A03',
          'A04',
          'A10'
        )
		and entry_days>7
      ) THEN final_amt
      WHEN (
        days_turnover > 45
        AND final_amt > 2000
        AND a.dept_id IN (
          'A05',
          'A06',
          'A07',
          'A08',
          'A09',
          'P01',
          'P10'
        )
		and entry_days>7
      ) THEN final_amt
	  WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN final_amt
    END
  ) highet_amt,
  count (
    case
      when no_sale_days > 30 entry_days>3
      and final_amt > 0 then goodsid
    end
  ) as no_sale_sku,
  sum (
    case
      when no_sale_days > 30 and entry_days>3
      and final_amt > 0 then final_amt
    end
  ) as no_sale_amt
from csx_dw.supply_inve_00 a
where
  1=1
group by
  dept_id,
  dept_name,
  bd_id,
  bd_name
union all
  --省份明细
select
  prov_code,
  prov_name,
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
  round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
  COUNT(case when a.sales_value <> 0 or final_amt !=0 then  goodsid end) goods_sku,
  COUNT (
    CASE
      WHEN a.sales_value <> 0 THEN goodsid
    END
  ) sale_sku,
  COUNT (
    CASE
      WHEN a.final_amt < 0 THEN goodsid
    END
  ) negative_inventory,
  SUM (
    CASE
      WHEN final_amt < 0 THEN final_amt
    END
  ) negative_amt,
  COUNT (
    CASE
      WHEN (
        days_turnover > 30
        AND final_amt > 2000
        AND a.dept_id IN (
          'A01',
          'A02',
          'A03',
          'A04',
          'A10'
        )
		and entry_days>7
      ) THEN goodsid
      WHEN (
        days_turnover > 45
        AND final_amt > 2000
        AND a.dept_id IN (
          'A05',
          'A06',
          'A07',
          'A08',
          'A09',
          'P01',
          'P10'
        )
		and entry_days>7
      ) THEN goodsid
	  WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN final_amt
    END
  ) highet_sku,
  SUM (
    CASE
      WHEN (
        days_turnover > 30
        AND final_amt > 2000
        AND a.dept_id IN (
          'A01',
          'A02',
          'A03',
          'A04',
          'A10'
        )
		and entry_days>7
      ) THEN final_amt
      WHEN (
        days_turnover > 45
        AND final_amt > 2000
        AND a.dept_id IN (
          'A05',
          'A06',
          'A07',
          'A08',
          'A09',
          'P01',
          'P10'
        )
		and entry_days>7
      ) THEN final_amt
	  WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN final_amt
    END
  ) highet_amt,
  count (
    case
      when no_sale_days> 30 and entry_days>3
      and final_amt > 0 then goodsid
    end
  ) as no_sale_sku,
  sum (
    case
      when no_sale_days > 30 and entry_days>3
      and final_amt > 0 then final_amt
    end
  ) as no_sale_amt
from csx_dw.supply_inve_01 a
where
 1=1
group by
  prov_code,
  prov_name,
  dept_id,
  dept_name,
  bd_id,
  bd_name
union all
  --省份课组汇总
select
  prov_code,
  prov_name,
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
  round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
  COUNT(case when a.sales_value <> 0 or final_amt !=0 then  goodsid end) goods_sku,
  COUNT (
    CASE
      WHEN a.sales_value <> 0 THEN goodsid
    END
  ) sale_sku,
  COUNT (
    CASE
      WHEN a.final_amt < 0 THEN goodsid
    END
  ) negative_inventory,
  SUM (
    CASE
      WHEN final_amt < 0 THEN final_amt
    END
  ) negative_amt,
  COUNT (
    CASE
      WHEN (
        days_turnover > 30
        AND final_amt > 2000
        AND a.dept_id IN (
          'A01',
          'A02',
          'A03',
          'A04',
          'A10'
        )
		and entry_days>7
      ) THEN goodsid
      WHEN (
        days_turnover > 45
        AND final_amt > 2000
        AND a.dept_id IN (
          'A05',
          'A06',
          'A07',
          'A08',
          'A09',
          'P01',
          'P10'
        )
		and entry_days>7
      ) THEN goodsid
	  WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN goodsid
    END
  ) highet_sku,
  SUM (
    CASE
      WHEN (
        days_turnover > 30
        AND final_amt > 2000
        AND a.dept_id IN (
          'A01',
          'A02',
          'A03',
          'A04',
          'A10'
        )
		and entry_days>7
      ) THEN final_amt
      WHEN (
        days_turnover > 45
        AND final_amt > 2000
        AND a.dept_id IN (
          'A05',
          'A06',
          'A07',
          'A08',
          'A09',
          'P01',
          'P10'
        )
		and entry_days>7
      ) THEN final_amt
	 WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN final_amt
    END
  ) highet_amt,
  count (
    case
      when a.no_sale_days > 30
      and final_amt > 0  and entry_days>3 then goodsid
    end
  ) as no_sale_sku,
  sum (
    case
      when no_sale_days > 30
      and final_amt > 0 and entry_days>3 then final_amt
    end
  ) as no_sale_amt
from csx_dw.supply_inve_01 a
where
  1=1
group by
  prov_code,
  prov_name,
  bd_id,
  bd_name
union all
  --省份课组汇总
select
  prov_code,
  prov_name,
  bd_id,
  '小计' bd_name,
  '00' dept_id,
  '小计' dept_name,
  SUM(a.sales_qty) sales_qty,
  SUM(a.sales_value) sales_value,
  SUM(a.profit) profit,
  sum(sales_cost) sales_cost,
  SUM(a.period_inv_amt) period_inv_amt,
  SUM(a.final_amt) final_amt,
  SUM(final_qty) final_qty,
  round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
  COUNT(case when a.sales_value <> 0 or final_amt !=0 then  goodsid end) goods_sku,
  COUNT (
    CASE
      WHEN a.sales_value <> 0 THEN goodsid
    END
  ) sale_sku,
  COUNT (
    CASE
      WHEN a.final_amt < 0 THEN goodsid
    END
  ) negative_inventory,
  SUM (
    CASE
      WHEN final_amt < 0 THEN final_amt
    END
  ) negative_amt,
  COUNT (
    CASE
      WHEN (
        days_turnover > 30
        AND final_amt > 2000
        AND a.dept_id IN (
          'A01',
          'A02',
          'A03',
          'A04',
          'A10'
        )
		and entry_days>7
      ) THEN goodsid
      WHEN (
        days_turnover > 45
        AND final_amt > 2000
        AND a.dept_id IN (
          'A05',
          'A06',
          'A07',
          'A08',
          'A09',
          'P01',
          'P10'
        )
		and entry_days>7
      ) THEN goodsid
	   WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN goodsid		  
    END
  ) highet_sku,
  SUM (
    CASE
      WHEN (
        days_turnover > 30
        AND final_amt > 2000
        AND a.dept_id IN (
          'A01',
          'A02',
          'A03',
          'A04',
          'A10'
        )
		and entry_days>7
      ) THEN final_amt
      WHEN (
        days_turnover > 45
        AND final_amt > 2000
        AND a.dept_id IN (
          'A05',
          'A06',
          'A07',
          'A08',
          'A09',
          'P01',
          'P10'
        )
		and entry_days>7
      ) THEN final_amt
	  WHEN (days_turnover > 15 and entry_days>3
          AND final_amt > 500 and bd_id='11') THEN final_amt	
    END
  ) highet_amt,
  count (
    case
      when a.no_sale_days > 30 and entry_days>3
      and final_amt > 0 then goodsid
    end
  ) as no_sale_sku,
  sum (
    case
      when no_sale_days > 30 and entry_days>3
      and final_amt > 0 then final_amt
    end
  ) as no_sale_amt
from csx_dw.supply_inve_01 a
where
 1=1
group by
  prov_code,
  prov_name,
  bd_id
) a --group by prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name
order by
  prov_code,
  bd_id,
  dept_id;
  
  -- 插入生鲜周转
  
 insert into table csx_dw.supply_turnover_province partition (sdt)
select
  '11' as type,
  substr(
    regexp_replace(date_sub(current_date(), 1), '-', ''),
    1,
    4
  ) years,
  substr(
    regexp_replace(date_sub(current_date(), 1), '-', ''),
    1,
    6
  ) months,
  prov_code,
  prov_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  sales_qty / 10000 as sales_qty,
  sales_value / 10000 as sales_value,
  profit / 10000 as profit,
  COALESCE(profit / sales_value, 0) * 1.00 AS profit_rate,
  sales_cost / 10000 as sales_cost,
  period_inv_amt / 10000 as period_inv_amt,
  final_amt / 10000 as final_amt,
  final_qty / 10000 final_qty,
  days_turnover,
  goods_sku,
  sale_sku,
  round(sale_sku / goods_sku, 4) * 1.00 pin_rate,
  negative_inventory,
  negative_amt / 10000 as negative_amt,
  highet_sku,
  highet_amt / 10000 as highet_amt,
  no_sale_sku,
  no_sale_amt / 10000 as no_sale_amt,
  regexp_replace(date_sub(current_date(), 1), '-', '') sdt
from (
    select
      '00' prov_code,
      '全国' prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_00 a
    where
      bd_id = '11'
      group by  bd_id,
      bd_name
    union all
      -- 全国课组情况
    select
      '00' AS prov_code,
      '全国' AS prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_00 a
    where
      bd_id = '11'
    group by
      dept_id,
      dept_name,
      bd_id,
      bd_name
    union all
      --省份明细
    select
      prov_code,
      prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_01 a
    where
      bd_id = '11'
    group by
      prov_code,
      prov_name,
      dept_id,
      dept_name,
      bd_id,
      bd_name
    union all
      --省份课组汇总
    select
      prov_code,
      prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when a.no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_01 a
    where
      bd_id = '11'
    group by
      prov_code,
      prov_name,
      bd_id,
      bd_name
    union all
      --省份课组汇总
    select
      prov_code,
      prov_name,
      '00' bd_id,
      '小计' bd_name,
      '00' dept_id,
      '小计' dept_name,
      SUM(a.sales_qty) sales_qty,
      SUM(a.sales_value) sales_value,
      SUM(a.profit) profit,
      sum(sales_cost) sales_cost,
      SUM(a.period_inv_amt) period_inv_amt,
      SUM(a.final_amt) final_amt,
      SUM(final_qty) final_qty,
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when a.no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_01 a
    where
      bd_id = '11'
    group by
      prov_code,
      prov_name
  ) a --group by prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name
order by
  prov_code,
  bd_id,
  dept_id;
  
  
  
--插入联营小店
insert into table csx_dw.supply_turnover_province partition (sdt)
select
  'E' as type,
  substr(
    regexp_replace(date_sub(current_date(), 1), '-', ''),
    1,
    4
  ) years,
  substr(
    regexp_replace(date_sub(current_date(), 1), '-', ''),
    1,
    6
  ) months,
  prov_code,
  prov_name,
  bd_id,
  bd_name,
  dept_id,
  dept_name,
  sales_qty / 10000 as sales_qty,
  sales_value / 10000 as sales_value,
  profit / 10000 as profit,
  COALESCE(profit / sales_value, 0) * 1.00 AS profit_rate,
  sales_cost / 10000 as sales_cost,
  period_inv_amt / 10000 as period_inv_amt,
  final_amt / 10000 as final_amt,
  final_qty / 10000 as final_qty,
  days_turnover,
  goods_sku,
  sale_sku,
  round(sale_sku / goods_sku, 4) * 1.00 pin_rate,
  negative_inventory,
  negative_amt / 10000 as negative_amt,
  highet_sku,
  highet_amt / 10000 as highet_amt,
  no_sale_sku,
  no_sale_amt / 10000 as no_sale_amt,
  regexp_replace(date_sub(current_date(), 1), '-', '') sdt
from (
    select
      '00' prov_code,
      '全国' prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end ) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_00 a
    where
      bd_id = '11'
     and runtype = '寄售门店'
      group by  bd_id,
      bd_name
    union all
      -- 全国课组情况
    select
      '00' AS prov_code,
      '全国' AS prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_00 a
    where
      bd_id = '11'
    and runtype = '寄售门店'
    group by
      dept_id,
      dept_name,
      bd_id,
      bd_name
    union all
      --省份明细
    select
      prov_code,
      prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days> 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_01 a
    where
      bd_id = '11'
      and runtype = '寄售门店'
    group by
      prov_code,
      prov_name,
      dept_id,
      dept_name,
      bd_id,
      bd_name
    union all
      --省份课组汇总
    select
      prov_code,
      prov_name,
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
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when a.no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_01 a
    where
      bd_id = '11'
      and runtype = '寄售门店'
    group by
      prov_code,
      prov_name,
      bd_id,
      bd_name
    union all
      --省份课组汇总
    select
      prov_code,
      prov_name,
      '00' bd_id,
      '小计' bd_name,
      '00' dept_id,
      '小计' dept_name,
      SUM(a.sales_qty) sales_qty,
      SUM(a.sales_value) sales_value,
      SUM(a.profit) profit,
      sum(sales_cost) sales_cost,
      SUM(a.period_inv_amt) period_inv_amt,
      SUM(a.final_amt) final_amt,
      SUM(final_qty) final_qty,
      round(case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end, 2) AS days_turnover,
      COUNT(case when a.sales_value <> 0 or period_inv_amt !=0 then  goodsid end) goods_sku,
      COUNT (
        CASE
          WHEN a.sales_value <> 0 THEN goodsid
        END
      ) sale_sku,
      COUNT (
        CASE
          WHEN a.final_amt < 0 THEN goodsid
        END
      ) negative_inventory,
      SUM (
        CASE
          WHEN final_amt < 0 THEN final_amt
        END
      ) negative_amt,
      COUNT (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_sku,
      SUM (
        CASE
          WHEN days_turnover > 15 and entry_days>3
          AND final_amt > 500 THEN final_amt
        END
      ) highet_amt,
      count (
        case
          when a.no_sale_days > 30
          and final_amt > 0 then goodsid
        end
      ) as no_sale_sku,
      sum (
        case
          when no_sale_days > 30
          and final_amt > 0 then final_amt
        end
      ) as no_sale_amt
    from csx_dw.supply_inve_01 a
    where
      bd_id = '11'
      and runtype = '寄售门店'
    group by
      prov_code,
      prov_name
  ) a --group by prov_code ,prov_name,bd_id,bd_name,dept_id,dept_name
order by
  prov_code,
  bd_id,
  dept_id;

--插入省区合计
insert into table csx_dw.supply_turnover_province partition (sdt)

 select 
 '00'type,
  substr(
    regexp_replace(date_sub(current_date(), 1), '-', ''), 1,4 ) years,
  substr(
    regexp_replace(date_sub(current_date(), 1), '-', ''), 1,6 ) months,
	prov_code,
	prov_name,
	'00'bd_id,
	'合计'bd_name,
	''dept_id,
	''dept_name,
	sum(sales_qty)as sales_qty,
	sum(sales_value)as sales_value,
	sum(profit)as profit,
	sum(profit)/sum(sales_value) as profit_rate,
	sum(sales_cost)as sales_cost,
	sum(period_inv_amt)as period_inv_amt,
	sum(final_amt)as final_amt,
	sum(final_qty)as final_qty,
	case when SUM(sales_cost)=0 then 999 else  SUM(period_inv_amt) / SUM(sales_cost) end as days_turnover,
	sum(goods_sku)as goods_sku,
	sum(sale_sku)as sale_sku,
	sum(sale_sku)/sum(goods_sku) as pin_rate,
	sum(negative_inventory)negative_inventory,
	sum(negative_amt)negative_amt,
	sum(highet_sku)highet_sku,
	sum(highet_amt)highet_amt,
	sum(no_sale_sku)no_sale_sku,
	sum(no_sale_amt)no_sale_amt,
  regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(), 1)), '-', '') sdt
from
	csx_dw.supply_turnover_province
where
	sdt=regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),1)),'-','')
	--and prov_code='500000'
--	and type !='E'
	and bd_id='00'
	group by prov_code,
	prov_name
;
