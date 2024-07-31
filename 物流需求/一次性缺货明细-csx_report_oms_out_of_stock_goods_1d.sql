-- ******************************************************************** 
-- @功能描述：物流一次性出库率
--            业务：郑晶晶；数据逻辑：彭承华
-- @创建者： 王威 
-- @创建者日期：2023-02-13 10:13:20 
-- @修改者日期：
-- @修改人：
-- @修改内容：单位不一致。销售单位基础出库数量
-- ******************************************************************** 

-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;
SET hive.exec.max.dynamic.partitions=3000; -- 在所有执行MR的节点上，最大一共可以创建多少个动态分区
SET hive.exec.max.dynamic.partitions.pernode=3000; -- 在每个执行MR的节点上，最大可以创建多少个动态分区


-- 确定排线
with csx_tmp_tms_order_route as
(
  select
    a.dc_code,
    entrucking_code,
    shipped_order_code,
    a.send_date,
    a.route_id,
    max_create_time
  from
    (
      select
        dc_code,
        send_date,
        entrucking_code,
        shipped_order_code,
        route_id
      from
        csx_dwd.csx_dwd_tms_sign_shipped_order_detail_di
      where
        sdt >= ${s_100days_ago}
        and shipped_type_code = '1'
      group by
        dc_code,
        send_date,
        entrucking_code,
        shipped_order_code,
        route_id
    ) a
    left join (
      select
        warehouse_code,
        route_id,
        max(create_time) max_create_time
      from
        csx_dwd.csx_dwd_tms_route_carrier_log_di
      where
        sdt >= ${s_100days_ago}
        and operator = '排线确认'
      group by
        route_id,
        warehouse_code
    ) b on a.dc_code = b.warehouse_code
    and a.route_id = b.route_id
),


-- 按规则找出销售单
csx_tmp_sale_order as
(
  SELECT
    inventory_dc_code,
    inventory_dc_name,
    customer_code,
    order_code,
    goods_code,
    sale_unit,
    basic_unit,
    purchase_unit_rate,
    sum(sale_unit_purchase_qty) AS sale_unit_purchase_qty,
    sum(basic_unit_purchase_qty) AS basic_unit_purchase_qty,
    max(is_unit_conversion) AS is_unit_conversion,
    sum(sale_unit_send_qty) sale_unit_send_qty,
    sum(send_qty) send_qty,
    is_plan_customer_flag,
    is_plan_flag
  FROM
    (
      SELECT
        inventory_dc_code,
        inventory_dc_name,
        customer_code,
        order_code,
        item_code,
        goods_code,
        goods_name,
        -- 销售单位
        purchase_unit_name AS sale_unit,
        -- 销售单位下单数量
        purchase_qty AS sale_unit_purchase_qty,
        -- 单位换算比例
        purchase_unit_rate,
        -- 是否单位换算
        if(purchase_unit_rate <> 1, 1, 0) AS is_unit_conversion,
        -- 基础单位
        unit_name AS basic_unit,
        -- 基础单位下单数量
        purchase_qty * purchase_unit_rate AS basic_unit_purchase_qty,
        --  基础单位发货数量
        send_qty,
        -- 销售单位数量
        sale_unit_send_qty,
        if(unit_name = purchase_unit_name, 1, 0) as unit_conversion_flag,
        is_plan_customer_flag,
        is_plan_flag
      FROM
        csx_dwd.csx_dwd_oms_sale_order_detail_di
      WHERE
        sdt >= ${s_100days_ago}
        AND order_channel_code = 1 -- B端
        AND order_channel_detail_code <> 13 -- 非红旗
        AND order_type_code = 1 -- 正常销售单
        AND order_business_type_code = 1 -- 日配
        AND delivery_type_code = 1 -- 配送
        AND partner_type_code = 0 -- 非合伙人
        AND order_status_code IN (30, 40, 50, 60, 70) -- 30-部分发货  40-配送中  50-待确认 60-已签收  70-已完成
    ) a
    JOIN (
      -- 外部城市服务商DC，需要过滤
      SELECT
        shop_code
      FROM
        csx_dim.csx_dim_shop
      WHERE
        sdt = 'current'
        AND purpose <> '09'
    ) b ON a.inventory_dc_code = b.shop_code
  GROUP BY
    inventory_dc_code,
    inventory_dc_name,
    customer_code,
    order_code,
    goods_code,
    sale_unit,
    basic_unit,
    purchase_unit_rate,
    is_plan_customer_flag,
    is_plan_flag
),


-- 按规则找出包裹单
csx_tmp_pakcage as
(
  SELECT
    a.delivery_date,
    a.sale_order_code,
    a.package_code,
    a.entrucking_code,
    a.goods_code,
    a.send_quantity,
    a.basic_unit,
    a.send_qty,
    cn,
    aa,
    case
      when cn = 1
      and aa >= 1 then 1
      else 0 end AS is_first_entrucking_code -- 是否首次发车单
  FROM
    (
      SELECT
        sale_order_code,
        package_code,
        entrucking_code,
        regexp_replace(substr(shipped_time, 1, 10), '-', '') AS delivery_date,
        goods_code,
        unit_name AS basic_unit,
        -- 基础单位
        sum(sale_unit_send_qty) AS send_quantity,
        -- 销售单位出库数量
        sum(send_qty) AS send_qty -- 基础单位出库数量
      FROM
        csx_dwd.csx_dwd_oms_package_order_detail_di
      WHERE
        sdt >= ${s_100days_ago}
        AND delivery_type_code = 1 -- 配送
      group by
        sale_order_code,
        package_code,
        entrucking_code,
        regexp_replace(substr(shipped_time, 1, 10), '-', ''),
        goods_code,
        unit_name
    ) a
    LEFT JOIN (
      -- 取TMS销售出库单号根据排线ID，按照更新时间，如果同一时间，未缺货 cn=1且aa>1 属于同单不同车，cn=1且aa=1属于同车同单
      select
        sale_order_code,
        entrucking_code,
        delivery_date,
        max(aa) as aa,
        max(cn) as cn
        -- ,
        -- 下面union all有重复数据，做一下过滤
        -- row_number() over(partition by sale_order_code, delivery_date, entrucking_code order by tab) as rank
      from
        (
          select
            shipped_order_code as sale_order_code,
            entrucking_code,
            regexp_replace(send_date, '-', '') delivery_date,
            count(max_create_time) as cn,
            count(a.route_id) aa,
            0 as tab
          from
            csx_tmp_tms_order_route a
          group by
            entrucking_code,
            shipped_order_code,
            regexp_replace(send_date, '-', '')
          union all
          SELECT
            sale_order_code,
            entrucking_code,
            delivery_date,
            1 cn,
            1 aa,
            1 as tab
          FROM
            (
              SELECT
                sale_order_code,
                entrucking_code,
                row_number() OVER(
                  PARTITION BY sale_order_code
                  ORDER BY
                    shipped_time
                ) AS rank,
                min(regexp_replace(substr(shipped_time, 1, 10), '-', '')) OVER(PARTITION BY sale_order_code) AS delivery_date
              FROM
                csx_dwd.csx_dwd_oms_package_order_detail_di -- 这个表只有一个包裹单号，缺省多条数据
              WHERE
                sdt >= ${s_100days_ago}
                AND delivery_type_code = 1 -- 配送
            ) tmp
          WHERE
            rank = 1
        ) a group by sale_order_code, entrucking_code, delivery_date
    ) c ON a.sale_order_code = c.sale_order_code
    AND a.delivery_date = c.delivery_date
    AND a.entrucking_code = c.entrucking_code
    -- AND c.rank = 1
),


csx_tmp_normal_sale_order_join_pakcage AS
(
  SELECT
    t1.inventory_dc_code,
    t1.inventory_dc_name,
    t1.customer_code,
    t1.order_code,
    t1.goods_code,
    t1.sale_unit_purchase_qty,
    t1.basic_unit_purchase_qty,
    t1.is_unit_conversion,
    t2.delivery_date,
    t2.package_code,
    t2.entrucking_code,
    t1.sale_unit,
    -- 销售单位出库数量
    t2.send_quantity,
    t2.basic_unit,
    -- 基础单位出库数量
    t2.send_qty,
    -- 是否首次发车单
    t2.is_first_entrucking_code,
    t1.is_plan_customer_flag,
    t1.is_plan_flag
  FROM csx_tmp_sale_order t1
    left join (
      select
        sale_order_code,
        goods_code,
        concat_ws(',', collect_set(delivery_date)) as delivery_date,
        concat_ws(',', collect_set(package_code)) as package_code,
        concat_ws(',', collect_set(entrucking_code)) as entrucking_code,
        sum(send_qty) as send_qty,
        sum(send_quantity) as send_quantity,
        concat_ws(',', collect_set(basic_unit)) as basic_unit,
        concat_ws(',', collect_set(cast(is_first_entrucking_code as string))) as is_first_entrucking_code
        -- ,
        -- row_number() over(partition by sale_order_code, goods_code order by entrucking_code) as rank
      from
        csx_tmp_pakcage
      group by sale_order_code, goods_code
    ) t2 ON t1.order_code = t2.sale_order_code
    AND t1.goods_code = t2.goods_code
  where
    t2.delivery_date is not null
    -- and t2.rank = 1
)


INSERT OVERWRITE TABLE csx_report.csx_report_oms_out_of_stock_goods_1d PARTITION(sdt)
SELECT
  concat_ws('&', delivery_date, package_code, a.goods_code) AS biz_id,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  order_code,
  package_code,
  a.customer_code,
  customer_name,
  delivery_date,
  a.inventory_dc_code,
  inventory_dc_name,
  business_division_name,
  purchase_group_code,
  purchase_group_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  a.goods_code,
  goods_name,
  basic_unit,
  sale_unit,
  purchase_qty,
  send_qty,
  send_qty - purchase_qty as out_of_stock_qty,
  spec,
  case
    when sale_unit in (
      '市斤',
      '公斤',
      '斤',
      'kg',
      'kG',
      'Kg',
      'KG',
      'g',
      'G',
      '千克',
      '克',
      '两'
    )
    and (send_qty - purchase_qty) / purchase_qty < -0.05 then 1
    when sale_unit not in (
      '市斤',
      '公斤',
      '斤',
      'kg',
      'kG',
      'Kg',
      'KG',
      'g',
      'G',
      '千克',
      '克',
      '两'
    )
    and send_qty < purchase_qty then 1
    else 0 end as is_out_of_stock,
  if(d.inventory_dc_code is not null, 1, 0) as is_base_goods,
  entrucking_code,
  is_plan_customer_flag,
  is_plan_flag,
  delivery_date AS sdt
FROM
  (
    SELECT
      inventory_dc_code,
      inventory_dc_name,
      delivery_date,
      customer_code,
      order_code,
      package_code,
      entrucking_code,
      goods_code,
      sale_unit,
      basic_unit,
      is_unit_conversion,
      is_first_entrucking_code,
      sale_unit_purchase_qty,
      basic_unit_purchase_qty,
      send_quantity,
      send_qty AS send_qty_1,
      -- case when is_unit_conversion = 1 then sale_unit_purchase_qty
      --     when is_unit_conversion = 0 and basic_unit!=unit_name then sale_unit_purchase_qty
      --     else basic_unit_purchase_qty end  AS purchase_qty,
      case
        when basic_unit in (
          '市斤',
          '公斤',
          '斤',
          'kg',
          'kG',
          'Kg',
          'KG',
          'g',
          'G',
          '千克',
          '克',
          '两'
        ) then sale_unit_purchase_qty
        when is_unit_conversion = 1
        and send_quantity <> 0 then sale_unit_purchase_qty
        else basic_unit_purchase_qty
      end AS purchase_qty,
      -- 单位转换 取销售单位订单数量否则取基础单位订单数量
      -- if(is_first_entrucking_code = 1, if(is_unit_conversion = 1  AND send_quantity <> 0, send_quantity, send_qty   ),  0 ) AS send_qty
      --   case when is_unit_conversion = 1 and send_quantity<>0 then send_quantity
      --         when  is_unit_conversion = 0 and basic_unit!=sale_unit and send_quantity<>0 then send_quantity
      --         else send_qty end send_qty
      case
        when basic_unit in (
          '市斤',
          '公斤',
          '斤',
          'kg',
          'kG',
          'Kg',
          'KG',
          'g',
          'G',
          '千克',
          '克',
          '两'
        )
        and send_quantity <> 0 then send_quantity
        when is_unit_conversion = 1
        and send_quantity <> 0 then send_quantity
        when is_unit_conversion = 0
        and basic_unit != sale_unit
        and send_quantity <> 0 then send_quantity
        else send_qty
      end send_qty, -- basic_unit_purchase_qty AS purchase_qty,
      -- if(is_first_entrucking_code = 1, send_qty, 0) AS send_qty
      is_plan_customer_flag,
      is_plan_flag
    FROM
      csx_tmp_normal_sale_order_join_pakcage
    where delivery_date >= ${s_last_month_start}
  ) a
  JOIN (
    -- 只取大客户
    SELECT
      customer_code,
      customer_name,
      performance_region_code,
      performance_region_name,
      performance_province_code,
      performance_province_name,
      performance_city_code,
      performance_city_name
    FROM
      csx_dim.csx_dim_crm_customer_info
    WHERE
      sdt = 'current'
      AND customer_code <> ''
      AND channel_code = '1' -- 大客户
  ) b ON a.customer_code = b.customer_code
  left join (
    SELECT
      goods_code,
      goods_name,
      standard as spec,
      business_division_name,
      division_code,
      division_name,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
      purchase_group_code,
      purchase_group_name
    FROM
      csx_dim.csx_dim_basic_goods
    WHERE
      sdt = 'current'
  ) c on a.goods_code = c.goods_code
  left join (
    select
      inventory_dc_code,
      product_code
    from
      csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
    where
      sdt = regexp_replace(date_sub(current_date(), 1), '-', '')
      and base_product_tag = 1
  ) d on a.inventory_dc_code = d.inventory_dc_code
  and a.goods_code = d.product_code;
