 -- 物流一次性出库率
 -- 找出符合条件的包裹单
 -- 在tms_route_item同DC内同子客户编码同发运日期的排线线路有几条，根据线路id查询排线确认时间一致的
 -- 根据排线ID查询确认时间
    SELECT
      a.delivery_date, 
      a.sale_order_code, 
      a.entrucking_code, 
      a.package_code,
      a.goods_code, 
      a.send_quantity, 
      a.basic_unit, 
      a.send_qty,
      if(c.sale_order_code IS NOT NULL, 1, 0) AS is_first_entrucking_code -- 是否首次发车单
    FROM
    (
      SELECT
        package_code, 
        sale_order_code, 
        entrucking_code, 
        regexp_replace(substr(shipped_time, 1, 10), '-', '') AS delivery_date,
        goods_code, 
        sale_unit_send_qty AS send_quantity, -- 销售单位出库数量
        unit_name AS basic_unit, -- 基础单位
        send_qty -- 基础单位出库数量
      FROM  csx_dwd.csx_dwd_oms_package_order_detail_di
      WHERE sdt >='20230301'
        AND delivery_type_code = 1 -- 配送
    ) a LEFT JOIN
    (
      SELECT
        sale_order_code, 
        entrucking_code, 
        delivery_date
      FROM
      (
    --     SELECT
    --       sale_order_code, 
    --       entrucking_code,
    --       row_number() OVER(PARTITION BY sale_order_code ORDER BY shipped_time) AS rank,
    --       min(regexp_replace(substr(shipped_time, 1, 10), '-', '')) OVER(PARTITION BY sale_order_code) AS delivery_date
    --     FROM csx_dwd.csx_dwd_oms_package_order_detail_di
    --     WHERE sdt >= ${s_100days_ago} 
    --       AND delivery_type_code = 1 -- 配送
    --   ) tmp WHERE rank = 1
    --   union all 
    -- 取TMS销售出库单号根据排线ID，按照更新时间，如果同一时间，未缺货 cn=1且aa>1 属于同单不同车，cn=1且aa=1属于同车同单
        select 
                shipped_order_code sale_order_code,
                entrucking_code,
                send_date delivery_date,
                aa,
                cn
               from  csx_dws.csx_dws_tms_entrucking_order_product_detail_di a
               join 
              (select  order_code,
                customer_code,
                send_date,
                count(b.max_create_time) as cn,
                count(a.route_id) aa
               from  csx_dwd.csx_dwd_tms_route_item_di  a 
             left  join 
              (select route_id,max(update_time) max_create_time
               from  csx_dwd.csx_dwd_tms_route_carrier_log_di 
                where operator = '排线确认' 
                  group by  route_id
              ) b on a.route_id=b.route_id
              --  where cn>1
              where 1=1
              -- order_code='OM23022100002164'
              and a.sdt>='20230301'
              and route_status=3
            --  and order_code!=''
                group by order_code,customer_code,plan_date
              -- having cn>1 
              --  and aa>=1
               )c on a.shipped_order_code=c.order_code
              where sdt>='20230301'
              group by shipped_order_code,
                entrucking_code,
                send_date,
                aa,
                cn
    ) c ON a.sale_order_code = c.sale_order_code 
      AND a.delivery_date = c.delivery_date 
      AND a.entrucking_code = c.entrucking_code
  


  
with tmp_normal_sale_order_item_v2
AS
(
  SELECT
    t1.inventory_dc_code, t1.inventory_dc_name, t1.customer_code, t1.order_code, t1.goods_code,
    t1.sale_unit_purchase_qty, t1.basic_unit_purchase_qty, t1.is_unit_conversion,
    t2.delivery_date, 
    t2.entrucking_code, 
    t2.package_code,
    t1.sale_unit, 
    t2.send_quantity, -- 销售单位出库数量
    t2.basic_unit, 
    t2.send_qty, -- 基础单位出库数量
    t2.is_first_entrucking_code -- 是否首次发车单
  FROM
  ( -- 找出符合条件的销售单
    SELECT
      inventory_dc_code, inventory_dc_name, customer_code, order_code, goods_code, sale_unit,
      sum(sale_unit_purchase_qty) AS sale_unit_purchase_qty,
      sum(basic_unit_purchase_qty) AS basic_unit_purchase_qty,
      max(is_unit_conversion) AS is_unit_conversion
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
        purchase_unit_name AS sale_unit, -- 销售单位
        purchase_qty AS sale_unit_purchase_qty, -- 销售单位下单数量
        purchase_unit_rate, -- 单位换算比例
        if(purchase_unit_rate <> 1, 1, 0) AS is_unit_conversion, -- 是否单位换算
        unit_name AS basic_unit, -- 基础单位
        purchase_qty * purchase_unit_rate AS basic_unit_purchase_qty -- 基础单位下单数量
      FROM csx_dwd.csx_dwd_oms_sale_order_detail_di
    WHERE sdt >= ${s_100days_ago}
      AND order_channel_code = 1 -- B端
      AND order_channel_detail_code <> 13 -- 非红旗
      AND order_type_code = 1 -- 正常销售单
      AND order_business_type_code = 1 -- 日配
      AND delivery_type_code = 1 -- 配送
      AND partner_type_code = 0 -- 非合伙人
      AND order_status_code IN (30, 40, 50, 60, 70) -- 30-部分发货  40-配送中  50-待确认 60-已签收  70-已完成
      AND substr(inventory_dc_code, 1, 1) <> 'L' -- 非外部城市服务商DC
    ) a LEFT JOIN
    ( -- 外部城市服务商DC，需要过滤
      SELECT
        shop_code
      FROM csx_dim.csx_dim_shop
      WHERE sdt = 'current' AND purpose = '09'
    ) b ON a.inventory_dc_code = b.shop_code
    WHERE b.shop_code IS NULL
    GROUP BY inventory_dc_code, inventory_dc_name, customer_code, order_code, goods_code, sale_unit
  ) t1 left JOIN
  ( -- 找出符合条件的包裹单
    SELECT
      a.delivery_date, 
      a.sale_order_code, 
      a.entrucking_code, 
      a.package_code,
      a.goods_code, 
      a.send_quantity, 
      a.basic_unit, 
      a.send_qty,
      if(c.sale_order_code IS NOT NULL, 1, 0) AS is_first_entrucking_code -- 是否首次发车单
    FROM
    (
      SELECT
        package_code, 
        sale_order_code, 
        entrucking_code, 
        regexp_replace(substr(shipped_time, 1, 10), '-', '') AS delivery_date,
        goods_code, 
        sale_unit_send_qty AS send_quantity, -- 销售单位出库数量
        unit_name AS basic_unit, -- 基础单位
        send_qty -- 基础单位出库数量
      FROM csx_dwd.csx_dwd_oms_package_order_detail_di
      WHERE sdt >= ${s_100days_ago} AND delivery_type_code = 1 -- 配送
    ) a LEFT JOIN
    (
      SELECT
        sale_order_code, 
        entrucking_code, 
        delivery_date
      FROM
      (
        SELECT
          sale_order_code, 
          entrucking_code,
          row_number() OVER(PARTITION BY sale_order_code ORDER BY shipped_time) AS rank,
          min(regexp_replace(substr(shipped_time, 1, 10), '-', '')) OVER(PARTITION BY sale_order_code) AS delivery_date
        FROM csx_dwd.csx_dwd_oms_package_order_detail_di
        WHERE sdt >= ${s_100days_ago} 
          AND delivery_type_code = 1 -- 配送
      ) tmp WHERE rank = 1
    ) c ON a.sale_order_code = c.sale_order_code 
      AND a.delivery_date = c.delivery_date 
      AND a.entrucking_code = c.entrucking_code
  ) t2 ON t1.order_code = t2.sale_order_code AND t1.goods_code = t2.goods_code
  left join 
  (select send_date,
    entrucking_code,
    shipped_order_code,
    goods_code
   from csx_dws.csx_dws_tms_entrucking_order_product_detail_di where sdt>='20230301') a
   join 
  (select  order_code,rank
   from  csx_dwd.csx_dwd_tms_route_item_di  a 
  join 
  (select route_id,create_time,count(*) cn
   from  csx_dwd.csx_dwd_tms_route_carrier_log_di 
    where operator = '排线确认' 
      group by  route_id,create_time
  ) b on a.route_id=b.route_id
    where cn>1
    group by order_code,rank
  )c on a.shipped_order_code=c.order_code

)


-- 生成缺货分析报表
-- INSERT OVERWRITE TABLE csx_report.csx_report_oms_out_of_stock_goods_1d PARTITION(sdt)
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
  send_qty-purchase_qty as out_of_stock_qty,
  spec,
  case when sale_unit in ('市斤', '公斤', '斤', 'kg', 'kG', 'Kg', 'KG', 'g', 'G', '千克', '克', '两')
    and  abs((send_qty-purchase_qty)/purchase_qty) > 0.05 then 1 
  when sale_unit not in ('市斤', '公斤', '斤', 'kg', 'kG', 'Kg', 'KG', 'g', 'G', '千克', '克', '两') 
    and send_qty < purchase_qty  then 1 else 0 end as is_out_of_stock,
  if(d.inventory_dc_code is not null, 1, 0) as is_base_goods,
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
    goods_code, 
    sale_unit, 
    basic_unit,
    -- if(is_unit_conversion = 1, sale_unit_purchase_qty, basic_unit_purchase_qty) AS purchase_qty,
    -- if(is_first_entrucking_code = 1, if(is_unit_conversion = 1 AND send_quantity <> 0, send_quantity, send_qty), 0) AS send_qty
    basic_unit_purchase_qty AS purchase_qty,
    if(is_first_entrucking_code = 1, send_qty, 0) AS send_qty
  FROM tmp_normal_sale_order_item_v2
  WHERE delivery_date >= ${s_last_month_start} AND delivery_date < regexp_replace(current_date(), '-', '')
) a JOIN
( -- 只取大客户
  SELECT
    customer_code,
    customer_name,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name
  FROM csx_dim.csx_dim_crm_customer_info
  WHERE sdt = 'current' AND customer_code <> '' AND channel_code = '1' -- 大客户
) b ON a.customer_code = b.customer_code
left join
(
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
  FROM csx_dim.csx_dim_basic_goods
  WHERE sdt = 'current'
) c on a.goods_code = c.goods_code
left join
(
  select inventory_dc_code, product_code
  from csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df	
  where sdt = regexp_replace(date_sub(current_date(), 1), '-', '') and base_product_tag = 1
) d on a.inventory_dc_code = d.inventory_dc_code and a.goods_code = d.product_code;



create table   csx_analyse_tmp.csx_analyse_tmp_pakcage_00 as  
 -- 找出符合条件的销售单
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
      max(is_unit_conversion) AS is_unit_conversion
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
        purchase_unit_name AS sale_unit, -- 销售单位
        purchase_qty AS sale_unit_purchase_qty, -- 销售单位下单数量
        purchase_unit_rate, -- 单位换算比例
        if(purchase_unit_rate <> 1, 1, 0) AS is_unit_conversion, -- 是否单位换算
        unit_name AS basic_unit, -- 基础单位
        purchase_qty * purchase_unit_rate AS basic_unit_purchase_qty -- 基础单位下单数量
      FROM csx_dwd.csx_dwd_oms_sale_order_detail_di
    WHERE sdt >='20230301'
      AND order_channel_code = 1 -- B端
      AND order_channel_detail_code <> 13 -- 非红旗
      AND order_type_code = 1 -- 正常销售单
      AND order_business_type_code = 1 -- 日配
      AND delivery_type_code = 1 -- 配送
      AND partner_type_code = 0 -- 非合伙人
      AND order_status_code IN (30, 40, 50, 60, 70) -- 30-部分发货  40-配送中  50-待确认 60-已签收  70-已完成
      AND substr(inventory_dc_code, 1, 1) <> 'L' -- 非外部城市服务商DC
    ) a   
    JOIN
    ( -- 外部城市服务商DC，需要过滤
      SELECT
        shop_code
      FROM csx_dim.csx_dim_shop
      WHERE sdt = 'current' 
        AND purpose != '09'
        and company_code ='2211'
    ) b ON a.inventory_dc_code = b.shop_code
    WHERE 1=1
    GROUP BY inventory_dc_code, inventory_dc_name, customer_code, order_code, goods_code, sale_unit
  ;
  
 drop table  csx_analyse_tmp.csx_analyse_tmp_pakcage;
create table csx_analyse_tmp.csx_analyse_tmp_pakcage as 
SELECT
      a.delivery_date, 
      a.sale_order_code, 
      a.entrucking_code, 
      a.package_code,
      a.goods_code, 
      a.send_quantity, 
      a.basic_unit, 
      a.send_qty,
      cn,
      aa,
      case when cn=1 and aa>=1 then 1 
      else 0 end AS is_first_entrucking_code -- 是否首次发车单
    FROM
    (
    
      SELECT
        package_code, 
        sale_order_code, 
        entrucking_code, 
        regexp_replace(substr(shipped_time, 1, 10), '-', '') AS delivery_date,
        goods_code, 
        sale_unit_send_qty AS send_quantity, -- 销售单位出库数量
        unit_name AS basic_unit, -- 基础单位
        send_qty -- 基础单位出库数量
      FROM   csx_dwd.csx_dwd_oms_package_order_detail_di
      WHERE sdt >='20230301'
        AND delivery_type_code = 1 -- 配送
    ) a LEFT JOIN
    (
     
    -- 取TMS销售出库单号根据排线ID，按照更新时间，如果同一时间，未缺货 cn=1且aa>1 属于同单不同车，cn=1且aa=1属于同车同单
    select 
            sale_order_code,
            entrucking_code,
            delivery_date,
            sum(aa) aa,
            sum(cn) cn
        from (
        select  a.dc_code ,
	              a.child_customer_code ,
	              entrucking_code,
	              shipped_order_code,
	              regexp_replace(send_date,'-','') delivery_date ,
                count(max_create_time) as cn,
                count(a.route_id) aa
              from  csx_analyse_tmp.csx_analyse_tmp_tms_order_route a
                group by a.dc_code ,
	                a.child_customer_code ,
	                entrucking_code,
	                shipped_order_code,
	                regexp_replace(send_date,'-','')
      union all         
      SELECT
        sale_order_code, 
        entrucking_code, 
        delivery_date,
        1 cn,
        1 aa
      FROM
      (
        
        SELECT
          sale_order_code, 
          entrucking_code,
          row_number() OVER(PARTITION BY sale_order_code ORDER BY shipped_time) AS rank,
          min(regexp_replace(substr(shipped_time, 1, 10), '-', '')) OVER(PARTITION BY sale_order_code) AS delivery_date
        FROM csx_dwd.csx_dwd_oms_package_order_detail_di
        WHERE sdt >= '20230301'
          AND delivery_type_code = 1 -- 配送
      ) tmp WHERE rank = 1
    )a 
        group by sale_order_code, 
        entrucking_code, 
        delivery_date
    ) c ON a.sale_order_code = c.sale_order_code 
      AND a.delivery_date = c.delivery_date 
      AND a.entrucking_code = c.entrucking_code
   ;

   
drop table  csx_analyse_tmp.csx_analyse_tmp_pakcage_01;
create table csx_analyse_tmp.csx_analyse_tmp_pakcage_01 as 

with tmp_normal_sale_order_item_v2
AS
(
  SELECT
    t1.inventory_dc_code, t1.inventory_dc_name, t1.customer_code, t1.order_code, t1.goods_code,
    t1.sale_unit_purchase_qty, t1.basic_unit_purchase_qty, t1.is_unit_conversion,
    t2.delivery_date, 
    t2.entrucking_code, 
    t2.package_code,
    t1.sale_unit, 
    t2.send_quantity, -- 销售单位出库数量
    t2.basic_unit, 
    t2.send_qty, -- 基础单位出库数量
    t2.is_first_entrucking_code -- 是否首次发车单
  FROM
  (
  select * from  csx_analyse_tmp.csx_analyse_tmp_pakcage_00) t1
  left join 
  (select * from  csx_analyse_tmp.csx_analyse_tmp_pakcage) t2
 ON t1.order_code = t2.sale_order_code AND t1.goods_code = t2.goods_code
  ) 
 -- create table csx_analyse_tmp.csx_analyse_tmp_pakcage_01 as 
  SELECT
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
  send_qty-purchase_qty as out_of_stock_qty,
  spec,
  case when sale_unit in ('市斤', '公斤', '斤', 'kg', 'kG', 'Kg', 'KG', 'g', 'G', '千克', '克', '两')
    and  abs((send_qty-purchase_qty)/purchase_qty) > 0.05 then 1 
  when sale_unit not in ('市斤', '公斤', '斤', 'kg', 'kG', 'Kg', 'KG', 'g', 'G', '千克', '克', '两') 
    and send_qty < purchase_qty  then 1 else 0 end as is_out_of_stock,
  if(d.inventory_dc_code is not null, 1, 0) as is_base_goods,
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
    goods_code, 
    sale_unit, 
    basic_unit,
    if(is_unit_conversion = 1, sale_unit_purchase_qty, basic_unit_purchase_qty) AS purchase_qty,  -- 单位转换 取销售单位订单数量否则取基础单位订单数量
    if(is_first_entrucking_code = 1, if(is_unit_conversion = 1 AND send_quantity <> 0, send_quantity, send_qty), 0) AS send_qty
    -- basic_unit_purchase_qty AS purchase_qty,
    -- if(is_first_entrucking_code = 1, send_qty, 0) AS send_qty
  FROM tmp_normal_sale_order_item_v2
  WHERE 1=1
) a JOIN
( -- 只取大客户
  SELECT
    customer_code,
    customer_name,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name
  FROM csx_dim.csx_dim_crm_customer_info
  WHERE sdt = 'current' AND customer_code <> '' AND channel_code = '1' -- 大客户
) b ON a.customer_code = b.customer_code
left join
(
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
  FROM csx_dim.csx_dim_basic_goods
  WHERE sdt = 'current'
) c on a.goods_code = c.goods_code
left join
(
  select inventory_dc_code, product_code
  from csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df	
  where sdt = regexp_replace(date_sub(current_date(), 1), '-', '') and base_product_tag = 1
) d on a.inventory_dc_code = d.inventory_dc_code and a.goods_code = d.product_code;


select * from csx_analyse_tmp.csx_analyse_tmp_pakcage_01



;




select a.warehouse_code ,
	a.customer_code ,
	entrucking_code,
	shipped_order_code,
	a.send_date  ,
	route_id,
  max_create_time
	from
(select dc_code ,
		child_customer_code ,
		send_date , 
		entrucking_code,
		shipped_order_code,
    route_id
	from  csx_dwd.csx_dwd_tms_sign_shipped_order_detail_di 
	where sdt >='20230301'
	group by dc_code ,
    child_customer_code ,
    send_date,
    entrucking_code,
		shipped_order_code
) a 
left join 
( dc_code,
  select route_id,
    max(create_time) max_create_time
from
	csx_dwd.csx_dwd_tms_route_carrier_log_di
where
	  sdt >= '20230201'
  and operator = '排线确认' 
  group by  route_id,
        dc_code
  )b on  a.dc_code=b.dc_code and a.route_id=b.route_id 
where a.child_customer_code !=''
;