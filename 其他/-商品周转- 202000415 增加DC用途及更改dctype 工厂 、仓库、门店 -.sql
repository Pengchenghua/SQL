-- 202000415 增加DC用途及更改dctype 工厂 、仓库、门店 --set mapreduce.job.queuename =caishixian; 
-- SET mapreduce.job.reduces =80;
SET hive.map.aggr = true;
--set hive.groupby.skewindata =true;
SET hive.exec.parallel = true;
SET hive.exec.dynamic.partition = true;
--开启动态分区
SET hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
SET hive.exec.max.dynamic.partitions = 10000;
--在所有执行MR的节点上，最大一共可以创建多少个动态分区。
SET hive.exec.max.dynamic.partitions.pernode = 100000;
--源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
SET edate = date_sub(current_date(), 1);
SET sdate = trunc(date_sub(current_date(), 1), 'MM');
DROP TABLE IF EXISTS csx_tmp.p_invt_1;
CREATE TEMPORARY TABLE IF NOT EXISTS csx_tmp.p_invt_1 AS -- 库存查询
SELECT dc_code,
    goods_code goodsid,
    sum (
        CASE
            WHEN sdt >= regexp_replace ($ { hiveconf :sdate }, '-', '') THEN qty
        end
    ) inv_qty,
    sum (
        CASE
            WHEN sdt >= regexp_replace ($ { hiveconf :sdate }, '-', '') THEN amt
        end
    ) inv_amt,
    sum (
        CASE
            WHEN sdt > regexp_replace (date_sub($ { hiveconf :edate }, 31), '-', '') THEN qty
        end
    ) inv_qty_30day,
    sum (
        CASE
            WHEN sdt > regexp_replace (date_sub($ { hiveconf :edate }, 31), '-', '') THEN amt
        end
    ) inv_amt_30day,
    sum (
        CASE
            WHEN sdt = regexp_replace ($ { hiveconf :edate }, '-', '') THEN qty
        end
    ) qm_qty,
    sum (
        CASE
            WHEN sdt = regexp_replace ($ { hiveconf :edate }, '-', '') THEN amt
        end
    ) qm_amt
FROM csx_dw.dws_wms_r_d_accounting_stock_m
WHERE sdt > regexp_replace (date_sub($ { hiveconf :edate }, 31), '-', '')
    AND sdt <= regexp_replace ($ { hiveconf :edate }, '-', '')
    AND reservoir_area_code not IN ('B999', 'B997', 'PD01', 'PD02', 'TS01')
GROUP BY dc_code,
    goods_code;
--最近销售日期
DROP TABLE IF EXISTS csx_tmp.p_sale_max;
CREATE temporary TABLE if not exists csx_tmp.p_sale_max AS
SELECT dc_code AS shop_id,
    goods_code AS goodsid,
    coalesce(MAX(sdt), '') AS max_sale_sdt
FROM csx_dw.dws_sale_r_d_customer_sale
WHERE sdt >= '20190101'
GROUP BY dc_code,
    goods_code;
--末次入库日期及数量
DROP TABLE IF EXISTS csx_tmp.p_entry_max;
CREATE temporary TABLE if not exists csx_tmp.p_entry_max AS
SELECT a.receive_location_code,
    a.goods_code,
    coalesce(SUM(receive_qty), 0) AS entry_qty,
    coalesce(SUM(price * receive_qty), 0) AS entry_value,
    coalesce(sdt, '') AS entry_sdt
FROM csx_dw.wms_entry_order a
    JOIN (
        SELECT receive_location_code,
            goods_code,
            MAX(sdt) AS max_sdt
        FROM csx_dw.wms_entry_order
        WHERE sdt > '20181231'
            AND receive_qty != 0
            AND entry_type != '客退入库'
        GROUP BY receive_location_code,
            goods_code
    ) AS b ON a.receive_location_code = b.receive_location_code
    AND a.goods_code = b.goods_code
    AND a.sdt = b.max_sdt
GROUP BY a.receive_location_code,
    a.goods_code,
    coalesce(sdt, '');
-- SELECT  *
FROM temp.p_invt_1 a
WHERE shop_id = 'W0A2';
--关联库存与销售 
--SELECT  prov_code prov_name bd_id bd_name dept_id dept_name SUM(sale)sale sum 
--FROM 
--(
DROP TABLE IF EXISTS csx_tmp.p_invt_2;
CREATE TEMPORARY TABLE IF NOT EXISTS csx_tmp.p_invt_2 AS
SELECT substr(regexp_replace ($ { hiveconf :edate }, '-', ''), 1, 4) AS years,
    substr(regexp_replace ($ { hiveconf :edate }, '-', ''), 1, 6) AS months,
    b.prov_code,
    b.prov_name,
    dist_code,
    dist_name,
    a.dc_code AS shop_id,
    b.shop_name,
    a.goodsid,
    SUM(qty) sales_qty,
    SUM(a.sale) sales_value,
    SUM(profit) profit,
    SUM(sales_cost) AS sales_cost,
    SUM(inv_qty) AS period_inv_qty,
    SUM(inv_amt) AS period_inv_amt,
    SUM(inv_qty_30day) as period_inv_qty_30day,
    SUM(inv_amt_30day) as period_inv_amt_30day,
    SUM(qm_qty) AS final_qty,
    SUM(qm_amt) AS final_amt,
    COALESCE (
        CASE
            WHEN SUM(sales_cost) = 0 THEN 999
            else SUM(inv_amt) / SUM(sale - profit)
        end,
        0
    ) AS days_turnover,
    COALESCE(SUM(sales_30day) / 30, 0) AS sale_30day,
    COALESCE(SUM(qty_30day) / 30, 0) AS qty_30day,
    coalesce(SUM(sales_cost30day), 0) AS cost_30day,
    COALESCE (
        CASE
            WHEN SUM(qty_30day) = 0 THEN 999
            else SUM(qm_qty) / SUM(qty_30day)
        end,
        0
    ) AS days_sale --日均销量 
,
    dc_type,
    dc_uses
FROM (
        SELECT dc_code,
            goods_code goodsid,
            SUM(sales_qty) qty,
            SUM(sales_cost) AS sales_cost,
            SUM(sales_value) sale,
            SUM(profit) profit,
            0 qty_30day,
            0 sales_30day,
            0 sales_cost30day,
            0 inv_qty,
            0 inv_amt,
            0 inv_qty_30day,
            0 inv_amt_30day,
            0 qm_qty,
            0 qm_amt
        FROM csx_dw.dws_sale_r_d_customer_sale
        WHERE sdt >= regexp_replace ($ { hiveconf :sdate }, '-', '')
            AND sdt <= regexp_replace ($ { hiveconf :edate }, '-', '')
        GROUP BY dc_code,
            goods_code
        UNION ALL
        SELECT dc_code,
            goods_code goodsid,
            0 qty,
            0 sales_cost,
            0 sale,
            0 profit,
            SUM(sales_qty) AS qty_30day,
            SUM(sales_value) as sales_30day,
            SUM(sales_cost) AS sales_cost30day,
            0 inv_qty,
            0 inv_amt,
            0 inv_qty_30day,
            0 inv_amt_30day,
            0 qm_qty,
            0 qm_amt
        FROM csx_dw.dws_sale_r_d_customer_sale
        WHERE sdt > regexp_replace (date_sub($ { hiveconf :edate }, 30), '-', '')
            AND sdt <= regexp_replace ($ { hiveconf :edate }, '-', '')
        GROUP BY dc_code,
            goods_code
        UNION ALL
        SELECT a.dc_code,
            a.goodsid,
            0 qty,
            0 sales_cost,
            0 sale,
            0 profit,
            0 qty_30day,
            0 sales_30day,
            0 sales_cost30day,
            a.inv_qty,
            a.inv_amt,
            a.inv_qty_30day,
            a.inv_amt_30day,
            a.qm_qty,
            a.qm_amt
        FROM csx_tmp.p_invt_1 a
    ) a
    JOIN (
        SELECT location_code shop_id,
            shop_name,
            dist_code,
            dist_name,
CASE
                WHEN a.location_code = 'W0H4' THEN 'W0H4'
                ELSE a.province_code
            END prov_code,
CASE
                WHEN a.location_code = 'W0H4' THEN '供应链平台'
                ELSE a.province_name
            END prov_name,
            a.purpose AS dc_uses,
            a.location_type AS dc_type
        FROM csx_dw.csx_shop a
        WHERE sdt = 'current'
    ) b ON dc_code = b.shop_id
GROUP BY b.prov_code,
    b.prov_name,
    a.dc_code,
    b.shop_name,
    a.goodsid,
    dist_code,
    dist_name,
    dc_type,
    dc_uses;
-- SELECT  SUM(sales_value)
FROM csx_dw.supply_turnover
WHERE sdt = '20200119';
-- SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE csx_dw.supply_turnover partition (sdt)
SELECT substr(regexp_replace ($ { hiveconf :edate }, '-', ''), 1, 4) AS years,
    substr(regexp_replace ($ { hiveconf :edate }, '-', ''), 1, 6) AS months,
    prov_code,
    prov_name,
    dist_code,
    dist_name,
    a.shop_id,
    shop_name,
    a.goodsid,
    goods_name,
    standard,
    c.unit_name,
    brand_name,
    dept_id,
    dept_name,
    bd_id,
    bd_name,
    div_id,
    div_name,
    catg_l_id,
    catg_l_name,
    catg_m_id,
    catg_m_name,
    catg_s_id,
    catg_s_name,
    nvl(valid_tag, '') valid_tag,
    nvl(valid_tag_name, '') valid_tag_name,
    nvl(goods_status_id, '') goods_status_id,
    nvl(goods_status_name, '') goods_status_name,
    sales_qty,
    sales_value,
    profit,
    sales_cost,
    period_inv_qty,
    period_inv_amt,
    -- period_inv_qty_30day -- period_inv_amt_30day final_qty 
,
    final_amt,
    days_turnover,
    sale_30day,
    qty_30day,
    -- cost_30day days_sale 
,
    nvl(max_sale_sdt, '') max_sale_sdt,
    coalesce(
        datediff(
            date_sub(current_date(), 1),
            from_unixtime(
                unix_timestamp(max_sale_sdt, 'yyyyMMdd'),
                'yyyy-MM-dd'
            )
        ),
        0
    ) AS no_sale_days,
    coalesce(dc_type, '') AS dc_type,
    coalesce(entry_qty, 0) AS entry_qty,
    coalesce(entry_value, 0) AS entry_value,
    nvl(entry_sdt, '') AS entry_sdt,
    coalesce(
        datediff(
            date_sub(current_date(), 1),
            from_unixtime(
                unix_timestamp(entry_sdt, 'yyyyMMdd'),
                'yyyy-MM-dd'
            )
        ),
        0
    ) AS entry_days,
    nvl(dc_uses, '') AS dc_uses,
    cost_30day,
    period_inv_qty_30day,
    period_inv_amt_30day,
    COALESCE (
        CASE
            WHEN (cost_30day) = 0 THEN 999
            else (period_inv_amt_30day) / (cost_30day)
        end,
        0
    ) AS days_trunover_30,
    regexp_replace ($ { hiveconf :edate }, '-', '') sdt
FROM csx_tmp.p_invt_2 a
    LEFT JOIN csx_tmp.p_sale_max b ON a.shop_id = b.shop_id
    AND a.goodsid = b.goodsid
    LEFT JOIN csx_tmp.p_entry_max j ON a.shop_id = j.receive_location_code
    AND a.goodsid = j.goods_code
    LEFT OUTER JOIN (
        SELECT shop_code AS shop_id,
            product_code goodsid,
            product_status_name AS goods_status_name,
            des_specific_product_status AS goods_status_id,
            valid_tag,
            valid_tag_name
        FROM csx_dw.dws_basic_w_a_csx_product_info
        WHERE sdt = regexp_replace ($ { hiveconf :edate }, '-', '')
    ) d ON a.shop_id = d.shop_id
    AND a.goodsid = d.goodsid
    LEFT JOIN (
        SELECT goods_id,
            goods_name,
            standard,
            unit_name,
            brand_name,
            department_id dept_id,
            department_name dept_name,
CASE
                WHEN division_code IN ('12', '13', '14') THEN '12'
                WHEN division_code IN ('10', '11') THEN '11'
                ELSE division_code
            END bd_id,
CASE
                WHEN division_code IN ('12', '13', '14') THEN '食品用品采购部'
                WHEN division_code IN ('10', '11') THEN '生鲜采购部'
                ELSE division_name
            END bd_name,
            division_code div_id,
            division_name div_name,
            category_large_code catg_l_id,
            category_large_name catg_l_name,
            category_middle_code catg_m_id,
            category_middle_name catg_m_name,
            category_small_code catg_s_id,
            category_small_name catg_s_name
        FROM csx_dw.dws_basic_w_a_csx_product_m
        WHERE sdt = 'current'
    ) c ON a.goodsid = c.goods_id;
