SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.parallel.thread.number = 80;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET sdate = '${startdate}';
SET s_date = regexp_replace(
        to_date(trunc($ { hiveconf :sdate }, 'MM')),
        '-',
        ''
    );
SET e_date = regexp_replace(last_day(to_date($ { hiveconf :sdate })), '-', '');
DROP TABLE IF EXISTS csx_tmp.temp_days_sale;
CREATE TEMPORARY TABLE IF NOT EXISTS csx_tmp.temp_days_sale AS -- 按照渠道
SELECT from_unixtime(unix_timestamp(calday, 'yyyyMMdd'), 'yyyy-MM-dd') AS sale_date,
    '2' AS LAYER,
    channel_name,
    province_code,
    province_name,
    division_code,
    division_name,
    sale,
    profit,
    profit / sale AS profit_rate
FROM (
        SELECT calday
        FROM csx_dw.dws_w_a_date_m
        WHERE calday >= $ { hiveconf :s_date }
            AND calday <= $ { hiveconf :e_date }
    ) AS a
    LEFT JOIN (
        SELECT sdt,
            CASE
                WHEN channel IN ('1', '7') THEN '大'
                WHEN channel IN ('2') THEN '商超'
                ELSE channel_name
            END channel_name,
            province_code,
            province_name,
            division_code,
            division_name,
            SUM(sales_value) sale,
            SUM(profit) profit,
            SUM(profit) / SUM(sales_value) profit_rate
        FROM csx_dw.dws_sale_r_d_customer_sale
        WHERE sdt >= $ { hiveconf :s_date }
            AND sdt <= $ { hiveconf :e_date } -- AND province_code ='2' 
        GROUP BY sdt,
            division_code,
            division_name,
            CASE
                WHEN channel IN ('1', '7') THEN '大'
                WHEN channel IN ('2') THEN '商超'
                ELSE channel_name
            END,
            province_code,
            province_name
    ) AS b ON a.calday = b.sdt;
-- 计算事业部 数据
INSERT INTO csx_tmp.temp_days_sale
SELECT sale_date,
    '1' AS LAYER,
    channel_name,
    province_code,
    province_name,
    CASE
        WHEN division_code IN('10', '11') THEN '10'
        WHEN division_code IN('12', '13', '14') THEN '12'
        ELSE division_code
    END division_code,
    CASE
        WHEN division_code IN('10', '11') THEN '生鲜采购部'
        WHEN division_code IN('12', '13', '14') THEN '食百采购部'
        ELSE division_name
    END division_code,
    SUM(sale) AS sale,
    SUM(profit) AS profit,
    SUM(profit) / SUM(sale) AS profit_rate
FROM csx_tmp.temp_days_sale
GROUP BY province_code,
    province_name,
    sale_date,
    CASE
        WHEN division_code IN('10', '11') THEN '10'
        WHEN division_code IN('12', '13', '14') THEN '12'
        ELSE division_code
    END,
    CASE
        WHEN division_code IN('10', '11') THEN '生鲜采购部'
        WHEN division_code IN('12', '13', '14') THEN '食百采购部'
        ELSE division_name
    END,
    channel_name;
-- 全渠道数据
DROP TABLE IF EXISTS csx_tmp.temp_days_sale_all;
CREATE TEMPORARY TABLE IF NOT EXISTS csx_tmp.temp_days_sale_all AS
SELECT from_unixtime(unix_timestamp(calday, 'yyyyMMdd'), 'yyyy-MM-dd') AS sale_date,
    '1' AS LAYER,
    channel_name,
    province_code,
    province_name,
    division_code,
    division_name,
    sale,
    profit,
    profit / sale AS profit_rate
FROM (
        SELECT calday
        FROM csx_dw.dws_w_a_date_m
        WHERE calday >= $ { hiveconf :s_date }
            AND calday <= $ { hiveconf :e_date }
    ) a
    LEFT JOIN (
        SELECT sdt,
            '全渠道' channel_name,
            province_code,
            province_name,
            division_code,
            division_name,
            SUM(sales_value) sale,
            SUM(profit) profit,
            SUM(profit) / SUM(sales_value) profit_rate
        FROM csx_dw.dws_sale_r_d_customer_sale
        WHERE sdt >= $ { hiveconf :s_date }
            AND sdt <= $ { hiveconf :e_date } -- AND province_code ='2' 
        GROUP BY sdt,
            division_code,
            division_name,
            province_code,
            province_name
    ) b ON a.calday = b.sdt;
-- 全渠道事业部数据
INSERT INTO csx_tmp.temp_days_sale_all
SELECT sale_date,
    '1' AS LAYER,
    channel_name,
    province_code,
    province_name,
    CASE
        WHEN division_code IN('10', '11') THEN '10'
        WHEN division_code IN('12', '13', '14') THEN '12'
        ELSE division_code
    END division_code,
    CASE
        WHEN division_code IN('10', '11') THEN '生鲜采购部'
        WHEN division_code IN('12', '13', '14') THEN '食百采购部'
        ELSE division_name
    END division_code,
    SUM(sale) AS sale,
    SUM(profit) AS profit,
    SUM(profit) / SUM(sale) AS profit_rate
FROM csx_tmp.temp_days_sale_all
GROUP BY province_code,
    province_name,
    sale_date,
    CASE
        WHEN division_code IN('10', '11') THEN '10'
        WHEN division_code IN('12', '13', '14') THEN '12'
        ELSE division_code
    END,
    CASE
        WHEN division_code IN('10', '11') THEN '生鲜采购部'
        WHEN division_code IN('12', '13', '14') THEN '食百采购部'
        ELSE division_name
    END,
    channel_name;
-- 插入表
INSERT OVERWRITE TABLE csx_dw.ads_sale_province_daily_sales_report partition(sdt)
SELECT sale_date,
    LAYER,
    channel_name,
    province_code,
    province_name,
    division_code,
    division_name,
    SUM(sale) as sale,
    SUM(profit) as profti,
    SUM(profit_rate) as profit_rate,
    0 AS sale_cust,
    0 AS atv,
    substring(regexp_replace(sale_date, '-', ''), 1, 6)
FROM (
        SELECT sale_date,
            LAYER,
            channel_name,
            province_code,
            province_name,
            division_code,
            division_name,
            sale,
            profit,
            profit_rate
        FROM csx_tmp.temp_days_sale
        UNION ALL
        SELECT sale_date,
            LAYER,
            channel_name,
            province_code,
            province_name,
            division_code,
            division_name,
            sale,
            profit,
            profit_rate
        FROM csx_tmp.temp_days_sale_all
    ) AS a
GROUP BY sale_date,
    LAYER,
    channel_name,
    province_code,
    province_name,
    division_code,
    division_name;
INSERT INTO TABLE csx_dw.ads_sale_province_daily_sales_report partition(sdt)
SELECT sale_date,
    LAYER,
    a.channel_name,
    a.province_code,
    a.province_name,
    division_code,
    division_name,
    SUM(sale) as sale,
    SUM(profit) as profti,
    SUM(profit) / SUM(sale) profit_rate,
    SUM(sale_cust) AS sale_cust,
    SUM(sale) / SUM(sale_cust) AS atv,
    substring(regexp_replace(sale_date, '-', ''), 1, 6)
FROM (
        SELECT sale_date,
            LAYER,
            channel_name,
            province_code,
            province_name,
            '00' AS division_code,
            '合计' AS division_name,
            SUM(sale) sale,
            SUM(profit) profit,
            SUM(profit) / SUM(profit) AS profit_rate
        FROM csx_tmp.temp_days_sale
        WHERE division_name IN ('生鲜采购部', '食百采购部')
        GROUP BY sale_date,
            LAYER,
            channel_name,
            province_code,
            province_name
        UNION ALL
        SELECT sale_date,
            LAYER,
            channel_name,
            province_code,
            province_name,
            '00' AS division_code,
            '合计' AS division_name,
            SUM(sale) as sale,
            SUM(profit) as profit,
            SUM(profit) / SUM(profit) AS profit_rate
        FROM csx_tmp.temp_days_sale_all
        WHERE division_name IN ('生鲜采购部', '食百采购部')
        GROUP BY sale_date,
            LAYER,
            channel_name,
            province_code,
            province_name
    ) AS a
    LEFT JOIN (
        SELECT from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd') AS sdt,
            CASE
                WHEN channel IN('1', '7') THEN '大'
                WHEN channel IN('2') THEN '商超'
            END AS channel_name,
            province_code,
            COUNT(DISTINCT customer_no) AS sale_cust
        FROM csx_dw.customer_sales
        WHERE sdt >= $ { hiveconf :s_date }
            AND sdt <= $ { hiveconf :e_date }
        GROUP BY sdt,
            province_code,
            CASE
                WHEN channel IN('1', '7') THEN '大'
                WHEN channel IN('2') THEN '商超'
            END
        UNION ALL
        SELECT from_unixtime(unix_timestamp(sdt, 'yyyyMMdd'), 'yyyy-MM-dd') AS sdt,
            '全渠道' as channel_name,
            province_code,
            COUNT(DISTINCT customer_no) as sale_cust
        FROM csx_dw.customer_sales
        WHERE sdt >= $ { hiveconf :s_date }
            AND sdt <= $ { hiveconf :e_date }
        GROUP BY sdt,
            province_code
    ) AS b ON a.sale_date = b.sdt
    AND a.province_code = b.province_code
    AND a.channel_name = b.channel_name
GROUP BY sale_date,
    LAYER,
    a.channel_name,
    a.province_code,
    a.province_name,
    division_code,
    division_name;
