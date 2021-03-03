SELECT * FROM csx_basic_data.csx_product_info cpi2 where product_code is null and shop_code ='W0A3';
-- 库存按照过帐日期查询 商品组别
select a.shipper_code,location_code,shop_name,a.company_code,a.reservoir_area_code,c.reservoir_area_name,a.product_code,product_name,purchase_group_code,purchase_group_name,qty,amt_no_tax
from 
(SELECT
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 sum( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS qty ,
 sum( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS amt_no_tax 
FROM
 csx_b2b_accounting.accounting_stock_detail  
 where posting_time < '2020-07-01 00:00:00'
GROUP BY
 shipper_code,
 location_code,
 company_code,
 reservoir_area_code,
 product_code 
 ) a 
 left join 
 (select * from csx_basic_data.csx_product_info cpi)b 
 on a.product_code=b.product_code and a.location_code=shop_code
LEFT JOIN 
(select warehouse_code,reservoir_area_code,reservoir_area_name from csx_b2b_wms.wms_reservoir_area wra)c on a.reservoir_area_code=c.reservoir_area_code and c.warehouse_code=location_code  ;
 

-- -------------- 
SELECT * FROM csx_b2b_accounting.accounting_stock_detail asd where biz_time>'2020-01-12 23:59:00' and biz_time<='2020-01-13 23:59:59' and location_code='W048'
and move_type in ('101A','102A','105A') and product_code='480';
SELECT DISTINCT workshop_code,workshop_name FROM  csx_b2b_factory.factory_setting_produce_line fsgb ;

SELECT * from csx_basic_data.md_company_code mcc ;
SELECT * FROM csx_b2b_crm.sys_province sp;
select * from csx_basic_data.md_shop_info msi  ;



select location_code,shop_name,a.company_code,
a.reservoir_area_code,c.reservoir_area_name,
a.product_code,product_name,purchase_group_code,purchase_group_name,qty,amt_no_tax
from 
(SELECT
 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 sum(qty) AS qty ,
 sum(amt_no_tax ) AS amt_no_tax 
FROM
 data_sync.data_sync_inventory_item  
 where posting_time < '2020-07-01 00:00:00'
GROUP BY
 location_code,
 company_code,
 reservoir_area_code,
 product_code 
 ) a 
 left join 
 (select * from csx_basic_data.csx_product_info cpi)b 
 on a.product_code=b.product_code and a.location_code=shop_code
LEFT JOIN 
(select warehouse_code,reservoir_area_code,reservoir_area_name from csx_b2b_wms.wms_reservoir_area wra)c 
on a.reservoir_area_code=c.reservoir_area_code and c.warehouse_code=location_code ;

 

-- 
SELECT
    aaa.地点 AS '地点',
    aaa.商品编码 AS '商品编码',
    aaa.商品名称 AS '商品名称',
    aaa.采购组编码 AS '采购组编码',
    aaa.采购组名称 AS '采购组名称',
    sum( aaa.数量 ) AS '数量',
    sum( aaa.不含税金额 ) AS '不含税金额',
    aaa.税率 AS '税率' 
FROM
    (
    SELECT
        a.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view12 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) a UNION
    SELECT
        b.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view11 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) b UNION
    SELECT
        c.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view10 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) c UNION
    SELECT
        d.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view9 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) d UNION
    SELECT
        e.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view8 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) e UNION
    SELECT
        f.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view7 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) f UNION
    SELECT
        h.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view6 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) h UNION
    SELECT
        i.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view5 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) i UNION
    SELECT
        j.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view4 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) j UNION
    SELECT
        k.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view3 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) k UNION
    SELECT
        l.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view2 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) l UNION
    SELECT
        m.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view1 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) m UNION
    SELECT
        n.* 
    FROM
        (
        SELECT
            a.location_code AS '地点',
            a.product_code AS '商品编码',
            a.product_name AS '商品名称',
            a.purchase_group_code AS '采购组编码',
            a.purchase_group_name AS '采购组名称',
            sum(
            IF
                ( a.in_or_out = 0, a.txn_qty, IF ( a.in_or_out = 1,- a.txn_qty, 0 ) ) 
            ) AS '数量',
            sum( IF ( a.in_or_out = 1, - a.amt_no_tax, a.amt_no_tax ) ) AS '不含税金额',
            b.tax_rate AS '税率' 
        FROM
            csx_b2b_accounting.accounting_stock_detail_view0 a
            LEFT JOIN csx_basic_data.md_product_info b ON b.product_code = a.product_code 
        WHERE
            a.posting_time < '2020-08-01 00:00:00' 
            AND a.shipper_code = 'YHCSX' 
            AND a.reservoir_area_code <> 'TS01' 
        GROUP BY
            a.location_code,
            a.product_code 
        ) n 
    ) aaa 
GROUP BY
    aaa.地点,
    aaa.商品编码;


