-- CONNECTION: name=Hadoop - HIVE

 --SEt sdate = 20190701;

--SET edate = 20190731;

SELECT
 	dist,
    sflag,
    regexp_replace(cust_id,
    '(^0*)',
    '')cust_id ,
    cust_name,
    round(SUM(sale_order),
    2)sale_order,
    SUM(sale_ods)sale_ods,
    SUM(profit_order)profit_order,
    SUM(profit_ods)profit_ods,
    CASE
        WHEN round(SUM(sale_order),
        2)>SUM(sale_ods) THEN concat('sale_ODS表数据缺失||',
        CAST(round(SUM(sale_order)-SUM(sale_ods),
        2) AS STRING))
        WHEN round(SUM(sale_order),
        2)<SUM(sale_ods) THEN concat('sale_ODS表数据多于order_item||',
        CAST(ABS(round(SUM(sale_order)-SUM(sale_ods), 2))AS STRING))
        ELSE '数据一致'
    END note
FROM
    (
    SELECT
     
        sflag ,
        dist,
        regexp_replace(customer_no,
        '(^0*)',
        '')cust_id ,
        cust_name,
        SUM(sales_value )sale_order,
        SUM(profit)profit_order,
        0 sale_ods,
        0 profit_ods
    FROM
        csx_dw.sale_b2b_item a
    JOIN (
        SELECT
            cust_id ,
            cust_name,
            sflag,
            dist
        FROM
            csx_ods.b2b_customer_new 
        WHERE
            sflag IN('${sflag}') 
            AND cust_id != '910001'
            ) b ON
        regexp_replace( a.customer_no,
        '(^0*)',
        '' )= regexp_replace( cust_id,
        '(^0*)',
        '' )
        AND sales_type !='md'
        AND sdt >=${hiveconf:sdate}
        AND sdt <= ${hiveconf:edate}
    GROUP BY
        
        sflag,
        regexp_replace(customer_no,
        '(^0*)',
        '') ,dist,
        cust_name
UNION ALL
    SELECT
       
        b.sflag,
        dist,
        regexp_replace(a.cust_id,
        '(^0*)',
        '')cust_id ,
        cust_name,
        0 sale_order,
        0 profit_order,
        SUM(tax_salevalue) sale_ods,
        SUM(tax_profit) profit_ods
    FROM
        csx_ods.sale_b2b_dtl_fct a
    JOIN (
        SELECT
            cust_id ,
            cust_name,
            sflag,
            dist
        FROM
            csx_ods.b2b_customer_new 
        WHERE
            sflag IN('${sflag}')
            AND cust_id != '910001'
            ) b ON
        regexp_replace(a.cust_id,
        '(^0*)',
        '')= regexp_replace(b.cust_id ,
        '(^0*)',
        '')
        AND sdt >= ${hiveconf:sdate}
        AND sdt <= ${hiveconf:edate}
        AND a.sflag != 'md'
    GROUP BY
       
        b.sflag,
        regexp_replace(a.cust_id,
        '(^0*)',
        '') ,
        cust_name,dist ) a
GROUP BY
    dist,
    sflag,
    regexp_replace(cust_id,
    '(^0*)',
    '') ,
    cust_name;
