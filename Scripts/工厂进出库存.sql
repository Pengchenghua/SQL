--损耗： 盘亏+报损-盘盈-退料差异
--出库： 调拨出库+调拨退货出库+领用出库+原料消耗
--损耗率：    损耗/出库
-- 限制过帐日期，工厂4日封帐

-- 移库类型剔除109A
 select
    province_code,
    province_name,
    j.dc_code,
   shop_name as dc_name,
    j.goods_code,
    goods_name,
    mrp_prop_key ,mrp_prop_value,
    department_id,
    department_name,
    category_large_code,
    category_large_name,
    end_qty,
    end_amt,
    first_qty,
    first_amt,
    `收货入库额`,
    `收货入库量`,
    `调拨入库额`,
    `调拨入库量`,
    `调拨退货入库额`,
    `调拨退货入库量`,
    `退货入库额`,
    `退货入库量`,
    `原料成品入库额`,
    `原料成品入库量`,
    `期初导入额`,
    `期初导入量`,
    `退货出库额`,
    `退货出库量`,
    `调拨出库额`,
    `调拨出库量`,
    `调拨退货出库额`,
    `调拨退货出库量`,
    `销售出库额`,
    `销售出库量`,
    `子品转母品额`,
    `子品转母品量`,
    `母品转子品额`,
    `母品转子品量`,
    `过帐盘盈额`,
    `过帐盘盈量`,
    `过帐盘亏额`,
    `过帐盘亏量`,
    `报损额`,
    `报损量`,
    `领用额`,
    `领用量`,
    `原料消耗额`,
    `原料消耗量`,
    `退料差异额`,
    `退料差异量`,
    `商品转码额`,
    `商品转码量`,
    `移库额`,
    `移库量`,
   ((coalesce(过帐盘亏量,0)+coalesce(报损量,0))-coalesce(过帐盘盈量,0)+coalesce(退料差异量,0)) AS loss_qty,
   ((coalesce(过帐盘亏量,0)+coalesce(报损量,0))-coalesce(过帐盘盈量,0)+coalesce(退料差异量,0))/coalesce(原料消耗量,0) AS loss_qty_rate,
   ((coalesce(过帐盘亏额,0)+coalesce(报损额,0))-coalesce(过帐盘盈额,0)+coalesce(退料差异额,0)) AS loss_amt,
   ((coalesce(过帐盘亏额,0)+coalesce(报损额,0))-coalesce(过帐盘盈额,0)+coalesce(退料差异额,0))/coalesce(原料消耗额,0) AS loss_amt_rate,
   (coalesce(原料消耗量,0)+coalesce(调拨出库量,0)+coalesce(调拨退货出库量,0)+coalesce(领用量,0)) out_qty,
   (coalesce(原料消耗额,0)+coalesce(调拨出库额,0)+coalesce(调拨退货出库额,0)+coalesce(领用额,0)) out_amt,
   ((coalesce(过帐盘亏量,0)+coalesce(报损量,0))-coalesce(过帐盘盈量,0)+coalesce(退料差异量,0))/(coalesce(原料消耗量,0)+coalesce(调拨出库量,0)+coalesce(调拨退货出库量,0)+coalesce(领用量,0)) AS out_loss_qty_rate,
   ((coalesce(过帐盘亏额,0)+coalesce(报损额,0))-coalesce(过帐盘盈额,0)+coalesce(退料差异额,0))/(coalesce(原料消耗额,0)+coalesce(调拨出库额,0)+coalesce(调拨退货出库额,0)+coalesce(领用额,0)) AS out_loss_amt_rate
 
from
    (
    select
        dc_code,
      --  dc_name,
        goods_code,
        a.unit,
        a.division_code,
        division_name,
        department_id,
        department_name,
        category_large_code,
        category_large_name,
        sum(qty)end_qty,
        sum(amt)end_amt
    from
        csx_dw.dws_wms_r_d_accounting_stock_m a
        --csx_dw.dws_wms_r_d_accounting_stock_operation_item_m 此表移动类型待确认
    where
        sdt = regexp_replace(to_date('${edate}'),'-','')
        -- and goods_code ='825121'
       and reservoir_area_code not in ('PD02','PD01','TS01')
    group by
        dc_code,
      --  dc_name,
        goods_code,
        a.unit,
        a.division_code,
        division_name,
        department_id,
        department_name,
        category_large_code,
        category_large_name ) j
left join (
    select
        dc_code,
        goods_code,
        sum(qty)first_qty,
        sum(amt)first_amt
    from
        csx_dw.dws_wms_r_d_accounting_stock_m a
    where
        sdt = regexp_replace(to_date(date_sub('${sdate}',1)),'-','')
         and reservoir_area_code not in ('PD02','PD01','TS01')
       --  and goods_code ='825121'
    group by
        dc_code,
        goods_code ) h on
    j.dc_code = h.dc_code
    and j.goods_code = h.goods_code
    --and j.reservoir_area_code = h.reservoir_area_code
left join ( select
		substr(to_date(posting_time),1,7) post_date,
        location_code as dc_code,
        a.product_code goods_code ,
        sum(case when move_type = '101A' then amt_no_tax*(1+tax_rate/100 ) end) `收货入库额`,
        sum(case when move_type = '101A' then txn_qty end) `收货入库量`,
        sum(case when move_type = '102A' then amt_no_tax*(1+tax_rate/100 )  end) `调拨入库额`,
        sum(case when move_type = '102A' then txn_qty end) `调拨入库量`,
        sum(case when move_type = '105A' then amt_no_tax*(1+tax_rate/100 )  end) `调拨退货入库额`,
        sum(case when move_type = '105A' then txn_qty end) `调拨退货入库量`,
        sum(case when move_type = '108A' then amt_no_tax*(1+tax_rate/100 )  end) `退货入库额`,
        sum(case when move_type = '108A' then txn_qty end) `退货入库量`,
        sum(case when move_type = '120A' then amt_no_tax*(1+tax_rate/100 )  end) `原料成品入库额`,
        sum(case when move_type = '120A' then txn_qty end) `原料成品入库量`,
        sum(case when move_type = '201A' then amt_no_tax*(1+tax_rate/100 )  end) `期初导入额`,
        sum(case when move_type = '201A' then txn_qty end) `期初导入量`,
        sum(case when move_type = '103A' then amt_no_tax*(1+tax_rate/100 )  end) `退货出库额`,
        sum(case when move_type = '103A' then txn_qty end) `退货出库量`,
        sum(case when move_type = '104A' then amt_no_tax*(1+tax_rate/100 )  end) `调拨出库额`,
        sum(case when move_type = '104A' then txn_qty end) `调拨出库量`,
        sum(case when move_type = '106A' then amt_no_tax*(1+tax_rate/100 )  end) `调拨退货出库额`,
        sum(case when move_type = '106A' then txn_qty end) `调拨退货出库量`,
        sum(case when move_type = '107A' then amt_no_tax*(1+tax_rate/100 )  end) `销售出库额`,
        sum(case when move_type = '107A' then txn_qty end) `销售出库量`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) ) end) `子品转母品额`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `子品转母品量`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) ) end) `母品转子品额`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `母品转子品量`,
        sum(case when move_type = '115A' then amt_no_tax*(1+tax_rate/100 )  end) `过帐盘盈额`,
        sum(case when move_type = '115A' then txn_qty end) `过帐盘盈量`,
        sum(case when move_type = '116A' then amt_no_tax*(1+tax_rate/100 )  end) `过帐盘亏额`,
        sum(case when move_type = '116A' then txn_qty end) `过帐盘亏量`,
        sum(case when move_type = '117A' then amt_no_tax*(1+tax_rate/100 )  end) `报损额`,
        sum(case when move_type = '117A' then txn_qty end) `报损量`,
        sum(case when move_type = '118A' then amt_no_tax*(1+tax_rate/100 )  end) `领用额`,
        sum(case when move_type = '118A' then txn_qty end) `领用量`,
        sum(case when move_type = '119A' then amt_no_tax*(1+tax_rate/100 )  end) `原料消耗额`,
        sum(case when move_type = '119A' then txn_qty end) `原料消耗量`,
        sum(case when move_type = '121A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) )  End) `退料差异额`,
        sum(case when move_type = '121A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `退料差异量`,
        sum(case when move_type = '202A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) ) end) `商品转码额`,
        sum(case when move_type = '202A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `商品转码量`,
        sum(case when move_type = '109A' then if(in_or_out = 0,amt_no_tax*(1+tax_rate/100 ) , 0) end) `移库额`,
        sum(case when move_type = '109A' then if(in_or_out = 0,txn_qty, 0) end) `移库量`
    from
       csx_dw.dwd_cas_r_d_accounting_stock_detail a 
    where
        sdt >= '20200801'
        and sdt <= '20200906'
        and posting_time <'2020-09-01 00:00:00.0'
        and posting_time >='2020-08-01 00:00:00.0'
        --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
        and move_type not in ('114A','110A','111A')
       -- and location_code ='W053'
        --排除盘点仓库存
        group by location_code,
        product_code,
        substr(to_date(posting_time),1,7))a on
    j.dc_code = a.dc_code
    and j.goods_code = a.goods_code
    --and j.reservoir_area_code = a.reservoir_area_code
left join (
    select
        location_code,
        shop_name,
        province_code,
        province_name
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and location_uses_code = '03' ) c on
    j.dc_code = c.location_code
    left join 
    (SELEct DISTINCT  factory_location_code ,product_code ,mrp_prop_key ,mrp_prop_value 
        from csx_dw.dws_mms_w_a_factory_bom_m where sdt=regexp_replace(to_date('${edate}'),'-','')) e on 
    j.dc_code=e.factory_location_code and j.goods_code=e.product_code
     left join 
    (SELEct product_code as goods_id ,product_name as goods_name,tax_rate from csx_dw.dws_basic_w_a_csx_product_info where sdt='curent' ) as k on 
    j.goods_code=k.goods_id
where
    shop_name is not null
   --   and J.dc_code = 'W053' --and a.goods_code='1040380'
    order by province_code,dc_code,category_large_code
 
   
--and j.goods_code='614'
;






select * 
from
        --csx_dw.dws_wms_r_d_accounting_stock_operation_item_m
        csx_dw.dwd_cas_r_d_accounting_stock_detail
    where
        sdt >= '20200501'
        and sdt <= '20200601'
        AND location_code ='W053'
        and posting_time <'2020-06-01 00:00:00.0'
        and move_type  in ('115A','116A')
        ;
    
    csx_dw.dwd_cas_r_d_accounting_stock_detail

select * from
        csx_dw.dws_wms_r_d_accounting_stock_operation_item_m where sdt>='20200501' and  dc_code ='W053' and credential_no ='PZ20200528030302';

SELEct DISTINCT  factory_location_code ,product_code ,mrp_prop_key ,mrp_prop_value from csx_dw.dws_mms_w_a_factory_bom_m where sdt='20200420' and product_code ='825121';
-- 按仓位展示 未剔除盘点仓


 select
    province_code,
    province_name,
    j.dc_code,
    j.dc_name,
    j.reservoir_area_code ,
        j.reservoir_area_name ,
    j.goods_code,
    j.goods_name,
    mrp_prop_key ,mrp_prop_value,
    department_id,
    department_name,
    category_large_code,
    category_large_name,
    end_qty,
    end_amt,
    first_qty,
    first_amt,
    `收货入库额`,
    `收货入库量`,
    `调拨入库额`,
    `调拨入库量`,
    `调拨退货入库额`,
    `调拨退货入库量`,
    `退货入库额`,
    `退货入库量`,
    `原料成品入库额`,
    `原料成品入库量`,
    `期初导入额`,
    `期初导入量`,
    `退货出库额`,
    `退货出库量`,
    `调拨出库额`,
    `调拨出库量`,
    `调拨退货出库额`,
    `调拨退货出库量`,
    `销售出库额`,
    `销售出库量`,
    `子品转母品额`,
    `子品转母品量`,
    `母品转子品额`,
    `母品转子品量`,
    `过帐盘盈额`,
    `过帐盘盈量`,
    `过帐盘亏额`,
    `过帐盘亏量`,
    `报损额`,
    `报损量`,
    `领用额`,
    `领用量`,
    `原料消耗额`,
    `原料消耗量`,
    `退料差异额`,
    `退料差异量`,
    `商品转码额`,
    `商品转码量`,
    `移库额`,
    `移库量`
from
    (
    select
        dc_code,
        dc_name,
        reservoir_area_code ,
        reservoir_area_name ,
        goods_code,
        goods_name,
        a.unit,
        a.division_code,
        division_name,
        department_id,
        department_name,
        category_large_code,
        category_large_name,
        sum(qty)end_qty,
        sum(amt)end_amt
    from
        csx_dw.dws_wms_r_d_accounting_stock_m a
    where
        sdt = '20200531'
     --  and reservoir_area_code not in ('PD02','PD01','TS01')
    group by
        dc_code,
        dc_name,
        reservoir_area_code ,
        reservoir_area_name ,
        goods_code,
        goods_name,
        a.unit,
        a.division_code,
        division_name,
        department_id,
        department_name,
        category_large_code,
        category_large_name ) j
left join (
    select
        dc_code,
        goods_code,
        reservoir_area_code ,
        reservoir_area_name ,
        sum(qty)first_qty,
        sum(amt)first_amt
    from
        csx_dw.dws_wms_r_d_accounting_stock_m a
    where
        sdt = '20200430'
     --    and reservoir_area_code not in ('PD02','PD01','TS01')
    group by
        dc_code,reservoir_area_code ,
        reservoir_area_name ,
        goods_code ) h on
    j.dc_code = h.dc_code
    and j.goods_code = h.goods_code
    and j.reservoir_area_code = h.reservoir_area_code
left join (
    select
        dc_code,
        goods_code,
        reservoir_area_code ,
        reservoir_area_name ,
        sum(case when move_type = '101A' then amt end) `收货入库额`,
        sum(case when move_type = '101A' then qty end) `收货入库量`,
        sum(case when move_type = '102A' then amt end) `调拨入库额`,
        sum(case when move_type = '102A' then qty end) `调拨入库量`,
        sum(case when move_type = '105A' then amt end) `调拨退货入库额`,
        sum(case when move_type = '105A' then qty end) `调拨退货入库量`,
        sum(case when move_type = '108A' then amt end) `退货入库额`,
        sum(case when move_type = '108A' then qty end) `退货入库量`,
        sum(case when move_type = '120A' then amt end) `原料成品入库额`,
        sum(case when move_type = '120A' then qty end) `原料成品入库量`,
        sum(case when move_type = '201A' then amt end) `期初导入额`,
        sum(case when move_type = '201A' then qty end) `期初导入量`,
        sum(case when move_type = '103A' then amt end) `退货出库额`,
        sum(case when move_type = '103A' then qty end) `退货出库量`,
        sum(case when move_type = '104A' then amt end) `调拨出库额`,
        sum(case when move_type = '104A' then qty end) `调拨出库量`,
        sum(case when move_type = '106A' then amt end) `调拨退货出库额`,
        sum(case when move_type = '106A' then qty end) `调拨退货出库量`,
        sum(case when move_type = '107A' then amt end) `销售出库额`,
        sum(case when move_type = '107A' then qty end) `销售出库量`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-amt, amt) end) `子品转母品额`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-qty, qty) end) `子品转母品量`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-amt, amt) end) `母品转子品额`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-qty, qty) end) `母品转子品量`,
        sum(case when move_type = '115A' then amt end) `过帐盘盈额`,
        sum(case when move_type = '115A' then qty end) `过帐盘盈量`,
        sum(case when move_type = '116A' then amt end) `过帐盘亏额`,
        sum(case when move_type = '116A' then qty end) `过帐盘亏量`,
        sum(case when move_type = '117A' then amt 
                when move_type='117B' then -amt end) `报损额`,
        sum(case when move_type = '117A' then qty 
        when move_type='117B' then -qty end) `报损量`,
        sum(case when move_type = '118A' then amt
                when  move_type = '118B' then -amt end) `领用额`,
        sum(case when move_type = '118A' then qty 
            move_type = '118B' then -qty end) `领用量`,
        sum(case when move_type = '119A' then amt end) `原料消耗额`,
        sum(case when move_type = '119A' then qty end) `原料消耗量`,
        sum(case when move_type = '121A' then if(in_or_out = 1,-amt, amt) End) `退料差异额`,
        sum(case when move_type = '121A' then if(in_or_out = 1,-amt, amt) end) `退料差异量`,
        sum(case when move_type = '202A' then if(in_or_out = 1,-amt, amt) end) `商品转码额`,
        sum(case when move_type = '202A' then if(in_or_out = 1,-qty, qty) end) `商品转码量`,
        sum(case when move_type = '109A' then if(in_or_out = 0,amt, 0) end) `移库额`,
        sum(case when move_type = '109A' then if(in_or_out = 0,qty, 0) end) `移库量`
    from
        csx_dw.dws_wms_r_d_accounting_stock_operation_item_m
    where
        sdt >= '20200301'
        and sdt <= '20200331'
        --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
        and move_type not in ('114A',
        '110A',
        '111A')
        --排除盘点仓库存

        group by dc_code,
        reservoir_area_code ,
        reservoir_area_name ,
        goods_code )a on
    j.dc_code = a.dc_code
    and j.goods_code = a.goods_code
    and j.reservoir_area_code = a.reservoir_area_code
left join (
    select
        location_code,
        shop_name,
        province_code,
        province_name
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and location_uses_code = '03' ) c on
    j.dc_code = c.location_code
    left join 
    (SELEct DISTINCT  factory_location_code ,product_code ,mrp_prop_key ,mrp_prop_value from csx_dw.dws_mms_w_a_factory_bom_m where sdt='20200420') e on 
    j.dc_code=e.factory_location_code and j.goods_code=e.product_code
where
    shop_name is not null
    and J.dc_code = 'W039'
   
--and j.goods_code='614'
;

SELECT
    shipped_location_code,
    shop_name,
    c.goods_code,
    qty,
    shipped_amt,
    d.product_code,
    mrp_prop_key,
    mrp_prop_value
FROM
    (
    SELECT
        shipped_location_code,
        shop_name,
        goods_code,
        qty,
        shipped_amt
    FROM
        (
        SELECT
            shipped_location_code,
            goods_code,
            sum(shipped_qty)qty,
            sum(shipped_qty*price)shipped_amt
        FROM
            csx_dw.wms_shipped_order
        WHERE
            sdt >= '20200301'
            AND sdt <= '20200331'
            AND shipped_type LIKE '调拨%'
        GROUP BY
            shipped_location_code,
            goods_code)a
    JOIN (
        SELECT
            location_code,
            shop_name
        FROM
            csx_dw.csx_shop
        WHERE
            sdt = 'current'
            AND location_type_code = '2') b ON
        a.shipped_location_code = b.location_code) c
LEFT JOIN (
    SELECT
        factory_location_code,
        goods_code,
        product_code,
        mrp_prop_key,
        mrp_prop_value
    FROM
        csx_dw.dws_mms_w_a_factory_bom_m
    WHERE
        sdt = '20200415'
        and mrp_prop_key in ('3061',
        '3010')) d ON
    c.shipped_location_code = d.factory_location_code
    AND c.goods_code = d.goods_code
LEFT JOIN 
(SELEct DISTINCT factory_location_code ,product_code ,mrp_prop_key ,mrp_prop_value from csx_dw.dws_mms_w_a_factory_bom_m where sdt='20200420') e on j.;

SELECT
    shipped_location_code,
    shop_name,
    c.goods_name,
    c.goods_code,
    qty,
    shipped_amt,
    d.product_code,
    mrp_prop_key,
    mrp_prop_value
FROM
    (
    SELECT
        shipped_location_code,
        shop_name,
        goods_code,
        goods_name,
        qty,
        shipped_amt
    FROM
        (
        SELECT
            shipped_location_code,
            goods_code,
            goods_name,
            sum(shipped_qty)qty,
            sum(shipped_qty*price)shipped_amt
        FROM
            csx_dw.wms_shipped_order
        WHERE
            sdt >= '20200301'
            AND sdt <= '20200331'
            AND shipped_type LIKE '调拨%'
            -- and goods_code='472'
            and shipped_location_code = 'W039'
        GROUP BY
            shipped_location_code,
            goods_code,
            goods_name)a
    JOIN (
        SELECT
            location_code,
            shop_name
        FROM
            csx_dw.csx_shop
        WHERE
            sdt = 'current'
            AND location_type_code = '2' ) b ON
        a.shipped_location_code = b.location_code) c
JOIN (
    SELECT
        factory_location_code,
        goods_code,
        goods_name,
        product_code,
        mrp_prop_key,
        mrp_prop_value
    FROM
        csx_dw.dws_mms_w_a_factory_bom_m
    WHERE
        sdt = '20200415'
        and mrp_prop_key in ('3061',
        '3010')
        and goods_code in ('1062192',
        '894880',
        '852458',
        '1066234',
        '894875',
        '846667')
        AND factory_location_code = 'W039') d ON
    c.shipped_location_code = d.factory_location_code
    AND c.goods_code = d.goods_code
where
    mrp_prop_key IS NOT NULL ;

select
    *
from
    csx_dw.dws_crm_w_a_customer_m
where
    customer_name like '福建亿力%'
    and sdt = '20200415' ;

SELECT
    *
FROM
    csx_dw.dws_mms_r_a_factory_order
where
    sdt >= '20200301'
    and goods_reality_receive_qty != goods_plan_receive_qty;
    

select *  from
        csx_dw.dws_wms_r_d_accounting_stock_operation_item_m
    where
        sdt >= '20200301'
        and sdt <= '20200331' and dc_code ='W048' and goods_code ='620';
        
  
   select
    province_code,
    province_name,
    j.dc_code,
   shop_name as dc_name,
    j.goods_code,
    goods_name,
    mrp_prop_key ,mrp_prop_value,
    department_id,
    department_name,
    category_large_code,
    category_large_name,
    end_qty,
    end_amt,
    first_qty,
    first_amt,
    `收货入库额`,
    `收货入库量`,
    `调拨入库额`,
    `调拨入库量`,
    `调拨退货入库额`,
    `调拨退货入库量`,
    `退货入库额`,
    `退货入库量`,
    `原料成品入库额`,
    `原料成品入库量`,
    `期初导入额`,
    `期初导入量`,
    `退货出库额`,
    `退货出库量`,
    `调拨出库额`,
    `调拨出库量`,
    `调拨退货出库额`,
    `调拨退货出库量`,
    g.shipped_amt as `销售出库额`,
    g.shipped_qty as `销售出库量`,
    `子品转母品额`,
    `子品转母品量`,
    `母品转子品额`,
    `母品转子品量`,
    `过帐盘盈额`,
    `过帐盘盈量`,
    `过帐盘亏额`,
    `过帐盘亏量`,
    `报损额`,
    `报损量`,
    `领用额`,
    `领用量`,
    `原料消耗额`,
    `原料消耗量`,
    `退料差异额`,
    `退料差异量`,
    `商品转码额`,
    `商品转码量`,
    `移库额`,
    `移库量`,
   ((coalesce(过帐盘亏量,0)+coalesce(报损量,0))-coalesce(过帐盘盈量,0)+coalesce(退料差异量,0)) AS loss_qty,
   ((coalesce(过帐盘亏量,0)+coalesce(报损量,0))-coalesce(过帐盘盈量,0)+coalesce(退料差异量,0))/coalesce(原料消耗量,0) AS loss_qty_rate,
   ((coalesce(过帐盘亏额,0)+coalesce(报损额,0))-coalesce(过帐盘盈额,0)+coalesce(退料差异额,0)) AS loss_amt,
   ((coalesce(过帐盘亏额,0)+coalesce(报损额,0))-coalesce(过帐盘盈额,0)+coalesce(退料差异额,0))/coalesce(原料消耗额,0) AS loss_amt_rate,
   (coalesce(原料消耗量,0)+coalesce(调拨出库量,0)+coalesce(调拨退货出库量,0)+coalesce(领用量,0)) out_qty,
   (coalesce(原料消耗额,0)+coalesce(调拨出库额,0)+coalesce(调拨退货出库额,0)+coalesce(领用额,0)) out_amt,
   ((coalesce(过帐盘亏量,0)+coalesce(报损量,0))-coalesce(过帐盘盈量,0)+coalesce(退料差异量,0))/(coalesce(原料消耗量,0)+coalesce(调拨出库量,0)+coalesce(调拨退货出库量,0)+coalesce(领用量,0)) AS out_loss_qty_rate,
   ((coalesce(过帐盘亏额,0)+coalesce(报损额,0))-coalesce(过帐盘盈额,0)+coalesce(退料差异额,0))/(coalesce(原料消耗额,0)+coalesce(调拨出库额,0)+coalesce(调拨退货出库额,0)+coalesce(领用额,0)) AS out_loss_amt_rate
 
from
    (
    select
        dc_code,
      --  dc_name,
        goods_code,
        a.unit,
        a.division_code,
        division_name,
        department_id,
        department_name,
        category_large_code,
        category_large_name,
        sum(qty)end_qty,
        sum(amt)end_amt
    from
        csx_dw.dws_wms_r_d_accounting_stock_m a
        --csx_dw.dws_wms_r_d_accounting_stock_operation_item_m 此表移动类型待确认
    where
        sdt = regexp_replace(to_date('${edate}'),'-','')
        -- and goods_code ='825121'
       and reservoir_area_code not in ('PD02','PD01','TS01')
    group by
        dc_code,
      --  dc_name,
        goods_code,
        a.unit,
        a.division_code,
        division_name,
        department_id,
        department_name,
        category_large_code,
        category_large_name ) j
left join (
    select
        dc_code,
        goods_code,
        sum(qty)first_qty,
        sum(amt)first_amt
    from
        csx_dw.dws_wms_r_d_accounting_stock_m a
    where
        sdt = regexp_replace(to_date(date_sub('${sdate}',1)),'-','')
         and reservoir_area_code not in ('PD02','PD01','TS01')
       --  and goods_code ='825121'
    group by
        dc_code,
        goods_code ) h on
    j.dc_code = h.dc_code
    and j.goods_code = h.goods_code
    --and j.reservoir_area_code = h.reservoir_area_code
left join ( select
        location_code as dc_code,
        a.product_code goods_code ,
        sum(case when move_type = '101A' then txn_amt end) `收货入库额`,
        sum(case when move_type = '101A' then txn_qty end) `收货入库量`,
        sum(case when move_type = '102A' then txn_amt end) `调拨入库额`,
        sum(case when move_type = '102A' then txn_qty end) `调拨入库量`,
        sum(case when move_type = '105A' then txn_amt end) `调拨退货入库额`,
        sum(case when move_type = '105A' then txn_qty end) `调拨退货入库量`,
        sum(case when move_type = '108A' then txn_amt end) `退货入库额`,
        sum(case when move_type = '108A' then txn_qty end) `退货入库量`,
        sum(case when move_type = '120A' then txn_amt end) `原料成品入库额`,
        sum(case when move_type = '120A' then txn_qty end) `原料成品入库量`,
        sum(case when move_type = '201A' then txn_amt end) `期初导入额`,
        sum(case when move_type = '201A' then txn_qty end) `期初导入量`,
        sum(case when move_type = '103A' then txn_amt end) `退货出库额`,
        sum(case when move_type = '103A' then txn_qty end) `退货出库量`,
        sum(case when move_type = '104A' then txn_amt end) `调拨出库额`,
        sum(case when move_type = '104A' then txn_qty end) `调拨出库量`,
        sum(case when move_type = '106A' then txn_amt end) `调拨退货出库额`,
        sum(case when move_type = '106A' then txn_qty end) `调拨退货出库量`,
        sum(case when move_type = '107A' then txn_amt end) `销售出库额`,
        sum(case when move_type = '107A' then txn_qty end) `销售出库量`,
--        sum(case when move_type = '114A' then txn_amt end) `出库在途额`,
--        sum(case when move_type = '114A' then txn_qty end) `出库在途量`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-txn_amt, txn_amt) end) `子品转母品额`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `子品转母品量`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-txn_amt, txn_amt) end) `母品转子品额`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `母品转子品量`,
        sum(case when move_type = '115A' then txn_amt end) `过帐盘盈额`,
        sum(case when move_type = '115A' then txn_qty end) `过帐盘盈量`,
        sum(case when move_type = '116A' then txn_amt end) `过帐盘亏额`,
        sum(case when move_type = '116A' then txn_qty end) `过帐盘亏量`,
        sum(case when move_type = '117A' then txn_amt end) `报损额`,
        sum(case when move_type = '117A' then txn_qty end) `报损量`,
        sum(case when move_type = '118A' then txn_amt end) `领用额`,
        sum(case when move_type = '118A' then txn_qty end) `领用量`,
        sum(case when move_type = '119A' then txn_amt end) `原料消耗额`,
        sum(case when move_type = '119A' then txn_qty end) `原料消耗量`,
        sum(case when move_type = '121A' then if(in_or_out = 1,-txn_amt, txn_amt)  End) `退料差异额`,
        sum(case when move_type = '121A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `退料差异量`,
        sum(case when move_type = '202A' then if(in_or_out = 1,-txn_amt, txn_amt) end) `商品转码额`,
        sum(case when move_type = '202A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `商品转码量`,
        sum(case when move_type = '109A' then if(in_or_out = 0,txn_amt, 0) end) `移库额`,
        sum(case when move_type = '109A' then if(in_or_out = 0,txn_qty, 0) end) `移库量`
    from
       csx_dw.dwd_cas_r_d_accounting_stock_detail a 
     left join 
     (select shipped_location_code , ws.goods_code,sum(ws.shipped_qty)as shipped_qty,sum(ws.amount) as shipped_amt from csx_dw.wms_shipped_order as ws 
     where ws.shipped_type LIKE '%销售%'  and  sdt >= '20200501'
        and sdt < '20200601'
        group by shipped_location_code,goods_code) as g on a.location_code=g.shipped_location_code and a.product_code=g.goods_code
    where
        sdt >= '20200501'
        and sdt < '20200601'
        and posting_time <'2020-06-01 00:00:00.0'
        --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
        and move_type not in ('110A','111A')
       -- and location_code ='W053'
        --排除盘点仓库存
        group by location_code,
        product_code )a on
    j.dc_code = a.dc_code
    and j.goods_code = a.goods_code
    --and j.reservoir_area_code = a.reservoir_area_code
left join (
    select
        location_code,
        shop_name,
        province_code,
        province_name
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and location_uses_code = '01' ) c on
    j.dc_code = c.location_code
    left join 
    (SELEct DISTINCT  factory_location_code ,product_code ,mrp_prop_key ,mrp_prop_value from csx_dw.dws_mms_w_a_factory_bom_m where sdt=regexp_replace(to_date('${edate}'),'-','')) e on 
    j.dc_code=e.factory_location_code and j.goods_code=e.product_code
     left join 
    (SELEct goods_id ,goods_name from csx_dw.goods_m where sdt=regexp_replace(to_date('${edate}'),'-','')) as k on 
    j.goods_code=k.goods_id
where 1=1
    --shop_name is not null
     and J.dc_code = 'W0A5'
    --and a.goods_code='1040380'
    order by province_code,dc_code,category_large_code
 
   
--and j.goods_code='614'
;



select sdt, goods_code ,sum(sales_value),SUM(sales_qty ) from csx_dw.dc_sale_inventory where sdt>='20200501' and sdt<'20200601' and  goods_code ='994929'  and dc_code ='W0A5'GROUP BY goods_code ,sdt;

SELECT sdt, sum(sales_value),SUM(sales_qty ),SUM(sales_cost )  FROM csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200501' and sdt<='20200531' and goods_code ='994929' and dc_code ='W0A5' group by sdt;

select * from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200501' and sdt<='20200531' and goods_code ='994929' and dc_code ='W0A5' group by sdt;

select * from csx_dw.dwd_wms_r_d_shipped_order_detail where order_no='OM200518007100' and sdt>='20200501';
select * from csx_dw.wms_shipped_order where order_no='OM200518007100' and sdt>='20200501';

SELECT
    a.mon,
    division_code ,
    division_name ,
    department_code ,
    department_name ,
    sales_value ,
    sales_qty ,
    sales_cost,
    check_amt,
    check_qty from (
    SELECT
        SUBSTRING(sdt, 1, 6) as mon, division_code , 
        division_name ,
        department_code ,
        department_name ,
        sum(sales_value)sales_value ,
        SUM(sales_qty) sales_qty ,
        SUM(sales_cost) sales_cost
    FROM
        csx_dw.dws_sale_r_d_customer_sale
    where
        sdt >= '20200101'
        and sdt <= '20200531'
        and dc_code = 'W0A5'
    group by
        SUBSTRING(sdt, 1, 6), division_code , division_name , department_code , department_name )a
left join (
    select
        substring(regexp_replace(to_date(posting_time), '-', ''), 1, 6)as mon, purchase_group_code , SUM(amt) check_amt, sum(dsii.qty) check_qty
    from
        csx_ods.source_sync_r_d_data_sync_inventory_item dsii
    where
        posting_time >= '2020-01-01 00:00:00'
        and posting_time <'2020-06-01 00:00:00'
        and location_code = 'W0A5'
    group by
        substring(regexp_replace(to_date(posting_time), '-', ''), 1, 6), purchase_group_code) b on
    a.mon = b.mon
    and a.department_code = purchase_group_code ;
    

