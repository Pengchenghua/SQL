  SELECT
	location_code,
	company_code,
	product_code, 
    begin_qty ,
    begin_amt_no_tax ,
    begin_amt_tax ,
    end_qty ,
    end_amt_no_tax ,
    end_amt_tax ,
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
    from (
SELECT
	location_code,
	company_code,
	product_code,
	sum(begin_qty) as begin_qty ,
    sum(begin_amt_no_tax) as begin_amt_no_tax ,
    sum(begin_amt_tax) as begin_amt_tax ,
    sum(end_qty) AS end_qty ,
    sum(end_amt_no_tax) AS end_amt_no_tax ,
    sum(end_amt_tax) AS end_amt_tax 
FROM
(SELECT

 location_code,
 company_code,
 reservoir_area_code,
 product_code ,
 0 as begin_qty ,
 0 as begin_amt_no_tax ,
 0 as begin_amt_tax ,
 sum( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS end_qty ,
 sum( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS end_amt_no_tax ,
 sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS end_amt_tax 
FROM
csx_dw.dwd_cas_r_d_accounting_stock_detail
where posting_time < '2021-01-01 00:00:00'
 and sdt<='20210110'
 -- and  reservoir_area_code not in ('PD01','PD02','TS01','CY01')
 and move_type not in ('114A','110A','111A')
GROUP BY
 location_code,
 company_code,
 product_code 
 union all 
 SELECT
 location_code,
 company_code,
 product_code ,
 sum( IF ( in_or_out = 0, txn_qty, IF ( in_or_out = 1,- txn_qty, 0 ) ) ) AS begin_qty ,
 sum( IF ( in_or_out = 1, -amt_no_tax, amt_no_tax) ) AS begin_amt_no_tax ,
 sum( IF ( in_or_out = 1, -amt_no_tax*(1+tax_rate/100) , amt_no_tax*(1+tax_rate/100)) ) AS begin_amt_tax ,
 0 as  end_qty,
 0 as end_amt_no_tax,
 0 as end_amt_tax 
FROM
csx_dw.dwd_cas_r_d_accounting_stock_detail
where posting_time < '2020-12-01 00:00:00'
 and sdt<='20210110'
 -- and  reservoir_area_code not in ('PD01','PD02','TS01','CY01')
  and  move_type not in ('114A','110A','111A')
GROUP BY
 location_code,
 company_code,
 product_code
  ) a 
group by 
    location_code,
	company_code,
	product_code )a
  left join 
  ( select
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
        and sdt < '20210114'
        and posting_time <'2021-01-01 00:00:00.0'
        and posting_time >='2020-12-01 00:00:00.0'
        --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
        and move_type not in ('114A','110A','111A')
       -- and location_code ='W053'
        --排除盘点仓库存
        group by location_code,
        product_code,
        substr(to_date(posting_time),1,7)
        ) a
 left join 
 (select * from csx_dw.dws_basic_w_a_csx_product_info cpi where sdt='current')b 
     on a.product_code=b.product_code and a.location_code=shop_code
LEFT JOIN 
(select warehouse_code,reservoir_area_code,reservoir_area_name from csx_ods.source_wms_w_a_wms_reservoir_area wra)c 
    on a.reservoir_area_code=c.reservoir_area_code and c.warehouse_code=location_code  ;
   
  
   
   