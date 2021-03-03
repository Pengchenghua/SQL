
select
	mon,
    dist_code,
    dist_name,
    a.dc_code,
    shop_name as dc_name,
    a.goods_code,
    goods_name,
    b.unit_name,
    tax_rate,
    sum(`收货入库额`) AS `收货入库额`,
    sum(`收货入库量`) AS `收货入库量`,
    sum(`调拨入库额`) AS `调拨入库额`,
    sum(`调拨入库额(不含税)`) AS `调拨入库额(不含税)`,
    sum(`调拨入库量`) AS `调拨入库量`,   
    sum(`过帐盘盈额`) AS `过帐盘盈额`,
    sum(`过帐盘盈量`) AS `过帐盘盈量`,
    sum(`过帐盘亏额`)*-1 AS `过帐盘亏额`,
    sum(`过帐盘亏量`)*-1 AS `过帐盘亏量`,
    sum(coalesce(`过帐盘盈量`,0)-coalesce(`过帐盘亏量`,0)) as `盘点差异量` ,
    sum(coalesce(`过帐盘盈额(不含税)`,0)-coalesce(`过帐盘亏额(不含税)`,0)) as `盘点差异金额`,
	sum(sales_value)sales_value,
    sum(sales_qty)  sales_qty,
    sum(amt_no_tax_sales)amt_no_tax_sales
    from (
select
		substr(regexp_replace(to_date(posting_time),'-',''),1,6) as mon,
		dist_code,
		dist_name,
        a.location_code as dc_code,
        shop_name,
        a.product_code goods_code ,
        sum(case when move_type = '101A' then amt_no_tax*(1+tax_rate/100 ) end) `收货入库额`,
        sum(case when move_type = '101A' then txn_qty end) `收货入库量`,
        sum(case when move_type = '102A' then amt_no_tax*(1+tax_rate/100 )  end) `调拨入库额`,
        sum(case when move_type = '102A' then amt_no_tax   end) `调拨入库额(不含税)`,
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
        sum(case when move_type = '107A' then amt_no_tax   end) `销售出库额(不含税)`,
        sum(case when move_type = '107A' then txn_qty end) `销售出库量`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) ) end) `子品转母品额`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `子品转母品量`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) ) end) `母品转子品额`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `母品转子品量`,
        sum(case when move_type = '115A' then amt_no_tax*(1+tax_rate/100 )  end) `过帐盘盈额`,
        sum(case when move_type = '115A' then amt_no_tax   end) `过帐盘盈额(不含税)`,
        sum(case when move_type = '115A' then txn_qty end) `过帐盘盈量`,
        sum(case when move_type = '116A' then amt_no_tax*(1+tax_rate/100 )  end) `过帐盘亏额`,
        sum(case when move_type = '116A' then amt_no_tax   end) `过帐盘亏额(不含税)`,
        sum(case when move_type = '116A' then txn_qty end) `过帐盘亏量`,
        sum(case when move_type = '117A' then amt_no_tax*(1+tax_rate/100 )  end) `报损额`,
        sum(case when move_type = '117A' then txn_qty end) `报损量`,
        0 sales_value,
        0 sales_qty,
        0 amt_no_tax_sales
    from
       csx_dw.dwd_cas_r_d_accounting_stock_detail a 
      join 
      (select
	s.location_code,
	s.shop_name,
	dist_code,
	dist_name
from
	csx_dw.csx_shop s
where
	sdt = 'current'
	and s.location_code like 'E%'
	and zone_id = '3') s on a.location_code =s.location_code 
    where
        sdt >= '20200101'
        and sdt <= '20201217'
        and posting_time <='2020-12-13 00:00:00.0'
        and posting_time >='2020-01-01 00:00:00.0'
        --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
        and move_type not in ('114A','110A','111A')
       -- and location_code ='W053'
        --排除盘点仓库存
        group by a.location_code,
        product_code ,
        shop_name,
        substr(regexp_replace(to_date(posting_time),'-',''),1,6)
        ,dist_code,dist_name
    union all 
 select    
 	substr( sdt ,1,6) as mon,
		dist_code,
		dist_name,
         dc_code,
        shop_name,
        goods_code ,
        0 `收货入库额`,
        0 `收货入库量`,
        0  `调拨入库额`,
        0 `调拨入库额(不含税)`,
        0 `调拨入库量`,
        0  `调拨退货入库额`,
        0 `调拨退货入库量`,
        0  `退货入库额`,
        0 `退货入库量`,
        0  `原料成品入库额`,
        0 `原料成品入库量`,
        0 `期初导入额`,
        0 `期初导入量`,
        0 `退货出库额`,
        0 `退货出库量`,
        0 `调拨出库额`,
        0 `调拨出库量`,
        0 `调拨退货出库额`,
        0 `调拨退货出库量`,
        0 `销售出库额`,
        0  `销售出库额(不含税)`,
        0 `销售出库量`,
        0   `子品转母品额`,
        0 `子品转母品量`,
        0  `母品转子品额`,
        0 `母品转子品量`,
        0 `过帐盘盈额`,
        0  `过帐盘盈额(不含税)`,
        0`过帐盘盈量`,
        0 `过帐盘亏额`,
        0 `过帐盘亏额(不含税)`,
        0`过帐盘亏量`,
        0 `报损额`,
        0`报损量`,
        sum(sales_value) sales_value,
        sum(sales_qty) sales_qty,
        sum(excluding_tax_sales) amt_no_tax_sales
       from csx_dw.dws_sale_r_d_customer_sale  a
        join 
      (select
	s.location_code,
	s.shop_name,
	dist_code,
	dist_name
from
	csx_dw.csx_shop s
where
	sdt = 'current'
	and s.location_code like 'E%'
	and zone_id = '3') s on a.dc_code=s.location_code
      where sdt>='20200101'
      	and sdt<='20201213'
  group by     	
      	substr(sdt,1,6) ,
		dist_code,
        dist_name,
         dc_code,
        shop_name,
        goods_code
 ) a 
 join 
 (select b.goods_id ,goods_name,b.unit_name,b.tax_rate from csx_dw.dws_basic_w_a_csx_product_m b  where sdt='current') b on a.goods_code=b.goods_id 
group by mon,
    dist_code,
    dist_name,
    a.dc_code,
    tax_rate,
   shop_name ,
    a.goods_code,
    goods_name,
    b.unit_name;




