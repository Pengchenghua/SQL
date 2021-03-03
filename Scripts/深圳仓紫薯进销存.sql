-- ：2020 年入库、销售、报损、盘盈亏，最终要看下毛利情况、损耗率
-- ：2020 年入库、销售、报损、盘盈亏，最终要看下毛利情况、损耗率

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
    sum(`领用额`) as `领用额`,
    sum(`领用量`) as `领用量`,
    sum(`退货出库额`) as 退货出库额,
    sum(`退货出库量`) as 退货出库量,
    sum(`调拨出库额`) as `调拨出库额`,
    sum(`调拨出库量` ) as `调拨出库量`,
    sum(`调拨退货出库额` ) as `调拨退货出库额`,
    sum( `调拨退货出库量`) as `调拨退货出库量`,
    sum(`原料消耗额` ) as  `原料消耗额`,
    sum(`原料消耗量`  ) as `原料消耗量`,
    sum(`退料差异额` ) as `退料差异额`,
    sum(`退料差异量` ) as `退料差异量`,
    sum(`商品转码额` ) as  `商品转码额`,
    sum( `报损额`) as `报损额`,
    sum(`报损量`) as `报损量`,
    sum(`过帐盘盈额`) AS `过帐盘盈额`,
    sum(`过帐盘盈量`) AS `过帐盘盈量`,
    sum(`过帐盘亏额`)*-1 AS `过帐盘亏额`,
    sum(`过帐盘亏量`)*-1 AS `过帐盘亏量`,
    sum(coalesce(`过帐盘盈量`,0)-coalesce(`过帐盘亏量`,0)) as `盘点差异量` ,
    sum(coalesce(`过帐盘盈额(不含税)`,0)-coalesce(`过帐盘亏额(不含税)`,0)-`报损额(未税)`) as `盘点差异金额(未税)`,
    sum(coalesce(`过帐盘盈额`,0)-(coalesce(`过帐盘亏额`,0)+coalesce(`报损额`,0)))   as `盘点差异金额`,
	sum(sales_cost) as sales_cost,
    sum(sales_value) as sales_value,
    sum(sales_qty) as  sales_qty,
    sum(profit) as profit ,
    sum(amt_no_tax_sales)as  amt_no_tax_sales,
    
    sum(amt_no_tax_cost) as amt_no_tax_cost,
    sum(amt_no_tax_profit) as  amt_no_tax_profit
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
        sum(case when move_type = '117A' then amt_no_tax  end) `报损额(未税)`,
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
 		sum(case when move_type = '109A' then if(in_or_out = 0,txn_qty, 0) end) `移库量`,
        0 as sales_cost,
    	0 as sales_value,
        0 as sales_qty,
        0 as amt_no_tax_sales,
        0 as profit ,
        0 as amt_no_tax_cost,
        0 as amt_no_tax_profit
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
	and dist_code ='20'
	-- and s.location_code IN ('W0Q3','W0R2','W0J7','W0Q4')
	 ) s on a.location_code =s.location_code 
    where
        sdt >= '20200101'
        and sdt <= '20210110'
        and posting_time <='2020-12-31 00:00:00.0'
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
        0 `调拨入库额`,
        0 `调拨入库额(不含税)`,
        0 `调拨入库量`,
        0 `调拨退货入库额`,
        0 `调拨退货入库量`,
        0 `退货入库额`,
        0 `退货入库量`,
        0 `原料成品入库额`,
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
        0 `销售出库额(不含税)`,
        0 `销售出库量`,
        0 `子品转母品额`,
        0 `子品转母品量`,
        0 `母品转子品额`,
        0 `母品转子品量`,
        0 `过帐盘盈额`,
        0 `过帐盘盈额(不含税)`,
        0 `过帐盘盈量`,
        0 `过帐盘亏额`,
        0 `过帐盘亏额(不含税)`,
        0 `过帐盘亏量`,
        0 `报损额`,
        0 `报损额(未税)`,
        0 `报损量`,
        0 `领用额`,
        0 `领用量`,
        0 `原料消耗额`,
        0 `原料消耗量`,
        0 `退料差异额`,
        0 `退料差异量`,
        0 `商品转码额`,
        0 `商品转码量`,
 		0 `移库额`,
 		0 `移库量`,
        sum(sales_cost) as sales_cost,
        sum(sales_value) as  sales_value,
        sum(sales_qty) as  sales_qty,
        sum(excluding_tax_sales)as  amt_no_tax_sales,
        sum(profit) as profit ,
        sum(excluding_tax_cost) as amt_no_tax_cost,
        sum(excluding_tax_profit) as  amt_no_tax_profit
       from csx_dw.dws_sale_r_d_detail  a
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
	--and s.location_code like 'E%'
	and dist_code ='20'
	-- and location_code  in  ('W0Q3','W0R2','W0J7','W0Q4')
	) s on a.dc_code=s.location_code
      where sdt>='20200101'
      	and sdt<='20201231'
  group by     	
      	substr(sdt,1,6) ,
		dist_code,
        dist_name,
         dc_code,
        shop_name,
        goods_code
 ) a 
 join 
 (select
	b.goods_id ,
	goods_name,
	b.unit_name,
	b.tax_rate
from
	csx_dw.dws_basic_w_a_csx_product_m b
where
	sdt = 'current'
	and b.goods_id in ('894871','244430','987502')
	--   杂粮
	--and b.goods_id in ('1082620','1082619','1082635','1082055','1082614','1082624','1082636','1082659','1082056','1082057','1082609','1082610','1082611','1082613','1082615','1082616','1082617','1082618','1082643','1082645','1082662','1089263','1233176','1116023','1231341','1231342','1231343','1231344','1231345','1231346','1231347','1231349','1231350','1231351','1231353','1231354','1231355','1233912','1233913','1233915','982034','4','323','796935','842429','317282','1081380','1064020','3','702131','530586','191586','263322','1132619','8763','6','318','266','2624','1081377','1082630','270','1150523','8764')
	) b on a.goods_code=b.goods_id 
group by mon,
    dist_code,
    dist_name,
    a.dc_code,
    tax_rate,
   shop_name ,
    a.goods_code,
    goods_name,
    b.unit_name;
    
 select sum(sales_value) from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200101' and goods_code in ('894871','244430','987502') and dc_code in ('W0Q3','W0R2');
 
 select substr(sdt,1,6),sdt,sum(price * receive_qty) from csx_dw.wms_entry_order 
 where sdt>='20200101' and sdt<='20201224' 
 and goods_code in ( '244430' ) 
 and receive_location_code in ('W0Q3') 
 group by  substr(sdt,1,6),sdt;
 
 select entry_type_code, substr(sdt,1,6),sum(price * receive_qty) from csx_dw.wms_entry_order 
 where sdt>='20200101' and sdt<='20201224' 
 and goods_code in ( '244430' ) 
 and receive_location_code in ('W0Q3')
 --and entry_type_code like 'P%'
 group by  substr(sdt,1,6),
entry_type_code;

 select *   from
       csx_dw.dwd_cas_r_d_accounting_stock_detail a where location_code ='W0R2';
       
 select *   from
       csx_dw.dws_sale_r_d_customer_sale a where dc_code ='W0R2';
       
      
 