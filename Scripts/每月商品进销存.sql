--损耗： 盘亏+报损-盘盈-退料差异
--出库： 调拨出库+调拨退货出库+领用出库+原料消耗
--损耗率：    损耗/出库
-- 限制过帐日期，工厂4日封帐

-- 移库类型剔除109A
 select
	post_date,
   a.dc_code , 
    a.goods_code ,
    goods_name,
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
	sales_value ,
    sales_qty 
from
    ( select
		substr(regexp_replace(to_date(posting_time),'-',''),1,6) as  post_date,
        location_code as dc_code,
        a.product_code as goods_code ,
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
        sdt >= '20200101'
        and sdt <= '20210115'
        and posting_time <'2021-01-15 00:00:00.0'
        and posting_time >='2020-01-01 00:00:00.0'
        --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
        and move_type not in ('114A','110A','111A')
        and location_code ='W0A5'
        and product_code in ('1266339','128','1286204','852358')
        --排除盘点仓库存
        group by location_code,
        product_code,
        substr(regexp_replace(to_date(posting_time),'-',''),1,6)
        )a 
left join (
    select
    	substr(sdt,1,6) mon,
        dc_code,
        goods_code ,
        sum(sales_value )sales_value ,
        sum(sales_qty) sales_qty 
    from
        csx_dw.dws_sale_r_d_detail d
    where
        sdt> = '20200101'        
		and dc_code ='W0A5'
		group by substr(sdt,1,6) ,
        dc_code,
        goods_code) c on
    a.dc_code = c.dc_code and a.goods_code=c.goods_code and a.post_date=c.mon
 join 
 (select goods_id ,goods_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' ) f on a.goods_code =f.goods_id
    
   ;
 -- select * from csx_dw.dim_area  where area_rank =13;
  
  
  select * from csx_dw.dws_sale_r_d_detail where goods_code ='128' and sdt>='20200101' and sdt<'20200201' and return_flag ='X'and dc_code ='W0A5';
select  regexp_replace(to_date(posting_time),'-','') sdt,
		product_code ,
		goods_name,
		unit_name,
		supplier_code ,
		vendor_name,
		wms_batch_no,
	 	sum(case when move_type = '101A' then amt_no_tax*(1+tax_rate/100 ) end) `收货入库额`,
        sum(case when move_type = '101A' then txn_qty end) `收货入库量`,
        sum(case when move_type = '103A' then amt_no_tax*(1+tax_rate/100 )  end) `退货出库额`,
        sum(case when move_type = '103A' then txn_qty end) `退货出库量`

from
        csx_dw.dwd_cas_r_d_accounting_stock_detail a 
  left join (select goods_id,goods_name,unit_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.product_code =b.goods_id
   left join 
   (select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') c on a.supplier_code =c.vendor_id
    where
        sdt >= '20200101'
        and sdt <= '20210115'
        AND location_code ='W0A5'
        and posting_time <'2021-01-15 00:00:00.0'
        and posting_time >='2020-01-01 00:00:00.0'
        and move_type  in ('101A','103A')
         and product_code in ('1266339','128','1286204','852358')
      group by regexp_replace(to_date(posting_time),'-','') ,
		product_code ,
		goods_name,
		vendor_name,
		supplier_code ,
		wms_batch_no ,
	unit_name;
    

       
       

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
    () j
    ;
select
--	post_date,
   a.dc_code , 
    a.goods_code ,
     f.goods_name,
     end_qty,
     end_amt,
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
	sales_value ,
    sales_qty 
from

    ( select
		--substr(regexp_replace(to_date(posting_time),'-',''),1,6) as  post_date,
        location_code as dc_code,
        a.product_code as goods_code ,
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
        sdt >= '20200101'
        and sdt <= '20210115'
        and posting_time <'2021-01-15 00:00:00.0'
        and posting_time >='2020-01-01 00:00:00.0'
        --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
        and move_type not in ('114A','110A','111A')
        and location_code ='W0A5'
        and product_code in ('1266339','128','1286204','852358')
        --排除盘点仓库存
        group by location_code,
        product_code
       -- substr(regexp_replace(to_date(posting_time),'-',''),1,6)
        )a 
    left join       
   (
    select
        dc_code,
        dc_name,
--        reservoir_area_code ,
--        reservoir_area_name ,
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
        sdt = '20210114'
      and reservoir_area_code not in ('PD02','PD01','TS01')
      and dc_code ='W0A5'
      and goods_code in ('1266339','128','1286204','852358')
    group by
        dc_code,
        dc_name,
--        reservoir_area_code ,
--        reservoir_area_name ,
        goods_code,
        goods_name,
        a.unit,
        a.division_code,
        division_name,
        department_id,
        department_name,
        category_large_code,
        category_large_name ) m on m.goods_code=a.goods_code and m.dc_code=a.dc_code
      
left join (
    select
    --	substr(sdt,1,6) mon,
        dc_code,
        goods_code ,
        sum(sales_value )sales_value ,
        sum(sales_qty) sales_qty 
    from
        csx_dw.dws_sale_r_d_detail d
    where
        sdt> = '20200101'        
		and dc_code ='W0A5'
		group by 
        dc_code,
        goods_code) c on
    a.dc_code = c.dc_code and a.goods_code=c.goods_code
 join 
 (select goods_id ,goods_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' ) f on a.goods_code =f.goods_id
    
   ;