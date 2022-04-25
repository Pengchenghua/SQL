
SET hive.execution.engine=mr;

create temporary table csx_dw.temp_fact_01
as 
SELECT shipped_location_code,
       shop_name,
       c.goods_code,
       qty,
       shipped_amt,
       d.product_code,
        mrp_prop_key,
          mrp_prop_value
FROM
  (SELECT shipped_location_code,
          shop_name,
          goods_code,
          qty,
          shipped_amt
   FROM
     (SELECT shipped_location_code,
             goods_code,
             sum(shipped_qty)qty,
             sum(shipped_qty*price)shipped_amt
      FROM csx_dw.wms_shipped_order
      WHERE sdt>='20200301'
        AND sdt<='20200331'
        AND shipped_type LIKE '调拨%'
      GROUP BY shipped_location_code,
               goods_code)a
   JOIN
     (SELECT location_code,
             shop_name
      FROM csx_dw.csx_shop
      WHERE sdt='current'
        AND location_type_code='2' ) b ON a.shipped_location_code=b.location_code) c
LEFT JOIN
  (SELECT factory_location_code,
          goods_code,
          product_code,
          mrp_prop_key,
          mrp_prop_value
   FROM csx_dw.dws_mms_w_a_factory_bom_m
   WHERE sdt='20200415' and mrp_prop_key in ('3061','3010')) d ON c.shipped_location_code=d.factory_location_code
AND c.goods_code=d.goods_code
where mrp_prop_key IS NOT NULL
;


 
-- SELECT shipped_location_code as shop_id,
       -- shop_name,
       -- qty,
       -- shipped_amt,
       -- product_code
-- FROM  csx_dw.temp_fact_01 as a 
-- LEFT JOIN 
-- (select * from csx_dw.temp_fact_02) b 
-- ; 
create temporary table csx_dw.temp_fact_02
as 
-- 移库类型剔除109A
select
	province_code,
	province_name,
	j.dc_code,
	j.dc_name,
	j.goods_code,
	j.goods_name,
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
`商品转码量`
-- `移库额`,
--`移库量`
from
(select dc_code,dc_name,goods_code,goods_name,a.unit,a.division_code,
division_name,
department_id,
department_name,
category_large_code,
category_large_name,
sum(qty)end_qty,
sum(amt)end_amt
from csx_dw.dws_wms_r_d_accounting_stock_m a 
where sdt='20200331' 
group by 
 dc_code,dc_name,goods_code,goods_name,a.unit,a.division_code,
division_name,
department_id,
department_name,
category_large_code,
category_large_name ) j
left join 
(select dc_code,goods_code,
sum(qty)first_qty,
sum(amt)first_amt
from csx_dw.dws_wms_r_d_accounting_stock_m a 
where sdt='20200229' 
group by 
 dc_code,goods_code ) h on j.dc_code=h.dc_code and j.goods_code=h.goods_code
left join 
	(
	select
		dc_code,
		goods_code,
		sum(case when move_type='101A' then amt end) `收货入库额`,
		sum(case when move_type='101A' then qty end) `收货入库量`,
		sum(case when move_type='102A' then amt end) `调拨入库额`,
		sum(case when move_type='102A' then qty end) `调拨入库量`,
		sum(case when move_type='105A' then amt end) `调拨退货入库额`,
		sum(case when move_type='105A' then qty end) `调拨退货入库量`,
		sum(case when move_type='108A' then amt end) `退货入库额`,
		sum(case when move_type='108A' then qty end) `退货入库量`,
		sum(case when move_type='120A' then amt end) `原料成品入库额`,
		sum(case when move_type='120A' then qty end) `原料成品入库量`,
		sum(case when move_type='201A' then amt end) `期初导入额`,
		sum(case when move_type='201A' then qty end) `期初导入量`,
		sum(case when move_type='103A' then amt end) `退货出库额`,
		sum(case when move_type='103A' then qty end) `退货出库量`,
		sum(case when move_type='104A' then amt end) `调拨出库额`,
		sum(case when move_type='104A' then qty end) `调拨出库量`,
		sum(case when move_type='106A' then amt end) `调拨退货出库额`,
		sum(case when move_type='106A' then qty end) `调拨退货出库量`,
		sum(case when move_type='107A' then amt end) `销售出库额`,
		sum(case when move_type='107A' then qty end) `销售出库量`,
		sum(case when move_type='112A' then if(in_or_out=1,-amt,amt) end) `子品转母品额`,
		sum(case when move_type='112A' then if(in_or_out=1,-qty,qty) end) `子品转母品量`,
		sum(case when move_type='113A' then if(in_or_out=1,-amt,amt) end) `母品转子品额`,
		sum(case when move_type='113A' then if(in_or_out=1,-qty,qty) end) `母品转子品量`,
		sum(case when move_type='115A' then amt end) `过帐盘盈额`,
		sum(case when move_type='115A' then qty end) `过帐盘盈量`,
		sum(case when move_type='116A' then amt end) `过帐盘亏额`,
		sum(case when move_type='116A' then qty end) `过帐盘亏量`,
		sum(case when move_type='117A' then amt end) `报损额`,
		sum(case when move_type='117A' then qty end) `报损量`,
		sum(case when move_type='118A' then if(in_or_out=1,-amt,amt) end) `领用额`,
		sum(case when move_type='118A' then if(in_or_out=1,-qty,qty) end) `领用量`,
		sum(case when move_type='119A' then amt end) `原料消耗额`,
		sum(case when move_type='119A' then qty end) `原料消耗量`,
		sum(case when move_type='121A' then amt end) `退料差异额`,
		sum(case when move_type='121A' then qty end) `退料差异量`,
		sum(case when move_type='202A' then if(in_or_out=1,-amt,amt) end) `商品转码额`,
		sum(case when move_type='202A' then if(in_or_out=1,-qty,qty) end) `商品转码量`
--		sum(case when move_type='109A' then amt end) `移库额`,
--		sum(case when move_type='109A' then qty end) `移库量`
	from
		csx_dw.dws_wms_r_d_accounting_stock_operation_item_m
	where
		sdt>='20200301' and  sdt<='20200331' 
    --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
		and move_type not in ('114A',
		'110A',
		'111A')
    --排除盘点仓库存
		
	group by
		dc_code,
		goods_code)a on j.dc_code=a.dc_code and j.goods_code=a.goods_code
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
where
	shop_name is not null ;

-- create table csx_dw.p_temp_fact_01
-- as 
select a.*,b.shipped_location_code ,
       b.qty,
       b.shipped_amt,
       b.product_code,
      round (coalesce(`过帐盘盈量`,0)-coalesce(`过帐盘亏量`,0)-coalesce(`报损量`,0),2) as loss_qty,
      - round (coalesce(`过帐盘盈量`,0)-coalesce(`过帐盘亏量`,0)-coalesce(`报损量`,0),2)/qty
       from csx_dw.temp_fact_02 as a 
 join 
 (SELECT shipped_location_code ,
       shop_name,
       SUM(qty)QTY,
       SUM(shipped_amt)shipped_amt,
       product_code
FROM  csx_dw.temp_fact_01 
GROUP BY  shipped_location_code ,
       shop_name,product_code) as b on a.dc_code=b.shipped_location_code and a.goods_code=b.product_code 
where a.dc_code in ('W079','W039')



