
	
	-- 省区供应商
	select province_code,province_name ,count(DISTINCT supplier_code),sum(qty)qty,sum(amount)/10000 amount from 
	(select receive_location_code as shop_id,supplier_code,supplier_name,sum(receive_qty)qty,sum(amount)amount  from csx_dw.wms_entry_order 
	where  sdt>='20190901' and sdt<='20200430'and (entry_type_code like 'P%'OR entry_type_code='999') 
	group by receive_location_code,supplier_code,supplier_name) a JOIN
	(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '平台' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' --and location_type_code in ('1','2')
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.shop_id=location_code
	group by province_code,province_name ;
	
-- 全国供应商
	select count(DISTINCT supplier_code),sum(qty)qty,sum(amount)/10000 amount from 
	(select receive_location_code as shop_id,supplier_code,supplier_name,sum(receive_qty)qty,sum(amount)amount  from csx_dw.wms_entry_order 
	where  sdt>='20190901' and sdt<='20200331'and (entry_type_code like 'P%'OR entry_type_code='999') 
	group by receive_location_code,supplier_code,supplier_name) a JOIN
	(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' --and location_type_code in ('1','2')
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.shop_id=location_code
	;
	

-- 省区食百供应商
	select province_code,province_name,count(DISTINCT supplier_code),sum(qty)qty,sum(amount)/10000 amount from 
	(select receive_location_code as shop_id,supplier_code,supplier_name,sum(receive_qty)qty,sum(amount)amount  from csx_dw.wms_entry_order 
	where sdt>='20190901' and sdt<='20200331'and (entry_type_code like 'P%'OR entry_type_code='999') and division_code in ('12','13','14')
	group by receive_location_code,supplier_code,supplier_name) a JOIN
	(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '平台' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current'-- and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.shop_id=location_code
	group by province_code,province_name ;

-- 省区食百供应商
	select count(DISTINCT supplier_code),sum(qty)qty,sum(amount)/10000 amount from 
	(select receive_location_code as shop_id,supplier_code,supplier_name,sum(receive_qty)qty,sum(amount)amount  from csx_dw.wms_entry_order 
	where sdt>='20190901' and sdt<='20200331'and (entry_type_code like 'P%'OR entry_type_code='999') and division_code in ('12','13','14')
	group by receive_location_code,supplier_code,supplier_name) a JOIN
	(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '平台' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current'-- and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.shop_id=location_code
	;

-- 省区生鲜供应商
	select province_code,province_name,count(DISTINCT supplier_code),sum(qty)qty,sum(amount)/10000 amount from 
	(select receive_location_code as shop_id,supplier_code,supplier_name,sum(receive_qty)qty,sum(amount)amount  from csx_dw.wms_entry_order 
	where sdt>='20190901' and sdt<='20200331'and (entry_type_code like 'P%'OR entry_type_code='999') and division_code in ('11','10')
	group by receive_location_code,supplier_code,supplier_name) a JOIN
	(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' --and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.shop_id=location_code
	group by province_code,province_name ;

-- 省区生鲜供应商
	select count(DISTINCT supplier_code),sum(qty)qty,sum(amount)/10000 amount from 
	(select receive_location_code as shop_id,supplier_code,supplier_name,sum(receive_qty)qty,sum(amount)amount  from csx_dw.wms_entry_order 
	where sdt>='20190901' and sdt<='20200331'and (entry_type_code like 'P%' OR entry_type_code ='999') and division_code in ('11','10')
	group by receive_location_code,supplier_code,supplier_name) a JOIN
	(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' --and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.shop_id=location_code
	;


-- 类别SKU 生鲜
select province_code,province_name,COUNT(DISTINCT case when (sale !=0 or inv_qty!=0 )then  goods_code end ) sku,sum(sale)/10000 sale,sum(inv_qty) from 
(
select dc_code,goods_code,sum(sale)sale,sum(inv_qty) inv_qty from (
select dc_code,goods_code,sum(sales_value)sale,0 inv_qty   from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200301' and sdt<='20200331'
and division_code in ('11','10')
group by  dc_code,goods_code
UNION ALL 
select dc_code,goods_code,0 sale,sum(qty) inv_qty   from csx_dw.dws_wms_r_d_accounting_stock_m 
where  sdt='20200331'
and division_code in ('11','10')
group by  dc_code,goods_code
) a group by dc_code,goods_code
) a join 
(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.dc_code=b.location_code
	group by province_code,province_name;
group by dc_code,goods_code
;
--合计
select COUNT(DISTINCT case when (sale !=0 or inv_qty!=0 )then  goods_code end ),sum(sale)/10000  from 
(
select dc_code,goods_code,sum(sale)sale,sum(inv_qty) inv_qty from (
select dc_code,goods_code,sum(sales_value)sale,0 inv_qty   from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200301' and sdt<='20200331'
and division_code in ('11','10')
group by  dc_code,goods_code
UNION ALL 
select dc_code,goods_code,0 sale,sum(qty) inv_qty   from csx_dw.dws_wms_r_d_accounting_stock_m 
where  sdt='20200331'
and division_code in ('11','10')
group by  dc_code,goods_code
) a group by dc_code,goods_code
) a join 
(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.dc_code=b.location_code
	
;

-- 类别SKU 食百
select province_code,province_name,COUNT(DISTINCT case when (sale !=0 or inv_qty!=0 )then  goods_code end ) sku,sum(sale)/10000 sale,sum(inv_qty) from 
(
select dc_code,goods_code,sum(sale)sale,sum(inv_qty) inv_qty from (
select dc_code,goods_code,sum(sales_value)sale,0 inv_qty   from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200301' and sdt<='20200331'
and division_code in ('12','13','14')
group by  dc_code,goods_code
UNION ALL 
select dc_code,goods_code,0 sale,sum(qty) inv_qty   from csx_dw.dws_wms_r_d_accounting_stock_m 
where  sdt='20200331'
and division_code in ('12','13','14')
group by  dc_code,goods_code
) a group by dc_code,goods_code
) a join 
(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.dc_code=b.location_code
	group by province_code,province_name;
group by dc_code,goods_code
;
--食百合计
select COUNT(DISTINCT case when (sale !=0 or inv_qty!=0 )then  goods_code end ),sum(sale)/10000  from 
(
select dc_code,goods_code,sum(sale)sale,sum(inv_qty) inv_qty from (
select dc_code,goods_code,sum(sales_value)sale,0 inv_qty   from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200301' and sdt<='20200331'
and division_code in ('12','13','14')
group by  dc_code,goods_code
UNION ALL 
select dc_code,goods_code,0 sale,sum(qty) inv_qty   from csx_dw.dws_wms_r_d_accounting_stock_m 
where  sdt='20200331'
and division_code in ('12','13','14')
group by  dc_code,goods_code
) a group by dc_code,goods_code
) a join 
(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.dc_code=b.location_code
	
;

-- 省区SKU 合计
select province_code,province_name,COUNT(DISTINCT case when (sale !=0 or inv_qty!=0 )then  goods_code end ) sku,sum(sale)/10000 sale,sum(inv_qty) from 
(
select dc_code,goods_code,sum(sale)sale,sum(inv_qty) inv_qty from (
select dc_code,goods_code,sum(sales_value)sale,0 inv_qty   from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200301' and sdt<='20200331'
--and division_code in ('11','10')
group by  dc_code,goods_code
UNION ALL 
select dc_code,goods_code,0 sale,sum(qty) inv_qty   from csx_dw.dws_wms_r_d_accounting_stock_m 
where  sdt='20200331'
--and division_code in ('11','10')
group by  dc_code,goods_code
) a group by dc_code,goods_code
) a join 
(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.dc_code=b.location_code
	group by province_code,province_name;
group by dc_code,goods_code
;
--合计
select COUNT(DISTINCT case when (sale !=0 or inv_qty!=0 )then  goods_code end ),sum(sale)/10000  from 
(
select dc_code,goods_code,sum(sale)sale,sum(inv_qty) inv_qty from (
select dc_code,goods_code,sum(sales_value)sale,0 inv_qty   from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200301' and sdt<='20200331'
--and division_code in ('11','10','12','13','14')
group by  dc_code,goods_code
UNION ALL 
select dc_code,goods_code,0 sale,sum(qty) inv_qty   from csx_dw.dws_wms_r_d_accounting_stock_m 
where  sdt='20200331'
--and division_code in ('11','10','12','13','14')
group by  dc_code,goods_code
) a group by dc_code,goods_code
) a join 
(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' and location_type_code='1'
	and location_code not in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1') ) b on a.dc_code=b.location_code
	
;


	

	-- 类别SKU 合计
select  COUNT(DISTINCT goods_code),sum(sale)/10000  from 
(select dc_code,goods_code,sum(sales_value)sale   from csx_dw.dws_sale_r_d_customer_sale 
where sdt>='20200301' and sdt<='20200331'
--and division_code in ('12','13','14')
group by  dc_code,goods_code
) a join 
(
	SELECT location_code,shop_name,case when location_code='W0H4' then '00' else  province_code end province_code,case when location_code='W0H4' then '全国' else  
	province_name end province_name FROM csx_dw.csx_shop WHERE sdt='current' and (location_type_code !='1'
	or location_code  in  ('W0H3','W0H7','W0H8','W0H6','W0H9','W0K2','W0G7','W0G1','W0K4','W0Q7','W0P4','W0H1')) ) b on a.dc_code=b.location_code
	
	;
	
	
-- DC省区供应商
 select
    mon,
    location_type,
    dist_code,
    dist_name,
    shop_id,
    shop_name ,
    supplier_code,
    supplier_name,
    goods_code ,
    goods_name ,
    division_code ,
    division_name,
    department_id ,
    department_name ,
    sum(qty)qty,
    sum(amount)/ 10000 amount
from
    (
    select
        substr(sdt,
        1,
        6)mon,
        receive_location_code as shop_id,
        supplier_code,
        supplier_name,
        division_code ,
        division_name,
        department_id ,
        department_name ,
        goods_code ,
        goods_name ,
        sum(receive_qty)qty,
        sum(amount)amount
    from
        csx_dw.wms_entry_order
    where
        sdt >= '20190901'
        and sdt <= '20200430'
        and (entry_type_code like 'P%'
        OR entry_type_code = '999')
    group by
        receive_location_code,
        supplier_code,
        supplier_name,
        division_code ,
        division_name ,
        goods_code ,
        goods_name ,
        substr(sdt,
        1,
        6),
        department_id ,
        department_name ) a
JOIN (
    SELECT
        location_code,
        shop_name,
        dist_code,
        dist_name,
        location_type
    FROM
        csx_dw.csx_shop
    WHERE
        sdt = 'current'
        and location_type_code in ('1',
        '2')
        and location_code not in ('W0H3',
        'W0H7',
        'W0H8',
        'W0H6',
        'W0H9',
        'W0K2',
        'W0G7',
        'W0G1',
        'W0K4',
        'W0Q7',
        'W0P4',
        'W0H1') ) b on
    a.shop_id = location_code
group by
    dist_code,
    dist_name,
    supplier_code,
    supplier_name,
    goods_code ,
    goods_name ,
    division_code ,
    division_name ,
    mon,
    location_type,
    shop_id,
    shop_name,
    department_id ,
    department_name ;