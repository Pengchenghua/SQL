--商品池销售占比
with temp_sale as 
(SELECT substr(sdt,1,6)mon,
        a.customer_no,
        a.customer_name,
       goods_code,
       goods_name,
       aa,
       bb,
       sum(sales_qty)qty,
       sum(sales_value) sale,
       sum(profit)profit
FROM csx_dw.dws_sale_r_d_detail a 
LEFT JOIN
(select customer_code,product_code ,'1' aa
from csx_ods.source_csms_w_a_yszx_customer_product  
    where sdt='20210915' and inventory_dc_code='W0A2') b on a.customer_no=b.customer_code and a.goods_code=b.product_code
LEFT JOIN
(SELECT location_code,product_code,'1' bb
FROM csx_dw.dws_scm_w_a_product_pool
WHERE SDT='20210915' 
and location_code='W0A2') c on a.dc_code=c.location_code and a.goods_code=cast(c.product_code as string)
WHERE sdt>='20210101'
  AND sdt<='20210915'
  and a.dc_code='W0A2'
GROUP BY substr(sdt,1,6),
         a.customer_no,
        a.customer_name,
       goods_code,
       goods_name,
       aa,
       bb
       ),
temp_sale_01 as (select customer_code,classify_middle_code,classify_middle_name,product_code ,'1' aa
from csx_ods.source_csms_w_a_yszx_customer_product a 
join 
(select goods_id,classify_middle_code,classify_middle_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.product_code=b.goods_id

    where sdt='20210915' and inventory_dc_code='W0A2')
    
select mon,
a.customer_no,
a.customer_name,
aa,
sku,
sku/all_sku as pin_rate,
qty,
sale,
profit_rate
from
(
select mon,
a.customer_no,
a.customer_name,
if(aa='1','是','否')aa,
count( goods_code) as sku,
count(distinct goods_code )over(partition by mon) as all_sku,
sum(qty) qty,
sum(sale)/10000 sale,
sum(profit)/sum(sale) profit_rate
from temp_sale a 
group by mon,aa ,a.customer_no,
        a.customer_name
)a 
;



CREATE temporary table csx_tmp.temp_sale_01 as 
SELECT substr(sdt,1,6)mon,
        a.customer_no,
        a.customer_name,
       goods_code,
       goods_name,
       aa,
       bb,
       sum(sales_qty)qty,
       sum(sales_value) sale,
       sum(profit)profit
FROM csx_dw.dws_sale_r_d_detail a 
LEFT JOIN
(select customer_code,product_code ,'1' aa
from csx_ods.source_csms_w_a_yszx_customer_product  
    where sdt='20210915' and inventory_dc_code='W0A2') b on a.customer_no=b.customer_code and a.goods_code=b.product_code
LEFT JOIN
(SELECT location_code,product_code,'1' bb
FROM csx_dw.dws_scm_w_a_product_pool
WHERE SDT='20210915' 
and location_code='W0A2') c on a.dc_code=c.location_code and a.goods_code=cast(c.product_code as string)
WHERE sdt>='20210101'
  AND sdt<='20210915'
  and a.dc_code='W0A2'
GROUP BY substr(sdt,1,6),
         a.customer_no,
        a.customer_name,
       goods_code,
       goods_name,
       aa,
       bb
      ;
  
  
 SELECT a.mon,
       a.customer_no,
       a.customer_name,
        pin_sku,
        all_sku,
        pin_sku/all_sku pin_rate,
       aa,
       qty,
       sale,
       profit,
     profit_rate
from (
SELECT mon,
       a.customer_no,
       a.customer_name,
       count(DISTINCT goods_code) pin_sku,
       -- count(DISTINCT goods_code)over(PARTITION BY mon) as all_sku,
       aa,
       sum(qty)qty,
       sum(sale)sale,
       sum(profit)profit,
       sum(profit)/sum(sale) profit_rate
    from csx_tmp.temp_sale_01 a
    
    GROUP BY  aa,
       mon,
       a.customer_no,
       a.customer_name
      )a
     LEFT JOIN
     (
SELECT mon,
       count(DISTINCT goods_code) as all_sku
    from csx_tmp.temp_sale_01 a
    
    GROUP BY mon
      ) b on a.mon=b.mon;
    
-- select mon,classify_middle_code,classify_middle_name,if(bb='1','是','否')bb,
-- count(distinct goods_code) as sku,
-- sum(sale)/10000 sale,
-- sum(profit)/sum(sale) from temp_sale a 
-- join 
-- (select goods_id,classify_middle_code,classify_middle_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
-- group by mon,bb ,classify_middle_code,classify_middle_name
-- order by mon,bb,classify_middle_code,classify_middle_name
--          ;