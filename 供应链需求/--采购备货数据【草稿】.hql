--采购备货数据
-- 重庆（W0A7）,四川（W0A6）,福州（W0A8），杭州（W0N0），苏州（W0A5）
set shop=('W0A2','W0A3','W0A7','W0A8','W0N0','W0A5','W0A6');
set enddt=regexp_replace('${enddate}','-','');
set sdt=regexp_replace(trunc('${enddate}','MM'),'-','');

--select  ${hiveconf:enddt},${hiveconf:sdt};

-- 1.0 供应商满足率
drop table  csx_tmp.temp_supplier_fill_rate;
create temporary table csx_tmp.temp_supplier_fill_rate as 
select 
a.receive_location_code ,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name,
sum(order_qty) order_qty,
sum(order_amt ) order_amt ,
sum(receive_qty) receive_qty ,
sum(receive_amt) receive_amt ,
sum(order_sign) order_sign,
sum(shipp_amt) shipp_amt,
sum(zs_order_amt)   zs_order_amt ,
sum(zs_receive_amt) zs_receive_amt
from 
(
select 
a.receive_location_code ,
a.supplier_code ,
order_code,
goods_code ,
max(plan_qty )order_qty,
max(price*plan_qty )order_amt ,
sum(receive_qty )as receive_qty ,
sum(amount )as receive_amt ,
if(max(plan_qty )=sum(receive_qty ),1,0) as order_sign,
sum(case when order_type_code='P03' then price*plan_qty end ) zs_order_amt,
sum(case when order_type_code='P03' then amount end ) zs_receive_amt,
0 shipp_amt
from  csx_dw.dws_wms_r_d_entry_detail a
where 
 sdt>= ${hiveconf:sdt} 
 and sdt<=${hiveconf:enddt} 
 and receive_status =2
 and receive_location_code in ${hiveconf:shop}
 and order_type_code  LIKE 'P%' 
 and order_type_code <>'P02'
group by 
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
order_code,
goods_code 
union all 
select shipped_location_code receive_location_code,
    supplier_code,
    order_no as order_code,
    goods_code,
    0 order_qty,
    0 order_amt,
    0 receive_qty,    
    0 receive_amt,
    0 order_sign,
    0 zs_order_amt ,
    0 zs_receive_amt,
    sum(shipped_amount) shipp_amt
from  csx_dw.dws_wms_r_d_ship_detail
where sdt>= ${hiveconf:sdt} 
 and sdt<=${hiveconf:enddt} 
    and shipped_location_code in  ${hiveconf:shop}
    and order_type_code like 'P%'
    AND business_type_code='05'
    and return_flag='Y'
    and status !='9'
group by 
    shipped_location_code,
    order_no,
    supplier_code,
    goods_code
) a

left join
(select goods_id,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt=${hiveconf:enddt} 
    ) b on a.goods_code=b.goods_id
group by a.receive_location_code ,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name
;

-- 2.0 库存周转天数

drop table  csx_tmp.temp_turnover;
create temporary table csx_tmp.temp_turnover as
select dc_code,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       a.classify_small_code,
       a.classify_small_name,
       sum(a.final_qty) final_qty,
       sum(a.final_amt) final_amt,
       sum(cost_30day) cost_30day,
       sum(period_inv_amt_30day) period_inv_amt_30day,
       sum(period_inv_amt_30day)/sum(cost_30day) as days_turnover_30,
       count(case when stock_properties=1 and final_qty>0  then goods_id end) stock_goods_inventory_sku,       -- 存储商品有库存SKU
       sum(case when stock_properties=1 then final_amt end ) stock_goods_inventory_amt,    -- 存储商品库存金额
       count(case when stock_properties=1 and final_qty<=0 then goods_id end) stockout_goods_sku,
       count(case when stock_properties !=1 and final_qty>0  then goods_id end) no_stock_goods_sku,       -- 非存储商品SKU
       sum(case when stock_properties !=1 then final_amt end ) no_stock_goods_amt    --  非存储商品库存金额 
from csx_tmp.ads_wms_r_d_goods_turnover a 
 left join 
   (select product_code ,
    shop_code,
    sales_return_tag,
    stock_properties,
	stock_properties_name,
	product_status_name
from  csx_dw.dws_basic_w_a_csx_product_info 
where sdt=${hiveconf:enddt} 
    and des_specific_product_status='0'
    and shop_code in  ${hiveconf:shop}
    ) b on a.goods_id=b.product_code
where sdt=${hiveconf:enddt}
    and dc_code in  ${hiveconf:shop}
group by dc_code,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       a.classify_small_code,
       a.classify_small_name
;


-- 3.0 储存属性为库存 销售占比
drop table  csx_tmp.temp_sale_01;
create temporary table  csx_tmp.temp_sale_01 as 
select dc_code,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    sum(sales_value) sales_value,
    sum(case when stock_properties='1' then sales_value end ) as stock_sale_value
from 
(select dc_code,goods_code,
    sum(sales_value) sales_value
from csx_dw.dws_sale_r_d_detail 
    where sdt>= ${hiveconf:sdt} 
    and sdt<=${hiveconf:enddt} 
    and business_type_code='1'
    and channel_code in ('1','7','9')
    and dc_code in  ${hiveconf:shop}
group by dc_code,goods_code
) a 
left join 
(
select product_code,
    shop_code,
    stock_properties,
	stock_properties_name 
from  csx_dw.dws_basic_w_a_csx_product_info 
where sdt='current'
) b on a.goods_code=b.product_code and a.dc_code=b.shop_code
left join
(select goods_id,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt='current'
    ) c on a.goods_code=c.goods_id
group by  dc_code,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name
;



--4.0 可退商品SKU占比
drop table  csx_tmp.temp_sku;
create temporary table csx_tmp.temp_sku as 
select 
    shop_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    count(case when sales_return_tag=1 then product_code end ) return_sku,
    count(product_code) all_sku,
    count(case when stock_properties=1 then product_code end ) stock_goods_sku,                       -- 存储商品SKU
    count(case when stock_properties=1 and sales_return_tag=1 then product_code end ) stock_goods_return_sku --存储商品可退sku
from 
(select product_code,
    shop_code,
    sales_return_tag,
    stock_properties,
	stock_properties_name,
	product_status_name
from  csx_dw.dws_basic_w_a_csx_product_info 
where sdt=${hiveconf:enddt} 
    and des_specific_product_status='0'
    and shop_code in  ${hiveconf:shop}
) a 
left join
(select goods_id,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt='current'
    ) c on a.product_code=c.goods_id
group by shop_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
;
    

-- 7.0 无动销SKU
drop table csx_tmp.temp_pin ;
create temporary table  csx_tmp.temp_pin as 
select a.dc_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    count(case when  amt_no_tax!=0 then a.goods_code end ) in_stock_sku,       -- 在库SKU
    count(case when coalesce(sales_value,0) >0   then a.goods_code end ) pin_sku,   -- 动销SKU
    count(CASE WHEN coalesce(sales_value,0)<=0 AND amt_no_tax>0 then a.goods_code end  ) no_pin_sku  --不动销库存大于0
FROM
(select dc_code,goods_code,
    sum(amt_no_tax) amt_no_tax
from csx_dw.dws_wms_r_d_accounting_stock_m
where sdt=${hiveconf:enddt} 
and dc_code in  ${hiveconf:shop}
and reservoir_area_code not in ('PD01','PD02','TS01','CY01')
group by dc_code,goods_code
) a 
left join
(
select dc_code,
    goods_code,
    sum(sales_value) sales_value
from csx_dw.dws_sale_r_d_detail
where sdt <=${hiveconf:enddt} 
    and sdt>=${hiveconf:sdt}
    and dc_code in  ${hiveconf:shop}
group by dc_code,goods_code
) b on a.dc_code=b.dc_code and a.goods_code=b.goods_code
left join 
(select goods_id,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt='current'
    ) c ON a.goods_code=c.goods_id
group by  a.dc_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
;


-- 满足率  剔除直送\福利单出库
drop table csx_tmp.temp_cust_sale_01;
create temporary table csx_tmp.temp_cust_sale_01 as  
select dc_code,
      classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(order_value) order_value,
    sum(sales_value) sales_value
from  csx_dw.ads_wms_r_d_lack_goods_detail
where sdt>= ${hiveconf:sdt} 
 and sdt<=${hiveconf:enddt} 
and dc_code  in  ${hiveconf:shop}
group by   
    classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  dc_code
  ;

drop table csx_tmp.temp_all_02;
create temporary table csx_tmp.temp_all_02 as 
select 
     a.dc_code,
    case when a.classify_large_code in ('B01','B02','B03') then '11' else '12' end  div_code,
    case when a.classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end div_name ,
     a.classify_large_code,
     a.classify_large_name,
     a.classify_middle_code,
     a.classify_middle_name,
     a.classify_small_code,
     a.classify_small_name,
     order_qty,
     order_amt ,
     receive_qty ,
     receive_amt ,
     order_sign,
    zs_order_amt  ,
    zs_receive_amt,
     order_value,
     d.sales_value,
     c.sales_value as daily_sales_value,
     stock_sale_value,
     return_sku,
     all_sku,           -- 正常状态商品SKU数
     stock_goods_sku,         -- 存储商品SKU
     stock_goods_return_sku,
     shipp_amt,
    a.final_qty,
    a.final_amt , 
    cost_30day,
    period_inv_amt_30day,
    (period_inv_amt_30day)/(cost_30day) as days_turnover_30,
     in_stock_sku,     -- 在库sku数
     pin_sku,
     no_pin_sku,
     stock_goods_inventory_sku,       -- 存储商品有库存SKU
     stock_goods_inventory_amt,    -- 存储商品库存金额
     stockout_goods_sku,
     no_stock_goods_sku,       -- 非存储商品SKU
     no_stock_goods_amt    --  非存储商品库存金额 
from  csx_tmp.temp_turnover a 
left join
-- 存储SKU
csx_tmp.temp_sku b on a.dc_code=b.shop_code and a.classify_small_code=b.classify_small_code 
left join 
-- 存储商品销售
csx_tmp.temp_sale_01 c on a.dc_code=c.dc_code and a.classify_small_code=c.classify_small_code
left join
-- 满足率
 csx_tmp.temp_cust_sale_01 d on a.dc_code=d.dc_code and a.classify_small_code=d.classify_small_code
 left join 
 csx_tmp.temp_supplier_fill_rate    j on a.dc_code=j.receive_location_code and a.classify_small_code=j.classify_small_code      --供应商满足率
left join csx_tmp.temp_pin k on a.dc_code=k.dc_code and a.classify_small_code=k.classify_small_code
;



drop table    csx_tmp.temp_all_00 ;
create temporary table csx_tmp.temp_all_00 as    
  select 
    dc_code ,
    div_code,
    div_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    sum(order_qty )  order_qty,     --订单数量
    sum(order_amt  )  order_amt ,   --订单金额
    sum(receive_qty  )  receive_qty ,   --入库数量
    sum(receive_amt  )  receive_amt ,   --入库金额
    sum(receive_qty  ) /sum(order_qty)  order_sign_rate,       --满足标识 
    sum(zs_order_amt  ) zs_order_amt  ,       --直送订单金额
    sum(zs_receive_amt)  zs_receive_amt,        --直送金额
    sum(zs_receive_amt)/sum(receive_amt) zs_ratio,   --直送入库占比
    sum(order_value )  order_value,     --配送订单金额 剔除地采 
    sum(sales_value )  sales_value,     --配送出库金额 剔除地采
    sum(sales_value )/ sum(order_value ) cust_sale_ratio,       --配送满足率占比
    sum(daily_sales_value )  daily_sales_value,     --日配销售金额
    sum(stock_sale_value )  stock_sale_value,       --存储商品销售额
    sum(return_sku )  return_sku,                   --退货标识SKU
    sum(all_sku )  all_sku,                         --在档SKU 正常状态商品
    sum(stock_goods_sku) stock_goods_sku,         -- 存储商品SKU
    sum(stock_goods_return_sku) stock_goods_return_sku ,    -- 存储商品可退SKU
    sum(shipp_amt )  shipp_amt,                     --退货金额
    sum(final_qty  )  final_qty ,                   --库存金额
    sum(final_amt  )  final_amt ,                   --库存金额
    sum(cost_30day )  cost_30day,                   --30天成本
    sum(period_inv_amt_30day ) period_inv_amt_30day,        --30天期间库存金额
    sum(period_inv_amt_30day )/sum(cost_30day )  days_turnover_30,  --30天周转
    sum(in_stock_sku )  in_stock_sku,             --库存SKU   
    sum(pin_sku )  pin_sku ,                         --动销SKU
    sum(no_pin_sku)no_pin_sku,
    sum(stock_goods_inventory_sku) stock_goods_inventory_sku,       -- 存储商品有库存SKU
    sum(stock_goods_inventory_amt) stock_goods_inventory_amt,    -- 存储商品库存金额
    sum(stockout_goods_sku) stockout_goods_sku,
    sum(no_stock_goods_sku) no_stock_goods_sku,       -- 非存储商品SKU
    sum(no_stock_goods_amt) no_stock_goods_amt    --  非存储商品库存金额 
	from csx_tmp.temp_all_02 a 
group by dc_code ,
    div_code,
    div_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name
grouping sets (( dc_code ,
    div_code,
    div_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name),
    ( dc_code ,
    div_code,
    div_name
    ),
    (dc_code))
    ;
    
set hive.exec.dynamic.partition.mode=nonstrict
;


 


insert overwrite table csx_tmp.report_scm_r_d_purchase_goods_fr partition(sdt,dim_date)
select 
    
    province_code,
    province_name,
    city_code,
    city_name,
    shop_name,
    a.dc_code ,
    case when div_code is null then '00' else div_code end div_code,
    case when div_code is null then '小计'else div_name end div_name,
    case when classify_large_code  is null and classify_middle_code is not null then '00' else classify_large_code end classify_large_code,
    case when classify_large_name  is null and classify_middle_code is not null then '小计' else classify_large_name end classify_large_name,
    coalesce( classify_middle_code ,'') classify_middle_code,
    coalesce(  classify_middle_name ,'') classify_middle_name,
    (order_qty )  order_qty,     --订单数量
    (order_amt  )/10000  order_amt ,   --订单金额
    (receive_qty  )  receive_qty ,   --入库数量
    (receive_amt  )/10000  receive_amt ,   --入库金额
    (receive_qty  ) /(order_qty)  order_sign_rate,       --满足标识 
    (zs_order_amt)/10000 zs_order_amt,       --直送订单金额
    (zs_receive_amt )/10000  zs_receive_amt,         --直送金额
    (zs_receive_amt)/(receive_amt) zs_receive_ratio,   --直送入库占比
    (order_value )/10000  order_value,     --配送订单金额 剔除地采 
    (sales_value )/10000  sales_value,     --配送出库金额 剔除地采
    (sales_value )/ (order_value ) cust_sale_ratio,       -- 配送满足率
    (daily_sales_value )/10000  daily_sales_value,     --日配销售金额
    (stock_sale_value )/10000  stock_sale_value,       --存储商品销售额
    stock_sale_value/daily_sales_value as stock_sale_ratio, -- 存储商品销售占比
    (return_sku )  return_sku,                   --退货标识SKU
    (all_sku )  all_sku,                         --在档SKU 正常状态商品
    (stock_goods_sku) stock_goods_sku,                       -- 存储商品SKU
    stock_goods_return_sku , -- 存储商品可退sku
    stock_goods_return_sku/stock_goods_sku as stock_goods_return_sku_ratio,  -- 存储商品可退占比
    -- (shipp_amt )/10000  shipp_amt,                     --退货金额
    (final_qty  )  final_qty ,                   --库存金额
    (final_amt  )/10000  final_amt ,                   --库存金额
    -- (cost_30day )  cost_30day,                   --30天成本
    -- (period_inv_amt_30day )/10000 period_inv_amt_30day,        --30天期间库存金额
    (period_inv_amt_30day )/(cost_30day )  days_turnover_30,  --30天周转
    (in_stock_sku )  in_stock_sku,                --库存SKU   
    (pin_sku )  pin_sku ,                         --动销SKU
    (no_pin_sku)no_pin_sku,
     no_pin_sku/all_sku as no_pin_sku_rate,      -- 无动销率
    (stock_goods_inventory_sku) stock_goods_inventory_sku,       -- 存储商品有库存SKU
    (stock_goods_inventory_amt)/10000 stock_goods_inventory_amt,    -- 存储商品库存金额
    (stockout_goods_sku) stockout_goods_sku,
    stockout_goods_sku/stock_goods_sku as stockout_goods_sku_rate,  -- 存储商品缺货率
    (no_stock_goods_sku) no_stock_goods_sku,       -- 非存储商品SKU
    (no_stock_goods_amt)/10000 no_stock_goods_amt,    --  非存储商品库存金额 
    current_timestamp(),
    substr(${hiveconf:enddt},1,6),
    '月' dim_date
from csx_tmp.temp_all_00 a 
left join
(select shop_id,shop_name,province_name,province_code,city_code,city_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.dc_code=b.shop_id
;


