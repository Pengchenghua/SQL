--采购备货数据
-- 重庆（W0A7）,四川（W0A6）,福州（W0A8），杭州（W0N0），苏州（W0A5）
set shop=('W0A7','W0A6','W0A8','W0N0','W0A5');
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
sum(zs_amount) zs_amount
from 
(
select 
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
order_code,
goods_code ,
max(plan_qty )order_qty,
max(price*plan_qty )order_amt ,
sum(receive_qty )as receive_qty ,
sum(amount )as receive_amt ,
if(max(plan_qty )=sum(receive_qty ),1,0) as order_sign,
sum(case when order_type_code='P03' then amount end ) zs_amount
from  csx_dw.dws_wms_r_d_entry_detail a

where 
sdt>='20220201' 
 and sdt<='20220228'
 and receive_status =2
 and receive_location_code in ${hiveconf:shop}
 and order_type_code  LIKE 'P%' and order_type_code <>'P02'
group by 
a.receive_location_code ,
receive_location_name ,
a.supplier_code ,
order_code,
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
where sdt='current'
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
       b.classify_large_code,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       b.classify_small_code,
       b.classify_small_name,
       sum(a.final_amt) final_amt,
       sum(cost_30day) cost_30day,
       sum(period_inv_amt_30day) period_inv_amt_30day,
       sum(period_inv_amt_30day)/sum(cost_30day) as days_turnover_30
from csx_tmp.ads_wms_r_d_goods_turnover a 
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
    ) b on a.goods_id=b.goods_id
where sdt='20220228'
    and dc_code in  ${hiveconf:shop}
group by dc_code,
       b.classify_large_code,
       b.classify_large_name,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       b.classify_small_code,
       b.classify_small_name
;


-- 3.0 储存属性为库存销售占比
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
    where sdt>='20220101'
    and sdt<='20220228'
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
    count(product_code) all_sku
from 
(select product_code,
    shop_code,
    sales_return_tag,
    stock_properties,
	stock_properties_name,
	product_status_name
from  csx_dw.dws_basic_w_a_csx_product_info 
where sdt='current'
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
--csx_dw.dws_sale_r_d_detail;
--5.0 客户直送、退货占比

create temporary table csx_tmp.temp_entry_01 as 
select receive_location_code as dc_code,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
    sum(entry_amount) entry_amount,
    sum(shipp_amt) shipp_amt
from
(
select receive_location_code,
        goods_code,
        sum(amount) entry_amount,
        0 shipp_amt
from csx_dw.dws_wms_r_d_entry_batch 
where sdt>='20210901'
    and sdt<='20220228'
    and receive_location_code in  ${hiveconf:shop}
    and receive_status='2'
    and order_type_code like'P%' 
    AND business_type!='02'
    group by
        receive_location_code,
        goods_code
union all 
select shipped_location_code receive_location_code,
    goods_code,
    0 entry_amount,
    sum(amount) shipp_amt
from  csx_dw.dws_wms_r_d_ship_batch
where sdt>='20210901'
    and sdt<='20220228'
    and shipped_location_code in  ${hiveconf:shop}
    and order_type_code like 'P%'
    AND business_type_code='05'
    and return_flag='Y'
    and status !='9'
group by 
    shipped_location_code,
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
where sdt='current'
    ) c on a.goods_code=c.goods_id
group by receive_location_code,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name
    ;
    
--  -- 生鲜高库存 周转天数大于15天，入库天数大于3，库存额大于500 ；食品 周转大于30天，库存额大于2000，入库天数大于7；用品 周转>45,库存额>2000 ，入库天数大于7;

-- 6.0 高周转天数商品=低周转天数商品
--干货周转天数>45天以上且库存金额>3000元以上；水果、蔬菜周转天数>5天以上且库存金额>500元；
--生鲜其他课周转天数>15天以上且库存金额>2000元；食品部周转天数>45天以上且库存金额>2000元以上，
--用品类周转天数>60天以上且库存金额>3000 元以上
--入库天数>3天以上；未销售天数>7天以上

drop table if exists csx_tmp.tmp_hight_turn_goods ;
create temporary table  csx_tmp.tmp_hight_turn_goods as 
SELECT
       a.dc_code,
       a.dc_name,
      b.classify_large_code,
      b.classify_large_name,
      b.classify_middle_code,
      b.classify_middle_name,
      b.classify_small_code,
      b.classify_small_name,
       sum(final_qty) final_qty,
       sum(final_amt) final_amt
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (select goods_id,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from csx_dw.dws_basic_w_a_csx_product_m 
where sdt='current'
    ) b ON a.goods_id=b.goods_id

WHERE    
    sdt='20220228'             --更改查询日期
  AND a.final_qty>a.entry_qty
  AND ( (category_large_code='1101' and days_turnover_30>45 AND final_amt>3000)
    or (dept_id in ('H02','H03') and days_turnover_30>5 and a.final_amt>500 )
    OR (dept_id IN ('H04','H05','H06','H07','H08','H09','H10','H11') AND days_turnover_30>15 and a.final_amt>2000) 
    or (division_code ='12' and days_turnover_30>45 and final_amt>2000 )
    or (division_code in ('13','14')  and days_turnover_30>60 and final_amt>3000))
    and final_qty>0
    and a.entry_days>3
    and (a.no_sale_days>7 or no_sale_days='')
    and dc_code in  ${hiveconf:shop}
group by  a.dc_code,
       a.dc_name,
    --   a.division_code,
    --   a.division_name,
      b.classify_large_code,
      b.classify_large_name,
      b.classify_middle_code,
      b.classify_middle_name,
      b.classify_small_code,
      b.classify_small_name
  ;
  
-- 7.0 无动销SKU

create temporary table  csx_tmp.temp_pin as 
select a.dc_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    count(case when  amt_no_tax!=0 then a.goods_code end ) stock_sku,
    count(case when  coalesce(sales_value,0) >0 and amt_no_tax!=0  then a.goods_code end ) pin_sku
FROM
(select dc_code,goods_code,
    sum(amt_no_tax) amt_no_tax
from csx_dw.dws_wms_r_d_accounting_stock_m
where sdt='20220228'
and dc_code in  ${hiveconf:shop}
and reservoir_area_code not in ('PD01','PD02','TS01')
group by dc_code,goods_code
) a 
left join
(
select dc_code,goods_code,sum(sales_value) sales_value
from csx_dw.dws_sale_r_d_detail
where sdt>='20210901'
    and sdt<='20220228'
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


-- 客户满足率  剔除客户直送\福利单出库
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
where sdt>='20220201'
and sdt<='20220228'
and dc_code  in  ${hiveconf:shop}
group by   classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
  dc_code
  ;


create temporary table csx_tmp.temp_all_01 as 
select 
    dc_code ,
    case when classify_large_code in ('B01','B02','B03') then '11' else '12' end div_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(order_qty )  order_qty,
    sum(order_amt  )  order_amt ,
    sum(receive_qty  )  receive_qty ,
    sum(receive_amt  )  receive_amt ,
    sum(order_sign )  order_sign,
    sum(zs_amount )  zs_amount,
    sum(order_value )  order_value,
    sum(sales_value )  sales_value,
    sum(daily_sales_value )  daily_sales_value,
    sum(stock_sale_value )  stock_sale_value,
    sum(return_sku )  return_sku,
    sum(all_sku )  all_sku,
    sum(entry_amount )  entry_amount,
    sum(shipp_amt )  shipp_amt,
    sum(final_amt  )  final_amt , 
    sum(cost_30day )  cost_30day,
    sum(period_inv_amt_30day ) period_inv_amt_30day,
    sum(days_turnover_30 )  days_turnover_30,
    sum(h_final_qty )  h_final_qty,
    sum(h_final_amt )  h_final_amt,
    sum(stock_all_sku )  stock_all_sku,
    sum(pin_sku )  pin_sku
from ( 
select 
receive_location_code as dc_code ,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name,
order_qty,
order_amt ,
receive_qty ,
receive_amt ,
order_sign,
zs_amount,
  0 order_value,
    0 sales_value,
    0 daily_sales_value,
    0 stock_sale_value,
    0 return_sku,
    0 all_sku,
    0 entry_amount,
    0 shipp_amt,
    0  final_amt , 
    0  cost_30day,
    0  period_inv_amt_30day,
    0  days_turnover_30,
    0 h_final_qty,
    0 h_final_amt,
    0  stock_all_sku,
    0 pin_sku
from 
csx_tmp.temp_supplier_fill_rate         --供应商满足率
union all 
-- 客户满足率
select dc_code,
      classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    0 order_qty,
    0 order_amt ,
    0 receive_qty ,
    0 receive_amt ,
    0 order_sign,
    0 zs_amount,
    order_value,
    sales_value,
    0 daily_sales_value,
    0 stock_sale_value,
    0 return_sku,
    0 all_sku,
    0 entry_amount,
    0 shipp_amt,
    0  final_amt , 
    0  cost_30day,
    0  period_inv_amt_30day,
    0  days_turnover_30,
    0 h_final_qty,
    0 h_final_amt,
    0  stock_all_sku,
    0 pin_sku
from csx_tmp.temp_cust_sale_01
union all 
-- 存储商品销售
select dc_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    0 order_qty,
    0 order_amt ,
    0 receive_qty ,
    0 receive_amt ,
    0 order_sign,
    0 zs_amount,
    0 order_value,
    0 sales_value,
    sales_value as daily_sales_value,
    stock_sale_value,
    0 return_sku,
    0 all_sku,
    0 entry_amount,
    0 shipp_amt,
    0  final_amt , 
    0  cost_30day,
    0  period_inv_amt_30day,
    0  days_turnover_30,
    0 h_final_qty,
    0 h_final_amt,
    0  stock_all_sku,
    0 pin_sku
from csx_tmp.temp_sale_01
union all 
-- 可退商品占比
select 
    shop_code dc_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    0 order_qty,
    0 order_amt ,
    0 receive_qty ,
    0 receive_amt ,
    0 order_sign,
    0 zs_amount,
    0 order_value,
    0 sales_value,
    0 daily_sales_value,
    0 stock_sale_value,
    return_sku,
    all_sku,
     0 entry_amount,
    0 shipp_amt,
    0  final_amt , 
    0  cost_30day,
    0  period_inv_amt_30day,
    0  days_turnover_30,
    0 h_final_qty,
    0 h_final_amt,
    0  stock_all_sku,
    0 pin_sku
from csx_tmp.temp_sku
union all 
-- 退货占比
select dc_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
     0 order_qty,
    0 order_amt ,
    0 receive_qty ,
    0 receive_amt ,
    0 order_sign,
    0 zs_amount,
    0 order_value,
    0 sales_value,
    0 daily_sales_value,
    0 stock_sale_value,
    0 return_sku,
    0 all_sku,
     entry_amount,
    shipp_amt,
     0  final_amt , 
    0  cost_30day,
    0  period_inv_amt_30day,
    0  days_turnover_30,
    0 h_final_qty,
    0 h_final_amt,
    0  stock_all_sku,
    0 pin_sku
from  csx_tmp.temp_entry_01
union all
-- 周转
select dc_code,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
     0 order_qty,
    0 order_amt ,
    0 receive_qty ,
    0 receive_amt ,
    0 order_sign,
    0 zs_amount,
    0 order_value,
    0 sales_value,
    0 daily_sales_value,
    0 stock_sale_value,
    0 return_sku,
    0 all_sku,
    0 entry_amount,
    0 shipp_amt,
    final_amt , 
    cost_30day,
    period_inv_amt_30day,
    (period_inv_amt_30day)/(cost_30day) as days_turnover_30,
    0 h_final_qty,
    0 h_final_amt,
    0  stock_all_sku,
    0 pin_sku
from  csx_tmp.temp_turnover  
union all 
-- 高库存金额占比
SELECT
      dc_code,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name,
          0 order_qty,
    0 order_amt ,
    0 receive_qty ,
    0 receive_amt ,
    0 order_sign,
    0 zs_amount,
    0 order_value,
    0 sales_value,
    0 daily_sales_value,
    0 stock_sale_value,
    0 return_sku,
    0 all_sku,
    0 entry_amount,
    0 shipp_amt,
     0  final_amt , 
     0  cost_30day,
     0  period_inv_amt_30day,
     0  days_turnover_30,
      final_qty h_final_qty,
      final_amt h_final_amt,
    0  stock_all_sku,
    0 pin_sku
FROM csx_tmp.tmp_hight_turn_goods
union all 
select dc_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
          0 order_qty,
    0 order_amt ,
    0 receive_qty ,
    0 receive_amt ,
    0 order_sign,
    0 zs_amount,
    0 order_value,
    0 sales_value,
    0 daily_sales_value,
    0 stock_sale_value,
    0 return_sku,
    0 all_sku,
    0 entry_amount,
    0 shipp_amt,
    0  final_amt , 
    0  cost_30day,
    0  period_inv_amt_30day,
    0 days_turnover_30,
    0 h_final_qty,
    0 h_final_amt,
    stock_sku as stock_all_sku,
    pin_sku
FROM csx_tmp.temp_pin
)a 
group by    dc_code ,
    case when classify_large_code in ('B01','B02','B03') then '11' else '12' end  ,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    ;
    
    
    select 
    dc_code ,
    div_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    -- classify_small_code,
    -- classify_small_name,
    sum(order_qty )  order_qty,     --订单数量
    sum(order_amt  )  order_amt ,   --订单金额
    sum(receive_qty  )  receive_qty ,   --入库数量
    sum(receive_amt  )  receive_amt ,   --入库金额
    sum(order_sign )  order_sign,       --满足标识 
    sum(zs_amount )  zs_amount,         --客户直送金额
    sum(order_value )  order_value,     --客户订单金额
    sum(sales_value )  sales_value,     --客户出库金额
    sum(daily_sales_value )  daily_sales_value,     --日配销售金额
    sum(stock_sale_value )  stock_sale_value,       --存储商品销售额
    sum(return_sku )  return_sku,                   --退货标识SKU
    sum(all_sku )  all_sku,                         --在档SKU 正常状态商品
    sum(entry_amount )  entry_amount,               -- 入库金额
    sum(shipp_amt )  shipp_amt,                     --退货金额
    sum(final_amt  )  final_amt ,                   --库存金额
    sum(cost_30day )  cost_30day,                   --30天成本
    sum(period_inv_amt_30day ) period_inv_amt_30day,        --30天期间库存金额
    sum(period_inv_amt_30day )/sum(cost_30day )  days_turnover_30,  --30天周转
    sum(h_final_qty )  h_final_qty,                 --高库存金额
    sum(h_final_amt )  h_final_amt,                 --高库存金额
    sum(stock_all_sku )  stock_all_sku,             --库存SKU   
    sum(pin_sku )  pin_sku                          --动销SKU
from csx_tmp.temp_all_01
group by dc_code ,
    div_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name
grouping sets (( dc_code ,
    div_code,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name),
    ( dc_code ,
    div_code
    ),
    (dc_code))
    ;