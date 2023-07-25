-- 报价指导
策略变更前的报价按时间来判断，策略更新了的就去报价指导里面查
1.如果生效区报价开始时间晚于策略更新时间，就去失效区里面提该商品的报价，失效区内的价格才是策略更新前的报价；
2.如果生效区报价开始时间小于策略更新时间，那生效区内的价格就是策略变更前的报价，不用去失效区提；
-- 策略配置
    select 
	  warehouse_code,
	  product_code,
	  price_begin_time,
	  raw,
      get_json_object(substr(raw, 2, length(raw) - 2),'$.mProductPrice') as product_price ,
      get_json_object(raw,'$[0].mProductPrice') as product_price ,
      substr(raw, 2, length(raw) - 2),
	  row_number() over(partition by warehouse_code,product_code order by price_begin_time desc) as top_customer_sale_no
    from csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df 
    where sdt=regexp_replace(date_sub(current_date, 1),'-','') and effective='true' and raw is not null and raw <> '[]'  

-- 近7天銷售  
select a.*,b.* 
from (select * 
from (
select *,
    row_number()over(partition by customer_code,warehouse_code,goods_code order by update_time desc ) num 
from csx_analyse_tmp.csx_analyse_tmp_price_guide_config 
    where change_after_code is not null and update_time >='2023-06-24 00:00:00' and update_time<='2023-06-30 23:59:59'
) a where num =1
    and warehouse_code='W0L3'
)a 
left join 
(select sale_time,inventory_dc_code,customer_code,goods_code,sum(sale_qty)qty,sum(sale_amt) sale_amt ,sum(profit) profit from    csx_dws.csx_dws_sale_detail_di a 
join (select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0) b on a.inventory_dc_code=b.shop_code
where sdt >='20230601' and business_type_code =1
group by inventory_dc_code,customer_code,goods_code,sale_time) b on a.goods_code=b.goods_code and a.warehouse_code=b.inventory_dc_code and a.customer_code=b.customer_code
and to_date(Sale_time)<= to_date(update_time) AND to_date(sale_time) >= to_date(date_sub(update_time,7)) 
;


--select * from csx_ods.csx_ods_csx_price_prod_goods_price_guide_df	 where sdt='20230705';
-- price_type 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)

-- 根据策略调整时间 关联商品售价指导
select 
    a.customer_code,
    customer_name,
    a.warehouse_code,
   dimension_type,      -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    a.update_time,
    suggest_price_type,
    price_type,
     price_type_name,
    addition_rate,
   second_priceType  ,  
    second_config,
    second_addition_rate,
    second_suggestPriceType, --f 建议售价类型: 1-高;2:中;3:低
    a.goods_code ,
    goods_name,
    suggest_price_high,
    suggest_price_mid,
    suggest_price_low,
    yh_shop_price,
   suggest_type,
    price_begin_time,
    price_end_time,
    goods_update_time,
    change_before_code,
    additionRate_before,
    additionRate_after,
    create_time,
    sum(qty)qty,
    sum(sale_amt) sale_amt ,
    sum(profit) profit
from(
select id,
    customer_code,
    a.warehouse_code,
    case when dimension_type=3 then '大类' when dimension_type=2 then '中类' when dimension_type=1 then '小类' when dimension_type=0 then '商品' end dimension_type,      -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    a.update_time,
    case when a.suggest_price_type=1 then '建议售价高' when a.suggest_price_type=2 then '建议售价中' when a.suggest_price_type=3 then '建议售价低' end suggest_price_type,
    price_type,
    case when price_type=1 then '建议售价' when price_type=2 then '对标对象' when price_type=3 then '销售成本价' when price_type=4 then '上一周价格' when price_type=5 then '售价' when price_type=6 then '采购/库存成本' end price_type_name,
    addition_rate,
   second_priceType  ,  
    second_config,
    second_addition_rate,
    case when a.second_suggestPriceType=1 then '建议售价高' when a.second_suggestPriceType=2 then '建议售价中' when a.second_suggestPriceType=3 then '建议售价低' end second_suggestPriceType, --f 建议售价类型: 1-高;2:中;3:低
    goods_code ,
    suggest_price_high,
    suggest_price_mid,
    suggest_price_low,
    yh_shop_price,
   case when b.suggest_price_type=1 then '目标定价法' when b.suggest_price_type=2 then '市调价格' when b.suggest_price_type=3 then '手动导入' end suggest_type,
    price_begin_time,
    price_end_time,
    b.update_time as goods_update_time,
    change_before_code,
    additionRate_before,
    additionRate_after,
    create_time
from csx_analyse_tmp.csx_analyse_tmp_price_guide_config a 
left join 
(select warehouse_code,product_code,suggest_price_high,suggest_price_mid,suggest_price_low,yh_shop_price,suggest_price_type,price_begin_time,price_end_time,update_time
from csx_ods.csx_ods_csx_price_prod_goods_price_guide_df	 -- 关联商品售价指导
    where sdt='20230705' 
       AND is_expired='false'
    ) b on a.goods_code=b.product_code and a.warehouse_code=b.warehouse_code and a.update_time<=price_begin_time
left join 
  
  (select * from (
select customer_price_guide_config_id,
    change_type ,
    get_json_object(get_json_object(get_json_object(change_before,'$.sellConfig'),'$.secondConfig'),'$.suggestPriceType') change_before_code,       -- 建议售价类型: 1-高;2:中;3:低
    get_json_object(get_json_object(change_before,'$.sellConfig'),'$.additionRate') additionRate_before,    -- 加成系数
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.floatUpRate')   floatUpRate_after,        -- 售价类型:上浮点数
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.dimensionType') dimensionType_after,    -- 商品 =0 小类 =1 中类 =2 大类 =3
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.floatDownRate') floatDownRate_after,    -- 售价类型:下浮点数
    get_json_object(get_json_object(get_json_object(change_after,'$.sellConfig'),'$.customerPriceGuideSecondConfig'),'$.additionRate')   additionRate_after, -- 变更后 售价类型:上浮点数,
    get_json_object(get_json_object(get_json_object(change_after,'$.sellConfig'),'$.customerPriceGuideSecondConfig'),'$.suggestPriceType')   change_after_code, --变更后
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.priceType')  price_type_after, -- price_type 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
    change_before,
    change_after,
    create_time,
    row_number()over(partition by customer_price_guide_config_id order by create_time desc ) num
    from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_log_df 
    where sdt='20230630'
    -- and customer_price_guide_config_id=352885
        and change_type in (1,2,3)
    ) a where num =1
       ) c on a.id=c.customer_price_guide_config_id and to_date(a.update_time)=to_date(c.create_time)
    where a.warehouse_code='W0A8'
    and b.update_time is not null 
  --  and a.customer_code!='107459'
  ) a 
  left join 
(select sale_time,inventory_dc_code,customer_code,customer_name,goods_code,goods_name,sum(sale_qty)qty,sum(sale_amt) sale_amt ,sum(profit) profit from    csx_dws.csx_dws_sale_detail_di a 
join (select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0) b on a.inventory_dc_code=b.shop_code
where sdt >='20230601' and business_type_code =1
group by inventory_dc_code,customer_code,goods_code,sale_time,customer_name,goods_name) b
on a.goods_code=b.goods_code and a.warehouse_code=b.inventory_dc_code and a.customer_code=b.customer_code
and to_date(Sale_time)<= to_date(update_time) AND to_date(sale_time) > to_date(date_sub(update_time,7)) 
group by  a.customer_code,
    customer_name,
    a.warehouse_code,
   dimension_type,      -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    a.update_time,
    suggest_price_type,
    price_type,
     price_type_name,
    addition_rate,
   second_priceType  ,  
    second_config,
    second_addition_rate,
    second_suggestPriceType, --f 建议售价类型: 1-高;2:中;3:低
    a.goods_code ,
    goods_name,
    suggest_price_high,
    suggest_price_mid,
    suggest_price_low,
    yh_shop_price,
   suggest_type,
    price_begin_time,
    price_end_time,
    goods_update_time,
    change_before_code,
    additionRate_before,
    additionRate_after,
    create_time
    ;

    -- 策略調整數據
drop table csx_analyse_tmp.csx_analyse_tmp_price_guide_config;
create table csx_analyse_tmp.csx_analyse_tmp_price_guide_config as 
select a.id,
  a.customer_code,
  a.warehouse_code,
  a.dimension_type,
  a.dimension_value_code,
  a.product_code ,
  a.update_time,
  b.change_type,
  change_after_code,
  change_before_code
from 
  (  
select 
  a1.id,
  a1.customer_code,
  a1.warehouse_code,
  a1.dimension_type,
  a1.dimension_value_code,
  a2.product_code ,
  a1.update_time
from 
  (select 
    id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    update_time
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='${sdt_date}' 
  and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=3
  group by 
    customer_code,
    warehouse_code,
    dimension_type,
    dimension_value_code ,
    update_time,
    id
  ) a1 
  left join 
  (select * 
  from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
  where sdt='${sdt_date}') a2 
  on a1.dimension_value_code=a2.one_product_category_code and a1.warehouse_code=a2.location_code 
group by 
  a1.customer_code,
  a1.warehouse_code,
  a1.dimension_type,
  a1.dimension_value_code,
  a2.product_code ,
  a1.update_time,
   a1.id
union all 

-- 中类对应的商品
select 
  b1.id,
  b1.customer_code,
  b1.warehouse_code,
  b1.dimension_type,
  b1.dimension_value_code,
  b2.product_code ,
  b1.update_time
from 
  (select id, 
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    update_time
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='${sdt_date}' 
  and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=2
  group by 
    customer_code,
    warehouse_code,
    dimension_type,
    dimension_value_code,
    update_time,id
  ) b1 
  left join 
  (select * 
  from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
  where sdt='${sdt_date}') b2 
  on b1.dimension_value_code=b2.two_product_category_code and b1.warehouse_code=b2.location_code 
group by 
  b1.customer_code,
  b1.warehouse_code,
  b1.dimension_type,
  b1.dimension_value_code,
  b2.product_code ,
  b1.update_time,b1.id

union all 

-- 小类对应的商品
select c1.id,
  c1.customer_code,
  c1.warehouse_code,
  c1.dimension_type,
  c1.dimension_value_code,
  c2.product_code ,
  c1.update_time
from 
  (select id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    update_time
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='${sdt_date}' 
  and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=1
  group by 
    customer_code,
    warehouse_code,
    dimension_type,
    dimension_value_code ,
    update_time,id
 ) c1 
  left join 
  (select * 
  from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
  where sdt='${sdt_date}') c2 
  on c1.dimension_value_code=c2.three_product_category_code and c1.warehouse_code=c2.location_code 
group by 
  c1.customer_code,
  c1.warehouse_code,
  c1.dimension_type,
  c1.dimension_value_code,
  c2.product_code ,
  c1.update_time,
  c1.id

union all 

-- 商品对应的商品
select id,
  customer_code,
  warehouse_code,
  dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
  dimension_value_code,
  dimension_value_code as product_code ,
  update_time
from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
where sdt='${sdt_date}' 
and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
and dimension_type=0
group by 
  customer_code,
  warehouse_code,
  dimension_type,
  dimension_value_code ,
  update_time,id
  ) a 
  join
  (select customer_price_guide_config_id,change_type ,
    get_json_object(change_after,'$.suggestPriceType') change_after_code,       -- 建议售价类型: 1-高;2:中;3:低
    get_json_object(change_before,'$.suggestPriceType') change_before_code
    from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_log_df 
    where sdt='20230629'
        and change_type in (1,2,3)
    ) b on a.id=b.customer_price_guide_config_id
  where 1=1
  ;

  -- 报价策略商品明细 
  select 
     id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    second_addition_rate,
    second_suggestPriceType,
    goods_code ,
    num 
from 
  (
select 
     id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    second_addition_rate,
    second_suggestPriceType,
    goods_code ,
    row_number()over(partition by customer_code,warehouse_code,goods_code order by update_time desc ) as num 
from 
  (
select 
     id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    second_addition_rate,
    second_suggestPriceType,
    a2.goods_code 
from 
  (select 
    id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    get_json_object(second_config,'$.additionRate') second_addition_rate,
    get_json_object(second_config,'$.suggestPriceType') second_suggestPriceType
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230630' 
  and warehouse_code not like 'L%'
  and to_date(update_time)>='2023-06-24' and to_date(update_time)<='2023-06-30'
 -- and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=3
  ) a1 
  left join 
  (select dc_code,a.goods_code,classify_large_code from csx_dim.csx_dim_basic_dc_goods a 
  left join 
  ( select goods_code,classify_large_code from csx_dim.csx_dim_basic_goods where sdt='current'
  ) b on a.goods_code=b.goods_code 
  where sdt='current') a2 
  on a1.dimension_value_code=a2.classify_large_code   and a1.warehouse_code=a2.dc_code
  union all 
  
select 
     id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    second_addition_rate,
    second_suggestPriceType,
    a2.goods_code 
from 
  (select 
    id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    get_json_object(second_config,'$.additionRate') second_addition_rate,
    get_json_object(second_config,'$.suggestPriceType') second_suggestPriceType
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230630' 
  and warehouse_code not like 'L%'
  and to_date(update_time)>='2023-06-24' and to_date(update_time)<='2023-06-30'
 -- and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=2
  ) a1 
  left join 
  (select dc_code,a.goods_code,classify_middle_code from csx_dim.csx_dim_basic_dc_goods a 
  left join 
  ( select goods_code,classify_middle_code from csx_dim.csx_dim_basic_goods where sdt='current'
  ) b on a.goods_code=b.goods_code 
  where sdt='current') a2 
  on a1.dimension_value_code=a2.classify_middle_code   and a1.warehouse_code=a2.dc_code
  union all 
  
select 
     id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    second_addition_rate,
    second_suggestPriceType,
    a2.goods_code 
from 
  (select 
    id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    get_json_object(second_config,'$.additionRate') second_addition_rate,
    get_json_object(second_config,'$.suggestPriceType') second_suggestPriceType
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230630' 
  and warehouse_code not like 'L%'
  and to_date(update_time)>='2023-06-24' and to_date(update_time)<='2023-06-30'
 -- and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=1
  ) a1 
  left join 
  (select dc_code,a.goods_code,classify_small_code from csx_dim.csx_dim_basic_dc_goods a 
  left join 
  ( select goods_code,classify_small_code from csx_dim.csx_dim_basic_goods where sdt='current'
  ) b on a.goods_code=b.goods_code 
  where sdt='current') a2 
  on a1.dimension_value_code=a2.classify_small_code   and a1.warehouse_code=a2.dc_code
union all 
-- 商品对应的商品
select id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    second_config,
    get_json_object(second_config,'$.additionRate') second_addition_rate,
    get_json_object(second_config,'$.suggestPriceType') second_suggestPriceType,
    dimension_value_code as goods_code
from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
 where sdt='20230630' 
  and warehouse_code not like 'L%'
  and to_date(update_time)>='2023-06-24' and to_date(update_time)<='2023-06-30'
 -- and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=0
  ) a 
  ) a where num =1
  ;
  
  select * from  csx_analyse_tmp.csx_analyse_tmp_price_guide_config where change_type in(1,2) and change_after_code is not null ;

   select a.*,b.* from 
 ( select * from  csx_analyse_tmp.csx_analyse_tmp_price_guide_config where change_type in(1,2) and change_after_code is not null )  a 
 left join 
(select
  warehouse_code,
  customer_code,
  product_code,
  price_begin_time,
  customer_price,
  price_end_time,
  effective,
  cost_price_time,
  id
from
  csx_ods.csx_ods_csx_price_prod_customer_price_guide_df
where
  sdt = '20230629'
  and price_begin_time >= '2023-06-29 00:00:00'
--   and customer_code = '110881'
--   and product_code = '949284'
  ) b on a.warehouse_code=b.warehouse_code and a.customer_code=b.customer_code and a.product_code=b.product_code
  where to_date(update_time)< to_date(price_begin_time)
 --   and to_date(update_time) >='2023-06-29'
 ;

drop table csx_analyse_tmp.csx_analyse_tmp_price_guide_config ;
create table csx_analyse_tmp.csx_analyse_tmp_price_guide_config as 
select a.id,
  a.customer_code,
  a.warehouse_code,
  a.dimension_type,
  a.dimension_value_code,
  a.goods_code ,
  a.update_time,
  b.change_type,
  change_after_code,
  change_before_code,
  additionRate_before, 
  floatUpRate_after,   
  dimensionType_after, 
  floatDownRate_after, 
  additionRate_after,
  price_type_after
from 
  (  
select 
  a1.id,
  a1.customer_code,
  a1.warehouse_code,
  a1.dimension_type,
  a1.dimension_value_code,
  a2.goods_code ,
  a1.update_time
from 
  (select 
    id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    update_time
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230630' 
  and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=3
  group by 
    customer_code,
    warehouse_code,
    dimension_type,
    dimension_value_code ,
    update_time,
    id
  ) a1 
  left join 
  (select dc_code,a.goods_code,classify_large_code from csx_dim.csx_dim_basic_dc_goods a 
  left join 
  ( select goods_code,classify_large_code from csx_dim.csx_dim_basic_goods where sdt='current'
  ) b on a.goods_code=b.goods_code 
  where sdt='current') a2 
  on a1.dimension_value_code=a2.classify_large_code   and a1.warehouse_code=a2.dc_code
group by 
  a1.customer_code,
  a1.warehouse_code,
  a1.dimension_type,
  a1.dimension_value_code,
  a2.goods_code ,
  a1.update_time,
   a1.id
union all 

-- 中类对应的商品
select 
  b1.id,
  b1.customer_code,
  b1.warehouse_code,
  b1.dimension_type,
  b1.dimension_value_code,
  b2.goods_code ,
  b1.update_time
from 
  (select id, 
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    update_time
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230630' 
  and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=2
  group by 
    customer_code,
    warehouse_code,
    dimension_type,
    dimension_value_code,
    update_time,id
  ) b1 
  left join 
  (select dc_code,a.goods_code,classify_middle_code from csx_dim.csx_dim_basic_dc_goods a 
  left join 
  ( select goods_code,classify_middle_code from csx_dim.csx_dim_basic_goods where sdt='current'
  ) b on a.goods_code=b.goods_code 
  where sdt='current') b2 
  on b1.dimension_value_code=b2.classify_middle_code and b1.warehouse_code=b2.dc_code 
group by 
  b1.customer_code,
  b1.warehouse_code,
  b1.dimension_type,
  b1.dimension_value_code,
  b2.goods_code ,
  b1.update_time,b1.id

union all 

-- 小类对应的商品
select c1.id,
  c1.customer_code,
  c1.warehouse_code,
  c1.dimension_type,
  c1.dimension_value_code,
  c2.goods_code ,
  c1.update_time
from 
  (select id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    update_time
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230630' 
  and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=1
  group by 
    customer_code,
    warehouse_code,
    dimension_type,
    dimension_value_code ,
    update_time,id
 ) c1 
   left join 
  (select dc_code,a.goods_code,classify_small_code from csx_dim.csx_dim_basic_dc_goods a 
  left join 
  ( select goods_code,classify_small_code from csx_dim.csx_dim_basic_goods where sdt='current'
  ) b on a.goods_code=b.goods_code 
  where sdt='current') c2 
  on c1.dimension_value_code=c2.classify_small_code and c1.warehouse_code=c2.dc_code 
group by 
  c1.customer_code,
  c1.warehouse_code,
  c1.dimension_type,
  c1.dimension_value_code,
  c2.goods_code ,
  c1.update_time,
  c1.id

union all 

-- 商品对应的商品
select id,
  customer_code,
  warehouse_code,
  dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
  dimension_value_code,
  dimension_value_code as goods_code ,
  update_time
from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
where sdt='20230630' 
and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
and dimension_type=0
group by 
  customer_code,
  warehouse_code,
  dimension_type,
  dimension_value_code ,
  update_time,id
  ) a 
  join
  (select * from (
select customer_price_guide_config_id,
    change_type ,
    get_json_object(get_json_object(get_json_object(change_before,'$.sellConfig'),'$.secondConfig'),'$.suggestPriceType') change_before_code,       -- 建议售价类型: 1-高;2:中;3:低
    get_json_object(get_json_object(change_before,'$.sellConfig'),'$.additionRate') additionRate_before,    -- 加成系数
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.floatUpRate')   floatUpRate_after,        -- 售价类型:上浮点数
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.dimensionType') dimensionType_after,    -- 商品 =0 小类 =1 中类 =2 大类 =3
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.floatDownRate') floatDownRate_after,    -- 售价类型:下浮点数
    get_json_object(get_json_object(get_json_object(change_after,'$.sellConfig'),'$.customerPriceGuideSecondConfig'),'$.additionRate')   additionRate_after, -- 变更后 售价类型:上浮点数,
    get_json_object(get_json_object(get_json_object(change_after,'$.sellConfig'),'$.customerPriceGuideSecondConfig'),'$.suggestPriceType')   change_after_code, --变更后
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.priceType')  price_type_after, -- price_type 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
    change_before,
    change_after,
    create_time,
    row_number()over(partition by customer_price_guide_config_id order by create_time desc ) num
    from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_log_df 
    where sdt='20230630'
    -- and customer_price_guide_config_id=352885
        and change_type in (1,2,3)
    ) a where num =1
       ) b on a.id=b.customer_price_guide_config_id and 
  where 1=1
  ;

select * from (
select customer_price_guide_config_id,
    change_type ,
    get_json_object(get_json_object(get_json_object(change_before,'$.sellConfig'),'$.secondConfig'),'$.suggestPriceType') change_before_code,       -- 建议售价类型: 1-高;2:中;3:低
    get_json_object(get_json_object(change_before,'$.sellConfig'),'$.additionRate') additionRate_before,    -- 加成系数
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.floatUpRate') floatUpRate_after,        -- 售价类型:上浮点数
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.dimensionType') dimensionType_after,    -- 商品 =0 小类 =1 中类 =2 大类 =3
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.floatDownRate') floatDownRate_after,    -- 售价类型:下浮点数
    get_json_object(get_json_object(get_json_object(change_after,'$.sellConfig'),'$.customerPriceGuideSecondConfig'),'$.additionRate')   additionRate_after, -- 变更后 售价类型:上浮点数,
    get_json_object(get_json_object(get_json_object(change_after,'$.sellConfig'),'$.customerPriceGuideSecondConfig'),'$.suggestPriceType')   change_after_code, --变更后
    get_json_object(get_json_object(change_after,'$.sellConfig'),'$.priceType')  price_type_after, -- price_type 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
    change_before,
    change_after,
    create_time,
    row_number()over(partition by customer_price_guide_config_id order by create_time desc ) num
    from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_log_df 
    where sdt='20230630'
    -- and customer_price_guide_config_id=352885
        and change_type in (1,2,3)
    ) a where num =1
       
        
 -- lateral view explode(split(replace(replace(replace(get_json_object(change_after,'$.customerPriceGuideSecondConfig'),'[',''),']',''),'},{"hero_type"','}|{"hero_type"'),"\\|")) tf as line_new;

 --  lateral view explode(split(replace(replace(replace(get_json_object(change_after,'$.customerPriceGuideSecondConfig'),'[',''),']',''),'},{"hero_type"','}|{"hero_type"'),"\\|")) tf as line_new;

{"bmkCode": "9446", "sellConfig": "{\"bmkCode\":\"9446\",\"bmkName\":\"石狮泰禾广场店\",\"bmkType\":0,\"customerCode\":\"121168\",\"customerName\":\"石狮市鸿山热电有限公司\",\"customerPriceGuideConfigBmkReqs\":[],\"customerPriceGuideSecondConfig\":{\"additionRate\":-0.0500,\"priceType\":1,\"suggestPriceType\":3},\"dimensionClassList\":[{\"dimensionValueCode\":\"B0202\",\"dimensionValueName\":\"蔬菜\"}],\"dimensionType\":2,\"floatDownRate\":22.1,\"floatUpRate\":0,\"id\":352885,\"isFixPrice\":0,\"isUnityFloatRate\":1,\"priceType\":2,\"sellingPriceStatus\":1,\"subCustomerCode\":\"\",\"warehouseCode\":\"W0L3\",\"warehouseName\":\"福建彩食鲜泉州物流DC\"}"}	
{"bmkCode": "9446", "sellConfig": "{\"bmkCode\":\"9446\",\"bmkName\":\"石狮泰禾广场店\",\"bmkType\":0,\"customerCode\":\"121168\",\"customerName\":\"石狮市鸿山热电有限公司\",\"customerPriceGuideConfigBmkReqs\":[],\"customerPriceGuideSecondConfig\":{\"additionRate\":-0.0500,\"priceType\":1,\"suggestPriceType\":3},\"dimensionClassList\":[{\"dimensionValueCode\":\"B0202\",\"dimensionValueName\":\"蔬菜\"}],\"dimensionType\":2,\"floatDownRate\":22.1,\"floatUpRate\":0,\"id\":352885,\"isFixPrice\":0,\"isUnityFloatRate\":1,\"priceType\":2,\"sellingPriceStatus\":1,\"subCustomerCode\":\"\",\"warehouseCode\":\"W0L3\",\"warehouseName\":\"福建彩食鲜泉州物流DC\"}"}
{"bmkCode": "90L1", "bmkType": 0, "priceType": 2, "secondConfig": "{\"additionRate\":-0.0500,\"bmkPriceType\":0,\"floatAmount\":0,\"floatDownRate\":0.2210,\"floatType\":0,\"isBottomSuggestPrice\":1,\"priceType\":1,\"suggestPriceType\":3}"}
{"bmkCode": "90L1", "sellConfig": "{\"additionRate\":0.0000,\"bmkCode\":\"90L1\",\"bmkName\":\"泉州市Bravo浦西万达店\",\"bmkPriceType\":0,\"bmkType\":0,\"createBy\":\"柯玮婷\",\"createTime\":1686305653000,\"customerPriceGuideConfigId\":352885,\"floatDownRate\":0.2210,\"floatUpRate\":0.0000,\"id\":45034,\"priceType\":3,\"secondConfig\":\"{\\\"floatType\\\": 0, \\\"priceType\\\": 1, \\\"floatAmount\\\": 0, \\\"additionRate\\\": -0.05, \\\"bmkPriceType\\\": 0, \\\"floatDownRate\\\": 0.221, \\\"suggestPriceType\\\": 3, \\\"isBottomSuggestPrice\\\": 1}\",\"suggestPriceType\":1,\"updateBy\":\"柯玮婷\",\"updateTime\":1686305653000}"}	
{"bmkCode": "ZD286", "bmkType": 3, "priceType": 2, "additionRate": 0}
{"bmkCode": "", "bmkType": 99, "priceType": 6, "additionRate": 0.1}