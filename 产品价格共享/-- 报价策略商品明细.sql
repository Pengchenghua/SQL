  -- 报价策略商品明细 
  策略变更后，客户报价开始时间低于策略变更时间，取商品售价指导的
  策略变更前的客户报价按时间来判断，策略更新了的就去报价指导里面查

1.如果生效区报价开始时间大于策略更新时间，就去失效区里面提该商品的报价，失效区内的价格才是策略更新前的客户报价；
2.如果生效区报价开始时间小于策略更新时间，那生效区内的价格就是策略变更前的客户报价，不用去失效区提；

  -- 报价策略商品明细 
  
  -- 报价策略商品明细 
  -- drop table csx_analyse_tmp.csx_analyse_tmp_price_guide_config ;
  create table csx_analyse_tmp.csx_analyse_tmp_price_guide_config as 
select 
     id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code , 
     bmk_type,
     case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type, -- 一级建议售价
    second_priceType,   -- 二级定价类型
    second_config,
    second_addition_rate,   -- 二级策略加成系数
    second_suggestPriceType, -- 二级策略建议售价
    goods_code ,
    -- change_before_code,
    -- additionRate_before,
    -- additionRate_after,
    a.num 
from 
  (
select 
     id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
     bmk_type,
    -- case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type,
    second_priceType,
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
     bmk_type,
    -- case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type,
    second_priceType,
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
     bmk_type,
    -- case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type,
    second_config,
    get_json_object(second_config,'$.priceType') second_priceType,
    get_json_object(second_config,'$.additionRate') second_addition_rate,
    get_json_object(second_config,'$.suggestPriceType') second_suggestPriceType
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230710' 
  and warehouse_code not like 'L%'
  and to_date(update_time)>='2023-07-04' and to_date(update_time)<='2023-07-10'
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
     bmk_type,
    -- case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type,
    second_priceType,
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
     bmk_type,
    -- case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type,
    second_config,
    get_json_object(second_config,'$.priceType') second_priceType,
    get_json_object(second_config,'$.additionRate') second_addition_rate,
    get_json_object(second_config,'$.suggestPriceType') second_suggestPriceType
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230710' 
  and warehouse_code not like 'L%'
  and to_date(update_time)>='2023-07-04' and to_date(update_time)<='2023-07-10'
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
     bmk_type,
    -- case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type,
    second_priceType,
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
     bmk_type,
    -- case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type,
    second_config,
    get_json_object(second_config,'$.priceType') second_priceType,
    get_json_object(second_config,'$.additionRate') second_addition_rate,
    get_json_object(second_config,'$.suggestPriceType') second_suggestPriceType
  from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
  where sdt='20230710' 
  and warehouse_code not like 'L%'
  and to_date(update_time)>='2023-07-04' and to_date(update_time)<='2023-07-10'
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
select 
    id,
    customer_code,
    warehouse_code,
    dimension_type, -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
     bmk_type,
    -- case when bmk_type='0' then '永辉门店' when bmk_type='1' then '网站' when bmk_type='2' then '市场' when bmk_type='3' then '市场' else bmk_type end bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    update_time,
    price_type,
    addition_rate,
    suggest_price_type,
    get_json_object(second_config,'$.priceType') second_priceType,
    second_config,
    get_json_object(second_config,'$.additionRate') second_addition_rate,
    get_json_object(second_config,'$.suggestPriceType') second_suggestPriceType,
    dimension_value_code as goods_code
from      csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df 
 where sdt='20230710' 
  and warehouse_code not like 'L%'
  and to_date(update_time)>='2023-07-04' and to_date(update_time)<='2023-07-10'
 -- and (price_type=1 or get_json_object(second_config,"$.priceType")=1) 
  and dimension_type=0
   ) a 
  ) a   where a.num =1
  ;
  
  --关联销售侧 近7天实际销售毛利率
  -- 
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
    where sdt='20230710' 
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
    where sdt='20230710'
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
where customer_name is not null 
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

    -- 商品售价指导预估策略调整后的毛利
    select * from csx_ods.csx_ods_csx_price_prod_customer_price_guide_config_df where sdt='20230710'
    ;

-- 根据策略更新时间关联销售近7天
    select 
    a.customer_code,
    customer_name,
    a.warehouse_code,
    a.goods_code ,
    goods_name,
     sum(qty)qty,
    sum(sale_amt) sale_amt ,
    sum(profit) profit
from csx_analyse_tmp.csx_analyse_tmp_price_guide_config a 

  left join 
(select sale_time,inventory_dc_code,customer_code,customer_name,goods_code,goods_name,sum(sale_qty)qty,sum(sale_amt) sale_amt ,sum(profit) profit from    csx_dws.csx_dws_sale_detail_di a 
join (select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0) b on a.inventory_dc_code=b.shop_code
where sdt >='20230601' and business_type_code =1
group by inventory_dc_code,customer_code,goods_code,sale_time,customer_name,goods_name) b
on a.goods_code=b.goods_code and a.warehouse_code=b.inventory_dc_code and a.customer_code=b.customer_code
and to_date(Sale_time)<= to_date(update_time) AND to_date(sale_time) > to_date(date_sub(update_time,7)) 
where  1=1
and b.customer_name is not null 
and warehouse_code='W0A8'
group by  a.customer_code,
    customer_name,
    a.warehouse_code,
    a.goods_code ,
    goods_name 
    ;



    WITH AA AS (
select id,
    customer_code,
    a.warehouse_code,
    case when dimension_type=3 then '大类' when dimension_type=2 then '中类' when dimension_type=1 then '小类' when dimension_type=0 then '商品' end dimension_type,      -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    bmk_type,
    bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    a.update_time,
    case when a.suggest_price_type=1 then '建议售价高' when a.suggest_price_type=2 then '建议售价中' when a.suggest_price_type=3 then '建议售价低' end suggest_price_type,
    price_type,
    case when price_type=1 then '建议售价' when price_type=2 then '对标对象' when price_type=3 then '销售成本价' when price_type=4 then '上一周价格' when price_type=5 then '售价' when price_type=6 then '采购/库存成本' end price_type_name,
    addition_rate,
   case when second_priceType=1 then '建议售价' when second_priceType=2 then '对标对象' when second_priceType=3 then '销售成本价' when second_priceType=4 then '上一周价格' when second_priceType=5 then '售价' when second_priceType=6 then '采购/库存成本' end  second_priceType  ,  
    second_config,
    second_addition_rate,
    case when a.second_suggestPriceType=1 then '建议售价高' when a.second_suggestPriceType=2 then '建议售价中' when a.second_suggestPriceType=3 then '建议售价低' end second_suggestPriceType, --f 建议售价类型: 1-高;2:中;3:低
    goods_code ,
    b.suggest_price_high,
    b.suggest_price_mid,
    b.suggest_price_low,
    b.yh_shop_price,
   case when b.suggest_price_type=1 then '目标定价法' when b.suggest_price_type=2 then '市调价格' when b.suggest_price_type=3 then '手动导入' end suggest_type,
    b.price_begin_time,
    b.price_end_time,
    b.update_time as goods_update_time,
    d.suggest_price_high disable_suggest_price_high,
    d.suggest_price_mid  disable_suggest_price_mid,
    d.suggest_price_low  disable_suggest_price_low,
    d.yh_shop_price disable_yh_shop_price,
    case when d.suggest_price_type=1 then '目标定价法' when d.suggest_price_type=2 then '市调价格' when d.suggest_price_type=3 then '手动导入' end disable_suggest_type,
    d.price_begin_time as disable_price_begin_time,
    d.price_end_time disable_price_end_time,
    d.update_time as disable_goods_update_time,
    change_before_code,
    additionRate_before,
    additionRate_after,
    create_time
from csx_analyse_tmp.csx_analyse_tmp_price_guide_config a 
left join 
-- 商品售价指导生效
(select warehouse_code,product_code,suggest_price_high,suggest_price_mid,suggest_price_low,yh_shop_price,suggest_price_type,price_begin_time,price_end_time,update_time
from csx_ods.csx_ods_csx_price_prod_goods_price_guide_df	 -- 关联商品售价指导
    where sdt='20230710' 
       AND is_expired='false'
    ) b on a.goods_code=b.product_code and a.warehouse_code=b.warehouse_code 
-- 商品售价指导失效
left join 
(select * from (select warehouse_code,product_code,suggest_price_high,suggest_price_mid,suggest_price_low,yh_shop_price,suggest_price_type,price_begin_time,price_end_time,update_time,is_expired,
    row_number()over(partition by warehouse_code,product_code order by price_begin_time desc ) as num 
from    csx_ods.csx_ods_csx_price_prod_goods_price_guide_df	 -- 关联商品售价指导
    where sdt='20230710' 
       AND is_expired='true'
    )a
 where num =1) d on a.goods_code=d.product_code and a.warehouse_code=d.warehouse_code and a.update_time<=d.price_begin_time
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
    where sdt='20230710'
    -- and customer_price_guide_config_id=352885
        and change_type in (1,2,3)
    ) a where num =1
       ) c on a.id=c.customer_price_guide_config_id and to_date(a.update_time)=to_date(c.create_time)
    where a.warehouse_code='W0A8'
    and b.update_time is not null 
  -- and a.CUSTOMER_CODE='129166' 
   -- AND GOODS_CODE='1456631'
   ) ,
   BB AS (select 
    a.customer_code,
    customer_name,
    a.warehouse_code,
    a.goods_code ,
    goods_name,
     sum(qty)qty,
    sum(sale_amt) sale_amt ,
    sum(profit) profit
from csx_analyse_tmp.csx_analyse_tmp_price_guide_config a 

  left join 
(select sale_time,inventory_dc_code,customer_code,customer_name,goods_code,goods_name,sum(sale_qty)qty,sum(sale_amt) sale_amt ,sum(profit) profit from    csx_dws.csx_dws_sale_detail_di a 
join (select shop_code from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag=0) b on a.inventory_dc_code=b.shop_code
where sdt >='20230601' and business_type_code =1
group by inventory_dc_code,customer_code,goods_code,sale_time,customer_name,goods_name) b
on a.goods_code=b.goods_code and a.warehouse_code=b.inventory_dc_code and a.customer_code=b.customer_code
and to_date(Sale_time)<= to_date(update_time) AND to_date(sale_time) > to_date(date_sub(update_time,7)) 
where  1=1
and b.customer_name is not null 
and warehouse_code='W0A8'
group by  a.customer_code,
    customer_name,
    a.warehouse_code,
    a.goods_code ,
    goods_name )

SELECT AA.id,
    performance_province_name,
    performance_city_name, 
    aa.customer_code,
    d.customer_name,
    aa.warehouse_code,
    dimension_type,      -- 商品 =0 小类 =1 中类 =2 大类 =3
    dimension_value_code ,
    aa.goods_code ,
    c.goods_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_middle_name,
    classify_small_name,
    bmk_type,
    bmk_type_name,
    bmk_code,
    bmk_name,
    float_up_rate,
    float_down_rate,
    float_amount,
    float_type,
    aa.update_time,
    suggest_price_type,
    price_type,
    price_type_name,
    addition_rate,
    second_priceType  ,  
    second_config,
    second_addition_rate,
    second_suggestPriceType, --f 建议售价类型: 1-高;2:中;3:低
    suggest_price_high,
    suggest_price_mid,
    suggest_price_low,
    yh_shop_price,
    suggest_type,
    price_begin_time,
    price_end_time,
    goods_update_time,
    disable_suggest_price_high,
    disable_suggest_price_mid,
    disable_suggest_price_low,
    disable_yh_shop_price,
    disable_suggest_type,
    disable_price_begin_time,
    disable_price_end_time,
    disable_goods_update_time,
    change_before_code,
    additionRate_before,
    additionRate_after,
    create_time,
    BB.qty,
    sale_amt ,
    profit FROM AA 
    LEFT JOIN 
    BB ON AA.customer_code=BB.customer_code AND AA.warehouse_code=BB.warehouse_code AND AA.goods_code=BB.goods_code
    left join 
    (select goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_middle_name,classify_small_name from csx_dim.csx_dim_basic_goods where sdt='current') c on aa.goods_code=c.goods_code
    left join 
    (select performance_province_name,performance_city_name,customer_code,customer_name from csx_dim.csx_dim_crm_customer_info where sdt='current')d on aa.customer_code=d.customer_code