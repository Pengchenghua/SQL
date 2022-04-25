set dcid = ('W0A8','W0A7','W0A3','W0A2','W0A6','W0A5','W0R9','W0N0','W0W7','W0N1','W0AS','W0P8','W0Q2','W0Q9');

select  province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        product_code,
        product_name,
        count(province_code)over(partition by product_name ) as aa,
        concat_ws('|', collect_set(province_name))over(partition by product_name ) as bb
from (
select province_code,province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        product_code,
       product_name
from 
(SELECT shop_code,
       product_code,
       product_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_info a 
join 
(select classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m where sdt='current') b on a.small_category_code=b.category_small_code
WHERE a.sdt='current'
    and des_specific_product_status in ('0','2')
  AND shop_code IN ${hiveconf:dcid} 
  ) a 
  join 
  (select shop_id,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.shop_code=b.shop_id
  group by 
   province_code,province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        product_code,
       product_name
       ) a 
       
       ;
group by province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        product_code,
        product_name
       ;

select * from csx_dw.dws_basic_w_a_csx_product_info where sdt='current'
;



create temporary table csx_tmp.temp_sku_01 as 
select province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        count(distinct product_code) as all_sku
from 
(SELECT shop_code,
       product_code,
       product_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name
FROM csx_dw.dws_basic_w_a_csx_product_info a 
join 
(select classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m where sdt='current') b on a.small_category_code=b.category_small_code
WHERE a.sdt='current'
    and des_specific_product_status in ('0','2')
  AND shop_code IN ${hiveconf:dcid} 
  ) a 
  join 
  (select shop_id,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.shop_code=b.shop_id
  group by 
        province_code,
        province_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
        ;
       

create temporary table csx_tmp.temp_sku as 
select
    province_code,
    province_name,
    -- case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city end prefecture_city,
    -- case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city_name end prefecture_city_name,
    -- a.shop_code ,
    -- shop_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name ,
    count( distinct product_code) as sku
from (
select
    province_code,
    province_name,
    -- case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city end prefecture_city,
    -- case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city_name end prefecture_city_name,
    -- a.shop_code ,
    -- shop_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name ,
    b.product_code ,
    b.product_bar_code
from(    
select
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    shop_code ,
    case when (regionalized_trade_names ='' or  regionalized_trade_names is null ) then product_name else regionalized_trade_names end  region_goods_name,
    COUNT(DISTINCT product_bar_code ) as aa 
from
    csx_dw.dws_basic_w_a_csx_product_info a
    join 
    (select classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m where sdt='current') b on a.small_category_code=b.category_small_code
where
    sdt='current'
  and  des_specific_product_status in ('0','2')
  and  root_category_code in ('10','11','12','13')
  and a.product_name!=''
  and shop_code in ('W0A2','W0A3', 'W0A5', 'W0A6', 'W0A7', 'W0A8',  'W0F4', 'W0K1', 'W0R9', 'W0K6', 'W0L3','W0N0', 'W0N1',  'W0P5', 'W0P8', 'W0Q2', 'W0Q9')
group by shop_code ,
classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
   -- product_code,
    case when  (regionalized_trade_names ='' or  regionalized_trade_names is null ) then product_name else regionalized_trade_names end 
)a
left join 
    (select shop_code ,
    	product_code ,
    case when (regionalized_trade_names ='' or  regionalized_trade_names is null ) then product_name else regionalized_trade_names  end regionalized_trade_names ,
    product_bar_code,
    root_category_code
    from csx_dw.dws_basic_w_a_csx_product_info 
    where sdt='current') b on trim(a.region_goods_name)=trim(b.regionalized_trade_names) and a.shop_code=b.shop_code
left join 
 (select location_code,
     shop_name,
     province_code,
     province_name,
     prefecture_city,
     prefecture_city_name 
 from csx_dw.csx_shop where sdt='current') c on a.shop_code=c.location_code
where aa >1
) a 
group by
    province_code,
    province_name,
    -- case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city end prefecture_city,
    -- case when province_name in ('北京市','上海市','重庆市') then '-' else prefecture_city_name end prefecture_city_name,
    -- a.shop_code ,
    -- shop_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name;
  
  
  
  --求省区一品多码
  
    select province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name ,
    sum(all_sku),
    sum(sku) sku from
(
    select province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name ,
    all_sku,
    0 sku from  csx_tmp.temp_sku_01 
    union all 
      select province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name ,
    0 all_sku,
    sku from  csx_tmp.temp_sku
    ) a 
    group by  province_code,
    province_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name ;