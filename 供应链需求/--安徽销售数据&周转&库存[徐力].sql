--安徽销售数据&周转&库存[徐力]
--省区	城市	采购部编码	采购部名称	管理一级分类编码	管理分类一级分类	管理三级分类编码	管理分类三级分类	管理三级分类编码	管理分类三级分类
--SPU	价格带	商品编码	商品名称	单位	规格	品牌	销售数量	销售额	毛利额	毛利率	动销天数	客户渗透率	动销客户数	下单次数	
--销售额排名	销售量排名	"动销天数排名"	渗透率排名
--取入库表： DC编码	DC名称	期间入库数量	期间入库金额	
--取周转表： 当前库存量	当前库存额	库存周转天数	期间DMS（日均销量）	商品状态  
--有效标识	库存属性（存储/货到即配）stock_properties_name	退货标识 sales_return_tag	安全库存天数 type=1  value

-- 销售基础
create temporary table csx_tmp.temp_sale_t1 as 
select province_code,province_name,
        city_group_code,
        city_group_name,
        business_type_name,
        dc_code,
        goods_code,
        sdt,
        customer_no,
        count(distinct order_no) as sales_order_cn,
        sum(sales_qty) sales_qty,
        sum(sales_cost) sales_cost,
        sum(sales_value) sales_value,
        sum(profit) profit
from csx_dw.dws_sale_r_d_detail
where sdt>='20211201' 
    and sdt<'20220301'
    and sales_type !='fanli'
    and business_type_code='1'
    and channel_code in ('1','7','9')
    and province_name='安徽省'
    group by  province_code,province_name,
        city_group_code,
        city_group_name,
        business_type_name,
        dc_code,
        goods_code,
        sdt,
        customer_no
;

-- 排名

create temporary table csx_tmp.temp_sale_t2 as 
select  a.province_code,
        a.province_name,
        a.city_group_code,
        a.city_group_name,
        a.business_type_name,
        a.dc_code,
        goods_code,
        cust_no,    -- 客户数
        sale_date,                       --销售天数
        sales_order_cn,      --订单数
        sales_qty,
        sales_cost,
        sales_value,
        profit,
        profit/sales_value profit_rate,
        cust_no/all_cust_no percolation_rate,        --客户渗透率
        dense_rank()over(partition by a.dc_code order by sales_value desc) as sales_rank,  --销售额排名
        dense_rank()over(partition by a.dc_code order by sales_qty desc) as sales_qty_rank,  --销售额排名
        dense_rank()over(partition by a.dc_code order by sale_date desc) as sale_date_rank,    -- 动销天数排名
        dense_rank()over(partition by a.dc_code order by cust_no/all_cust_no desc) percolation_rate_rank
from(
select  province_code,province_name,
        city_group_code,
        city_group_name,
        business_type_name,
        dc_code,
        goods_code,
        count(distinct customer_no ) as cust_no,    -- 客户数
        count(distinct sdt )sale_date,                       --销售天数
        sum(sales_order_cn) as sales_order_cn,      --订单数
        sum(sales_qty) sales_qty,
        sum(sales_cost) sales_cost,
        sum(sales_value) sales_value,
        sum(profit) profit
from  csx_tmp.temp_sale_t1
group by 
  province_code,province_name,
        city_group_code,
        city_group_name,
        business_type_name,
        dc_code,
        goods_code
        ) a
left join 
   (select province_code,province_name,
        city_group_code,
        city_group_name,
        business_type_name,
        dc_code,
        count(distinct customer_no ) all_cust_no
from csx_tmp.temp_sale_t1 
group by province_code,province_name,
        city_group_code,
        city_group_name,
        business_type_name,
        dc_code)  b on a.province_code=b.province_code and a.dc_code=b.dc_code   ;
 
--供应商入库        
create temporary table csx_tmp.temp_sale_t3 as 
select receive_location_code,
    goods_code,    
    sum(amount) receive_amt
from csx_dw.dws_wms_r_d_entry_batch 
where sdt>='20211201' 
    and sdt<'20220301'
    and order_type_code like 'P%'
    and province_name='安徽省'
    and receive_status in (1,2)
group by receive_location_code,
    goods_code
    ;

drop table   csx_tmp.temp_sale_t4;
CREATE temporary table  csx_tmp.temp_sale_t4 as 
SELECT province_code,
       province_name,
       dc_code,
       goods_id,
       standard,
       unit_name,
       brand_name,
       price_belt_type,         --价格带
       business_division_code,
       business_division_name,
       division_code,
       division_name,
       classify_large_code ,
       classify_large_name ,
       classify_middle_code ,
       classify_middle_name ,
       classify_small_code ,
       classify_small_name ,
       goods_status_name,
       dms ,
       days_turnover_30 ,
       final_qty ,
       final_amt ,
       value  stock_safety_days,    --库存安全天数
       sales_return_tag,
       stock_properties_name
FROM csx_tmp.ads_wms_r_d_goods_turnover a 
LEFT JOIN 
(select location_code,
        product_category_code,
        `type`,
        value
from csx_ods.source_scm_w_a_product_purchase_require_config 
where sdt='20220302'
    and product_category_level='5'
    and `type`=1
    ) b on a.dc_code=b.location_code and a.goods_id=b.product_category_code
LEFT JOIN 
(SELECT shop_code,
       product_code,
       sales_return_tag,
       stock_properties_name,
       price_belt_type
FROM csx_dw.dws_basic_w_a_csx_product_info
WHERE sdt='current') c on a.dc_code=c.shop_code and a.goods_id=c.product_code
WHERE sdt='20220302'
    and province_name ='安徽省';

drop table csx_tmp.temp_sale_t5;
create temporary table csx_tmp.temp_sale_t5
 as select a.province_code,
       a.province_name,
       a.dc_code,
       a.goods_id,
       goods_name,
       spu_goods_code,
       spu_goods_name,
       standard,
       unit_name,
       brand_name,
       price_belt_type,         --价格带
       business_division_code,
       business_division_name,
       division_code,
       division_name,
       classify_large_code ,
       classify_large_name ,
       classify_middle_code ,
       classify_middle_name ,
       classify_small_code ,
       classify_small_name ,
       goods_status_name,
       coalesce(sales_qty,0)as    sales_qty,
       coalesce(sales_cost,0)as    sales_cost,
       coalesce(sales_value,0)as    sales_value,
       coalesce(profit,0)as    profit,
       coalesce(profit_rate,0)as    profit_rate,
       coalesce(cust_no,               0)as    cust_no,                     -- 客户数
       coalesce(sale_date,             0)as    sale_date,                   --销售天数
       coalesce(sales_order_cn,        0)as    sales_order_cn,              --订单数
       coalesce(percolation_rate,      0)as    percolation_rate,            --客户渗透率
       coalesce(sales_rank,            0)as    sales_rank,                  --销售额排名
       coalesce(sales_qty_rank,        0)as    sales_qty_rank,              --销售额排名
       coalesce(sale_date_rank,        0)as    sale_date_rank,              -- 动销天数排名
       coalesce(percolation_rate_rank,0)as    percolation_rate_rank,
       coalesce(dms ,0)as    dms ,
       coalesce(days_turnover_30 ,0)as    days_turnover_30 ,
       coalesce(final_qty ,0)as    final_qty ,
       coalesce(final_amt ,0)as    final_amt ,
       coalesce(stock_safety_days,     0)as    stock_safety_days,           --库存安全天数
       coalesce(sales_return_tag,0)as    sales_return_tag,
       stock_properties_name,
       coalesce(receive_amt,0)as    receive_amt
from csx_tmp.temp_sale_t4 a 
left join 
csx_tmp.temp_sale_t2 b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
left join 
csx_tmp.temp_sale_t3  c on a.dc_code=c.receive_location_code and a.goods_id=c.goods_code
left join 
(select goods_id,goods_name,spu_goods_code,spu_goods_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') d on a.goods_id=d.goods_id
;

select * from csx_tmp.temp_sale_t5 where sales_value !=0;
    show create table csx_dw.dws_basic_w_a_csx_product_info;
    show create table csx_ods.source_scm_w_a_product_purchase_require_config;
    show create table csx_tmp.ads_wms_r_d_goods_turnover;
    show create table csx_dw.dws_basic_w_a_csx_product_m;
    
    select * from csx_tmp.temp_sale_t5  where goods_id='1008090';
    
    
    