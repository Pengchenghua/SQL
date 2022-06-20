--仓储备货销售数据&周转&库存[线上任务]
--省区	城市	采购部编码	采购部名称	管理一级分类编码	管理分类一级分类	管理三级分类编码	管理分类三级分类	管理三级分类编码	管理分类三级分类
--SPU	价格带	商品编码	商品名称	单位	规格	品牌	销售数量	销售额	毛利额	毛利率	动销天数	客户渗透率	动销客户数	下单次数	
--销售额排名	销售量排名	"动销天数排名"	渗透率排名
--取入库表： DC编码	DC名称	期间入库数量	期间入库金额	
--取周转表： 当前库存量	当前库存额	库存周转天数	期间DMS（日均销量）	商品状态  
--有效标识	库存属性（存储/货到即配）stock_properties_name	退货标识 sales_return_tag	安全库存天数 type=1  value

set shopid=('W0A8','W0A7','W0A6','W0N0','W0A5','W0AS','W0R9','W0A3','W0A2');
set edate='${enddate}';
set sdate=regexp_replace(add_months(trunc(${hiveconf:edate},'MM'),-2),'-',''); -- 近3个月
set sdate_1=regexp_replace(add_months(trunc(${hiveconf:edate},'MM'),-1),'-',''); -- 近1个月
set edate=regexp_replace(${hiveconf:edate},'-','');

-- select ${hiveconf:sdate},${hiveconf:edate},'${enddate}',${hiveconf:sdate_1};

-- 销售基础

drop table csx_tmp.temp_sale_t1;
create temporary table csx_tmp.temp_sale_t1 as 
select province_code,
        province_name,
        business_type_name,
        dc_code,
        goods_code,
        classify_middle_code,
        classify_middle_name,
        sdt,
        customer_no,
        count(distinct order_no) as sales_order_cn,
        sum(sales_qty) sales_qty,
        sum(sales_cost) sales_cost,
        sum(sales_value) sales_value,
        sum(profit) profit,
        sum(case when substr(sdt,1,6)= substr(${hiveconf:sdate},1,6) then sales_qty end )   sales_qty_02,
        sum(case when substr(sdt,1,6)= substr(${hiveconf:sdate},1,6)  then sales_value end ) sales_value_02,
        sum(case when substr(sdt,1,6)= substr(${hiveconf:sdate_1},1,6)  then sales_qty end )   sales_qty_03,
        sum(case when substr(sdt,1,6)= substr(${hiveconf:sdate_1},1,6)  then sales_value end ) sales_value_03,
        sum(case when substr(sdt,1,6)= substr(${hiveconf:edate},1,6)  then sales_qty end )   sales_qty_04,
        sum(case when substr(sdt,1,6)= substr(${hiveconf:edate},1,6)  then sales_value end ) sales_value_04
from csx_dw.dws_sale_r_d_detail
where sdt >= ${hiveconf:sdate}
    and sdt<= ${hiveconf:edate}
    and dc_code in ${hiveconf:shopid}
    and sales_type !='fanli'
    and business_type_code='1'
    and channel_code in ('1','7','9')
   -- and province_name='安徽省'
    group by  province_code,
        province_name,
        classify_middle_code,
        classify_middle_name,
         business_type_name,
        dc_code,
        goods_code,
        sdt,
        customer_no
;

-- 排名
drop table csx_tmp.temp_sale_t2;
create temporary table csx_tmp.temp_sale_t2 as 
select  
        a.dc_code,
        goods_code,
        a.classify_middle_code,
        classify_middle_name,
        cust_no,    -- 客户数
        sale_date,                       --销售天数
        sales_order_cn,      --订单数
        sales_qty,
        sales_cost,
        sales_value,
        profit,
        profit/sales_value profit_rate,
        all_cust_no,
        cust_no/all_cust_no percolation_rate,        --客户渗透率
        dense_rank()over(partition by a.dc_code,a.classify_middle_code order by sales_value desc) as sales_rank,  --销售额排名
        dense_rank()over(partition by a.dc_code,a.classify_middle_code order by sales_qty desc) as sales_qty_rank,  --销售额排名
        dense_rank()over(partition by a.dc_code,a.classify_middle_code order by sale_date desc) as sale_date_rank ,   -- 动销天数排名
        dense_rank()over(partition by a.dc_code,a.classify_middle_code order by cust_no/all_cust_no desc) percolation_rate_rank,
         sales_qty_02,
         sales_value_02,
         sales_qty_03,
         sales_value_03,
         sales_qty_04,
         sales_value_04
from(
select 
        dc_code,
        goods_code,
        classify_middle_code,
        classify_middle_name,
        count(distinct customer_no ) as cust_no,    -- 客户数
        count(distinct sdt )sale_date,                       --销售天数
        sum(sales_order_cn) as sales_order_cn,      --订单数
        sum(sales_qty) sales_qty,
        sum(sales_cost) sales_cost,
        sum(sales_value) sales_value,
        sum(profit) profit,
        sum( sales_qty_02)  sales_qty_02,
        sum( sales_value_02)  sales_value_02,
        sum( sales_qty_03)  sales_qty_03,
        sum( sales_value_03)  sales_value_03,
        sum( sales_qty_04)  sales_qty_04,
        sum( sales_value_04)  sales_value_04
from  csx_tmp.temp_sale_t1
group by 
       
        classify_middle_code,
        classify_middle_name,
        dc_code,
        goods_code
        ) a
left join 
   (select 
        dc_code,
        classify_middle_code,
        count(distinct customer_no ) all_cust_no
from csx_tmp.temp_sale_t1 
group by 
        classify_middle_code,
        dc_code)  b on  a.dc_code=b.dc_code  and a.classify_middle_code=b.classify_middle_code  ;

 
--供应商入库 
drop table csx_tmp.temp_sale_t3;
create temporary table csx_tmp.temp_sale_t3 as 
select receive_location_code,
    goods_code,  
    sum(receive_qty) receive_qty,
    sum(amount) receive_amt
from csx_dw.dws_wms_r_d_entry_batch 
where sdt >= ${hiveconf:sdate}
    and sdt<= ${hiveconf:edate}
    and receive_location_code in ${hiveconf:shopid}
    and order_type_code like 'P%'
  --  and province_name='安徽省'
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
where  sdt= ${hiveconf:edate}
   -- and dc_code in ${hiveconf:shopid}
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
WHERE sdt = ${hiveconf:edate}
    and dc_code in ${hiveconf:shopid}

;


drop table csx_tmp.temp_sale_t5;
create temporary table csx_tmp.temp_sale_t5
 as select a.province_code,
       a.province_name,
       city_code,city_name,
       a.dc_code,
       shop_name,
       business_division_code,
       business_division_name,
       division_code,
       division_name,
       classify_large_code ,
       classify_large_name ,
       a.classify_middle_code ,
       a.classify_middle_name ,
       classify_small_code ,
       classify_small_name ,
        spu_goods_code,
       spu_goods_name,
       price_belt_type,         --价格带
       a.goods_id,
       goods_name,
       unit_name,
       standard,
       brand_name,
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
       coalesce(sales_qty_rank,        0)as    sales_qty_rank,              --销售量排名
       coalesce(sale_date_rank,        0)as    sale_date_rank,              -- 动销天数排名
       dense_rank()over(partition by a.dc_code,a.classify_middle_code order by percolation_rate desc) as    percolation_rate_rank,        -- 渗透率排名
       coalesce(dms ,0)as    dms ,
       coalesce(receive_qty) as receive_qty,
        coalesce(receive_amt,0)as    receive_amt,                           --期间入库额
       coalesce(final_qty ,0)as    final_qty ,
       coalesce(final_amt ,0)as    final_amt ,
       coalesce(days_turnover_30 ,0)as    days_turnover_30 ,
       coalesce(stock_safety_days,     0)as    stock_safety_days,           --库存安全天数
       coalesce(sales_return_tag,0)as    sales_return_tag,
       stock_properties_name,
        sales_qty_02,
        sales_value_02,
        sales_qty_03,
        sales_value_03,
        sales_qty_04,
        sales_value_04
      
from csx_tmp.temp_sale_t4 a 
left join 
csx_tmp.temp_sale_t2 b on a.dc_code=b.dc_code and a.goods_id=b.goods_code
left join 
csx_tmp.temp_sale_t3  c on a.dc_code=c.receive_location_code and a.goods_id=c.goods_code
left join 
(select goods_id,goods_name,spu_goods_code,spu_goods_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') d on a.goods_id=d.goods_id

left join
(select shop_id,shop_name,province_code,province_name,city_code,city_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') f on a.dc_code=f.shop_id
;

insert overwrite table  csx_tmp.report_scm_r_d_select_goods partition(months)
select b.province_code province_code,
       b.province_name province_name,
       b.city_code,
       b.city_name,
       a.dc_code,
       b.shop_name,
       business_division_code,
       business_division_name,
       division_code,
       division_name,
       classify_large_code ,
       classify_large_name ,
       a.classify_middle_code ,
       a.classify_middle_name ,
       classify_small_code ,
       classify_small_name ,
        spu_goods_code,
       spu_goods_name,
       price_belt_type,         --价格带
       a.goods_id,
       goods_name,
       unit_name,
       standard,
       brand_name,
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
       coalesce(sales_qty_rank,        0)as    sales_qty_rank,              --销售量排名
       coalesce(sale_date_rank,        0)as    sale_date_rank,              -- 动销天数排名
       dense_rank()over(partition by a.dc_code,a.classify_middle_code order by percolation_rate desc) as    percolation_rate_rank,        -- 渗透率排名
       coalesce(dms ,0)as    dms ,
       coalesce(receive_qty) as receive_qty,
        coalesce(receive_amt,0)as    receive_amt,                           --期间入库额
       coalesce(final_qty ,0)as    final_qty ,
       coalesce(final_amt ,0)as    final_amt ,
       coalesce(days_turnover_30 ,0)as    days_turnover_30 ,
       coalesce(stock_safety_days,     0)as    stock_safety_days,           --库存安全天数
       coalesce(sales_return_tag,0)as    sales_return_tag,
       stock_properties_name,
        sales_qty_02,
        sales_value_02,
        sales_qty_03,
        sales_value_03,
        sales_qty_04,
        sales_value_04,
        current_timestamp(),
        substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from csx_tmp.temp_sale_t5 a 
left join
(select * from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.dc_code=b.shop_id
where sales_value !=0;



show create table  csx_tmp.temp_sale_t5 ;

drop table csx_tmp.report_scm_r_d_select_goods;
CREATE  TABLE `csx_tmp.report_scm_r_d_select_goods`(
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_code` string COMMENT '城市编码', 
  `city_name` string COMMENT '城市名称', 
  `dc_code` string COMMENT 'DC编码',
  `dc_name` string COMMENT 'DC名称', 
  `business_division_code` string COMMENT '采购部类', 
  `business_division_name` string COMMENT '采购部类名称', 
  `division_code` string COMMENT '部类编码', 
  `division_name` string COMMENT '部类名称', 
  `classify_large_code` string COMMENT '管理大类编码', 
  `classify_large_name` string COMMENT '管理大类名称', 
  `classify_middle_code` string COMMENT '管理中类编码', 
  `classify_middle_name` string COMMENT '管理中类名称', 
  `classify_small_code` string COMMENT '管理小类编码', 
  `classify_small_name` string COMMENT '管理小类名称', 
  `spu_goods_code` string comment 'SPU商品编码', 
  `spu_goods_name` string COMMENT 'SPU商品名称',  
  `price_belt_type` string COMMENT '价格带', 
  `goods_id` string COMMENT '商品编码', 
  `goods_name` string COMMENT '商品名称', 
  `unit_name` string COMMENT '单位', 
  `standard` string COMMENT '规格', 
  `brand_name` string COMMENT '品牌', 
  `goods_status_name` string COMMENT '商品状态', 
  `sales_qty` decimal(38,6) comment '累计销量', 
  `sales_cost` decimal(38,6) COMMENT '累计销售成本', 
  `sales_value` decimal(38,6) COMMENT '累计销售额', 
  `profit` decimal(38,6) comment '累计毛利额', 
  `profit_rate` decimal(38,18) comment '累计毛利率', 
  `cust_no` bigint COMMENT '累计成交客户', 
  `sale_date` bigint COMMENT '累计成交天数', 
  `sales_order_cn` bigint COMMENT '累计订单数', 
  `percolation_rate` decimal(38,6) COMMENT '客户渗透率', 
  `sales_rank` int COMMENT '销售排名', 
  `sales_qty_rank` int COMMENT '销量排名', 
  `sale_date_rank` int COMMENT '成交天数排名', 
  `percolation_rate_rank` int COMMENT '客户渗透率排名', 
  `dms` decimal(38,6) COMMENT 'DMS', 
  `receive_qty` decimal(38,6) COMMENT '累计供应商入库量', 
  `receive_amt` decimal(38,6) COMMENT '累计供应商入库额', 
  `final_qty` decimal(38,6) COMMENT '当前库存量', 
  `final_amt` decimal(38,6) COMMENT '当前库存额', 
  `days_turnover_30` decimal(38,6) COMMENT '近30天周转天数', 
  `stock_safety_days` string COMMENT '库存安全天数', 
  `sales_return_tag` string COMMENT '退货标识', 
  `stock_properties_name` string COMMENT '库存属性', 
  `sales_qty_03` decimal(38,6) COMMENT '前第三个月销量', 
  `sales_value_03` decimal(38,6) COMMENT '前第三个销量额', 
  `sales_qty_02` decimal(38,6) COMMENT '前第二个月销量', 
  `sales_value_02` decimal(38,6) COMMENT '前第二个月销售额', 
  `sales_qty_01` decimal(38,6) COMMENT '当前月销量', 
  `sales_value_01` decimal(38,6) COMMENT '当前月销售额',
  update_time timestamp comment '数据更新日期'
)comment'仓储商品选品及相关参数配置表'
partitioned by (months string comment'月分区')
STORED AS parquet
;


W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0P8','W0Q9','W0A5','W0R9','W0AS','W0N0','W0W7','W0F4','W0A8','W0J2','W0K6','W0L3','W0AH','W0K1','WA96','W0BK','W0A2','W0BR','W0BH'