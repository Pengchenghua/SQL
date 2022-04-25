-- 商品简报  B端商品负毛利月统计
-- 核心逻辑：统计月负毛利商品

-- 切换tez计算引擎
set mapred.job.name=report_sale_r_m_negative_goods;
-- set hive.execution.engine=tez;
set tez.queue.name=caishixian;

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.snappycodec;
set mapred.output.compression.type=block;
set parquet.compression=snappy;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 统计日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月月初
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'),'-','');

-- 库存操作起始日期
set wms_start_day = regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -11),'-','');

-- 当前月
set currnet_month = substr(${hiveconf:current_day}, 1, 6);

-- 销售明细表
set source_sale_detail = csx_dw.dws_sale_r_d_detail;

-- 库存操作明细表
set source_wms_stock_detail = csx_dw.dws_wms_r_d_batch_detail;

-- 工厂订单明细
set source_factory_order = csx_dw.dws_mms_r_a_factory_order;

-- 客户维度表
set source_customer = csx_dw.dws_crm_w_a_customer;

-- 商品维度表
set source_product = csx_dw.dws_basic_w_a_csx_product_m;

-- 目标表
set target_table = csx_dw.report_sale_r_m_negative_goods;


-- 负毛利商品价格链
drop table csx_tmp.tmp_2b_sale_trace;
create temporary table csx_tmp.tmp_2b_sale_trace
as
select 
  t1.credential_no,
  t1.region_code,
  t1.region_name,
  t1.province_code,
  t1.province_name,
  t1.city_group_code,
  t1.city_group_name,  
  t1.customer_no,
  t4.customer_name,
  t1.goods_code,
  t5.goods_name,
  t1.sales_qty,
  t1.sales_value,
  t1.sales_cost,
  t1.profit,
  t1.cost_price,
  t1.purchase_price,
  t1.middle_office_price,
  t1.sales_price,
  t3.fact_price,
  t1.goods_group_sales_value,
  t1.goods_group_profit
from 
  (
    select 
      split(id, '&')[0] as credential_no,
      region_code,
      region_name,
      province_code,
      province_name,
	    city_group_code,
  	  city_group_name,
      customer_no,
      customer_name,
      goods_code,
      goods_name,
      sales_qty,
      sales_value,
      sales_cost,
      profit,
	    purchase_price_flag,
      case when sales_type<>'fanli' then cost_price end as cost_price,
      case when purchase_price_flag='1' and sales_type<>'fanli' then purchase_price end as purchase_price,
      case when sales_type<>'fanli' then middle_office_price end as middle_office_price,
      case when sales_type<>'fanli' then sales_price end as sales_price,
      sum(sales_value) over(partition by goods_code) as goods_group_sales_value,
      sum(profit) over(partition by goods_code) as goods_group_profit
    from ${hiveconf:source_sale_detail} 
    where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day} and channel_code in ('1', '7', '9')
  )t1 
  left outer join 
  (
    select
	  t2.goods_code,
	  t2.credential_no,
	  sum(t2.qty) as qty,
	  sum(t3.fact_price*t2.qty)/sum(case when t3.fact_price is not null then t2.qty end) fact_price
	from 
	(
	  select
	  	goods_code,
	  	credential_no,
	  	source_order_no,
	  	sum(qty) as qty
	  from ${hiveconf:source_wms_stock_detail} 
	  where sdt >= ${hiveconf:wms_start_day} 
	  and move_type in ('107A', '108A')
	  group by goods_code, credential_no, source_order_no
    )t2 
    left outer join 
    (
      select 
      	goods_code,
      	order_code,
        sum(fact_values)/sum(goods_reality_receive_qty) as fact_price
      from ${hiveconf:source_factory_order} 
      where sdt >= ${hiveconf:wms_start_day} and mrp_prop_key in('3061','3010')
      group by goods_code, order_code
    )t3 on t2.source_order_no = t3.order_code and t2.goods_code = t3.goods_code
	group by t2.goods_code,t2.credential_no
  )t3 on t1.goods_code = t3.goods_code and t1.credential_no = t3.credential_no
  left outer join 
  (
    select 
		customer_no,
		customer_name
    from ${hiveconf:source_customer} 
    where sdt = 'current' 
  )t4 on t4.customer_no = t1.customer_no
  left outer join 
  (
    select 
		goods_id,
		regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name,
		department_id,
		department_name
    from ${hiveconf:source_product} 
    where sdt = 'current'
  )t5 on t5.goods_id = t1.goods_code
;



-- 负毛利商品汇总
drop table csx_tmp.tmp_negative_goods_sale;
create temporary table csx_tmp.tmp_negative_goods_sale
as
select 
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,  
  customer_no,
  customer_name,
  goods_code,
  goods_name,
  --  总销量
  sum(sales_qty) as sales_qty,          
  --  总毛利额
  sum(profit) as profit,				  
  --  总销售额
  sum(sales_value) as sales_value,	  
  -- 负毛利额
  sum(case when profit < 0 then profit else 0 end) as negative_profit,  
   -- 物料价格
  round(sum(coalesce(fact_price, 0)*sales_qty)/sum(case when fact_price is null then 0 else sales_qty end), 6) as fact_price,
  --  成本价格
  sum(cost_price * sales_qty) / sum(case when cost_price is null then 0 else sales_qty end) as cost_price,
  --  采购价格
  sum(purchase_price * sales_qty) / sum(case when purchase_price is null then 0 else sales_qty end) as purchase_price,
  --  中台报价
  sum(middle_office_price * sales_qty) / sum(case when middle_office_price is null then 0 else sales_qty end) as middle_office_price,
  --  销售价格
  sum(sales_price * sales_qty) / sum(case when sales_price is null then 0 else sales_qty end) as sales_price,
  --  大区商品客户毛利排名
  row_number() over(partition by region_code, goods_code order by sum(profit) asc) as region_customer_profit_ranking,
  -- 	省区商品客户毛利排名
  row_number() over(partition by province_code, goods_code order by sum(profit) asc) as province_customer_profit_ranking
from csx_tmp.tmp_2b_sale_trace 
where goods_group_sales_value > 0 and goods_group_profit < 0 
group by region_code, region_name, province_code, province_name,city_group_code,city_group_name, customer_no, customer_name, goods_code, goods_name
union all 
select 
  '0' as region_code,
  '全国' as region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,    
  customer_no,
  customer_name,
  goods_code,
  goods_name,
  sum(sales_qty) as sales_qty,          --  总销量
  sum(profit) as profit,				  --  总毛利额
  sum(sales_value) as sales_value,	  --  总销售额
   -- 负毛利额
  sum(case when profit < 0 then profit else 0 end) as negative_profit,  
   -- 物料价格
  round(sum(coalesce(fact_price, 0)*sales_qty)/sum(case when fact_price is null then 0 else sales_qty end), 6) as fact_price,
  --  成本价格
  sum(cost_price * sales_qty) / sum(case when cost_price is null then 0 else sales_qty end) as cost_price,
  --  采购价格
  sum(purchase_price * sales_qty) / sum(case when purchase_price is null then 0 else sales_qty end) as purchase_price,
  --  中台报价
  sum(middle_office_price * sales_qty) / sum(case when middle_office_price is null then 0 else sales_qty end) as middle_office_price,
  --  销售价格
  sum(sales_price * sales_qty) / sum(case when sales_price is null then 0 else sales_qty end) as sales_price,
  --  大区商品客户毛利排名
  row_number() over(partition by  goods_code order by sum(profit) asc) as region_customer_profit_ranking,
  -- 	省区商品客户毛利排名
  row_number() over(partition by province_code, goods_code order by sum(profit) asc) as province_customer_profit_ranking
from csx_tmp.tmp_2b_sale_trace 
where goods_group_sales_value > 0 and goods_group_profit < 0 
group by  province_code, province_name, city_group_code,city_group_name,customer_no, customer_name, goods_code, goods_name
;



with negative_city_goods_sale as
-- 城市负毛利商品销售
(
  select 
    t1.region_code,
    t1.region_name,
    t1.province_code,
    t1.province_name,
    t1.city_group_code,
    t1.city_group_name, 	
    t1.goods_code,
    t1.goods_name,
    t1.sales_qty,
    t1.profit,
    t1.sales_value,
    t1.negative_profit,
    t1.fact_price,
    t1.cost_price,
    t1.purchase_price,
    t1.middle_office_price,
    t1.sales_price,
    --t1.province_profit_ranking,
    t1.city_goods_profit_ranking,
    concat(t2.top1_customer, ':', round(t2.top1_customer_profit/t1.negative_profit*100, 0), '%') as top1_customer_prorate
  from 
  (
     select 
       region_code,
       region_name,
       province_code,
       province_name,
       city_group_code,
       city_group_name, 	   
       goods_code,
       goods_name,
       sum(sales_qty) as sales_qty,          --  总销量
       sum(profit) as profit,          --  总毛利额
       sum(sales_value) as sales_value,    --  总销售额
       sum(negative_profit) as negative_profit, 
       -- 物料价格
       round(sum(coalesce(fact_price, 0)*sales_qty)/sum(case when fact_price is null then 0 else sales_qty end), 6) as fact_price, 
       --  成本价格
       sum(cost_price * sales_qty) / sum(case when cost_price is null then 0 else sales_qty end) as cost_price,
       --  采购价格
       sum(purchase_price * sales_qty) / sum(case when purchase_price is null then 0 else sales_qty end) as purchase_price,
       --  中台报价
       sum(middle_office_price * sales_qty) / sum(case when middle_office_price is null then 0 else sales_qty end) as middle_office_price,
       --  销售价格
       sum(sales_price * sales_qty) / sum(case when sales_price is null then 0 else sales_qty end) as sales_price,
       -- 省区毛利排名
       --row_number() over(partition by region_code, goods_code order by sum(profit) asc) as province_profit_ranking,
       -- 城市商品毛利排名
       row_number() over(partition by region_code, province_code,city_group_code order by sum(profit) asc) as city_goods_profit_ranking
     from csx_tmp.tmp_negative_goods_sale 
     where region_code <> '0'
     group by region_code, region_name, province_code, province_name,city_group_code,city_group_name, goods_code, goods_name
     having sales_value > 0 and profit < 0 
  )t1 join 
  (
    select 
      region_code,
      province_code,
      city_group_code,	  
      goods_code,
      concat_ws('-', customer_no, customer_name) as top1_customer,
      profit as top1_customer_profit
    from csx_tmp.tmp_negative_goods_sale　
    where province_customer_profit_ranking = 1 
	and region_code <> '0'
  )t2 on t1.region_code = t2.region_code and t1.province_code = t2.province_code and t1.city_group_code = t2.city_group_code and t1.goods_code = t2.goods_code
),
-- 省区负毛利商品销售
negative_province_goods_sale as
(
  select 
    t1.region_code,
    t1.region_name,
    t1.province_code,
    t1.province_name,
    t1.goods_code,
    t1.goods_name,
    t1.sales_qty,
    t1.profit,
    t1.sales_value,
    t1.negative_profit,
    t1.fact_price,
    t1.cost_price,
    t1.purchase_price,
    t1.middle_office_price,
    t1.sales_price,
    t1.province_profit_ranking,
    t1.province_goods_profit_ranking,
    concat(t2.top1_customer, ':', round(t2.top1_customer_profit/t1.negative_profit*100, 0), '%') as top1_customer_prorate
  from 
  (
     select 
       region_code,
       region_name,
       province_code,
       province_name,
       goods_code,
       goods_name,
       sum(sales_qty) as sales_qty,          --  总销量
       sum(profit) as profit,          --  总毛利额
       sum(sales_value) as sales_value,    --  总销售额
       sum(negative_profit) as negative_profit, 
       -- 物料价格
       round(sum(coalesce(fact_price, 0)*sales_qty)/sum(case when fact_price is null then 0 else sales_qty end), 6) as fact_price, 
       --  成本价格
       sum(cost_price * sales_qty) / sum(case when cost_price is null then 0 else sales_qty end) as cost_price,
       --  采购价格
       sum(purchase_price * sales_qty) / sum(case when purchase_price is null then 0 else sales_qty end) as purchase_price,
       --  中台报价
       sum(middle_office_price * sales_qty) / sum(case when middle_office_price is null then 0 else sales_qty end) as middle_office_price,
       --  销售价格
       sum(sales_price * sales_qty) / sum(case when sales_price is null then 0 else sales_qty end) as sales_price,
       -- 省区毛利排名
       row_number() over(partition by region_code, goods_code order by sum(profit) asc) as province_profit_ranking,
       -- 省区商品毛利排名
       row_number() over(partition by region_code, province_code order by sum(profit) asc) as province_goods_profit_ranking
     from csx_tmp.tmp_negative_goods_sale 
     --where region_code <> '0'
     group by region_code, region_name, province_code, province_name, goods_code, goods_name
     having sales_value > 0 and profit < 0 
  )t1 join 
  (
    select 
      region_code,
      province_code,
      goods_code,
      concat_ws('-', customer_no, customer_name) as top1_customer,
      profit as top1_customer_profit
    from csx_tmp.tmp_negative_goods_sale　
    where province_customer_profit_ranking = 1 
	--and region_code <> '0'
  )t2 on t1.region_code = t2.region_code and t1.province_code = t2.province_code and t1.goods_code = t2.goods_code
),
-- 大区负毛利商品销售
negative_region_sale as 
(
  select 
   t1.region_code,
   -- 大区名称
   t1.region_name,
   -- 商品编码
   t1.goods_code,
   -- 商品名称
   t1.goods_name,
   -- 物料价格
   t1.fact_price,
   t1.cost_price,
   t1.purchase_price,
   t1.middle_office_price,
   t1.sales_price,
   t1.sales_qty,
   t1.profit,
   t1.sales_value,
   t1.negative_profit,
   -- 最大负毛利客户在总负毛利中占比
   concat(t3.top1_customer, ':', round(t3.top1_customer_profit/t1.negative_profit*100, 0), '%') as top1_customer_prorate, 
   concat(t2.top1_province, ':', round(t2.top1_province_profit/t1.negative_profit*100, 0), '%') as top1_province_prorate,
   region_goods_profit_ranking
  from 
  (
    select 
      region_code,
      -- 大区名称
      region_name,
      -- 商品编码
      goods_code,
      -- 商品名称
      goods_name,
      -- 物料价格
      round(sum(coalesce(fact_price, 0)*sales_qty)/sum(case when fact_price is null then 0 else sales_qty end), 6) as fact_price, 
      --  成本价格
      sum(cost_price * sales_qty) / sum(case when cost_price is null then 0 else sales_qty end) as cost_price,
      --  采购价格
      sum(purchase_price * sales_qty) / sum(case when purchase_price is null then 0 else sales_qty end) as purchase_price,
      --  中台报价
      sum(middle_office_price * sales_qty) / sum(case when middle_office_price is null then 0 else sales_qty end) as middle_office_price,
      --  销售价格
      sum(sales_price * sales_qty) / sum(case when sales_price is null then 0 else sales_qty end) as sales_price,
      --  总销量
      sum(sales_qty) as sales_qty,    
      --  总毛利额      
      sum(profit) as profit,
      --  总销售额        
      sum(sales_value) as sales_value,  
      -- 负毛利金额  
      sum(negative_profit) as negative_profit,  
      -- 大区商品毛利排名
      row_number() over(partition by region_code order by sum(profit) asc) as region_goods_profit_ranking
    from csx_tmp.tmp_negative_goods_sale
    group by region_code, region_name, goods_code, goods_name 
    having sales_value > 0 and profit < 0
  )t1 join 
  (
    select 
      region_code,
      goods_code,
      concat_ws('-', province_code, province_name) as top1_province,
      profit as top1_province_profit
    from negative_province_goods_sale 
    where province_profit_ranking = 1
  )t2 on t1.region_code = t2.region_code and t1.goods_code = t2.goods_code
  join 
  (
    select 
      region_code,
      goods_code,
      concat(customer_no, '-', customer_name) as top1_customer,
      profit as top1_customer_profit
    from csx_tmp.tmp_negative_goods_sale 
    where region_customer_profit_ranking = 1
  )t3 on t1.region_code = t3.region_code and t1.goods_code = t3.goods_code
)
insert overwrite table ${hiveconf:target_table} partition(month)
-- 全国范围
select 
  -- 唯一主键
  concat_ws('&', '0', region_code, goods_code, ${hiveconf:currnet_month}) as id,
  -- 全国范围标识
  '0' as statistics_erea,
  -- 大区编码
  region_code,
  -- 大区名称
  region_name,
  -- 省区编码
  '-' as province_code,
  -- 省区名称
  '-' as province_name,
  '-' as city_group_code,
  '-' as city_group_name,  
  -- 商品编码
  goods_code,
  -- 商品名称
  goods_name,
  -- 物料价格
  cast(coalesce(fact_price, 0) as decimal(20, 6)) as fact_price, 
  --  成本价格
  cast(cost_price as decimal(20, 6)) as cost_price,
  --  采购价格
  cast(purchase_price as decimal(20, 6)) as purchase_price,
  --  中台报价
  cast(middle_office_price as decimal(20, 6)) as middle_office_price,
  --  销售价格
  cast(sales_price as decimal(20, 6)) as sales_price,
  --  总销量
  sales_qty,    
  --  总毛利额      
  profit,
  --  总销售额				
  sales_value,	
  -- 负毛利金额  
  negative_profit, 
  -- 最大负毛利客户在总负毛利中占比
  top1_customer_prorate,
  -- 最大负毛利省区在总负毛利中占比
  top1_province_prorate,
  region_goods_profit_ranking as goods_profit_ranking,
  ${hiveconf:currnet_month} as month
from negative_region_sale 
where region_code = '0' and region_goods_profit_ranking <= 20
union all 
-- 大区范围
 select 
  -- 唯一主键
  concat_ws('&', '1', region_code, goods_code, ${hiveconf:currnet_month}) as id,
  -- 全国范围标识
  '1' as statistics_erea,
  -- 大区编码
  region_code,
  -- 大区名称
  region_name,
  -- 省区编码
  '-' as province_code,
  -- 省区名称
  '-' as province_name,
  '-' as city_group_code,
  '-' as city_group_name,  
  -- 商品编码
  goods_code,
  -- 商品名称
  goods_name,
  -- 物料价格
  cast(coalesce(fact_price, 0) as decimal(20, 6)) as fact_price, 
  --  成本价格
  cast(cost_price as decimal(20, 6)) as cost_price,
  --  采购价格
  cast(purchase_price as decimal(20, 6)) as purchase_price,
  --  中台报价
  cast(middle_office_price as decimal(20, 6)) as middle_office_price,
  --  销售价格
  cast(sales_price as decimal(20, 6)) as sales_price,
  --  总销量
  sales_qty,    
  --  总毛利额      
  profit,
  --  总销售额				
  sales_value,	
  -- 负毛利金额  
  negative_profit, 
  -- 最大负毛利客户在总负毛利中占比
  top1_customer_prorate,
  -- 最大负毛利省区在总负毛利中占比
  top1_province_prorate,
  region_goods_profit_ranking as goods_profit_ranking,
  ${hiveconf:currnet_month} as month
from negative_region_sale 
where region_code <> '0' and region_goods_profit_ranking <= 20
-- 省区范围
union all 
select 
  -- 唯一主键
  concat_ws('&', '2', province_code, goods_code, ${hiveconf:currnet_month}) as id,
   -- 省区范围标识
  '2' as statistics_erea,
  region_code,
  region_name,
  province_code,
  province_name,
  '-' as city_group_code,
  '-' as city_group_name,  
   -- 商品编码
  goods_code,
  -- 商品名称
  goods_name,
  -- 物料价格
  cast(coalesce(fact_price, 0) as decimal(20, 6)) as fact_price, 
  --  成本价格
  cast(cost_price as decimal(20, 6)) as cost_price,
  --  采购价格
  cast(purchase_price as decimal(20, 6)) as purchase_price,
  --  中台报价
  cast(middle_office_price as decimal(20, 6)) as middle_office_price,
  --  销售价格
  cast(sales_price as decimal(20, 6)) as sales_price,
  --  总销量
  sales_qty,    
  --  总毛利额      
  profit,
  --  总销售额				
  sales_value,	
  -- 负毛利金额  
  negative_profit, 
  -- 最大负毛利客户在总负毛利中占比
  top1_customer_prorate,
  -- 最大负毛利省区在总负毛利中占比
  '-' as top1_province_prorate,
  province_goods_profit_ranking as goods_profit_ranking,
  ${hiveconf:currnet_month} as month
from negative_province_goods_sale
where region_code <> '0' and province_goods_profit_ranking <= 20
-- 城市范围
union all 
select 
  -- 唯一主键
  concat_ws('&', '3', city_group_code, goods_code, ${hiveconf:currnet_month}) as id,
   -- 城市范围标识
  '3' as statistics_erea,
  region_code,
  region_name,
  province_code,
  province_name,
  city_group_code,
  city_group_name,  
   -- 商品编码
  goods_code,
  -- 商品名称
  goods_name,
  -- 物料价格
  cast(coalesce(fact_price, 0) as decimal(20, 6)) as fact_price, 
  --  成本价格
  cast(cost_price as decimal(20, 6)) as cost_price,
  --  采购价格
  cast(purchase_price as decimal(20, 6)) as purchase_price,
  --  中台报价
  cast(middle_office_price as decimal(20, 6)) as middle_office_price,
  --  销售价格
  cast(sales_price as decimal(20, 6)) as sales_price,
  --  总销量
  sales_qty,    
  --  总毛利额      
  profit,
  --  总销售额				
  sales_value,	
  -- 负毛利金额  
  negative_profit, 
  -- 最大负毛利客户在总负毛利中占比
  top1_customer_prorate,
  -- 最大负毛利省区在总负毛利中占比
  '-' as top1_province_prorate,
  city_goods_profit_ranking as goods_profit_ranking,
  ${hiveconf:currnet_month} as month
from negative_city_goods_sale
where region_code <> '0' and city_goods_profit_ranking <= 20;
