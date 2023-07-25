-- 基地成本分析
采购订单大致分为
侧：
1.临时加单：一般为销售采买，报销用
2.直送：通常给报销用
3.紧急补货：一般为销售采买，报销用
采购侧
1.工厂调拨、采购导入、日采补货、手工创建、智能补货 ：正常采购单
2.临时地材：因我司原因需要地采的，由第三方地采服务公司采买。

-- 采购报价成本
create table csx_analyse_tmp.csx_analyse_tmp_purchase_price as 
  select 
    basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
    warehouse_code as dc_code,
    goods_code,
    calday as sdt,
    avg(purchase_price) as purchase_price,
	1 as data_type
  from 
  (
    select 
      a.*,
      b.calday,
	  row_number() over(partition by warehouse_code,goods_code,b.calday order by price_end_time desc) as ranks  
    from 
    (
      select 
        warehouse_code,
        product_code as goods_code,
        purchase_price,
		    price_end_time,
        regexp_replace(to_date(price_begin_time), '-', '') as price_begin_date,
        regexp_replace(to_date(price_end_time), '-', '') as price_end_date
      from csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df
      where sdt = '20230518'
        and effective = 'true' 
    ) a 
    left join 
    (
      select 
        calday
      from csx_dim.csx_dim_basic_date
      where calday >= regexp_replace(trunc(add_months('2023-05-18',-2), 'MM'), '-', '')
        and calday <= regexp_replace(current_date,'-','')  -- 关联时间维表,获取有效日期维度
    ) b on 1 = 1
    where a.price_begin_date <= b.calday and b.calday <= a.price_end_date
   ) a 
  left join 
  ( select *  
                                from csx_dim.csx_dim_shop  
                                where sdt='current' 
   )    b on a.warehouse_code=b.shop_code                         
                                where ranks = 1 
  
  
  group by warehouse_code,goods_code,calday,basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name
   ;


-- 对标对象成本
create table csx_analyse_tmp.csx_analyse_tmp_goods_pro_price as 
            select     gg2.sdt,
                gg1.performance_region_code,
                gg1.performance_province_code,
                gg1.performance_city_code,
                basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
                gg1.goods_code,
                avg(gg1.cost_price) as pro_db_price 
         from 
                        (select 
                            g3.performance_region_code,
                            g3.performance_province_code,
                            g3.performance_city_code,
                             basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
                            g4.goods_code,
                            g1.price,
                            (case when g1.source_type_code in (4,5) then g1.price -- 市调地点为批发市场，就取批发市场，如果不是则按照系统中的品类目标毛利求出成本价
                                  else g1.price*(1-g6.net_profit) end) as cost_price,
                            g1.price_begin_date,
                            g1.price_end_date 
                        from 
                                (-- 非云超报价
                                select 
                                         shop_code,product_id,price,source_type_code,
                                         regexp_replace(substr(price_begin_time,1,10),'-','') as price_begin_date,
                                         regexp_replace(substr(price_end_time,1,10),'-','') as price_end_date 
                                from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_di 
                                where sdt>=regexp_replace(add_months('2023-05-01',-2),'-','')  
                                and regexp_replace(substr(price_end_time,1,10),'-','')>='20230501'    -- 限制市调价生效期的结束日期大于开始日期
                                and shop_code in ('9149','9272','9294','9396','95E0','RW7','YS33','YW173','ZD106','ZD21','ZD281','ZD95','ZD101','9201','RS26') 
                                union all 
                                -- 云超报价
                                select 
                                         shop_code,product_id,price,source_type_code,
                                         regexp_replace(substr(price_begin_time,1,10),'-','') as price_begin_date,
                                         regexp_replace(substr(price_end_time,1,10),'-','') as price_end_date 
                                from csx_ods.csx_ods_csx_price_prod_market_research_price_di 
                                where sdt>=regexp_replace(add_months('2023-05-01',-2),'-','')  
                                and regexp_replace(substr(price_end_time,1,10),'-','')>='2023-05-01' -- 限制市调价生效期的结束日期大于开始日期
                                and shop_code in ('9149','9272','9294','9396','95E0','RW7','YS33','YW173','ZD106','ZD21','ZD281','ZD95','ZD101','9201','RS26') 
                                ) g1 
                                left join 
                                (select * 
                                from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
                                where sdt='20230518'
                                ) g2 
                                on g1.product_id=g2.id 
                                left join 
                                (select *  
                                from csx_dim.csx_dim_shop  
                                where sdt='current' 
                                ) g3 
                                on g2.location_code=g3.shop_code 
                                left join 
                                (select * 
                                from csx_dim.csx_dim_basic_goods 
                                where sdt='current'
                                ) g4 
                                on g2.product_code=g4.goods_code 
                                left join 
                                -- 每个仓每个商品的品类目标毛利
                                (select 
                                        t1.dc_code,
                                        t1.product_code,
                                        max(cast(t1.net_profit as decimal(16,6))*0.01) as net_profit 
                                 from 
                                        (select 
                                                a1.location_code as dc_code,
                                                a2.product_code,
                                                a1.net_profit
                                        from 
                                                (select 
                                                    level,
                                                    location_code,
                                                    category_code, 
                                                    net_profit 
                                                from csx_ods.csx_ods_csx_b2b_scm_scm_product_category_strategy_df 
                                                where sdt='20230518' 
                                                and level=1) a1 
                                                left join 
                                                (select * 
                                                from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
                                                where sdt='20230518') a2 
                                                on a1.category_code=a2.one_product_category_code and a1.location_code=a2.location_code          

                                        union all 
                                        -- 中类商品对标商品
                                        select 
                                                a1.location_code as dc_code,
                                                a2.product_code,
                                                a1.net_profit 
                                        from 
                                                (select 
                                                    level,
                                                    location_code,
                                                    category_code, 
                                                    net_profit 
                                                from csx_ods.csx_ods_csx_b2b_scm_scm_product_category_strategy_df 
                                                where sdt='20230518' 
                                                and level=2) a1 
                                                left join 
                                                (select * 
                                                from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
                                                where sdt='20230518') a2 
                                                on a1.category_code=a2.two_product_category_code and a1.location_code=a2.location_code   

                                        union all 
                                        -- 小类商品对标商品
                                        select 
                                                a1.location_code as dc_code,
                                                a2.product_code,
                                                a1.net_profit 
                                        from 
                                                (select 
                                                    level,
                                                    location_code,
                                                    category_code, 
                                                    net_profit 
                                                from csx_ods.csx_ods_csx_b2b_scm_scm_product_category_strategy_df 
                                                where sdt='20230518' 
                                                and level=3) a1 
                                                left join 
                                                (select * 
                                                from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
                                                where sdt='20230518') a2 
                                                on a1.category_code=a2.three_product_category_code and a1.location_code=a2.location_code
                                                ) t1 
                                group by 
                                        t1.dc_code,
                                        t1.product_code
                                ) g6 
                                on  g6.dc_code=g2.location_code and g6.product_code=g2.product_code 
                        ) gg1 
                        cross join 
                        (select distinct calday as sdt 
                         from csx_dim.csx_dim_basic_date 
                         where calday>='20230501'  
                         and calday<='20230518' 
                        ) gg2 
                        on gg1.price_begin_date<=gg2.sdt and gg1.price_end_date>=gg2.sdt 
        group by 
                   gg2.sdt,
                gg1.performance_region_code,
                gg1.performance_province_code,
                gg1.performance_city_code,
                basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
                gg1.goods_code
                ;

-- 基地入库占比与采购报价、对标报价对比
-- create table  csx_analyse_tmp.csx_analyse_tmp_entry_goods_city as  
-- create table  csx_analyse_tmp.csx_analyse_tmp_entry_goods_city as  
select   belong_region_code  ,
  belong_region_name  ,
  a.basic_performance_province_code ,
  a.basic_performance_province_name ,
  a.basic_performance_city_name ,
  a.goods_code , 
  bar_code   ,
  a.goods_name , 
  unit_name , 
  brand_name , 
  a.classify_large_code , 
  a.classify_large_name , 
  a.classify_middle_code , 
  a.classify_middle_name ,
 -- supplier_code, 
 -- supplier_name, 
  sum(receive_qty ) receive_qty , 
  sum(receive_amt) receive_amt ,
  sum(receive_amt)/sum(receive_qty) avg_cost,
  sum(case when a.order_business_type_name='是' then receive_qty end ) as jd_qty,
  sum(case when a.order_business_type_name='是' then receive_amt end ) jd_amt,
   avg(purchase_price) as purchase_price,       -- 采购报价
  avg( pro_db_price) pro_db_price               -- 对标报价
--   sum(case when a.is_central_tag='1'  then receive_qty end ) as jc_qty,
--   sum(case when a.is_central_tag='1'  then receive_amt end ) jc_amt,
--   sum(case when a. source_type_name  in('临时地采','临时加单','直送','紧急采购' ) then receive_qty end ) as lc_qty,
--   sum(case when a. source_type_name  in('临时地采','临时加单','直送','紧急采购' ) then receive_amt end ) lc_amt,
--   sum(case when a. source_type_name not in('临时地采','临时加单','直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name !='是' then receive_qty end ) as qt_qty,
--   sum(case when a. source_type_name not in('临时地采','临时加单','直送','紧急采购' )and a.is_central_tag !='1' and a.order_business_type_name!='是' then receive_amt end ) qt_amt,
--   if(b.goods_code is null ,'否','是') as is_jd,
--   if(c.goods_code is null ,'否','是') as is_jc,
--   if(d.goods_code is null ,'否','是') as is_lc
--   rank_aa
from   csx_analyse_tmp.csx_analyse_tmp_entry_goods  a 
left join 
(select 
    basic_performance_province_code,
    basic_performance_province_name,
    basic_performance_city_code,
    basic_performance_city_name,
    goods_code,
    sdt,
    avg(purchase_price) as purchase_price,
	1 as data_type
  from 
csx_analyse_tmp.csx_analyse_tmp_purchase_price 
group by  basic_performance_province_code,
    basic_performance_province_name,
    basic_performance_city_code,
    basic_performance_city_name,
    goods_code,
    sdt
) b  on a.basic_performance_province_code=b.basic_performance_province_code and a.basic_performance_city_name=b.basic_performance_city_name and a.goods_code=b.goods_code
and receive_sdt=b.sdt
left join 
(   select 
    basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
                goods_code,
                sdt,
                avg(pro_db_price) as pro_db_price
    from csx_analyse_tmp.csx_analyse_tmp_goods_pro_price
   -- where goods_code='620'
 group by basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
                goods_code,
                sdt
                )
 
 c
 on a.basic_performance_province_code=c.basic_performance_province_code
 and a.basic_performance_city_name=c.basic_performance_city_name and a.goods_code=c.goods_code and receive_sdt=c.sdt
-- left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where order_business_type_name='是' ) b on a.goods_code=b.goods_code  -- 基地标识
-- left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where is_central_tag='1' ) c on a.goods_code=c.goods_code  -- 集采标识 
-- left  join (select distinct goods_code  from   csx_analyse_tmp.csx_analyse_tmp_entry_goods where source_type_name  in('临时地采','临时加单','直送','紧急采购' )) d on a.goods_code=d.goods_code  -- 临采标识 
-- join csx_analyse_tmp.csx_analyse_tmp_goods_top_20 b on a.goods_code=b.goods_code
where source_type_name not in ('城市服务商','联营直送','项目合伙人')
    and is_supplier_dc='是'
    and receive_sdt>='20230501'
    and classify_middle_name='蔬菜'
group by 
-- rank_aa,
  belong_region_code  ,
  belong_region_name  ,
  a.basic_performance_province_code ,
  a.basic_performance_province_name ,
  a.basic_performance_city_name,
  a.goods_code , 
  bar_code   ,
  goods_name , 
  unit_name , 
  brand_name , 
  a.classify_large_code , 
  a.classify_large_name , 
  a.classify_middle_code , 
  a.classify_middle_name
 -- supplier_code, 
 -- supplier_name
--   if(b.goods_code is null ,'否','是'),
-- if(c.goods_code is null ,'否','是'),
-- if(d.goods_code is null ,'否','是');



-- 基地商品销售

select  
    sdt,
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
 
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
 -- coalesce(short_name,'')short_name,
  if(purchase_order_type=1,1,'') as central_pursh_tag,
  if(purchase_order_type=2,1,'') as base_pursh_tag,
  coalesce(sum(case when purchase_order_type=1 then product_cost_amt        end ),0)  central_pursh_cost_amt,
  coalesce(sum(case when purchase_order_type=1 then product_cost_amt_no_tax end ),0)  central_pursh_cost_amt_no_tax,
  coalesce(sum(case when purchase_order_type=1 then product_profit          end ),0)  central_pursh_profit,
  coalesce(sum(case when purchase_order_type=1 then product_profit_no_tax   end ),0)  central_pursh_profit_no_tax,
  coalesce(sum(case when purchase_order_type=1 then product_sale_amt        end ),0)  central_pursh_sale_amt,
  coalesce(sum(case when purchase_order_type=1 then product_sale_amt_no_tax end ),0)  central_pursh_sale_amt_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_cost_amt        end ),0)  base_pursh_cost_amt,
  coalesce(sum(case when purchase_order_type=2 then product_cost_amt_no_tax end ),0)  base_pursh_cost_amt_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_profit          end ),0)  base_pursh_profit,
  coalesce(sum(case when purchase_order_type=2 then product_profit_no_tax   end ),0)  base_pursh_profit_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_sale_amt        end ),0)  base_pursh_sale_amt,
  coalesce(sum(case when purchase_order_type=2 then product_sale_amt_no_tax end ),0)  base_pursh_sale_amt_no_tax
from  (
select
  sale_sdt as sdt,
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  order_code,
  batch_no,
  meta_batch_no,
  product_code,
  c.business_division_code,
    c.business_division_name,
    c.division_code,
    c.division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
  purchase_order_type,              -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
  goods_shipped_type,               -- 商品出库类型1 A进A出 2工厂加工 3其他
  (product_cost_amt)product_cost_amt,
  (product_cost_amt_no_tax) product_cost_amt_no_tax,
  (product_profit) product_profit,
  (product_profit_no_tax) product_profit_no_tax,
  (product_sale_amt ) product_sale_amt,
  (product_sale_amt_no_tax) product_sale_amt_no_tax
from
   csx_analyse.csx_analyse_fr_fina_goods_sale_trace_po_di a 
   left join 
   (SELECT goods_code,
          tax_rate/100 product_tax_rate,
          business_division_code,
          business_division_name,
          division_code,
          division_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          a.classify_small_code,
          classify_small_name,
          short_name,
          start_date,
          end_date
   FROM  csx_dim.csx_dim_basic_goods a 
   left join 
   (
    select
      short_name,
      classify_small_code,
      start_date,
      end_date
    from
      csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
  ) b on a.classify_small_code=b.classify_small_code 
   WHERE sdt='current') c on a.product_code = c.goods_code
   where sale_sdt >= '20230501'
    and sale_sdt <= '20230518' 
    and purchase_order_type=2
    group by 
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  order_code,
  batch_no,
  meta_batch_no,
  product_code  ,
  purchase_order_type,
  goods_shipped_type,
  sale_sdt,
  product_cost_amt,
  product_cost_amt_no_tax,
  product_profit,
  product_profit_no_tax,
  product_sale_amt,
  product_sale_amt_no_tax,
  c.business_division_code,
    c.business_division_name,
    c.division_code,
    c.division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name
  ) a  
group by  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  business_division_code,
  business_division_name,
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
 -- short_name,
  if(purchase_order_type=1,1,''),
  if(purchase_order_type=2,1,''),
  sdt
;

-- 基地与非基地的毛利率
with aa as  (
select
  order_code,
  goods_code,
  product_sale_amt,
  product_profit,
  batch_no,
  meta_batch_no
from
   csx_analyse.csx_analyse_fr_fina_goods_sale_trace_po_di a 
    where sale_sdt >= '20230501'
    and sale_sdt <= '20230518' 
        and month='202305'
    and purchase_order_type=2
    group by 
    order_code,
    goods_code,
    product_sale_amt,
    product_profit,
    batch_no,
  meta_batch_no
    ) ,
    bb as (select  basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
                order_code, 
                goods_code,
                sum(sale_amt) sale_amt,
                sum(profit) profit
        from csx_dws.csx_dws_sale_detail_di a 
        left join (select shop_code,shop_low_profit_flag, basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name from csx_dim.csx_dim_shop where sdt='current') b on a.inventory_dc_code=b.shop_code
        where sdt >='20230501' and classify_middle_name='蔬菜' 
        and business_type_code ='1'   -- 日配业务 
        and b.shop_low_profit_flag =0   -- 剔除DC直送仓
        and refund_order_flag =0            -- 正向单
	    and order_channel_code not in ('4','5','6')  -- 不含返利
	    group by  basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
                order_code,
                goods_code
	  )
	  select  basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name,
                sum(sale_amt) sale_amt,
                sum(profit) profit,
                sum(case when aa.order_code is not  null then sale_amt end ) as jd_amt,
                sum(case when aa.order_code is not  null then profit end ) as jd_profit,
                SUM(product_sale_amt) product_sale_amt,
  SUM(product_profit) product_profit
        from bb 
	  left join aa on bb.order_code=aa.order_code and aa.goods_code=bb.goods_code
	 group by basic_performance_province_code,
                basic_performance_province_name,
                basic_performance_city_code,
                basic_performance_city_name
	  
  
;



-- 基地商品周销售环比 A进A出

select  
    csx_week,csx_week_begin,csx_week_end,
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
 
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
 -- coalesce(short_name,'')short_name,
  if(purchase_order_type=1,1,'') as central_pursh_tag,
  if(purchase_order_type=2,1,'') as base_pursh_tag,
  coalesce(sum(case when purchase_order_type=1 then product_cost_amt        end ),0)  central_pursh_cost_amt,
  coalesce(sum(case when purchase_order_type=1 then product_cost_amt_no_tax end ),0)  central_pursh_cost_amt_no_tax,
  coalesce(sum(case when purchase_order_type=1 then product_profit          end ),0)  central_pursh_profit,
  coalesce(sum(case when purchase_order_type=1 then product_profit_no_tax   end ),0)  central_pursh_profit_no_tax,
  coalesce(sum(case when purchase_order_type=1 then product_sale_amt        end ),0)  central_pursh_sale_amt,
  coalesce(sum(case when purchase_order_type=1 then product_sale_amt_no_tax end ),0)  central_pursh_sale_amt_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_cost_amt        end ),0)  base_pursh_cost_amt,
  coalesce(sum(case when purchase_order_type=2 then product_cost_amt_no_tax end ),0)  base_pursh_cost_amt_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_profit          end ),0)  base_pursh_profit,
  coalesce(sum(case when purchase_order_type=2 then product_profit_no_tax   end ),0)  base_pursh_profit_no_tax,
  coalesce(sum(case when purchase_order_type=2 then product_sale_amt        end ),0)  base_pursh_sale_amt,
  coalesce(sum(case when purchase_order_type=2 then product_sale_amt_no_tax end ),0)  base_pursh_sale_amt_no_tax,
    if(goods_shipped_type=1,'A进A出','其他采购') goods_shipped_type

from  (
select
  sale_sdt as sdt,
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  order_code,
  batch_no,
  meta_batch_no,
  product_code,
  c.business_division_code,
    c.business_division_name,
    c.division_code,
    c.division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name,
  purchase_order_type,              -- 采购订单类型1 集采采购 2 基地采购 3 其他采购
  goods_shipped_type,               -- 商品出库类型1 A进A出 2工厂加工 3其他
  (product_cost_amt)product_cost_amt,
  (product_cost_amt_no_tax) product_cost_amt_no_tax,
  (product_profit) product_profit,
  (product_profit_no_tax) product_profit_no_tax,
  (product_sale_amt ) product_sale_amt,
  (product_sale_amt_no_tax) product_sale_amt_no_tax
from
     csx_analyse.csx_analyse_fr_fina_goods_sale_trace_po_di a 
   left join 
   (SELECT goods_code,
          tax_rate/100 product_tax_rate,
          business_division_code,
          business_division_name,
          division_code,
          division_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          a.classify_small_code,
          classify_small_name,
          short_name,
          start_date,
          end_date
   FROM  csx_dim.csx_dim_basic_goods a 
   left join 
   (
    select
      short_name,
      classify_small_code,
      start_date,
      end_date
    from
      csx_ods.csx_ods_data_analysis_prd_source_scm_w_a_group_purchase_classily_df
  ) b on a.classify_small_code=b.classify_small_code 
   WHERE sdt='current') c on a.product_code = c.goods_code
   where sale_sdt >= '20230501'
    and sale_sdt <= '20230525' 
    and month>='202304'
    and purchase_order_type=2
    group by 
  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  order_code,
  batch_no,
  meta_batch_no,
  product_code  ,
  purchase_order_type,
  goods_shipped_type,
  sale_sdt,
  product_cost_amt,
  product_cost_amt_no_tax,
  product_profit,
  product_profit_no_tax,
  product_sale_amt,
  product_sale_amt_no_tax,
  c.business_division_code,
    c.business_division_name,
    c.division_code,
    c.division_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name

  ) a  
  join 
  (select calday,csx_week,csx_week_begin,csx_week_end from csx_dim.csx_dim_basic_date) b on a.sdt=b.calday
group by  receive_dc_code,
  receive_dc_name,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  business_division_code,
  business_division_name,
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  classify_small_code,
  classify_small_name,
 -- short_name,
  if(purchase_order_type=1,1,''),
  if(purchase_order_type=2,1,''),
  csx_week,csx_week_begin,csx_week_end,
   if(goods_shipped_type=1,'A进A出','其他采购') 
;