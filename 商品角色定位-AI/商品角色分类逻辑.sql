--商品分类逻辑（参考苏州定价模型文档 张晴的abc分类）
--【商品角色的定义】
--A：民生商品，sku数量占比5%，此类属价格导向性商品要确保公司价格形象及市场竞争力，避免价格波动过大，原则上要求价格不高于竞对
--B：重点商品，sku数量占比65%，此类商品sku占比、销售额贡献率均较大，应尽量稳定此类商品毛利
--C：结构商品，sku数量占比30%，此类商品多用以完善品类结构和满足个性化需求，属低频补充性商品，应赚取相应毛利；
--注：各品类及各省区间或会因实际业务情况产生差异，上述sku数量占比数值可根据实际情况进行调整；
--【区分规则】
--根据商品销售额（权重30%）、销量（权重30%）、动销天数（权重20%）、渗透率（权重20%）进行商品分级；
--①仅针对基础商品池内商品进行商品分级；
--②以品类维度（二级品类），针对品类下商品进行数据分析；
--③根据数据按照月度销售额、销量、动销天数、渗透率排名进行综合排名（剔除贸易大宗业绩、季节性明显上商品）；
--④按照排名及sku占比，确认最终商品角色

--确认项结果
--1、商品覆盖（目标商品）：本月基础商品池商品+当前采购报价生效报价商品
--2、目标商品销售额统计范围：当月往前推3个月内，销售额统计B端（不含BBC）剔除贸易大宗业绩
--3、目标商品前3个月销售额为空的或小于等于0，都归到C类
--4、目标商品销售、销售量、动销天数、渗透率排名均为DC+管理二级分类下的排名


-- 昨日、昨日、昨日月1日、昨日月1日、上月最后一天、上月最后一天、3月前1日、3月前1日
--select ${hiveconf:current_day},${hiveconf:current_start_mon},${hiveconf:before1_last_mon},${hiveconf:before3_start_mon};
set current_day1 =date_sub(current_date,1);
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set current_start_mon1 =add_months(trunc(date_sub(current_date,1),'MM'),0);
set current_start_mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
set before1_last_mon1 =last_day(add_months(date_sub(current_date,1),-1));
set before1_last_mon =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');
set before3_start_mon1 =add_months(trunc(date_sub(current_date,1),'MM'),-3;
set before3_start_mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-3),'-','');


--临时表：目标商品销售排名
drop table csx_tmp.tmp_dc_goods_sale_rank;
create temporary table csx_tmp.tmp_dc_goods_sale_rank
as
  select a.*,b.sales_value,b.sales_qty,b.count_day,coalesce(b.count_cust,0)/c.count_cust_all as cust_penetration_rate,
    --DC二级分类商品销售额排名
    dense_rank() over(partition by a.location_code,a.classify_middle_code order by b.sales_value desc) as DC_classify_m_sales_value_rno,
    --DC二级分类商品销售量排名
    dense_rank() over(partition by a.location_code,a.classify_middle_code order by b.sales_qty desc) as DC_classify_m_sales_qty_rno,
    --DC二级分类商品动销天数排名
    dense_rank() over(partition by a.location_code,a.classify_middle_code order by b.count_day desc) as DC_classify_m_count_day_rno,
    --DC二级分类商品渗透率排名排名
    dense_rank() over(partition by a.location_code,a.classify_middle_code order by coalesce(b.count_cust,0)/c.count_cust_all desc) as DC_classify_m_penetration_rno
  from
  (
    --目标商品及商品基础信息：基础商品池商品+采购报价生效报价商品 
    select 
      b.sales_region_code,b.sales_region_name,b.sales_province_code,b.sales_province_name,b.city_group_code,b.city_group_name,
      a.location_code,b.shop_name as dc_name,a.product_code as goods_code,c.goods_name,c.unit,c.unit_name,
      c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,c.classify_small_name
    from 
    (
      select distinct location_code,product_code
      from
      (
        --基础商品池商品
        select distinct location_code,product_code
        from csx_ods.source_scm_w_a_scm_product_pool
        where sdt>=${hiveconf:current_start_mon}
        and sdt<=${hiveconf:current_day}
        and status in('0','2')  --商品状态:0 B 正常商品;2 A 新品;3 H 停售;6 L 退场;7 K 永久停购;9 E 暂时停购
        union all
        --采购报价生效报价商品
        select distinct warehouse_code as location_code,product_code
        from csx_ods.source_price_r_d_effective_purchase_prices
        where sdt=${hiveconf:current_day}
        and effective = 1 
        and ( (price_begin_time >= ${hiveconf:current_start_mon1} and price_begin_time<${hiveconf:current_day1})
          or (price_end_time >= ${hiveconf:current_start_mon1} and price_end_time<${hiveconf:current_day1})
          or(price_begin_time < ${hiveconf:current_start_mon1} and price_end_time>=${hiveconf:current_day1}) )
      ) a
    ) a
    left join
    (
      select shop_id,shop_name,sales_region_code,sales_region_name,
        sales_province_code,sales_province_name,city_group_code,city_group_name
      from csx_dw.dws_basic_w_a_csx_shop_m
      where sdt = 'current'
    ) b on a.location_code = b.shop_id
    left join
    (
      select goods_id,goods_name,unit,unit_name,classify_large_code,classify_large_name,
        classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
      from csx_dw.dws_basic_w_a_csx_product_m
      where sdt = 'current'
    ) c on a.product_code = c.goods_id
  ) a
  --前3个月销售情况 业务类型：剔除贸易大宗业绩
  left join 
  (
    select dc_code,goods_code,
      sum(sales_value) as sales_value,
      sum(profit) as profit,
      sum(sales_qty) as sales_qty,
      count(distinct sdt) as count_day,
  	count(distinct customer_no) as count_cust
    from csx_dw.dws_sale_r_d_detail 
    where sdt>=${hiveconf:before3_start_mon} 
    and sdt<=${hiveconf:before1_last_mon} 
    and channel_code in ('1','9') --不含BBC
	--业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
    and business_type_code<>'5'  --剔除贸易大宗业绩
    group by dc_code,goods_code
  ) b on a.goods_code=b.goods_code and a.location_code=b.dc_code
  --前3个月销售总数
  left join 
  (
    select dc_code,
  	count(distinct customer_no) as count_cust_all
    from csx_dw.dws_sale_r_d_detail 
    where sdt>=${hiveconf:before3_start_mon} 
    and sdt<=${hiveconf:before1_last_mon} 
    and channel_code in ('1','9') 
    and business_type_code<>'5'
    group by dc_code
  ) c on a.location_code=c.dc_code;

--结果表：商品角色分类标签
with DC_classify_middle_goods_score as
-- DC管理二级分类商品 综合得分
(
  select a.*,
  --综合得分：商品销售额（权重30%）、销量（权重30%）、动销天数（权重20%）、渗透率（权重20%）
    DC_classify_m_sales_value_rno*0.3+DC_classify_m_sales_qty_rno*0.3+
      DC_classify_m_count_day_rno*0.2+DC_classify_m_penetration_rno*0.2 as goods_score  --综合得分
  from csx_tmp.tmp_dc_goods_sale_rank a
),
-- DC管理二级分类商品 综合排名
DC_classify_middle_goods_score_rank as
(
  select a.*,
    dense_rank() over(partition by location_code,classify_middle_code order by goods_score asc) as DC_classify_m_goods_score_rno	
  from DC_classify_middle_goods_score a
), 
-- DC管理二级分类商品 sku数
DC_classify_middle_goods_sku as
(
  select location_code,classify_middle_code,
  count(distinct goods_code) DC_classify_m_sku
  from DC_classify_middle_goods_score 
  group by location_code,classify_middle_code
), 
-- DC管理二级分类商品 sku数量累计占比
DC_classify_middle_goods_sku_rate as
(
  select a.*,DC_classify_m_goods_score_rno/DC_classify_m_sku as DC_classify_m_goods_sku_rate
  from DC_classify_middle_goods_score_rank a
  left join DC_classify_middle_goods_sku b 
      on b.location_code=a.location_code and b.classify_middle_code=a.classify_middle_code
) 
-- DC管理二级分类商品 商品角色
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select a.*,
  --商品角色  A：民生商品，sku数量占比5%; B：重点商品，sku数量占比65%; C：结构商品，sku数量占比30%
  case when sales_value is null or sales_value<=0 then 'C'
       when DC_classify_m_goods_sku_rate<=0.05 then 'A'
       when DC_classify_m_goods_sku_rate>0.05 and DC_classify_m_goods_sku_rate<=0.7 then 'B'
       when DC_classify_m_goods_sku_rate>0.7 then 'C'	
       end  as DC_classify_m_goods_role
from DC_classify_middle_goods_sku_rate a;










