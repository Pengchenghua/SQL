-- 调整
  -- 近2月新客周毛利率下滑超2%：近2月新客周毛利率下滑超2%的客户中业绩降序TOP5
  -- 近2月新客周毛利率小于省区超5%：近2月新客周毛利率小于省区超5%的客户中业绩降序TOP5
  -- 近2月新客周毛利率低于10%：近2月新客周毛利率低于10%的客户中业绩降序TOP5
  -- 老客周毛利率下滑超2%：老客周毛利率下滑超2%的客户中业绩降序TOP5
  -- 老客周毛利率小于省区超5%：老客周毛利率小于省区超5%的客户中业绩降序TOP5
  -- 老客周毛利率低于10%：老客周毛利率低于10%的客户中业绩降序TOP5
  -- 月至今负毛利额top客户：月至今负毛利额升序top5客户
  
-- 每周四出上周四到本周三的数据
-- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-4)) week


--------------- 客户表
drop table csx_analyse_tmp.tmp_cust_sale_profit_trend_list;
create table  csx_analyse_tmp.tmp_cust_sale_profit_trend_list
as
with 
sale_detail as -- 销售明细
(
  select 
	a.performance_region_code,
	a.performance_region_name,	--	业绩大区名称
	a.performance_province_code,
	a.performance_province_name,	--	业绩省区名称
	a.performance_city_name,	--	业绩城市名称	
	d.performance_region_name as performance_region_name_cust,
	d.performance_province_name as performance_province_name_cust,
	d.performance_city_name as performance_city_name_cust,
	a.inventory_dc_code,
	a.inventory_dc_name,
	if(a.order_channel_code=6 ,'是','否') as is_tiaojia,
	if(a.order_channel_code=4 ,'是','否') as is_fanli,
	if(a.delivery_type_name='直送','是','否') as is_zhisong,
	case 
		when a.delivery_type_name='配送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end direct_delivery_type,	
    if(b.first_sales_date>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),'是','否') as is_new_cust_2m,
	a.customer_code,
	d.customer_name,
	d.second_category_name,
	b.first_sales_date,
    a.classify_middle_code,
    a.classify_middle_name,
	a.goods_code,
	a.goods_name,
	a.sdt,
	a.week,
	if(a.order_channel_detail_code=26,0,sale_qty) as sale_qty,
	a.sale_cost,
	a.sale_amt,
	a.profit
  from 
  (
        select *,weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-4)) week 
       from csx_dws.csx_dws_sale_detail_di
       where sdt>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')  
	   and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	   and channel_code in('1','7','9')
	   and business_type_code=1
	   and inventory_dc_code<>'W0J2'
	)  a 
join 
    ( 
	  select
          distinct shop_code 
	  from csx_dim.csx_dim_shop 
	  where sdt='current' and shop_low_profit_flag=0 
	 )c  on a.inventory_dc_code = c.shop_code
left join  
   (
     select 
		customer_code,	--	客户编码
		customer_name,	--	客户名称
		channel_name,	--	渠道名称
		performance_region_name,	--	业绩大区名称
		performance_province_name,	--	业绩省区名称
		performance_city_name,	--	业绩城市名称
		first_category_name,	--	一级客户分类名称
		second_category_name,	--	二级客户分类名称
		third_category_name,	--	三级客户分类名称
		business_attribute_desc,	--	商机属性描述
		sign_time,	--	签约时间
		first_sign_time	--	第一次签约时间	 
     from  csx_dim.csx_dim_crm_customer_info 
     where sdt='current'
	 and customer_type_code=4
	)d on d.customer_code=a.customer_code 	
left join  -- 首单日期
   (
     select 
       customer_code,
  	   min(first_business_sale_date) first_sales_date
     from csx_dws.csx_dws_crm_customer_business_active_di
     where sdt ='current'  and  business_type_code in (1)
     group by customer_code
    )b on b.customer_code=a.customer_code	 
),

 -- 月至今客户top品类
 sale_cust_mclass_TOP
 as 
(
select *
from 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
	sum(sale_amt) sale_amt,
	sum(profit) profit,	
	--  客户品类销售排名
	row_number() over(partition by customer_code order by sum(sale_amt) desc) as mclass_rank	
from sale_detail
where sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
and sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') 
group by 
	customer_code,
	classify_middle_code,
	classify_middle_name
)a where mclass_rank=1
),

 -- 月至今省区业绩毛利
 province_sale_profit
 as 
(
select 
    a.performance_province_code,     
	a.performance_province_name,     	
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) prov_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) prov_profit
  from sale_detail a
group by
    a.performance_province_code,     
	a.performance_province_name
),
 -- 本周省区业绩毛利
 bw0_province_sale_profit
 as 
(
select 
    a.performance_province_code,     
	a.performance_province_name,     	
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_amt end) bw0_prov_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.profit end) bw0_prov_profit
  from sale_detail a
group by
    a.performance_province_code,     
	a.performance_province_name
),

 -- 客户月周趋势表现及对比省区
 cust_sale_profit_trend
 as 
(
select 
	a.performance_region_name, 
    a.performance_province_code,
	a.performance_province_name,
	a.performance_city_name,	
    customer_code,
	customer_name,
	second_category_name,
	first_sales_date,

    bw4_sale_amt,
	bw3_sale_amt,
	bw2_sale_amt,
	bw1_sale_amt,
	bw0_sale_amt,

	bw4_profit,	
	bw3_profit,
	bw2_profit,
	bw1_profit,
	bw0_profit,
	
	bw4_profit/abs(bw4_sale_amt) bw4_profitlv,
	bw3_profit/abs(bw3_sale_amt) bw3_profitlv,
	bw2_profit/abs(bw2_sale_amt) bw2_profitlv,
	bw1_profit/abs(bw1_sale_amt) bw1_profitlv,
	if(bw0_sale_amt<>0,bw0_profit/abs(bw0_sale_amt),0) bw0_profitlv,

	sale_amt,
	profit,
    profit/abs(sale_amt) profitlv,	 
	sale_amt_lastm,
	profit_lastm,
	profit_lastm/abs(sale_amt_lastm) profit_lastmlv, 
	profit/abs(sale_amt)-profit_lastm/abs(sale_amt_lastm) profit_hblv,
	b.prov_sale_amt,
	b.prov_profit,
	if(b.prov_sale_amt<>0,b.prov_profit/abs(b.prov_sale_amt),0) prov_profitlv,
	c.bw0_prov_sale_amt,
	c.bw0_prov_profit,
	if(c.bw0_prov_sale_amt<>0,c.bw0_prov_profit/abs(c.bw0_prov_sale_amt),0) bw0_prov_profitlv	

from
( select 
    performance_region_name,
	performance_province_code,     
	performance_province_name,
	performance_city_name,	
    customer_code,
	customer_name,
	second_category_name,
	first_sales_date,	
	-- 注意找上周需要根据跑数是周几相应调整相对数字
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_amt end) bw0_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.profit end) bw0_profit,	
	
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.sale_amt end) bw1_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.profit end) bw1_profit,

    sum(case when a.week=weekofyear(date_sub(current_date,16+0)) then a.sale_amt end) bw2_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,16+0)) then a.profit end) bw2_profit,	

    sum(case when a.week=weekofyear(date_sub(current_date,23+0)) then a.sale_amt end) bw3_sale_amt,
  	sum(case when a.week=weekofyear(date_sub(current_date,23+0)) then a.profit end) bw3_profit,	

    sum(case when a.week=weekofyear(date_sub(current_date,30+0)) then a.sale_amt end) bw4_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,30+0)) then a.profit end) bw4_profit,		
	
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sale_amt_lastm,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) profit_lastm
  from sale_detail a	
group by 
	performance_region_name,
	performance_province_code,     
	performance_province_name,
	performance_city_name,	
    customer_code,
	customer_name,
	second_category_name,
	first_sales_date
  )a
left join province_sale_profit b on a.performance_province_code=b.performance_province_code
left join bw0_province_sale_profit c on a.performance_province_code=c.performance_province_code
),

 -- 重点关注客户清单-未去重
 cust_sale_profit_trend_list0
 as 
(

-- 重点关注客户清单
--近2月新客TOP5（毛利率周下滑2%或毛利率低于省区5%、业绩降序）
--老客毛利率下滑TOP5（毛利率周下滑2%、业绩降序）
--老客毛利率低TOP5（周毛利率低于10%、业绩降序）
--负毛利额TOP5客户（负毛利额升序top5）

select *
from
(
select *,
	'近2月新客周毛利率下滑超2%' as cust_flag,
	row_number() over(partition by performance_province_code order by nvl(sale_amt,0) desc) rown
from cust_sale_profit_trend
where first_sales_date>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
and bw1_profitlv-bw0_profitlv>0.02
and nvl(bw0_profitlv,0)<>0
)a where rown<=5

union all
select *
from
( 
select *,
	'近2月新客周毛利率小于省区超5%' as cust_flag,
	row_number() over(partition by performance_province_code order by nvl(sale_amt,0) desc) rown
from cust_sale_profit_trend
where first_sales_date>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
and bw0_prov_profitlv-bw0_profitlv>0.05
and nvl(bw0_profitlv,0)<>0
)a where rown<=5

union all 
select *
from
(
select *,
	'近2月新客周毛利率低于10%' as cust_flag,
	row_number() over(partition by performance_province_code order by nvl(sale_amt,0) desc) rown
from cust_sale_profit_trend
where first_sales_date>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
and bw0_profitlv<0.1
and bw0_sale_amt>0
)a where rown<=5

union all
select *
from
( 
select *,
	'老客周毛利率下滑超2%' as cust_flag,
	row_number() over(partition by performance_province_code order by nvl(sale_amt,0) desc) rown
from cust_sale_profit_trend
where first_sales_date<regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
and bw1_profitlv-bw0_profitlv>0.02
and nvl(bw0_profitlv,0)<>0
)a where rown<=5

union all
select *
from
( 
select *,
	'老客周毛利率小于省区超5%' as cust_flag,
	row_number() over(partition by performance_province_code order by nvl(sale_amt,0) desc) rown
from cust_sale_profit_trend
where first_sales_date<regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
and bw0_prov_profitlv-bw0_profitlv>0.05
and nvl(bw0_profitlv,0)<>0
)a where rown<=5

union all 
select *
from
(
select *,
	'老客周毛利率低于10%' as cust_flag,
	row_number() over(partition by performance_province_code order by nvl(sale_amt,0) desc) rown
from cust_sale_profit_trend
where first_sales_date<regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
and bw0_profitlv<0.1
and bw0_sale_amt>0
)a where rown<=5

union all 
select *
from
(
select *,
	'月至今负毛利额top客户' as cust_flag,
	row_number() over(partition by performance_province_code order by nvl(profit,0) asc) rown
from cust_sale_profit_trend
where first_sales_date<regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')
and profitlv<0
and sale_amt>0
)a where rown<=5
)	
-- 重点客户清单	
select a.cust_flag as cust_flag_group,
	c.sale_amt as mclass_sale_amt,
	c.profit as mclass_profit,
concat(c.classify_middle_name, ':', '销售额', round(c.sale_amt,0),',毛利率',round(c.profit/abs(c.sale_amt)*100, 0),'%',',销售额占比',round(c.sale_amt/b.sale_amt*100, 0),'%') mclass_des_top1,	
	b.*
from 
(
select customer_code,
	concat_ws('；',collect_list(cust_flag)) as cust_flag
from cust_sale_profit_trend_list0
group by customer_code
)a 
left join 
(
select *,
	row_number() over(partition by customer_code order by cust_flag asc) rn
from cust_sale_profit_trend_list0
)b on a.customer_code=b.customer_code
left join sale_cust_mclass_TOP c on b.customer_code=c.customer_code
where b.rn=1
;


-- top客户销售明细
drop table csx_analyse_tmp.tmp_top_cust_sale_detail;
create table csx_analyse_tmp.tmp_top_cust_sale_detail
as
  select 
	split(a.id, '&')[0] as credential_no,
	a.order_code,
	a.original_order_code,
	a.performance_region_code,
	a.performance_region_name,	--	业绩大区名称
	a.performance_province_code,
	a.performance_province_name,	--	业绩省区名称
	a.performance_city_name,	--	业绩城市名称	
	d.performance_region_name as performance_region_name_cust,
	d.performance_province_name as performance_province_name_cust,
	d.performance_city_name as performance_city_name_cust,
	a.inventory_dc_code,
	a.inventory_dc_name,
	a.business_type_name,
	if(a.order_channel_code=6 ,'是','否') as is_tiaojia,
	if(a.order_channel_code=4 ,'是','否') as is_fanli,
	if(a.delivery_type_name='直送','是','否') as is_zhisong,
	delivery_type_name,
	case 
		when a.delivery_type_name='配送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end direct_delivery_type,		
	if( c.shop_code is null,'否','是') types,
    if(b.first_sales_date>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),'是','否') as is_new_cust_2m,
	a.customer_code,
	d.customer_name,
	d.second_category_name,
	b.first_sales_date,
	a.business_division_name,
	a.purchase_group_code,a.purchase_group_name,
	a.classify_large_code,a.classify_large_name,
	a.classify_middle_code,a.classify_middle_name,
	a.classify_small_code,a.classify_small_name,
	a.goods_code,a.goods_name,
	a.sdt,
	a.week,
	a.cost_price,
	a.sale_price,
	if(a.order_channel_detail_code=26,0,sale_qty) as sale_qty,
	a.sale_cost,
	a.sale_amt,
	a.profit
  from 
  (
        select *,weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-4)) week 
       from csx_dws.csx_dws_sale_detail_di
       where sdt>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')  
	   and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	   and channel_code in('1','7','9')
	   and business_type_code=1
	   and inventory_dc_code<>'W0J2'
	)  a 
join 
    ( 
	  select
          distinct shop_code 
	  from csx_dim.csx_dim_shop 
	  where sdt='current' and shop_low_profit_flag=0 
	 )c  on a.inventory_dc_code = c.shop_code
left join  
   (
     select 
		customer_code,	--	客户编码
		customer_name,	--	客户名称
		channel_name,	--	渠道名称
		performance_region_name,	--	业绩大区名称
		performance_province_name,	--	业绩省区名称
		performance_city_name,	--	业绩城市名称
		first_category_name,	--	一级客户分类名称
		second_category_name,	--	二级客户分类名称
		third_category_name,	--	三级客户分类名称
		business_attribute_desc,	--	商机属性描述
		sign_time,	--	签约时间
		first_sign_time	--	第一次签约时间	 
     from  csx_dim.csx_dim_crm_customer_info 
     where sdt='current'
	 and customer_type_code=4
	)d on d.customer_code=a.customer_code 	
left join  -- 首单日期
   (
     select 
       customer_code,
  	   min(first_business_sale_date) first_sales_date
     from csx_dws.csx_dws_crm_customer_business_active_di
     where sdt ='current'  and  business_type_code in (1)
     group by customer_code
    )b on b.customer_code=a.customer_code
join csx_analyse_tmp.tmp_cust_sale_profit_trend_list e on a.customer_code=e.customer_code;



 -- top客户异常毛利影响 本周上周月至今
drop table csx_analyse_tmp.tmp_top_cust_profit_eff;
create table csx_analyse_tmp.tmp_top_cust_profit_eff
as
with 
-- sale_detail as -- 销售明细
-- (
--   select * from csx_analyse_tmp.tmp_top_cust_sale_detail	
-- ),
-- 
 -- top客户销售 本周上周月至今
 sale_cust_sale
 as 
(
select *
from 
(
select 
	customer_code,

	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_qty end) bw0_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_cost end) bw0_sale_cost,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_amt end) bw0_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.profit end) bw0_profit,	

	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.sale_qty end) bw1_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.sale_cost end) bw1_sale_cost,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.sale_amt end) bw1_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.profit end) bw1_profit,	

	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_qty end) sale_qty,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_cost end) sale_cost,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) profit,	

	-- 直送
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_zhisong='是' then a.sale_qty end) bw0_sale_qty_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_zhisong='是' then a.sale_cost end) bw0_sale_cost_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_zhisong='是' then a.sale_amt end) bw0_sale_amt_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_zhisong='是' then a.profit end) bw0_profit_zs,	

	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_zhisong='是' then a.sale_qty end) bw1_sale_qty_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_zhisong='是' then a.sale_cost end) bw1_sale_cost_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_zhisong='是' then a.sale_amt end) bw1_sale_amt_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_zhisong='是' then a.profit end) bw1_profit_zs,	

	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_zhisong='是' then a.sale_qty end) sale_qty_zs,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_zhisong='是' then a.sale_cost end) sale_cost_zs,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_zhisong='是' then a.sale_amt end) sale_amt_zs,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_zhisong='是' then a.profit end) profit_zs,	
	
	-- 调价
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_tiaojia='是' then a.sale_qty end) bw0_sale_qty_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_tiaojia='是' then a.sale_cost end) bw0_sale_cost_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_tiaojia='是' then a.sale_amt end) bw0_sale_amt_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_tiaojia='是' then a.profit end) bw0_profit_tj,	

	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_tiaojia='是' then a.sale_qty end) bw1_sale_qty_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_tiaojia='是' then a.sale_cost end) bw1_sale_cost_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_tiaojia='是' then a.sale_amt end) bw1_sale_amt_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_tiaojia='是' then a.profit end) bw1_profit_tj,	

	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_tiaojia='是' then a.sale_qty end) sale_qty_tj,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_tiaojia='是' then a.sale_cost end) sale_cost_tj,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_tiaojia='是' then a.sale_amt end) sale_amt_tj,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_tiaojia='是' then a.profit end) profit_tj,	

	-- 返利
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_fanli='是' then a.sale_qty end) bw0_sale_qty_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_fanli='是' then a.sale_cost end) bw0_sale_cost_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_fanli='是' then a.sale_amt end) bw0_sale_amt_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_fanli='是' then a.profit end) bw0_profit_fl,	

	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_fanli='是' then a.sale_qty end) bw1_sale_qty_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_fanli='是' then a.sale_cost end) bw1_sale_cost_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_fanli='是' then a.sale_amt end) bw1_sale_amt_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_fanli='是' then a.profit end) bw1_profit_fl,	

	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_fanli='是' then a.sale_qty end) sale_qty_fl,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_fanli='是' then a.sale_cost end) sale_cost_fl,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_fanli='是' then a.sale_amt end) sale_amt_fl,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_fanli='是' then a.profit end) profit_fl	
from csx_analyse_tmp.tmp_top_cust_sale_detail a
group by 
	customer_code
)a 
)

 -- top客户毛利影响 本周上周月至今
select
	customer_code,
	-- 月至今
	sale_amt,
	profit,
	sale_amt_zs,
	sale_amt_tj,
	sale_amt_fl,
	round(profit_zs/abs(sale_amt_zs),6) as profit_rate_zs,   -- 直送毛利率
	round(sale_amt_zs/sale_amt,6) as sale_rate_zs,  -- 直送金额占比
	round(profit/abs(sale_amt)-(profit-profit_zs)/abs(sale_amt-sale_amt_zs),6) as profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	round(profit/abs(sale_amt)-(profit-profit_tj)/abs(sale_amt-sale_amt_tj),6) as profit_rate_eff_tj,  -- 调价对品类毛利率影响
	round(profit/abs(sale_amt)-(profit-profit_fl)/abs(sale_amt-sale_amt_fl),6) as profit_rate_eff_fl,  -- 返利对品类毛利率影响

	-- 本周
	bw0_sale_amt,
	bw0_profit,
	bw0_sale_amt_zs,
	bw0_sale_amt_tj,
	bw0_sale_amt_fl,
	round(bw0_profit_zs/abs(bw0_sale_amt_zs),6) as bw0_profit_rate_zs,   -- 直送毛利率	
	round(bw0_sale_amt_zs/bw0_sale_amt,6) as bw0_sale_rate_zs,  -- 直送金额占比
	round(bw0_profit/abs(bw0_sale_amt)-(bw0_profit-bw0_profit_zs)/abs(bw0_sale_amt-bw0_sale_amt_zs),6) as bw0_profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	round(bw0_profit/abs(bw0_sale_amt)-(bw0_profit-bw0_profit_tj)/abs(bw0_sale_amt-bw0_sale_amt_tj),6) as bw0_profit_rate_eff_tj,  -- 调价对品类毛利率影响
	round(bw0_profit/abs(bw0_sale_amt)-(bw0_profit-bw0_profit_fl)/abs(bw0_sale_amt-bw0_sale_amt_fl),6) as bw0_profit_rate_eff_fl,  -- 返利对品类毛利率影响

	-- 上周
	bw1_sale_amt,
	bw1_profit,
	bw1_sale_amt_zs,
	bw1_sale_amt_tj,
	bw1_sale_amt_fl,
	round(bw1_profit_zs/abs(bw1_sale_amt_zs),6) as bw1_profit_rate_zs,   -- 直送毛利率	
	round(bw1_sale_amt_zs/bw1_sale_amt,6) as bw1_sale_rate_zs,  -- 直送金额占比
	round(bw1_profit/abs(bw1_sale_amt)-(bw1_profit-bw1_profit_zs)/abs(bw1_sale_amt-bw1_sale_amt_zs),6) as bw1_profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	round(bw1_profit/abs(bw1_sale_amt)-(bw1_profit-bw1_profit_tj)/abs(bw1_sale_amt-bw1_sale_amt_tj),6) as bw1_profit_rate_eff_tj,  -- 调价对品类毛利率影响
	round(bw1_profit/abs(bw1_sale_amt)-(bw1_profit-bw1_profit_fl)/abs(bw1_sale_amt-bw1_sale_amt_fl),6) as bw1_profit_rate_eff_fl  -- 返利对品类毛利率影响
from sale_cust_sale
;



 -- Top客户-品类周毛利影响
drop table csx_analyse_tmp.tmp_sale_cust_mclass_eff_TOP;
create table csx_analyse_tmp.tmp_sale_cust_mclass_eff_TOP
as
with 
sale_detail as -- 销售明细
(
  select * from csx_analyse_tmp.tmp_top_cust_sale_detail	
),
 sale_cust_mclass_eff
 as 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
	sale_amt,
	profit,
	profit_rate,
	sale_amt_s,
	profit_s,	
	profit_rate_s,
	cust_sale_amt,
	cust_profit,
	cust_sale_amt_s,
	cust_profit_s,
	-- 销售额占比
	sale_amt/cust_sale_amt as sale_amt_rate,
	-- 剔除法影响值
	round(cust_profit/abs(cust_sale_amt)-(cust_profit-profit)/abs(cust_sale_amt-sale_amt),6) as profit_rate_eff_mclass
	-- 还原法影响值
	-- (品类本期销售额*品类毛利率差值+(品类本期销售额-品类上期销售额)*(品类本期毛利率-客户整体本期毛利率))/客户整体上期销售额
	-- round((sale_amt*(profit_rate-profit_rate_s)+(sale_amt-sale_amt_s)*(profit_rate-cust_profit/abs(cust_sale_amt)))/abs(cust_sale_amt_s),6) as profit_rate_eff_mclass
from 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
	sale_amt,
	profit,
	profit/abs(sale_amt) as profit_rate,
	sale_amt_s,
	profit_s,
	profit_s/abs(sale_amt_s) as profit_rate_s,
	sum (sale_amt) over(partition by customer_code) as cust_sale_amt,
	sum (profit) over(partition by customer_code) as cust_profit,
	sum (sale_amt_s) over(partition by customer_code) as cust_sale_amt_s,
	sum (profit_s) over(partition by customer_code) as cust_profit_s		
from 
	(
	select 
		customer_code,
		classify_middle_code,
		classify_middle_name,
		sum(case when week=weekofyear(date_sub(current_date,2+0)) then sale_amt end) sale_amt,
		sum(case when week=weekofyear(date_sub(current_date,2+0)) then profit end) profit,
		sum(case when week=weekofyear(date_sub(current_date,9+0)) then sale_amt end) sale_amt_s,
		sum(case when week=weekofyear(date_sub(current_date,9+0)) then profit end) profit_s,		
		--  客户品类销售排名
		row_number() over(partition by customer_code order by sum(case when week=weekofyear(date_sub(current_date,2+0)) then sale_amt end) desc) as mclass_rank	
	from sale_detail
	-- where week=weekofyear(date_sub(current_date,2+0))
	group by 
		customer_code,
		classify_middle_code,
		classify_middle_name
	)a		
)a 
),
 -- Top客户-品类周毛利影响+异常影响
sale_cust_mclass_eff_1
 as 
(
select 
	a.*,
	b.bw0_sale_rate_zs,  -- 直送金额占比
	b.bw0_profit_rate_eff_zs,  -- 直送对毛利率影响	
	b.bw0_profit_rate_eff_tj,  -- 调价对毛利率影响
	b.bw0_profit_rate_eff_fl,  -- 返利对毛利率影响
	
	b.sale_rate_zs,  -- 直送金额占比
	b.profit_rate_eff_zs,  -- 直送对毛利率影响	
	b.profit_rate_eff_tj,  -- 调价对毛利率影响
	b.profit_rate_eff_fl  -- 返利对毛利率影响	
from sale_cust_mclass_eff a
-- left join csx_analyse_tmp.tmp_top_cust_profit_eff b on a.customer_code=b.customer_code 
left join csx_analyse_tmp.tmp_top_cust_mclass_profit_eff b on a.customer_code=b.customer_code and a.classify_middle_code=b.classify_middle_code
),
 -- Top客户-品类周毛利影响top
sale_cust_mclass_eff_TOP
  as 
(
select *
from 
(
select *,
	concat(classify_middle_name, ':', '销售额', round(sale_amt,0),',销售额占比',round(sale_amt_rate*100, 1),'%',',毛利率',round(profit_rate*100, 1),'%',',毛利率影响',round(profit_rate_eff_mclass*100, 2),'%',
	case when bw0_sale_rate_zs>0.3 then concat(',直送占比高',round(bw0_sale_rate_zs*100, 1),'%') else '' end,
	case when bw0_profit_rate_eff_zs<-0.01 then concat(',直送影响',round(bw0_profit_rate_eff_zs*100, 1),'%') else '' end,
	case when bw0_profit_rate_eff_tj<-0.01 then concat(',调价影响',round(bw0_profit_rate_eff_tj*100, 1),'%') else '' end,
	case when bw0_profit_rate_eff_fl<-0.01 then concat(',返利影响',round(bw0_profit_rate_eff_fl*100, 1),'%') else '' end
	) as classify_middle_ms,
	row_number() over(partition by customer_code order by profit_rate_eff_mclass asc) as mclass_eff_rank	
from sale_cust_mclass_eff_1
)a where mclass_eff_rank<=2
),
 -- Top客户-品类商品周毛利影响top
sale_cust_mclass_goods_eff_TOP
  as 
(
select a.*,i.goods_list
from sale_cust_mclass_eff_TOP a
left join 
(
select customer_code,
classify_middle_code,
classify_middle_name,
concat_ws('；',collect_list(concat(classify_middle_name,'-',goods_ms))) as goods_list
from csx_analyse_tmp.tmp_sale_cust_goods_eff_TOP
where goods_eff_rank<=2
group by customer_code,
classify_middle_code,
classify_middle_name
)i on i.customer_code=a.customer_code and i.classify_middle_code=a.classify_middle_code
)

select customer_code,
concat_ws('；',collect_list(classify_middle_ms)) as classify_middle_list,
concat_ws('；',collect_list(goods_list)) as goods_list_1
from sale_cust_mclass_goods_eff_TOP
group by customer_code
;




-- 结果表：客户
select cust_flag_group,
	a.performance_region_name, 
	a.performance_province_name, 
	a.performance_city_name,    
    a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,

    a.bw4_sale_amt/10000 bw4_sale_amt,
	a.bw3_sale_amt/10000 bw3_sale_amt,
	a.bw2_sale_amt/10000 bw2_sale_amt,
	a.bw1_sale_amt/10000 bw1_sale_amt,
	a.bw0_sale_amt/10000 bw0_sale_amt,

	a.bw4_profit/10000 bw4_profit,	
	a.bw3_profit/10000 bw3_profit,
	a.bw2_profit/10000 bw2_profit,
	a.bw1_profit/10000 bw1_profit,
	a.bw0_profit/10000 bw0_profit,

	a.bw4_profitlv,
	a.bw3_profitlv,
	a.bw2_profitlv,
	a.bw1_profitlv,
	a.bw0_profitlv,

	'' as aa_zw,
	a.bw0_prov_sale_amt/10000 bw0_prov_sale_amt,
	-- a.bw0_prov_profit,
	a.bw0_prov_profitlv,
	a.bw0_profitlv-a.bw0_prov_profitlv as bw0_profitlv_diff,
		-- 本周
	-- bw0_sale_amt,
	-- bw0_profit,
	b.bw0_sale_amt_zs,
	b.bw0_sale_amt_tj,
	b.bw0_sale_amt_fl,
	b.bw0_profit_rate_zs,   -- 直送毛利率	
	b.bw0_sale_rate_zs,  -- 直送金额占比
	b.bw0_profit_rate_eff_zs,  -- 直送对毛利率影响	
	b.bw0_profit_rate_eff_tj,  -- 调价对毛利率影响
	b.bw0_profit_rate_eff_fl,  -- 返利对毛利率影响

	-- 上周
	-- bw1_sale_amt,
	-- bw1_profit,
	b.bw1_sale_amt_zs,
	b.bw1_sale_amt_tj,
	b.bw1_sale_amt_fl,
	b.bw1_profit_rate_zs,   -- 直送毛利率	
	b.bw1_sale_rate_zs,  -- 直送金额占比
	b.bw1_profit_rate_eff_zs,  -- 直送对毛利率影响	
	b.bw1_profit_rate_eff_tj,  -- 调价对毛利率影响
	b.bw1_profit_rate_eff_fl,  -- 返利对毛利率影响


	-- 月维度数据
	a.sale_amt/10000 sale_amt,
	a.profit/10000 profit,
    a.profitlv,	 
	a.sale_amt_lastm/10000 sale_amt_lastm,
	-- profit_lastm,
	a.profit_lastmlv, 
	a.profit_hblv,
	a.prov_sale_amt/10000 prov_sale_amt,
	-- prov_profit,
	a.prov_profitlv,
	a.profitlv-prov_profitlv as profitlv_diff,	
	-- mclass_des_top1,

	-- 月至今异常数据
	-- sale_amt,
	-- profit,
	b.sale_amt_zs,
	b.sale_amt_tj,
	b.sale_amt_fl,
	b.profit_rate_zs,   -- 直送毛利率
	b.sale_rate_zs,  -- 直送金额占比
	b.profit_rate_eff_zs,  -- 直送对毛利率影响	
	b.profit_rate_eff_tj,  -- 调价对毛利率影响
	b.profit_rate_eff_fl,  -- 返利对毛利率影响

	'' zw2,
	c.classify_middle_list,
	c.goods_list_1
from csx_analyse_tmp.tmp_cust_sale_profit_trend_list a
left join csx_analyse_tmp.tmp_top_cust_profit_eff b on a.customer_code=b.customer_code
left join csx_analyse_tmp.tmp_sale_cust_mclass_eff_TOP c on a.customer_code=c.customer_code 
;






 -- top客户品类毛利影响 本周上周月至今
drop table csx_analyse_tmp.tmp_top_cust_mclass_profit_eff;
create table csx_analyse_tmp.tmp_top_cust_mclass_profit_eff
as
with 
sale_detail as -- 销售明细
(
  select * from csx_analyse_tmp.tmp_top_cust_sale_detail	
),

 -- top客户品类销售 本周上周月至今
 sale_cust_mclass_sale
 as 
(
select *
from 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
	
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_qty end) bw0_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_cost end) bw0_sale_cost,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_amt end) bw0_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.profit end) bw0_profit,	

	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.sale_qty end) bw1_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.sale_cost end) bw1_sale_cost,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.sale_amt end) bw1_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.profit end) bw1_profit,	

	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_qty end) sale_qty,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_cost end) sale_cost,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) profit,	

	-- 直送
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_zhisong='是' then a.sale_qty end) bw0_sale_qty_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_zhisong='是' then a.sale_cost end) bw0_sale_cost_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_zhisong='是' then a.sale_amt end) bw0_sale_amt_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_zhisong='是' then a.profit end) bw0_profit_zs,	

	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_zhisong='是' then a.sale_qty end) bw1_sale_qty_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_zhisong='是' then a.sale_cost end) bw1_sale_cost_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_zhisong='是' then a.sale_amt end) bw1_sale_amt_zs,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_zhisong='是' then a.profit end) bw1_profit_zs,	

	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_zhisong='是' then a.sale_qty end) sale_qty_zs,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_zhisong='是' then a.sale_cost end) sale_cost_zs,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_zhisong='是' then a.sale_amt end) sale_amt_zs,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_zhisong='是' then a.profit end) profit_zs,	
	
	-- 调价
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_tiaojia='是' then a.sale_qty end) bw0_sale_qty_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_tiaojia='是' then a.sale_cost end) bw0_sale_cost_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_tiaojia='是' then a.sale_amt end) bw0_sale_amt_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_tiaojia='是' then a.profit end) bw0_profit_tj,	

	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_tiaojia='是' then a.sale_qty end) bw1_sale_qty_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_tiaojia='是' then a.sale_cost end) bw1_sale_cost_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_tiaojia='是' then a.sale_amt end) bw1_sale_amt_tj,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_tiaojia='是' then a.profit end) bw1_profit_tj,	

	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_tiaojia='是' then a.sale_qty end) sale_qty_tj,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_tiaojia='是' then a.sale_cost end) sale_cost_tj,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_tiaojia='是' then a.sale_amt end) sale_amt_tj,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_tiaojia='是' then a.profit end) profit_tj,	

	-- 返利
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_fanli='是' then a.sale_qty end) bw0_sale_qty_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_fanli='是' then a.sale_cost end) bw0_sale_cost_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_fanli='是' then a.sale_amt end) bw0_sale_amt_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) and is_fanli='是' then a.profit end) bw0_profit_fl,	

	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_fanli='是' then a.sale_qty end) bw1_sale_qty_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_fanli='是' then a.sale_cost end) bw1_sale_cost_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_fanli='是' then a.sale_amt end) bw1_sale_amt_fl,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) and is_fanli='是' then a.profit end) bw1_profit_fl,	

	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_fanli='是' then a.sale_qty end) sale_qty_fl,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_fanli='是' then a.sale_cost end) sale_cost_fl,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_fanli='是' then a.sale_amt end) sale_amt_fl,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
			and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') and is_fanli='是' then a.profit end) profit_fl	
from sale_detail a
group by 
	customer_code,
	classify_middle_code,
	classify_middle_name
)a 
)

 -- top客户品类毛利影响 本周上周月至今
select
	customer_code,
	classify_middle_code,
	classify_middle_name,
	-- 月至今
	sale_amt,
	profit,
	sale_amt_zs,
	sale_amt_tj,
	sale_amt_fl,
	round(profit_zs/abs(sale_amt_zs),6) as profit_rate_zs,   -- 直送毛利率
	round(sale_amt_zs/sale_amt,6) as sale_rate_zs,  -- 直送金额占比
	round(profit/abs(sale_amt)-(profit-profit_zs)/abs(sale_amt-sale_amt_zs),6) as profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	round(profit/abs(sale_amt)-(profit-profit_tj)/abs(sale_amt-sale_amt_tj),6) as profit_rate_eff_tj,  -- 调价对品类毛利率影响
	round(profit/abs(sale_amt)-(profit-profit_fl)/abs(sale_amt-sale_amt_fl),6) as profit_rate_eff_fl,  -- 返利对品类毛利率影响

	-- 本周
	bw0_sale_amt,
	bw0_profit,
	bw0_sale_amt_zs,
	bw0_sale_amt_tj,
	bw0_sale_amt_fl,
	round(bw0_profit_zs/abs(bw0_sale_amt_zs),6) as bw0_profit_rate_zs,   -- 直送毛利率	
	round(bw0_sale_amt_zs/bw0_sale_amt,6) as bw0_sale_rate_zs,  -- 直送金额占比
	round(bw0_profit/abs(bw0_sale_amt)-(bw0_profit-bw0_profit_zs)/abs(bw0_sale_amt-bw0_sale_amt_zs),6) as bw0_profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	round(bw0_profit/abs(bw0_sale_amt)-(bw0_profit-bw0_profit_tj)/abs(bw0_sale_amt-bw0_sale_amt_tj),6) as bw0_profit_rate_eff_tj,  -- 调价对品类毛利率影响
	round(bw0_profit/abs(bw0_sale_amt)-(bw0_profit-bw0_profit_fl)/abs(bw0_sale_amt-bw0_sale_amt_fl),6) as bw0_profit_rate_eff_fl,  -- 返利对品类毛利率影响

	-- 上周
	bw1_sale_amt,
	bw1_profit,
	bw1_sale_amt_zs,
	bw1_sale_amt_tj,
	bw1_sale_amt_fl,
	round(bw1_profit_zs/abs(bw1_sale_amt_zs),6) as bw1_profit_rate_zs,   -- 直送毛利率	
	round(bw1_sale_amt_zs/bw1_sale_amt,6) as bw1_sale_rate_zs,  -- 直送金额占比
	round(bw1_profit/abs(bw1_sale_amt)-(bw1_profit-bw1_profit_zs)/abs(bw1_sale_amt-bw1_sale_amt_zs),6) as bw1_profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	round(bw1_profit/abs(bw1_sale_amt)-(bw1_profit-bw1_profit_tj)/abs(bw1_sale_amt-bw1_sale_amt_tj),6) as bw1_profit_rate_eff_tj,  -- 调价对品类毛利率影响
	round(bw1_profit/abs(bw1_sale_amt)-(bw1_profit-bw1_profit_fl)/abs(bw1_sale_amt-bw1_sale_amt_fl),6) as bw1_profit_rate_eff_fl  -- 返利对品类毛利率影响
from sale_cust_mclass_sale
;




 -- Top客户品类-商品周毛利影响
drop table csx_analyse_tmp.tmp_sale_cust_goods_eff_TOP;
create table csx_analyse_tmp.tmp_sale_cust_goods_eff_TOP
as
with 
sale_detail as -- 销售明细
(
  select * from csx_analyse_tmp.tmp_top_cust_sale_detail	
),
 sale_cust_goods_eff -- 商品对品类影响
 as 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
	goods_code,
	goods_name,
	sale_amt,
	profit,
	profit_rate,
	sale_amt_s,
	profit_s,	
	profit_rate_s,
	cust_sale_amt,
	cust_profit,
	cust_sale_amt_s,
	cust_profit_s,
	-- 剔除法影响值
	round(cust_profit/abs(cust_sale_amt)-(cust_profit-profit)/abs(cust_sale_amt-sale_amt),6) as profit_rate_eff_mclass
	-- 还原法影响值 此处为品类里商品对品类的影响值
	-- (品类本期销售额*品类毛利率差值+(品类本期销售额-品类上期销售额)*(品类本期毛利率-客户整体本期毛利率))/客户整体上期销售额
	-- round((sale_amt*(profit_rate-profit_rate_s)+(sale_amt-sale_amt_s)*(profit_rate-cust_profit/abs(cust_sale_amt)))/abs(cust_sale_amt_s),6) as profit_rate_eff_mclass
from 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
	goods_code,
	goods_name,
	sale_amt,
	profit,
	profit/abs(sale_amt) as profit_rate,
	sale_amt_s,
	profit_s,
	profit_s/abs(sale_amt_s) as profit_rate_s,
	sum (sale_amt) over(partition by customer_code,classify_middle_code) as cust_sale_amt,
	sum (profit) over(partition by customer_code,classify_middle_code) as cust_profit,
	sum (sale_amt_s) over(partition by customer_code,classify_middle_code) as cust_sale_amt_s,
	sum (profit_s) over(partition by customer_code,classify_middle_code) as cust_profit_s		
from 
	(
	select 
		customer_code,
		classify_middle_code,
		classify_middle_name,
		goods_code,
		goods_name,
		sum(case when week=weekofyear(date_sub(current_date,2+0)) then sale_amt end) sale_amt,
		sum(case when week=weekofyear(date_sub(current_date,2+0)) then profit end) profit,
		sum(case when week=weekofyear(date_sub(current_date,9+0)) then sale_amt end) sale_amt_s,
		sum(case when week=weekofyear(date_sub(current_date,9+0)) then profit end) profit_s
	from sale_detail
	-- where week=weekofyear(date_sub(current_date,2+0))
	group by 
		customer_code,
		classify_middle_code,
		classify_middle_name,
		goods_code,
		goods_name
	)a		
)a 
)

-- 商品价格原因定位及价格来源
--  sale_cust_goods_reason_price_source
select 
	a.*,
	d.sale_price_reason,   -- 售价原因
	d.cost_price_reason,   -- 成本原因
	d.price_source,   -- 价格来源
	d.price_type_final,   -- 定价类型
	d.suggest_price_type,   -- 建议售价取值类型
	d.bmk_price,   -- 对标价
	d.price_begin_date,   -- 报价开始时间
	d.suggest_price,   -- 建议售价
	d.purchase_price,   -- 采购报价
	d.received_price,   -- 近期入库成本
	d.classify_middle_threshold,   -- 品类阈值
	concat(goods_name, ':', '销售额', round(sale_amt,0),',毛利率',round(profit_rate*100, 1),'%',',毛利率影响',round(profit_rate_eff_mclass*100, 2),'%'
		, ',价格来源-',d.price_source, case when d.sale_price_reason<>'' then concat(',', d.sale_price_reason) else '' end,
				case when d.cost_price_reason<>'' then concat(',', d.cost_price_reason) else '' end	
	) as goods_ms,
	row_number() over(partition by a.customer_code order by a.profit_rate_eff_mclass asc) as goods_eff_rank	

from sale_cust_goods_eff a
-- 客户商品的价格原因\价格来源以及参考售价
left join 
(
select 
customer_code,goods_code,inventory_dc_code,
cus_goods_type_profit_infect,-- 客户商品毛利影响
sale_price_reason,   -- 售价原因
cost_price_reason,   -- 成本原因
price_source,   -- 价格来源
price_type_final,   -- 定价类型
suggest_price_type,   -- 建议售价取值类型
bmk_price,   -- 对标价 对标地点价格
price_begin_date,   -- 报价开始时间
suggest_price,   -- 建议售价
purchase_price,   -- 采购报价
received_price,   -- 近期入库成本
classify_middle_threshold,   -- 品类阈值
cost_price 
from csx_analyse_tmp.c_tmp_cus_price_guide_order_final
-- 客户商品毛利影响排名 取影响最大对应的原因
where cus_goods_type_profit_infect_pm=1
-- 客户商品定价类型排名 取最后一单的定价类型以及建议售价等
and order_pm=1
) d on a.customer_code=d.customer_code and a.goods_code=d.goods_code 
;


--  -- Top客户品类-商品周毛利影响top
-- sale_cust_goods_eff_TOP
--   as 
-- (
-- select *
-- from 
-- (
-- -- select *,
-- -- 	concat(goods_name, ':', '销售额', round(sale_amt,0),',毛利率',round(profit_rate*100, 1),'%',',毛利率影响',round(profit_rate_eff_mclass*100, 2),'%') as goods_ms,
-- -- 	row_number() over(partition by customer_code order by profit_rate_eff_mclass asc) as goods_eff_rank	
-- -- from sale_cust_goods_eff 
-- )a 
-- where goods_eff_rank<=2
-- )
-- select customer_code,
-- classify_middle_code,
-- classify_middle_name,
-- concat_ws('；',collect_list(goods_ms)) as goods_list
-- from sale_cust_goods_eff_TOP
-- group by customer_code,
-- classify_middle_code,
-- classify_middle_name
-- ;




------------TOP客户的品类

--  报价策略表 csx_analyse_tmp.tmp3 
-- customer_code	classify_middle_name	order_value 客户，品类，报价策略;
-- 如果没有匹上就是客户报价策略没有线上化
with 
sale_detail as -- 销售明细
(
  select 
	split(a.id, '&')[0] as credential_no,
	a.order_code,
	a.original_order_code,	
	a.performance_region_code,
	a.performance_region_name,	--	业绩大区名称
	a.performance_province_code,
	a.performance_province_name,	--	业绩省区名称
	a.performance_city_name,	--	业绩城市名称	
	d.performance_region_name as performance_region_name_cust,
	d.performance_province_name as performance_province_name_cust,
	d.performance_city_name as performance_city_name_cust,
	a.inventory_dc_code,
	a.inventory_dc_name,
	if(a.order_channel_code=6 ,'是','否') as is_tiaojia,
	if(a.order_channel_code=4 ,'是','否') as is_fanli,
	if(a.delivery_type_name='直送','是','否') as is_zhisong,
	case 
		when a.delivery_type_name='配送' then ''
		when a.direct_delivery_type=1 then 'R直送1'
		when a.direct_delivery_type=2 then 'Z直送2'
		when a.direct_delivery_type=11 then '临时加单'
		when a.direct_delivery_type=12 then '紧急补货'
		when a.direct_delivery_type=0 then '普通' else '普通' end direct_delivery_type,	
    if(b.first_sales_date>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-',''),'是','否') as is_new_cust_2m,
	a.customer_code,
	d.customer_name,
	d.second_category_name,
	b.first_sales_date,
    a.classify_middle_code,
    a.classify_middle_name,
	a.goods_code,
	a.goods_name,
	a.sdt,
	a.week,
	if(a.order_channel_detail_code=26,0,sale_qty) as sale_qty,
	a.sale_cost,
	a.sale_amt,
	a.profit
  from 
  (
        select *,weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-4)) week 
       from csx_dws.csx_dws_sale_detail_di
       where sdt>=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')  
	   and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	   and channel_code in('1','7','9')
	   and business_type_code=1	
	   and inventory_dc_code<>'W0J2'		   
	)  a 
join 
    ( 
	  select
          distinct shop_code 
	  from csx_dim.csx_dim_shop 
	  where sdt='current' and shop_low_profit_flag=0 
	 )c  on a.inventory_dc_code = c.shop_code
left join  
   (
     select 
		customer_code,	--	客户编码
		customer_name,	--	客户名称
		channel_name,	--	渠道名称
		performance_region_name,	--	业绩大区名称
		performance_province_name,	--	业绩省区名称
		performance_city_name,	--	业绩城市名称
		first_category_name,	--	一级客户分类名称
		second_category_name,	--	二级客户分类名称
		third_category_name,	--	三级客户分类名称
		business_attribute_desc,	--	商机属性描述
		sign_time,	--	签约时间
		first_sign_time	--	第一次签约时间	 
     from  csx_dim.csx_dim_crm_customer_info 
     where sdt='current'
	 and customer_type_code=4
	)d on d.customer_code=a.customer_code 	
left join  -- 首单日期
   (
     select 
       customer_code,
  	   min(first_business_sale_date) first_sales_date
     from csx_dws.csx_dws_crm_customer_business_active_di
     where sdt ='current'  and  business_type_code in (1)
     group by customer_code
    )b on b.customer_code=a.customer_code	 
),

 -- 月至今省区业绩毛利
 province_sale_profit
 as 
(
select 
    a.performance_province_code,     
	a.performance_province_name,     	
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) prov_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) prov_profit
  from sale_detail a
group by
    a.performance_province_code,     
	a.performance_province_name
),

 -- 月至今客户业绩毛利
 cust_sale_profit
 as 
(
select 
    performance_province_code,     
	performance_province_name,
	customer_code,
	sum(case when sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then sale_amt end) cust_sale_amt,
	sum(case when sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then profit end) cust_profit
  from sale_detail 
group by
    performance_province_code,     
	performance_province_name,
	customer_code
),
 -- 本周客户业绩毛利
 cust_sale_profit_w
 as 
(
select 
    performance_province_code,     
	performance_province_name,
	customer_code,
	sum(case when week=weekofyear(date_sub(current_date,2+0)) then sale_amt end) cust_sale_amt_w,
	sum(case when week=weekofyear(date_sub(current_date,2+0)) then profit end) cust_profit_w
  from sale_detail 
group by
    performance_province_code,     
	performance_province_name,
	customer_code
),

top_cust_sale_mclass_profit as -- 重点客户品类毛利
(
select 
    a.performance_region_name,
	a.performance_province_code,     
	a.performance_province_name,
	a.performance_city_name,
    a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,
    a.classify_middle_code,
    a.classify_middle_name,
	mclass_rank,
	rown,
	-- rank_cust,
	bw0_sale_amt,
	bw0_profit,	
	bw0_profit/abs(bw0_sale_amt) bw0_profitlv,
	bw1_sale_amt,
	bw1_profit,
    bw1_profit/abs(bw1_sale_amt) bw1_profitlv,
    bw2_sale_amt,
	bw2_profit,	
    bw2_profit/abs(bw2_sale_amt) bw2_profitlv,
    bw3_sale_amt,
  	bw3_profit,	
    bw3_profit/abs(bw3_sale_amt) bw3_profitlv,
    bw4_sale_amt,
	bw4_profit,		
    bw4_profit/abs(bw4_sale_amt) bw4_profitlv,
	sale_amt,
	profit,
    profit/abs(sale_amt) profitlv,	 
	sale_amt_lastm,
	profit_lastm,
	profit_lastm/abs(sale_amt_lastm) profit_lastmlv, 
	profit/abs(sale_amt)-profit_lastm/abs(sale_amt_lastm) profit_hblv
from(
select
a.*,
 row_number() over(partition by a.performance_province_code,a.customer_code order by nvl(a.sale_amt,0) desc) mclass_rank,
 row_number() over(partition by a.performance_province_code,a.classify_middle_code order by nvl(a.sale_amt,0) desc) rown
from 
( select 
    a.performance_region_name,
	a.performance_province_code,     
	a.performance_province_name,
	a.performance_city_name,	
    a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,
    a.classify_middle_code,
    a.classify_middle_name,	
	-- 注意找上周需要根据跑数是周几相应调整相对数字
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.sale_amt end) bw0_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,2+0)) then a.profit end) bw0_profit,	
	
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.sale_amt end) bw1_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,9+0)) then a.profit end) bw1_profit,

    sum(case when a.week=weekofyear(date_sub(current_date,16+0)) then a.sale_amt end) bw2_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,16+0)) then a.profit end) bw2_profit,	

    sum(case when a.week=weekofyear(date_sub(current_date,23+0)) then a.sale_amt end) bw3_sale_amt,
  	sum(case when a.week=weekofyear(date_sub(current_date,23+0)) then a.profit end) bw3_profit,	

    sum(case when a.week=weekofyear(date_sub(current_date,30+0)) then a.sale_amt end) bw4_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date,30+0)) then a.profit end) bw4_profit,		
	
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sale_amt_lastm,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) profit_lastm
  from sale_detail a	
group by 
	a.performance_region_name,
	a.performance_province_code,     
	a.performance_province_name, 
	a.performance_city_name,    
    a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,
    a.classify_middle_code,
    a.classify_middle_name
  )a
 join csx_analyse_tmp.tmp_cust_sale_profit_trend_list b on a.customer_code=b.customer_code
)a
-- where rown<=10
),

 -- 月至今省区品类业绩毛利
 province_sale_mclass_profit
 as 
(
select 
    performance_province_code,     
	performance_province_name,     
    classify_middle_code,
    classify_middle_name,		
	sum(sale_amt) sale_amt,
	sum(profit) profit
  from sale_detail 
	where sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') 
	and sdt <= regexp_replace(add_months('${i_sdate}',0),'-','')  
group by
    performance_province_code,     
	performance_province_name,     
    classify_middle_code,
    classify_middle_name
),

 -- 本周省区品类业绩毛利
 bw0_province_sale_mclass_profit
 as 
(
select 
    performance_province_code,     
	performance_province_name,     
    classify_middle_code,
    classify_middle_name,		
	sum(sale_amt) bw0_sale_amt,
	sum(profit) bw0_profit
  from sale_detail 
	where week=weekofyear(date_sub(current_date,2+0))  
group by
    performance_province_code,     
	performance_province_name,     
    classify_middle_code,
    classify_middle_name
),


 -- top客户品类定价类型占比  本周 彩食鲜周
 cust_sale_mclass_price_top
 as 
(
select customer_code,
	classify_middle_code,
	classify_middle_name,
	price_type,
	sale_amt,
	profit,
	sum(sale_amt) over(partition by customer_code) as cust_sale_amt,
    sum(profit) over(partition by customer_code) as cust_profit,
	sum(sale_amt) over(partition by customer_code,classify_middle_code) as cust_mclass_sale_amt,
    sum(profit) over(partition by customer_code,classify_middle_code) as cust_mclass_profit,	
	row_number() over(partition by customer_code,classify_middle_code,price_type order by sale_amt desc) as mclass_price_rank
from 
(
select 
	a.customer_code,
    a.classify_middle_code,
    a.classify_middle_name,
	-- 定价类型(1-建议售价 2 -  对标对象  3 -销售成本价 4-上一周价格 5-售价 6-采购/库存成本)
	b.price_type_final as price_type,
	-- b.price_begin_date,
	-- b.final_qj_date, -- 取价时间 
	-- b.bmk_code,  -- 对标地点编码
	-- b.bmk_price, -- 对标地点价格 
	-- b.suggest_price, -- 建议售价
	-- b.suggest_price_type, -- 建议售价取值类型
	-- b.purchase_price, -- 采购报价
	sum(a.sale_amt) sale_amt,
	sum(a.profit) profit	
	from 
	(
	select *
	from sale_detail
	-- from csx_analyse_tmp.tmp_top_cust_sale_detail --测试用，改回sale_detail
	where week=weekofyear(date_sub(current_date,2+0)) 
	-- where weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2))=weekofyear(date_sub(current_date,2+0))	
	)a
left join 
	(
	select * 
	from csx_analyse_tmp.c_tmp_cus_price_guide_order_final 
	)b on a.order_code=b.order_code and a.goods_code=b.goods_code and a.customer_code=b.customer_code 
group by 
	a.customer_code,
    a.classify_middle_code,
    a.classify_middle_name,
	b.price_type_final
)a
-- join csx_analyse_tmp.tmp_cust_sale_profit_trend_list c on a.customer_code=c.customer_code
-- left join 
--  (
--    select 
--      *  
--    from  csx_dim.csx_dim_basic_goods 
--    where sdt = 'current'
--  ) c on c.goods_code = a.goods_code 
),

 -- top客户品类定价类型占比  本周 彩食鲜周
 cust_sale_mclass_price_rate
 as 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
concat_ws('、',collect_list(price_mclass_des)) as price_mclass_des_list
from 
(
select 
	customer_code,
	classify_middle_code,
	classify_middle_name,
	nvl(price_type,'空') price_type,
	mclass_price_rank,
	concat(nvl(price_type,'空'),':',round(sale_amt/cust_mclass_sale_amt*100, 0), '%') as price_mclass_des	
from cust_sale_mclass_price_top
order by mclass_price_rank
)a
where price_type<>'空'
group by 
	customer_code,
	classify_middle_code,
	classify_middle_name
)
select
	e.cust_flag_group,
	a.mclass_rank,
	a.performance_region_name,    
	a.performance_province_name, 
	a.performance_city_name,
    a.customer_code,
	a.customer_name,
	a.second_category_name,
	a.first_sales_date,
    a.classify_middle_code,
    a.classify_middle_name,
	d.order_value bjcl, -- 报价策略
	-- '' bjcl, -- 报价策略

-- 周维度数据

	f2.cust_sale_amt_w/10000 cust_sale_amt_w,
	a.bw0_sale_amt/f2.cust_sale_amt_w as sale_amt_rate_w,  -- 品类销售占比
	round(f2.cust_profit_w/abs(f2.cust_sale_amt_w)-(f2.cust_profit_w-a.bw0_profit)/abs(f2.cust_sale_amt_w-a.bw0_sale_amt),6) as profit_rate_eff_w,  -- 品类对客户毛利率影响		

    a.bw4_sale_amt/10000 bw4_sale_amt,
	a.bw3_sale_amt/10000 bw3_sale_amt,
	a.bw2_sale_amt/10000 bw2_sale_amt,
	a.bw1_sale_amt/10000 bw1_sale_amt,
	a.bw0_sale_amt/10000 bw0_sale_amt,
	a.bw4_profit/10000 bw4_profit,	
	a.bw3_profit/10000 bw3_profit,
	a.bw2_profit/10000 bw2_profit,
	a.bw1_profit/10000 bw1_profit,
	a.bw0_profit/10000 bw0_profit,
	a.bw4_profitlv,
	a.bw3_profitlv,
	a.bw2_profitlv,
	a.bw1_profitlv,
	a.bw0_profitlv,
	a.bw0_profitlv-a.bw1_profitlv as bw_profit_hblv,
	'' aa_aw,
	
	b2.bw0_sale_amt/10000 as bw0_prov_sale_amt,
	-- b2.bw0_profit/10000 as bw0_prov_profit,
	b2.bw0_profit/abs(b2.bw0_sale_amt) as bw0_prov_profitlv,
	a.bw0_profitlv-b2.bw0_profit/abs(b2.bw0_sale_amt) as bw0_profitlv_diff,
		
		-- 本周
	-- bw0_sale_amt,
	-- bw0_profit,
	bw0_sale_amt_zs,
	bw0_sale_amt_tj,
	bw0_sale_amt_fl,
	bw0_profit_rate_zs,   -- 直送毛利率	
	bw0_sale_rate_zs,  -- 直送金额占比
	bw0_profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	bw0_profit_rate_eff_tj,  -- 调价对品类毛利率影响
	bw0_profit_rate_eff_fl,  -- 返利对品类毛利率影响
	-- 上周
	-- bw1_sale_amt,
	-- bw1_profit,
	bw1_sale_amt_zs,
	bw1_sale_amt_tj,
	bw1_sale_amt_fl,
	bw1_profit_rate_zs,   -- 直送毛利率	
	bw1_sale_rate_zs,  -- 直送金额占比
	bw1_profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	bw1_profit_rate_eff_tj,  -- 调价对品类毛利率影响
	bw1_profit_rate_eff_fl,  -- 返利对品类毛利率影响

--  月维度数据

	a.sale_amt/10000 sale_amt,
	a.profit/10000 profit,	
	a.profitlv,	
	a.sale_amt_lastm/10000 sale_amt_lastm,
	a.profit_lastm/10000 profit_lastm,
	a.profit_lastmlv, 
	a.profit_hblv,
	a.profitlv - b.profit/abs(b.sale_amt) as profit_rate_province_db,		
	b.sale_amt/10000 as sale_amt_province,
	-- b.profit as profit_province,
	b.profit/abs(b.sale_amt) as profit_rate_province,
	f.cust_sale_amt/10000 cust_sale_amt,
	f.cust_profit/abs(f.cust_sale_amt) cust_profitlv, -- 本月客户整体毛利率
	a.sale_amt/f.cust_sale_amt as sale_amt_rate,  -- 品类销售占比
	round(f.cust_profit/abs(f.cust_sale_amt)-(f.cust_profit-a.profit)/abs(f.cust_sale_amt-a.sale_amt),6) as profit_rate_eff,  -- 品类对客户毛利率影响
	-- 月至今
	-- sale_amt,
	-- profit,
	sale_amt_zs,
	sale_amt_tj,
	sale_amt_fl,
	profit_rate_zs,   -- 直送毛利率
	sale_rate_zs,  -- 直送金额占比
	profit_rate_eff_zs,  -- 直送对品类毛利率影响	
	profit_rate_eff_tj,  -- 调价对品类毛利率影响
	profit_rate_eff_fl,  -- 返利对品类毛利率影响
	if(a.mclass_rank<=5 or (a.sale_amt>500 and a.profitlv<0.05),
	case when (a.profitlv - b.profit/abs(b.sale_amt))<-0.05 then '是'
		 when (a.bw0_profitlv-a.bw1_profitlv)<-0.02 then '是'
		 end ,'') as focus_flag,
	-- 定价类型金额占比
	h.price_mclass_des_list,
	-- concat(c1.goods_name, ':', '销售额', round(c1.sale_amt,0),',毛利率',round(c1.profit/abs(c1.sale_amt)*100, 0),'%',',销售额占比',round(c1.sale_amt/a.sale_amt*100, 0),'%') goods_top1
	-- 低负毛利TOP2商品
	-- c.goods_top_ms_list
	-- 毛利影响TOP2商品
	i.goods_list
	-- concat(c3.goods_name, ':', '销售额', round(c3.sale_amt,0),',毛利率',round(c3.profit/abs(c3.sale_amt)*100, 0),'%') goods_top3	
from top_cust_sale_mclass_profit a
left join province_sale_mclass_profit b on a.performance_province_code=b.performance_province_code and a.classify_middle_code=b.classify_middle_code
left join bw0_province_sale_mclass_profit  b2 on a.performance_province_code=b2.performance_province_code and a.classify_middle_code=b2.classify_middle_code
-- left join cust_sale_mclass_goods_top_list c on a.customer_code=c.customer_code and a.classify_middle_code=c.classify_middle_code
-- left join cust_sale_mclass_goods_top c3 on a.customer_code=c3.customer_code and a.classify_middle_code=c3.classify_middle_code and c3.mclass_goods_rank=3
-- 客户品类报价类型
left join csx_analyse_tmp.tmp3 d on a.customer_code=d.customer_code and a.classify_middle_code=d.classify_middle_code
join csx_analyse_tmp.tmp_cust_sale_profit_trend_list e on a.customer_code=e.customer_code
left join cust_sale_profit f on f.customer_code=a.customer_code
left join cust_sale_profit_w f2 on f2.customer_code=a.customer_code
left join csx_analyse_tmp.tmp_top_cust_mclass_profit_eff g on g.customer_code=a.customer_code and g.classify_middle_code=a.classify_middle_code
left join cust_sale_mclass_price_rate h on h.customer_code=a.customer_code and h.classify_middle_code=a.classify_middle_code
-- left join csx_analyse_tmp.tmp_sale_cust_goods_eff_TOP i on i.customer_code=a.customer_code and i.classify_middle_code=a.classify_middle_code
left join 
(
select customer_code,
classify_middle_code,
classify_middle_name,
concat_ws('；',collect_list(goods_ms)) as goods_list
from csx_analyse_tmp.tmp_sale_cust_goods_eff_TOP
where goods_eff_rank<=2
group by customer_code,
classify_middle_code,
classify_middle_name
)i on i.customer_code=a.customer_code and i.classify_middle_code=a.classify_middle_code
;






-- 客户商品
-- +同期入库成本
with top_cust_sale_detail as -- top客户销售明细
(
    select * from csx_analyse_tmp.tmp_top_cust_sale_detail
 ),
  
 dc_goods_received -- 商品入库成本
 as 
 (
	select 
		b.*,
		case when (c.business_division_name like '%生鲜%' and c.classify_middle_code='B0101') or  c.business_division_name like '%食百%' then '食百' else '生鲜' end as division_name,
		case 
		 when (c.business_division_name like '%生鲜%' and c.classify_middle_code='B0101') or  c.business_division_name like '%食百%' then coalesce(b.received_price_30,b.received_price_60)
		 else coalesce(b.received_price_7,b.received_price_14) end as received_price
	from 	
	(-- 入库减供应商退货作为最终入库
		select *,
			cast(received_amount_all/received_qty_all as decimal(20,6)) as received_price_avg,
			cast(received_amount_60/received_qty_60 as decimal(20,6)) as received_price_60,
			cast(received_amount_30/received_qty_30 as decimal(20,6)) as received_price_30,
			cast(received_amount_14/received_qty_14 as decimal(20,6)) as received_price_14,
			cast(received_amount_7/received_qty_7 as decimal(20,6)) as received_price_7,
			cast(received_amount_1/received_qty_1 as decimal(20,6)) as received_price_1
		from 
		(select 
			b1.target_location_code,
			b1.goods_code,
			-- sum((case when b1.received_amount<0 then 0 else b1.received_amount end)) as all_not_t_received_amount,
			-- sum(nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0)) as all_gys_shipped_amount,
			-- sum((case when b1.received_qty<0 then 0 else b1.received_qty end)) as all_not_t_received_qty,
			-- sum(nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as all_gys_shipped_qty,
			-- 入库-供应商退货，如果有价格补救则取价格补救后的值,若同时有价格补救与退货则只看退货
			-- 近30天/近7天
			sum((case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
				-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0)) as received_amount_all,
				
			sum((case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
				-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as received_qty_all,
				
			-- cast(max(case when b1.received_amount<0 then 0 
			-- 	else if(b2.received_amount is not null,b2.received_price2,
			-- 			if(coalesce(b1.received_price2,0)=0,b1.received_price1,b1.received_price2)) end)
			-- as decimal(20,6)) as received_price_max,


			-- 近60日
		 	sum(if(b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-60),'-',''),
					(case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
						-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0),0))
		 	 as received_amount_60,

		 	sum(if(b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-60),'-',''),
					(case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
						-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0),0))
		 	 as received_qty_60,

			-- 近30日
		 	sum(if(b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-',''),
					(case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
						-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0),0))
		 	 as received_amount_30,

		 	sum(if(b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-',''),
					(case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
						-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0),0))
		 	 as received_qty_30,

			-- 近14日
		 	sum(if(b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-13),'-',''),
					(case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
						-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0),0))
		 	 as received_amount_14,

		 	sum(if(b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-13),'-',''),
					(case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
						-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0),0))
		 	 as received_qty_14,
			 
			-- 近7日
		 	sum(if(b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-',''),
					(case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
						-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0),0))
		 	 as received_amount_7,

		 	sum(if(b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-',''),
					(case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
						-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0),0))
		 	 as received_qty_7,

			
			-- 昨日
		 	sum(if(b1.sdt=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-1),'-',''),
					(case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
						-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0),0))
		 	 as received_amount_1,

		 	sum(if(b1.sdt=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-1),'-',''),
					(case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
						-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0),0))
		 	 as received_qty_1			
		from 
			-- 入库数据
			(
				select target_location_code, 
					order_code,goods_code,sdt,
					sum(received_amount) received_amount,
					sum(received_qty) received_qty,
					sum(received_amount)/sum(received_qty) as received_price1,
					max(received_price2) received_price2
				from csx_dws.csx_dws_scm_order_received_di 
				where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-60),'-','') 
				and sdt<='${sdt_yes}' 
				and super_class in (1,3) -- 加上调拨入库的数据 供应商订单
				and header_status=4 
				and source_type not in (2,3,4,11,15,16)  -- 剔除项目合伙人
				and local_purchase_flag='0' -- 剔除地采，是否地采(0-否、1-是)
				and direct_delivery_type='0' -- 直送类型 0-P(普通) 1-R(融单)、2-Z(过账)
				-- and target_location_code in ('W0BK')
				and nvl(received_amount,0)<>0
				group by target_location_code,order_code,goods_code,sdt
			) b1 
			-- 关联价格补救订单数据，如果有价格补救则成本取补救单中的价格
			left join 
			(
				select * 
				from csx_dws.csx_dws_scm_order_received_di 
				where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-60),'-','') 
				and sdt<='${sdt_yes}' 
				-- and target_location_code in ('W0BK') 
				and price_remedy_flag=1 
			) b2 on b1.order_code=b2.original_order_code and b1.goods_code=b2.goods_code 
			-- 关联供应商退货订单
			left join 
			(
				select * 
				from csx_dws.csx_dws_scm_order_shipped_di   
				where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-60),'-','') 
				and super_class in (2) 
				-- and target_location_code in ('W0BK') 
			) b3 on b1.order_code=b3.original_order_code and b1.goods_code=b3.goods_code 
			left join 
			(
				select * 
				from csx_dim.csx_dim_basic_goods 
				where sdt='current' 
			) b4 on b1.goods_code=b4.goods_code 
		-- where b2.original_order_code is null 
		where b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
		and b1.sdt<='${sdt_yes}'
		group by b1.target_location_code,b1.goods_code
		) b 
	) b 
	left join 
	(
		select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current' 
	) c on b.goods_code=c.goods_code 
  ),

 cust_goods_reason -- 客户商品的价格原因\价格来源以及参考售价
 as 
 (
select 
customer_code,goods_code,inventory_dc_code,
cus_goods_type_profit_infect,-- 客户商品毛利影响
sale_price_reason,   -- 售价原因
cost_price_reason,   -- 成本原因
price_source,   -- 价格来源
price_type_final,   -- 定价类型
suggest_price_type,   -- 建议售价取值类型
bmk_price,   -- 对标价 对标地点价格
price_begin_date,   -- 报价开始时间
suggest_price,   -- 建议售价
purchase_price,   -- 采购报价
received_price,   -- 近期入库成本
classify_middle_threshold,   -- 品类阈值
cost_price 
from csx_analyse_tmp.c_tmp_cus_price_guide_order_final
-- 客户商品毛利影响排名 取影响最大对应的原因
where cus_goods_type_profit_infect_pm=1
-- 客户商品定价类型排名 取最后一单的定价类型以及建议售价等
and order_pm=1
  ),
  
 cust_goods -- 客户商品
 as 
 (
select
	a.performance_province_name,
	a.performance_city_name,
	a.business_type_name,
	a.customer_code,
	a.customer_name,
	a.business_division_name,
	a.purchase_group_code,a.purchase_group_name,
	a.classify_large_code,a.classify_large_name,
	a.classify_middle_code,a.classify_middle_name,
	a.classify_small_code,a.classify_small_name,
	a.goods_code,a.goods_name,
	a.is_tiaojia,
	a.is_fanli,
	a.delivery_type_name,
	a.direct_delivery_type,
	a.inventory_dc_code,
	a.types,
	a.is_new_cust_2m,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_cost end) by_sale_cost,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_qty end) by_sale_qty,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_cost end) sy_sale_cost,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_qty end) sy_sale_qty,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit,	
	sum(case when a.week=weekofyear(date_sub(current_date, 2+0)) then a.sale_amt end) bz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 2+0)) then a.sale_cost end) bz_sale_cost,
	sum(case when a.week=weekofyear(date_sub(current_date, 2+0)) then a.sale_qty end) bz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 2+0)) then a.profit end) bz_profit,	
	
	sum(case when a.week=weekofyear(date_sub(current_date, 9+0)) then a.sale_amt end) sz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 9+0)) then a.sale_cost end) sz_sale_cost,
	sum(case when a.week=weekofyear(date_sub(current_date, 9+0)) then a.sale_qty end) sz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 9+0)) then a.profit end) sz_profit		
from csx_analyse_tmp.tmp_top_cust_sale_detail a
-- join csx_analyse_tmp.tmp_cust_sale_profit_trend_list e on a.customer_code=e.customer_code
group by a.performance_province_name,
	   a.performance_city_name,
	   a.business_type_name,
	   a.customer_code,
	   a.customer_name,
	   a.business_division_name,
	   a.purchase_group_code,
	   a.purchase_group_name,
	   a.classify_large_code,
	   a.classify_large_name,
	   a.classify_middle_code,
	   a.classify_middle_name,
	   a.classify_small_code,
	   a.classify_small_name,
	   a.goods_code,
	   a.goods_name,
	   a.is_tiaojia,
	   a.is_fanli,
	   a.delivery_type_name,
	   a.direct_delivery_type,
	   a.inventory_dc_code,
	   a.types,
	   a.is_new_cust_2m
having by_sale_amt is not null or sy_sale_amt is not null or bz_sale_amt is not null or sz_sale_amt is not null
)
select 
	a.performance_province_name,
	a.performance_city_name,
	a.business_type_name,
	a.customer_code,
	a.customer_name,
	a.business_division_name,
	a.purchase_group_code,a.purchase_group_name,
	a.classify_large_code,a.classify_large_name,
	a.classify_middle_code,a.classify_middle_name,
	a.classify_small_code,a.classify_small_name,
	a.goods_code,a.goods_name,
	a.is_tiaojia,
	a.is_fanli,
	a.delivery_type_name,
	a.direct_delivery_type,
	a.inventory_dc_code,
	a.types,
	a.is_new_cust_2m,
	by_sale_amt,
	-- by_sale_cost,
	by_sale_qty,
	by_profit,
	by_profit/abs(by_sale_amt) as by_profitlv,
	by_sale_cost/by_sale_qty as by_cost_price,
	by_sale_amt/by_sale_qty as by_sale_price,
	
	sy_sale_amt,
	-- sy_sale_cost,
	sy_sale_qty,
	sy_profit,
	sy_profit/abs(sy_sale_amt) as sy_profitlv,
	sy_sale_cost/sy_sale_qty as sy_cost_price,	
	sy_sale_amt/sy_sale_qty as sy_sale_price,
		
	bz_sale_amt,
	-- bz_sale_cost,
	bz_sale_qty,
	bz_profit,
	bz_profit/abs(bz_sale_amt) as bz_profitlv,
	bz_sale_cost/bz_sale_qty as bz_cost_price,
	bz_sale_amt/bz_sale_qty as bz_sale_price,
	
	sz_sale_amt,
	-- sz_sale_cost,
	sz_sale_qty,
	sz_profit,
	sz_profit/abs(sz_sale_amt) as sz_profitlv,
	sz_sale_cost/sz_sale_qty as sz_cost_price,
	sz_sale_amt/sz_sale_qty as sz_sale_price,
	-- b.received_price,
	-- if (bz_profit/abs(bz_sale_amt)<-0.3 and (bz_profit/abs(bz_sale_amt)) is not null ,'是','') as zdgz
	i.profit_rate_eff_mclass,  -- 商品对品类毛利影响
	d.sale_price_reason,   -- 售价原因
	d.cost_price_reason,   -- 成本原因
	d.price_source,   -- 价格来源
	d.price_type_final,   -- 定价类型
	d.suggest_price_type,   -- 建议售价取值类型
	d.bmk_price,   -- 对标价
	d.price_begin_date,   -- 报价开始时间
	d.suggest_price,   -- 建议售价
	d.purchase_price,   -- 采购报价
	b.received_price,   -- 近期入库成本
	d.classify_middle_threshold   -- 品类阈值
from cust_goods a
left join dc_goods_received b on a.inventory_dc_code=b.target_location_code and a.goods_code=b.goods_code
-- left join wms_product_change c on a.credential_no=c.credential_no and a.goods_code=c.goods_code
-- 商品对品类的影响值
left join csx_analyse_tmp.tmp_sale_cust_goods_eff_TOP i on i.customer_code=a.customer_code and i.goods_code=a.goods_code
-- 客户商品的价格原因/价格来源以及参考售价
left join cust_goods_reason d on a.customer_code=d.customer_code and a.goods_code=d.goods_code and a.inventory_dc_code=d.inventory_dc_code
order by a.customer_code,a.classify_middle_code,bz_sale_amt desc;















select 
customer_code,goods_code,inventory_dc_code,
cus_goods_type_profit_infect,-- 客户商品毛利影响
sale_price_reason,   -- 售价原因
cost_price_reason,   -- 成本原因
price_source,   -- 价格来源
price_type_final,   -- 定价类型
suggest_price_type,   -- 建议售价取值类型
bmk_price,   -- 对标价 对标地点价格
suggest_price,   -- 建议售价
purchase_price,   -- 采购报价
received_price,   -- 近期入库成本
cost_price 
from csx_analyse_tmp.c_tmp_cus_price_guide_order_final
where cus_goods_type_profit_infect_pm=1
where order_pm=1


goods_ms
concat(goods_name, ':', '销售额', round(sale_amt,0),',毛利率',round(profit_rate*100, 1),'%',',毛利率影响',round(profit_rate_eff_mclass*100, 2),'%') as goods_ms,





