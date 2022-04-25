-- 统计日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');
-- 当月月初
--set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'),'-','');
-- 14天前
set current_start_day = regexp_replace(date_sub(current_date, 1+14),'-','');
-- 库存操作起始日期
set wms_start_day = regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -11),'-','');


--临时表1：明细数据 销售表中取各维度数据+成本、采购报价、中台报价、售价、销售额、毛利等，关联批次取批次库存成本价、关联工厂取原料价
drop table csx_tmp.tmp_goods_salezp;
create temporary table csx_tmp.tmp_goods_salezp
as
select 
  a.sdt,--日期
  a.credential_no,--凭证号
  a.order_no,
  c.batch_no,
  a.region_code,--大区编码
  a.region_name,--大区
  a.province_code,--省区编码
  a.province_name,--省区
  a.city_group_code,--城市组编码
  a.city_group_name,--城市组
  a.dc_code, --DC编码
  f.shop_name as dc_name,  --DC名称
  a.customer_no,--客户编码
  d.customer_name,--客户名称
  a.goods_code,--商品编码
  regexp_replace(regexp_replace(e.goods_name,'\n',''),'\r','') as goods_name,--商品名称
  e.unit,--单位
  e.unit_name,--单位名称
  e.department_id,--课组编码
  e.department_name,--课组名称
  e.classify_middle_code,--管理中类编码
  e.classify_middle_name,--管理中类名称
  case when e.division_code in ('10','11') then '11'
  	   when e.division_code in ('12','13','14','15') then '12'
  	   else '' end as division_code, --部类编码 
  case when e.division_code in ('10','11') then '生鲜'
  	   when e.division_code in ('12','13','14','15') then '食百'
  	   else '' end as division_name,--部类名称
  a.is_factory_goods_desc,--是否工厂加工商品
  case when c.fact_price is not null then '是' end as is_fact, --是否有原料价
  case when purchase_price_flag='1' then '是' end as is_purchase,  --是否有采购报价
  sum(a.sales_qty) sales_qty,--销售数量
  sum(coalesce(c.fact_price,0)*a.sales_qty) fact_value, --原料金额
  sum(coalesce(a.cost_price,0)*a.sales_qty) cost_value,--成本金额
  sum(coalesce(a.purchase_price,0)*a.sales_qty) purchase_value,--采购成本
  sum(coalesce(a.middle_office_price,0)*a.sales_qty) middle_office_value,  --中台成本
  sum(a.sales_value) sales_value,--销售额
  sum(a.sales_cost) sales_cost,--销售成本
  sum(a.profit) profit--毛利
from 
  (
    select 
      sdt,
	  split(id, '&')[0] as credential_no,
	  order_no,
      region_code,
      region_name,
      province_code,
      province_name,
	  city_group_code,
	  city_group_name,
	  dc_code, 
      customer_no,
      customer_name,
      goods_code,
      goods_name,
	  is_factory_goods_desc,
      sales_qty,
      sales_value,
      sales_cost,
      profit,
	  purchase_price_flag,
      cost_price,
      case when purchase_price_flag='1' then purchase_price end as purchase_price,
      middle_office_price,
      sales_price
    from csx_dw.dws_sale_r_d_detail 
    where sdt >= ${hiveconf:current_start_day} and sdt <= ${hiveconf:current_day} 
	and channel_code in ('1', '7', '9')
	and business_type_code ='1'
	and sales_type<>'fanli'
	and return_flag<>'X'
	and province_name in('重庆市','安徽省')
  )a 
  left outer join 
  (
    select
	  b.goods_code,
	  b.credential_no,
	  b.batch_no,
	  sum(b.qty) as qty,
	  --sum(b.price*b.qty)/sum(b.qty) cost_price_0,  --多批次平均库存成本价
	  sum(c.fact_price*b.qty)/sum(case when c.fact_price is not null then b.qty end) fact_price --原料价
	from 
	--批次操作明细表
	(
	  select
	  	goods_code,
	  	credential_no,
		batch_no,
	  	source_order_no,
	  	sum(qty) as qty,
		sum(amt)/sum(qty) price
	  from csx_dw.dws_wms_r_d_batch_detail
	  where sdt >= ${hiveconf:wms_start_day} 
	  and move_type in ('107A', '108A')
	  group by goods_code,credential_no,batch_no,source_order_no
    )b 
	--工厂加工表
    left outer join 
    (
      select 
      	goods_code,
      	order_code,
        sum(fact_values)/sum(goods_reality_receive_qty) as fact_price --原料价
      from csx_dw.dws_mms_r_a_factory_order
      where sdt >= ${hiveconf:wms_start_day} and mrp_prop_key in('3061','3010')
      group by goods_code, order_code
    )c on b.source_order_no = c.order_code and b.goods_code = c.goods_code
	group by b.goods_code,b.credential_no,b.batch_no
  )c on a.goods_code = c.goods_code and a.credential_no = c.credential_no
  --客户信息表
  left outer join 
  (
    select 
		customer_no,
		customer_name
    from csx_dw.dws_crm_w_a_customer
    where sdt = 'current' 
  )d on d.customer_no = a.customer_no
  --商品维表
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_product_m 
    where sdt = 'current'
  )e on e.goods_id = a.goods_code
  --DC门店维表
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt = 'current'
  )f on f.shop_id = a.dc_code
group by 
  a.sdt,
  a.credential_no,
  a.order_no,
  c.batch_no,
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  a.dc_code, 
  f.shop_name,  
  a.customer_no,
  d.customer_name,
  a.goods_code,
  regexp_replace(regexp_replace(e.goods_name,'\n',''),'\r',''),
  e.unit,
  e.unit_name,
  e.department_id,
  e.department_name,
  e.classify_middle_code,
  e.classify_middle_name,
  case when e.division_code in ('10','11') then '11'
  	   when e.division_code in ('12','13','14','15') then '12'
  	   else '' end,  
  case when e.division_code in ('10','11') then '生鲜'
  	   when e.division_code in ('12','13','14','15') then '食百'
  	   else '' end,
  a.is_factory_goods_desc,
  case when c.fact_price is not null then '是' end,
  case when purchase_price_flag='1' then '是' end;
  

---临时表2：昨日销售价格与该省区近14天历史平均价格,各环节加价率
drop table csx_tmp.tmp_goods_res; 
create temporary table csx_tmp.tmp_goods_res
as
select a.sdt,a.credential_no,a.order_no,a.batch_no,
  a.division_code,a.division_name,
  a.department_id,a.department_name,a.classify_middle_code,a.classify_middle_name,
  a.goods_code,a.goods_name,a.unit_name,a.is_factory_goods_desc,a.is_fact,a.is_purchase,
  a.customer_no,a.customer_name,a.dc_code,a.dc_name,a.province_name,
  a.sales_qty,a.fact_value,a.cost_value,a.purchase_value,a.middle_office_value,a.sales_value,
  a.fact_price,a.cost_price,a.purchase_price,a.middle_office_price,a.sales_price,a.tprorate,a.front_prorate,
  b.fact_price_std,b.cost_std,b.purchase_price_std,b.sale_std,
  a.sales_value-b.sale_std*a.sales_qty diff_sale,--当前销售额与核准销售额的差异
  a.sales_price-b.sale_std diff_sale_price,  --销售价格偏差较大
  --销售价格差%
  case when b.sale_std  is null or b.sale_std<0 then 0 else (a.sales_price-b.sale_std)/b.sale_std end add_sale_rate,
  --工厂加价率 
  case when b.fact_price_std is not null and a.is_fact='是' then a.cost_price/b.fact_price_std else 0 end add_gc_rate,
  --中台加价率 
  case when b.cost_std is null then 0 else (a.middle_office_price-b.cost_std)/b.cost_std end add_zt_rate,
  --成本价对比
  case when b.cost_std is null then 0 else a.cost_price/b.cost_std end diff_cost,
  --采购报价对比
  case when b.purchase_price_std is null then 0 else a.purchase_price/b.purchase_price_std end diff_purchase,
  --历史毛利率
  case when b.sale_std is null or b.sale_std<0 then 0 else 1-b.cost_std/b.sale_std end prorate_std,
  --总毛利率-前台毛利率=中台毛利率
  tprorate-front_prorate zt_prorate
from 
  (
  select *,
    fact_value/sales_qty fact_price,
    cost_value/sales_qty cost_price,
    purchase_value/sales_qty purchase_price,
    middle_office_value/sales_qty middle_office_price,
    sales_value/sales_qty sales_price,
    1-cost_value/sales_value tprorate,
    1-middle_office_value/sales_value front_prorate	
  from csx_tmp.tmp_goods_salezp where sdt=${hiveconf:current_day}
  )a 
--一段时间内（近14天历史）各平均价格
join 
  (
  select province_name,goods_code,
    sum(sales_qty) sales_qty,sum(cost_value) cost_value,
    sum(sales_value) sales_value,
	--平均原料价
    sum(if(is_fact='是',fact_value,0))/sum(if(is_fact='是',sales_qty,0)) fact_price_std,
	--平均成本价
    sum(cost_value)/sum(sales_qty) cost_std,
	--平均采购报价
	sum(if(is_purchase='是',purchase_value,0))/sum(if(is_purchase='是',sales_qty,0)) purchase_price_std,	
	--平均售价
    sum(sales_value)/sum(sales_qty) sale_std
  from 
  --临时表1中订单批次粒度，取订单维度数据，再算平均价
    (select credential_no,goods_code,province_name,is_purchase,is_fact,
	  max(sales_qty) sales_qty,
	  max(fact_value) fact_value,
	  max(cost_value) cost_value,
	  max(purchase_value) purchase_value,
	  max(sales_value) sales_value
    from csx_tmp.tmp_goods_salezp where sdt<${hiveconf:current_day}
	group by credential_no,goods_code,province_name,is_purchase,is_fact
    )a
  group by province_name,goods_code
  )b on (a.province_name=b.province_name and a.goods_code=b.goods_code)
where a.sales_value<>0;

--临时表3：销售关联工厂采购入库各环节异常标签
--结果表
--drop table csx_tmp.tmp_goods_res02; 
--create table csx_tmp.tmp_goods_res02
--as
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_tmp.tmp_sale_detail_abnormal_label partition(sdt)
select 
  concat_ws('&', a.credential_no,a.batch_no,a.goods_code,a.sdt) as id,
  a.credential_no,a.batch_no,a.division_code,a.division_name,a.department_id,a.department_name,
  a.classify_middle_code,a.classify_middle_name,a.goods_code,a.goods_name,a.unit_name,
  a.is_fact,a.is_purchase,a.customer_no,a.customer_name,a.dc_code,a.dc_name,a.province_name,
  a.sales_qty,a.fact_value,a.cost_value,a.purchase_value,a.middle_office_value,a.sales_value,
  a.fact_price,a.cost_price,a.purchase_price,a.middle_office_price,a.sales_price,
  --销售价格异常高:销售价格比历史均价高10个百分点，同时毛利率高于0.3，前台毛利高于0.1; 或者总销售额差异大于5千元，总毛利率大于0
  case when (a.add_sale_rate>0.1 and a.tprorate>0.3 and a.front_prorate>0.1)or (a.diff_sale>5000 and a.tprorate>0) then 1 else 0 end high_sale_price,
  --中台报价异常高:总毛利率-前台毛利率高于0.2，工厂加价率大于0
  case when a.zt_prorate>0.2 and (a.is_fact is null or a.add_gc_rate>0) then 1 else 0 end high_zt_price,
  --成本价异常高:成本价是历史成本价的1.2及以上，且历史的毛利率低于30%
  case when a.diff_cost>1.2 and a.prorate_std<0.3 then 1 else 0 end high_cost_pice,
  --销售价格异常低:销售价格比历史均价低10个百分点，同时总毛利率低于0，前台毛利率低于0；或者销售总额差异小于-5千元，毛利率小于0
  case when (a.add_sale_rate<-0.1 and a.tprorate<0 and a.front_prorate<0) or (a.diff_sale<-5000 and a.tprorate<0) then 1 else 0 end low_sale_price,
  --中台报价异常低:总毛利率-前台毛利率低于-0.1，且中台加价率小于0
  case when a.zt_prorate<-0.1 and a.add_zt_rate<0 then 1 else 0 end low_zt_price,
  --成本价异常低:工厂加价率小于0，毛利率高于0.5；或总毛利率高于0.5，成本价低于历史平均成本价80%
  case when (a.add_gc_rate<0 and a.tprorate>0.5) or (a.tprorate>0.5 and a.diff_cost<=0.8) then 1 else 0 end low_cost_price,
  --工厂加价异常高:工厂加价率大于30%
  case when a.add_gc_rate>0.3 then 1 else 0 end high_gc_price,
  --工厂加价异常低:工厂加价率小于5%
  case when a.add_gc_rate<0.05 then 1 else 0 end low_gc_price,
  --采购报价异常高:采购报价/历史采购报价>1.2
  case when a.diff_purchase>1.2 then 1 else 0 end high_purchase_price,
  --采购报价异常低:采购报价/历史采购报价<0.8
  case when a.diff_purchase<0.8 then 1 else 0 end low_purchase_price,
  --d.received_qty_ls,d.received_value_ls,d.received_price_ls,
  --d.received_qty_last,d.received_value_last,d.received_price_last,
  --d.received_qty_yc,d.received_value_yc,d.received_price_yc,
  d.received_price_hight, --入库价异常高
  d.received_price_low, --入库价异常低
  d.received_price_up, --入库价突涨
  d.received_price_down,  --入库价突降  
  a.sdt  
from csx_tmp.tmp_goods_res a
--采购入库到入库批次明细  可能存在一个工单号一个入库批次对应多个采购入库单号情况
left join 
  (
  select distinct goods_code,credential_no,batch_no,
  received_qty_ls,received_value_ls,received_price_ls,
  received_qty_last,received_value_last,received_price_last,
  received_qty_yc,received_value_yc,received_price_yc,
  received_price_hight, --入库价异常高
  received_price_low, --入库价异常低
  received_price_up, --入库价突涨
  received_price_down,  --入库价突降
  row_number() over (partition by goods_code,credential_no,batch_no order by received_price_hight) as cn1
  from csx_tmp.tmp_factory_order_to_scm_13
  )d on a.goods_code=d.goods_code and a.credential_no=d.credential_no and a.batch_no=d.batch_no and d.cn1=1
;




/*
---------------------------------------------------------------------------------------------------------
---------------------------------------------hive 建表语句-----------------------------------------------
--销售关联工厂采购入库各环节异常标签 csx_tmp.tmp_sale_detail_abnormal_label

drop table if exists csx_tmp.tmp_sale_detail_abnormal_label;
CREATE TABLE `csx_tmp.tmp_sale_detail_abnormal_label`(
  `id` string COMMENT '唯一id',
  `credential_no` string COMMENT '成本核算凭证号',
  `batch_no` string COMMENT '成本批次号',
  `division_code` string COMMENT '部类编码',
  `division_name` string COMMENT '部类名称',
  `department_id` string COMMENT '课组编码',
  `department_name` string COMMENT '课组名称',
  `classify_middle_code` string COMMENT '管理中类编码',
  `classify_middle_name` string COMMENT '管理中类名称',
  `goods_code` string COMMENT '商品编码(业务主键)',
  `goods_name` string COMMENT '商品名称',
  `unit_name` string COMMENT '单位名称',
  `is_fact` string COMMENT '是否加工',
  `is_purchase` string COMMENT '是否有采购报价',
  `customer_no` string COMMENT '客户编号',
  `customer_name` string COMMENT '客户名称',
  `dc_code` string COMMENT '库存地点编码',
  `dc_name` string COMMENT '库存地点名称',
  `province_name` string COMMENT '战报省区名称',
  `sales_qty` string COMMENT '销售数量',
  `fact_value` string COMMENT '原料成本',
  `cost_value` string COMMENT '销售成本',
  `purchase_value` string COMMENT '采购报价金额',
  `middle_office_value` string COMMENT '中台报价金额',
  `sales_value` string COMMENT '含税销售额',
  `fact_price` string COMMENT '原料价',
  `cost_price` string COMMENT '成本价',
  `purchase_price` string COMMENT '采购报价',
  `middle_office_price` string COMMENT '中台报价',
  `sales_price` string COMMENT '正常含税售价',
  `high_sale_price` string COMMENT '销售价格异常高',
  `high_zt_price` string COMMENT '中台报价异常高',
  `high_cost_pice` string COMMENT '成本价异常高',
  `low_sale_price` string COMMENT '销售价格异常低',
  `low_zt_price` string COMMENT '中台报价异常低',
  `low_cost_price` string COMMENT '成本价异常低',
  `high_gc_price` string COMMENT '工厂加价异常高',
  `low_gc_price` string COMMENT '工厂加价异常低',
  `high_purchase_price` string COMMENT '采购报价异常高',
  `low_purchase_price` string COMMENT '采购报价异常低',
  `received_price_hight` string COMMENT '入库价异常高',
  `received_price_low` string COMMENT '入库价异常低',
  `received_price_up` string COMMENT '入库价突涨',
  `received_price_down` string COMMENT '入库价突降'
) COMMENT '销售关联工厂采购入库各环节异常标签'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;






