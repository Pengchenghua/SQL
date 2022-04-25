-- 统计日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');
set current_day1 = date_sub(current_date, 1);
--前3天
set current_day_3bf = regexp_replace(date_sub(current_date, 1+3),'-','');
-- 14天前
set current_start_day = regexp_replace(date_sub(current_date, 1+14),'-','');
set current_start_day1 = date_sub(current_date, 1+14);
-- 库存操作起始日期
set wms_start_day = regexp_replace(add_months(trunc(date_sub(current_date, 1), 'MM'), -11),'-','');





--临时表1：明细数据 采购入库表中取到采购单商品维度数据+采购量 采购单价，关联凭证号获取销售相关数据，为最细粒度明细数据
drop table csx_tmp.tmp_goods_received;
create temporary table csx_tmp.tmp_goods_received
as
select 
  aa.source_type,
  super_class,
  aa.sdt scm_sdt,
  aa.order_code,
  ff.province_code DC_province_code,--省区编码
  ff.province_name DC_province_name,--省区
  ff.city_group_code DC_city_group_code,--城市组编码
  ff.city_group_name DC_city_group_name,--城市组
  aa.target_location_code DC_DC_code, --DC编码
  ff.shop_name DC_DC_name,  --DC名称
  aa.goods_code,--商品编码
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
  sum(received_qty) received_qty,
  sum(coalesce(aa.received_price,0)*aa.received_qty) received_value --采购入库金额
from 
  --采购入库表
  (
    select 
      if(sdt='19990101',regexp_replace(substr(order_time,1,10),'-',''),sdt) sdt,
      goods_code,order_code,target_location_code,source_type,super_class,
	  sum(received_qty) as received_qty,
	  sum(received_price1*received_qty)/sum(received_qty) received_price	
	from csx_dw.dws_scm_r_d_order_received
    where header_status in (3,4)  --3入库中4已完成
    --and ((sdt>='20210401' and sdt<='20210410' ) or (sdt='19990101' and order_time>'2021-04-01' and  order_time<'2021-04-11'))
	and ((sdt>=${hiveconf:current_start_day} and sdt<=${hiveconf:current_day}) 
	  or (sdt='19990101' and order_time>${hiveconf:current_start_day1} and  order_time<${hiveconf:current_day1}))	
    and super_class in (1,3)   --1 供应商订单 
	and source_type<>4 --剔除项目合伙人
	and local_purchase_flag='0'--剔除地采，是否地采(0-否、1-是)
   group by if(sdt='19990101',regexp_replace(substr(order_time,1,10),'-',''),sdt),
     goods_code,order_code,super_class,target_location_code,source_type
  )aa
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt = 'current'
  )ff on ff.shop_id = aa.target_location_code	  
  --商品维表
  left outer join 
  (
    select *
    from csx_dw.dws_basic_w_a_csx_product_m 
    where sdt = 'current'
  )e on e.goods_id = aa.goods_code	 
where  ff.province_name in('重庆市','安徽省')
group by 
  aa.source_type,
  aa.super_class,
  aa.sdt,
  aa.order_code,
  ff.province_code,--省区编码
  ff.province_name,--省区
  ff.city_group_code,--城市组编码
  ff.city_group_name,--城市组
  aa.target_location_code, --DC编码
  ff.shop_name ,  --DC名称
  aa.goods_code,--商品编码
  e.goods_name,--商品名称
  e.unit,--单位
  e.unit_name,--单位名称
  e.department_id ,--课组编码
  e.department_name ,--课组名称
  e.classify_middle_code,--管理中类编码
  e.classify_middle_name,--管理中类名称
  case when e.division_code in ('10','11') then '11'
  	   when e.division_code in ('12','13','14','15') then '12'
  	   else '' end, --部类编码 
  case when e.division_code in ('10','11') then '生鲜'
  	   when e.division_code in ('12','13','14','15') then '食百'
  	   else '' end; --部类名称
   

  
    
--临时表2：当日采购入库订单明细+采购入库异常标签 
--结果表1标签1
drop table csx_tmp.tmp_goods_received_d;
create table csx_tmp.tmp_goods_received_d
as
select a.*,
  b.received_qty_ls,b.received_value_ls,b.received_price_ls,
  c.received_qty_last,c.received_value_last,c.received_price_last,
  d.received_qty_yc,d.received_value_yc,d.received_price_yc,
 --入库价异常高:入库价是历史入库价的1.2倍以上，或是竞争对手入库价的1.2倍以上 
  case when b.received_price_ls is not null and a.received_price/b.received_price_ls>1.2 then 1
       when d.received_price_yc is not null and a.received_price/d.received_price_yc>1.2 then 1
	   else 0 end as received_price_hight,
 --入库价异常低：入库价是历史入库价的0.8以下，或是竞争对手入库价的0.8倍以下	   
  case when b.received_price_ls is not null and a.received_price/b.received_price_ls<0.8 then 1
       when d.received_price_yc is not null and a.received_price/d.received_price_yc<0.8 then 1
	   else 0 end as received_price_low,
 --入库价突涨：入库价/前一日入库价>1.2，且入库价/历史入库价>1.1
  case when c.received_price_last is not null and a.received_price/c.received_price_last>1.2 
         and b.received_price_ls is not null and a.received_price/b.received_price_ls>1.1 then 1
	   else 0 end as received_price_up,
 --入库价突降：入库价/前一日入库价<0.8，且入库价/历史入库价<0.9		   
  case when c.received_price_last is not null and a.received_price/c.received_price_last<0.8
         and b.received_price_ls is not null and a.received_price/b.received_price_ls<0.9 then 1
	   else 0 end as received_price_down	   
from
( 
select distinct source_type,super_class,scm_sdt,order_code,
  DC_province_code,DC_province_name,DC_city_group_code,DC_city_group_name,DC_DC_code,DC_DC_name,
  goods_code,goods_name,unit,department_id,department_name,classify_middle_code,classify_middle_name,division_code,division_name,
  received_qty,received_value,
  received_value/received_qty as received_price
from csx_tmp.tmp_goods_received 
where scm_sdt=${hiveconf:current_day}
)a 
--历史各仓库每个商品的采购入库单价
left join 
(
select 
  goods_code,
  DC_DC_code,
  sum(received_qty) as received_qty_ls,
  sum(received_value) received_value_ls,
  sum(received_value)/sum(received_qty) received_price_ls	
from csx_tmp.tmp_goods_received
where scm_sdt<${hiveconf:current_day}
group by goods_code,DC_DC_code
)b on b.goods_code=a.goods_code and b.DC_DC_code=a.DC_DC_code
--最近一次入库价
left join 
(
  select 
    goods_code,
    DC_DC_code,
    sum(received_qty) as received_qty_last,
    sum(received_value) received_value_last,
    sum(received_value)/sum(received_qty) received_price_last
  from 
    (
    select *,
    rank() over (partition by goods_code,DC_DC_code order by scm_sdt desc ) as cn1  
    from csx_tmp.tmp_goods_received
    where scm_sdt<${hiveconf:current_day}
	)a 
	where a.cn1=1
  group by goods_code,DC_DC_code
)c on c.goods_code=a.goods_code and b.DC_DC_code=a.DC_DC_code
--近3天永辉入库价
left join 
  (
  select
    a.goods_code,
    sum(a.received_qty_yc) received_qty_yc,
    sum(a.received_value_yc) received_value_yc,
    sum(a.received_value_yc)/sum(a.received_qty_yc) received_price_yc	 
  from
  (
    select
    	shop_id_in,
    	goodsid goods_code,
    	pur_doc_id,
  	sum(pur_qty_in) received_qty_yc,
  	sum(tax_pur_val_in) received_value_yc,
  	sum(tax_pur_val_in)/sum(pur_qty_in) received_price_yc
    from b2b.ord_orderflow_t 
    where sdt>=${hiveconf:current_day_3bf} and sdt<${hiveconf:current_day}
    	and pur_qty_in>0 
    	and tax_pur_val_in >0  --剔除入库金额为 0 的商品		
    	and ordertype not in ('返配','退货') 
    	and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
  	group by shop_id_in,goodsid,pur_doc_id
  )a
  join
  (
    select distinct
    	shop_id as shop_id, 
    	city_name as city_name,
    	province_name as province_name,
    	dept_id_channel,
    	case when shop_channel ='csx' then '彩食鲜' else '云超' end stype
    from 
    	csx_dw.ads_sale_r_d_purprice_globaleye_shop
    where 
    	sdt='current' 
    	and  province_name in('重庆市','安徽省')
  ) as d on a.shop_id_in=d.shop_id--拿到云超的数据
  group by a.goods_code
 )d on d.goods_code=a.goods_code;






