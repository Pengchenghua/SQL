--Z68返利;Z69调价

--①1、对抵负库存成本调整
--①2、采购退货金额差异调整
--①3、工厂月末分摊-调整销售
--①4、工厂月末分摊-调整跨公司调拨
--①5、工厂月末分摊-调整其他
--①6、手工调整销售成本
-- 7、价量差工厂未使用的商品
-- 8、工厂分摊后成本小于0，未分摊金额
-- 9、报损
--★10、盘盈(盘盈用负数表示，表示减成本）
--★11、盘亏
-- 12、后台收入
-- 13、后台支出

------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

drop table csx_tmp.tmp_cbgb_tz_htzh;
create temporary table csx_tmp.tmp_cbgb_tz_htzh 
as 
select 
	a.adjust_reason,a.dc_code,a.dc_name,
	case when a.dc_code='W0H4' then '-' else a.province_code end province_code,
	case when a.dc_code='W0H4' then '供应链' else a.province_name end province_name,
	case when a.dc_code='W0H4' then '-' else a.city_code end city_code,
	case when a.dc_code='W0H4' then '供应链' else a.city_name end city_name,
	c.channel_name,a.goods_code as product_code,a.goods_name as product_name,e.dept_id,e.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(sales_value/(1+tax_rate/100)) amt_no_tax,
	sum(sales_value) amt
from
	(select a.*
	from csx_dw.dwd_csms_r_d_rebate_order a
	where a.order_type_code in ('0','1')
	and a.commit_time>=${hiveconf:i_sdate_12}
	and a.commit_time<${hiveconf:i_sdate_11}
	and a.order_status='1')a	
--from
--	(select a.adjust_reason,a.dc_code,a.dc_name,a.customer_no,a.customer_name,
--		a.goods_code,a.goods_name,a.sales_value,a.tax_rate,
--		b.province_code,b.province_name,b.city_code,b.city_name
--	from csx_dw.dwd_csms_r_d_rebate_order a
--	left join --省区
--		(select 
--			shop_id,shop_name,province_code,province_name,city_code,city_name
--		from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.dc_code
--	where a.order_type_code in ('0','1')
--	and a.commit_time>=${hiveconf:i_sdate_12}
--	and a.commit_time<${hiveconf:i_sdate_11}
--	and a.order_status='1')a	
left join --渠道
(
  select * from csx_dw.dws_crm_w_a_customer
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '') 
) c on a.customer_no = c.customer_no
left join --是否工厂商品
	(select
		workshop_code, province_code, goods_code
	  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
	  where sdt='current' and new_or_old=1
	)d on a.province_code=d.province_code and a.goods_code=d.goods_code
left join --课组
	(select goods_id,goods_name,department_id dept_id,department_name dept_name
		from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )e on a.goods_code=e.goods_id
group by a.adjust_reason,a.dc_code,a.dc_name,
	case when a.dc_code='W0H4' then '-' else a.province_code end,
	case when a.dc_code='W0H4' then '供应链' else a.province_name end,
	case when a.dc_code='W0H4' then '-' else a.city_code end,
	case when a.dc_code='W0H4' then '供应链' else a.city_name end,
c.channel_name,a.goods_code,a.goods_name,e.dept_id,e.dept_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品') ;


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_htzh partition(sdt)
select adjust_reason,dc_code as inventory_dc_code,dc_name as inventory_dc_name,
	province_code,province_name,city_code,city_name,
	channel_name,product_code,product_name,dept_id,dept_name,is_factory_goods_name,
	amt_no_tax,amt,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_htzh;




/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.cbgb_tz_m_cbgb_htzh  财报管报调整成本-后台支出

drop table if exists csx_dw.cbgb_tz_m_cbgb_htzh;
create table csx_dw.cbgb_tz_m_cbgb_htzh(
 `adjust_reason` string COMMENT '调整原因',
 `inventory_dc_code` string COMMENT '库存dc编码',
 `inventory_dc_name` string COMMENT '库存dc名称',
  `province_code` string COMMENT  '省区编号',
  `province_name` string COMMENT  '省区',
  `city_code` string COMMENT '城市编号',  
  `city_name` string COMMENT '城市',
  `channel_name` string COMMENT '渠道',
  `product_code` string COMMENT '商品编码', 
  `product_name` string COMMENT '商品名称',    
  `dept_id` string COMMENT '课组编号',
  `dept_name` string COMMENT '课组名称',
  `is_factory_goods_name` string COMMENT '是否工厂商品',  
  `amt_no_tax` decimal(26,6) COMMENT '不含税金额',
  `amt` decimal(26,6) COMMENT '含税金额'  
) COMMENT '财报管报调整成本-后台支出'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

--------------------------------- mysql建表语句 -------------------------------
drop table if exists `cbgb_tz_m_cbgb_htzh`;
create table `cbgb_tz_m_cbgb_htzh`(
 `adjust_reason` varchar(64) DEFAULT NULL COMMENT '调整原因',
 `inventory_dc_code` varchar(64) DEFAULT NULL COMMENT '库存dc编码',
 `inventory_dc_name` varchar(64) DEFAULT NULL COMMENT '库存dc名称',
  `province_code` varchar(64) DEFAULT NULL COMMENT  '省区编号',
  `province_name` varchar(64) DEFAULT NULL COMMENT  '省区',
  `city_code` varchar(64) DEFAULT NULL COMMENT '城市编号',  
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市',
  `channel_name` varchar(64) DEFAULT NULL COMMENT '渠道',
  `product_code` varchar(64) DEFAULT NULL COMMENT '商品编码', 
  `product_name` varchar(64) DEFAULT NULL COMMENT '商品名称',    
  `dept_id` varchar(64) DEFAULT NULL COMMENT '课组编号',
  `dept_name` varchar(64) DEFAULT NULL COMMENT '课组名称',
  `is_factory_goods_name` varchar(64) DEFAULT NULL COMMENT '是否工厂商品',  
  `amt_no_tax` decimal(26,6) COMMENT '不含税金额',
  `amt` decimal(26,6) COMMENT '含税金额',  
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='财报管报调整成本-后台支出';



select 
	sum(amt_no_tax) amt_no_tax,sum(amt) amt
from csx_dw.cbgb_tz_m_cbgb_htzh
where sdt='202005';


*/


