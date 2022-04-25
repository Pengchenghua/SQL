

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

drop table csx_tmp.tmp_cbgb_tz_pd;
create temporary table csx_tmp.tmp_cbgb_tz_pd 
as 
select a.province_code,a.province_name,a.city_code,a.city_name,
	--c.channel_name,a.cost_center_code,a.cost_center_name,
	a.location_code,a.location_name,a.company_code,a.company_name,a.product_code,
	regexp_replace(regexp_replace(a.product_name,'\n',''),'\r','') product_name,
	b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(case when amt_no_tax>=0 then -amt_no_tax end )  inventory_p_no, --盘盈  
	sum(case when amt_no_tax<0 then -amt_no_tax end )  inventory_l_no, --盘亏

	sum(case when amt>=0 then -amt end )  inventory_p, --盘盈  
	sum(case when amt<0 then -amt end ) inventory_l --盘亏
from
	(select a.*,
		case when a.location_code='W0H4' then '-' else b.city_code end province_code,
		case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
		case when a.location_code='W0H4' then '-' else b.city_code end city_code,
		case when a.location_code='W0H4' then '供应链' else b.city_name end city_name,
		b.shop_id,b.shop_name
	from 
		(select a.*
		from csx_ods.source_sync_r_d_data_sync_inventory_item a
		where a.sdt = '19990101'
		and a.reservoir_area_code = 'PD01' 
		and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) 
		and a.posting_time >= ${hiveconf:i_sdate_12} 
		and a.posting_time < ${hiveconf:i_sdate_11} ) a 
	left join 
		(select shop_id,shop_name,province_code,province_name,city_group_code as city_code,city_group_name as city_name
		from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
	) a
left join 
	(select goods_id,goods_name,department_id dept_id,department_name dept_name
		from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
	(select distinct
		workshop_code,province_code,goods_code
	  from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
	  where sdt='current' and new_or_old=1
	)d on a.province_code=d.province_code and a.product_code=d.goods_code
--left join csx_tmp.tmp_sale_order_flag c on a.wms_order_no=c.order_no and a.product_code=c.goods_code
group by a.province_code,a.province_name,a.city_code,a.city_name,
	--c.channel_name,a.cost_center_code,a.cost_center_name,
	a.location_code,a.location_name,a.company_code,a.company_name,a.product_code,
	regexp_replace(regexp_replace(a.product_name,'\n',''),'\r',''),
	b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品');


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_pd partition(sdt)
select province_code,province_name,city_code,city_name,
	location_code,location_name,company_code,company_name,product_code,product_name,
	dept_id,dept_name,is_factory_goods_name,
	inventory_p_no,inventory_l_no,
	inventory_p,inventory_l,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_pd;



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.cbgb_tz_m_cbgb_pd  财报管报调整成本-盘点

drop table if exists csx_dw.cbgb_tz_m_cbgb_pd;
create table csx_dw.cbgb_tz_m_cbgb_pd(
  `province_code` string COMMENT  '省区编号',
  `province_name` string COMMENT  '省区',
  `city_code` string COMMENT '城市编号',  
  `city_name` string COMMENT '城市',
  `location_code` string COMMENT 'DC编号',
  `location_name` string COMMENT 'DC名称',
  `company_code` string COMMENT '公司编码', 
  `company_name` string COMMENT '公司名称', 
  `product_code` string COMMENT '商品编码', 
  `product_name` string COMMENT '商品名称',    
  `dept_id` string COMMENT '课组编号',
  `dept_name` string COMMENT '课组名称',
  `is_factory_goods_name` string COMMENT '是否工厂商品',  
  `inventory_p_no` decimal(26,6) COMMENT '盘盈_未税',
  `inventory_l_no` decimal(26,6) COMMENT '盘亏_未税',
  `inventory_p` decimal(26,6) COMMENT '盘盈_含税',
  `inventory_l` decimal(26,6)  COMMENT '盘亏_含税'  
) COMMENT '财报管报调整成本-盘点'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

--------------------------------- mysql建表语句 -------------------------------
drop table if exists `cbgb_tz_m_cbgb_pd`;
create table `cbgb_tz_m_cbgb_pd`(
  `province_code` varchar(64) DEFAULT NULL COMMENT '省区编号',
  `province_name` varchar(64) DEFAULT NULL COMMENT '省区',
  `city_code` varchar(64) DEFAULT NULL COMMENT '城市编号',  
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市',
  `location_code` varchar(64) DEFAULT NULL COMMENT 'DC编号',
  `location_name` varchar(64) DEFAULT NULL COMMENT 'DC名称',
  `company_code` varchar(64) DEFAULT NULL COMMENT '公司编码', 
  `company_name` varchar(64) DEFAULT NULL COMMENT '公司名称', 
  `product_code` varchar(64) DEFAULT NULL COMMENT '商品编码', 
  `product_name` varchar(64) DEFAULT NULL COMMENT '商品名称',    
  `dept_id` varchar(64) DEFAULT NULL COMMENT '课组编号',
  `dept_name` varchar(64) DEFAULT NULL COMMENT '课组名称',
  `is_factory_goods_name` varchar(64) DEFAULT NULL COMMENT '是否工厂商品', ,
  `inventory_p_no` decimal(26,6) COMMENT '盘盈_未税',
  `inventory_l_no` decimal(26,6) COMMENT '盘亏_未税',
  `inventory_p` decimal(26,6) COMMENT '盘盈_含税',
  `inventory_l` decimal(26,6)  COMMENT '盘亏_含税' , 
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='财报管报调整成本-盘点';



select 
	sum(inventory_p_no) inventory_p_no,sum(inventory_l_no) inventory_l_no,
	sum(inventory_p) inventory_p,sum(inventory_l) inventory_l
from csx_dw.cbgb_tz_m_cbgb_pd
where sdt='202005';


*/


