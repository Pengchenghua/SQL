

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

drop table csx_tmp.tmp_cbgb_tz_jlc;
create temporary table csx_tmp.tmp_cbgb_tz_jlc 
as 
select 
	a.province_code,
	a.province_name,
	a.city_code,
	a.city_name,
	a.location_code,
	a.cost_center_code,
	a.product_code,
	b.goods_name product_name,
	b.dept_id,
	b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(amount)amount
from
	(select 
		a.*,
		case when a.location_code='W0H4' then '-' else b.province_code end province_code,
		case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
		case when a.location_code='W0H4' then '-' else b.city_code end city_code,
		case when a.location_code='W0H4' then '供应链' else b.city_name end city_name,
		b.shop_id,b.shop_name
	from (select * from csx_ods.source_mms_r_a_factory_report_no_share_product
	--where sdt='20200606'
	where sdt=regexp_replace(date_sub(current_date,0),'-','') 
	and period in(substr(${hiveconf:i_sdate_12},1,7)))a  --'2020-05'
	left join 
		(select 
			shop_id,
			shop_name,
			province_code,
			province_name,
			city_group_code as city_code,
			city_group_name as city_name
		from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
	) a
left join 
	(select regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') goods_name,
			goods_id,
			department_id dept_id,
			department_name dept_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id
left join
	(select
		workshop_code, 
		province_code, 
		goods_code
	from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
	where sdt='current' and new_or_old=1
	)d on a.province_code=d.province_code and a.product_code=d.goods_code
group by a.province_code,a.province_name,a.city_code,a.city_name,
a.location_code,a.cost_center_code,a.product_code,b.goods_name,b.dept_id,b.dept_name,
if(d.workshop_code is null,'不是工厂商品','是工厂商品');



--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_jlc partition(sdt)
select 
	province_code,province_name,city_code,city_name,
	location_code,cost_center_code,
	product_code,product_name,dept_id,dept_name,is_factory_goods_name,
	amount,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_jlc;




/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.cbgb_tz_m_cbgb_jlc  财报管报调整成本-价量差

drop table if exists csx_dw.cbgb_tz_m_cbgb_jlc;
create table csx_dw.cbgb_tz_m_cbgb_jlc(
  `province_code` string COMMENT  '省区编号',
  `province_name` string COMMENT  '省区',
  `city_code` string COMMENT '城市编号',  
  `city_name` string COMMENT '城市',
  `location_code` string COMMENT 'DC编码',
  `cost_center_code` string COMMENT '成本中心编码',
  `product_code` string COMMENT '商品编码', 
  `product_name` string COMMENT '商品名称',    
  `dept_id` string COMMENT '课组编号',
  `dept_name` string COMMENT '课组名称',
  `is_factory_goods_name` string COMMENT '是否工厂商品',  
  `amount` decimal(26,6) COMMENT '金额'  
) COMMENT '财报管报调整成本-价量差'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

--------------------------------- mysql建表语句 -------------------------------
drop table if exists `cbgb_tz_m_cbgb_jlc`;
create table `cbgb_tz_m_cbgb_jlc`(
  `province_code` varchar(64) DEFAULT NULL COMMENT  '省区编号',
  `province_name` varchar(64) DEFAULT NULL COMMENT  '省区',
  `city_code` varchar(64) DEFAULT NULL COMMENT '城市编号',  
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市',
  `location_code` varchar(64) DEFAULT NULL COMMENT 'DC编码',
  `cost_center_code` varchar(64) DEFAULT NULL COMMENT '成本中心编码',
  `product_code` varchar(64) DEFAULT NULL COMMENT '商品编码', 
  `product_name` varchar(64) DEFAULT NULL COMMENT '商品名称',    
  `dept_id` varchar(64) DEFAULT NULL COMMENT '课组编号',
  `dept_name` varchar(64) DEFAULT NULL COMMENT '课组名称',
  `is_factory_goods_name` varchar(64) DEFAULT NULL COMMENT '是否工厂商品',  
  `amount` decimal(26,6) COMMENT '金额',  
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='财报管报调整成本-价量差';



select province_name,
	sum(amount) amount
from csx_dw.cbgb_tz_m_cbgb_jlc
where sdt='202005'
group by province_name;


*/


