

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

drop table csx_tmp.tmp_cbgb_tz_bs;
create table csx_tmp.tmp_cbgb_tz_bs
as 
select a.province_code,
		a.province_name,
		a.city_code,
		a.city_name,
		a.location_code,a.location_name,
		a.posting_time               ,
		a.wms_order_no               ,
		a.wms_biz_type               ,
		a.wms_biz_type_name          ,
		a.credential_no              ,
		a.purchase_group_code        ,
		a.purchase_group_name        ,
		b.dept_id,b.dept_name,
		a.product_code               ,
		a.product_name               ,
		a.unit                       ,
		a.qty                        ,
		a.price_no_tax               ,
		a.amt_no_tax                 ,
		a.amt                        ,
		a.fac_adjust_amt_no_tax      ,
		a.negative_adjust_amt_no_tax ,
		a.remedy_adjust_amt_no_tax   ,
		a.manual_adjust_amt_no_tax   ,
		a.cost_amt_no_tax            ,
		a.company_code               ,
		a.company_name               ,
		a.cost_center_code           ,
		a.cost_center_name           ,
		a.small_category_code        ,
		a.small_category_name        ,
		a.reservoir_area_code        ,
		a.reservoir_area_name        ,
		a.reservoir_area_prop        
from
(select a.*,
case when a.location_code='W0H4' then '-' else b.province_code end province_code,
case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
case when a.location_code='W0H4' then '-' else b.city_code end city_code,
case when a.location_code='W0H4' then '供应链' else b.city_name end city_name,
b.shop_id,b.shop_name
from (select a.*
from csx_ods.source_sync_r_d_data_sync_broken_item a
where a.sdt = '19990101'
and (( a.wms_biz_type <>'64' and a.reservoir_area_prop = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
or a.wms_biz_type = '64' )
and a.posting_time >= ${hiveconf:i_sdate_12} 
and a.posting_time < ${hiveconf:i_sdate_11}) a 
left join 
(select shop_id,shop_name,province_code,province_name,city_group_code as city_code,city_group_name as city_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
) a
left join 
(select goods_id,goods_name,department_id dept_id,department_name dept_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )b on a.product_code=b.goods_id;




--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_bs partition(sdt) 
select 
	province_code,province_name,city_code,city_name,
	location_code,location_name,
	posting_time,
	wms_order_no,
	wms_biz_type,
	wms_biz_type_name,
	credential_no,
	purchase_group_code,
	purchase_group_name,	
	dept_id,dept_name,	
	product_code,
	product_name,
	unit,
	qty,
	price_no_tax,
	amt_no_tax,
	amt,
	fac_adjust_amt_no_tax,
	negative_adjust_amt_no_tax,
	remedy_adjust_amt_no_tax,
	manual_adjust_amt_no_tax,
	cost_amt_no_tax,
	company_code,
	company_name,
	cost_center_code,
	cost_center_name,
	small_category_code,
	small_category_name,
	reservoir_area_code,
	reservoir_area_name,
	reservoir_area_prop,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_bs;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.cbgb_tz_m_cbgb_bs  财报管报调整成本-报损

drop table if exists csx_dw.cbgb_tz_m_cbgb_bs;
create table csx_dw.cbgb_tz_m_cbgb_bs(
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区编码',
  `city_code` string COMMENT '城市编码',
  `city_name` string COMMENT '城市',
  `location_code` string COMMENT '地点编码',
  `location_name` string COMMENT '地点名称',
  `posting_time` string COMMENT '过账时间',
  `wms_order_no` string COMMENT '订单号',
  `wms_biz_type` string COMMENT '成本类型编码',
  `wms_biz_type_name` string COMMENT '成本类型',
  `credential_no` string COMMENT '凭证号',
  `purchase_group_code` string COMMENT '商品采购组编码',
  `purchase_group_name` string COMMENT '商品采购组名称',
  `dept_id` string COMMENT '课组编号',
  `dept_name` string COMMENT '课组名称',
  `product_code` string COMMENT '商品编码',
  `product_name` string COMMENT '商品名称',
  `unit` decimal(26,6)  COMMENT  '单位',
  `qty` decimal(26,6)  COMMENT  '数量',
  `price_no_tax` decimal(26,6)  COMMENT  '不含税单价',
  `amt_no_tax` decimal(26,6)  COMMENT  '不含税金额',
  `amt` decimal(26,6)  COMMENT  '含税金额',
  `fac_adjust_amt_no_tax` decimal(26,6)  COMMENT  '工厂倒杂调整成本（不含税）',
  `negative_adjust_amt_no_tax` decimal(26,6)  COMMENT  '负库存调整成本（不含税）',
  `remedy_adjust_amt_no_tax` decimal(26,6)  COMMENT  '价格补救调整成本（不含税）',
  `manual_adjust_amt_no_tax` decimal(26,6)  COMMENT  '手工调整成本（不含税）',
  `cost_amt_no_tax` decimal(26,6)  COMMENT  '成本合计（不含税）',
  `company_code` string COMMENT '公司编码',
  `company_name` string COMMENT '公司名称',
  `cost_center_code` string COMMENT '成本中心编码',
  `cost_center_name` string COMMENT '成本中心名称',
  `small_category_code` string COMMENT '小类编码',
  `small_category_name` string COMMENT '小类名称',
  `reservoir_area_code` string COMMENT '库区编码',
  `reservoir_area_name` string COMMENT '库区名称',
  `reservoir_area_prop` string COMMENT '库区属性'
) COMMENT '财报管报调整成本-报损'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

--------------------------------- mysql建表语句 -------------------------------
drop table if exists `cbgb_tz_m_cbgb_bs`;
create table `cbgb_tz_m_cbgb_bs`(
  `province_code` varchar(64) DEFAULT NULL COMMENT '省区编码',
  `province_name` varchar(64) DEFAULT NULL COMMENT '省区编码',
  `city_code` varchar(64) DEFAULT NULL COMMENT '城市编码',
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市',
  `location_code` varchar(64) DEFAULT NULL COMMENT '地点编码',
  `location_name` varchar(64) DEFAULT NULL COMMENT '地点名称',
  `posting_time` varchar(64) DEFAULT NULL COMMENT '过账时间',
  `wms_order_no` varchar(64) DEFAULT NULL COMMENT '订单号',
  `wms_biz_type` varchar(64) DEFAULT NULL COMMENT '成本类型编码',
  `wms_biz_type_name` varchar(64) DEFAULT NULL COMMENT '成本类型',
  `credential_no` varchar(64) DEFAULT NULL COMMENT '凭证号',
  `purchase_group_code` varchar(64) DEFAULT NULL COMMENT '商品采购组编码',
  `purchase_group_name` varchar(64) DEFAULT NULL COMMENT '商品采购组名称',
  `dept_id` varchar(64) DEFAULT NULL COMMENT '课组编号',
  `dept_name` varchar(64) DEFAULT NULL COMMENT '课组名称',
  `product_code` varchar(64) DEFAULT NULL COMMENT '商品编码',
  `product_name` varchar(64) DEFAULT NULL COMMENT '商品名称',
  `unit` decimal(26,6)  COMMENT  '单位',
  `qty` decimal(26,6)  COMMENT  '数量',
  `price_no_tax` decimal(26,6)  COMMENT  '不含税单价',
  `amt_no_tax` decimal(26,6)  COMMENT  '不含税金额',
  `amt` decimal(26,6)  COMMENT  '含税金额',
  `fac_adjust_amt_no_tax` decimal(26,6)  COMMENT  '工厂倒杂调整成本（不含税）',
  `negative_adjust_amt_no_tax` decimal(26,6)  COMMENT  '负库存调整成本（不含税）',
  `remedy_adjust_amt_no_tax` decimal(26,6)  COMMENT  '价格补救调整成本（不含税）',
  `manual_adjust_amt_no_tax` decimal(26,6)  COMMENT  '手工调整成本（不含税）',
  `cost_amt_no_tax` decimal(26,6)  COMMENT  '成本合计（不含税）',
  `company_code` varchar(64) DEFAULT NULL COMMENT '公司编码',
  `company_name` varchar(64) DEFAULT NULL COMMENT '公司名称',
  `cost_center_code` varchar(64) DEFAULT NULL COMMENT '成本中心编码',
  `cost_center_name` varchar(64) DEFAULT NULL COMMENT '成本中心名称',
  `small_category_code` varchar(64) DEFAULT NULL COMMENT '小类编码',
  `small_category_name` varchar(64) DEFAULT NULL COMMENT '小类名称',
  `reservoir_area_code` varchar(64) DEFAULT NULL COMMENT '库区编码',
  `reservoir_area_name` varchar(64) DEFAULT NULL COMMENT '库区名称',
  `reservoir_area_prop` varchar(64) DEFAULT NULL COMMENT '库区属性', 
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='财报管报调整成本-报损';


select province_name,
	sum(amt_no_tax) amt_no_tax
from csx_dw.cbgb_tz_m_cbgb_bs
where sdt='202005'
group by province_name;


*/


