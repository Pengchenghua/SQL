

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

--后台收入明细
drop table csx_tmp.cbgb_tz_m_cbgb_htsr;
create table csx_tmp.cbgb_tz_m_cbgb_htsr
as 
select 
	case when a.settle_place_code='W0H4' then '-' else b.province_code end province_code,
	case when a.settle_place_code='W0H4' then '供应链' else b.province_name end province_name,
	case when a.settle_place_code='W0H4' then '-' else b.city_code end city_code,
	case when a.settle_place_code='W0H4' then '供应链' else b.city_name end city_name,
	a.settle_no,a.agreement_no,a.settle_date,a.purchase_org_code,a.purchase_org_name,a.purchase_code dept_id,a.purchase_name dept_name,a.cost_code,a.cost_name,
	a.attribution_date,a.supplier_code,a.supplier_name,a.settle_place_code,a.settle_place_name,a.company_code,a.company_name,
	a.net_value,a.tax_amount,a.value_tax_total,a.bill_total_amount,a.invoice_code,a.invoice_name,substr(${hiveconf:i_sdate_22},1,6) as sdt
from 
( select * from csx_ods.settle_settle_bill_ods 
where sdt='19990101'
and attribution_date >= ${hiveconf:i_sdate_12} 
and attribution_date < ${hiveconf:i_sdate_11} )a
left join (select shop_id,shop_name,province_code,province_name,city_group_code as city_code,city_group_name as city_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code;

drop table csx_tmp.cbgb_tz_m_cbgb_htsr_2;
create temporary table csx_tmp.cbgb_tz_m_cbgb_htsr_2
as 
select 
	case when cost_name like '目标返利%' then '目标返利'
		when cost_name like '仓储服务费%' then '仓储服务费'  
		else cost_name end cost_name ,
	province_code,province_name,city_code,city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settle_place_code,settle_place_name,sdt,
	sum( net_value) net_value,sum( value_tax_total) value_tax_total
from csx_tmp.cbgb_tz_m_cbgb_htsr
group by case when cost_name like '目标返利%' then '目标返利'
			when cost_name like '仓储服务费%' then '仓储服务费'  
			else cost_name end,
	province_code,province_name,city_code,city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settle_place_code,settle_place_name,sdt;
	
	

--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_htsr partition(sdt) 
select 
	cost_name,province_code,province_name,city_code,city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settle_place_code,settle_place_name,net_value,value_tax_total,sdt
from csx_tmp.cbgb_tz_m_cbgb_htsr_2;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.cbgb_tz_m_cbgb_htsr  财报管报调整成本-后台收入

drop table if exists csx_dw.cbgb_tz_m_cbgb_htsr;
create table csx_dw.cbgb_tz_m_cbgb_htsr(
  `cost_name` string COMMENT '费用名称',  
  `province_code` string COMMENT '省区编号',
  `province_name` string COMMENT '省区',
  `city_code` string COMMENT '城市编号',  
  `city_name` string COMMENT '城市',  
  `dept_id` string COMMENT '课组编号',
  `dept_name` string COMMENT '课组名称', 
  `supplier_code` string COMMENT 'DC编码',
  `supplier_name` string COMMENT  'DC名称', 
  `settle_place_code` string COMMENT '供应商编码',
  `settle_place_name` string COMMENT '供应商名称',  
  `net_value` decimal(26,6)  COMMENT  '净价值',
  `value_tax_total` decimal(26,6)  COMMENT  '价税合计'
) COMMENT '财报管报调整成本-后台收入'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

--------------------------------- mysql建表语句 -------------------------------
drop table if exists `cbgb_tz_m_cbgb_htsr`;
create table `cbgb_tz_m_cbgb_htsr`(
  `cost_name` varchar(64) DEFAULT NULL COMMENT '费用名称',  
  `province_code` varchar(64) DEFAULT NULL COMMENT '省区编号',
  `province_name` varchar(64) DEFAULT NULL COMMENT '省区',
  `city_code` varchar(64) DEFAULT NULL COMMENT '城市编号',  
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市',  
  `dept_id` varchar(64) DEFAULT NULL COMMENT '课组编号',
  `dept_name` varchar(64) DEFAULT NULL COMMENT '课组名称',
  `supplier_code` varchar(64) DEFAULT NULL COMMENT 'DC编码',
  `supplier_name` varchar(64) DEFAULT NULL COMMENT 'DC名称',  
  `settle_place_code` varchar(64) DEFAULT NULL COMMENT '供应商编码',
  `settle_place_name` varchar(64) DEFAULT NULL COMMENT '供应商名称',  
  `net_value` decimal(26,6)  COMMENT  '净价值',
  `value_tax_total` decimal(26,6)  COMMENT  '价税合计,  
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='财报管报调整成本-后台收入';


select province_name,
	sum(net_value) net_value
from csx_dw.cbgb_tz_m_cbgb_htsr
where sdt='202005'
group by province_name;


*/


