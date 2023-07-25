

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

--20200705 6月增加
--①14、采购入库价格补救-调整销售
--①15、采购入库价格补救-调整跨公司调拨
--①16、采购入库价格补救-调整其他

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

--销售订单所在省区渠道
drop table csx_tmp.tmp_sale_order_flag;
create table csx_tmp.tmp_sale_order_flag 
as 
select case when channel_name='业务代理' then '大' else channel_name end channel_name,
province_code,province_name,origin_order_no,order_no,goods_code,
sum(sales_value)sales_value,
sum(excluding_tax_sales)excluding_tax_sales
from csx_dw.dws_sale_r_d_detail
where sdt>=add_months(trunc(date_sub(current_date,1),'MM'),-6)
--and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
--and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
group by case when channel_name='业务代理' then '大' else channel_name end,
	province_code,province_name,origin_order_no,order_no,goods_code;




--1 2 3 4 5 6  成本调整 adjustment_amt_no_tax,adjustment_amt
drop table csx_tmp.tmp_cbgb_tz_v11;
create table csx_tmp.tmp_cbgb_tz_v11 
as 
select case when a.location_code='W0H4' then '-' else b.province_code end province_code,
	case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
	case when a.location_code='W0H4' then '-' else b.city_code end city_code,
	case when a.location_code='W0H4' then '供应链' else b.city_name end city_name,
	a.location_code,a.location_name,
	c.channel_name,d.dept_id,d.dept_name,
	sum(adj_ddfkc_no) adj_ddfkc_no,
	sum(adj_ddfkc) adj_ddfkc,
	sum(adj_cgth_no) adj_cgth_no,
	sum(adj_cgth) adj_cgth,
	sum(adj_gc_xs_no) adj_gc_xs_no,
	sum(adj_gc_xs) adj_gc_xs,
	sum(adj_gc_db_no) adj_gc_db_no,
	sum(adj_gc_db) adj_gc_db,
	sum(adj_gc_qt_no) adj_gc_qt_no,
	sum(adj_gc_qt) adj_gc_qt,
	sum(adj_sg_no) adj_sg_no,
	sum(adj_sg) adj_sg,
	sum(adj_bj_xs_no) adj_bj_xs_no,
	sum(adj_bj_xs) adj_bj_xs,
	sum(adj_bj_db_no) adj_bj_db_no,
	sum(adj_bj_db) adj_bj_db,
	sum(adj_bj_qt_no) adj_bj_qt_no,
	sum(adj_bj_qt) adj_bj_qt
from
	(select item_source_order_no,product_code,location_code,location_name,
		--对抵负库存的成本调整
		case when adjustment_reason='in_remark' then adjustment_amt_no_tax end adj_ddfkc_no,
		case when adjustment_reason='in_remark' then adjustment_amt end adj_ddfkc,
		--采购退货金额差异的成本调整
		case when adjustment_reason='out_remark' then adjustment_amt_no_tax end adj_cgth_no,
		case when adjustment_reason='out_remark' then adjustment_amt end adj_cgth,
		--工厂月末分摊-调整销售订单
		case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
				then adjustment_amt_no_tax end adj_gc_xs_no,
		case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
				then adjustment_amt end adj_gc_xs,		
		--工厂月末分摊-调整跨公司调拨订单
		case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and item_wms_biz_type in('06','07','08','09','12','15','17') )
				then adjustment_amt_no_tax end adj_gc_db_no,
		case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and item_wms_biz_type in('06','07','08','09','12','15','17') )
				then adjustment_amt end adj_gc_db,		
		--工厂月末分摊-调整其他
		case when adjustment_reason in('fac_remark_sale','fac_remark_span')		
				and adjustment_type='sale'
				and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_gc_qt_no,
		case when adjustment_reason in('fac_remark_sale','fac_remark_span')  
				and adjustment_type='sale'
				and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt end adj_gc_qt,		
		--手工调整销售成本
		case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt_no_tax,adjustment_amt_no_tax) end adj_sg_no,
		case when adjustment_reason='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt,adjustment_amt) end adj_sg,
		--采购入库价格补救-调整销售
		case when adjustment_reason = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and item_wms_biz_type in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
				then adjustment_amt_no_tax end adj_bj_xs_no,
		case when adjustment_reason = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and item_wms_biz_type in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
				then adjustment_amt end adj_bj_xs,				
		--采购入库价格补救-调整跨公司调拨	
		case when adjustment_reason = 'pur_remark_remedy'
				and adjustment_type='sale'
				and item_wms_biz_type in ('06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_bj_db_no,
		case when adjustment_reason = 'pur_remark_remedy'
				and adjustment_type='sale'
				and item_wms_biz_type in ('06','07','08','09','12','15','17')
				then adjustment_amt end adj_bj_db,				
		--采购入库价格补救-调整其他
		case when adjustment_reason = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and item_wms_biz_type not in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_bj_qt_no,	
		case when adjustment_reason = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and item_wms_biz_type not in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt end adj_bj_qt
	from 
		(select * 
		from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
		where sdt = '19990101'
		and posting_time >= ${hiveconf:i_sdate_12}
		and posting_time < ${hiveconf:i_sdate_11}
	)a
)a
left join csx_tmp.tmp_sale_order_flag c on a.item_source_order_no=c.order_no and a.product_code=c.goods_code
left join 
	(select shop_id,shop_name,province_code,province_name,city_group_code as city_code,city_group_name as city_name
	from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.location_code
left join 
	(select goods_id,department_id dept_id,department_name dept_name
	from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' )d on a.product_code=d.goods_id
group by case when a.location_code='W0H4' then '-' else b.province_code end,
	case when a.location_code='W0H4' then '供应链' else b.province_name end,
	case when a.location_code='W0H4' then '-' else b.city_code end,
	case when a.location_code='W0H4' then '供应链' else b.city_name end,
	a.location_code,a.location_name,
	c.channel_name,d.dept_id,d.dept_name;

--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_xstz partition(sdt)
select province_code,province_name,city_code,city_name,
	location_code,location_name,channel_name,dept_id,dept_name,
	adj_ddfkc_no,adj_ddfkc,
	adj_cgth_no,adj_cgth,
	adj_gc_xs_no,adj_gc_xs,
	adj_gc_db_no,adj_gc_db,
	adj_gc_qt_no,adj_gc_qt,
	adj_sg_no,adj_sg,
	adj_bj_xs_no,adj_bj_xs,
	adj_bj_db_no,adj_bj_db,
	adj_bj_qt_no,adj_bj_qt,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_v11;



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.cbgb_tz_m_cbgb_xstz  财报管报调整成本-销售调整

drop table if exists csx_dw.cbgb_tz_m_cbgb_xstz;
create table csx_dw.cbgb_tz_m_cbgb_xstz(
  `province_code` string COMMENT  '省区编号',
  `province_name` string COMMENT  '省区',
  `city_code` string COMMENT '城市编号',  
  `city_name` string COMMENT '城市',
  `location_code` string COMMENT 'DC编号',
  `location_name` string COMMENT 'DC名称',
  `channel_name` string COMMENT '渠道',  
  `dept_id` string COMMENT '课组编号',
  `dept_name` string COMMENT '课组名称',
  `adj_ddfkc_no` decimal(26,6) COMMENT '对抵负库存_未税',
  `adj_ddfkc` decimal(26,6) COMMENT '对抵负库存_含税',
  `adj_cgth_no` decimal(26,6) COMMENT '采购退货_未税',
  `adj_cgth` decimal(26,6) COMMENT '采购退货_含税',
  `adj_gc_xs_no` decimal(26,6) COMMENT '工厂月末分摊-调整销售_未税',
  `adj_gc_xs` decimal(26,6) COMMENT '工厂月末分摊-调整销售_含税',
  `adj_gc_db_no` decimal(26,6) COMMENT '工厂月末分摊-调整跨公司调拨_未税',
  `adj_gc_db` decimal(26,6) COMMENT '工厂月末分摊-调整跨公司调拨_含税',
  `adj_gc_qt_no` decimal(26,6) COMMENT '工厂月末分摊-调整其他_未税',
  `adj_gc_qt` decimal(26,6) COMMENT '工厂月末分摊-调整其他_含税',
  `adj_sg_no` decimal(26,6) COMMENT '手工调整销售成本_未税',
  `adj_sg` decimal(26,6)  COMMENT '手工调整销售成本_含税',
  `adj_bj_xs_no` decimal(26,6) COMMENT '采购入库价格补救-调整销售_未税',
  `adj_bj_xs` decimal(26,6) COMMENT '采购入库价格补救-调整销售_含税',
  `adj_bj_db_no` decimal(26,6) COMMENT '采购入库价格补救-调整跨公司调拨_未税',
  `adj_bj_db` decimal(26,6) COMMENT '采购入库价格补救-调整跨公司调拨_含税',
  `adj_bj_qt_no` decimal(26,6) COMMENT '采购入库价格补救-调整其他_未税',
  `adj_bj_qt` decimal(26,6)  COMMENT '采购入库价格补救-调整其他_含税'  
) COMMENT '财报管报调整成本-销售调整'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

--------------------------------- mysql建表语句 -------------------------------
drop table if exists `cbgb_tz_m_cbgb_xstz`;
create table `cbgb_tz_m_cbgb_xstz`(
  `province_code` varchar(64) DEFAULT NULL COMMENT '省区编号',
  `province_name` varchar(64) DEFAULT NULL COMMENT '省区',
  `city_code` varchar(64) DEFAULT NULL COMMENT '城市编号',  
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市',
  `location_code` varchar(64) DEFAULT NULL COMMENT 'DC编号',
  `location_name` varchar(64) DEFAULT NULL COMMENT 'DC名称',
  `channel_name` varchar(64) DEFAULT NULL COMMENT '渠道',  
  `dept_id` varchar(64) DEFAULT NULL COMMENT '课组编号',
  `dept_name` varchar(64) DEFAULT NULL COMMENT '课组名称',
  `adj_ddfkc_no` decimal(26,6) COMMENT '对抵负库存_未税',
  `adj_ddfkc` decimal(26,6) COMMENT '对抵负库存_含税',
  `adj_cgth_no` decimal(26,6) COMMENT '采购退货_未税',
  `adj_cgth` decimal(26,6) COMMENT '采购退货_含税',
  `adj_gc_xs_no` decimal(26,6) COMMENT '工厂月末分摊-调整销售_未税',
  `adj_gc_xs` decimal(26,6) COMMENT '工厂月末分摊-调整销售_含税',
  `adj_gc_db_no` decimal(26,6) COMMENT '工厂月末分摊-调整跨公司调拨_未税',
  `adj_gc_db` decimal(26,6) COMMENT '工厂月末分摊-调整跨公司调拨_含税',
  `adj_gc_qt_no` decimal(26,6) COMMENT '工厂月末分摊-调整其他_未税',
  `adj_gc_qt` decimal(26,6) COMMENT '工厂月末分摊-调整其他_含税',
  `adj_sg_no` decimal(26,6) COMMENT '手工调整销售成本_未税',
  `adj_sg` decimal(26,6)  COMMENT '手工调整销售成本_含税',
  `adj_bj_xs_no` decimal(26,6) COMMENT '采购入库价格补救-调整销售_未税',
  `adj_bj_xs` decimal(26,6) COMMENT '采购入库价格补救-调整销售_含税',
  `adj_bj_db_no` decimal(26,6) COMMENT '采购入库价格补救-调整跨公司调拨_未税',
  `adj_bj_db` decimal(26,6) COMMENT '采购入库价格补救-调整跨公司调拨_含税',
  `adj_bj_qt_no` decimal(26,6) COMMENT '采购入库价格补救-调整其他_未税',
  `adj_bj_qt` decimal(26,6)  COMMENT '采购入库价格补救-调整其他_含税',  
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='财报管报调整成本-销售调整';



select 
	sum(adj_ddfkc_no) adj_ddfkc_no,sum(adj_ddfkc) adj_ddfkc,
	sum(adj_cgth_no) adj_cgth_no,sum(adj_cgth) adj_cgth,
	sum(adj_gc_xs_no) adj_gc_xs_no,sum(adj_gc_xs) adj_gc_xs,
	sum(adj_gc_db_no) adj_gc_db_no,sum(adj_gc_db) adj_gc_db,
	sum(adj_gc_qt_no) adj_gc_qt_no,sum(adj_gc_qt) adj_gc_qt,
	sum(adj_sg_no) adj_sg_no,sum(adj_sg) adj_sg,
	sum(adj_bj_xs_no) adj_bj_xs_no,sum(adj_bj_xs) adj_bj_xs,
	sum(adj_bj_db_no) adj_bj_db_no,sum(adj_bj_db) adj_bj_db,
	sum(adj_bj_qt_no) adj_bj_qt_no,sum(adj_bj_qt) adj_bj_qt
from csx_dw.cbgb_tz_m_cbgb_xstz
where sdt='202005';


*/


