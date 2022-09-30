CREATE external TABLE IF NOT EXISTS csx_analyse.csx_analyse_scm_purchase_order_flow_di( 

`purchase_org_code` STRING  COMMENT '采购组织',
`purchase_org_name` STRING  COMMENT '采购组织名称',
`purchase_order_code` STRING  COMMENT '采购订单号',
`order_code` STRING  COMMENT '入库/出库单号',
`batch_code` STRING  COMMENT '批次单号',
`performance_region_code` STRING  COMMENT '大区编码',
`performance_region_name` STRING  COMMENT '大区名称',
`performance_province_code` STRING  COMMENT '业绩省区编码',
`performance_province_name` STRING  COMMENT '业绩省区名称',
`performance_city_code` STRING  COMMENT '业绩城市',
`performance_city_name` STRING  COMMENT '业绩城市名称',
`province_code` STRING  COMMENT '物理省区编码',
`province_name` STRING  COMMENT '物理省区名称',
`city_code` STRING  COMMENT '物理归属城市',
`city_name` STRING  COMMENT '物理归属城市名称',
`source_type_code` STRING  COMMENT '来源采购订单类型',
`source_type_name` STRING  COMMENT '来源采购订单名称',
`super_class_code` STRING  COMMENT '单据类型编码',
`super_class_name` STRING  COMMENT '单据类型名称',
`dc_code` STRING  COMMENT 'dc编码',
`dc_name` STRING  COMMENT 'dc名称',
`goods_code` STRING  COMMENT '商品编码',
`bar_code` STRING  COMMENT '商品条码',
`goods_name` STRING  COMMENT '商品名称',
`spu_goods_code` STRING  COMMENT 'spu商品编码',
`spu_goods_name` STRING  COMMENT 'spu商品名称',
`spu_goods_status` STRING  COMMENT 'spu商品状态',
`unit_name` STRING  COMMENT '单位',
`brand_name` STRING  COMMENT '品牌',
`division_code` STRING  COMMENT '部类编码',
`division_name` STRING  COMMENT '部类名称',
`purchase_group_code` STRING  COMMENT '课组编码',
`purchase_group_name` STRING  COMMENT '课组名称',
`classify_large_code` STRING  COMMENT '管理一级编码',
`classify_large_name` STRING  COMMENT '管理一级名称',
`classify_middle_code` STRING  COMMENT '管理二级编码',
`classify_middle_name` STRING  COMMENT '管理二级名称',
`classify_small_code` STRING  COMMENT '管理三级编码',
`classify_small_name` STRING  COMMENT '管理三级名称',
`category_large_code` STRING  COMMENT '大类编码',
`category_large_name` STRING  COMMENT '大类名称',
`category_middle_code` STRING  COMMENT '中类编码',
`category_middle_name` STRING  COMMENT '中类名称',
`category_small_code` STRING  COMMENT '小类编码',
`category_small_name` STRING  COMMENT '小类名称',
`csx_purchase_level_code` STRING  COMMENT '彩食鲜商品采购级别编码 01-全国商品,02-一般商品,03-oem商品',
`csx_purchase_level_name` STRING  COMMENT '彩食鲜商品采购级别名称',
`supplier_code` STRING  COMMENT '供应商编码',
`supplier_name` STRING  COMMENT '供应商名称',
`send_dc_code` STRING  COMMENT '发货dc编码',
`send_dc_name` STRING  COMMENT '发货dc名称',
`settle_dc_code` STRING  COMMENT '结算dc',
`settle_dc_name` STRING  COMMENT '结算dc名称',
`settle_company_code` STRING  COMMENT '结算公司编码',
`settle_company_name` STRING  COMMENT '结算公司名称',
`local_purchase_flag` STRING  COMMENT '是否地采',
`business_type_name` STRING  COMMENT '业务类型名称',
`order_qty` DECIMAL (38,6) COMMENT '订单数量',
`order_price1` DECIMAL (38,6) COMMENT '单价1',
`order_price2` DECIMAL (38,6) COMMENT '单价2',
`receive_qty` DECIMAL (38,6) COMMENT '入库数量',
`receive_amt` DECIMAL (38,6) COMMENT '入库金额',
`no_tax_receive_amt` DECIMAL (38,6) COMMENT '入库不含税金额',
`shipped_qty` DECIMAL (38,6) COMMENT '出库数量',
`shipped_amt` DECIMAL (38,6) COMMENT '出库金额',
`no_tax_shipped_amt` DECIMAL (38,6) COMMENT '出库不含税金额',
`receive_sdt` STRING  COMMENT '收货日期',
`order_create_date` STRING  COMMENT '订单日期',
`daily_source` STRING  COMMENT '日采标识',
`pick_gather_flag` STRING  COMMENT '已拣代收',
`urgency_flag` STRING  COMMENT '紧急补货',
`has_change` STRING  COMMENT '有无变更',
`entrust_outside` STRING  COMMENT '委外标识',
`order_business_type` STRING  COMMENT '业务类型 0缺省 1基地订单',
`order_type` STRING  COMMENT '订单类型(0-普通供应商订单 1-囤货订单 2-日采订单 3-计划订单)',
`extra_flag` STRING  COMMENT '补货标识',
`timeout_cancel_flag` STRING  COMMENT '超时订单取消',
`joint_purchase_flag` STRING  COMMENT '集采订单',
`supplier_joint` STRING  COMMENT '集采供应商',
`business_owner_code` STRING  COMMENT '业态归属编码',
`business_owner_name` STRING  COMMENT '业态归属名称',
`special_customer` STRING  COMMENT '专项客户',
`borrow_flag` STRING  COMMENT '是否借用',
`direct_trans_flag` STRING  COMMENT '是否直供',
`supplier_classify_code` STRING  COMMENT '供应商类型编码  0：基础供应商   1:农户供应商',
`valuation_category_code` STRING  COMMENT '评估类编码',
`valuation_category_name` STRING  COMMENT '评估类名称',
`order_goods_status` STRING  COMMENT '订单商品状态 状态(1-已创建,2-已发货,3-入库中,4-已完成,5-已取消)',
`purpose` STRING  COMMENT 'dc类型编码',
`purpose_name` STRING  COMMENT 'dc类型名称',
`update_time` STRING  COMMENT '数据更新时间' ) 
 COMMENT 'csx_analyse_scm_purchase_order_flow_di' 
 PARTITIONED BY
 (
`sdt` STRING  COMMENT '订单创建日期{"FORMAT":"yyyymmdd"}' )
 STORED AS PARQUET


 -- 财务采购分析【现金采买分析】
-- 采购现金分析,
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式

set hive.support.quoted.identifiers=none;

set purpose = ('01','02','03','05','07','08');
set edate = regexp_replace('${enddate}','-','');
set sdate = regexp_replace(trunc('${enddate}','MM'),'-','');
set month = substr(regexp_replace('${enddate}','-',''),1,6);
set year  = concat(substr('${enddate}',1,4),'0101');


-- select ${hiveconf:edate},${hiveconf:sdate},${hiveconf:month};
-- select * from  csx_analyse_tmp.csx_analyse_tmp_dc_new ;

-- 大区处理
drop table csx_analyse_tmp.csx_analyse_tmp_dc_new ;
create  TABLE csx_analyse_tmp.csx_analyse_tmp_dc_new as 
select case when performance_region_code!='10' then '大区'else '平台' end dept_name,
    purchase_org,
    purchase_org_name,
    case when performance_province_name like'平台%' then '00'    else   performance_region_code end  region_code,
    case when performance_province_name like'平台%' then '平台'  else  performance_region_name end  region_name,
    shop_code ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    basic_performance_city_code as performance_city_code,
    basic_performance_city_name as performance_city_name,
    basic_performance_province_code as performance_province_code,
    basic_performance_province_name as performance_province_name,
    case when c.dc_code is not null then '1' else '0' end as is_purchase_dc ,
    enable_date
from csx_dim.csx_dim_shop a 
left join 
(select a.code as province_code,
       a.name as province_name,
       b.code region_code,
       b.name region_name 
from csx_dim.csx_dim_basic_performance_region_province_city_tomysql a 
 left join 
(select code,name,parent_code 
from csx_dim.csx_dim_basic_performance_region_province_city_tomysql
 where level=1)  b on a.parent_code=b.code
 where level=2) b on a.basic_performance_province_code=b.province_code
 left join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date 
 from csx_dim.csx_dim_csx_data_market_conf_supplychain_location 
 where sdt='current') c on a.shop_code=c.dc_code
 where sdt='current'    
    ;
    
    

drop table  csx_analyse_tmp.csx_analyse_tmp_purchase_01 ;
create  table csx_analyse_tmp.csx_analyse_tmp_purchase_01 as 
SELECT dept_name,
       d.region_code,
       d.region_name,
       d.performance_province_code province_code,
       d.performance_province_name province_name,
       d.performance_city_code city_code,
       d.performance_city_name city_name,
       b.source_type_name,
       source_channel,
       a.order_business_type,
       a.supplier_classify_code,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       supplier_code,
       d.is_purchase_dc,
       case when a.supplier_name like '%永辉%' then '云超配送'
            when business_type_name like '云超配送%' then '云超配送'
       else '供应商配送' end business_type_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       sum(coalesce(receive_amt,0)-coalesce(shipped_amt,0)) AS net_entry_amount,
       coalesce(sum(case when d.is_purchase_dc='1' and classify_large_code ='B02' then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0) as B02_entry_amount,
       coalesce(sum(case when order_business_type=1 and classify_large_code ='B02' and d.is_purchase_dc='1' then coalesce(receive_amt,0)-coalesce(shipped_amt,0) end ),0 ) base_entry_amount,
       d.purpose,
       sdt,
       substr(sdt,1,6) months,
       concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) quarter  ,
       substr(sdt,1,4) year,
       t.week_of_year,
       concat(t.week_begin,'-',week_end) as week_date,
       enable_date
FROM  csx_analyse_tmp.report_fr_r_m_financial_purchase_detail a 
left join 
(select order_code, source_type,config_value as source_type_name,channel,concat(config_value,channel) source_channel from csx_dw.dws_scm_r_d_order_detail a
 left join
(select *
from  csx_ods.source_scm_w_d_configuration  a where a.config_type = 'PURCHASE_ORDER_SOURCE_TYPE' and sdt=regexp_replace(date_sub(current_date(),1),'-','') ) b on a.source_type=b.config_key
group by  order_code,source_type,channel,config_value,concat(config_value,channel)
) b on a.purchase_order_code = b.order_code
join csx_dw.dws_basic_w_a_date t on a.sdt=t.calday
join csx_analyse_tmp.csx_analyse_tmp_dc_new  d on a.dc_code=d.shop_id 
WHERE months <= ${hiveconf:month}
    and months>= substr(${hiveconf:year},1,6)
  AND d.purpose in ${hiveconf:purpose}
  and b.source_type_name not in  ('城市服务商','联营直送','项目合伙人')
  GROUP BY dept_name,
       d.region_code,
       d.region_name,
       performance_city_code,
       performance_city_name,
       performance_province_code,
       performance_province_name,
       a.order_business_type,
       a.supplier_classify_code,
       classify_middle_name,
       a.classify_middle_code,
       supplier_code,
       case when a.supplier_name like '%永辉%' then '云超配送'
       when business_type_name like '云超配送%' then '云超配送'
              ELSE '供应商配送' end ,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end,
       classify_large_code,
       classify_large_name,
       sdt,
       d.is_purchase_dc,
       d.purpose,
       enable_date,
        substr(sdt,1,6) ,
       concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1)   ,
       substr(sdt,1,4),
       t.week_of_year,
       concat(t.week_begin,'-',week_end),
       b.source_type_name,
       source_channel
;

create table csx_analyse_tmp.csx_analyse_tmp_cash_01 as 
SELECT dept_name,
        source_channel,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       sum( net_entry_amount) net_entry_amount,
       sum(coalesce(case when supplier_classify_code=2 then net_entry_amount end  ,0  ) ) cash_entry_amount,  -- 现金采购
       months
 from  csx_analyse_tmp.csx_analyse_tmp_purchase_01   a
 group by dept_name,
       region_code,
       region_name,
       province_code,
       province_name,
       city_code,
       city_name,
       bd_id,
       bd_name,
       months,
       classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       classify_middle_name,
       source_channel;


show CREATE TABLE  csx_analyse_tmp.report_fr_r_m_financial_purchase_detail
;



  
 drop table csx_analyse_tmp.report_r_d_purchase_cash_analysis;
CREATE TABLE `csx_analyse_tmp.report_r_d_purchase_cash_analysis`(
  `dept_name` string comment '运营部门平台、大区', 
  `region_code` string comment '大区编码', 
  `region_name` string comment '大区名称', 
  `province_code` string comment '省区编码', 
  `province_name` string comment '省区名称', 
  `city_code` string comment '城市编码', 
  `city_name` string comment '城市名称', 
   source_terminal string comment '来源终端',
   source_type_code string comment '来源类型编码',
   source_type_name string comment '来源类型名称',
   source_channel string comment '来源类型订单',
  `bd_id` string comment '部类编码', 
  `bd_name` string comment '部类名称', 
  `classify_large_code` string comment '管理大类', 
  `classify_large_name` string comment '管理大类', 
  `classify_middle_code` string comment '管理中类', 
  `classify_middle_name` string comment '管理中类', 
  `net_entry_amount` decimal(38,6) comment '净入库额', 
  `cash_entry_amount` decimal(38,6) COMMENT '现金采买金额', 
  update_time TIMESTAMP COMMENT '数据更新日期'
  )comment'现金采买报表明细'
 partitioned by (months string comment '日期分区')
STORED AS parquet 
  
  