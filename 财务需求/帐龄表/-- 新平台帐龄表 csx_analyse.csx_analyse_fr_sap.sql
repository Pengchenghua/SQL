-- 新平台帐龄表 csx_analyse.csx_analyse_fr_sap_subject_customer_account_df 
CREATE external TABLE IF NOT EXISTS csx_analyse.csx_analyse_fr_sap_subject_customer_account_analyse_df( 
`channel_name` STRING  COMMENT '类型',
`subject_code` STRING  COMMENT '科目编码',
`subject_name` STRING  COMMENT '科目名称',
`company_code` STRING  COMMENT '公司编码',
`company_name` STRING  COMMENT '公司名称',
`performance_region_code` STRING  COMMENT '大区编码',
`performance_region_name` STRING  COMMENT '大区名称',
`performance_province_code` STRING  COMMENT '省区编码',
`performance_province_name` STRING  COMMENT '省区名称',
`performance_city_code` STRING  COMMENT '城市编码',
`performance_city_name` STRING  COMMENT '城市名称',
`profit_center` STRING  COMMENT '利润中心',
`profit_center_name` STRING  COMMENT '利润中心',
`customer_code` STRING  COMMENT '编码',
`customer_name` STRING  COMMENT '名称',
`business_attribute_code` STRING  COMMENT '属性编码',
`business_attribute_name` STRING  COMMENT '属性',
`customer_level` STRING  COMMENT '信控等级',
`customer_level_name` STRING  COMMENT '信控等级名称',
`first_category_code` STRING  COMMENT '一级分类编码',
`first_category_name` STRING  COMMENT '一级分类名称',
`second_category_code` STRING  COMMENT '二级分类编码',
`second_category_name` STRING  COMMENT '二级分类名称',
`third_category_code` STRING  COMMENT '三级分类编码',
`third_category_name` STRING  COMMENT '三级分类名称',
`sales_id` STRING  COMMENT '销售员id',
`sales_employee_code` STRING  COMMENT '销售员工号',
`sales_employee_name` STRING  COMMENT '销售员名称',
`supervisor_user_id` STRING  COMMENT '销售主管id',
`supervisor_user_number` STRING  COMMENT '销售主管工号',
`supervisor_user_name` STRING  COMMENT '销售主管名称',
`rp_service_user_name_new` STRING  COMMENT '日配服务管家名称',
`rp_service_user_work_no_new` STRING  COMMENT '日配服务管家工号',
`credit_limit` DECIMAL (26,4) COMMENT '信控额度',
`temp_credit_limit` DECIMAL (26,4) COMMENT '临时额度',
`temp_begin_time` STRING  COMMENT '临时额度起始时间',
`temp_end_time` STRING  COMMENT '临时额度截止时间',
`account_period_code` STRING  COMMENT '账期类型',
`account_period_name` STRING  COMMENT '账期名称',
`account_period_value` STRING  COMMENT '帐期天数',
`back_money_amount_all` DECIMAL (36,6) COMMENT '回款总金额',
`sale_amt_all` DECIMAL (36,6) COMMENT '总销售金额',
`remain_back_amount` DECIMAL (36,6) COMMENT '剩余负单未核销金额',
`back_money_amount_month` DECIMAL (36,6) COMMENT '当月回款金额',
`sale_amt_month` DECIMAL (36,6) COMMENT '当月总销售金额',
`receivable_amount` DECIMAL (36,6) COMMENT '应收账款',
`no_overdue_amount` DECIMAL (36,6) COMMENT '未逾期金额',
`overdue_amount` DECIMAL (36,6) COMMENT '逾期金额',
`overdue_rate` DECIMAL (36,6) COMMENT '逾期率',
`overdue_amount_15_day` DECIMAL (36,6) COMMENT '逾期1-15天',
`overdue_amount_30_day` DECIMAL (36,6) COMMENT '逾期15-30天',
`overdue_amount_60_day` DECIMAL (36,6) COMMENT '逾期30-60天',
`overdue_amount_90_day` DECIMAL (36,6) COMMENT '逾期60-90天',
`overdue_amount_120_day` DECIMAL (36,6) COMMENT '逾期90-120天',
`overdue_amount_180_day` DECIMAL (36,6) COMMENT '逾期120-180天',
`overdue_amount_1_year` DECIMAL (36,6) COMMENT '逾期180-365天',
`overdue_amount_2_year` DECIMAL (36,6) COMMENT '逾期1-2年',
`overdue_amount_3_year` DECIMAL (36,6) COMMENT '逾期2-3年',
`overdue_amount_more_3_year` DECIMAL (36,6) COMMENT '逾期3年以上',
`last_sales_date` STRING  COMMENT '最后一次销售日期',
`last_to_now_days` STRING  COMMENT '最后一次销售距今天数',
`customer_active_sts_code` STRING  COMMENT '活跃状态标签编码（1 活跃；2 沉默；3预流失；4 流失）',
`customer_active_sts` STRING  COMMENT '活跃状态名称',
`receivable_amount_month_last_day` DECIMAL (36,6) COMMENT '月末应收账款',
`no_overdue_amount_month_last_day` DECIMAL (36,6) COMMENT '月末未到期账款',
`max_overdue_day` BIGINT  COMMENT '最大逾期天数',
`update_time` TIMESTAMP  COMMENT '更新时间' ,
sale_sdt string comment '销售日期') 
 COMMENT 'csx_analyse_fr_sap_subject_customer_account_df' 
 PARTITIONED BY
 (
`sdt` STRING  COMMENT '统计日期{format:yyyymmdd}{"FORMAT":"yyyymmdd"}' )
 STORED AS PARQUET
 ;

CREATE external TABLE IF NOT EXISTS csx_dws.csx_dws_sap_subject_customer_credit_settle_detail        ( 

`subject_code` STRING  COMMENT '科目编码',
`customer_code` STRING  COMMENT '编码',
`customer_name` STRING  COMMENT '名称',
`company_code` STRING  COMMENT '公司编码',
`company_name` STRING  COMMENT '公司名称',
`dc_code` STRING  COMMENT 'dc编码',
`dc_name` STRING  COMMENT 'dc名称',
`profit_center` STRING  COMMENT '利润中心',
`account_period_code` STRING  COMMENT '账期类型',
`account_period_name` STRING  COMMENT '账期名称',
`account_period_value` STRING  COMMENT '帐期天数',
`performance_region_code` STRING  COMMENT '大区编码',
`performance_region_name` STRING  COMMENT '大区名称',
`performance_province_code` STRING  COMMENT '省区编码',
`performance_province_name` STRING  COMMENT '省区名称',
`performance_city_code` STRING  COMMENT '城市编码',
`performance_city_name` STRING  COMMENT '城市名称',
`channel_code` STRING  COMMENT '渠道编码',
`channel_name` STRING  COMMENT '渠道名称',
`business_attribute_code` STRING  COMMENT '属性编码',
`business_attribute_name` STRING  COMMENT '属性',
`sales_id` STRING  COMMENT '销售员id',
`sales_employee_code` STRING  COMMENT '销售员工号',
`sales_employee_name` STRING  COMMENT '销售员名称',
`supervisor_user_id` STRING  COMMENT '一级主管编码 b端：销售主管,s端：采购总监 大宗：主管',
`supervisor_user_number` STRING  COMMENT '一级主管工号',
`supervisor_user_name` STRING  COMMENT '一级主管姓名',
`customer_level` STRING  COMMENT '等级',
`credit_limit` STRING  COMMENT '信控额度',
`temp_credit_limit` STRING  COMMENT '临时额度',
`temp_begin_time` STRING  COMMENT '临时额度起始时间',
`temp_end_time` STRING  COMMENT '临时额度截止时间',
`back_money_amount_all` DECIMAL (36,6) COMMENT '回款总金额',
`sale_amt_all` DECIMAL (36,6) COMMENT '总销售金额',
`remain_back_amount` DECIMAL (36,6) COMMENT '剩余负单未核销金额',
`back_money_amount_month` DECIMAL (36,6) COMMENT '当月回款金额',
`sale_amt_month` DECIMAL (36,6) COMMENT '当月总销售金额',
`overdue_amount` DECIMAL (36,6) COMMENT '逾期金额',
`overdue_amount_15_day` DECIMAL (36,6) COMMENT '逾期1-15天',
`overdue_amount_30_day` DECIMAL (36,6) COMMENT '逾期15-30天',
`overdue_amount_60_day` DECIMAL (36,6) COMMENT '逾期30-60天',
`overdue_amount_90_day` DECIMAL (36,6) COMMENT '逾期60-90天',
`overdue_amount_120_day` DECIMAL (36,6) COMMENT '逾期90-120天',
`overdue_amount_180_day` DECIMAL (36,6) COMMENT '逾期120-180天',
`overdue_amount_1_year` DECIMAL (36,6) COMMENT '逾期180-365天',
`overdue_amount_2_year` DECIMAL (36,6) COMMENT '逾期1-2年',
`overdue_amount_3_year` DECIMAL (36,6) COMMENT '逾期2-3年',
`overdue_amount_more_3_year` DECIMAL (36,6) COMMENT '逾期3年以上',
`last_sales_date` string COMMENT '最后一次销售日期', 
`last_to_now_days` string COMMENT '最后一次销售距今天数', 
`customer_active_sts_code` string COMMENT '活跃状态标签编码（1 活跃；2 沉默；3预流失；4 流失）', 
`customer_active_sts` string COMMENT '活跃状态名称', 
`no_overdue_amount` DECIMAL (36,6) COMMENT '未逾期金额',
`receivable_amount` DECIMAL (36,6) COMMENT '应收账款',
`receivable_amount_month_last_day` DECIMAL (36,6) COMMENT '月末应收账款',
`no_overdue_amount_month_last_day` DECIMAL (36,6) COMMENT '月末未到期账款',
`max_overdue_day` BIGINT  COMMENT '最大逾期天数',
`overdue_rate` DECIMAL (36,6) COMMENT '逾期率' ) 
 COMMENT 'csx_dws_sap_subject_customer_credit_settle_detail        ' 
 PARTITIONED BY
 (
`sdt` STRING  COMMENT '统计日期{"FORMAT":"yyyymmdd"}' )
 STORED AS PARQUET
 ;


	CREATE TABLE `csx_tmp.ads_fr_r_d_account_receivables_scar`(
	  `channel_name` string COMMENT '类型', 
	  `hkont` string COMMENT '科目代码', 
	  `account_name` string COMMENT '科目名称', 
	  `comp_code` string COMMENT '公司代码', 
	  `comp_name` string COMMENT '公司名称', 
	  `region_code` string COMMENT '大区编码', 
	  `region_name` string COMMENT '大区名称', 
	  `province_code` string COMMENT '销售省区编码', 
	  `province_name` string COMMENT '销售省区名称', 
	  `sales_city` string COMMENT '销售城市名称', 
	  `prctr` string COMMENT '利润中心', 
	  `shop_name` string COMMENT '利润中心名称', 
	  `customer_no` string COMMENT '编码', 
	  `customer_name` string COMMENT '名称', 
	  `attribute_desc` string COMMENT '属性', 
	  `customer_level` string COMMENT '信控等级', 
	  `customer_level_name` string COMMENT '信控等级名称', 
	  `first_category_code` string COMMENT '第一分类编码', 
	  `first_category` string COMMENT '第一分类', 
	  `second_category_code` string COMMENT '第二分类编码', 
	  `second_category` string COMMENT '第二分类', 
	  `third_category_code` string COMMENT '第三分类编码', 
	  `third_category` string COMMENT '第三分类', 
	  `work_no` string COMMENT '销售员工号', 
	  `sales_name` string COMMENT '销售员姓名', 
	  `first_supervisor_name` string COMMENT '销售主管', 
	  `rp_service_user_name_new` string COMMENT '日配服务管家名称', 
	  `rp_service_user_work_no_new` string COMMENT '日配服务管家工号', 
	  `credit_limit` decimal(26,4) COMMENT '信控额度', 
	  `temp_credit_limit` decimal(26,4) COMMENT '临时信控额度', 
	  `payment_terms` string COMMENT '付款条件', 
	  `payment_name` string COMMENT '付款条件名称', 
	  `payment_days` string COMMENT '帐期', 
	  `zterm` string COMMENT '账期类型', 
	  `diff` string COMMENT '账期', 
	  `back_money_amount_total` decimal(36,6) COMMENT '回款总金额', 
	  `unpaid_amount_total` decimal(36,6) COMMENT '总销售金额', 
	  `remaining_back_amount` decimal(36,6) COMMENT '剩余负单未核销金额', 
	  `back_money_amount_month` decimal(36,6) COMMENT '当月回款金额', 
	  `unpaid_amount_month` decimal(36,6) COMMENT '当月总销售金额', 
	  `ac_all` decimal(26,4) COMMENT '全部账款', 
	  `ac_wdq` decimal(26,4) COMMENT '未到期账款', 
	  `overdue_amount` decimal(36,6) COMMENT '逾期金额', 
	  `overdue_ratio` decimal(36,6) COMMENT '逾期率', 
	  `ac_15d` decimal(26,4) COMMENT '15天内账款', 
	  `ac_30d` decimal(26,4) COMMENT '30天内账款', 
	  `ac_60d` decimal(26,4) COMMENT '60天内账款', 
	  `ac_90d` decimal(26,4) COMMENT '90天内账款', 
	  `ac_120d` decimal(26,4) COMMENT '120天内账', 
	  `ac_180d` decimal(26,4) COMMENT '半年内账款', 
	  `ac_365d` decimal(26,4) COMMENT '1年内账款', 
	  `ac_2y` decimal(26,4) COMMENT '2年内账款', 
	  `ac_3y` decimal(26,4) COMMENT '3年内账款', 
	  `ac_over3y` decimal(26,4) COMMENT '逾期3年账款', 
	  `last_sales_date` string COMMENT '最后一次销售日期', 
	  `last_to_now_days` string COMMENT '最后一次销售距今天数', 
	  `customer_active_sts_code` string COMMENT '活跃状态标签编码（1 活跃；2 沉默；3预流失；4 流失）', 
	  `customer_active_sts` string COMMENT '活跃状态名称', 
	  `ac_all_month_last_day` decimal(26,4) COMMENT '月底全部账款', 
	  `ac_wdq_month_last_day` decimal(26,4) COMMENT '月底未到期账款', 
	  `max_overdue_day` string COMMENT '最大逾期天数', 
	  `update_time` timestamp COMMENT '更新时间')
	COMMENT '应收帐龄结果表-帆软使用（新逻辑）'
	PARTITIONED BY ( 
	  `sdt` string COMMENT '日期分区')
	ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
	STORED AS INPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
	OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
	LOCATION
	  'hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/ads_fr_r_d_account_receivables_scar'
	TBLPROPERTIES (
	  'last_modified_by'='pengchenghua', 
	  'last_modified_time'='1653465254', 


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;


drop table csx_tmp.temp_account_age;
CREATE   table csx_tmp.temp_account_age
as
select
    x.channel_name channel ,
    x.subjects_code hkont ,
    account_name ,
    x.company_code ,
    x.company_name ,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    x.performance_city_name as sales_city ,
    '' as prctr,			--成本中心
    '' as shop_name,
    x.customer_code customer_no ,
    x.customer_name ,
    b.customer_level_code  ,           -- 新增
    b.customer_level_name ,
    attribute_desc,
    first_category_code,
    first_category_name  ,
    second_category_code,
    second_category_name  ,
    third_category_code,
    third_category_name  ,
    `sales_id`  ,
    `sales_employee_code` ,
    `sales_employee_name` ,
    `supervisor_user_id`  ,
    `supervisor_user_number`  ,
    `supervisor_user_name`    ,
    x.credit_limit ,
    x.temp_credit_limit ,
    `temp_begin_time,
    `temp_end_time` ,
     account_period_code  , -- '账期类型',
    `account_period_name`  , -- '账期名称',
    `account_period_value` , --  '帐期天数',
    max(`back_money_amount_all`)  back_money_amount_all,   -- 总回款金额新增
    max(`sale_amt_all`)  sale_amt_all ,         -- 总销售新增
    sum(`remain_back_amount`) remain_back_amount  ,  -- 新增剩余负单未核销金额
    sum(`back_money_amount_month`) back_money_amount_month  ,    -- 新增当月回款金额
    sum(`sale_amt_month` ) sale_amt_month,     -- 新增当月总销售金额
    sum(receivable_amount) receivable_amount ,
    sum(no_overdue_amount) no_overdue_amount ,
    sum(`overdue_amount`) `overdue_amount`,    
    sum(overdue_amount_15_day)  overdue_amount_15_day ,
    sum(overdue_amount_30_day) overdue_amount_30_day ,
    sum(overdue_amount_60_day) overdue_amount_60_day ,
    sum(overdue_amount_90_day) overdue_amount_90_day ,
    sum(overdue_amount_120_day) overdue_amount_120_day ,
    sum(overdue_amount_180_day)  overdue_amount_180_day ,
    sum(overdue_amount_1_year)  overdue_amount_1_year ,
    sum(overdue_amount_2_year) overdue_amount_2_year ,
    sum(overdue_amount_3_year) overdue_amount_3_year ,
    sum(overdue_amount_more_3_year) overdue_amount_more_3_year,
    sum(receivable_amount_month_last_day) ac_all_month_last_day,
    sum(no_overdue_amount_month_last_day) ac_wdq_month_last_day,  
	max(max_overdue_day)max_overdue_day
	x.sdt
from
-- csx_dws_sap_subject_customer_settle_detail 旧表切换
    csx_dws.csx_dws_sap_subject_customer_credit_settle_detail         x
LEFT JOIN 
(
  SELECT customer_code, 
  customer_name,
  channel_code,
  channel_name, 
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  sales_province_id, 
  sales_province_name,
  sales_city_code,
  sales_city_name,
  first_category_code,
  first_category_name    ,
  second_category_code,
  second_category_name ,
  third_category_code,
  third_category_name ,
  attribute_desc,
  customer_level_code  ,           -- 等级编码
  `customer_level_name`            -- 等级名称
  FROM csx_dim.csx_dim_crm_customer_info
  WHERE sdt = 'current' ) b on x.customer_code=b.customer_code
left join
(select code as accunt_code,name as account_name from csx_ods.source_basic_w_a_md_accounting_subject where sdt=regexp_replace(${hiveconf:e_date}, '-', '')) as  e
on x.subjects_code=e.accunt_code
where 
    x.sdt= '${edate}'
GROUP BY    x.channel_name   ,
    x.subjects_code   ,
    account_name ,
    x.company_code ,
    x.company_name ,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    x.performance_city_name  ,    
    x.customer_code   ,
    x.customer_name ,
    b.customer_level_code  ,           -- 新增
    b.customer_level_name ,
    attribute_desc,
    first_category_code,
    first_category_name  ,
    second_category_code,
    second_category_name  ,
    third_category_code,
    third_category_name  ,
    `sales_id`  ,
    `sales_employee_code` ,
    `sales_employee_name` ,
    `supervisor_user_id`  ,
    `supervisor_user_number`  ,
    `supervisor_user_name`    ,
    x.credit_limit ,
    x.temp_credit_limit ,
    `temp_begin_time,
    `temp_end_time` ,
     account_period_code  , -- '账期类型',
    `account_period_name`  , -- '账期名称',
    `account_period_value` 
    ;



drop table csx_tmp.temp_account_age_00;
CREATE   table csx_tmp.temp_account_age_00
as
select
   coalesce(channel,'其他') channel ,
    hkont ,
    account_name ,
    comp_code ,
    comp_name ,
    coalesce(region_code,'') region_code,
    coalesce(region_name,'') region_name,
    coalesce(province_code,'') province_code,
    coalesce(province_name ,'') province_name ,
    coalesce(sales_city ,'') sales_city ,
    prctr,			--成本中心
    shop_name,
    x.customer_no ,
    coalesce(x.customer_name ,'') customer_name ,
    coalesce(attribute_desc,'') attribute_desc,
    coalesce(`customer_level` ,'') customer_level,           -- 新增
    coalesce(`customer_level_name`,'') customer_level_name,
    coalesce(first_category_code ,'') first_category_code ,
    coalesce(first_category ,'') first_category ,
    coalesce(second_category_code,'') second_category_code,
    coalesce(second_category ,'') second_category ,
    coalesce(third_category_code,'') third_category_code,
    coalesce(third_category ,'') third_category ,
    coalesce(work_no ,'') work_no ,
    coalesce(sales_name ,'') sales_name ,
    coalesce(first_supervisor_name,'') first_supervisor_name,
    coalesce(rp_service_user_name_new,'')    rp_service_user_name_new,
    coalesce(rp_service_user_work_no_new,'') rp_service_user_work_no_new,
    coalesce(x.credit_limit ,'')    credit_limit ,
    coalesce(x.temp_credit_limit, '') temp_credit_limit ,
    coalesce(payment_terms,'') payment_terms,
    coalesce(payment_name,'') payment_name,
    coalesce(payment_days,'') payment_days,
    coalesce(zterm,'') zterm,                -- 帐期类型
    coalesce(diff,'') diff,                 -- 帐期天数
	back_money_amount_total,   -- 总回款金额新增
    unpaid_amount_total ,         -- 总销售新增
    remaining_back_amount  ,  -- 新增
    back_money_amount_month  ,    -- 新增
    unpaid_amount_month ,     -- 新增
    ac_all ,
    ac_wdq ,
	overdue_amount,
	if(overdue_amount/ac_all>1 , 1 , overdue_amount/ac_all) overdue_ratio,	-- 逾期率
    ac_15d ,
    ac_30d ,
    ac_60d ,
    ac_90d ,
    ac_120d ,
    ac_180d ,
    ac_365d ,
    ac_2y ,
    ac_3y ,
    ac_over3y,
    coalesce(last_sales_date,'') last_sales_date,
	coalesce(last_to_now_days,'') last_to_now_days,
	coalesce(customer_active_sts_code,'') customer_active_sts_code,
    coalesce(customer_active_sts,'') customer_active_sts,
	coalesce(ac_all_month_last_day,'') ac_all_month_last_day,
	coalesce(ac_wdq_month_last_day,'') ac_wdq_month_last_day,
	
	coalesce(max_overdue_day,'') max_overdue_day,
    	current_timestamp() update_time,
	x.sdt
from    csx_tmp.temp_account_age  x
left join
(select a.customer_no,
        a.city_group_code,
        a.rp_service_user_name_new,
        a.rp_service_user_work_no_new
    from  csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df a 
    where month= '${month}')  b on x.customer_no=b.customer_no
LEFT JOIN
(
  select customer_code,
        sign_company_code, 
        last_sale_date,
        last_to_today_days,
         customer_sign_company_active_status_code  customer_active_sts_cpde
		 customer_sign_company_active_status_name  as  customer_active_sts_name
	from csx_dws.csx_dws_crm_customer_sign_company_active
  where sdt = '${edate}' ) c on x.customer_no=c.customer_code  and x.comp_code = c.sign_company_code
;



INSERT OVERWRITE table csx_analyse.csx_analyse_fr_sap_subject_customer_account_df PARTITION(sdt)
select * from  csx_tmp.temp_account_age_00 ;