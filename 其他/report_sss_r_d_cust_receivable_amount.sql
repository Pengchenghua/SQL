--源单据中已核销金额>=认领单号中核销金额，因存在无认领单核销方式，money_back_id  --回款关联ID为0是微信支付、-1是退货系统核销、-2？
--根据源单据与初始订单的金额、未回款金额、逾期金额，关联核销明细算单据的核销金额
--根据认领单号算认领回款金额（含核销与未核销），关联核销明细算认领金额中的核销金额
--根据原单与认领单算的核销金额可能不一致，存在部分无认领单核销



-- 切换tez计算引擎
set mapred.job.name=report_sss_r_d_cust_receivable_amount;
set hive.execution.engine=tez;
set tez.queue.name=caishixian;

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 昨日、昨日、昨日月1日
--select ${hiveconf:current_day},${hiveconf:current_start_mon},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};
set current_day1 =date_sub(current_date,1);
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set current_start_mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
set created_time =from_utc_timestamp(current_timestamp(),'GMT');	------当前时间
set created_by='raoyanhua';


--临时表：订单应收金额、逾期日期、应收天数
drop table csx_tmp.tmp_cust_order_overdue_dtl_1;
create temporary table csx_tmp.tmp_cust_order_overdue_dtl_1
as
select
  regexp_replace(substr(a.happen_date,1,7),'-','') smonth,
  a.order_no,	-- 来源单号
  a.customer_no,	-- 编码
  a.company_code,	-- 签约公司编码
  a.happen_date,	-- 发生时间		
  a.overdue_date,	-- 逾期时间	
  a.source_statement_amount,	-- 源单据对账金额
  a.money_back_status,	-- 回款状态
  b.payment_amount,  --核销金额
  a.unpaid_amount receivable_amount,	-- 应收金额
  a.account_period_code,	--账期编码 
  a.account_period_name,	--账期名称 
  a.account_period_val,	--账期值
  a.beginning_mark,	--是否期初
  a.bad_debt_amount,	--坏账金额	
  a.over_days	-- 逾期天数
from
(
  select 
  	source_bill_no as order_no,	-- 来源单号
  	customer_code as customer_no,	-- 编码
  	company_code,	-- 签约公司编码
  	happen_date,	-- 发生时间		
  	overdue_date,	-- 逾期时间	
  	source_statement_amount,	-- 源单据对账金额
  	money_back_status,	-- 回款状态
  	unpaid_amount,	-- 未回款金额
  	account_period_code,	--账期编码 
  	account_period_name,	--账期名称 
  	account_period_val,	--账期值
  	'否' as beginning_mark,	--是否期初
  	bad_debt_amount,	--坏账金额
  	if((money_back_status<>'ALL' or (datediff(${hiveconf:current_day1}, overdue_date)+1)>=1),datediff(${hiveconf:current_day1}, overdue_date)+1,0) as over_days	-- 逾期天数
  from csx_ods.source_sss_r_a_source_bill  --对账来源单  -- 全量未处理
  where sdt=${hiveconf:current_day}
  and beginning_mark='1'  	-- 期初标识 0-是 1-否
  --and money_back_status<>'ALL'	
  and date(happen_date)<=${hiveconf:current_day1}
  union all
  select 
  	id as order_no,	-- 来源单号
  	customer_code as customer_no,	-- 编码
  	company_code,	-- 签约公司编码		
  	date_sub(from_unixtime(unix_timestamp(overdue_date,'yyyy-MM-dd hh:mm:ss')),coalesce(account_period_val,0)) as happen_date,	-- 发生时间		
  	overdue_date,	-- 逾期时间	
  	beginning_amount source_statement_amount,	-- 源单据对账金额
  	money_back_status,	-- 回款状态
  	unpaid_amount,	-- 未回款金额
  	account_period_code,	--账期编码 
  	account_period_name,	--账期名称 
  	account_period_val,	--账期值
  	'是' as beginning_mark,	--是否期初	
  	bad_debt_amount,	--坏账金额
  	if((money_back_status<>'ALL' or (datediff(${hiveconf:current_day1}, overdue_date)+1)>=1),datediff(${hiveconf:current_day1}, overdue_date)+1,0) as over_days	-- 逾期天数		
  from csx_ods.source_sss_r_a_beginning_receivable   --期初应收账款表---全量未处理 
  where sdt=${hiveconf:current_day}
  --and money_back_status<>'ALL'	
)a
left join
  (	
--核销流水明细表中已核销金额--来源单中
  select   
    close_bill_no,
	--customer_code as customer_no,company_code,
    sum(payment_amount) payment_amount	--核销金额
  from
  	csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
  where (regexp_replace(substr(happen_date,1,10),'-','')<=${hiveconf:current_day} or happen_date='' or happen_date is NULL)
  and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:current_day} 
  and is_deleted ='0'
  --and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
  group by close_bill_no
  )b on b.close_bill_no=a.order_no
;

--临时表：认领单的回款核销数据
drop table csx_tmp.tmp_cust_claim_bill;
create temporary table csx_tmp.tmp_cust_claim_bill
as
select 
  a.claim_bill_no,
  a.smonth,
  a.customer_no,
  a.company_code,
  a.claim_amount,
  b.payment_amount
from 
  (
  select
    claim_bill_no,		--认领单号
	regexp_replace(substr(claim_time,1,7),'-','') smonth,
	customer_code as customer_no, -- 编码
    company_code, -- 公司代码
    sum(claim_amount) as claim_amount,	--回款金额（含核销与未核销的，含补救单）
    sum(paid_amount) as paid_amount,	--回款已核销金额
    sum(residual_amount) as residual_amount	--回款未核销金额
  from csx_dw.dwd_sss_r_d_money_back -- sdt以过账日期分区，只看20200601及以后的，该表计算回款只计算已核销金额
  where ((sdt>='20200601' and sdt<=${hiveconf:current_day}) 
  or (sdt='19990101' and regexp_replace(substr(posting_time,1,10),'-','')>='20200601' and regexp_replace(substr(posting_time,1,10),'-','')<=${hiveconf:current_day}))
  and regexp_replace(substr(update_time,1,10),'-','')<=${hiveconf:current_day}  --回款以过账日期为准，但每次已核销金额发生变化更新日期都会变化，此表无法查历史回款已核销金额
  and (paid_amount<>'0' or residual_amount<>'0') --剔除补救单和对应原单
  group by claim_bill_no,regexp_replace(substr(claim_time,1,7),'-',''),customer_code,company_code
  )a
left join
  (	
--核销流水明细表中已核销金额
  select   
    claim_bill_no,		--认领单号
	customer_code as customer_no,company_code,
    sum(payment_amount) payment_amount	--核销金额
  from
  	csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
  where (regexp_replace(substr(happen_date,1,10),'-','')<=${hiveconf:current_day} or happen_date='' or happen_date is NULL)
  and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:current_day} 
  and is_deleted ='0'
  --and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
  group by claim_bill_no,customer_code,company_code
  )b on b.claim_bill_no=a.claim_bill_no and b.customer_no=a.customer_no and b.company_code=a.company_code; 

 

--临时表：各月金额
drop table csx_tmp.tmp_cust_receivable_amount_1;
create temporary table csx_tmp.tmp_cust_receivable_amount_1
as	
select
  coalesce(e.sales_region_code,d.sales_region_code,'999') as region_code,
  coalesce(e.sales_region_name,d.sales_region_name,'其他') as region_name,  
  coalesce(e.sales_province_code,d.sales_province_code,'999') as province_code,					 
  coalesce(e.sales_province_name,d.sales_province_name,'其他') as province_name,
  coalesce(e.city_group_code,d.city_group_code,'999') as city_group_code,
  coalesce(e.city_group_name,d.city_group_name,'其他') as city_group_name, 
  a.smonth,
  a.customer_no,
  a.company_code,
  coalesce(e.customer_name,d.shop_name) as customer_name, 
  a.source_statement_amount,	--源单据对账金额
  a.payment_amount,		--已核销金额 
  a.bad_debt_amount,	--坏账金额
  a.receivable_amount,	-- 应收金额
  a.over_amt,	-- 逾期金额
  a.max_over_days,		--最大逾期天数
  a.claim_amount,	--认领回款金额（含核销与未核销的，含补救单）
  a.payment_amount_1		--认领回款中已核销金额   
from
  (			
  select
    a.smonth,
    a.customer_no,
    a.company_code,
    sum(a.source_statement_amount) source_statement_amount,	--源单据对账金额
    sum(a.payment_amount) payment_amount,		--已核销金额 
    sum(a.bad_debt_amount) bad_debt_amount,	--坏账金额
    sum(a.receivable_amount) as receivable_amount,	-- 应收金额
    sum(a.over_amt) as over_amt,	-- 逾期金额
	max(a.max_over_days) max_over_days,		--最大逾期天数
    sum(a.claim_amount) claim_amount,	--认领回款金额（含核销与未核销的，含补救单）
    sum(a.payment_amount_1) payment_amount_1		--认领回款中已核销金额  
  from
    (
    select smonth,customer_no,company_code,
      sum(source_statement_amount) source_statement_amount,	--源单据对账金额
      sum(payment_amount) payment_amount,		--已核销金额 
      sum(bad_debt_amount) bad_debt_amount,	--坏账金额
      --case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
      --case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
      sum(receivable_amount) as receivable_amount,	-- 应收金额
      sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,	-- 逾期金额
	  max(case when receivable_amount>0 then over_days end) max_over_days,		--最大逾期天数
      '' claim_amount,	--认领回款金额（含核销与未核销的，含补救单）
      '' payment_amount_1		--认领回款中已核销金额
    from csx_tmp.tmp_cust_order_overdue_dtl_1
	group by smonth,customer_no,company_code
    union all
    -- 获取回款金额
    select
      smonth,
  	  customer_no, -- 编码
      company_code, -- 公司代码
  	  '' source_statement_amount,	--源单据对账金额
  	  '' payment_amount,		--已核销金额 
  	  '' bad_debt_amount,	--坏账金额
  	  '' receivable_amount,	-- 应收金额
  	  '' over_amt,	-- 逾期金额
	  '' max_over_days,		--最大逾期天数
      sum(claim_amount) as claim_amount,	--认领回款金额（含核销与未核销的，含补救单）
      sum(payment_amount) as payment_amount_1	--认领回款中已核销金额
    from csx_tmp.tmp_cust_claim_bill
    group by smonth,customer_no,company_code
    )a
  group by a.smonth,a.customer_no,a.company_code
  )a
--left join 
--( -- 获取+签约公司的详细信息 账期
--  select * from csx_dw.dws_crm_w_a_customer_company
--  where sdt = 'current'
--)c on a.customer_no = c.customer_no and a.company_code = c.company_code
left join
(
  select * from csx_dw.dws_basic_w_a_csx_shop_m 
  where sdt='current'
)d on a.customer_no=concat('S', d.shop_id)
left join
(
  select * from csx_dw.dws_crm_w_a_customer 
  where sdt='current'
)e on a.customer_no=e.customer_no
;

--结果表 应收账款
insert overwrite table csx_dw.report_sss_r_d_cust_receivable_amount partition(sdt)
--drop table csx_dw.report_sss_r_d_cust_receivable_amount;
--create temporary table csx_dw.report_sss_r_d_cust_receivable_amount
--as
select
  concat_ws('-',${hiveconf:current_day},a.customer_no,a.company_code,a.province_code,a.city_group_code,a.smonth) as biz_id,
  a.region_code,
  a.region_name,  
  a.province_code,					 
  a.province_name,
  a.city_group_code,
  a.city_group_name, 
  g.channel_code,
  g.channel_name,
  a.smonth,
  a.grouping_id,
  a.customer_no,
  a.customer_name,
  a.company_code,
  c.company_name, 
  coalesce(d.payment_terms,f.payment_terms,'-') payment_terms,		--账期类型
  coalesce(d.payment_name,f.payment_name,'-') payment_name,
  coalesce(d.payment_days,f.payment_days,'-') payment_days,
  coalesce(d.payment_short_name,f.payment_short_name,'-') payment_short_name, 
  coalesce(d.credit_limit,f.credit_limit,'-') credit_limit,		--固定信控额度
  coalesce(d.temp_credit_limit,f.temp_credit_limit,'-') temp_credit_limit,		--临时信控额度
  g.first_category_code,		--一级分类编码
  g.first_category_name,		--一级分类名称
  g.second_category_code,		--二级分类编码
  g.second_category_name,		--二级分类名称
  g.third_category_code,		--三级分类编码
  g.third_category_name,		--三级分类名称
  g.sales_id,		--主销售员Id
  g.work_no,		--销售员工号
  g.sales_name,		--销售员名称
  g.first_supervisor_code,		--一级主管编码,B端：销售主管,S端：采购总监 大宗：主管
  g.first_supervisor_work_no,		--一级主管工号
  g.first_supervisor_name,		--一级主管姓名
  g.dev_source_code,		--开发来源编码(1:自营,2:业务代理人,3:城市服务商,4:内购)
  g.dev_source_name,		--开发来源名称
  h.customer_active_status_code,	--活跃状态编码
  h.customer_active_status_name,	--活跃状态
  a.source_statement_amount,	--源单据对账金额
  a.payment_amount,		--已核销金额 
  a.bad_debt_amount,	--坏账金额
  a.receivable_amount,	-- 应收金额
  a.over_amt,	-- 逾期金额
  a.max_over_days,		--最大逾期天数
  a.claim_amount,	--认领回款金额（含核销与未核销的，含补救单）
  a.payment_amount_1,		--认领回款中已核销金额  
  ${hiveconf:created_by} create_by,
  ${hiveconf:created_time} create_time,
  ${hiveconf:created_time} update_time,
  ${hiveconf:current_day} as sdt -- 统计日期  
from 
(
  select 
    region_code,
    region_name,  
    province_code,					 
    province_name,
    city_group_code,
    city_group_name, 
    smonth,
    3 as grouping_id,
    customer_no,
    company_code,
    customer_name, 
    source_statement_amount,	--源单据对账金额
    payment_amount,		--已核销金额 
    bad_debt_amount,	--坏账金额
    receivable_amount,	-- 应收金额
    over_amt,	-- 逾期金额
    max_over_days,		--最大逾期天数
    claim_amount,	--认领回款金额（含核销与未核销的，含补救单）
    payment_amount_1		--认领回款中已核销金额  
  from csx_tmp.tmp_cust_receivable_amount_1
  union all 
  -- 小计、省区合计、城市合计
  select 
    region_code,
    region_name,  
    province_code,					 
    province_name,
    coalesce(city_group_code,'-') as city_group_code,
    coalesce(city_group_name,'-') as city_group_name,
    if(customer_no is null,'合计','小计') smonth, 
    if(customer_no is null,if(city_group_code is null and customer_no is null,0,1),2) grouping_id,
    coalesce(customer_no,'-') as customer_no,
    coalesce(company_code,'-') as company_code,
    coalesce(customer_name,'-') as customer_name, 
    sum(source_statement_amount) source_statement_amount,	--源单据对账金额
    sum(payment_amount) payment_amount,		--已核销金额 
    sum(bad_debt_amount) bad_debt_amount,	--坏账金额
    sum(receivable_amount) receivable_amount,	-- 应收金额
    sum(over_amt) over_amt,	-- 逾期金额
    max(max_over_days) max_over_days,		--最大逾期天数
    sum(claim_amount) claim_amount,	--认领回款金额（含核销与未核销的，含补救单）
    sum(payment_amount_1) payment_amount_1		--认领回款中已核销金额  
  from csx_tmp.tmp_cust_receivable_amount_1
  group by region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,customer_name,company_code
  grouping sets(
    (region_code,region_name,province_code,province_name),
    (region_code,region_name,province_code,province_name,city_group_code,city_group_name),
    (region_code,region_name,province_code,province_name,city_group_code,city_group_name,customer_no,customer_name,company_code))
  --having smonth='小计' or province_name<>city_group_name
)a 
left join -- 结算公司主体
(
  select distinct company_code,company_name 
  from csx_dw.dws_basic_w_a_csx_shop_m 
  where sdt='current'
)c on a.company_code=c.company_code
left join -- 各月信控额度、临时信控额度、账期类型、账期 csx_dw.dws_crm_w_a_customer_company
(
  select distinct a.customer_no,
    a.company_code,
    a.payment_terms,
    a.payment_name,
    a.payment_days,
    a.payment_short_name,
    a.credit_limit,
    a.temp_credit_limit,
    substr(if(a.sdt='current',regexp_replace(current_date,'-',''),a.sdt),1,6) smonth
  from csx_dw.dws_crm_w_a_customer_company a
  right join 
  (
    select customer_no,
      company_code,
      substr(if(sdt='current',regexp_replace(current_date,'-',''),sdt),1,6) smonth,
      max(if(sdt='current',regexp_replace(current_date,'-',''),sdt)) max_sdt
    from csx_dw.dws_crm_w_a_customer_company
    group by customer_no,company_code,substr(if(sdt='current',regexp_replace(current_date,'-',''),sdt),1,6)
  )b on b.customer_no=a.customer_no and b.company_code=a.company_code and b.max_sdt=if(a.sdt='current',regexp_replace(current_date,'-',''),a.sdt)
) d on d.customer_no=a.customer_no and d.company_code=a.company_code and d.smonth=a.smonth
left join -- 小计信控额度、临时信控额度、账期类型、账期 
(
  select distinct customer_no customer_no,
    company_code,
    '小计' smonth,
    payment_terms,
    payment_name,
    payment_days,
    payment_short_name,
    credit_limit,
    temp_credit_limit
  from csx_dw.dws_crm_w_a_customer_company 
  where sdt='current'
)f on f.customer_no=a.customer_no and f.company_code=a.company_code and f.smonth=a.smonth
left join
(
  select * from csx_dw.dws_crm_w_a_customer 
  where sdt='current'
)g on a.customer_no=g.customer_no
left join
(
  select distinct customer_no,sign_company_code,
    last_sales_date,
    last_to_now_days,
    customer_active_status_code,
	case when  customer_active_status_code = 1 then '活跃'
		when customer_active_status_code = 2 then '沉默'
		when customer_active_status_code = 3 then '预流失'
		when customer_active_status_code = 4 then '流失'
		else '其他'
		end  as  customer_active_status_name	--活跃状态
  from csx_dw.dws_sale_w_a_customer_company_active
  where sdt = 'current'
)h on a.customer_no=h.customer_no and a.company_code = h.sign_company_code
order by a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_no,a.company_code,a.grouping_id,a.smonth;







--INVALIDATE METADATA csx_dw.report_sss_r_d_cust_receivable_amount;

/*

---------------------------------------------------------------------------------------------------------
---------------------------------------------hive 建表语句-----------------------------------------------

--应收账款-新系统 csx_dw.report_sss_r_d_cust_receivable_amount

drop table if exists csx_dw.report_sss_r_d_cust_receivable_amount;
create table csx_dw.report_sss_r_d_cust_receivable_amount(
  `biz_id` string COMMENT  '唯一值',
  `region_code` string COMMENT  '大区编码',
  `region_name` string COMMENT  '大区名称',
  `province_code` string COMMENT  '省区编码',
  `province_name` string COMMENT  '省区名称',
  `city_group_code` string COMMENT  '城市组编码',
  `city_group_name` string COMMENT  '城市组名称',
  `channel_code` string COMMENT  '渠道编码',
  `channel_name` string COMMENT  '渠道名称',
  `smonth` string COMMENT  '年月',
  `grouping_id` string COMMENT  '区域粒度编码',
  `customer_no` string COMMENT  '编号',
  `customer_name` string COMMENT  '名称',
  `company_code` string COMMENT  '公司代码',
  `company_name` string COMMENT  '公司名称',
  `payment_terms` string COMMENT  '账期类型',
  `payment_name` string COMMENT  '账期名称',
  `payment_days` string COMMENT  '账期值',
  `payment_short_name` string COMMENT  '账期简称',
  `credit_limit` decimal(26,6)  COMMENT '信控额度',
  `temp_credit_limit` decimal(26,6)  COMMENT '临时额度',
  `first_category_code` string COMMENT  '一级分类编码',
  `first_category_name` string COMMENT  '一级分类名称',
  `second_category_code` string COMMENT  '二级分类编码',
  `second_category_name` string COMMENT  '二级分类名称',
  `third_category_code` string COMMENT  '三级分类编码',
  `third_category_name` string COMMENT  '三级分类名称',
  `sales_id` string COMMENT  '销售员Id',
  `work_no` string COMMENT  '销售员工号',
  `sales_name` string COMMENT  '销售员',
  `first_supervisor_code` string COMMENT  '销售主管编码',
  `first_supervisor_work_no` string COMMENT  '销售主管工号',
  `first_supervisor_name` string COMMENT  '销售主管姓名',
  `dev_source_code` string COMMENT  '开发来源编码(1:自营,2:业务代理人,3:城市服务商,4:内购)',
  `dev_source_name` string COMMENT  '开发来源名称',
  `customer_active_status_code` string COMMENT  '活跃状态编码',
  `customer_active_status_name` string COMMENT  '活跃状态',
  `source_statement_amount` decimal(26,6)  COMMENT '源单据对账金额',
  `payment_amount` decimal(26,6)  COMMENT '已核销金额',
  `bad_debt_amount` decimal(26,6)  COMMENT '坏账金额',
  `receivable_amount` decimal(26,6)  COMMENT '应收金额',
  `over_amt` decimal(26,6)  COMMENT '逾期金额',
  `max_over_days` decimal(26,0)  COMMENT '最大逾期天数',
  `claim_amount` decimal(26,6)  COMMENT '认领回款金额',
  `payment_amount_1` decimal(26,6)  COMMENT '认领回款中已核销金额',
  `create_by` string COMMENT  '创建人',
  `create_time` timestamp comment '创建时间',
  `update_time` timestamp comment '更新时间'
) COMMENT '应收账款-新系统'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;













