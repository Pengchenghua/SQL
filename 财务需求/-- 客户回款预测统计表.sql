-- 客户回款预测统计表20220119
SET hive.execution.engine=spark; 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1200;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=1024000000;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.exec.compress.output=true;
set parquet.compression=SNAPPY;
set mapred.output.compress=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set hive.support.quoted.identifiers=none;

set e_date='${enddate}';
set sdt_1=regexp_replace(trunc(${hiveconf:e_date},'MM'),'-','');		--每月1日日期
set l_sdt=regexp_replace(add_months( trunc(${hiveconf:e_date},'MM'),-1),'-','');		--上月1号日期


-- 1.  预测回款金额=月底预测逾期金额（取1号的预测逾期金额，当月的预测逾期金额保持不变）-特殊原因无法回款金额。
-- 2. 客户号G,V开头的客户也要纳入到月度回款预测。
-- 3. 增加还需回款金额=回款目标值-当期回款金额
-- 4. 在月底预测逾期金额列后增加一列，名称叫“特殊原因无法回款金额”，此金额每月初手动导入（导入字段a签约主体编码，b客户号,c金额）。
-- 5. 城市服务商客户标识取数-数据中心-城市服务商主题-各省区城市服务客户业绩表，匹配逻辑按签约主体+客户号匹配，匹配到即为城市服务商客户（营运资金看板需要按此展示）。
-- 6. 供应链(生鲜)及供应链(食百)及平台-B客户，预测回款金额=上期含税销售金额。（取上期的销售金额，展示的上期完整月的金额，若无销售金额展示为0）
-- 7. 增加法务已介入客户手动标识，点击确认及点击取消（营运资金看板需要按此标识展示）。
-- 8. 表格内需要增加字段已标注颜色


-- 1.1 先计算1号的预测逾期金额
drop table if exists csx_tmp.temp_account_01;
create temporary table csx_tmp.temp_account_01 as 
select a.comp_code,
	   kunnr,
	   ac_all,
	   ac_all_month_last_day,
	   ac_wdq_month_last_day,
	  coalesce(ac_all_month_last_day-ac_wdq_month_last_day,0) as payment_collection_target  --预测回款金额
from csx_dw.account_age_dtl_fct_new_scar a
	where sdt=${hiveconf:sdt_1}
	    and a.ac_all <> 0
		and hkont like '1122%'
;

-- 1.2 计算上月整月销售额 取客户签约公司
drop table if exists csx_tmp.temp_account_02 ;
create temporary table csx_tmp.temp_account_02 as 
	select channel_code,
	       sign_company_code,
		   customer_no,
		   sum(sales_value) sales_value 
	from csx_dw.dws_sale_r_d_detail 
		where sdt>=${hiveconf:l_sdt}
			and sdt<${hiveconf:sdt_1}
			and channel_code in('4','5','6')
	group by channel_code,
			 customer_no,
			 sign_company_code
;

-- 1.3 预测回款目标，大宗、供应链取上个月销售目标
drop table if exists csx_tmp.temp_account_03 ;
create temporary table csx_tmp.temp_account_03 as
select a.comp_code,
	   kunnr as customer_no,
	   ac_all,
	   ac_all_month_last_day,
	   ac_wdq_month_last_day,
	   payment_collection_target,
	  if(b.sales_value is null,a.payment_collection_target,b.sales_value) as cash_collection_targets  
from  csx_tmp.temp_account_01 a 
left join 
 csx_tmp.temp_account_02  b on a.kunnr=b.customer_no and a.comp_code=b.sign_company_code
;


-- 1.4 查找城市服务商取2021年之后的城市服务商
drop table if exists  csx_tmp.temp_channel ;
CREATE temporary table csx_tmp.temp_channel as 
  SELECT  sign_company_code,
         customer_no,
         business_type_name as  sales_channel_name
  FROM csx_dw.dws_sale_r_d_detail
  where  sdt>='20210101'
    and business_type_code='4'
  GROUP BY   sign_company_code,
             customer_no,
             business_type_name

;

-- 1.7 查找客户渠道,城市服务商从销售表取，渠道从客户信息表取
drop table if exists  csx_tmp.temp_channel_04 ;
CREATE temporary table csx_tmp.temp_channel_04 as 
-- 剔除一样的客户 
select   a.sign_company_code,
         a.customer_no,
         a.channel_name as sales_channel_name
from csx_dw.dws_crm_w_a_customer a 
left join 
csx_tmp.temp_channel b on a.customer_no=b.customer_no
where b.customer_no is null
and a.sdt='current'
union all 
select   a.sign_company_code,
         a.customer_no,
         sales_channel_name
from csx_tmp.temp_channel a 
;


-- 处理帐龄数据，将团购客户号单独处理，根据公司代码划分省区
drop table csx_tmp.temp_account_age;
CREATE temporary table csx_tmp.temp_account_age
as
select
    channel ,
    hkont ,
    account_name ,
    comp_code ,
    comp_name ,
    dist ,
    sales_city ,
    customer_no ,
    customer_name ,
    first_category ,
    second_category ,
    third_category ,
    work_no ,
    sales_name ,
    first_supervisor_name,
    -- credit_limit ,
    -- temp_credit_limit ,
    zterm ,
    diff ,
    ac_all ,
    ac_wdq ,
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
    last_sales_date,
	last_to_now_days,
	ac_all_month_last_day,
	ac_wdq_month_last_day,
	customer_active_sts_code,
	customer_active_sts,
	sdt
from
    (
		select
			case
				when b.channel_name is null then '其他'
				else b.channel_name
			end channel , 
			hkont , 
			a.account_name , 
			comp_code , 
			comp_name ,
			case when kunnr in ('G7150')  THEN '平台-食百采购'
				when 
					(b.province_name is null
				and comp_code in( '1933','2116')) then '福建省'
				when (b.province_name is null
				and comp_name like '%北京%' )then concat(substring(comp_name, 1, 2 ), '市')
				when (b.province_name is null )then concat(substring(comp_name, 1, 2 ), '省')
				else b.province_name
			end as dist , 
			b.sales_city_name sales_city , 
			regexp_replace(kunnr, '^0*','') as customer_no, 
			b.customer_name as customer_name , 
			b.first_category , 
			b.second_category , 
			b.third_category , 
			b.work_no , 
			b.sales_name , 
			b.first_supervisor_name , 
			-- b.credit_limit , 
			-- b.temp_credit_limit , 
			zterm , 
			diff , 
			ac_all ,
			case
				when ac_all<0 then ac_all
				else ac_wdq
			end ac_wdq,
			case
				when ac_all<0 then 0
				else ac_15d
			end ac_15d,
			case
				when ac_all<0 then 0
				else ac_30d
			end ac_30d,
			case
				when ac_all<0 then 0
				else ac_60d
			end ac_60d,
			case
				when ac_all<0 then 0
				else ac_90d
			end ac_90d,
			case
				when ac_all<0 then 0
				else ac_120d
			end ac_120d,
			case
				when ac_all<0 then 0
				else ac_180d
			end ac_180d,
			case
				when ac_all<0 then 0
				else ac_365d
			end ac_365d,
			case
				when ac_all<0 then 0
				else ac_2y
			end ac_2y,
			case
				when ac_all<0 then 0
				else ac_3y
			end ac_3y,
			case
				when ac_all<0 then 0
				else ac_over3y
			end ac_over3y,
				last_sales_date,
	        	last_to_now_days,
	        	ac_all_month_last_day,
	        	ac_wdq_month_last_day,
	        	customer_active_sts_code,
	        	customer_active_sts,
	        	sdt
		from
        (
			select
				sflag ,
				hkont ,
				account_name,
				comp_code ,
				comp_name ,
				prctr ,
				shop_name ,
				kunnr ,
				-- NAME        ,
				zterm ,
				diff ,
				ac_all ,
				ac_wdq ,
				ac_15d ,
				ac_30d ,
				ac_60d ,
				ac_90d ,
				ac_120d ,
				ac_180d ,
				ac_365d ,
				ac_2y ,
				ac_3y ,
				ac_over3y ,
	        	last_sales_date,
	        	last_to_now_days,
	        	ac_all_month_last_day,
	        	ac_wdq_month_last_day,
	        	customer_active_sts_code,
	        	case when  customer_active_sts_code = 1 then '活跃客户'
	        		when customer_active_sts_code = 2 then '沉默客户'
	        		when customer_active_sts_code = 3 then '预流失客户'
	        		when customer_active_sts_code = 4 then '流失客户'
	        		else '其他'
	        		end  as  customer_active_sts,
	        	sdt
			from
				csx_dw.account_age_dtl_fct_new_scar a
			where
				a.sdt = regexp_replace(${hiveconf:e_date},'-','')
		
				and a.ac_all <> 0
				and kunnr <> '0000910001'
				and hkont like '1122%'
        ) a
		left join 
	(
  SELECT customer_no, 
  		 customer_name,
  		 channel_code,
  		 channel_name, 
  		 province_code,
  		 province_name,
  		 city_group_code,
  		 city_group_name,
  		 sales_province_code, 
  		 sales_province_name, 
  		 sales_city_code,
  		 sales_city_name,	
  		 first_category_name  as first_category   ,
  		 second_category_name as second_category,
  		 third_category_name  as third_category  ,  
  		 work_no , 
  		 sales_name , 
  		 first_supervisor_name
  FROM csx_dw.dws_crm_w_a_customer
  WHERE sdt = 'current' 
		) b on lpad(a.kunnr, 10, '0')= lpad(b.customer_no, 10, '0')
		
union all
		select
			a.sflag as channel , hkont , a.account_name, comp_code , comp_name ,
			case
				when substr(comp_name, 1, 2)in('上海', '北京', '重庆') then concat(substr(comp_name, 1, 2), '市')
				when substr(comp_name, 1, 2)= '永辉' then '福建省'
				else concat(substr(comp_name, 1, 2), '省')
			end dist , 
			substr(comp_name, 1, 2) sales_city, 
			regexp_replace(kunnr, '^0*', '') as customer_no , 
			name as customer_name , 
			'个人及其他' first_category , 
			'个人及其他' second_category , 
			'个人及其他' third_category , 
			'' work_no , 
			'' sales_name , 
			'' first_supervisor_name, 
-- 			0 as credit_limit, 
-- 			0 as temp_credit_limit, 
			zterm , 
			diff , 
			ac_all,
			case
				when ac_all<0 then ac_all
				else ac_wdq
			end ac_wdq,
			case
				when ac_all<0 then 0
				else ac_15d
			end ac_15d,
			case
				when ac_all<0 then 0
				else ac_30d
			end ac_30d,
			case
				when ac_all<0 then 0
				else ac_60d
			end ac_60d,
			case
				when ac_all<0 then 0
				else ac_90d
			end ac_90d,
			case
				when ac_all<0 then 0
				else ac_120d
			end ac_120d,
			case
				when ac_all<0 then 0
				else ac_180d
			end ac_180d,
			case
				when ac_all<0 then 0
				else ac_365d
			end ac_365d,
			case
				when ac_all<0 then 0
				else ac_2y
			end ac_2y,
			case
				when ac_all<0 then 0
				else ac_3y
			end ac_3y,
			case
				when ac_all<0 then 0
				else ac_over3y
			end ac_over3y,
			last_sales_date,
	        last_to_now_days,
	        ac_all_month_last_day,
	        ac_wdq_month_last_day,
	        customer_active_sts_code,
	        case when  customer_active_sts_code = 1 then '活跃客户'
	        	when customer_active_sts_code = 2 then '沉默客户'
	        	when customer_active_sts_code = 3 then '预流失客户'
	        	when customer_active_sts_code = 4 then '流失客户'
	        else '其他'
	        end  as  customer_active_sts,
	        sdt
		from
			csx_dw.account_age_dtl_fct_new_scar a
		where
				a.sdt = regexp_replace(${hiveconf:e_date},'-','')
		
			and a.ac_all <> 0
			and kunnr = '0000910001' 
	) x
where
    1 = 1 
order by
    channel ,
    comp_code,
    dist ,
    customer_no ;


-- 收入预测
drop table if exists csx_tmp.temp_target_value ;
create temporary table csx_tmp.temp_target_value as 
select 
  customer_id,
  customer_no,
  owner_user_id,
  business_attribute,
  project_code,
  target_code,
  concat_ws('',cast(target_year as string),month) as target_month,
  cast(target_value as decimal(26,6)) as target_value
from 
(
  select 
    a.customer_id as customer_id,
    customer_no,
	owner_user_id,
	business_attribute,
	project_code,
	target_code,
	target_year,
	map('01',january,'02',february,'03',march,'04',april,'05',may,'06',june,
	  '07',july,'08',august,'09',september,'10',october,'11',november,'12',december) as month_map
  from csx_ods.source_crm_r_a_target a 
  left join 
  (select customer_id,customer_no from csx_dw.dws_crm_w_a_customer where sdt='current') b on a.customer_id=b.customer_id
  where sdt = regexp_replace(date_sub(current_date,1),'-','')
    
   --  and target_code in (1,3)    --存量与增量客户
    and project_code in (1)     --取预测销售额
    -- and target_year >= '2022'
) a lateral VIEW explode(month_map) col1s AS month,target_value
;



-- select * from csx_tmp.source_data_analysis_prd_province_overdue_rate_target;

 
drop table if exists csx_tmp.temp_account;
create temporary table csx_tmp.temp_account as 
select 
   a.channel  as channel_name,
   a.hkont  as hkont,
   a.account_name  as account_name,
   a.comp_code  as comp_code,
   a.comp_name as  comp_name,
   CASE WHEN a.customer_no LIKE 'V%' then c.province_code else  coalesce(d.province_code,   b.province_code ) end   as  province_code,		--省区编码
   CASE WHEN a.customer_no LIKE 'V%' then c.province_name else   coalesce(d.province_name ,  dist ) end province_name,
   CASE WHEN a.customer_no LIKE 'V%' then '-' else  coalesce(d.city_group_name,a.sales_city) end as sales_city,
   '' as prctr,			--成本中心
   '' as shop_name,
   a.customer_no ,
   a.customer_name ,
   a.first_category ,
   a.second_category ,
   a.third_category ,
   a.work_no ,
   a.sales_name ,
   a.first_supervisor_name,
   b.credit_limit ,
   b.temp_credit_limit ,
	 payment_terms,
   payment_name,
   payment_days,
   a.zterm ,
   a.diff ,
   a.ac_all ,
   a.ac_wdq ,
   a.ac_15d ,
   a.ac_30d ,
   a.ac_60d ,
   a.ac_90d ,
   a.ac_120d ,
   a.ac_180d ,
   a.ac_365d ,
   a.ac_2y ,
   a.ac_3y ,
   a.ac_over3y,
   a.last_sales_date,
   a.last_to_now_days,
   a.customer_active_sts_code as customer_active_sts_code,  --客户活跃状态标签编码（1 活跃客户；2 沉默客户；3预流失客户；4 流失客户）
   a.customer_active_sts as customer_active_sts,
	ac_all_month_last_day,
    ac_wdq_month_last_day,
    (ac_all_month_last_day-ac_wdq_month_last_day) as ac_overdue_month_last_day,  --预测逾期金额
    coalesce(target_value,0) as target_sale_value  ,   --预测收入
	current_timestamp() as update_time, 
	a.sdt
from 
(
	select 
		*
	from 
		csx_tmp.temp_account_age
) as a 
left  join 
(	
	select 
		customer_no,
		company_code,
		payment_terms,
		payment_name,
		credit_limit,
		temp_credit_limit,
		payment_days,
		province_code,
		province_name,
		city_code,
		city_name
	from 
		csx_dw.dws_crm_w_a_customer_company   --客户账期表
	where 
		sdt='current'
) as b 	on a.customer_no =b.customer_no and a.comp_code=b.company_code
left join 
(select customer_no,sum(target_value)target_value, target_month,project_code 
    from csx_tmp.temp_target_value 
        where project_code='1'
            and target_month=substr(regexp_replace(${hiveconf:e_date},'-',''),1,6) 
    group by customer_no,target_month,project_code
    )t on a.customer_no=t.customer_no
left join
( -- 获取管理大区、省区与城市组信息
  SELECT
    city_code, 
    area_location_code, 
    city_group_code, 
    city_group_name,
    province_code, 
    province_name, 
    region_code, 
    region_name,
    city_name
  FROM csx_dw.dws_sale_w_a_area_belong g 
) d on  a.sales_city = d.city_name
	 and a.dist=d.province_name
 left join
( -- 获取公司所在地理省区城市
  select distinct  a.code,
  a.province_code,
    a.province_name
  from csx_dw.dws_basic_w_a_company_code a
  where a.table_type=1
  and sdt='current'
) c on a.comp_code = c.code
;

 
-- insert overwrite table csx_tmp.ads_fr_r_d_account_receivables_scar_20211223 partition (sdt)
drop table csx_tmp.temp_account_ads;
create  table csx_tmp.temp_account_ads as 
select 
   channel_name,
   coalesce(c.sales_channel_name,channel_name) sales_channel_name,
   hkont,
   account_name,
   a.comp_code,
   a.comp_name,
   a.province_code,		--省区编码
   a.province_name,
   a.sales_city,
   prctr,			--成本中心
   shop_name,
   a.customer_no ,
   customer_name ,
   first_category ,
   second_category ,
   third_category ,
   work_no ,
   sales_name ,
   first_supervisor_name,
   credit_limit ,
   temp_credit_limit ,
   payment_terms,
   payment_name,
   payment_days,
   zterm ,
   diff ,
   a.ac_all ,
   ac_wdq ,
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
   last_sales_date,
   last_to_now_days,
   customer_active_sts_code,  --客户活跃状态标签编码（1 活跃客户；2 沉默客户；3预流失客户；4 流失客户）
   customer_active_sts,
   a.ac_all_month_last_day,
   a.ac_wdq_month_last_day,
   a.ac_overdue_month_last_day,  --预测逾期金额
   round(target_sale_value*10000,2)  target_sale_value ,   --预测收入
   coalesce( cash_collection_targets,0) as forecast_returned_value , --预测回款金额目标
   coalesce(loss_amount,0)loss_amount,         --无法回款金额
   coalesce(law_is_flag,0)law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time, 
	a.sdt
from  csx_tmp.temp_account a 
left join 
(select a.comp_code,
	   customer_no,
	   sum(ac_all)ac_all,
	   sum(ac_all_month_last_day)ac_all_month_last_day,
	   sum(ac_wdq_month_last_day)ac_wdq_month_last_day,
	   sum(payment_collection_target)payment_collection_target,
	   sum(cash_collection_targets )cash_collection_targets  
from csx_tmp.temp_account_03 a 
    group by a.comp_code,
	   customer_no
) b on a.customer_no=b.customer_no and a.comp_code=b.comp_code 
left join 
csx_tmp.temp_channel_04 c on a.customer_no=c.customer_no and a.comp_code=c.sign_company_code
left join
    (select  company_code,customer_no,is_flag as law_is_flag 
    from csx_tmp.source_fr_w_a_customer_legallegal_intervene 
        where sdt=regexp_replace(${hiveconf:e_date},'-','')) d on  a.customer_no=d.customer_no and a.comp_code=d.company_code
left join 
    (select company_code,customer_no,sum(amount) as loss_amount
    from csx_tmp.source_fr_w_a_customer_unable_payment_collection 
    where sdt=regexp_replace(${hiveconf:e_date},'-','') 
    group by company_code,customer_no ) f on a.customer_no=f.customer_no and a.comp_code=f.company_code
left join 
(select province_code,
    province_name,months, 
    round(overdue_rate_target/100,4) overdue_rate_target 
from csx_tmp.source_data_analysis_prd_province_overdue_rate_target
 where sdt=regexp_replace(date_sub(current_date(),1),'-','') 
    and months=substr(regexp_replace(${hiveconf:e_date},'-',''),1,6) ) k on a.province_code=k.province_code 

;






select comp_code,customer_no,aa
from (
select comp_code,customer_no,count(1) aa  from csx_tmp.temp_account_ads
group by comp_code,customer_no
)a 
where aa>1

;

select *  from csx_tmp.temp_account_ads where customer_no='104114';


show create table csx_tmp.ads_fr_r_d_account_receivables_scar_20211223 ;

      
  drop table       `csx_tmp.ads_fr_r_d_forecast_collection_report`;
CREATE TABLE `csx_tmp.ads_fr_r_d_forecast_collection_report`(
  `channel_name` string COMMENT '客户类型', 
  `sales_channel_name` string COMMENT '客户销售渠道,增加城市服务商,其他根据CRM渠道', 
  `hkont` string COMMENT '科目代码', 
  `account_name` string COMMENT '科目名称', 
  `comp_code` string COMMENT '公司代码', 
  `comp_name` string COMMENT '公司名称', 
  `province_code` string COMMENT '销售省区编码', 
  `province_name` string COMMENT '销售省区名称', 
  `sales_city` string COMMENT '销售城市名称', 
  `prctr` string COMMENT '利润中心', 
  `shop_name` string COMMENT '利润中心名称', 
  `customer_no` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `first_category` string COMMENT '第一分类', 
  `second_category` string COMMENT '第二分类', 
  `third_category` string COMMENT '第三分类', 
  `work_no` string COMMENT '销售员工号', 
  `sales_name` string COMMENT '销售员姓名', 
  `first_supervisor_name` string COMMENT '销售主管', 
  `credit_limit` decimal(26,4) COMMENT '信控额度', 
  `temp_credit_limit` decimal(26,4) COMMENT '临时信控额度', 
  `payment_terms` string COMMENT '付款条件', 
  `payment_name` string COMMENT '付款条件名称', 
  `payment_days` string COMMENT '帐期', 
  `zterm` string COMMENT '账期类型', 
  `diff` string COMMENT '账期', 
   ac_all decimal(26,4) COMMENT '应收金额',
   ac_wdq decimal(26,4) COMMENT '未到期金额',
  `ac_all_month_last_day` decimal(26,4) COMMENT '月底预测应收账款', 
  `ac_wdq_month_last_day` decimal(26,4) COMMENT '月底预测未到期账款', 
   ac_overdue_month_last_day decimal(26,4) comment '月底预测逾期金额',
   ac_overdue_month_last_day_rate decimal(26,4) comment '月底预测逾期率',
   target_sale_value decimal(26,4) comment '预测收入',
   receivable_amount_target decimal(26,6) comment'回款目标:取1号预测回款金额',
   unreceivable_amount decimal(26,4) comment '无法回款金额',
   current_receivable_amount decimal(26,4) comment '当期回款金额',
   need_receivable_amount DECIMAL(26,6) COMMENT'可回款金额:回款目标-当期回款金额',
   temp_1 DECIMAL(26,6) COMMENT'预留',
   temp_2 DECIMAL(26,6) COMMENT'预留',
   temp_3 DECIMAL(26,6) COMMENT'预留',
   law_is_flag INT COMMENT '法务介入标识',
  `update_time` timestamp COMMENT '更新时间'
  )
COMMENT '预测回款金额-帆软'
PARTITIONED BY ( 
  `sdt` string COMMENT '日期分区')
STORED AS parquet 
 
    ; 

CREATE TABLE `csx_tmp.source_fr_w_a_customer_unable_payment_collection` (
  `id` string COMMENT '唯一ID ',
  `months` string   COMMENT '月份',
  `company_code` string COMMENT '公司代码',
  `customer_no` string COMMENT '客户',
  `amount` decimal(12,4)  COMMENT '无法回款金额',
  `create_by` string COMMENT '创建人',
  `create_time` TIMESTAMP comment '创建时间',
  `update_time`TIMESTAMP comment '更新时间',
  `update_by` string COMMENT '更新人'
)
COMMENT'财务客户无法回款入口表'
PARTITIONED BY (sdt STRING COMMENT '同步日期')
;

CREATE TABLE `csx_tmp.source_fr_w_a_customer_legallegal_intervene` (
  `id` string COMMENT '唯一ID company_code&customer_no ',
  `company_code`string COMMENT '公司代码',
  `customer_no`string COMMENT '客户',
  `is_flag` int COMMENT '是否介入 0 否，1是',
  `create_by`string COMMENT '创建人',
  `create_time` timestamp COMMENT '创建时间',
  `update_time` timestamp COMMENT '更新时间',
  `update_by` string comment'sys' 
)  COMMENT'财务_法务介入客户'
partitioned by (sdt string comment '同步日期分区')


hive_database=csx_tmp
table_name=source_fr_w_a_customer_legallegal_intervene 
day=`date -d "yesterday" +%Y%m%d` 
username="dataanprd_all"
password="slH25^672da"
sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false  \
 --username "$username" \
 --password "$password" \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/sdt=${day} \
 --delete-target-dir \
 --query "select id,company_code,customer_no,is_flag,create_by,create_time,update_time,update_by  from source_fr_w_a_customer_legallegal_intervene where \$CONDITIONS" \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --hive-partition-key sdt \
 --hive-partition-value "$day" \
 --split-by id
;


hive_database=csx_tmp
table_name=source_fr_w_a_customer_unable_payment_collection 
day=`date -d "yesterday" +%Y%m%d` 
username="dataanprd_all"
password="slH25^672da"
sqoop import \
 --connect jdbc:mysql://10.0.74.77:7477/data_analysis_prd?tinyInt1isBit=false  \
 --username "$username" \
 --password "$password" \
 --fields-terminated-by '\001' \
 --target-dir hdfs://nameservice1/user/hive/warehouse/csx_tmp.db/${table_name}/sdt=${day} \
 --delete-target-dir \
 --query "select id,months,company_code,customer_no,amount,create_by,create_time,update_time,update_by  from source_fr_w_a_customer_unable_payment_collection where \$CONDITIONS" \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --hive-partition-key sdt \
 --hive-partition-value "$day" \
 --split-by id