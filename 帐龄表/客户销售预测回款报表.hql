-- 客户销售预测回款报表
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
  SELECT customer_no, customer_name,
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
    
    and target_code in (1,3)    --存量与增量客户
    and project_code in (1)     --取预测销售额
    -- and target_year >= '2022'
) a lateral VIEW explode(month_map) col1s AS month,target_value
;


-- 逾期目标率
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

 
insert overwrite table csx_tmp.ads_fr_r_d_account_receivables_scar_20211223 partition (sdt)
select 
   channel_name,
   hkont,
   account_name,
   comp_code,
   comp_name,
   a.province_code,		--省区编码
   a.province_name,
   a.sales_city,
   prctr,			--成本中心
   shop_name,
   customer_no ,
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
   customer_active_sts_code,  --客户活跃状态标签编码（1 活跃客户；2 沉默客户；3预流失客户；4 流失客户）
   customer_active_sts,
   ac_all_month_last_day,
   ac_wdq_month_last_day,
   ac_overdue_month_last_day,  --预测逾期金额
   round(target_sale_value*10000,2)  target_sale_value ,   --预测收入
   overdue_rate_target ,    --逾期率目标
    --预测回款金额 (月底预测逾期金额-预测逾期目标率*预测应收金额)/(1-预测逾期率目标)
   ((ac_all_month_last_day-ac_wdq_month_last_day) - (overdue_rate_target*ac_all_month_last_day))/(1-overdue_rate_target) as forecast_returned_value , --预测回款金额
	current_timestamp() as update_time, 
	a.sdt
from  csx_tmp.temp_account a 
left join 
(select province_code,province_name,months, round(overdue_rate_target/100,4) overdue_rate_target from csx_tmp.source_data_analysis_prd_province_overdue_rate_target where sdt=regexp_replace(date_sub(current_date(),1),'-','') and months=substr(regexp_replace(${hiveconf:e_date},'-',''),1,6) ) b on a.province_code=b.province_code 
;

select distinct target_sale_value from csx_tmp.ads_fr_r_d_account_receivables_scar_20211223;


-- 省区逾期目标率
select * from csx_tmp.source_data_analysis_prd_province_overdue_rate_target;

--帐龄表
select * from 
    csx_tmp.ads_fr_r_d_account_receivables_scar	where sdt='20211222';

CREATE TABLE `csx_tmp.ads_fr_r_d_account_receivables_scar`(
  `channel_name` string COMMENT '客户类型', 
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
  `ac_all` decimal(26,4) COMMENT '全部账款', 
  `ac_wdq` decimal(26,4) COMMENT '未到期账款', 
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
  `customer_active_sts_code` string COMMENT '客户活跃状态标签编码（1 活跃客户；2 沉默客户；3预流失客户；4 流失客户）', 
  `customer_active_sts` string COMMENT '客户活跃状态名称', 
  `ac_all_month_last_day` decimal(26,4) COMMENT '月底全部账款', 
  `ac_wdq_month_last_day` decimal(26,4) COMMENT '月底未到期账款', 
   ac_overdue_month_last_day decimal(26,4) comment '月底预测逾期金额',
   ac_overdue_month_last_day_rate decimal(26,4) comment '月底预测逾期率',
   forecast_sale_value decimal(26,4) comment '预测收入',
   overdue_rate_target decimal(26,6) comment'省区预测目标',
  `update_time` timestamp COMMENT '更新时间'
  )
COMMENT '应收帐龄结果表-帆软使用（新逻辑）'
PARTITIONED BY ( 
  `sdt` string COMMENT '日期分区')
STORED AS parquet 
 
    ; 
    
CREATE TABLE `csx_tmp.source_data_analysis_prd_province_overdue_rate_target` (
  `id` bigint  AUTO_INCREMENT COMMENT 'ID',
  `months` string COMMENT '月份',
  `region_code` string  COMMENT '大区编码',
  `region_name` string  COMMENT '大区名称',
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区名称',
  `overdue_rate_target` decimal(26,6)   COMMENT '逾期目标率',
  `update_time` timestamp   COMMENT '更新日期',
  `create_time` timestamp  COMMENT '创建时间',
  `create_by` string    COMMENT '创建人',
  `update_by` string   COMMENT '更新人'
  
)  COMMENT='省区逾期率目标'
partitioned by (sdt string comment '全量分区，同步日期');



hive_database=csx_tmp
table_name=source_data_analysis_prd_province_overdue_rate_target 
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
 --query "select id,months,region_code,region_name,province_code,province_name,overdue_rate_target,update_time,create_time,create_by,update_by  from province_overdue_rate_target where \$CONDITIONS" \
 --hive-drop-import-delims \
 --hive-import \
 --hive-database $hive_database \
 --hive-table $table_name \
 --hive-partition-key sdt \
 --hive-partition-value "$day" \
 --split-by id
