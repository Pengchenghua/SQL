    -- 帐龄【20220512】切换底层
     create table  csx_tmp.ads_fr_r_d_account_receivables_scar_new
     (
      channel_name string COMMENT '类型', 
	  `hkont` string COMMENT '科目代码', 
	  `account_name` string COMMENT '科目名称', 
	  `comp_code` string COMMENT '公司代码', 
	  `comp_name` string COMMENT '公司名称', 
      region_name string COMMENT '大区编码',
      region_name string COMMENT '大区名称',
	  `province_code` string COMMENT '销售省区编码', 
	  `province_name` string COMMENT '销售省区名称', 
	  `sales_city` string COMMENT '销售城市名称', 
	  `prctr` string COMMENT '利润中心', 
	  `shop_name` string COMMENT '利润中心名称', 
	  `customer_no` string COMMENT '编码', 
	  `customer_name` string COMMENT '名称', 
       attribute_desc string COMMENT '属性',
       first_category_code string COMMENT '第一分类编码',
	  `first_category` string COMMENT '第一分类', 
      second_category_code string COMMENT '第二分类编码',
	  `second_category` string COMMENT '第二分类', 
      third_category_code string COMMENT '第三分类编码',
	  `third_category` string COMMENT '第三分类', 
	  `work_no` string COMMENT '销售员工号', 
	  `sales_name` string COMMENT '销售员姓名', 
	  `first_supervisor_name` string COMMENT '销售主管',
       rp_service_user_name_new string COMMENT '日配服务管家名称',
       rp_service_user_work_no_new string COMMENT '日配服务管家工号',
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
	  `customer_active_sts_code` string COMMENT '活跃状态标签编码（1 活跃；2 沉默；3预流失；4 流失）', 
	  `customer_active_sts` string COMMENT '活跃状态名称', 
	  `ac_all_month_last_day` decimal(26,4) COMMENT '月底全部账款', 
	  `ac_wdq_month_last_day` decimal(26,4) COMMENT '月底未到期账款', 
	  `max_overdue_day` string COMMENT '最大逾期天数', 
	  `update_time` timestamp COMMENT '更新时间')
	COMMENT '应收帐龄结果表-帆软使用（新逻辑）'
	PARTITIONED BY ( 
	  `sdt` string COMMENT '日期分区')

columns='channel_name,hkont,account_name,comp_code,comp_name,region_code,region_name,province_code,province_name,sales_city,prctr,shop_name,customer_no,customer_name,attribute_desc,first_category_code,first_category,second_category_code,second_category,third_category_code,third_category,work_no,sales_name,first_supervisor_name,rp_service_user_name_new,rp_service_user_work_no_new,credit_limit,temp_credit_limit,payment_terms,payment_name,payment_days,zterm,diff,ac_all,ac_wdq,ac_15d,ac_30d,ac_60d,ac_90d,ac_120d,ac_180d,ac_365d,ac_2y,ac_3y,ac_over3y,last_sales_date,last_to_now_days,customer_active_sts_code,customer_active_sts,ac_all_month_last_day,ac_wdq_month_last_day,max_overdue_day,update_time,sdt'
day=2022-05-11
yesterday=${day//-/}
sqoop export \
--connect "jdbc:mysql://10.0.74.77:7477/data_analysis_prd?useUnicode=true&characterEncoding=utf-8" \
--username dataanprd_all \
--password 'slH25^672da' \
--table ads_fr_r_d_account_receivables_scar \
--m 64 \
--hcatalog-database csx_tmp \
--hcatalog-table ads_fr_r_d_account_receivables_scar_new \
--hive-partition-key sdt \
--hive-partition-value "$yesterday" \
--input-null-string '\\N'  \
--input-null-non-string '\\N' \
--columns "${columns}"

drop table csx_tmp.temp_account_age;
CREATE temporary table csx_tmp.temp_account_age
as
select
    x.channel_name channel ,
    x.subjects_code hkont ,
    account_name ,
    x.company_code as comp_code ,
    x.company_name as comp_name ,
    x.region_code,
    x.region_name,
    x.province_code,
    x.province_name ,
    x.city_group_name as sales_city ,
    '' as prctr,			--成本中心
    '' as shop_name,
    x.customer_code customer_no ,
    x.customer_name ,
    attribute_desc,
    first_category_code,
    first_category_name first_category ,
    second_category_code,
    second_category_name second_category ,
    third_category_code,
    third_category_name third_category ,
    work_no ,
    sales_name ,
    first_supervisor_name,
    x.credit_limit ,
    x.temp_credit_limit ,
    payment_terms,
    payment_name,
    payment_days,
    account_period_code as zterm,   -- 帐期类型
    account_period_val as diff,     -- 帐期天数
    sum(receivable_amount) ac_all ,
    sum(non_overdue_amount) ac_wdq ,
    sum(overdue_amount1)  ac_15d ,
    sum(overdue_amount15) ac_30d ,
    sum(overdue_amount30) ac_60d ,
    sum(overdue_amount60) ac_90d ,
    sum(overdue_amount90) ac_120d ,
    sum(overdue_amount120)  ac_180d ,
    sum(overdue_amount180)  ac_365d ,
    sum(overdue_amount365) ac_2y ,
    sum(overdue_amount730) ac_3y ,
    sum(overdue_amount1095) ac_over3y,
    sum(ac_all_month_last_day) ac_all_month_last_day,
    sum(ac_wdq_month_last_day) ac_wdq_month_last_day,
	max(max_overdue_day)max_overdue_day,
	x.sdt
from
    csx_dw.dws_sap_r_d_subjects_customer_settle_detail x
LEFT JOIN 
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
  first_category_code,
  first_category_name    ,
  second_category_code,
  second_category_name ,
  third_category_code,
  third_category_name ,
  attribute_desc
  FROM csx_dw.dws_crm_w_a_customer
  WHERE sdt = 'current' ) b on x.customer_code=b.customer_no
LEFT JOIN
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
		csx_dw.dws_crm_w_a_customer_company   --账期表
	where 
		sdt='current'
) d on x.customer_code=d.customer_no and x.company_code=d.company_code
left join
(select code as accunt_code,name as account_name from csx_ods.source_basic_w_a_md_accounting_subject where sdt=regexp_replace(${hiveconf:e_date}, '-', '')) as  e
on x.subjects_code=e.accunt_code
where 
    x.sdt= regexp_replace(${hiveconf:e_date},'-','')
GROUP BY   x.channel_name   ,
    x.subjects_code   ,
    account_name ,
    x.company_code  ,
    x.company_name  ,
    x.region_code,
    x.region_name,
    x.province_code,
    x.province_name ,
    x.city_group_name     ,
    x.sdt,
    x.customer_code   ,
    x.customer_name ,
    attribute_desc,
    first_category_code,
    first_category_name   ,
    second_category_code,
    second_category_name   ,
    third_category_code,
    third_category_name   ,
    work_no ,
    sales_name ,
    first_supervisor_name,
    x.credit_limit ,
    x.temp_credit_limit ,
    payment_terms,
    payment_name,
    payment_days,
    account_period_code   ,   -- 帐期类型
    account_period_val 
    ;


drop table csx_tmp.temp_account_age_00;
CREATE temporary table csx_tmp.temp_account_age_00
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
    from  csx_tmp.report_crm_w_a_customer_service_manager_info_business_new a 
    where month= substr(regexp_replace(${hiveconf:e_date},'-',''),1,6) )  b on x.customer_no=b.customer_no
LEFT JOIN
(
  select customer_no,
        sign_company_code, 
        last_sales_date,
        last_to_now_days,
         customer_active_status_code  customer_active_sts_code,
    case when  customer_active_status_code = 1 then '活跃'
	    when customer_active_status_code = 2 then '沉默'
	    when customer_active_status_code = 3 then '预流失'
	    when customer_active_status_code = 4 then '流失'
	    else '其他'
	end  as  customer_active_sts
	from csx_dw.dws_sale_w_a_customer_company_active
  where sdt = regexp_replace(${hiveconf:e_date},'-','') ) c on x.customer_no=c.customer_no  and x.comp_code = c.sign_company_code
;

 set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE table csx_tmp.ads_fr_r_d_account_receivables_scar_new PARTITION(sdt)
select * from  csx_tmp.back_ads_fr_r_d_account_receivables_scar_20220512
where sdt<'20220511'
;

alter table csx_tmp.ads_fr_r_d_account_receivables_scar_new rename to csx_tmp.ads_fr_r_d_account_receivables_scar;


select * from  csx_tmp.ads_fr_r_d_account_receivables_scar ;


drop table csx_tmp.back_ads_fr_r_d_account_receivables_scar_20220512;
create table  csx_tmp.back_ads_fr_r_d_account_receivables_scar_20220512
as 
select x.channel_name ,
    hkont ,
    account_name ,
    comp_code ,
    comp_name ,
    coalesce(region_code,'') region_code,
    coalesce(region_name,'') region_name,
    coalesce(f.province_code,'') province_code,
    coalesce(f.province_name ,'') province_name ,
    coalesce(f.city_group_name ,'') sales_city ,
    prctr,			--成本中心
    shop_name,
    x.customer_no ,
    coalesce(x.customer_name ,'') customer_name ,
    coalesce(attribute_desc,'') attribute_desc,
    first_category_code,
    coalesce(first_category ,'') first_category ,
    second_category_code,
    coalesce(second_category ,'') second_category ,
    third_category_code,
    coalesce(third_category ,'') third_category ,
    coalesce(work_no ,'') work_no ,
    coalesce(sales_name ,'') sales_name ,
    coalesce(rp_service_user_name_new,'') rp_service_user_name_new,
    coalesce(rp_service_user_work_no_new,'') rp_service_user_work_no_new,
    coalesce(first_supervisor_name,'') first_supervisor_name,
    coalesce(x.credit_limit ,'')    credit_limit ,
    coalesce(x.temp_credit_limit, '') temp_credit_limit ,
    coalesce(payment_terms,'') payment_terms,
    coalesce(payment_name,'') payment_name,
    coalesce(payment_days,'') payment_days,
    coalesce(zterm,'') zterm,                -- 帐期类型
    coalesce(diff,'') diff,                 -- 帐期天数
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
    coalesce(last_sales_date,'') last_sales_date,
	coalesce(last_to_now_days,'') last_to_now_days,
	coalesce(customer_active_sts_code,'') customer_active_sts_code,
    coalesce(customer_active_sts,'') customer_active_sts,
	coalesce(ac_all_month_last_day,'') ac_all_month_last_day,
	coalesce(ac_wdq_month_last_day,'') ac_wdq_month_last_day,
	coalesce(max_overdue_day,'') max_overdue_day,
	current_timestamp() updata_time,
	x.sdt
from  csx_tmp.ads_fr_r_d_account_receivables_scar x
left join 
(select a.customer_no,
        a.city_group_code,
        a.rp_service_user_name_new,
        a.rp_service_user_work_no_new
    from  csx_tmp.report_crm_w_a_customer_service_manager_info_business_new a 
    where month= substr(regexp_replace(${hiveconf:e_date},'-',''),1,6) )  b on x.customer_no=b.customer_no
LEFT JOIN 
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
  first_category_code,
  second_category_code,
  third_category_code,
  first_category_name    ,
  second_category_name ,
  third_category_name ,
  attribute_desc
  FROM csx_dw.dws_crm_w_a_customer
  WHERE sdt = 'current' ) d on x.customer_no=d.customer_no
 left join
 ( -- 获取管理大区、省区与城市组信息
  SELECT  company_code,
    customer_code,
    province_code,
    province_name,
    region_code,
    region_name,
    city_group_name
  FROM  csx_dw.dws_sap_r_d_subjects_customer_settle_detail
  where sdt='20220511'
  group by  company_code,
    customer_code,
    province_code,
    province_name,
    region_code,
    region_name,
    city_group_name
) f on  x.customer_no = f.customer_code and x.comp_code=f.company_code
;


 select * from csx_tmp.back_ads_fr_r_d_account_receivables_scar_20220512;

select 
province_name,
sum(ac_all) ac_all,
sum(ac_wdq )ac_wdq ,
sum(ac_15d )ac_15d ,
sum(ac_30d )ac_30d ,
sum(ac_60d )ac_60d ,
sum(ac_90d )ac_90d ,
sum(ac_120d )ac_120d ,
sum(ac_180d )ac_180d ,
sum(ac_365d )ac_365d ,
sum(ac_2y )ac_2y ,
sum(ac_3y )ac_3y ,
sum(ac_over3y)ac_over3y 
from  csx_tmp.back_ads_fr_r_d_account_receivables_scar_20220512
where sdt='20220511'
group by province_name
;

select * from  csx_tmp.temp_account_age_00 where customer_no='100326';
select * from csx_tmp.temp_account_age where customer_no='100326';
    
    select 
province_name,
sum(ac_all) ac_all,
sum(ac_wdq )ac_wdq ,
sum(ac_15d )ac_15d ,
sum(ac_30d )ac_30d ,
sum(ac_60d )ac_60d ,
sum(ac_90d )ac_90d ,
sum(ac_120d )ac_120d ,
sum(ac_180d )ac_180d ,
sum(ac_365d )ac_365d ,
sum(ac_2y )ac_2y ,
sum(ac_3y )ac_3y ,
sum(ac_over3y)ac_over3y 
from  csx_tmp.temp_account_age_00
where sdt='20220511'
group by province_name
;
select 
sum(ac_wdq )ac_wdq ,
sum(ac_15d )ac_15d ,
sum(ac_30d )ac_30d ,
sum(ac_60d )ac_60d ,
sum(ac_90d )ac_90d ,
sum(ac_120d )ac_120d ,
sum(ac_180d )ac_180d ,
sum(ac_365d )ac_365d ,
sum(ac_2y )ac_2y ,
sum(ac_3y )ac_3y ,
sum(ac_over3y)ac_over3y
from  csx_tmp.temp_account_age_00
union all 
select 
sum(ac_wdq )ac_wdq ,
sum(ac_15d )ac_15d ,
sum(ac_30d )ac_30d ,
sum(ac_60d )ac_60d ,
sum(ac_90d )ac_90d ,
sum(ac_120d )ac_120d ,
sum(ac_180d )ac_180d ,
sum(ac_365d )ac_365d ,
sum(ac_2y )ac_2y ,
sum(ac_3y )ac_3y ,
sum(ac_over3y)ac_over3y
    from  csx_tmp.ads_fr_r_d_account_receivables_scar where sdt='20220511'
;

select 
province_name,
sum(ac_all) ac_all,
sum(ac_wdq )ac_wdq ,
sum(ac_15d )ac_15d ,
sum(ac_30d )ac_30d ,
sum(ac_60d )ac_60d ,
sum(ac_90d )ac_90d ,
sum(ac_120d )ac_120d ,
sum(ac_180d )ac_180d ,
sum(ac_365d )ac_365d ,
sum(ac_2y )ac_2y ,
sum(ac_3y )ac_3y ,
sum(ac_over3y)ac_over3y 
from  csx_tmp.temp_account_age_00
where 1=1
group by province_name
;