--帐龄旧平台20221012
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;

set e_date='${enddate}';

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
    b.customer_level  ,           -- 新增
    b.customer_level_name ,
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
   account_period_code payment_terms,
   account_period_name payment_name,
   account_period_val  payment_days,
    account_period_code as zterm,   -- 帐期类型
    account_period_val as diff,     -- 帐期天数
  
    max(`back_money_amount_total`)  back_money_amount_total,   -- 总回款金额新增
    max(`unpaid_amount_total`)  unpaid_amount_total ,         -- 总销售新增
    sum(`remaining_back_amount`) remaining_back_amount  ,  -- 新增
    sum(`back_money_amount_month`) back_money_amount_month  ,    -- 新增
    sum(`unpaid_amount_month` ) unpaid_amount_month,     -- 新增
    sum(receivable_amount) ac_all ,
    sum(non_overdue_amount) ac_wdq ,
    sum(`overdue_amount`) `overdue_amount`,    
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
  first_category_code,
  first_category_name    ,
  second_category_code,
  second_category_name ,
  third_category_code,
  third_category_name ,
  attribute_desc,
  customer_level  ,           -- 新增
  `customer_level_name`    -- 新增
  FROM csx_dw.dws_crm_w_a_customer
  WHERE sdt = 'current' ) b on x.customer_code=b.customer_no
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
     account_period_code,
     account_period_name,
     account_period_val ,
    account_period_code   ,   -- 帐期类型
    account_period_val ,
    b.customer_level  ,           -- 新增
    customer_level_name
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
    from  csx_tmp.report_crm_w_a_customer_service_manager_info_business_new a 
    where month= substr(regexp_replace(${hiveconf:e_date},'-',''),1,6) )  b on x.customer_no=b.customer_no
LEFT JOIN
(
  select customer_no,
        sign_company_code, 
        last_sales_date,
        last_to_now_days,
         customer_active_status_code  customer_active_sts_code,
    case when  customer_active_status_code = 1 then '活跃客户'
	    when customer_active_status_code = 2 then '沉默客户'
	    when customer_active_status_code = 3 then '预流失客户'
	    when customer_active_status_code = 4 then '流失客户'
	    else '其他'
	end  as  customer_active_sts
	from csx_dw.dws_sale_w_a_customer_company_active
  where sdt = regexp_replace(${hiveconf:e_date},'-','') ) c on x.customer_no=c.customer_no  and x.comp_code = c.sign_company_code
;



INSERT OVERWRITE table csx_tmp.ads_fr_r_d_account_receivables_scar PARTITION(sdt)
select * from  csx_tmp.temp_account_age_00 ;