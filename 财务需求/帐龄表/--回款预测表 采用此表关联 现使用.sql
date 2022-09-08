--回款预测表 采用此表关联 现使用

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.exec.compress.output=true;
set parquet.compression=SNAPPY;
set mapred.output.compress=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set hive.support.quoted.identifiers=none;


create  table csx_analyse_tmp.csx_analyse_tmp_account_01 as 
select a.company_code,
	   a.customer_code,
	   performance_province_code,
	   ac_all,
	   ac_all_month_last_day,
	   ac_wdq_month_last_day,
	   coalesce(unreceivable_amount,0) unreceivable_amount,
	   coalesce(ac_all_month_last_day-ac_wdq_month_last_day-coalesce(unreceivable_amount,0),0) as payment_collection_target  --预测回款金额
from
(select a.company_code,
	   coalesce(a.customer_code,'')customer_code,
	   performance_province_code,
	   sum(receivable_amount) ac_all,
	   sum(receivable_amount_month_last_day) ac_all_month_last_day,
	   sum(no_overdue_amount_month_last_day) ac_wdq_month_last_day
from csx_analyse.csx_analyse_fr_sap_subject_customer_account_analyse_df a
    	where sdt=regexp_replace(trunc('${edate}','MM'),'-','')
	  group by  a.company_code,
	   a.performance_province_code,
	   customer_code
)a
left join 
    (select company_code,customer_no,sum(amount) as unreceivable_amount
    from csx_ods.csx_ods_data_analysis_prd_source_fr_w_a_customer_unable_payment_collection_df 
    where sdt=regexp_replace(date_sub(current_date(),1),'-','')
    group by company_code,customer_no ) f on a.customer_code=f.customer_no and a.company_code=f.company_code

;

create  table csx_analyse_tmp.csx_analyse_tmp_account_02 as 
	select channel_code,
	       sign_company_code,
	       performance_province_code,
		   customer_code,
		   sum(sale_amt) sales_value 
	from csx_dws.csx_dws_sale_detail_di 
		where sdt>= regexp_replace(add_months( trunc('${edate}','MM'),-1),'-','')
			and sdt< regexp_replace(trunc('${edate}','MM'),'-','')
			and performance_province_code in('33','35','36')
	group by channel_code,
	       sign_company_code,
	       performance_province_code,
		   customer_code
;


create  table csx_analyse_tmp.csx_analyse_tmp_account_03 as
select a.company_code,
	   a.customer_code,
	   sum(ac_all) ac_all,
	   sum(ac_all_month_last_day) ac_all_month_last_day,
	   sum(ac_wdq_month_last_day) ac_wdq_month_last_day,
	   sum(payment_collection_target) payment_collection_target,
	   sum(unreceivable_amount) unreceivable_amount,
	   sum(case when performance_province_code in ('33','35','36') then coalesce(sales_value,0) else coalesce(a.payment_collection_target,0)  end) as receivable_amount_target  
from 
(
select a.company_code,
	   a.customer_code,
	   performance_province_code,
	   ac_all,
	   ac_all_month_last_day,
	   ac_wdq_month_last_day,
	   payment_collection_target,
	   unreceivable_amount,
	   0 sales_value  
from  csx_analyse_tmp.csx_analyse_tmp_account_01 a 
union all 
select sign_company_code comp_code,
        a.customer_code,
	    performance_province_code,
        0 as ac_all,
	    0 as ac_all_month_last_day,
	    0 as ac_wdq_month_last_day,
	    0 as payment_collection_target,
	    0 as unreceivable_amount,
	   sales_value 
	from  csx_analyse_tmp.csx_analyse_tmp_account_02 a
) a
group by a.company_code,
	   a.customer_code;


CREATE  table csx_analyse_tmp.csx_analyse_tmp_channel as 
  SELECT  sign_company_code,
         customer_code,
         business_type_name as  sales_channel_name
  FROM csx_dws.csx_dws_sale_detail_di
  where  sdt>='20210101'
    and business_type_code='4'
  GROUP BY   sign_company_code,
             customer_code,
             business_type_name

;


CREATE  table csx_analyse_tmp.csx_analyse_tmp_channel_04 as 
-- 剔除一样的客户 
select   a.sign_company_code,
         a.customer_code,
         a.channel_name as sales_channel_name
from csx_dws.csx_dws_sale_detail_di a 
left join 
csx_analyse_tmp.csx_analyse_tmp_channel b on a.customer_code=b.customer_code
where b.customer_code is null
and a.sdt='current'
union all 
select   a.sign_company_code,
         a.customer_code,
         sales_channel_name
from csx_analyse_tmp.csx_analyse_tmp_channel a 
;



-- 收入预测
drop table if exists csx_analyse_tmp.csx_analyse_tmp_target_value ;
create  table csx_analyse_tmp.csx_analyse_tmp_target_value as 
select 
  customer_id,
  customer_code,
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
    customer_code,
	owner_user_id,
	business_attribute,
	project_code,
	target_code,
	target_year,
	map('01',january,'02',february,'03',march,'04',april,'05',may,'06',june,
	  '07',july,'08',august,'09',september,'10',october,'11',november,'12',december) as month_map
  from csx_ods.csx_ods_csx_crm_prod_target_df a 
  left join 
  (select customer_id,customer_code
    from csx_dim.csx_dim_crm_cs_customer_info
    where sdt='current') b on a.customer_id=b.customer_id
  where sdt = regexp_replace(date_sub(current_date,1),'-','')
    
   --  and target_code in (1,3)    --存量与增量客户
    and project_code in (1)     --取预测销售额
    -- and target_year >= '2022'
) a lateral VIEW explode(month_map) col1s AS month,target_value
;


drop table csx_analyse_tmp.csx_analyse_tmp_cust ;
create  table csx_analyse_tmp.csx_analyse_tmp_cust as 
select 
   a.channel_name,
   a.company_code ,
   a.company_name ,
   a.performance_region_code    ,
   a.performance_region_name ,
   a.performance_province_code    ,
   a.performance_province_name ,
   a.performance_city_code,
   a.performance_city_name,
   '' prctr,            --成本中心
   '' shop_name,
   a.customer_code  ,
   a.customer_name ,
   first_category_code,
   first_category_name,
   second_category_code,
   second_category_name,
   third_category_code,
   third_category_name,
   sales_user_number,
   sales_user_name,
   b.supervisor_user_number,
   b.supervisor_user_name,
   sales_manager_user_number,
   sales_manager_user_name,
   city_manager_user_number,
   city_manager_user_name,
   province_manager_user_number,
   province_manager_user_name,
   a.credit_limit ,
   a.temp_credit_limit ,
   a.account_period_name,
   a.account_period_code,
   a.account_period_value,
   back_money_amount_month
  from 
  (
  select *
  from csx_dws.csx_dws_sap_customer_settle_detail_di
    where 1=1 
    and sdt= regexp_replace('${edate}','-','')
  )a
      left join 
    (select 
        channel_code,
        channel_name,
        customer_code,
        company_code,
        account_period_code,
        account_period_name,
        account_period_value,
        credit_limit,
        temp_credit_limit,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name
    from 
        csx_dim.csx_dim_crm_customer_company   --客户账期表
    where 
        sdt='current'  ) c on a.customer_code = c.customer_code and a.company_code = c.company_code
     left join 
   (select channel_name,
           customer_code,
           customer_name,
           first_category_code,
           first_category_name,
           second_category_code,
           second_category_name,
           third_category_code,
           third_category_name,
           performance_region_code,
           performance_region_name,
           performance_province_code,
           performance_province_name,
           performance_city_code,
           performance_city_name,
           sales_user_number,
           sales_user_name,
           supervisor_user_number,
           supervisor_user_name,
           sales_manager_user_number,
           sales_manager_user_name,
           city_manager_user_number,
           city_manager_user_name,
           province_manager_user_number,
           province_manager_user_name
    from csx_dim.csx_dim_crm_customer_info 
        where sdt='current'
            and customer_code!='') b on  a.customer_code = b.customer_code
    ;
   

create  table csx_analyse_tmp.csx_analyse_tmp_ads_fr_r_d_forecast_collection_report_01 as
 select 
   t.channel_name,
   coalesce(c.sales_channel_name,t.channel_name) sales_channel_name,
   '' hkont,
   '' account_name,
   t.company_code,
   t.company_name,
   t.performance_region_code    ,
   t.performance_region_name ,
   t.performance_province_code    ,
   t.performance_province_name ,
   t.performance_city_code,
   t.performance_city_name,
   t.prctr,         --成本中心
   t.shop_name,
   t.customer_code,
   t.customer_name ,
   t.first_category_code,
   t.first_category_name,
   t.second_category_code,
   t.second_category_name,
   t.third_category_code,
   t.third_category_name ,
   sales_user_number,
   sales_user_name,
   t.supervisor_user_number,
   t.supervisor_user_name,
   sales_manager_user_number,
   sales_manager_user_name,
   city_manager_user_number,
   city_manager_user_name,
   province_manager_user_number,
   province_manager_user_name,
   t.credit_limit ,
   t.temp_credit_limit ,
   t.account_period_name,
   t.account_period_code,
   t.account_period_value,
   coalesce(a.receivable_amount,0 ) receivable_amount ,
   coalesce(a.no_overdue_amount,0 ) no_overdue_amount ,
   coalesce(a.receivable_amount_month_last_day,0) receivable_amount_month_last_day,
   coalesce(a.no_overdue_amount_month_last_day,0) no_overdue_amount_month_last_day,
   -- payment_collection_target as ac_overdue_month_last_day,  --预测逾期金额
   coalesce(ac_overdue_month_last_day,0) ac_overdue_month_last_day,
   0 ac_overdue_month_last_day_rate,    --月底预测逾期率 预留
   round(coalesce(target_value,0)*10000,2)  target_sale_value ,   --预测收入
   if(coalesce(receivable_amount_target,0)<=0,0,receivable_amount_target) as receivable_amount_target , --预测回款金额-无法回款金额
   coalesce(unreceivable_amount,0) unreceivable_amount,         --无法回款金额
   coalesce(back_money_amount_month,0)back_money_amount_month,     --当期回款金额
  -- coalesce(current_receivable_amount,0)*-1    current_receivable_amount ,   --当期回款金额因回款为负，current_receivable_amount*-1 
   -- 当预测回款金额<0 则0-当前回款，则正常计算
  --if(coalesce(receivable_amount_target,0)<=0,0-coalesce(current_receivable_amount*-1,0),coalesce(receivable_amount_target,0)-coalesce(current_receivable_amount*-1,0)) as need_receivable_amount , --需回款金额=预测回款目标-当期回款金额
   0 csx_analyse_tmp_1,
   0 csx_analyse_tmp_2,
   0 csx_analyse_tmp_3,
  coalesce(law_is_flag,0)law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time,
   regexp_replace('${edate}','-','') 
from csx_analyse_tmp.csx_analyse_tmp_cust t

left join 
(select 
   a.company_code,
   a.customer_code,
   sum(a.receivable_amount) receivable_amount,
   sum(no_overdue_amount) no_overdue_amount,
   sum(a.receivable_amount_month_last_day) receivable_amount_month_last_day,
   sum(a.no_overdue_amount_month_last_day) no_overdue_amount_month_last_day,
   sum(a.receivable_amount_month_last_day)-sum( a.no_overdue_amount_month_last_day ) ac_overdue_month_last_day,  --预测逾期金额
   0 ac_overdue_month_last_day_rate                                                          --月底预测逾期率 预留
from  csx_analyse.csx_analyse_fr_sap_subject_customer_account_analyse_df  a 
where sdt=regexp_replace('${edate}','-','') 
group by 
   a.company_code,
   a.customer_code
) a on t.customer_code=a.customer_code and t.company_code=a.company_code 
left join 
(select a.company_code,
       customer_code,
       sum(ac_all) ac_all,
	   sum(ac_all_month_last_day) ac_all_month_last_day,
	   sum(ac_wdq_month_last_day) ac_wdq_month_last_day,
	   sum(payment_collection_target) payment_collection_target,    -- 预测逾期金额
	   sum(unreceivable_amount) unreceivable_amount,                -- 无法回款金额
	   sum(receivable_amount_target) as receivable_amount_target     -- 预测回款目标取1号
from csx_analyse_tmp.csx_analyse_tmp_account_03 a 
    group by a.company_code,
       customer_code
)b on t.customer_code=b.customer_code and t.company_code=b.company_code
left join 
csx_analyse_tmp.csx_analyse_tmp_channel_04 c on t.customer_code=c.customer_code and t.company_code=c.sign_company_code
left join
(select  company_code,
         customer_no,
         is_flag as law_is_flag 
    from csx_ods.csx_ods_data_analysis_prd_source_fr_w_a_customer_legallegal_intervene_df    --是否法务介入
        where sdt=regexp_replace(date_sub(current_date(),1),'-','')  ) d on  t.customer_code=d.customer_no and t.company_code=d.company_code
left join 
(select customer_code,
    sum(target_value)target_value,
    target_month,
    project_code 
 from csx_analyse_tmp.csx_analyse_tmp_target_value 
    where project_code='1'
        and target_month=substr(regexp_replace('${edate}','-',''),1,6) 
    group by customer_code,target_month,project_code
    )k on t.customer_code=k.customer_code

;


 create  table csx_analyse_tmp.csx_analyse_tmp_ads_fr_r_d_forecast_collection_report_02 as

select 
   t.channel_name,
   sales_channel_name,
   hkont,
   account_name,
   t.company_code,
   t.company_name,
   t.performance_region_code    ,
   t.performance_region_name ,
   t.performance_province_code    ,
   t.performance_province_name ,
   t.performance_city_code,
   t.performance_city_name,
   t.prctr,         --成本中心
   t.shop_name,
   t.customer_code ,
   t.customer_name ,
   t.first_category_code,
   t.first_category_name,
   t.second_category_code,
   t.second_category_name,
   t.third_category_code,
   t.third_category_name ,
   sales_user_number,
   sales_user_name,
   supervisor_user_number,
   supervisor_user_name,
   sales_manager_user_number,
   sales_manager_user_name,
   city_manager_user_number,
   city_manager_user_name,
   province_manager_user_number,
   province_manager_user_name,
   t.credit_limit ,
   t.temp_credit_limit ,
   t.account_period_name,
   t.account_period_code,
   t.account_period_value,
   coalesce(t.receivable_amount,0 ) receivable_amount ,
   coalesce(t.no_overdue_amount,0 ) no_overdue_amount ,
   coalesce(t.receivable_amount_month_last_day,0) receivable_amount_month_last_day,
   coalesce(t.no_overdue_amount_month_last_day,0) no_overdue_amount_month_last_day,
   coalesce(ac_overdue_month_last_day,0) ac_overdue_month_last_day,
   0 ac_overdue_month_last_day_rate,    --月底预测逾期率 预留
   target_sale_value ,   --预测收入
   receivable_amount_target , --预测回款金额-无法回款金额
   coalesce(unreceivable_amount,0) unreceivable_amount,         --无法回款金额
   back_money_amount_month  as current_receivable_amount ,   --当期回款金额因回款为负
   -- 当预测回款金额<0 则0-当前回款，则正常计算
  if(coalesce(receivable_amount_target,0)<=0,0-coalesce(back_money_amount_month,0)*-1,coalesce(receivable_amount_target,0)-coalesce(back_money_amount_month,0)*-1) as need_receivable_amount , --需回款金额=预测回款目标-当期回款金额
   0 csx_analyse_tmp_1,
   0 csx_analyse_tmp_2,
   0 csx_analyse_tmp_3,
   law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time,
   regexp_replace('${edate}','-','') 
from csx_analyse_tmp.csx_analyse_tmp_ads_fr_r_d_forecast_collection_report_01 t 

;

 create  table csx_analyse_tmp.csx_analyse_tmp_ads_fr_r_d_forecast_collection_report_02 as

select 
   coalesce(channel_name,'-')channel_name,
   coalesce(sales_channel_name,'-')sales_channel_name,
   hkont,
   account_name,
   t.company_code,
   t.company_name,
   t.performance_region_code    ,
   t.performance_region_name ,
   t.performance_province_code    ,
   t.performance_province_name ,
   t.performance_city_code,
   t.performance_city_name,
   prctr,         --成本中心
   shop_name,
   customer_code ,
   coalesce(customer_name,'')customer_name ,
   coalesce(first_category_code,'') as first_category_code,
   coalesce(first_category_name,'') as first_category_name,
   coalesce(second_category_code,'') as second_category_code,
   coalesce(second_category_name,'') as second_category_name,
   coalesce(third_category_code,'') as third_category_code,
   coalesce(third_category_name ,'') as third_category_name ,
   coalesce(work_no ,'') as work_no ,
   coalesce(sales_name ,'') as sales_name ,
   coalesce(first_supervisor_work_no,'') as first_supervisor_work_no,
   coalesce(first_supervisor_name,'') as first_supervisor_name,
   coalesce(second_supervisor_work_no,'')   sales_manager_work_no	,
   coalesce(second_supervisor_name,'')      sales_manager	,
   coalesce(third_supervisor_work_no,'')    city_manager_work_no	,
   coalesce(third_supervisor_name,'')       city_manager	,
   coalesce(fourth_supervisor_work_no,'')   area_manager_work_no	,
   coalesce(fourth_supervisor_name,'')      area_manager	,
   credit_limit ,
   csx_analyse_tmp_credit_limit ,
   payment_terms,
   payment_name,
   payment_days,
   zterm ,
   diff ,
   ac_all ,
   ac_wdq ,
   ac_all_month_last_day,
   ac_wdq_month_last_day,
  -- payment_collection_target as ac_overdue_month_last_day,  --预测逾期金额
   ac_overdue_month_last_day,
   ac_overdue_month_last_day_rate,    --月底预测逾期率 预留
   target_sale_value ,   --预测收入
   receivable_amount_target , --预测回款金额-无法回款金额
   unreceivable_amount,         --无法回款金额
   current_receivable_amount ,   --当期回款金额因回款为负，current_receivable_amount*-1 
   -- 当预测回款金额<0 则0-当前回款，则正常计算
   need_receivable_amount , --需回款金额=预测回款目标-当期回款金额
   csx_analyse_tmp_1,
   csx_analyse_tmp_2,
   csx_analyse_tmp_3,
   law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time,
   regexp_replace('${edate}','-','')  
from csx_analyse_tmp.csx_analyse_tmp_ads_fr_r_d_forecast_collection_report_02 t
 ;

insert overwrite table csx_analyse.csx_analyse_fr_sap_forecast_collection_report_df partition (sdt)
select 
   coalesce(channel_name,'-')channel_name,
   coalesce(sales_channel_name,'-')sales_channel_name,
   hkont,
   account_name,
   company_code,
   company_name,
   coalesce(performance_region_code,'-') as region_code,
   coalesce(performance_region_name,'-') as region_name,
   coalesce(performance_province_code,'-') province_code,     --省区编码
   coalesce(performance_province_name,'-') province_name,
   performance_city_code,
   performance_city_name,
   prctr,         --成本中心
   shop_name,
   customer_code ,
   coalesce(customer_name,'')customer_name ,
   coalesce(first_category_code,'') as first_category_code,
   coalesce(first_category_name,'') as first_category_name,
   coalesce(second_category_code,'') as second_category_code,
   coalesce(second_category_name,'') as second_category_name,
   coalesce(third_category_code,'') as third_category_code,
   coalesce(third_category_name ,'') as third_category_name ,
   coalesce(sales_user_number ,'') as work_no ,
   coalesce(sales_user_name ,'') as sales_name ,
   coalesce(supervisor_user_number,'') as first_supervisor_work_no,
   coalesce(supervisor_user_name,'') as first_supervisor_name,
   coalesce(sales_manager_user_number,'')   sales_manager_work_no	,
   coalesce(sales_manager_user_name,'')      sales_manager	,
   coalesce(city_manager_user_number,'')    city_manager_work_no	,
   coalesce(city_manager_user_name,'')       city_manager	,
   coalesce(province_manager_user_number,'')   area_manager_work_no	,
   coalesce(province_manager_user_name,'')      area_manager	,
   credit_limit ,
   temp_credit_limit ,
   account_period_name,
   account_period_code,
   account_period_value,
   coalesce(receivable_amount,0 ) receivable_amount ,
   coalesce(no_overdue_amount,0 ) no_overdue_amount ,
   coalesce(receivable_amount_month_last_day,0) receivable_amount_month_last_day,
   coalesce(no_overdue_amount_month_last_day,0) no_overdue_amount_month_last_day,
  -- payment_collection_target as ac_overdue_month_last_day,  --预测逾期金额
   ac_overdue_month_last_day,
   ac_overdue_month_last_day_rate,    --月底预测逾期率 预留
   target_sale_value ,   --预测收入
   receivable_amount_target , --预测回款金额-无法回款金额
   unreceivable_amount,         --无法回款金额
   current_receivable_amount ,   --当期回款金额因回款为负，current_receivable_amount*-1 
   -- 当预测回款金额<0 则0-当前回款，则正常计算
   need_receivable_amount , --需回款金额=预测回款目标-当期回款金额
   csx_analyse_tmp_1,
   csx_analyse_tmp_2,
   csx_analyse_tmp_3,
   law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time,
   regexp_replace('${edate}','-','')  sale_sdt,
   regexp_replace('${edate}','-','') 
from csx_analyse_tmp.csx_analyse_tmp_ads_fr_r_d_forecast_collection_report_02 t
 ;



insert overwrite table csx_analyse_tmp.ads_fr_r_d_forecast_collection_report partition (sdt)
select 
   coalesce(channel_name,'-')channel_name,
   coalesce(sales_channel_name,'-')sales_channel_name,
   hkont,
   account_name,
   company_code,
   company_name,
   coalesce(performance_region_code,'-') as region_code,
   coalesce(performance_region_name,'-') as region_name,
   coalesce(performance_province_code,'-') province_code,     --省区编码
   coalesce(performance_province_name,'-') province_name,
   performance_city_code,
   performance_city_name,
   prctr,         --成本中心
   shop_name,
   customer_code ,
   coalesce(customer_name,'')customer_name ,
   coalesce(first_category_code,'') as first_category_code,
   coalesce(first_category_name,'') as first_category_name,
   coalesce(second_category_code,'') as second_category_code,
   coalesce(second_category_name,'') as second_category_name,
   coalesce(third_category_code,'') as third_category_code,
   coalesce(third_category_name ,'') as third_category_name ,
   coalesce(sales_user_number ,'') as work_no ,
   coalesce(sales_user_name ,'') as sales_name ,
   coalesce(supervisor_user_number,'') as first_supervisor_work_no,
   coalesce(supervisor_user_name,'') as first_supervisor_name,
   coalesce(sales_manager_user_number,'')   sales_manager_work_no	,
   coalesce(sales_manager_user_name,'')      sales_manager	,
   coalesce(city_manager_user_number,'')    city_manager_work_no	,
   coalesce(city_manager_user_name,'')       city_manager	,
   coalesce(province_manager_user_number,'')   area_manager_work_no	,
   coalesce(province_manager_user_name,'')      area_manager	,
   credit_limit ,
   temp_credit_limit ,
   t.account_period_name,
   t.account_period_code,
   t.account_period_value,
    coalesce(a.receivable_amount,0 ) receivable_amount ,
   coalesce(a.no_overdue_amount,0 ) no_overdue_amount ,
   coalesce(a.receivable_amount_month_last_day,0) receivable_amount_month_last_day,
   coalesce(a.no_overdue_amount_month_last_day,0) no_overdue_amount_month_last_day,
  -- payment_collection_target as ac_overdue_month_last_day,  --预测逾期金额
   ac_overdue_month_last_day,
   ac_overdue_month_last_day_rate,    --月底预测逾期率 预留
   target_sale_value ,   --预测收入
   receivable_amount_target , --预测回款金额-无法回款金额
   unreceivable_amount,         --无法回款金额
   current_receivable_amount ,   --当期回款金额因回款为负，current_receivable_amount*-1 
   -- 当预测回款金额<0 则0-当前回款，则正常计算
   need_receivable_amount , --需回款金额=预测回款目标-当期回款金额
   csx_analyse_tmp_1,
   csx_analyse_tmp_2,
   csx_analyse_tmp_3,
   law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time,
   regexp_replace('${edate}','-','')  
from csx_analyse_tmp.csx_analyse_tmp_ads_fr_r_d_forecast_collection_report_02 t
 ;




 