--销售预测回款表【20220308】关联 ecc_ytbcustomer 表

SET hive.execution.engine=mr; 
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


-- 1.1 先计算1号的预测逾期金额
drop table if exists csx_tmp.temp_account_01;
create temporary table csx_tmp.temp_account_01 as 
select a.comp_code,
	   a.customer_no,
	   province_code,
	   ac_all,
	   ac_all_month_last_day,
	   ac_wdq_month_last_day,
	   coalesce(unreceivable_amount,0) unreceivable_amount,
	   coalesce(ac_all_month_last_day-ac_wdq_month_last_day-coalesce(unreceivable_amount,0),0) as payment_collection_target  --预测回款金额
from
(select a.comp_code,
	   a.customer_no,
	   province_code,
	   sum(ac_all) ac_all,
	   sum(ac_all_month_last_day) ac_all_month_last_day,
	   sum(ac_wdq_month_last_day) ac_wdq_month_last_day
from csx_tmp.ads_fr_r_d_account_receivables_scar a
    	where sdt=${hiveconf:sdt_1}
	  group by  a.comp_code,
	   a.customer_no,
	   province_code
)a
left join 
    (select company_code,customer_no,sum(amount) as unreceivable_amount
    from csx_tmp.source_fr_w_a_customer_unable_payment_collection 
    where sdt=regexp_replace(date_sub(current_date(),1),'-','')
    group by company_code,customer_no ) f on a.customer_no=f.customer_no and a.comp_code=f.company_code

;



-- 1.2 计算上月整月销售额 取客户签约公司
drop table if exists csx_tmp.temp_account_02 ;
create temporary table csx_tmp.temp_account_02 as 
	select channel_code,
	       sign_company_code,
	       province_code,
		   customer_no,
		   sum(sales_value) sales_value 
	from csx_dw.dws_sale_r_d_detail 
		where sdt>=${hiveconf:l_sdt}
			and sdt<${hiveconf:sdt_1}
			and province_code in('33','35','36')
	group by channel_code,
			 customer_no,
			 sign_company_code,
			 province_code
;

-- 1.3 预测回款目标，供应链取上个月销售目标,其中部分销售属于云超客户，属于关联交易
drop table if exists csx_tmp.temp_account_03 ;
create temporary table csx_tmp.temp_account_03 as
select a.comp_code,
	   a.customer_no,
	   sum(ac_all) ac_all,
	   sum(ac_all_month_last_day) ac_all_month_last_day,
	   sum(ac_wdq_month_last_day) ac_wdq_month_last_day,
	   sum(payment_collection_target) payment_collection_target,
	   sum(unreceivable_amount) unreceivable_amount,
	   sum(case when province_code in ('33','35','36') then coalesce(sales_value,0) else coalesce(a.payment_collection_target,0)  end) as receivable_amount_target  
from 
(
select a.comp_code,
	   a.customer_no,
	   a.province_code,
	   ac_all,
	   ac_all_month_last_day,
	   ac_wdq_month_last_day,
	   payment_collection_target,
	   unreceivable_amount,
	   0 sales_value  
from  csx_tmp.temp_account_01 a 
-- where comp_code in ('2116','2126')
union all 
select sign_company_code comp_code,
        customer_no,
        province_code,
        0 as ac_all,
	    0 as ac_all_month_last_day,
	    0 as ac_wdq_month_last_day,
	    0 as payment_collection_target,
	    0 as unreceivable_amount,
	   sales_value 
	from  csx_tmp.temp_account_02 
) a
group by a.comp_code,
	   a.customer_no;


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


-- 全量客户
drop table csx_tmp.temp_cust ;
create temporary table csx_tmp.temp_cust as 
select 
   b.channel_name,
   a.bukrs comp_code,
   comp_name,
   sales_region_code,
   sales_region_name,
   sales_province_code,
   sales_province_name,
   city_group_code,
   city_group_name,
   city_name,
   '' prctr,            --成本中心
   '' shop_name,
   regexp_replace(a.kunnr,'(^0*)','') as  customer_no ,
   customer_name ,
   first_category_code,
   first_category_name,
   second_category_code,
   second_category_name,
   third_category_code,
   third_category_name,
   work_no ,
   sales_name ,
   first_supervisor_work_no,
   first_supervisor_name,
   credit_limit ,
   temp_credit_limit ,
   payment_terms,
   payment_name,
   payment_days,
   payment_terms as zterm ,
   cast(payment_days as int)  diff 
  from 
  (
  select  
        kunnr,
        hkont,
        bukrs
  from 
  ods_ecc.ecc_ytbcustomer
    where sdt = regexp_replace(date_add(${hiveconf:e_date},1), '-', '')
        and mandt = '800' --sdt budat月初刷数据-当前分区刷月底
        and hkont like '1122%'
        and budat < regexp_replace(date_add(${hiveconf:e_date},1), '-', '')
      --  and kunnr='0000125440'
    group by kunnr,
        hkont,
        bukrs
    )a
    left join 
    (select 
        channel_code,
        channel_name,
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
        sdt='current'  ) c on lpad(a.kunnr, 10, '0') = lpad(c.customer_no, 10, '0') and a.bukrs = c.company_code
   left join 
   (select channel_name,
           customer_no,
           customer_name,
           first_category_code,
           first_category_name,
           second_category_code,
           second_category_name,
           third_category_code,
           third_category_name,
           sales_region_code,
           sales_region_name,
           sales_province_code,
           sales_province_name,
           city_group_code,
           city_group_name,
           work_no,
           sales_name,
           first_supervisor_work_no,
           first_supervisor_name
    from csx_dw.dws_crm_w_a_customer 
        where sdt='current'  ) b on  lpad(a.kunnr, 10, '0') = lpad(b.customer_no, 10, '0')
    left join 
     (select code as comp_code, name as comp_name
         from csx_dw.dws_basic_w_a_company_code
         where sdt = 'current'
     ) f on a.bukrs = f.comp_code
         left join
     (select code as accunt_code, name as account_name
      from csx_ods.source_basic_w_a_md_accounting_subject
      ) as d  on a.hkont = d.accunt_code
group by  b.channel_name,
   a.bukrs ,
   comp_name,
   sales_region_code,
   sales_region_name,
   sales_province_code,
   sales_province_name,
   city_group_code,
   city_group_name,
   city_name,
   regexp_replace(a.kunnr,'(^0*)','') ,
   customer_name ,
   first_category_code,
   first_category_name,
   second_category_code,
   second_category_name,
   third_category_code,
   third_category_name,
   work_no ,
   sales_name ,
   first_supervisor_work_no,
   first_supervisor_name,
   credit_limit ,
   temp_credit_limit ,
   payment_terms,
   payment_name,
   payment_days,
   payment_terms ,
   cast(payment_days as int)  ;
   
  -- select * from csx_tmp.temp_cust where customer_no='123920';

  ; 
  
 
 drop table   csx_tmp.temp_cust_00 ;
 create temporary table csx_tmp.temp_cust_00 as 
 select 
  channel_name,
  comp_code,
  comp_name,
  case
          when customer_no in ('G7150') then '36'
          when sales_province_name is null and substr(comp_name, 1, 2) = '北京' then '1'
          when sales_province_name is null and substr(comp_name, 1, 2) = '上海' then '2'
          when sales_province_name is null and substr(comp_name, 1, 2) = '河北' then '6'
          when sales_province_name is null and substr(comp_name, 1, 2) = '江苏' then '10'
          when sales_province_name is null and substr(comp_name, 1, 2) = '安徽' then '11'
          when sales_province_name is null and substr(comp_name, 1, 2) = '浙江' then '13'
          when sales_province_name is null and substr(comp_name, 1, 2) = '福建' then '15'
          when sales_province_name is null and substr(comp_name, 1, 3) = 'BBC' then '16'
          when sales_province_name is null and substr(comp_name, 1, 2) = '广东' then '20'
          when sales_province_name is null and substr(comp_name, 1, 2) = '贵州' then '23'
          when sales_province_name is null and substr(comp_name, 1, 2) = '四川' then '24'
          when sales_province_name is null and substr(comp_name, 1, 2) = '陕西' then '26'
          when sales_province_name is null and substr(comp_name, 1, 2) = '重庆' then '32'
          when sales_province_name is null and substr(comp_name, 1, 2) = '永辉' then '15'
          else sales_province_code end sales_province_code ,
    case when customer_no in ('G7150') then '平台-食百采购'
         when  sales_province_name is null and substr(comp_name, 1, 2) in ('上海', '北京', '重庆') then concat(substr(comp_name, 1, 2), '市')
         when  sales_province_name is null and substr(comp_name, 1, 2) = '永辉' then '福建省'
         when  sales_province_name is null then concat(substr(comp_name, 1, 2), '省') 
        else  sales_province_name end sales_province_name,
  city_group_code,
  city_group_name,
  city_name,
  prctr,            --成本中心
  shop_name,
  customer_no ,
  customer_name ,
  first_category_code,
  first_category_name,
  second_category_code,
  second_category_name,
  third_category_code,
  third_category_name,
  work_no ,
  sales_name ,
  first_supervisor_work_no,
  first_supervisor_name,
  credit_limit ,
  temp_credit_limit ,
  payment_terms,
  payment_name,
  payment_days,
  zterm ,
  diff 
  from
  (
select 
  channel_name,
  comp_code,
  comp_name,
  coalesce(a.sales_region_code,b.sales_region_code) sales_region_code,
  coalesce(a.sales_region_name,b.sales_region_name)    sales_region_name,
  coalesce(a.sales_province_code, b.sales_province_code) sales_province_code,
  coalesce(a.sales_province_name, b.sales_province_name) sales_province_name,
  coalesce(a.city_group_code,b.city_group_code) city_group_code,
  coalesce(a.city_group_name,b.city_group_name) city_group_name,
  coalesce(a.city_name,  b.city_name)city_name,
  prctr,            --成本中心
  shop_name,
  customer_no ,
  customer_name ,
  first_category_code,
  first_category_name,
  second_category_code,
  second_category_name,
  third_category_code,
  third_category_name,
  work_no ,
  sales_name ,
  first_supervisor_work_no,
  first_supervisor_name,
  credit_limit ,
  temp_credit_limit ,
  payment_terms,
  payment_name,
  payment_days,
  payment_terms as zterm ,
  diff 
  from csx_tmp.temp_cust a 
  left join
  (select concat('S',shop_id) cust_id,
    sales_region_code,
    sales_region_name,
    sales_province_code,
    sales_province_name,
    city_group_code,
    city_group_name,
    city_name
  from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current') b on a.customer_no=b.cust_id
  ) a 
  ; 
  


--关联 法务介入表、目标表
-- select * from csx_tmp.temp_ads_fr_r_d_forecast_collection_report_01 where customer_no like '125913';
 drop table csx_tmp.temp_ads_fr_r_d_forecast_collection_report_01 ;
 create temporary table csx_tmp.temp_ads_fr_r_d_forecast_collection_report_01 as
 select 
   t.channel_name,
   coalesce(c.sales_channel_name,t.channel_name) sales_channel_name,
   '' hkont,
   '' account_name,
   t.comp_code,
   t.comp_name,
  coalesce(t.sales_province_code,t.sales_province_code) province_code,     --省区编码
  coalesce(t.sales_province_name,t.sales_province_name) province_name,
  coalesce(t.city_group_code,'-') city_group_code,
  coalesce(t.city_group_name,'-')city_group_name,
   city_name,
   t.prctr,         --成本中心
   t.shop_name,
   t.customer_no ,
   t.customer_name ,
   t.first_category_code,
   t.first_category_name,
   t.second_category_code,
   t.second_category_name,
   t.third_category_code,
   t.third_category_name ,
   t.work_no ,
   t.sales_name ,
   t.first_supervisor_work_no,
   t.first_supervisor_name,
   t.credit_limit ,
   t.temp_credit_limit ,
   t.payment_terms,
   t.payment_name,
   t.payment_days,
   t.zterm ,
   t.diff ,
   coalesce(a.ac_all,0 )ac_all ,
   coalesce(ac_wdq,0 ) ac_wdq ,
   coalesce(a.ac_all_month_last_day,0)ac_all_month_last_day,
   coalesce(a.ac_wdq_month_last_day,0)ac_wdq_month_last_day,
   -- payment_collection_target as ac_overdue_month_last_day,  --预测逾期金额
   coalesce(ac_overdue_month_last_day,0)ac_overdue_month_last_day,
   0 ac_overdue_month_last_day_rate,    --月底预测逾期率 预留
   round(coalesce(target_value,0)*10000,2)  target_sale_value ,   --预测收入
   if(coalesce(receivable_amount_target,0)<=0,0,receivable_amount_target) as receivable_amount_target , --预测回款金额-无法回款金额
   coalesce(unreceivable_amount,0) unreceivable_amount,         --无法回款金额
  -- coalesce(current_receivable_amount,0)*-1    current_receivable_amount ,   --当期回款金额因回款为负，current_receivable_amount*-1 
   -- 当预测回款金额<0 则0-当前回款，则正常计算
  --if(coalesce(receivable_amount_target,0)<=0,0-coalesce(current_receivable_amount*-1,0),coalesce(receivable_amount_target,0)-coalesce(current_receivable_amount*-1,0)) as need_receivable_amount , --需回款金额=预测回款目标-当期回款金额
   0 temp_1,
   0 temp_2,
   0 temp_3,
  coalesce(law_is_flag,0)law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time,
   regexp_replace(${hiveconf:e_date},'-','') 
from csx_tmp.temp_cust t

left join 
(select 
   a.comp_code,
   a.customer_no ,
   sum(a.ac_all) ac_all,
   sum(ac_wdq) ac_wdq,
   sum(a.ac_all_month_last_day) ac_all_month_last_day,
   sum(a.ac_wdq_month_last_day) ac_wdq_month_last_day,
   sum(a.ac_all_month_last_day)-sum( a.ac_wdq_month_last_day ) ac_overdue_month_last_day,  --预测逾期金额
   0 ac_overdue_month_last_day_rate    --月底预测逾期率 预留
from  csx_tmp.ads_fr_r_d_account_receivables_scar  a 
where sdt=regexp_replace(${hiveconf:e_date},'-','') 
group by 
   a.comp_code,
   a.customer_no 
) a on t.customer_no=a.customer_no and t.comp_code=a.comp_code 
left join 
(select a.comp_code,
       customer_no,
       sum(unreceivable_amount) unreceivable_amount,        --无法回款金额
       sum(ac_all)ac_all,
       sum(ac_all_month_last_day)   ac_all_month_last_day,
       sum(ac_wdq_month_last_day)   ac_wdq_month_last_day,
       sum(payment_collection_target)   payment_collection_target,  --预测逾期金额
       sum(receivable_amount_target )   receivable_amount_target    --预测回款目标取1号
from csx_tmp.temp_account_03 a 
    group by a.comp_code,
       customer_no
)b on t.customer_no=b.customer_no and t.comp_code=b.comp_code
left join 
csx_tmp.temp_channel_04 c on t.customer_no=c.customer_no and t.comp_code=c.sign_company_code
left join
(select  company_code,
         customer_no,
         is_flag as law_is_flag 
    from csx_tmp.source_fr_w_a_customer_legallegal_intervene    --是否法务介入
        where sdt=regexp_replace(date_sub(current_date(),1),'-','')  ) d on  t.customer_no=d.customer_no and t.comp_code=d.company_code
left join 
(select customer_no,
    sum(target_value)target_value,
    target_month,
    project_code 
 from csx_tmp.temp_target_value 
    where project_code='1'
        and target_month=substr(regexp_replace(${hiveconf:e_date},'-',''),1,6) 
    group by customer_no,target_month,project_code
    )k on t.customer_no=k.customer_no

;

 drop table csx_tmp.temp_ads_fr_r_d_forecast_collection_report_02 ;
 create temporary table csx_tmp.temp_ads_fr_r_d_forecast_collection_report_02 as

select 
   t.channel_name,
   sales_channel_name,
   hkont,
   account_name,
   t.comp_code,
   t.comp_name,
  coalesce(t. province_code,j.sales_province_code) province_code,     --省区编码
  coalesce(t. province_name,j.sales_province_name) province_name,
  coalesce(t.city_group_code,'-') city_group_code,
  coalesce(t.city_group_name,'-')city_group_name,
   city_name,
   t.prctr,         --成本中心
   t.shop_name,
   t.customer_no ,
   t.customer_name ,
   t.first_category_code,
   t.first_category_name,
   t.second_category_code,
   t.second_category_name,
   t.third_category_code,
   t.third_category_name ,
   t.work_no ,
   t.sales_name ,
   t.first_supervisor_work_no,
   t.first_supervisor_name,
   t.credit_limit ,
   t.temp_credit_limit ,
   t.payment_terms,
   t.payment_name,
   t.payment_days,
   t.zterm ,
   t.diff ,
   ac_all ,
   ac_wdq ,
   ac_all_month_last_day,
   ac_wdq_month_last_day,
   coalesce(ac_overdue_month_last_day,0) ac_overdue_month_last_day,
   0 ac_overdue_month_last_day_rate,    --月底预测逾期率 预留
   target_sale_value ,   --预测收入
   receivable_amount_target , --预测回款金额-无法回款金额
   coalesce(unreceivable_amount,0) unreceivable_amount,         --无法回款金额
   current_receivable_amount ,   --当期回款金额因回款为负，current_receivable_amount*-1 
   -- 当预测回款金额<0 则0-当前回款，则正常计算
  if(coalesce(receivable_amount_target,0)<=0,0-coalesce(current_receivable_amount*-1,0),coalesce(receivable_amount_target,0)-coalesce(current_receivable_amount*-1,0)) as need_receivable_amount , --需回款金额=预测回款目标-当期回款金额
   0 temp_1,
   0 temp_2,
   0 temp_3,
   law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time,
   regexp_replace(${hiveconf:e_date},'-','') 
from csx_tmp.temp_ads_fr_r_d_forecast_collection_report_01 t 
left join 
-- 取当前回款金额
(select regexp_replace(kunnr,'(^0*)','') customer_no,
    channel,
    income as current_receivable_amount,
    sap_merchant_code,
    sales_province_code,
    sales_province sales_province_name,
    sales_city
from csx_dw.fixation_report_customer_sale_income1_scar
    where sdt =regexp_replace(${hiveconf:e_date},'-','') 
        and kunnr is not null
       -- and kunnr='0000118602'
       and smonth = substr(regexp_replace(${hiveconf:e_date},'-',''),1,6) ) j on t.customer_no=j.customer_no and t.comp_code=j.sap_merchant_code

;


-- insert overwrite table csx_tmp.ads_fr_r_d_forecast_collection_report_20220304 partition (sdt)
drop table csx_tmp.temp_acc_01;
create temporary table csx_tmp.temp_acc_01 as 
select 
   coalesce(channel_name,'-')channel_name,
   coalesce(sales_channel_name,'-')sales_channel_name,
   hkont,
   account_name,
   comp_code,
   comp_name,
   coalesce(region_code,'-') as region_code,
   coalesce(region_name,'-') as region_name,
   coalesce(province_code,'-') province_code,     --省区编码
   coalesce(t.province_name,'-') province_name,
   coalesce(city_group_code,'-') city_group_code,
   coalesce(city_group_name,'-') city_group_name,
   coalesce(city_name,'')city_name,
   prctr,         --成本中心
   shop_name,
   customer_no ,
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
   credit_limit ,
   temp_credit_limit ,
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
   temp_1,
   temp_2,
   temp_3,
   law_is_flag,         --是否法务介入 1 是 0 否
   current_timestamp() as update_time,
   regexp_replace(${hiveconf:e_date},'-','')  
from csx_tmp.temp_ads_fr_r_d_forecast_collection_report_02 t
left join
(select distinct province_name,region_code,region_name from csx_dw.dws_sale_w_a_area_belong  ) b on t.province_name=b.province_name;


;



select distinct region_code,region_name,province_code,province_name from csx_tmp.ads_fr_r_d_forecast_collection_report_20220304;

select * from csx_tmp.ads_fr_r_d_forecast_collection_report_20220304 where sdt='20220307' and customer_no='104617';


select * from csx_dw.fixation_report_province_sale_income_output_scar where sales_province_code='350000' and sdt='20220306';

SHOW CREATE TABLE  csx_tmp.ads_fr_r_d_forecast_collection_report_20220304;