-- 逻辑规则于20191112 更改
-- 增加大宗、供应链 万四舍五入 round(new_cr,-4), 大客户、商超 千四舍五入round(a,-3)
-- set mapreduce.job.queuename =caishixian;
set hive.groupby.skewindata = true;
set hive.exec.parallel      =true;
-- set hive.exec.parallel.thread.number = 12;
set s_date =regexp_replace(date_sub(current_date,1),'-','');
set s_date_30 =regexp_replace(date_sub(current_date,31),'-','');
set s_date_60 =regexp_replace(date_sub(current_date,61),'-','');
set s_date_90 =regexp_replace(date_sub(current_date,91),'-','');
set s_date_yy =regexp_replace(trunc(date_sub(current_date,31),'yy'),'-','');

drop table if exists csx_tmp.p_sale_01
;

create temporary table if not exists csx_tmp.p_sale_01 as
select
	'票到0' diff ,
	0     index_data
union all
select
	'票到15' diff ,
	15     index_data
union all
select
	'票到30' diff ,
	30     index_data
union all
select
	'票到45' diff ,
	45     index_data
union all
select
	'票到60' diff ,
	60     index_data
union all
select
	'票到7' diff ,
	7     index_data
union all
select
	'月结10' diff ,
	25     index_data
union all
select
	'月结15' diff ,
	30     index_data
union all
select
	'月结30' diff ,
	45     index_data
union all
select
	'月结45' diff ,
	60     index_data
union all
select
	'月结60' diff ,
	75     index_data
union all
select
	'月结90' diff ,
	105 index_data
;

-- 客户资料
drop table if exists csx_tmp.p_sale_02
;

create temporary table if not exists csx_tmp.p_sale_02 as
 select
    channel                     ,
    channel_code,
    sales_province_code         ,
    sales_province              ,
    sales_city_code             ,
    sales_city                  ,
    sales_province_code as province_code               ,
    sales_province as  province_name               ,
    attribute                   ,
    sales_city_code as city_code  ,
    sales_city as    city_name                   ,
    sales_name                  ,
    work_no                     ,
    customer_no                 ,
    customer_name               ,
    credit_limit                ,
    temp_credit_limit           ,
    first_category              ,
    second_category             ,
    third_category              ,
    to_date(sign_time)sign_date ,
    sign_company_code ,
    company_code 
from
    csx_dw.dws_crm_w_a_customer_m_v1 as a
left join 
  (select
    id,
    customer_id,
    customer_number,
    company_code,
    payment_terms,
    payment_name,
    payment_days,
    customer_level,
    credit_limit,
    temp_credit_limit,
    temp_begin_time,
    temp_end_time,
    credit_type,
    company_status
from
    csx_dw.dws_crm_r_a_customer_account_day
where
    sdt = 'current') as b 
    on a.customer_no=b.customer_number 
where
    sdt             =   'current'
    and source !='dev'  
;

	
;

drop table if exists csx_tmp.p_sale_03
;

create temporary table if not exists csx_tmp.p_sale_03 as
-- 帐龄数据
select
	sflag                                 ,
	hkont                                 ,
	account_name                          ,
	comp_code                             ,
	comp_name                             ,
	regexp_replace(kunnr,'(^0*)','')kunnr ,
	name                                  ,
	zterm                                 ,
	diff                                  ,
	ac_all                                ,
	ac_wdq                                ,
	ac_15d                                ,
	ac_30d                                ,
	ac_60d                                ,
	ac_90d                                ,
	ac_120d                               ,
	ac_180d                               ,
	ac_365d                               ,
	ac_2y                                 ,
	ac_3y                                 ,
	ac_over3y
from
	csx_dw.account_age_dtl_fct_new
where
	sdt=${hiveconf:s_date}
;

-- regexp_replace(${hiveconf:sdate},'-','')
drop table if exists csx_tmp.p_sale_04
;

create temporary table if not exists csx_tmp.p_sale_04 as
-- 销售数据
select --channel,
       --channel_name,
       customer_no,
       a.company_name,
       a.company_code,
      coalesce(sale_30,sale_60)sale,
      coalesce(sale_30/30,sale_60/60) avg_sale,
       min_sdt
from
  (select customer_no,
          a.sign_company_code as  company_code,
          a.sign_company_name as  company_name,
          sum(case when(channel in ('4','5','6')  and sdt>=${hiveconf:s_date_60}) then a.sales_value end )as sale_60,
          sum(case when (channel in ('1','2','3','7') and sdt>=${hiveconf:s_date_30} ) then a.sales_value end)as sale_30,
          min(sdt)as min_sdt
   from csx_dw.dws_sale_r_d_customer_sale a
   where a.sdt>=${hiveconf:s_date_yy}
   group by customer_no,
            a.sign_company_code ,
          a.sign_company_name) a;
            
drop table if exists csx_tmp.p_sale_05
;

create temporary table if not exists csx_tmp.p_sale_05 as
select  nvl(sflag,'')as sflag,
        nvl(hkont,'')as hkont,
        nvl(account_name,'')as account_name,
        nvl(comp_code,'')as comp_code,
        nvl(comp_name,'')as comp_name,
        nvl(a.company_code,'')as company_code,
        nvl(c.company_code,'') as sale_company_code,
        sales_province_code,
        sales_province,
        sales_city_code,
        sales_city,
        province_code,
        province_name,
        city_code,
        city_name,
        sales_name,
        work_no,
        a.customer_no,
        customer_name,
        attribute,
        credit_limit,
        temp_credit_limit,
        a.channel,
        a.channel_code,
        --c.channel as channel_id,
        -- c.channel_name ,
        first_category,
        second_category,
        third_category,
        nvl(sign_date,'')sign_date,
        nvl(zterm,'')zterm,
        coalesce(ac_wdq,0)as ac_wdq,
        coalesce(ac_15d,0)as ac_15d,
        coalesce(ac_30d,0)as ac_30d,
        coalesce(ac_60d,0)as ac_60d,
        coalesce(ac_90d,0)as ac_90d,
        coalesce(ac_120d,0)as ac_120d,
        coalesce(ac_180d,0)as ac_180d,
        coalesce(ac_365d,0)as ac_365d,
        coalesce(ac_2y,0)as ac_2y,
        coalesce(ac_3y,0)as ac_3y,
        coalesce(ac_over3y,0)as ac_over3y,
        coalesce(ac_all,0)as ac_all, -- 应收金额=已使用额度
        coalesce(ac_all-ac_wdq ,0)as overdue_account, -- 逾期金额
        coalesce(ac_all,0) as use_ac_all, -- 应收金额=已使用额度
        round(coalesce(avg_sale,0),2)as  avg_sale, -- 大宗与供应链60日均销售额其他30日均
        nvl(d.diff,'')as diff, --帐期
        case when channel_code in ('4','5','6') then 30 else  d.index_data end index_data, -- 转换
        round(case when channel_code in ('4','5','6','8') then coalesce(if(ac_all*30<0,0,ac_all*30),0)
                   else coalesce(b.ac_all*index_data,0)  end ,2)    as unuse_ac, -- 未使用额度
        round(case when channel_code in ('4','5','6') then if( (ac_all+coalesce(avg_sale,0)*30)<0,0, coalesce(ac_all+coalesce(avg_sale,0)*30,0))
                when coalesce(ac_all+coalesce(avg_sale*index_data,0),0)<0 then 0
			    else coalesce(ac_all+coalesce(avg_sale*index_data,0),0)	 end ,2)as new_credit_limit, -- 新信控=ac_all+未来信控
		round(case  when channel_code in ('4','5','6') then coalesce(avg_sale*30,0)
               --when coalesce(avg_sale*index_data,0)<0 then 0
               else  coalesce(avg_sale*index_data,0) end ,2) as future_amount , -- 未来信控=sale/30*index_data
        case when coalesce(a.credit_limit-b.ac_all,0)<= 0 then 0 else round(coalesce(a.credit_limit-b.ac_all,0),2) end  as surplus_credit, -- 剩余信控
        coalesce(c.min_sdt,'') as min_sdt
 from csx_tmp.p_sale_02 as a
   left join csx_tmp.p_sale_03 as b on a.customer_no=b.kunnr and a.company_code=b.comp_code
   left outer join csx_tmp.p_sale_04 as c on b.kunnr=c.customer_no and a.company_code=c.company_name
   left outer join csx_tmp.p_sale_01 as d on b.diff=d.diff
   ;

-- 1、如果首单日期是空白的，且过去30个自然日日均销售也是空白的，那么信控降为0。
-- 2、如果首单日期不是空白的，但是过去30个自然日日均销售是空白的（代表销售未满30天），则信控和原固定额度保持一致。
-- 3、其他情况下都是对比（已使用信控额度+未来信控额度）和原固定额度，取较小值作为新的信控额度。如果对比下来较小值是个负数，则新信控额度为0。
-- 预测客户信控 20191022
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =1000;     --在所有执行mr的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=1000;     --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
insert overwrite table csx_dw.crm_credit_rating partition(sdt)
select sflag,
       hkont,
       account_name,
       comp_code,
       comp_name,
       company_code,
       sale_company_code,
       sales_province_code,
       sales_province,
       sales_city_code,
       sales_city,
       province_code,
       province_name,
       city_code,
       city_name,
       sales_name,
       work_no,
       customer_no,
       customer_name,
       attribute,
       credit_limit,
       temp_credit_limit,
       channel,
       first_category,
       second_category,
       third_category,
       sign_date,
       zterm,
       diff,  -- 帐期时长
       ac_wdq,
       ac_15d,
       ac_30d,
       ac_60d,
       ac_90d,
       ac_120d,
       ac_180d,
       ac_365d,
       ac_2y,
       ac_3y,
       ac_over3y,
       -- 应收金额=已使用额度
       ac_all, 
        -- 逾期金额
       overdue_account,
       ac_all as use_ac_all, -- 应收金额=已使用额度
       avg_sale, -- 30日均销售额
       diff, --帐期
       index_data, -- 转换
       unuse_ac, -- 未使用额度
       new_credit_limit, -- 调整后额度
       future_amount, -- 未来额度,
       surplus_credit,-- 剩余信控
       case when a.customer_no in('105384','102156') then credit_limit 
	        when regexp_replace(sign_date,'-','')>=${hiveconf:s_date_30}  and channel_code in ('1','2','3')  then credit_limit
            when regexp_replace(sign_date,'-','')>=${hiveconf:s_date_90}  and channel_code in ('4','5','6')   then credit_limit
            when channel_code in ('4','5','6') and  min_sdt>${hiveconf:s_date_90} then round(credit_limit,-4)
            when channel_code in ('1','2','3') and  min_sdt>${hiveconf:s_date_30} then round(credit_limit,-3)
                    -- when new_credit_limit=0 then 0
            when channel_code in ('1','2','3') and (min_sdt<=${hiveconf:s_date_30} or min_sdt='')    then 
                --least(a.credit_limit,a.new_credit_limit) else 0
                 round(sort_array(array(credit_limit,new_credit_limit))[0],-3) 
            when channel_code in ('4','5','6') and (min_sdt<=${hiveconf:s_date_90} or min_sdt='')   then 
            -- least(a.credit_limit,a.new_credit_limit) else 0
                 round(sort_array(array(credit_limit,new_credit_limit))[0] ,-4)
            end  credit_limit1,
        min_sdt,
        if(case when customer_no in('105384','102156') then credit_limit  
	        	when regexp_replace(sign_date,'-','')>=${hiveconf:s_date_30} and channel_code in ('1','2','3')  then credit_limit
                when regexp_replace(sign_date,'-','')>=${hiveconf:s_date_90} and channel_code in ('4','5','6')   then credit_limit
                when channel_code in ('4','5','6') and  min_sdt>${hiveconf:s_date_90} then round(credit_limit,-4)
                when channel_code in ('1','2','3') and  min_sdt>${hiveconf:s_date_30} then round(credit_limit,-3)
  -- when new_credit_limit=0 then 0
                when channel_code in ('1','2','3') and (min_sdt<=${hiveconf:s_date_30} or min_sdt='')    then 
   --least(a.credit_limit,a.new_credit_limit) else 0
                 round(sort_array(array(credit_limit,new_credit_limit))[0],-3) 
                when channel_code in ('4','5','6') and (min_sdt<=${hiveconf:s_date_90} or min_sdt='')   then 
   --least(a.credit_limit,a.new_credit_limit) else 0
                  round(sort_array(array(credit_limit,new_credit_limit))[0] ,-4)
                end =credit_limit,'否','是') as change_type,
         ${hiveconf:s_date} sdt
 from
  csx_tmp.p_sale_05 as a
where a.customer_no not like 's%'  ;
