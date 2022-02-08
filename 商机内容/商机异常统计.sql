-- 1.同时存在同一名称商机 
-- 规则：同一商机名称大于1，且商机审批完成时间间隔小于30天。 
-- 2.商机预计签约时间晚于预计履约时间 
-- 3.预估一次性配送金额大于50万 
-- 4.预估月度配送金额大于200万。 
-- 5.预估合同签约金额大于2千万。 
-- 6.预估毛利率大于30% 
-- 7.预估毛利率小于5%。

--   宽表 dws_crm_w_a_business_customer 报表 ads_crm_r_m_business_customer


--   gross_profit_rate  --预计毛利率
--   approval_status    --审批状态 0:待发起 1：审批中 2：审批完成 3：审批拒绝
--   status             --  '是否有效 0.无效 1.有效', 
--  `expect_sign_time` string COMMENT '预计签约时间', 
--  `expect_execute_time` string COMMENT '预计履约时间', 
--  `sign_time` string COMMENT '签约时间', 
--  `first_sign_time` string COMMENT '首次签约时间'
--  `estimate_once_amount` string COMMENT '预估一次性配送金额', 
--  `estimate_month_amount` string COMMENT '预估月度配送金额', 
--  `estimate_contract_amount` string COMMENT '预计合同签约金额', 



-- 1.1 计算重复商机客户
drop  table  csx_tmp.temp_sj_01 ;
create temporary table csx_tmp.temp_sj_01 as 
 select province_code,
    province_name,
    customer_name,
    count(*) as cust_num
 from 
 csx_dw.dws_crm_w_a_business_customer
 where sdt='20220131'
     and create_time>='2022-01-01 00:00:00 '

    and approval_status=2
    and status=1
    group by province_code,
    province_name,
    customer_name
    ;
    
    
-- 更新最早时间与最新时间
drop table csx_tmp.temp_sj_02;
create temporary table csx_tmp.temp_sj_02 as 
 select 
   a.province_code,
   a.province_name,
   a.customer_name,
   max_update, 
   min_update,
   datediff(max_update,min_update) as diff_days
from (
 select province_code,
    province_name,
    customer_name,
    min(update_time) min_update
 from 
 csx_dw.dws_crm_w_a_business_customer
 where sdt='20220131'
     and create_time>='2022-01-01 00:00:00 '
    and approval_status=2
    and status=1
group by province_code,
    province_name,
    customer_name
)a 
left join 
(select province_code,
    province_name,
    customer_name,
    max(update_time) max_update
 from 
 csx_dw.dws_crm_w_a_business_customer
 where sdt='20220131'
     and create_time>='2022-01-01 00:00:00 '

    and approval_status=2
    and status=1
group by province_code,
    province_name,
    customer_name
 )  c on a.province_code=c.province_code and a.customer_name=c.customer_name

 ;


-- 1.3 计算更新日期30天以内的客户数 
  select 
a.province_code,
a.province_name,
a.city_group_name,
a.customer_name,
a.channel_name,
a.sales_name,
a.first_category_name,
a.second_category_name,
a.archive_category_name,
a.cooperation_mode_name,
a.estimate_once_amount,
a.estimate_month_amount,
a.estimate_contract_amount,
a.price_period_name,
a.gross_profit_rate,
a.expect_sign_time,
a.expect_execute_time,
a.create_time,
a.create_by,
a.update_time,
a.sign_time,
a.first_sign_time,
max_update, 
min_update,
diff_days
from  
 ( select 
 province_code,
province_name,
city_group_name,
customer_name,
channel_name,
sales_name,
first_category_name,
second_category_name,
archive_category_name,
cooperation_mode_name,
estimate_once_amount,
estimate_month_amount,
estimate_contract_amount,
price_period_name,
gross_profit_rate,
expect_sign_time,
expect_execute_time,
create_time,
create_by,
update_time,
sign_time,
first_sign_time
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131'
    and create_time>='2022-01-01 00:00:00 '
   and approval_status=2
    and status=1
)a
join (
 select  a.province_code,
   a.province_name,
   a.customer_name,
   max_update, 
   min_update,
   diff_days 
 from csx_tmp.temp_sj_02 a 
 join csx_tmp.temp_sj_01 b on  a.province_code=b.province_code and a.customer_name=b.customer_name
 where b.cust_num>1
     and a.diff_days between 1 and 29
) b on a.customer_name=b.customer_name and a.province_code=b.province_code
 ;



--2.1 商机签约时间大于履约时间
  select 
a.province_code,
a.province_name,
a.city_group_name,
a.customer_name,
a.channel_name,
a.sales_name,
a.first_category_name,
a.second_category_name,
a.archive_category_name,
a.cooperation_mode_name,
a.estimate_once_amount,
a.estimate_month_amount,
a.estimate_contract_amount,
a.price_period_name,
a.gross_profit_rate,
a.expect_sign_time,
a.expect_execute_time,
a.create_time,
a.create_by,
a.update_time,
a.sign_time,
a.first_sign_time
from  
 ( select 
 province_code,
province_name,
city_group_name,
customer_name,
channel_name,
sales_name,
first_category_name,
second_category_name,
archive_category_name,
cooperation_mode_name,
estimate_once_amount,
estimate_month_amount,
estimate_contract_amount,
price_period_name,
gross_profit_rate,
expect_sign_time,
expect_execute_time,
create_time,
create_by,
update_time,
sign_time,
first_sign_time
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131'
    and create_time>='2022-01-01 00:00:00 '

   and approval_status=2
    and status=1
)a
join
(
SELECT province_code,
    province_name,
    customer_name,
    expect_execute_time,
    expect_sign_time
from  csx_dw.dws_crm_w_a_business_customer 
where sdt='20220131' 
    and create_time>='2022-01-01 00:00:00 '

    and expect_sign_time>expect_execute_time
    and expect_execute_time!=''
    and approval_status=2
    and status=1
 ) b on a.province_code=b.province_code and a.customer_name=b.customer_name   ;
    
--3.1 预估配送额>50万

SELECT province_code,
    province_name,
    customer_name,
    estimate_once_amount
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131' 
    and create_time>='2022-01-01 00:00:00 '

    and  estimate_once_amount>=50
    and approval_status=2
    and status=1
GROUP BY province_code,
    province_name,
    customer_name,
    estimate_once_amount
    ;
    
    
--4.1 预估配送额>50万

SELECT province_code,
    province_name,
    customer_name,
    estimate_month_amount
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131' 
    and create_time>='2022-01-01 00:00:00 '

    and  estimate_month_amount>=200
    and approval_status=2
    and status=1
GROUP BY province_code,
    province_name,
    customer_name,
    estimate_month_amount
    ;


--5.1 预估合同金额>2000万

SELECT province_code,
    province_name,
    customer_name,
    estimate_contract_amount
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131' 
    and  estimate_contract_amount>=2000
    and approval_status=2
    and status=1
GROUP BY province_code,
    province_name,
    customer_name,
    estimate_contract_amount
    ;
    
-- 预估毛利率大于30% 
-- 7.预估毛利率小于5%。
 select 
a.province_code,
a.province_name,
a.city_group_name,
a.customer_name,
a.channel_name,
a.sales_name,
a.first_category_name,
a.second_category_name,
a.archive_category_name,
a.cooperation_mode_name,
a.estimate_once_amount,
a.estimate_month_amount,
a.estimate_contract_amount,
a.price_period_name,
a.gross_profit_rate,
a.expect_sign_time,
a.expect_execute_time,
a.create_time,
a.create_by,
a.update_time,
a.sign_time,
a.first_sign_time
from  
 ( select 
 province_code,
province_name,
city_group_name,
customer_name,
channel_name,
sales_name,
first_category_name,
second_category_name,
archive_category_name,
cooperation_mode_name,
estimate_once_amount,
estimate_month_amount,
estimate_contract_amount,
price_period_name,
gross_profit_rate,
expect_sign_time,
expect_execute_time,
create_time,
create_by,
update_time,
sign_time,
first_sign_time
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131'
    and create_time>='2022-01-01 00:00:00 '

   and approval_status=2
    and status=1
)a
join 
(
SELECT province_code,
    province_name,
    customer_name,
    count(DISTINCT case when gross_profit_rate<5 then customer_name end ) as gross_5,
    count(DISTINCT case when gross_profit_rate>30 then customer_name end ) as gross_30
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131' 
    and create_time>='2022-01-01 00:00:00 '

    and  (gross_profit_rate<5 or gross_profit_rate>30)
    and approval_status=2
    and status=1
GROUP BY province_code,
    province_name,
    customer_name
) b on a.province_code=b.province_code and a.customer_name=b.customer_name
    ;

    -- 月度金额>200万&一次性金额>50万&合同金额>2000千万
 select 
a.province_code,
a.province_name,
a.city_group_name,
a.customer_name,
a.channel_name,
a.sales_name,
a.first_category_name,
a.second_category_name,
a.archive_category_name,
a.cooperation_mode_name,
a.estimate_once_amount,
a.estimate_month_amount,
a.estimate_contract_amount,
a.price_period_name,
a.gross_profit_rate,
a.expect_sign_time,
a.expect_execute_time,
a.create_time,
a.create_by,
a.update_time,
a.sign_time,
a.first_sign_time
from  
 ( select 
 province_code,
province_name,
city_group_name,
customer_name,
channel_name,
sales_name,
first_category_name,
second_category_name,
archive_category_name,
cooperation_mode_name,
estimate_once_amount,
estimate_month_amount,
estimate_contract_amount,
price_period_name,
gross_profit_rate,
expect_sign_time,
expect_execute_time,
create_time,
create_by,
update_time,
sign_time,
first_sign_time
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131'
    and create_time>='2022-01-01 00:00:00 '

   and approval_status=2
    and status=1
)a
join 
(SELECT province_code,
    province_name,
    customer_name
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131' 
    and create_time>='2022-01-01 00:00:00 '

    and (coalesce( estimate_contract_amount,0) >= 2000  or  coalesce(estimate_once_amount,0) >= 50 or coalesce( estimate_month_amount,0) >= 200)
    and approval_status=2
    and status=1
GROUP BY province_code,
    province_name,
    customer_name
)b on a.province_code=b.province_code and a.customer_name=b.customer_name
    ;



-- 统计异常配送额客户数


select 
    a.province_code,
   a.province_name,
   all_num,
   diff_days,
   execute_num,
   estimate_once,           --预估一次性配送金额
   estimate_month,          --月配送金额
   estimate_contract,       --合同金额
   gross_5,
   gross_30
from 
(select 
    a.province_code,
   a.province_name,
   count(customer_name) as all_num
from  csx_dw.dws_crm_w_a_business_customer  a 
    where sdt ='20220131' 
    and create_time>='2022-01-01 00:00:00 '
    and approval_status=2
    and status=1
group by a.province_code,
   a.province_name
    )a 
    left join 
    (
 select 
   a.province_code,
   a.province_name,
   sum( diff_days) diff_days,
   sum(execute_num) execute_num,
   sum(estimate_once) estimate_once,
   sum(estimate_month) estimate_month,
   sum(estimate_contract) estimate_contract,
   sum(gross_5 ) gross_5,
   sum(gross_30) gross_30
  from (
  select 
   a.province_code,
   a.province_name,
   count( a.customer_name) diff_days,
   0 execute_num,
   0 estimate_contract,
   0 estimate_month,
   0 estimate_once,
   0 gross_5,
   0 gross_30
 from csx_tmp.temp_sj_02 a 
 join csx_tmp.temp_sj_01 b on  a.province_code=b.province_code and a.customer_name=b.customer_name
 where b.cust_num>1
     and a.diff_days between 1 and 29
    group by a.province_code,
             a.province_name
union all 
SELECT province_code,
    province_name,
    0 diff_days,
    count(distinct customer_name) as execute_num,
   0 estimate_contract,
   0 estimate_month,
   0 estimate_once,
   0 gross_5,
    0 gross_30
from  csx_dw.dws_crm_w_a_business_customer 
where sdt='20220131' 
    and create_time>='2022-01-01 00:00:00 '

    and expect_sign_time>expect_execute_time
    and expect_execute_time!=''
group by 
    province_code,
    province_name
union all 
SELECT province_code,
    province_name,
    0 diff_days,
    0 as execute_num,
    0 estimate_contract, --合同数
    0 estimate_month,
    0 estimate_once,
     count(DISTINCT case when gross_profit_rate<5 then customer_name end ) as  gross_5,
     count(DISTINCT case when gross_profit_rate>30 then customer_name end ) as gross_30
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131' 
    and create_time>='2022-01-01 00:00:00 '
    and  (gross_profit_rate<5 or gross_profit_rate>30)   
    and approval_status=2
    and status=1
GROUP BY province_code,
    province_name
union all 
SELECT province_code,
    province_name,
    0 diff_days,
    0 as execute_num,
    count(DISTINCT case when cast(coalesce( estimate_contract_amount,0) as int)>=2000 then customer_name end ) estimate_contract, --合同数
    count(DISTINCT case when cast(coalesce( estimate_month_amount,0) as int)>=200 then customer_name end ) estimate_month,
    count(DISTINCT case when coalesce(estimate_once_amount,0)>=50 then  customer_name end  ) estimate_once,
    0 gross_5,
    0 gross_30
from  csx_dw.dws_crm_w_a_business_customer 
where sdt ='20220131' 
    and create_time>='2022-01-01 00:00:00 '

    and (coalesce( estimate_contract_amount,0) >= 2000  or  coalesce(estimate_once_amount,0) >= 50 or coalesce( estimate_month_amount,0) >= 200)
    and approval_status=2
    and status=1
GROUP BY province_code,
    province_name
) a  
group by    a.province_code,
   a.province_name
  ) b on a.province_code=b.province_code;




