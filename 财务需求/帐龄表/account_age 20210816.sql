-- 设置动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;
-- 今天日期
set today = '${enddate}';
-- 昨日日期
set yesterday_1 = date_sub(${hiveconf:today}, 1);
set yesterday = date_sub(${hiveconf:today}, 1);
--刷历史用，指定刷某一天,如刷上月底最后一天
--set yesterday = last_day(add_months(date_sub(current_date,1),-1)); --上月底最后一天'2021-07-31'


-- 应收账款和回款数据清洗
drop table csx_tmp.csx_hepecc_bsid;
CREATE temporary table csx_tmp.csx_hepecc_bsid
as
select
  a.hkont,
  a.bukrs as comp_code, -- 公司代码
  case when length(a.kunnr) < 3 then a.lifnr else regexp_replace(a.kunnr,'(^0*)','') end as kunnr, -- 号
  a.budat, -- 过机时间
  'A' as prctr,
  '' as shop_name,
  a.dmbtr, -- 账款
 c.zterm  as zterm, -- 账期类型
 c.diff  as diff, -- 账期天数
 payment_name,  --帐期名称
   concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2)) as sdate,
    case when kunnr like 'V7%'
    then date_add(last_day(concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2))), 45)
   when c.zterm like 'Y%'
    then date_add(last_day(concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2))), COALESCE(c.diff, 0))
  else date_add(concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2)), COALESCE(c.diff, 0)) end as edate -- 帐期结束日期
--     date_add(concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2)), COALESCE(c.diff, 0))  as edate
from
(
--  select mandt,bukrs,belnr,gjahr,budat,kunnr,lifnr,prctr,hkont,dmbtr,sdt
--  from ods_ecc.ecc_ytbcustomer
--  where sdt = regexp_replace(${hiveconf:today}, '-', '') and budat <= regexp_replace(${hiveconf:yesterday}, '-', '')
--    and mandt='800' and (substr(hkont, 1, 3) <> '139' or (substr(hkont, 1, 3) = '139' and budat >= '20190201'))
----and -- 剔除利润调整凭证 科目+年度+凭证号+公司代码
-----      concat_ws('-', hkont ,  gjahr,  belnr, bukrs) not in (
-----        '1122010000-2020-0090526358-1933', '1122010000-2020-0090526357-1933', '1122010000-2020-0090446438-1933', '1122010000-2020-0090446437-1933',
----       '1122010000-2020-0090446436-1933', '1122010000-2020-0101042210-2200', '1122010000-2020-0100794408-2121', '1122010000-2020-0100794407-2121',
----        '1122010000-2020-0100698829-2121', '1122010000-2020-0100698828-2121', '1122010000-2020-0100698815-2121', '1122010000-2020-0100698814-2121',
-----        '1122010000-2020-0100698811-2121', '1122010000-2020-0100698810-2121', '1122010000-2020-0100698807-2121', '1122010000-2020-0100698806-2121',
-----        '1122010000-2020-0100599788-2202', '1122010000-2020-0100387789-2400', '1122010000-2020-0100384016-2300', '1122010000-2020-0100343582-2403',
-----       '1122010000-2020-0100343559-2403', '1122010000-2020-0100343558-2403', '1122010000-2020-0100339686-2402', '1122010000-2020-0100245041-2303',
-----        '1122010000-2020-0100154283-2700','1122010000-2020-0100004543-2800', '1122010000-2020-0100183238-2700',
----        '1122010000-2020-0100404461-2402', '1122010000-2020-0100467273-2400', '1122010000-2020-0100468834-2300', '1122010000-2020-0100755372-2202',
-----        '1122010000-2020-0100873656-2121', '1122010000-2020-0101263298-2200', '1122010000-2020-0090572072-1933')
--union all
--  select mandt,bukrs,belnr,gjahr,budat,kunnr,'' lifnr,prctr,hkont,if(shkzg='H',-dmbtr,dmbtr) as dmbtr,sdt
--  from dw.fin_csx_bsad_fct
--  where sdt = regexp_replace(${hiveconf:yesterday_1}, '-', '')
--  and budat <= regexp_replace(${hiveconf:yesterday}, '-', '')
--  and mandt='800' and (substr(hkont, 1, 3) <> '139' or (substr(hkont, 1, 3) = '139' and budat >= '20190201'))

  select distinct concat_ws('-',belnr,kunnr,bukrs,budat,buzei) as id,
    mandt,bukrs,belnr,gjahr,buzei,budat,kunnr,lifnr,prctr,hkont,dmbtr
  from ods_ecc.ecc_ytbcustomer
  where sdt >= '20210629' 
  --where sdt = regexp_replace(${hiveconf:today}, '-', '') 
    and budat <= regexp_replace(${hiveconf:yesterday}, '-', '')
	and (budat<'20210722' or belnr not like'21%')   --过账日期>=20210722,且凭证号以21开头的单据剔除
    and mandt='800' and (substr(hkont, 1, 3) <> '139' or (substr(hkont, 1, 3) = '139' and budat >= '20190201')) 
) a left join
(
  select
    customer_no,
    company_code,
    payment_name , --帐期类型名称
    payment_terms zterm,     --帐期类型代码
    cast(payment_days as int) diff   --帐期天数
  from csx_dw.dws_crm_w_a_customer_company
  where sdt = 'current'
)c on lpad(a.kunnr, 10, '0') = lpad(c.customer_no, 10, '0') and a.bukrs = c.company_code;

-- 应收金额

-- select * from csx_tmp.temp_account_out;
drop table csx_tmp.temp_account_out;
CREATE temporary table csx_tmp.temp_account_out
as
select
  a.*,
  row_number() OVER(PARTITION BY hkont,comp_code,kunnr,prctr ORDER BY budat asc) as rno,
  sum(amount) over(PARTITION BY hkont,comp_code,kunnr,prctr order by budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) as sum_sq,
  sum(amount) over(PARTITION BY hkont,comp_code,kunnr,prctr order by budat asc) as sum_bq
from
(
  select
    comp_code,
    kunnr,
    hkont,
    cast(budat as int) as budat,
    prctr,
    shop_name,
    sdate,
    edate,
    zterm,
    diff,
    payment_name,
    sum(dmbtr) as amount
  from csx_tmp.csx_hepecc_bsid
  where dmbtr >= 0
  group by comp_code, kunnr, hkont, budat, prctr, shop_name, sdate, edate, zterm, diff,payment_name
)a;

-- 回款金额
drop table csx_tmp.temp_account_in;
CREATE temporary table csx_tmp.temp_account_in
as
select
  hkont,
  comp_code,
  kunnr,
  prctr,
  sum(dmbtr) as amount
from csx_tmp.csx_hepecc_bsid a
where dmbtr < 0
group by hkont, comp_code, kunnr, prctr;


-- 已收账款不足应收账款
drop table csx_tmp.temp_account_left;
CREATE temporary table csx_tmp.temp_account_left
as
select
  a.comp_code, a.prctr, a.shop_name, a.kunnr, a.hkont, a.budat, a.sdate, a.edate, zterm, diff,payment_name,
  case when coalesce(a.sum_sq, 0) + b.amount < 0 then a.sum_bq + b.amount else a.amount end as amount,
  a.rno, a.sum_bq + b.amount as amount_left
from csx_tmp.temp_account_out a
join csx_tmp.temp_account_in b
  on a.hkont = b.hkont and a.comp_code = b.comp_code and a.kunnr = b.kunnr and a.prctr = b.prctr
where a.sum_bq + b.amount >= 0
-- 已收账款超过应收账款
union all
select
  a.comp_code, a.prctr, a.shop_name, a.kunnr, a.hkont, a.budat, a.sdate, a.edate, zterm, diff,payment_name,
  a.sum_bq + b.amount as amount, a.rno, a.sum_bq + b.amount as amount_left
from csx_tmp.temp_account_out a
join
(
  select
    hkont, comp_code, kunnr, prctr, max(rno) as rno_max
  from csx_tmp.temp_account_out
  group by hkont,comp_code,kunnr,prctr
)c on a.hkont = c.hkont and a.comp_code = c.comp_code and a.kunnr = c.kunnr and a.rno = c.rno_max and a.prctr = c.prctr
join csx_tmp.temp_account_in b
  on a.hkont = b.hkont and a.comp_code = b.comp_code and a.kunnr = b.kunnr and a.prctr = b.prctr
where a.sum_bq + b.amount < 0
-- 只有应收没有回款
union all
select
  a.comp_code, a.prctr, a.shop_name, a.kunnr, a.hkont, a.budat, a.sdate, a.edate, zterm, diff,payment_name,
  a.amount, a.rno, a.sum_bq as amount_left
from csx_tmp.temp_account_out a
left join csx_tmp.temp_account_in b
  on a.hkont = b.hkont and a.comp_code = b.comp_code and a.kunnr = b.kunnr and a.prctr = b.prctr
where b.amount is null
union all
-- 只有预付没有应收
select
  a.comp_code, a.prctr, a.shop_name, a.kunnr, a.hkont, a.budat, a.sdate, a.edate, zterm, diff,payment_name,
  a.amount as amount, null as rno, a.amount as amount_left
from
(
  select
    comp_code, kunnr, hkont, cast(budat as int) as budat, prctr,
    shop_name, sdate, edate, zterm, diff,payment_name, sum(dmbtr) as amount
  from csx_tmp.csx_hepecc_bsid
  where dmbtr < 0
  group by comp_code, kunnr, hkont, budat, prctr, shop_name, sdate, edate, zterm, diff,payment_name
)a left join
(
  select
    hkont, comp_code, kunnr, prctr, sum(amount) as amount
  from csx_tmp.temp_account_out
  group by hkont, comp_code, kunnr, prctr
)c on a.hkont = c.hkont and a.comp_code = c.comp_code and a.kunnr = c.kunnr and a.prctr = c.prctr
where c.amount is null;

-- 账龄表
insert overwrite table csx_dw.account_age_dtl_fct partition (sdt)
select
  NULL as sflag, -- 类型
  a.hkont, -- 科目代码
  d.account_name, -- 科目名称
  a.comp_code, -- 公司代码
  b.comp_name,
  a.prctr, -- 利润中心
  a.shop_name,
  regexp_replace(kunnr,'(^0*)','') as kunnr, -- 编码
  c.customer_name as name,
  zterm, -- 账期类型
  -- case when zterm like 'Y%' then concat('月结', diff) else concat('票到', diff) end as diff, -- 账期
  payment_name as diff,
  sum(case when datediff(${hiveconf:yesterday}, a.sdate) >= 0 then amount else 0 end) as ac_all, -- 全部账款
  sum(case when a.edate >= ${hiveconf:yesterday} then amount else 0 end) as ac_wdq, -- 未到期账款
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 1 and 15 then amount else 0 end) as ac_15d, -- 15天内账款
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 16 and 31 then amount else 0 end) as ac_30d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 32 and 60 then amount else 0 end) as ac_60d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 61 and 90 then amount else 0 end) as ac_90d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 91 and 120 then amount else 0 end) ac_120d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 121 and 180 then amount else 0 end) as ac_180d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 181 and 365 then amount else 0 end) as ac_365d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) > 365 then amount else 0 end) as ac_over365d, -- 满一年账款
  regexp_replace(${hiveconf:yesterday}, '-', '') as sdt  -- 过账日期分区
from csx_tmp.temp_account_left a
join
(
  select code as comp_code, name as comp_name
  from csx_dw.dws_basic_w_a_company_code
  where sdt = 'current'
)b on a.comp_code = b.comp_code
left join
(
  select distinct customer_no, customer_name
  from csx_dw.dws_crm_w_a_customer
  where sdt = 'current'
) c on lpad(a.kunnr,10,'0')=lpad(c.customer_no,10,'0')
left join 
(select code as accunt_code,name as account_name from csx_ods.source_basic_w_a_md_accounting_subject where sdt=regexp_replace(${hiveconf:yesterday}, '-', '')) as  d
  on a.hkont = d.accunt_code
group by a.hkont, d.account_name, a.comp_code, b.comp_name, a.prctr, a.shop_name, a.kunnr, c.customer_name,
   zterm,payment_name;

-- 账龄表（新增字段）
insert overwrite table csx_dw.account_age_dtl_fct_new partition (sdt)
select
  NULL as sflag,
  a.hkont, -- 科目代码
  d.account_name, -- 科目名称
  a.comp_code, -- 公司代码
  b.comp_name,
  a.prctr, -- 利润中心
  a.shop_name,
  a.kunnr, -- 编码
  c.customer_name as name,
   zterm, -- 账期类型
  payment_name as diff, -- 账期
  sum(case when datediff(${hiveconf:yesterday}, a.sdate) >= 0 then amount else 0 end) as ac_all, -- 全部账款
  sum(case when a.edate >= ${hiveconf:yesterday} then amount else 0 end) as ac_wdq, -- 未到期账款
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 1 and 15 then amount else 0 end) as ac_15d, -- 15天内账款
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 16 and 31 then amount else 0 end) as ac_30d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 32 and 60 then amount else 0 end) as ac_60d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 61 and 90 then amount else 0 end) as ac_90d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 91 and 120 then amount else 0 end) ac_120d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 121 and 180 then amount else 0 end) as ac_180d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 181 and 365 then amount else 0 end) as ac_365d,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 366 and 730 then amount else 0 end) as ac_2y,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) between 731 and 1095 then amount else 0 end) as ac_3y,
  sum(case when datediff(${hiveconf:yesterday}, a.edate) > 1095 then amount else 0 end) as ac_over3y,
  e.last_sales_date,
  e.last_to_now_days,
  e.customer_active_status_code as customer_active_sts_code,
  regexp_replace(${hiveconf:yesterday}, '-', '') as sdt  -- 过账日期分区
from csx_tmp.temp_account_left a
join
(
  select code as comp_code, name as comp_name
  from csx_dw.dws_basic_w_a_company_code
  where sdt = 'current'
)b on a.comp_code = b.comp_code
left join
(
  select distinct customer_no, customer_name
  from csx_dw.dws_crm_w_a_customer
  where sdt = 'current'
) c on lpad(a.kunnr,10,'0')=lpad(c.customer_no,10,'0')
left join 
(select code as accunt_code,name as account_name from csx_ods.source_basic_w_a_md_accounting_subject where sdt=regexp_replace(${hiveconf:yesterday}, '-', '')) as  d
  on a.hkont = d.accunt_code
left join
(
  select * from csx_dw.dws_sale_w_a_customer_company_active
  where sdt = 'current'
) e on lpad(a.kunnr,10,'0')=lpad(e.customer_no,10,'0') and a.comp_code = e.sign_company_code
group by a.hkont, d.account_name, a.comp_code, b.comp_name, a.prctr, a.shop_name, a.kunnr, c.customer_name,
   zterm, diff, e.last_sales_date,
   e.last_to_now_days, e.customer_active_status_code,payment_name;

-- 账龄每日明细数据插入
insert overwrite table csx_dw.ads_fis_r_a_customer_days_overdue_dtl partition(sdt)
select
  c.channel,
  c.channel_code,
  a.hkont as subject_code,
  d.account_name as subject_name,
  a.comp_code,
  b.comp_name,
  regexp_replace(a.prctr,'(^0*)','') as shop_id,
  '' as shop_name,
  regexp_replace(a.kunnr ,'(^0*)','') as customer_no,
  customer_name,
   zterm,
  payment_name as  payment_terms,
  diff as payment_days, -- 账期天数
  a.sdate, -- 过账日期
  a.edate, -- 帐期结束日期
  datediff(${hiveconf:yesterday}, a.edate) as over_days, -- 逾期天数
  sum(amount) as ac_all, -- 账期金额
  -- if 应收金额*账期天数<= 0 ，0，else 逾期金额*逾期天数/应收金额*账期天数
  round(case when SUM(amount) * diff <= 0 then 0
    else coalesce(SUM(case when datediff(${hiveconf:yesterday}, a.edate) > 0 then amount * datediff(${hiveconf:yesterday}, a.edate) end ), 0)
      / coalesce(SUM(amount) * diff, 0) end, 4) as rate, -- 逾期率
  current_timestamp() as write_time,
  regexp_replace(${hiveconf:yesterday},'-','') as sdt
from csx_tmp.temp_account_left a
join
(
  select code as comp_code, name as comp_name
  from csx_dw.dws_basic_w_a_company_code
  where sdt = 'current'
)b on a.comp_code=b.comp_code
left join
(
  select customer_no,customer_name,channel_name channel,channel_code
  from csx_dw.dws_crm_w_a_customer
  where sdt=regexp_replace(${hiveconf:yesterday},'-','')
) c on lpad(a.kunnr,10,'0')=lpad(c.customer_no,10,'0')
left join
(select code as accunt_code,name as account_name from csx_ods.source_basic_w_a_md_accounting_subject where sdt=regexp_replace(${hiveconf:yesterday}, '-', '')) as  d
on a.hkont=d.accunt_code
group by c.channel,c.channel_code,a.hkont,d.account_name,a.comp_code,b.comp_name,a.prctr,regexp_replace(a.kunnr ,'(^0*)',''),
  c.customer_name,zterm,diff,a.sdate,payment_name,
  datediff(${hiveconf:yesterday}, a.edate),${hiveconf:yesterday},a.edate;
