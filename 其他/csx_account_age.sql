-- 设置队列
set mapreduce.job.queuename=caishixian;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=2000;
set hive.groupby.skewindata=false;
set hive.map.aggr = true;
set hive.optimize.sort.dynamic.partition=true;
set i_sdate = '${START_DATE}';
set i_date=date_add(${hiveconf:i_sdate},1);

-- 应收账款和回款数据清洗
drop table b2b.csx_hepecc_bsid;
CREATE table b2b.csx_hepecc_bsid
as
select 
  a.hkont,
  a.bukrs as comp_code, -- 公司代码
  case when length(a.kunnr) < 3 then a.lifnr else a.kunnr end as kunnr, -- 号
  a.budat, -- 过机时间
  'A' as prctr,
  '' as shop_name,
  a.dmbtr, -- 账款
  case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000') then 'Y004' else coalesce(c.zterm, d.zterm) end as zterm, -- 账期类型
  case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000') then 45 else coalesce(c.diff, d.diff) end as diff, -- 账期
  concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2)) as sdate,
  case when kunnr in ('V7126','V7127','V7128','V7129','V7130','V7131','V7132','V7000')
      then date_add(last_day(concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2))), 45)
    when coalesce(c.zterm, d.zterm) like 'Y%' 
      then date_add(last_day(concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2))), COALESCE(c.diff, d.diff, 0))
    else date_add(concat(substr(a.budat, 1, 4), '-', substr(a.budat, 5, 2), '-', substr(a.budat, 7, 2)), COALESCE(c.diff, d.diff, 0)) end as edate -- 帐期结束日期
from 
(
  select * 
  from ods_ecc.ecc_ytbcustomer
  where sdt = regexp_replace(${hiveconf:i_date}, '-', '') and budat < regexp_replace(${hiveconf:i_date}, '-', '')
    and mandt='800' and (substr(hkont, 1, 3) <> '139' or (substr(hkont, 1, 3) = '139' and budat >= '20190201'))
) a 
left join 
( 
  select 
    customer_number,
    company_code,
    payment_terms zterm,
    cast(payment_days as int) diff 
  from csx_dw.customer_account_day a 
  where sdt = 'current' and customer_number <> ''
)c on lpad(a.kunnr, 10, '0') = lpad(c.customer_number, 10, '0') and a.bukrs = c.company_code
left join 
(
  select 
    customer_no,
    payment_terms zterm,
	cast(payment_days as int) diff 
  from csx_dw.customer_m 
  where sdt = regexp_replace(date_sub(current_date, 1), '-', '') and customer_no <> ''
)d on lpad(a.kunnr, 10, '0') = lpad(d.customer_no, 10, '0');

-- 应收金额
drop table b2b_tmp.temp_account_out;
CREATE temporary table b2b_tmp.temp_account_out
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
	sum(dmbtr) as amount
  from b2b.csx_hepecc_bsid
  where dmbtr >= 0 
  group by comp_code, kunnr, hkont, budat, prctr, shop_name, sdate, edate, zterm, diff
)a;

-- 回款金额
drop table b2b_tmp.temp_account_in;
CREATE temporary table b2b_tmp.temp_account_in 
as
select 
  hkont,
  comp_code,
  kunnr,
  prctr,
  sum(dmbtr) as amount
from b2b.csx_hepecc_bsid a 
where dmbtr < 0 
group by hkont, comp_code, kunnr, prctr;


-- 已收账款不足应收账款
drop table b2b_tmp.temp_account_left;
CREATE temporary table b2b_tmp.temp_account_left
as
select 
  a.comp_code, a.prctr, a.shop_name, a.kunnr, a.hkont, a.budat, a.sdate, a.edate, zterm, diff,
  case when coalesce(a.sum_sq, 0) + b.amount < 0 then a.sum_bq + b.amount else a.amount end as amount,
  a.rno, a.sum_bq + b.amount as amount_left
from b2b_tmp.temp_account_out a 
join b2b_tmp.temp_account_in b 
  on a.hkont = b.hkont and a.comp_code = b.comp_code and a.kunnr = b.kunnr and a.prctr = b.prctr
where a.sum_bq + b.amount >= 0
-- 已收账款超过应收账款
union all 
select 
  a.comp_code, a.prctr, a.shop_name, a.kunnr, a.hkont, a.budat, a.sdate, a.edate, zterm, diff,
  a.sum_bq + b.amount as amount, a.rno, a.sum_bq + b.amount as amount_left
from b2b_tmp.temp_account_out a 
join 
(
  select 
    hkont, comp_code, kunnr, prctr, max(rno) as rno_max 
  from b2b_tmp.temp_account_out 
  group by hkont,comp_code,kunnr,prctr
)c on a.hkont = c.hkont and a.comp_code = c.comp_code and a.kunnr = c.kunnr and a.rno = c.rno_max and a.prctr = c.prctr
join b2b_tmp.temp_account_in b 
  on a.hkont = b.hkont and a.comp_code = b.comp_code and a.kunnr = b.kunnr and a.prctr = b.prctr
where a.sum_bq + b.amount < 0 
-- 只有应收没有回款
union all 
select 
  a.comp_code, a.prctr, a.shop_name, a.kunnr, a.hkont, a.budat, a.sdate, a.edate, zterm, diff,
  a.amount, a.rno, a.sum_bq as amount_left
from b2b_tmp.temp_account_out a 
left join b2b_tmp.temp_account_in b 
  on a.hkont = b.hkont and a.comp_code = b.comp_code and a.kunnr = b.kunnr and a.prctr = b.prctr
where b.amount is null
union all 
-- 只有预付没有应收
select 
  a.comp_code, a.prctr, a.shop_name, a.kunnr, a.hkont, a.budat, a.sdate, a.edate, zterm, diff,
  a.amount as amount, null as rno, a.amount as amount_left
from 
(
  select 
    comp_code, kunnr, hkont, cast(budat as int) as budat, prctr,
    shop_name, sdate, edate, zterm, diff, sum(dmbtr) as amount
  from b2b.csx_hepecc_bsid
  where dmbtr < 0
  group by comp_code, kunnr, hkont, budat, prctr, shop_name, sdate, edate, zterm, diff
)a left join 
(
  select 
    hkont, comp_code, kunnr, prctr, sum(amount) as amount 
  from b2b_tmp.temp_account_out 
  group by hkont, comp_code, kunnr, prctr
)c on a.hkont = c.hkont and a.comp_code = c.comp_code and a.kunnr = c.kunnr and a.prctr = c.prctr
where c.amount is null;


insert overwrite table csx_dw.account_age_dtl_fct partition (sdt) 
select 
  c.sflag, -- 类型
  a.hkont, -- 科目代码
  d.account_name, -- 科目名称
  a.comp_code, -- 公司代码
  b.comp_name,
  a.prctr, -- 利润中心
  a.shop_name,
  a.kunnr, -- 编码
  c.cust_name as name,
  zterm, -- 账期类型
  case when zterm like 'Y%' then concat('月结', diff) else concat('票到', diff) end as diff, -- 账期
  sum(case when datediff(${hiveconf:i_sdate}, a.sdate) >= 0 then amount else 0 end) as ac_all, -- 全部账款
  sum(case when a.edate >= ${hiveconf:i_sdate} then amount else 0 end) as ac_wdq, -- 未到期账款
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 1 and 15 then amount else 0 end) as ac_15d, -- 15天内账款
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 31 then amount else 0 end) as ac_30d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 32 and 60 then amount else 0 end) as ac_60d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90 then amount else 0 end) as ac_90d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120 then amount else 0 end) ac_120d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180 then amount else 0 end) as ac_180d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365 then amount else 0 end) as ac_365d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) > 365 then amount else 0 end) as ac_over365d, -- 满一年账款
  regexp_replace(${hiveconf:i_sdate}, '-', '') as sdt  -- 过账日期分区
from b2b_tmp.temp_account_left a
join 
(
  select distinct comp_code,comp_name 
  from dim.dim_shop 
  where edate='9999-12-31'
)b on a.comp_code = b.comp_code
left join csx_ods.b2b_customer_new c 
  on lpad(a.kunnr, 10, '0') = lpad(c.cust_id, 10, '0')
left join csx_dw.sap_account_type d 
  on a.hkont = d.accunt_code
group by c.sflag, a.hkont, d.account_name, a.comp_code, b.comp_name, a.prctr, a.shop_name, a.kunnr, 
  c.cust_name, zterm, case when zterm like 'Y%' then concat('月结', diff) else concat('票到', diff) end;

insert overwrite table csx_dw.account_age_dtl_fct_new partition (sdt) 
select 
  c.sflag,
  a.hkont,
  d.account_name,
  a.comp_code,
  b.comp_name,
  a.prctr,
  a.shop_name,
  a.kunnr,
  c.cust_name as name,
  zterm,
  case when zterm like 'Y%' then concat('月结', diff) else concat('票到', diff) end as diff,
  sum(case when datediff(${hiveconf:i_sdate}, a.sdate) >= 0 then amount else 0 end) as ac_all,
  sum(case when a.edate >= ${hiveconf:i_sdate} then amount else 0 end) as ac_wdq,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 1 and 15 then amount else 0 end) as ac_15d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 31 then amount else 0 end) as ac_30d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 32 and 60 then amount else 0 end) as ac_60d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90 then amount else 0 end) as ac_90d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120 then amount else 0 end) as ac_120d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180 then amount else 0 end) as ac_180d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365 then amount else 0 end) as ac_365d,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 366 and 730 then amount else 0 end) as ac_2y,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) between 731 and 1095 then amount else 0 end) as ac_3y,
  sum(case when datediff(${hiveconf:i_sdate}, a.edate) > 1095 then amount else 0 end) as ac_over3y,
  regexp_replace(${hiveconf:i_sdate}, '-', '') as sdt 
from b2b_tmp.temp_account_left a
join 
(
  select distinct comp_code,comp_name 
  from dim.dim_shop 
  where edate = '9999-12-31'
)b on a.comp_code = b.comp_code
left join csx_ods.b2b_customer_new c 
  on lpad(a.kunnr, 10, '0') = lpad(c.cust_id, 10, '0')
left join csx_dw.sap_account_type d 
  on a.hkont = d.accunt_code
group by c.sflag, a.hkont, d.account_name, a.comp_code, b.comp_name, a.prctr, a.shop_name, a.kunnr, 
  c.cust_name, zterm, case when zterm like 'Y%' then concat('月结', diff) else concat('票到', diff) end;


-- 插入
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
  case when zterm like 'Y%' then concat('月结',diff) else concat('票到',diff) end payment_terms,
  diff as payment_days, -- 账期天数
  a.sdate, -- 过账日期
  a.edate, -- 帐期结束日期
  datediff(${hiveconf:i_sdate}, a.edate) as over_days, -- 逾期天数
  sum(amount) as ac_all, -- 账期金额
  -- if 应收金额*账期天数<= 0 ，0，else 逾期金额*逾期天数/应收金额*账期天数
  round(case when SUM(amount) * diff <= 0 then 0 
    else coalesce(SUM(case when datediff(${hiveconf:i_sdate}, a.edate) > 0 then amount * datediff(${hiveconf:i_sdate}, a.edate) end ), 0)
      / coalesce(SUM(amount) * diff, 0) end, 4) as rate, -- 逾期率
  current_timestamp() as write_time,
  regexp_replace(${hiveconf:i_sdate},'-','') as sdt
from b2b_tmp.temp_account_left a
join
(
  select distinct
    comp_code,
    comp_name
  from dim.dim_shop
  where edate='9999-12-31'
)b on a.comp_code=b.comp_code
left join
(
  select customer_no,customer_name,channel,channel_code 
  from csx_dw.customer_m 
  where sdt=regexp_replace(${hiveconf:i_sdate},'-','') 
) c on lpad(a.kunnr,10,'0')=lpad(c.customer_no,10,'0')
left join csx_dw.sap_account_type d
on a.hkont=d.accunt_code
left join 
(
  select shop_name, location_code 
  from csx_dw.csx_shop 
  where sdt='current'
)e on regexp_replace(a.prctr,'(^0*)','') = e.location_code
group by c.channel,c.channel_code,a.hkont,d.account_name,a.comp_code,b.comp_name,a.prctr,regexp_replace(a.kunnr ,'(^0*)',''),
  c.customer_name,zterm,diff,a.sdate,case when zterm like 'Y%' then concat('月结',diff) else concat('票到',diff) end,
  datediff(${hiveconf:i_sdate}, a.edate),${hiveconf:i_sdate},a.edate;
