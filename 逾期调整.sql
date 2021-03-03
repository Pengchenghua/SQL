--20200518将利润中心调整凭证号剔除
-- 设置队列
--set mapreduce.job.queuename=caishixian;
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
CREATE table b2b.csx_hepecc_bsid as
select
	a.hkont          ,
	a.bukrs comp_code,
	case
		when length(a.kunnr)<3
			then regexp_replace(a.lifnr,'^0*','')
			else regexp_replace(a.kunnr,'^0*','')
	end kunnr   ,
	a.budat     ,
	'A'prctr    ,
	'' shop_name,
	a.dmbtr     ,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then 'Y004'
			else coalesce(c.zterm,d.zterm)
	end zterm,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then 45
			else coalesce(c.diff,d.diff)
	end                                                                         diff ,
	concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)) sdate,
	case
		when kunnr in ('V7126',
					   'V7127',
					   'V7128',
					   'V7129',
					   'V7130',
					   'V7131',
					   'V7132',
					   'V7000')
			then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),45)
		when coalesce(c.zterm,d.zterm) like 'Y%'
			then date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,d.diff,0))
			else date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,d.diff,0))
	end edate
from
	(
		select *
		from
			ods_ecc.ecc_ytbcustomer
		where
			sdt      =regexp_replace(${hiveconf:i_date},'-','')
			and budat<regexp_replace(${hiveconf:i_date},'-','')
			and mandt='800'
			and
			(
				substr(hkont,1,3)<>'139'
				or
				(
					substr(hkont,1,3)='139'
					and budat       >='20190201'
				)
			)
	and 
	-- 剔除利润调整凭证 科目+年度+凭证号+公司代码
		concat_ws('-',
		hkont ,
		gjahr,
		belnr,
		bukrs) not in (
	'1122010000-2020-0090526358-1933',
    '1122010000-2020-0090526357-1933',
    '1122010000-2020-0090446438-1933',
    '1122010000-2020-0090446437-1933',
    '1122010000-2020-0090446436-1933',
    '1122010000-2020-0101042210-2200',
    '1122010000-2020-0100794408-2121',
    '1122010000-2020-0100794407-2121',
    '1122010000-2020-0100698829-2121',
    '1122010000-2020-0100698828-2121',
    '1122010000-2020-0100698815-2121',
    '1122010000-2020-0100698814-2121',
    '1122010000-2020-0100698811-2121',
    '1122010000-2020-0100698810-2121',
    '1122010000-2020-0100698807-2121',
    '1122010000-2020-0100698806-2121',
    '1122010000-2020-0100599788-2202',
    '1122010000-2020-0100387789-2400',
    '1122010000-2020-0100384016-2300',
    '1122010000-2020-0100343582-2403',
    '1122010000-2020-0100343559-2403',
    '1122010000-2020-0100343558-2403',
    '1122010000-2020-0100339686-2402',
    '1122010000-2020-0100245041-2303',
    '1122010000-2020-0100154283-2700',
    '1122010000-2020-0100066952-2105',
    '1122010000-2020-0100004543-2800',
	-- 20200526 增加
	'1122010000-2020-0100183238-2700',
	'1122010000-2020-0100404461-2402',
	'1122010000-2020-0100467273-2400',
	'1122010000-2020-0100468834-2300',
	'1122010000-2020-0100755372-2202',
	'1122010000-2020-0100873656-2121',
	'1122010000-2020-0101263298-2200',
	'1122010000-2020-0090572072-1933'
)
			)
	a
	left join
		(
			select
				customer_number    ,
				company_code       ,
				payment_terms             zterm,
				cast(payment_days as int) diff
			from
				csx_dw.customer_account_day a
			where
				sdt                 ='current'
				and customer_number<>''
		)
		c
		on
			(
				regexp_replace(a.kunnr,'^0*','')=c.customer_number
				and a.bukrs         =c.company_code
			)
	left join
		(
			select
				customer_no        ,
				payment_terms             zterm,
				cast(payment_days as int) diff
			from
				csx_dw.dws_crm_w_a_customer_m
			where
				sdt             =regexp_replace(date_sub(current_date,1),'-','')
				and customer_no<>''
		)
		d
		on
			regexp_replace(a.kunnr,'^0*','')=d.customer_no
;


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
drop table csx_dw.peng_temp_account_data_02
;

CREATE table csx_dw.peng_temp_account_data_02 as
-- insert overwrite table csx_dw.account_age_dtl_fct_new partition
--  (sdt
--  )
select
	c.channel       ,
	a.hkont         ,
	d.account_name  ,
	a.comp_code     ,
	b.comp_name     ,
	a.prctr         ,
	a.shop_name     ,
	a.kunnr         ,
	c.customer_name ,
	a.zterm         ,
	case
		when a.zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end diff,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.sdate) >=0
					then amount
					else 0
			end
		)
	ac_all,
	sum
		(
			case
				when a.edate>=${hiveconf:i_sdate}
					then amount
					else 0
			end
		)
	ac_wdq,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 1 and 15
					then amount
					else 0
			end
		)
	ac_15d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 31
					then amount
					else 0
			end
		)
	ac_30d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 32 and 60
					then amount
					else 0
			end
		)
	ac_60d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 61 and 90
					then amount
					else 0
			end
		)
	ac_90d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 91 and 120
					then amount
					else 0
			end
		)
	ac_120d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 121 and 180
					then amount
					else 0
			end
		)
	ac_180d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 181 and 365
					then amount
					else 0
			end
		)
	ac_365d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)between 366 and 730
					then amount
					else 0
			end
		)
	ac_2y,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)between 731 and 1095
					then amount
					else 0
			end
		)
	ac_3y,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate)>1095
					then amount
					else 0
			end
		)
	                                           ac_over3y,
	regexp_replace(${hiveconf:i_sdate},'-','') sdt
from
	b2b_tmp.temp_account_left a
	join
		(
			select distinct
				comp_code,
				comp_name
			from
				dim.dim_shop
			where
				edate='9999-12-31'
		)
		b
		on
			a.comp_code=b.comp_code
	left join
		(
			select *
			from
				csx_dw.dws_crm_w_a_customer_m
			where
				sdt=regexp_replace(date_sub(current_date,1),'-','')
		)
		c
		on
			a.kunnr=c.customer_no
	left join
		csx_dw.sap_account_type d
		on
			a.hkont=d.accunt_code
where
	1=1
	--a.hkont like '1122%'
group by
	c.channel       ,
	a.hkont         ,
	d.account_name  ,
	a.comp_code     ,
	b.comp_name     ,
	a.prctr         ,
	a.shop_name     ,
	a.kunnr         ,
	c.customer_name ,
	a.zterm         ,
	case
		when a.zterm like 'Y%'
			then concat('月结',diff)
			else concat('票到',diff)
	end
;