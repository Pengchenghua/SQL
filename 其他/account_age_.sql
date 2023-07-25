set i_sdate = '${START_DATE}';
set i_date  =date_add(${hiveconf:i_sdate},1);
drop table b2b.csx_hepecc_bsid
;

CREATE table b2b.csx_hepecc_bsid as
select
	a.bukrs comp_code                                                                ,--公司代码
	a.kunnr                                                                          ,--编码
	a.budat                                                                          ,--日期
	regexp_extract(a.prctr, '(0|^)([^0].*)',2) prctr                                 ,--利润中心代码
	e.shop_name                                                                      ,--利润中心名称
	a.dmbtr                                                                          ,--金额
	c.zterm                                                                          ,--帐期
	d.diff                                                                           ,--帐期天数
	concat(substr(a.budat,1,4),'-',
	substr(a.budat,5,2),'-',substr(a.budat,7,2))            as                  sdate,
	date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(d.diff,0)) edate
from
	(
		select *
		from
			ods_ecc.ecc_ytbcustomer
		where
			hkont like '1122%'
			and sdt  =regexp_replace(${hiveconf:i_date},'-','')
			and budat<regexp_replace(${hiveconf:i_date},'-','')
			and mandt='800'
			
	)
	a
	left join
		(
			select distinct
				shop_id,
				shop_name
			from
				dim.dim_shop
			where
				edate             ='9999-12-31'
				and sales_dist_new like '6%'
		)
		e
		on
			regexp_extract(a.prctr, '(0|^)([^0].*)',2)=e.shop_id
	left join
		(
			select
				bukrs,
				kunnr,
				zterm
			from
				ods_ecc.ecc_knb1
			where
				kunnr not like 'S%'
			group by
				bukrs,
				kunnr,
				zterm
		)
		c
		on
			a.bukrs    =c.bukrs
			and a.kunnr=c.kunnr
	left join
		(
			select
				zterm,
				cast(ztag1 as int) diff
			from
				ods_ecc.ecc_t052
			where
				mandt='800'
		)
		d
		on
			c.zterm=d.zterm
;

-- 回款金额
drop table b2b_tmp.temp_account_out
;

CREATE temporary table b2b_tmp.temp_account_out as
select
	a.*,
	row_number() OVER(PARTITION BY comp_code,kunnr,prctr ORDER BY
					  budat asc)rno,
	sum(amount)over(PARTITION BY comp_code,kunnr,prctr order by
					budat asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) sum_sq,
	sum(amount)over(PARTITION BY comp_code,kunnr,prctr order by
					budat asc)sum_bq
from
	(
		select
			comp_code              ,
			kunnr                  ,
			cast(budat as int)budat,
			prctr                  ,
			shop_name              ,
			sdate                  ,
			edate                  ,
			zterm                  ,
			diff                   ,
			sum(dmbtr)amount
		from
			b2b.csx_hepecc_bsid a
		where
			dmbtr>=0
		group by
			comp_code,
			kunnr    ,
			budat    ,
			prctr    ,
			shop_name,
			sdate    ,
			edate    ,
			zterm    ,
			diff
	)
	a
;

--应收金额
drop table b2b_tmp.temp_account_in
;

CREATE temporary table b2b_tmp.temp_account_in as
select
	comp_code,
	kunnr    ,
	prctr    ,
	sum(dmbtr)amount
from
	b2b.csx_hepecc_bsid a
where
	dmbtr<0
group by
	comp_code,
	kunnr    ,
	prctr
;

--已收账款不足应收账款
drop table b2b_tmp.temp_account_left
;

CREATE temporary table b2b_tmp.temp_account_left as
select
	a.comp_code,
	a.prctr    ,
	a.shop_name,
	a.kunnr    ,
	a.budat    ,
	a.sdate    ,
	a.edate    ,
	zterm      ,
	diff       ,
	case
		when coalesce(a.sum_sq,0)+b.amount<0
			then a.sum_bq        +b.amount
			else a.amount
	end amount ,
	a.rno      ,
	a.sum_bq+b.amount amount_left
from
	b2b_tmp.temp_account_out a
	join
		b2b_tmp.temp_account_in b
		on
			(
				a.comp_code=b.comp_code
				and a.kunnr=b.kunnr
				and a.prctr=b.prctr
			)
where
	a.sum_bq+b.amount>=0
--已收账款超过应收账款
union all
select
	a.comp_code              ,
	a.prctr                  ,
	a.shop_name              ,
	a.kunnr                  ,
	a.budat                  ,
	a.sdate                  ,
	a.edate                  ,
	zterm                    ,
	diff                     ,
	a.sum_bq+b.amount amount ,
	a.rno                    ,
	a.sum_bq+b.amount amount_left
from
	b2b_tmp.temp_account_out a
	join
		(
			select
				comp_code,
				kunnr    ,
				prctr    ,
				max(rno)rno_max
			from
				b2b_tmp.temp_account_out
			group by
				comp_code,
				kunnr    ,
				prctr
		)
		c
		on
			(
				a.comp_code=c.comp_code
				and a.kunnr=c.kunnr
				and a.rno  =c.rno_max
				and a.prctr=c.prctr
			)
	join
		b2b_tmp.temp_account_in b
		on
			(
				a.comp_code=b.comp_code
				and a.kunnr=b.kunnr
				and a.prctr=b.prctr
			)
where
	a.sum_bq+b.amount<0
--只有应收没有收款
union all
select
	a.comp_code,
	a.prctr    ,
	a.shop_name,
	a.kunnr    ,
	a.budat    ,
	a.sdate    ,
	a.edate    ,
	zterm      ,
	diff       ,
	a.amount   ,
	a.rno      ,
	a.sum_bq amount_left
from
	b2b_tmp.temp_account_out a
	left join
		b2b_tmp.temp_account_in b
		on
			(
				a.comp_code=b.comp_code
				and a.kunnr=b.kunnr
				and a.prctr=b.prctr
			)
where
	b.amount is null
union all
--只有预付没有收款
select
	a.comp_code     ,
	a.prctr         ,
	a.shop_name     ,
	a.kunnr         ,
	a.budat         ,
	a.sdate         ,
	a.edate         ,
	zterm           ,
	diff            ,
	a.amount amount ,
	null     rno    ,
	a.amount amount_left
from
	(
		select
			comp_code              ,
			kunnr                  ,
			cast(budat as int)budat,
			prctr                  ,
			shop_name              ,
			sdate                  ,
			edate                  ,
			zterm                  ,
			diff                   ,
			sum(dmbtr)amount
		from
			b2b.csx_hepecc_bsid a
		where
			dmbtr<0
		group by
			comp_code,
			kunnr    ,
			budat    ,
			prctr    ,
			shop_name,
			sdate    ,
			edate    ,
			zterm    ,
			diff
	)
	a
	left join
		(
			select
				comp_code,
				kunnr    ,
				prctr    ,
				sum(amount)amount
			from
				b2b_tmp.temp_account_out
			group by
				comp_code,
				kunnr    ,
				prctr
		)
		c
		on
			(
				a.comp_code=c.comp_code
				and a.kunnr=c.kunnr
				and a.prctr=c.prctr
			)
where
	c.amount is null
;

set hive.exec.parallel              =true;
set hive.exec.dynamic.partition     =true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.account_age_dtl_fct partition
	(sdt
	)
select
	a.comp_code,
	b.comp_name,
	a.prctr    ,
	a.shop_name,
	a.kunnr    ,
	c.name     ,
	zterm      ,
	diff       ,
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
				when a.edate>${hiveconf:i_sdate}
					then amount
					else 0
			end
		)
	ac_wdq,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 0 and 15
					then amount
					else 0
			end
		)
	ac_15d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 16 and 30
					then amount
					else 0
			end
		)
	ac_30d,
	sum
		(
			case
				when datediff(${hiveconf:i_sdate}, a.edate) between 31 and 60
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
				when datediff(${hiveconf:i_sdate}, a.edate)>365
					then amount
					else 0
			end
		)
	                                           ac_over365d,
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
		csx_ods.b2b_customer c
		on
			a.kunnr=c.cust_id
group by
	a.comp_code,
	b.comp_name,
	a.prctr    ,
	a.shop_name,
	a.kunnr    ,
	c.name     ,
	zterm      ,
	diff
;
